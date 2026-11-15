//===----------------------------------------------------------------------===//
// Spark expression grammar on top of DuckDB's PEG grammar.
//
// DuckDB's Expression rule is replaced by Spark's precedence ladder (from Spark 3.5's
// SqlBaseParser.g4):
//
//   expression  : lambda | booleanExpression
//   boolean     : NOT boolean | valueExpression predicate? | boolean AND boolean | boolean OR boolean
//   predicate   : [NOT] BETWEEN | [NOT] IN | [NOT] RLIKE | [NOT] LIKE/ILIKE [ANY|ALL] | IS ...
//   value       : unary (- + ~) > * / % DIV > + - || > & > ^ > | > comparisons
//   primary     : atom ( [index] | .field | ::type )*
//
// Atoms reuse DuckDB's SingleExpression (literals, function calls, CASE, CAST, subqueries, ...).
// Operators become calls into the `spark` function schema so they get Spark semantics
// (integer overflow wraps, division by zero is NULL, 0-based array subscripts, ...).
//===----------------------------------------------------------------------===//

#include "spark_dialect.hpp"

#if defined(__linux__) || defined(__APPLE__)
#include <pthread.h>
#endif

#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/peg/parsed_grammar.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include <atomic>
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/query_node/select_node.hpp"

#include <functional>
#include <exception>

namespace duckdb {

namespace {

using ExprPtr = unique_ptr<ParsedExpression>;
typedef ExprPtr (*expr_fn_t)(PEGTransformer &, ParseResult &);

//! The hooks below transform sub-expressions recursively (DuckDB's own transformer does not), and every level
//! of nesting passes through the whole precedence ladder (about 1 KB of stack per hook, ~17 hooks per level).
//! Deeply nested input must fail with an error instead of overflowing the stack, so the stack used by nested
//! hooks is bounded by a share of the thread's stack, leaving the rest for binding and execution.
idx_t ThreadStackSize() {
#if defined(__APPLE__)
	return pthread_get_stacksize_np(pthread_self());
#elif defined(__linux__) && defined(__GLIBC__)
	pthread_attr_t attr;
	if (pthread_getattr_np(pthread_self(), &attr) == 0) {
		size_t size = 0;
		pthread_attr_getstacksize(&attr, &size);
		pthread_attr_destroy(&attr);
		if (size > 0) {
			return size;
		}
	}
	return 2 * 1024 * 1024;
#else
	return 1024 * 1024;
#endif
}

//! Runs `fn` on a thread with a large stack and waits for it; exceptions are rethrown here. Returns false (without
//! running `fn`) on platforms where the stack size of a thread cannot be chosen.
bool RunOnLargeStack(const std::function<void()> &fn) {
#if defined(__linux__) || defined(__APPLE__)
	struct Job {
		const std::function<void()> *fn;
		std::exception_ptr error;
	} job {&fn, nullptr};
	pthread_attr_t attr;
	if (pthread_attr_init(&attr) != 0) {
		return false;
	}
	// the stack is mapped lazily: only what a statement really uses is committed
	if (pthread_attr_setstacksize(&attr, size_t(64) * 1024 * 1024) != 0) {
		pthread_attr_destroy(&attr);
		return false;
	}
	pthread_t thread;
	auto entry = [](void *arg) -> void * {
		auto &j = *static_cast<Job *>(arg);
		try {
			(*j.fn)();
		} catch (...) {
			j.error = std::current_exception();
		}
		return nullptr;
	};
	if (pthread_create(&thread, &attr, entry, &job) != 0) {
		pthread_attr_destroy(&attr);
		return false;
	}
	pthread_attr_destroy(&attr);
	pthread_join(thread, nullptr);
	if (job.error) {
		std::rethrow_exception(job.error);
	}
	return true;
#else
	return false;
#endif
}

const idx_t MAX_EXPRESSION_DEPTH = 1000;

//! The nesting depth of an expression, not looking deeper than `limit` levels (so the walk itself cannot overflow)
idx_t ExpressionDepth(const ParsedExpression &expr, idx_t limit = MAX_EXPRESSION_DEPTH + 1) {
	if (limit <= 1) {
		return 1;
	}
	idx_t deepest = 0;
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { deepest = MaxValue(deepest, ExpressionDepth(child, limit - 1)); });
	return deepest + 1;
}

void CheckExpressionDepth(const ParsedExpression &expr) {
	// DuckDB rejects expressions nested deeper than 1000 levels, but only after the binder has walked them
	// recursively on the calling thread's stack, which a very deep tree can overflow: reject them here
	if (ExpressionDepth(expr) > MAX_EXPRESSION_DEPTH) {
		throw ParserException("Max expression depth limit of %llu exceeded. Use \"SET max_expression_depth TO x\" to "
		                      "increase the maximum expression depth.",
		                      (unsigned long long)MAX_EXPRESSION_DEPTH);
	}
}

const char *const NESTED_TOO_DEEPLY = "Expression is nested too deeply for the Spark dialect";

struct HookDepthGuard {
	struct State {
		idx_t depth = 0;
		bool large_stack = false;
		uintptr_t base = 0;
		idx_t budget = 0;
	};
	static State &Get() {
		static thread_local State state;
		return state;
	}
	HookDepthGuard() {
		auto &state = Get();
		char marker;
		auto here = reinterpret_cast<uintptr_t>(&marker);
		if (state.depth == 0) {
			state.base = here;
			if (state.budget == 0) {
				// once per thread: pthread_getattr_np reads /proc/self/maps for the main thread
				// Statements deeper than this run on the helper thread, whose whole stack is reserved up front: a
				// process short of address space then gets a clean error from the thread creation, instead of a
				// SIGSEGV when this thread's stack cannot grow. 1 MB covers about 60 levels of nesting.
				state.budget = MinValue<idx_t>(ThreadStackSize() / 3, 1024 * 1024);
			}
		}
		state.depth++;
		auto used = state.base > here ? state.base - here : here - state.base;
		if (used > state.budget) {
			state.depth--;
			throw ParserException(NESTED_TOO_DEEPLY);
		}
	}
	~HookDepthGuard() {
		Get().depth--;
	}
};

template <expr_fn_t FN>
arena_ptr<TransformResultValue> FinalizeExpr(PEGTransformer &t, ParseResult &pr) {
	auto &state = HookDepthGuard::Get();
	if (state.depth > 0 || state.large_stack) {
		HookDepthGuard guard;
		return t.MakeResult<ExprPtr>(FN(t, pr));
	}
	// the outermost hook: ordinary statements are transformed right here; one that is nested too deeply for this
	// thread's stack is transformed again on a thread with a large stack (DuckDB's own depth limit then applies)
	try {
		HookDepthGuard guard;
		auto transformed = FN(t, pr);
		CheckExpressionDepth(*transformed);
		return t.MakeResult<ExprPtr>(std::move(transformed));
	} catch (ParserException &e) {
		if (string(e.what()).find(NESTED_TOO_DEEPLY) == string::npos) {
			throw;
		}
	}
	arena_ptr<TransformResultValue> result;
	bool ran = RunOnLargeStack([&]() {
		auto &inner = HookDepthGuard::Get();
		inner.large_stack = true;
		// enough for DuckDB's limit of 1000 nested expressions (about 17 KB of hooks per level); a deeper tree would
		// only be rejected later, after the main thread had to bind, copy or free it
		inner.budget = size_t(24) * 1024 * 1024;
		try {
			HookDepthGuard guard;
			auto transformed = FN(t, pr);
			CheckExpressionDepth(*transformed);
			result = t.MakeResult<ExprPtr>(std::move(transformed));
		} catch (...) {
			inner.large_stack = false;
			throw;
		}
		inner.large_stack = false;
	});
	if (!ran) {
		throw ParserException(NESTED_TOO_DEEPLY);
	}
	return result;
}

template <expr_fn_t FN>
arena_ptr<TransformProcess> ExprProcess(PEGTransformer &t, ParseResult &pr) {
	return t.MakeProcess<FinalizeTransformProcess>(t, pr, FinalizeExpr<FN>);
}

//===----------------------------------------------------------------------===//
// Parse result helpers
//===----------------------------------------------------------------------===//
ParseResult &Unwrap(ParseResult &pr) {
	if (pr.type == ParseResultType::CHOICE) {
		return Unwrap(pr.Cast<ChoiceParseResult>().GetResult());
	}
	return pr;
}

ListParseResult &AsList(ParseResult &pr) {
	return Unwrap(pr).Cast<ListParseResult>();
}

//! The matched alternative of a rule whose body is a choice (A <- B / C)
ParseResult &Alt(ParseResult &pr) {
	return Unwrap(Unwrap(pr).Cast<ListParseResult>().GetChild(0));
}

//! Text of a literal token (keyword / operator), looking through choices and single-element lists.
string Literal(ParseResult &pr_p) {
	auto &pr = Unwrap(pr_p);
	switch (pr.type) {
	case ParseResultType::KEYWORD:
		return pr.Cast<KeywordParseResult>().keyword;
	case ParseResultType::OPERATOR:
		return pr.Cast<OperatorParseResult>().operator_token;
	case ParseResultType::TOKEN:
		return pr.Cast<TokenParseResult>().text;
	case ParseResultType::LIST: {
		auto &list = pr.Cast<ListParseResult>();
		string result;
		for (auto &child : list.GetChildren()) {
			if (!result.empty()) {
				result += " ";
			}
			result += Literal(child.get());
		}
		return result;
	}
	default:
		throw InternalException("spark dialect: expected a literal token, got %s", pr.ToString());
	}
}

bool HasOptional(ParseResult &pr) {
	return pr.Cast<OptionalParseResult>().HasResult();
}

ParseResult &OptionalValue(ParseResult &pr) {
	return pr.Cast<OptionalParseResult>().GetResult();
}

ExprPtr Expr(PEGTransformer &t, ParseResult &pr) {
	return t.Transform<ExprPtr>(pr);
}

//! Elements of X* (an optional repeat) or X+ (a repeat)
vector<reference<ParseResult>> RepeatItems(ParseResult &pr) {
	vector<reference<ParseResult>> result;
	if (pr.type == ParseResultType::OPTIONAL) {
		auto &opt = pr.Cast<OptionalParseResult>();
		if (!opt.HasResult()) {
			return result;
		}
		return RepeatItems(opt.GetResult());
	}
	for (auto &item : pr.Cast<RepeatParseResult>().GetChildren()) {
		result.push_back(item.get());
	}
	return result;
}

//! Elements of a List(D) macro (D (',' D)* ','?) - returns the D parse results
vector<reference<ParseResult>> ListItems(ParseResult &pr_p) {
	vector<reference<ParseResult>> result;
	auto &list = AsList(pr_p);
	result.push_back(list.GetChild(0));
	for (auto &item : RepeatItems(list.GetChild(1))) {
		result.push_back(AsList(item.get()).GetChild(1));
	}
	return result;
}

//===----------------------------------------------------------------------===//
// Expression builders
//===----------------------------------------------------------------------===//
ExprPtr SparkCall(const string &name, vector<ExprPtr> args) {
	return make_uniq<FunctionExpression>(QualifiedName(Identifier(SYSTEM_CATALOG), Identifier(SPARK_SCHEMA),
	                                                   Identifier(name)),
	                                     std::move(args));
}

ExprPtr SparkCall(const string &name, ExprPtr a) {
	vector<ExprPtr> args;
	args.push_back(std::move(a));
	return SparkCall(name, std::move(args));
}

ExprPtr SparkCall(const string &name, ExprPtr a, ExprPtr b) {
	vector<ExprPtr> args;
	args.push_back(std::move(a));
	args.push_back(std::move(b));
	return SparkCall(name, std::move(args));
}

ExprPtr DuckCall(const string &name, vector<ExprPtr> args) {
	return make_uniq<FunctionExpression>(Identifier(name), std::move(args));
}

ExprPtr Not(ExprPtr e) {
	return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(e));
}

ExprPtr MaybeNot(bool negate, ExprPtr e) {
	return negate ? Not(std::move(e)) : std::move(e);
}

//===----------------------------------------------------------------------===//
// Transforms
//===----------------------------------------------------------------------===//
ExprPtr TransformForward(PEGTransformer &t, ParseResult &pr) {
	return Expr(t, Alt(pr));
}

//! Left-associative binary chains: Head Tail* with Tail <- Op Operand
template <class BUILD>
ExprPtr TransformChain(PEGTransformer &t, ParseResult &pr, BUILD build) {
	auto &list = AsList(pr);
	auto result = Expr(t, list.GetChild(0));
	for (auto &tail_ref : RepeatItems(list.GetChild(1))) {
		auto &tail = AsList(tail_ref.get());
		auto op = StringUtil::Upper(Literal(tail.GetChild(0)));
		auto rhs = Expr(t, tail.GetChild(1));
		result = build(op, std::move(result), std::move(rhs));
	}
	return result;
}

ExprPtr TransformOr(PEGTransformer &t, ParseResult &pr) {
	return TransformChain(t, pr, [](const string &, ExprPtr l, ExprPtr r) -> ExprPtr {
		return make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(l), std::move(r));
	});
}

ExprPtr TransformAnd(PEGTransformer &t, ParseResult &pr) {
	return TransformChain(t, pr, [](const string &, ExprPtr l, ExprPtr r) -> ExprPtr {
		return make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(l), std::move(r));
	});
}

ExprPtr TransformNotPrefix(PEGTransformer &t, ParseResult &pr) {
	return Not(Expr(t, AsList(pr).GetChild(1)));
}

ExprPtr TransformComparison(PEGTransformer &t, ParseResult &pr) {
	// bound to a native comparison with Spark's implicit casts (see BindComparisonAs)
	return TransformChain(t, pr, [](const string &op, ExprPtr l, ExprPtr r) -> ExprPtr {
		static const unordered_map<string, string> names = {{"=", "cmp_eq"},  {"==", "cmp_eq"}, {"!=", "cmp_ne"},
		                                                    {"<>", "cmp_ne"}, {"<", "cmp_lt"},  {">", "cmp_gt"},
		                                                    {"<=", "cmp_le"}, {">=", "cmp_ge"}, {"<=>", "cmp_nsafe"}};
		auto entry = names.find(op);
		if (entry == names.end()) {
			throw InternalException("spark dialect: unknown comparison %s", op);
		}
		return SparkCall(entry->second, std::move(l), std::move(r));
	});
}

ExprPtr BinaryOperator(const string &op, ExprPtr l, ExprPtr r) {
	static const unordered_map<string, string> names = {
	    {"+", "add"},     {"-", "subtract"}, {"*", "multiply"}, {"/", "divide"},  {"%", "remainder"},
	    {"DIV", "div"},   {"||", "concat_op"}, {"&", "op_bit_and"}, {"^", "op_bit_xor"}, {"|", "op_bit_or"}};
	auto entry = names.find(op);
	if (entry == names.end()) {
		throw InternalException("spark dialect: unknown operator %s", op);
	}
	return SparkCall(entry->second, std::move(l), std::move(r));
}

ExprPtr TransformBinary(PEGTransformer &t, ParseResult &pr) {
	return TransformChain(t, pr, BinaryOperator);
}

ExprPtr TransformUnary(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	auto op = Literal(list.GetChild(0));
	auto child = Expr(t, list.GetChild(1));
	if (op == "-") {
		// fold negative literals so that -2147483648 stays an INT like in Spark
		if (child->GetExpressionClass() == ExpressionClass::CONSTANT) {
			auto &constant = child->Cast<ConstantExpression>();
			auto text = constant.ToString();
			if (!text.empty() && StringUtil::CharacterIsDigit(text[0]) &&
			    text.find_first_not_of("0123456789.") == string::npos) {
				return ConstantExpression::Number("-" + text);
			}
		}
		return SparkCall("negative", std::move(child));
	}
	if (op == "+") {
		return SparkCall("positive", std::move(child));
	}
	return SparkCall("op_bit_not", std::move(child));
}

//! SpPredicated <- SpComparison SpPredicate?
ExprPtr TransformPredicated(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	auto value = Expr(t, list.GetChild(0));
	auto &opt = list.GetChild(1);
	if (!HasOptional(opt)) {
		return value;
	}
	auto &predicate = Alt(OptionalValue(opt)).Cast<ListParseResult>();
	auto rule = StringUtil::Lower(predicate.Name());
	// every predicate rule starts with an optional NOT except the IS forms
	if (rule == "spbetween") {
		bool negate = HasOptional(predicate.GetChild(0));
		auto lower = Expr(t, predicate.GetChild(2));
		auto upper = Expr(t, predicate.GetChild(4));
		// like Spark: value >= lower AND value <= upper, each comparison with Spark's implicit casts
		auto ge = SparkCall("cmp_ge", value->Copy(), std::move(lower));
		auto le = SparkCall("cmp_le", std::move(value), std::move(upper));
		return MaybeNot(negate, make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(ge), std::move(le)));
	}
	if (rule == "spinquery") {
		bool negate = HasOptional(predicate.GetChild(0));
		auto subquery = make_uniq<SubqueryExpression>();
		subquery->SubqueryMutable() = make_uniq<SelectStatement>();
		auto select = t.Transform<unique_ptr<SelectStatement>>(predicate.GetChild(3));
		subquery->SubqueryMutable() = std::move(select);
		subquery->GetSubqueryTypeMutable() = SubqueryType::ANY;
		subquery->GetChildMutable() = std::move(value);
		subquery->GetComparisonTypeMutable() = ExpressionType::COMPARE_EQUAL;
		return MaybeNot(negate, std::move(subquery));
	}
	if (rule == "spinlist") {
		bool negate = HasOptional(predicate.GetChild(0));
		vector<ExprPtr> children;
		children.push_back(std::move(value));
		for (auto &item : ListItems(predicate.GetChild(3))) {
			children.push_back(Expr(t, item.get()));
		}
		// the value and the list are brought to Spark's common type first (1 IN ('abc', 1) compares strings)
		return MaybeNot(negate, SparkCall("in_list_value", std::move(children)));
	}
	if (rule == "sprlike") {
		bool negate = HasOptional(predicate.GetChild(0));
		auto pattern = Expr(t, predicate.GetChild(2));
		return MaybeNot(negate, SparkCall("rlike", std::move(value), std::move(pattern)));
	}
	if (rule == "splikequantified") {
		bool negate = HasOptional(predicate.GetChild(0));
		auto kind = StringUtil::Lower(Literal(predicate.GetChild(1)));
		auto quantifier = StringUtil::Upper(Literal(predicate.GetChild(2)));
		// x NOT LIKE ANY (p1, p2) is true if x does not match at least one pattern
		vector<ExprPtr> parts;
		auto &items = predicate.GetChild(4);
		if (HasOptional(items)) {
			for (auto &item : ListItems(OptionalValue(items))) {
				parts.push_back(MaybeNot(negate, SparkCall(kind, value->Copy(), Expr(t, item.get()))));
			}
		}
		if (parts.empty()) {
			throw ParserException("Expected something between '(' and ')'");
		}
		auto conjunction = quantifier == "ALL" ? ExpressionType::CONJUNCTION_AND : ExpressionType::CONJUNCTION_OR;
		ExprPtr result = std::move(parts[0]);
		for (idx_t i = 1; i < parts.size(); i++) {
			result = make_uniq<ConjunctionExpression>(conjunction, std::move(result), std::move(parts[i]));
		}
		return result;
	}
	if (rule == "splike") {
		bool negate = HasOptional(predicate.GetChild(0));
		auto kind = StringUtil::Lower(Literal(predicate.GetChild(1)));
		vector<ExprPtr> args;
		args.push_back(std::move(value));
		args.push_back(Expr(t, predicate.GetChild(2)));
		auto &escape = predicate.GetChild(3);
		if (HasOptional(escape)) {
			args.push_back(Expr(t, AsList(OptionalValue(escape)).GetChild(1)));
			if (args.back()->GetExpressionClass() == ExpressionClass::CONSTANT &&
			    StringUtil::CIEquals(args.back()->ToString(), "NULL")) {
				throw ParserException("[INVALID_ESCAPE] the escape character must be a single-character string literal");
			}
		}
		if (args.size() == 2 && (kind == "like" || kind == "ilike")) {
			// no ESCAPE clause: bound by the dialect, which can use DuckDB's own (non-fallible) LIKE operators
			return SparkCall(string(negate ? "not_" : "") + kind + "_op", std::move(args));
		}
		return MaybeNot(negate, SparkCall(kind, std::move(args)));
	}
	if (rule == "spisnull") {
		bool negate = HasOptional(predicate.GetChild(1));
		return make_uniq<OperatorExpression>(negate ? ExpressionType::OPERATOR_IS_NOT_NULL
		                                            : ExpressionType::OPERATOR_IS_NULL,
		                                     std::move(value));
	}
	if (rule == "spisbool") {
		bool negate = HasOptional(predicate.GetChild(1));
		auto literal = StringUtil::Upper(Literal(predicate.GetChild(2)));
		if (literal == "UNKNOWN") {
			return make_uniq<OperatorExpression>(negate ? ExpressionType::OPERATOR_IS_NOT_NULL
			                                            : ExpressionType::OPERATOR_IS_NULL,
			                                     std::move(value));
		}
		auto constant = ConstantExpression::Boolean(literal == "TRUE");
		return make_uniq<ComparisonExpression>(negate ? ExpressionType::COMPARE_DISTINCT_FROM
		                                              : ExpressionType::COMPARE_NOT_DISTINCT_FROM,
		                                       std::move(value), std::move(constant));
	}
	if (rule == "spisdistinct") {
		bool negate = HasOptional(predicate.GetChild(1));
		auto rhs = Expr(t, predicate.GetChild(4));
		return make_uniq<ComparisonExpression>(negate ? ExpressionType::COMPARE_NOT_DISTINCT_FROM
		                                              : ExpressionType::COMPARE_DISTINCT_FROM,
		                                       std::move(value), std::move(rhs));
	}
	throw InternalException("spark dialect: unknown predicate %s", predicate.Name());
}

//! SpPrimary <- SpAtom SpIndirection*
ExprPtr TransformPrimary(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	auto result = Expr(t, list.GetChild(0));
	for (auto &item_ref : RepeatItems(list.GetChild(1))) {
		auto &item = Alt(item_ref.get());
		auto name = StringUtil::Lower(item.Name());
		if (name == "spsubscript") {
			auto index = Expr(t, item.Cast<ListParseResult>().GetChild(1));
			result = SparkCall("subscript", std::move(result), std::move(index));
		} else if (name == "spfield") {
			auto field = t.Transform<string>(item.Cast<ListParseResult>().GetChild(1));
			vector<ExprPtr> args;
			args.push_back(std::move(result));
			args.push_back(ConstantExpression::String(field));
			result = SparkCall("get_field", std::move(args));
		} else {
			// '::' Type
			auto type = t.Transform<LogicalType>(item.Cast<ListParseResult>().GetChild(1));
			result = make_uniq<CastExpression>(type, std::move(result));
		}
	}
	return result;
}

//! Lambda parameters recovered from an already parsed expression (`x -> ...`, `(x, y) -> ...`)
vector<string> LambdaParamsFromExpression(const ParsedExpression &expr) {
	auto fail = []() -> vector<string> { throw ParserException("syntax error at or near \"->\""); };
	auto name_of = [&](const ParsedExpression &e, string &name) {
		if (e.GetExpressionClass() != ExpressionClass::COLUMN_REF || !e.GetAlias().empty()) {
			return false;
		}
		auto &ref = e.Cast<ColumnRefExpression>();
		if (ref.ColumnNames().size() != 1) {
			return false;
		}
		name = ref.ColumnNames()[0].GetIdentifierName();
		return true;
	};
	vector<string> names;
	string name;
	if (name_of(expr, name)) {
		names.push_back(name);
		return names;
	}
	if (expr.GetExpressionClass() == ExpressionClass::FUNCTION) {
		auto &func = expr.Cast<FunctionExpression>();
		auto fname = StringUtil::Lower(func.FunctionName().GetIdentifierName());
		if (fname == "row" || fname == "struct_pack") {
			for (auto &arg : func.GetArguments()) {
				if (!name_of(arg.GetExpression(), name)) {
					return fail();
				}
				names.push_back(name);
			}
			if (!names.empty()) {
				return names;
			}
		}
	}
	return fail();
}

//! SpExpression <- SpOr SpLambdaTail?   (the parameters of `x -> body` are read from the parsed left side)
ExprPtr TransformExprMain(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	auto head = Expr(t, list.GetChild(0));
	auto &tail = list.GetChild(1);
	if (!HasOptional(tail)) {
		return head;
	}
	auto names = LambdaParamsFromExpression(*head);
	auto body = Expr(t, AsList(OptionalValue(tail)).GetChild(1));
	auto lambda = make_uniq<LambdaExpression>(std::move(names), std::move(body));
	lambda->GetLambdaSyntaxTypeMutable() = LambdaSyntaxType::LAMBDA_KEYWORD;
	return std::move(lambda);
}

//! SpArrayCtor <- 'ARRAY' '(' List(SpExpression)? ')'
ExprPtr TransformArrayCtor(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	vector<ExprPtr> args;
	if (HasOptional(list.GetChild(2))) {
		for (auto &item : ListItems(OptionalValue(list.GetChild(2)))) {
			args.push_back(Expr(t, item.get()));
		}
	}
	return SparkCall("array", std::move(args));
}

//! SpMapCtor <- 'MAP' '(' List(SpExpression)? ')'
ExprPtr TransformMapCtor(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	vector<ExprPtr> args;
	if (HasOptional(list.GetChild(2))) {
		for (auto &item : ListItems(OptionalValue(list.GetChild(2)))) {
			args.push_back(Expr(t, item.get()));
		}
	}
	if (args.size() % 2 != 0) {
		throw BinderException("map expects a positive even number of arguments");
	}
	vector<ExprPtr> keys, values;
	for (idx_t i = 0; i < args.size(); i += 2) {
		keys.push_back(std::move(args[i]));
		values.push_back(std::move(args[i + 1]));
	}
	vector<ExprPtr> map_args;
	map_args.push_back(SparkCall("array", std::move(keys)));
	map_args.push_back(SparkCall("array", std::move(values)));
	return SparkCall("map_from_arrays", std::move(map_args));
}

//! exists(array, x -> predicate): the higher-order function, not EXISTS (subquery)
ExprPtr TransformExistsFunction(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	vector<ExprPtr> args;
	for (auto &item : ListItems(list.GetChild(2))) {
		args.push_back(Expr(t, item.get()));
	}
	return SparkCall("exists", std::move(args));
}

//! NULL if any of the (copied) inputs is NULL, else result: Spark's higher-order and zip functions return NULL
//! for a NULL array or map, where DuckDB's list functions treat it as empty
ExprPtr NullIfAnyNull(const vector<ExprPtr> &inputs, ExprPtr result) {
	ExprPtr any_null;
	for (auto &input : inputs) {
		ExprPtr check = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NULL, input->Copy());
		any_null = any_null ? make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(any_null),
		                                                       std::move(check))
		                    : std::move(check);
	}
	if (!any_null) {
		return result;
	}
	auto guarded = make_uniq<CaseExpression>();
	CaseCheck check;
	check.when_expr = std::move(any_null);
	check.then_expr = ConstantExpression::Null();
	guarded->CaseChecksMutable().push_back(std::move(check));
	guarded->ElseMutable() = std::move(result);
	return std::move(guarded);
}

ExprPtr RewriteFunction(ExprPtr expr);

//! The single (unqualified) name token under a parse result, or empty when the name is qualified or absent
void CollectTokens(ParseResult &pr, vector<string> &out) {
	switch (pr.type) {
	case ParseResultType::KEYWORD:
		if (pr.Cast<KeywordParseResult>().keyword != ".") {
			out.push_back(pr.Cast<KeywordParseResult>().keyword);
		}
		break;
	case ParseResultType::IDENTIFIER:
		out.push_back(pr.Cast<IdentifierParseResult>().identifier.GetIdentifierName());
		break;
	case ParseResultType::TOKEN:
		out.push_back(pr.Cast<TokenParseResult>().text);
		break;
	case ParseResultType::OPERATOR:
		out.push_back(pr.Cast<OperatorParseResult>().operator_token);
		break;
	case ParseResultType::CHOICE:
		CollectTokens(pr.Cast<ChoiceParseResult>().GetResult(), out);
		break;
	case ParseResultType::OPTIONAL: {
		auto &opt = pr.Cast<OptionalParseResult>();
		if (opt.HasResult()) {
			CollectTokens(opt.GetResult(), out);
		}
		break;
	}
	case ParseResultType::REPEAT:
		for (auto &child : pr.Cast<RepeatParseResult>().GetChildren()) {
			CollectTokens(child.get(), out);
		}
		break;
	case ParseResultType::LIST:
		for (auto &child : pr.Cast<ListParseResult>().GetChildren()) {
			CollectTokens(child.get(), out);
		}
		break;
	default:
		break;
	}
}

//! spark_strict: only Spark's built-in functions can be called by their plain name
void CheckSparkFunctionName(const string &name) {
	auto lower = StringUtil::Lower(name);
	// percentile_cont / percentile_disc are Spark functions (WITHIN GROUP syntax) that SHOW FUNCTIONS does not list
	if (!IsSparkFunction(lower) && !StringUtil::StartsWith(lower, "__spark_") && lower != "percentile_cont" &&
	    lower != "percentile_disc") {
		throw BinderException("[UNRESOLVED_ROUTINE] Cannot resolve function `%s`: it is not a Spark SQL function", name);
	}
}

//! any(x) / some(x) / every(x): aggregate functions whose names are keywords
ExprPtr TransformKeywordFunction(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	auto name = StringUtil::Lower(Literal(list.GetChild(0)));
	vector<ExprPtr> args;
	for (auto &item : ListItems(list.GetChild(2))) {
		args.push_back(Expr(t, item.get()));
	}
	// these are Spark names already (any, some, every, char, position, left, right)
	return RewriteFunction(make_uniq<FunctionExpression>(Identifier(name), std::move(args)));
}

//! POSITION(substr IN str)
ExprPtr TransformPositionIn(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	return SparkCall("locate", Expr(t, list.GetChild(2)), Expr(t, list.GetChild(4)));
}

//! substr(str FROM pos [FOR len])
ExprPtr TransformSubstrFrom(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	vector<ExprPtr> args;
	args.push_back(Expr(t, list.GetChild(2)));
	args.push_back(Expr(t, list.GetChild(4)));
	if (HasOptional(list.GetChild(5))) {
		args.push_back(Expr(t, AsList(OptionalValue(list.GetChild(5))).GetChild(1)));
	}
	return SparkCall("substr", std::move(args));
}

//! int(x), string(x), date(x), ...: shorthand casts
ExprPtr TransformCastFunction(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	auto name = StringUtil::Upper(Literal(list.GetChild(0)));
	static const unordered_map<string, string> types = {
	    {"INT", "INTEGER"},    {"INTEGER", "INTEGER"}, {"BIGINT", "BIGINT"}, {"SMALLINT", "SMALLINT"},
	    {"TINYINT", "TINYINT"}, {"DOUBLE", "DOUBLE"},  {"FLOAT", "FLOAT"},   {"STRING", "VARCHAR"},
	    {"BOOLEAN", "BOOLEAN"}, {"DATE", "DATE"},      {"TIMESTAMP", "TIMESTAMP"}, {"BINARY", "BLOB"}};
	unique_ptr<TypeExpression> type;
	if (name == "DECIMAL") {
		vector<ExprPtr> mods;
		mods.push_back(ConstantExpression::Integer(10));
		mods.push_back(ConstantExpression::Integer(0));
		type = make_uniq<TypeExpression>(Identifier("DECIMAL"), std::move(mods));
	} else {
		type = make_uniq<TypeExpression>(Identifier(types.at(name)), vector<ExprPtr>());
	}
	auto value = Expr(t, list.GetChild(2));
	auto null_of_type = make_uniq<CastExpression>(std::move(type), ConstantExpression::Null());
	return SparkCall("cast_value", std::move(value), std::move(null_of_type));
}

string DefaultFieldName(const ParsedExpression &expr, idx_t index) {
	if (!expr.GetAlias().empty()) {
		return expr.GetAlias().GetIdentifierName();
	}
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		return colref.GetColumnName().GetIdentifierName();
	}
	if (expr.GetExpressionClass() == ExpressionClass::FUNCTION) {
		auto &func = expr.Cast<FunctionExpression>();
		if (StringUtil::CIEquals(func.FunctionName().GetIdentifierName(), "get_field") &&
		    func.GetArguments().size() == 2 &&
		    func.GetArguments()[1].GetExpression().GetExpressionClass() == ExpressionClass::CONSTANT) {
			auto text = func.GetArguments()[1].GetExpression().ToString();
			return text.size() >= 2 ? text.substr(1, text.size() - 2) : text;
		}
	}
	return "col" + to_string(index + 1);
}

ExprPtr StructPack(vector<pair<string, ExprPtr>> fields) {
	vector<FunctionArgument> args;
	for (auto &field : fields) {
		args.emplace_back(Identifier(field.first), std::move(field.second));
	}
	return make_uniq<FunctionExpression>(Identifier("struct_pack"), std::move(args));
}

//! SpStructCtor <- 'STRUCT' '(' List(SpStructArg)? ')' ; SpStructArg <- SpExpression SpAsAlias?
ExprPtr TransformStructCtor(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	vector<pair<string, ExprPtr>> fields;
	if (HasOptional(list.GetChild(2))) {
		idx_t index = 0;
		for (auto &item_ref : ListItems(OptionalValue(list.GetChild(2)))) {
			auto &item = AsList(item_ref.get());
			auto expr = Expr(t, item.GetChild(0));
			string name;
			if (HasOptional(item.GetChild(1))) {
				name = t.Transform<string>(AsList(OptionalValue(item.GetChild(1))).GetChild(1));
			} else {
				name = DefaultFieldName(*expr, index);
			}
			fields.emplace_back(name, std::move(expr));
			index++;
		}
	}
	if (fields.empty()) {
		return StructPack(std::move(fields));
	}
	return StructPack(std::move(fields));
}

//===----------------------------------------------------------------------===//
// Lambda helpers for higher-order functions
//===----------------------------------------------------------------------===//
ExprPtr ColRef(const string &name) {
	return make_uniq<ColumnRefExpression>(Identifier(name));
}

ExprPtr Operator(const string &op, ExprPtr a, ExprPtr b) {
	vector<ExprPtr> args;
	args.push_back(std::move(a));
	args.push_back(std::move(b));
	auto result = make_uniq<FunctionExpression>(Identifier(op), std::move(args));
	result->IsOperatorMutable() = true;
	return std::move(result);
}

ExprPtr DuckCall(const string &name, ExprPtr a) {
	vector<ExprPtr> args;
	args.push_back(std::move(a));
	return DuckCall(name, std::move(args));
}

ExprPtr DuckCall(const string &name, ExprPtr a, ExprPtr b) {
	vector<ExprPtr> args;
	args.push_back(std::move(a));
	args.push_back(std::move(b));
	return DuckCall(name, std::move(args));
}

ExprPtr MakeLambda(vector<string> params, ExprPtr body) {
	auto lambda = make_uniq<LambdaExpression>(std::move(params), std::move(body));
	lambda->GetLambdaSyntaxTypeMutable() = LambdaSyntaxType::LAMBDA_KEYWORD;
	return std::move(lambda);
}

//! Splits a lambda argument into its parameter names and body
bool LambdaParts(ParsedExpression &expr, vector<string> &params, ExprPtr &body) {
	if (expr.GetExpressionClass() != ExpressionClass::LAMBDA) {
		return false;
	}
	auto &lambda = expr.Cast<LambdaExpression>();
	string error;
	for (auto &colref : lambda.ExtractColumnRefExpressions(error)) {
		params.push_back(colref.get().Cast<ColumnRefExpression>().GetColumnName().GetIdentifierName());
	}
	body = lambda.Right().Copy();
	return error.empty();
}

//! Replaces references to the lambda parameter `name` in `expr`
void Substitute(ExprPtr &expr, const string &name, const ParsedExpression &replacement) {
	if (expr->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &colref = expr->Cast<ColumnRefExpression>();
		if (!colref.IsQualified() && StringUtil::CIEquals(colref.GetColumnName().GetIdentifierName(), name)) {
			expr = replacement.Copy();
			return;
		}
		auto &names = colref.ColumnNames();
		if (names.size() > 1 && StringUtil::CIEquals(names[0].GetIdentifierName(), name)) {
			// l.field.sub: the parameter is a struct value
			ExprPtr result = replacement.Copy();
			for (idx_t i = 1; i < names.size(); i++) {
				result = DuckCall("struct_extract", std::move(result), ConstantExpression::String(names[i].GetIdentifierName()));
			}
			expr = std::move(result);
			return;
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { Substitute(child, name, replacement); });
}

//! The lambda argument at `index` with its parameters replaced by the given expressions
ExprPtr InlineLambda(vector<ExprPtr> &args, idx_t index, const char *function, vector<ExprPtr> replacements) {
	if (index >= args.size()) {
		throw BinderException("%s expects a lambda function argument", function);
	}
	vector<string> params;
	ExprPtr body;
	if (!LambdaParts(*args[index], params, body)) {
		throw BinderException("%s expects a lambda function argument", function);
	}
	if (params.size() > replacements.size()) {
		throw BinderException("%s: the lambda function takes too many arguments", function);
	}
	for (idx_t i = 0; i < params.size(); i++) {
		Substitute(body, params[i], *replacements[i]);
	}
	return body;
}

ExprPtr StructField(const string &var, const string &field) {
	return DuckCall("struct_extract", ColRef(var), ConstantExpression::String(field));
}

//===----------------------------------------------------------------------===//
// Function call hook: route Spark functions to the spark schema; rewrite variadic generators
//===----------------------------------------------------------------------===//
grammar_transform_process_function_t original_function_transform;

ExprPtr GeneratorUnnest(ExprPtr list_of_structs) {
	// unnest reads its options from the alias of the argument
	vector<ExprPtr> args;
	args.push_back(std::move(list_of_structs));
	auto recursive = ConstantExpression::Boolean(true);
	recursive->SetAlias(Identifier("recursive"));
	args.push_back(std::move(recursive));
	auto max_depth = ConstantExpression::Integer(2);
	max_depth->SetAlias(Identifier("max_depth"));
	args.push_back(std::move(max_depth));
	return make_uniq<FunctionExpression>(Identifier("unnest"), std::move(args));
}

vector<ExprPtr> TakeArguments(FunctionExpression &func) {
	vector<ExprPtr> result;
	for (auto &arg : func.GetArgumentsMutable()) {
		result.push_back(std::move(arg.GetExpressionMutable()));
	}
	return result;
}

//! The key of a comparator lambda `(l, r) -> CASE WHEN f(l) < f(r) THEN -1 WHEN f(l) > f(r) THEN 1 ELSE 0 END`
//! (either order of the branches, or with the results swapped for a descending sort).
string StripSpaces(string text) {
	string out;
	for (auto c : text) {
		if (c != ' ' && c != '(' && c != ')') {
			out += c;
		}
	}
	return out;
}

//! `text` with the word `from` replaced by `to`
string ReplaceWord(const string &text, const string &from, const string &to) {
	string out;
	for (idx_t i = 0; i < text.size();) {
		bool boundary_before = i == 0 || !(isalnum(uint8_t(text[i - 1])) || text[i - 1] == '_' || text[i - 1] == '.');
		if (boundary_before && text.compare(i, from.size(), from) == 0) {
			auto after = i + from.size();
			if (after >= text.size() || !(isalnum(uint8_t(text[after])) || text[after] == '_')) {
				out += to;
				i = after;
				continue;
			}
		}
		out += text[i++];
	}
	return out;
}

bool MentionsWord(const string &text, const string &word) {
	return ReplaceWord(text, word, "\x01") != text;
}

bool MatchSortComparator(ParsedExpression &lambda, bool &descending, ExprPtr &key, string &key_param) {
	vector<string> params;
	ExprPtr body;
	if (!LambdaParts(lambda, params, body) || params.size() != 2 ||
	    body->GetExpressionClass() != ExpressionClass::FUNCTION) {
		return false;
	}
	auto &outer = body->Cast<FunctionExpression>();
	if (StringUtil::Lower(outer.FunctionName().GetIdentifierName()) != "case_when" || outer.GetArguments().size() != 5 ||
	    StripSpaces(outer.GetArguments()[4].GetExpression().ToString()) != "0") {
		return false;
	}
	struct Branch {
		bool less;
		bool negative;
		string left, right;
		ExprPtr left_expr;
	};
	Branch branches[2];
	for (idx_t b = 0; b < 2; b++) {
		auto &condition = outer.GetArguments()[b * 2].GetExpression();
		if (condition.GetExpressionClass() != ExpressionClass::FUNCTION) {
			return false;
		}
		auto &compare = condition.Cast<FunctionExpression>();
		auto compare_name = StringUtil::Lower(compare.FunctionName().GetIdentifierName());
		if ((compare_name != "cmp_lt" && compare_name != "cmp_gt") || compare.GetArguments().size() != 2) {
			return false;
		}
		auto value = StripSpaces(outer.GetArguments()[b * 2 + 1].GetExpression().ToString());
		if (value != "1" && value != "-1") {
			return false;
		}
		branches[b].less = compare_name == "cmp_lt";
		branches[b].negative = value == "-1";
		branches[b].left = compare.GetArguments()[0].GetExpression().ToString();
		branches[b].right = compare.GetArguments()[1].GetExpression().ToString();
		branches[b].left_expr = compare.GetArguments()[0].GetExpression().Copy();
	}
	// both branches compare the same pair, in opposite directions with opposite results
	if (branches[0].left != branches[1].left || branches[0].right != branches[1].right ||
	    branches[0].less == branches[1].less || branches[0].negative == branches[1].negative) {
		return false;
	}
	auto &first = branches[0];
	// "x < y gives -1" or "x > y gives 1" sorts x before y: ascending in x
	bool ascending_in_left = first.less == first.negative;
	auto left = first.left, right = first.right;
	auto left_expr = std::move(first.left_expr);
	auto &p1 = params[0], &p2 = params[1];
	string param;
	if (MentionsWord(left, p1) && !MentionsWord(left, p2) && ReplaceWord(left, p1, p2) == right) {
		param = p1;
	} else if (MentionsWord(left, p2) && !MentionsWord(left, p1) && ReplaceWord(left, p2, p1) == right) {
		// f(r) compared with f(l): the sort direction flips
		param = p2;
		ascending_in_left = !ascending_in_left;
	} else {
		return false;
	}
	if (!MentionsWord(left, param)) {
		return false;
	}
	descending = !ascending_in_left;
	key = std::move(left_expr);
	key_param = param;
	return true;
}

ExprPtr RewriteFunction(ExprPtr expr) {
	if (expr->GetExpressionClass() != ExpressionClass::FUNCTION) {
		return expr;
	}
	auto &func = expr->Cast<FunctionExpression>();
	auto &qname = func.GetQualifiedName();
	if (!qname.Schema().empty() || !qname.Catalog().empty()) {
		return expr;
	}
	auto name = StringUtil::Lower(func.FunctionName().GetIdentifierName());
	// calls generated by the tokenizer's rewrites (window, session_window); they bypass the strict name check
	if (name == "__spark_unnest" || name == "__spark_struct_pack") {
		func.SetFunctionName(name.substr(8));
		for (auto &arg : func.GetArgumentsMutable()) {
			auto &argument = arg.GetExpressionMutable();
			if (argument->GetExpressionClass() == ExpressionClass::STAR) {
				// struct_pack(t.*): the row of t as a struct, i.e. struct_pack(*COLUMNS(t.*))
				argument->Cast<StarExpression>().IsColumnsMutable() = true;
				vector<ExprPtr> unpacked;
				unpacked.push_back(std::move(argument));
				argument = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_UNPACK, std::move(unpacked));
			}
		}
		return expr;
	}
	{
		// argument counts of the functions rewritten below (the rewrites index their arguments)
		static const unordered_map<string, pair<idx_t, idx_t>> ARITY = {
		    {"stack", {1, 100000}},         {"json_tuple", {1, 100000}},     {"inline", {1, 1}},
		    {"inline_outer", {1, 1}},       {"explode", {1, 1}},             {"explode_outer", {1, 1}},
		    {"posexplode", {1, 1}},         {"posexplode_outer", {1, 1}},    {"substring", {2, 3}},
		    {"str_to_map", {1, 3}},         {"reflect", {2, 100000}},        {"java_method", {2, 100000}},
		    {"try_reflect", {2, 100000}},   {"if", {3, 3}},                  {"iff", {3, 3}},
		    {"format_string", {1, 100000}}, {"printf", {1, 100000}},         {"elt", {2, 100000}},
		    {"concat_ws", {1, 100000}},     {"transform", {2, 2}},           {"filter", {2, 2}},
		    {"array_sort", {1, 2}},         {"zip_with", {3, 3}},            {"map_filter", {2, 2}},
		    {"transform_keys", {2, 2}},     {"transform_values", {2, 2}},    {"map_zip_with", {3, 3}}};
		auto entry = ARITY.find(name);
		auto count = func.GetArguments().size();
		if (entry != ARITY.end() && (count < entry->second.first || count > entry->second.second)) {
			throw BinderException("[WRONG_NUM_ARGS] The `%s` requires %llu parameters but the actual number is %llu.",
			                      name, (unsigned long long)entry->second.first, (unsigned long long)count);
		}
	}
	{
		// INT parameters: Spark's implicit cast of a BIGINT argument wraps around (non-ANSI); DuckDB's raises an
		// out-of-range error. narrow_int does the wrapping for BIGINT arguments and leaves other types alone.
		static const unordered_map<string, vector<idx_t>> INT_PARAMETERS = {
		    {"add_months", {1}},        {"bround", {1}},            {"round", {1}},             {"bit_get", {1}},           {"getbit", {1}},
		    {"conv", {1, 2}},           {"date_from_unix_date", {0}}, {"make_dt_interval", {0, 1, 2}},
		    {"make_ym_interval", {0, 1}}, {"make_date", {0, 1, 2}}, {"make_interval", {0, 1, 2, 3, 4, 5}},
		    {"left", {1}},              {"right", {1}},             {"repeat", {1}},            {"space", {0}},
		    {"lpad", {1}},              {"rpad", {1}},              {"substring", {1, 2}},      {"substr", {1, 2}},
		    {"shiftleft", {1}},         {"shiftright", {1}},        {"shiftrightunsigned", {1}},
		    {"slice", {1, 2}},          {"format_number", {1}},     {"split_part", {2}},        {"overlay", {2, 3}},
		    {"locate", {2}},            {"factorial", {0}},         {"nth_value", {1}},         {"ntile", {0}},
		    {"levenshtein", {2}},       {"lag", {1}},               {"lead", {1}},              {"make_timestamp", {0, 1, 2, 3, 4}},
		    {"make_timestamp_ntz", {0, 1, 2, 3, 4}}, {"make_timestamp_ltz", {0, 1, 2, 3, 4}},
		    {"regexp_extract", {2}},    {"regexp_extract_all", {2}}, {"regexp_instr", {2}},   {"sha2", {1}}};
		// STRING parameters: Spark's implicit cast turns any atomic value into a string (upper(1) is '1'); DuckDB's
		// string functions accept only strings. as_string leaves strings alone.
		static const unordered_map<string, vector<idx_t>> STRING_PARAMETERS = {
		    {"upper", {0}},        {"lower", {0}},         {"ucase", {0}},        {"lcase", {0}},          {"initcap", {0}},
		    {"trim", {0}},         {"ltrim", {0}},         {"rtrim", {0}},        {"btrim", {0}},          {"replace", {0, 1, 2}},
		    {"translate", {0, 1, 2}}, {"unhex", {0}},      {"luhn_check", {0}},   {"repeat", {0}},         {"instr", {0, 1}},
		    {"locate", {0, 1}},    {"position", {0, 1}},   {"find_in_set", {0, 1}}, {"levenshtein", {0, 1}}, {"sentences", {0}},
		    {"split_part", {0, 1}}, {"soundex", {0}},      {"ascii", {0}},
		    {"date_format", {1}},  {"to_date", {1}},       {"to_timestamp", {1}}, {"from_unixtime", {1}},  {"unix_timestamp", {1}},
		    {"to_unix_timestamp", {1}}, {"trunc", {1}},    {"date_trunc", {0}},   {"next_day", {1}},       {"regexp_extract", {0, 1}},
		    {"regexp_extract_all", {0, 1}}, {"regexp_replace", {0, 1, 2}}, {"regexp_like", {0, 1}}, {"regexp", {0, 1}}, {"rlike", {0, 1}},
		    {"regexp_count", {0, 1}}, {"to_binary", {0}}, {"try_to_binary", {0}}, {"url_decode", {0}}, {"url_encode", {0}}, {"to_number", {0}}, {"try_to_number", {0}}, {"format_string", {0}}, {"printf", {0}}, {"regexp_instr", {0, 1}},  {"regexp_substr", {0, 1}}, {"split", {0, 1}},
		    {"substring_index", {0, 1}}};
		auto string_entry = STRING_PARAMETERS.find(name);
		if (string_entry != STRING_PARAMETERS.end()) {
			auto &arguments = func.GetArgumentsMutable();
			for (auto position : string_entry->second) {
				if (position < arguments.size()) {
					auto &argument = arguments[position].GetExpressionMutable();
					argument = SparkCall("as_string", std::move(argument));
				}
			}
		}
		// DOUBLE parameters of the math functions: a string is converted (NULL when malformed), decimals widen
		static const unordered_set<string> DOUBLE_FUNCTIONS = {
		    "acos", "acosh", "asin", "asinh", "atan", "atanh", "cbrt", "cos", "cosh", "cot", "csc", "degrees", "exp", "expm1",
		    "ln", "log10", "log1p", "log2", "radians", "rint", "sec", "sin", "sinh", "sqrt", "tan", "tanh",
		    "atan2", "hypot", "pow", "power"};
		if (DOUBLE_FUNCTIONS.count(name)) {
			for (auto &argument_entry : func.GetArgumentsMutable()) {
				auto &argument = argument_entry.GetExpressionMutable();
				argument = SparkCall("as_double_param", std::move(argument));
			}
		}
		auto entry = INT_PARAMETERS.find(name);
		if (entry != INT_PARAMETERS.end()) {
			auto &arguments = func.GetArgumentsMutable();
			for (auto position : entry->second) {
				if (position >= arguments.size()) {
					continue;
				}
				auto &argument = arguments[position].GetExpressionMutable();
				if (argument->GetExpressionClass() == ExpressionClass::CONSTANT) {
					// small literals are INT already; only a literal with many digits can be out of range
					auto text = argument->ToString();
					if (text.size() < 10 && text.find_first_not_of("-+0123456789") == string::npos) {
						continue;
					}
				}
				if (name == "format_number") {
					argument = SparkCall("narrow_int", std::move(argument), ConstantExpression::Boolean(true));
				} else {
					argument = SparkCall("narrow_int", std::move(argument));
				}
			}
		}
	}
	if (name == "named_struct") {
		auto args = TakeArguments(func);
		if (args.size() % 2 != 0) {
			throw BinderException("named_struct expects an even number of arguments");
		}
		vector<pair<string, ExprPtr>> fields;
		for (idx_t i = 0; i < args.size(); i += 2) {
			if (args[i]->GetExpressionClass() != ExpressionClass::CONSTANT) {
				throw BinderException("named_struct: field names must be string literals");
			}
			auto text = args[i]->ToString();
			fields.emplace_back(text.size() >= 2 ? text.substr(1, text.size() - 2) : text, std::move(args[i + 1]));
		}
		return StructPack(std::move(fields));
	}
	if (name == "struct") {
		auto args = TakeArguments(func);
		vector<pair<string, ExprPtr>> fields;
		for (idx_t i = 0; i < args.size(); i++) {
			auto field_name = DefaultFieldName(*args[i], i);
			fields.emplace_back(field_name, std::move(args[i]));
		}
		return StructPack(std::move(fields));
	}
	if (name == "stack") {
		// stack(n, v1, ..., vk): n rows of ceil(k / n) columns col0, col1, ...
		auto args = TakeArguments(func);
		if (args.empty() || args[0]->GetExpressionClass() != ExpressionClass::CONSTANT) {
			throw BinderException("stack: the number of rows must be a constant");
		}
		auto rows = std::stoll(args[0]->ToString());
		if (rows <= 0) {
			throw BinderException("stack: the number of rows must be positive");
		}
		idx_t values = args.size() - 1;
		idx_t columns = (values + NumericCast<idx_t>(rows) - 1) / NumericCast<idx_t>(rows);
		vector<ExprPtr> structs;
		for (idx_t r = 0; r < idx_t(rows); r++) {
			vector<pair<string, ExprPtr>> fields;
			for (idx_t c = 0; c < columns; c++) {
				idx_t index = 1 + r * columns + c;
				ExprPtr value = index < args.size() ? args[index]->Copy() : ConstantExpression::Null();
				fields.emplace_back("col" + to_string(c), std::move(value));
			}
			structs.push_back(StructPack(std::move(fields)));
		}
		return GeneratorUnnest(DuckCall("list_value", std::move(structs)));
	}
	if (name == "json_tuple") {
		auto args = TakeArguments(func);
		if (args.size() < 2) {
			throw BinderException("json_tuple requires at least two arguments");
		}
		vector<pair<string, ExprPtr>> fields;
		for (idx_t i = 1; i < args.size(); i++) {
			fields.emplace_back("c" + to_string(i - 1),
			                    SparkCall("json_tuple_field", args[0]->Copy(), std::move(args[i])));
		}
		vector<ExprPtr> rows;
		rows.push_back(StructPack(std::move(fields)));
		return GeneratorUnnest(DuckCall("list_value", std::move(rows)));
	}
	if (name == "inline" || name == "inline_outer") {
		auto args = TakeArguments(func);
		if (args.size() != 1) {
			throw BinderException("%s expects one argument", name);
		}
		return GeneratorUnnest(SparkCall(name == "inline" ? "inline_rows" : "inline_outer_rows", std::move(args)));
	}
	if (name == "explode" || name == "explode_outer" || name == "posexplode" || name == "posexplode_outer") {
		auto args = TakeArguments(func);
		if (args.size() != 1) {
			throw BinderException("%s expects one argument", name);
		}
		return GeneratorUnnest(SparkCall(name + "_rows", std::move(args)));
	}
	// ---- aliases and default arguments ----
	if (name == "substring") {
		name = "substr";
		func.SetFunctionName(name);
	}
	if (name == "str_to_map") {
		auto &args = func.GetArgumentsMutable();
		if (args.size() == 1) {
			args.emplace_back(ConstantExpression::String(","));
		}
		if (args.size() == 2) {
			args.emplace_back(ConstantExpression::String(":"));
		}
	}
	if (name == "reflect" || name == "java_method" || name == "try_reflect") {
		auto args = TakeArguments(func);
		if (args.size() < 2 || args[0]->GetExpressionClass() != ExpressionClass::CONSTANT ||
		    args[1]->GetExpressionClass() != ExpressionClass::CONSTANT) {
			throw BinderException("%s: class and method must be string literals", name);
		}
		auto unquote = [](const string &s) {
			return s.size() >= 2 ? s.substr(1, s.size() - 2) : s;
		};
		auto cls = unquote(args[0]->ToString());
		auto method = unquote(args[1]->ToString());
		vector<ExprPtr> rest;
		for (idx_t i = 2; i < args.size(); i++) {
			rest.push_back(std::move(args[i]));
		}
		ExprPtr result;
		if (cls == "java.util.UUID" && method == "randomUUID" && rest.empty()) {
			result = DuckCall("uuid", vector<ExprPtr>());
		} else if (cls == "java.util.UUID" && method == "fromString" && rest.size() == 1) {
			result = make_uniq<CastExpression>(LogicalType::UUID, std::move(rest[0]));
		} else if (cls == "java.lang.Math" || cls == "java.lang.StrictMath") {
			static const unordered_map<string, string> math = {
			    {"abs", "abs"},   {"sqrt", "sqrt"}, {"cbrt", "cbrt"}, {"exp", "exp"},     {"log", "ln"},
			    {"log10", "log10"}, {"sin", "sin"}, {"cos", "cos"},   {"tan", "tan"},     {"asin", "asin"},
			    {"acos", "acos"}, {"atan", "atan"}, {"atan2", "atan2"}, {"pow", "pow"},   {"max", "greatest"},
			    {"min", "least"}, {"floor", "floor"}, {"ceil", "ceil"}, {"signum", "sign"}, {"toRadians", "radians"},
			    {"toDegrees", "degrees"}};
			auto entry = math.find(method);
			if (entry == math.end()) {
				throw BinderException("%s: unsupported method %s.%s", name, cls, method);
			}
			result = DuckCall(entry->second, std::move(rest));
		} else if (cls == "java.lang.String" && method == "valueOf" && rest.size() == 1) {
			result = SparkCall("as_string", std::move(rest[0]));
		} else if (name == "try_reflect") {
			return ConstantExpression::Null();
		} else {
			throw BinderException("%s: unsupported method %s.%s", name, cls, method);
		}
		return SparkCall("as_string", std::move(result));
	}
	if (name == "count" && func.GetArguments().size() > 1 && !func.ExportState()) {
		// count(a, b, ...) counts the rows where all of the expressions are not NULL; with DISTINCT, distinct tuples
		auto args = TakeArguments(func);
		ExprPtr any_null;
		vector<ExprPtr> row_fields;
		for (auto &arg : args) {
			ExprPtr is_null = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NULL, arg->Copy());
			any_null = any_null ? make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(any_null),
			                                                       std::move(is_null))
			                    : std::move(is_null);
			row_fields.push_back(std::move(arg));
		}
		auto row = make_uniq<FunctionExpression>(Identifier("row"), std::move(row_fields));
		auto guarded = make_uniq<CaseExpression>();
		CaseCheck check;
		check.when_expr = std::move(any_null);
		check.then_expr = ConstantExpression::Null();
		guarded->CaseChecksMutable().push_back(std::move(check));
		guarded->ElseMutable() = std::move(row);
		func.GetArgumentsMutable().clear();
		func.GetArgumentsMutable().emplace_back(ExprPtr(std::move(guarded)));
		return expr;
	}
	if ((name == "sum" || name == "avg" || name == "mean") && func.GetArguments().size() == 1 && !func.ExportState()) {
		// Spark's result types (sum(decimal(p, s)) is decimal(p + 10, s), integer sums are BIGINT, ...) come from
		// the dialect's own aggregates; DISTINCT, FILTER and ORDER BY stay on the call
		func.SetQualifiedName(Identifier(SYSTEM_CATALOG), Identifier(SPARK_SCHEMA),
		                      Identifier(name == "sum" ? "spark_sum" : "spark_avg"));
		return expr;
	}
	if (name == "coalesce" && func.GetArguments().size() >= 2) {
		// the operands are brought to Spark's common type first (a string with a number is a string)
		auto args = TakeArguments(func);
		return SparkCall("coalesce_value", std::move(args));
	}
	if (name == "if" || name == "iff") {
		auto args = TakeArguments(func);
		if (args.size() != 3) {
			throw BinderException("if expects three arguments");
		}
		return SparkCall("case_when", std::move(args));
	}
	if (name == "format_string" || name == "printf") {
		auto args = TakeArguments(func);
		return SparkCall("format_string_impl", std::move(args));
	}
	if (name == "elt") {
		auto args = TakeArguments(func);
		if (args.size() < 2) {
			throw BinderException("elt requires at least two arguments");
		}
		vector<ExprPtr> values;
		for (idx_t i = 1; i < args.size(); i++) {
			values.push_back(std::move(args[i]));
		}
		return SparkCall("elt", std::move(args[0]), SparkCall("elt_list", std::move(values)));
	}
	if (name == "decode" && func.GetArguments().size() > 2) {
		// decode(expr, search1, result1, ..., default): null-safe equality
		auto args = TakeArguments(func);
		auto result = make_uniq<CaseExpression>();
		idx_t i = 1;
		for (; i + 1 < args.size(); i += 2) {
			CaseCheck check;
			check.when_expr = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM,
			                                                  args[0]->Copy(), std::move(args[i]));
			check.then_expr = std::move(args[i + 1]);
			result->CaseChecksMutable().push_back(std::move(check));
		}
		result->ElseMutable() = i < args.size() ? std::move(args[i]) : ConstantExpression::Null();
		return std::move(result);
	}
	if (name == "concat_ws") {
		auto args = TakeArguments(func);
		if (args.empty()) {
			throw BinderException("concat_ws requires at least one argument");
		}
		vector<ExprPtr> parts;
		parts.push_back(args[0]->Copy());
		for (idx_t i = 1; i < args.size(); i++) {
			parts.push_back(SparkCall("concat_ws_arg", std::move(args[i]), args[0]->Copy()));
		}
		if (parts.size() == 1) {
			parts.push_back(ConstantExpression::Null());
		}
		return SparkCall("concat_ws_impl", std::move(parts));
	}
	// ---- higher-order functions ----
	if (name == "transform" || name == "filter") {
		auto args = TakeArguments(func);
		if (args.size() != 2) {
			throw BinderException("%s expects two arguments", name);
		}
		vector<string> params;
		ExprPtr body;
		if (!LambdaParts(*args[1], params, body)) {
			throw BinderException("%s expects a lambda function argument", name);
		}
		// DuckDB's lambda index parameter is 0-based like Spark's
		auto lambda = MakeLambda(params, std::move(body));
		return DuckCall(name == "transform" ? "list_transform" : "list_filter", std::move(args[0]), std::move(lambda));
	}
	if (name == "array_sort" && func.GetArguments().size() == 2) {
		auto args = TakeArguments(func);
		// a comparator of a recognized shape sorts by key with list_sort (O(n log n)); others rank each element by
		// how many elements the comparator puts before it (O(n^2))
		bool descending = false;
		ExprPtr key;
		string key_param;
		bool recognized = MatchSortComparator(*args[1], descending, key, key_param);
		vector<ExprPtr> replacements;
		replacements.push_back(ColRef("__spark_other"));
		replacements.push_back(ColRef("__spark_elem"));
		auto result = InlineLambda(args, 1, "array_sort", std::move(replacements));
		// a comparator that returns NULL is an error in Spark (when there is anything to compare)
		auto comparison = SparkCall("comparator_result", std::move(result));
		auto is_below_case = make_uniq<CaseExpression>();
		CaseCheck null_check;
		null_check.when_expr = make_uniq<ConjunctionExpression>(
		    ExpressionType::CONJUNCTION_AND,
		    SparkCall("cmp_gt", DuckCall("len", args[0]->Copy()), ConstantExpression::Integer(1)),
		    make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NULL, comparison->Copy()));
		null_check.then_expr = make_uniq<CastExpression>(
		    LogicalType::BOOLEAN,
		    DuckCall("error", ConstantExpression::String("[COMPARATOR_RETURNS_NULL] The comparator has returned a NULL "
		                                                  "for a comparison.")));
		is_below_case->CaseChecksMutable().push_back(std::move(null_check));
		is_below_case->ElseMutable() = Operator("<", std::move(comparison), ConstantExpression::Integer(0));
		ExprPtr is_below = std::move(is_below_case);
		auto rank = DuckCall("len", DuckCall("list_filter", args[0]->Copy(),
		                                     MakeLambda({"__spark_other"}, std::move(is_below))));
		vector<pair<string, ExprPtr>> fields;
		fields.emplace_back("r", std::move(rank));
		fields.emplace_back("i", ColRef("__spark_idx"));
		fields.emplace_back("v", ColRef("__spark_elem"));
		auto keyed = DuckCall("list_transform", args[0]->Copy(),
		                      MakeLambda({"__spark_elem", "__spark_idx"}, StructPack(std::move(fields))));
		auto sorted = DuckCall("list_sort", std::move(keyed));
		ExprPtr slow = DuckCall("list_transform", std::move(sorted), MakeLambda({"__spark_e"}, StructField("__spark_e", "v")));
		if (!recognized) {
			return slow;
		}
		// the fast path: sort (key, +-index, element); the index keeps equal keys in input order like TimSort
		auto key_of = [&]() {
			auto copy = key->Copy();
			Substitute(copy, key_param, *ColRef("__spark_elem"));
			return copy;
		};
		vector<pair<string, ExprPtr>> sort_fields;
		sort_fields.emplace_back("k", key_of());
		sort_fields.emplace_back("i", descending ? Operator("-", ConstantExpression::Integer(0), ColRef("__spark_idx"))
		                                         : ColRef("__spark_idx"));
		sort_fields.emplace_back("v", ColRef("__spark_elem"));
		auto sort_keyed = DuckCall("list_transform", args[0]->Copy(),
		                           MakeLambda({"__spark_elem", "__spark_idx"}, StructPack(std::move(sort_fields))));
		ExprPtr fast_sorted = descending ? DuckCall("list_sort", std::move(sort_keyed), ConstantExpression::String("DESC"))
		                                 : DuckCall("list_sort", std::move(sort_keyed));
		auto fast = DuckCall("list_transform", std::move(fast_sorted),
		                     MakeLambda({"__spark_e"}, StructField("__spark_e", "v")));
		auto keys = DuckCall("list_transform", args[0]->Copy(), MakeLambda({"__spark_elem"}, key_of()));
		// only keys that DuckDB orders like Spark's comparison does (no nested NULLs), and no NULL keys
		auto keys_simple = SparkCall("simple_sort_key", keys->Copy());
		auto keys_counted = DuckCall("list_count", keys->Copy());
		auto keys_length = DuckCall("len", std::move(keys));
		ExprPtr usable = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(keys_simple),
		                                                  Operator("=", std::move(keys_counted), std::move(keys_length)));
		vector<ExprPtr> choice;
		if (key->GetExpressionClass() == ExpressionClass::COLUMN_REF && key->ToString() == key_param) {
			// the elements themselves are the keys: equal elements are indistinguishable (except the zeros of floating
			// point numbers, which take the general path), so a plain sort does it
			ExprPtr direct = descending ? DuckCall("list_sort", args[0]->Copy(), ConstantExpression::String("DESC"))
			                            : DuckCall("list_sort", args[0]->Copy());
			auto exact = SparkCall("simple_sort_key", args[0]->Copy(), ConstantExpression::Boolean(true));
			auto counted = DuckCall("list_count", args[0]->Copy());
			auto length = DuckCall("len", args[0]->Copy());
			choice.push_back(make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(exact),
			                                                  Operator("=", std::move(counted), std::move(length))));
			choice.push_back(std::move(direct));
		}
		choice.push_back(std::move(usable));
		choice.push_back(std::move(fast));
		choice.push_back(std::move(slow));
		return SparkCall("case_when", std::move(choice));
	}
	if (name == "zip_with") {
		auto args = TakeArguments(func);
		vector<ExprPtr> replacements;
		replacements.push_back(DuckCall("struct_extract_at", ColRef("__spark_pair"), ConstantExpression::Integer(1)));
		replacements.push_back(DuckCall("struct_extract_at", ColRef("__spark_pair"), ConstantExpression::Integer(2)));
		auto body = InlineLambda(args, 2, "zip_with", std::move(replacements));
		vector<ExprPtr> inputs;
		inputs.push_back(args[0]->Copy());
		inputs.push_back(args[1]->Copy());
		return NullIfAnyNull(inputs, DuckCall("list_transform", DuckCall("list_zip", std::move(args[0]), std::move(args[1])),
		                                      MakeLambda({"__spark_pair"}, std::move(body))));
	}
	if (name == "map_filter") {
		auto args = TakeArguments(func);
		vector<ExprPtr> replacements;
		replacements.push_back(StructField("__spark_entry", "key"));
		replacements.push_back(StructField("__spark_entry", "value"));
		auto body = InlineLambda(args, 1, "map_filter", std::move(replacements));
		return SparkCall("map_from_entries_value",
		                DuckCall("list_filter", DuckCall("map_entries", std::move(args[0])),
		                         MakeLambda({"__spark_entry"}, std::move(body))));
	}
	if (name == "transform_keys" || name == "transform_values") {
		auto args = TakeArguments(func);
		vector<ExprPtr> replacements;
		replacements.push_back(StructField("__spark_entry", "key"));
		replacements.push_back(StructField("__spark_entry", "value"));
		auto body = InlineLambda(args, 1, name.c_str(), std::move(replacements));
		vector<pair<string, ExprPtr>> fields;
		if (name == "transform_keys") {
			fields.emplace_back("key", std::move(body));
			fields.emplace_back("value", StructField("__spark_entry", "value"));
		} else {
			fields.emplace_back("key", StructField("__spark_entry", "key"));
			fields.emplace_back("value", std::move(body));
		}
		return SparkCall("map_from_entries_value",
		                DuckCall("list_transform", DuckCall("map_entries", std::move(args[0])),
		                         MakeLambda({"__spark_entry"}, StructPack(std::move(fields)))));
	}
	if (name == "map_zip_with") {
		auto args = TakeArguments(func);
		vector<ExprPtr> replacements;
		replacements.push_back(ColRef("__spark_key"));
		replacements.push_back(DuckCall("map_extract_value", args[0]->Copy(), ColRef("__spark_key")));
		replacements.push_back(DuckCall("map_extract_value", args[1]->Copy(), ColRef("__spark_key")));
		auto body = InlineLambda(args, 2, "map_zip_with", std::move(replacements));
		vector<pair<string, ExprPtr>> fields;
		fields.emplace_back("key", ColRef("__spark_key"));
		fields.emplace_back("value", std::move(body));
		vector<ExprPtr> inputs;
		inputs.push_back(args[0]->Copy());
		inputs.push_back(args[1]->Copy());
		auto keys = SparkCall("array_distinct", DuckCall("list_concat", DuckCall("map_keys", std::move(args[0])),
		                                                    DuckCall("map_keys", std::move(args[1]))));
		return NullIfAnyNull(inputs, SparkCall("map_from_entries_value", DuckCall("list_transform", std::move(keys),
		                                                                   MakeLambda({"__spark_key"}, StructPack(std::move(fields))))));
	}
	if (name == "arrays_zip") {
		auto args = TakeArguments(func);
		vector<ExprPtr> lengths;
		vector<pair<string, ExprPtr>> fields;
		for (idx_t i = 0; i < args.size(); i++) {
			string field_name = args[i]->GetExpressionClass() == ExpressionClass::COLUMN_REF
			                        ? DefaultFieldName(*args[i], i)
			                        : to_string(i);
			lengths.push_back(DuckCall("len", args[i]->Copy()));
			fields.emplace_back(field_name, DuckCall("list_extract", args[i]->Copy(),
			                                         Operator("+", ColRef("__spark_idx"), ConstantExpression::Integer(1))));
		}
		if (args.empty()) {
			return SparkCall("array", vector<ExprPtr>());
		}
		auto longest = lengths.size() == 1 ? std::move(lengths[0]) : DuckCall("greatest", std::move(lengths));
		return NullIfAnyNull(args, DuckCall("list_transform", DuckCall("range", std::move(longest)),
		                                    MakeLambda({"__spark_idx"}, StructPack(std::move(fields)))));
	}
	// helpers such as add / cast_value / concat_ws_impl are only reachable through qualified internal calls
	if (IsSparkOverride(name) && IsSparkFunction(name)) {
		func.SetQualifiedName(Identifier(SYSTEM_CATALOG), Identifier(SPARK_SCHEMA), Identifier(name));
	}
	return expr;
}

ExprPtr FinalizeFunctionHookResult(ExprPtr expr) {
	if (expr->GetExpressionClass() == ExpressionClass::OPERATOR && expr->GetExpressionType() == ExpressionType::OPERATOR_COALESCE &&
	    expr->Cast<OperatorExpression>().GetChildren().size() >= 2) {
		// COALESCE(a, b, ...): the operands are brought to Spark's common type first (a string with a number is a string)
		// (no alias here: an aliased argument reads as a named argument to a macro, see CoalesceColumnName)
		vector<ExprPtr> operands;
		for (auto &child : expr->Cast<OperatorExpression>().GetChildrenMutable()) {
			operands.push_back(std::move(child));
		}
		return SparkCall("coalesce_value", std::move(operands));
	}
	if (expr->GetExpressionClass() == ExpressionClass::WINDOW) {
		auto &window = expr->Cast<WindowExpression>();
		auto name = StringUtil::Lower(window.FunctionName().GetIdentifierName());
		auto &window_args = window.GetArgumentsMutable();
		if (name == "rank" || name == "dense_rank" || name == "percent_rank" || name == "cume_dist" ||
		    name == "row_number") {
			// Spark accepts (and ignores) arguments here
			window_args.clear();
		}
		if (name == "rank" || name == "dense_rank" || name == "row_number" || name == "ntile") {
			// Spark's ranking functions return INT (DuckDB: BIGINT)
			return make_uniq<CastExpression>(LogicalType::INTEGER, std::move(expr));
		}
		if ((name == "first_value" || name == "last_value" || name == "first" || name == "last" ||
		     name == "nth_value") &&
		    window_args.size() >= 2) {
			auto &flag = window_args.back().GetExpression();
			auto text = StringUtil::Lower(flag.ToString());
			if (flag.GetExpressionClass() == ExpressionClass::CONSTANT && (text == "true" || text == "false")) {
				window.IgnoreNullsMutable() = text == "true";
				window.HasIgnoreNullsMutable() = true;
				window_args.pop_back();
			}
		}
		if (name == "first" || name == "last") {
			window.SetFunctionName(name + "_value");
		}
		if (name == "count" && window_args.size() > 1) {
			// count(a, b) OVER (...): rows where all of the expressions are not NULL
			ExprPtr any_null;
			vector<ExprPtr> row_fields;
			for (auto &arg : window_args) {
				ExprPtr is_null =
				    make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NULL, arg.GetExpression().Copy());
				any_null = any_null ? make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR,
				                                                       std::move(any_null), std::move(is_null))
				                    : std::move(is_null);
				row_fields.push_back(arg.GetExpression().Copy());
			}
			auto guarded = make_uniq<CaseExpression>();
			CaseCheck check;
			check.when_expr = std::move(any_null);
			check.then_expr = ConstantExpression::Null();
			guarded->CaseChecksMutable().push_back(std::move(check));
			guarded->ElseMutable() = make_uniq<FunctionExpression>(Identifier("row"), std::move(row_fields));
			window_args.clear();
			window_args.emplace_back(ExprPtr(std::move(guarded)));
		}
		if ((name == "sum" || name == "avg" || name == "mean") && window_args.size() == 1) {
			// like the plain aggregates: Spark's result types, and BIGINT sums wrap on overflow
			window.SetQualifiedName(Identifier(SYSTEM_CATALOG), Identifier(SPARK_SCHEMA),
			                        Identifier(name == "sum" ? "spark_sum" : "spark_avg"));
			return expr;
		}
		if (window.GetQualifiedName().Schema().empty() && IsSparkOverride("aggregate_" + name)) {
			// an aggregate of the dialect (var_pop, regr_*, ...) used as a window function
			window.SetQualifiedName(Identifier(SYSTEM_CATALOG), Identifier(SPARK_SCHEMA), Identifier(name));
			return expr;
		}
		if (window.GetQualifiedName().Schema().empty() && IsSparkOverride("window_" + name) && IsSparkFunction(name)) {
			window.SetQualifiedName(Identifier(SYSTEM_CATALOG), Identifier(SPARK_SCHEMA),
			                        Identifier("window_" + name));
		}
		return expr;
	}
	return RewriteFunction(std::move(expr));
}

//! Names the output columns of a Spark generator (explode / posexplode, ...) after the aliases of the select item
//! (comma separated). DuckDB's unnest of a struct names its columns by the struct fields and ignores the alias.
bool RenameGeneratorColumns(ParsedExpression &expr, const string &aliases) {
	if (expr.GetExpressionClass() != ExpressionClass::FUNCTION) {
		return false;
	}
	auto &unnest = expr.Cast<FunctionExpression>();
	if (StringUtil::Lower(unnest.FunctionName().GetIdentifierName()) != "unnest" || unnest.GetArguments().empty() ||
	    unnest.GetArguments()[0].GetExpression().GetExpressionClass() != ExpressionClass::FUNCTION) {
		return false;
	}
	auto &generator = unnest.GetArgumentsMutable()[0].GetExpressionMutable()->Cast<FunctionExpression>();
	auto generator_name = StringUtil::Lower(generator.FunctionName().GetIdentifierName());
	if (generator_name != "explode_rows" && generator_name != "explode_outer_rows" &&
	    generator_name != "posexplode_rows" && generator_name != "posexplode_outer_rows") {
		return false;
	}
	auto args = TakeArguments(generator);
	args.push_back(ConstantExpression::String(aliases));
	auto renamed = SparkCall(generator_name.substr(0, generator_name.size() - 5) + "_as_rows", std::move(args));
	unnest.GetArgumentsMutable()[0].GetExpressionMutable() = std::move(renamed);
	return true;
}

//! A select-list item that is COALESCE(a, b) (rewritten to system.spark.coalesce_value) gets the column name Spark
//! gives it. Set on the select-list item only: an alias on a function argument would read as a named argument.
void SetCoalesceColumnName(ParsedExpression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::FUNCTION) {
		return;
	}
	auto &func = expr.Cast<FunctionExpression>();
	if (func.GetQualifiedName().Schema().empty() ||
	    !StringUtil::CIEquals(func.FunctionName().GetIdentifierName(), "coalesce_value")) {
		return;
	}
	string name = "coalesce(";
	idx_t i = 0;
	for (auto &argument : func.GetArguments()) {
		name += (i++ ? ", " : "") + argument.GetExpression().ToString();
	}
	expr.SetAlias(Identifier(name + ")"));
}

//===----------------------------------------------------------------------===//
// Window functions inside window definitions (rank() OVER (ORDER BY sum(x) OVER (...))): Spark evaluates the inner
// window first. DuckDB cannot nest window calls, so a query block with one is split: the inner windows are computed
// in a derived table, and the outer query reads them as columns.
//===----------------------------------------------------------------------===//
//! Replace window calls found inside another window call by column references, collecting them (with their alias)
void LiftInnerWindows(unique_ptr<ParsedExpression> &expr, bool inside_window, vector<unique_ptr<ParsedExpression>> &lifted,
                      idx_t &counter) {
	if (!expr) {
		return;
	}
	auto cls = expr->GetExpressionClass();
	if (cls == ExpressionClass::SUBQUERY) {
		return; // its own query block
	}
	if (cls == ExpressionClass::WINDOW) {
		if (inside_window) {
			auto name = "__spark_w" + to_string(counter++);
			expr->SetAlias(Identifier(name));
			lifted.push_back(std::move(expr));
			expr = make_uniq<ColumnRefExpression>(Identifier(name));
			return;
		}
		ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child) {
			LiftInnerWindows(child, true, lifted, counter);
		});
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child) {
		LiftInnerWindows(child, inside_window, lifted, counter);
	});
}

//! A source without a single alias (a join): the outer query's qualified references (t.c, t.*) cannot name the
//! derived table, so each one is computed in the derived table as a helper column and the outer query reads that
struct QualifiedReferenceLift {
	SelectNode &inner;
	vector<Identifier> &names;
	idx_t &counter;
	unordered_map<string, Identifier> helpers;
	vector<identifier_set_t> lambda_params;

	static string ColumnKey(const ColumnRefExpression &column) {
		string key;
		for (auto &part : column.ColumnNames()) {
			key += StringUtil::Lower(part.GetIdentifierName()) + ".";
		}
		return key;
	}

	static string StarKey(const Identifier &relation) {
		return "*" + StringUtil::Lower(relation.GetIdentifierName());
	}

	//! Reuse the helper columns of a derived table made earlier (ORDER BY is transformed after the query block)
	void Seed() {
		for (auto &item : inner.select_list) {
			if (!StringUtil::StartsWith(item->GetAlias().GetIdentifierName(), "__spark_w")) {
				continue;
			}
			if (item->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
				helpers.emplace(ColumnKey(item->Cast<ColumnRefExpression>()), item->GetAlias());
			} else if (item->GetExpressionClass() == ExpressionClass::FUNCTION) {
				// struct_pack(*COLUMNS(t.*))
				auto &arguments = item->Cast<FunctionExpression>().GetArguments();
				if (arguments.size() == 1 && StarExpression::IsColumnsUnpacked(arguments[0].GetExpression())) {
					auto &unpack = arguments[0].GetExpression().Cast<OperatorExpression>();
					auto &star = unpack.GetChildren()[0]->Cast<StarExpression>();
					helpers.emplace(StarKey(star.RelationName()), item->GetAlias());
				}
			}
		}
	}

	Identifier Helper(const string &key, unique_ptr<ParsedExpression> source) {
		auto entry = helpers.find(key);
		if (entry != helpers.end()) {
			return entry->second;
		}
		Identifier name("__spark_w" + to_string(counter++));
		source->SetAlias(name);
		inner.select_list.push_back(std::move(source));
		names.push_back(name);
		helpers.emplace(key, name);
		return name;
	}

	void Rewrite(unique_ptr<ParsedExpression> &expr) {
		if (!expr) {
			return;
		}
		switch (expr->GetExpressionClass()) {
		case ExpressionClass::SUBQUERY:
			return; // its own query block
		case ExpressionClass::COLUMN_REF: {
			auto &column = expr->Cast<ColumnRefExpression>();
			if (!column.IsQualified() || LambdaExpression::IsLambdaParameter(lambda_params, column.ColumnNames()[0])) {
				return;
			}
			auto key = ColumnKey(column);
			auto alias = expr->GetAlias();
			auto source = expr->Copy();
			source->ClearAlias();
			expr = make_uniq<ColumnRefExpression>(Helper(key, std::move(source)));
			expr->SetAlias(alias);
			return;
		}
		case ExpressionClass::STAR: {
			auto &star = expr->Cast<StarExpression>();
			if (star.RelationName().empty()) {
				return;
			}
			if (!star.ExcludeList().empty() || !star.ReplaceList().empty() || !star.RenameList().empty() ||
			    star.Expression() || star.IsColumns()) {
				throw ParserException("[UNSUPPORTED_FEATURE] spark_dialect does not support a window function inside a "
				                      "window definition over a join with a modified qualified star (t.* EXCLUDE ...); "
				                      "compute the inner window in a subquery");
			}
			// t.* reads the row of t as a struct (struct_pack(*COLUMNS(t.*)): a bare t could be a column of the same
			// name), which the outer query expands again
			auto relation = star.RelationName();
			auto row_star = make_uniq<StarExpression>(relation);
			row_star->IsColumnsMutable() = true;
			vector<ExprPtr> unpacked;
			unpacked.push_back(std::move(row_star));
			vector<ExprPtr> fields;
			fields.push_back(make_uniq<OperatorExpression>(ExpressionType::OPERATOR_UNPACK, std::move(unpacked)));
			auto name = Helper(StarKey(relation), make_uniq<FunctionExpression>(Identifier("struct_pack"), std::move(fields)));
			expr = make_uniq<StarExpression>(name);
			return;
		}
		case ExpressionClass::LAMBDA: {
			auto &lambda = expr->Cast<LambdaExpression>();
			string error;
			auto params = lambda.ExtractColumnRefExpressions(error);
			if (!error.empty()) {
				break; // the JSON -> operator
			}
			identifier_set_t names_set;
			for (auto &param : params) {
				names_set.insert(param.get().Cast<ColumnRefExpression>().GetColumnName());
			}
			lambda_params.push_back(std::move(names_set));
			Rewrite(lambda.RightMutable());
			lambda_params.pop_back();
			return;
		}
		default:
			break;
		}
		ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child) { Rewrite(child); });
	}
};

//! Each split takes its own range of helper column names (__spark_w<n>)
idx_t NextHelperRange() {
	static std::atomic<idx_t> counter {0};
	return counter.fetch_add(1000);
}

//! SELECT * of the outer query does not show the helper columns (t.* of a helper struct shows its fields only)
void ExcludeHelperColumns(SelectNode &node, const vector<Identifier> &names) {
	for (auto &item : node.select_list) {
		if (item->GetExpressionClass() != ExpressionClass::STAR) {
			continue;
		}
		auto &star = item->Cast<StarExpression>();
		if (StringUtil::StartsWith(star.RelationName().GetIdentifierName(), "__spark_w")) {
			continue;
		}
		for (auto &name : names) {
			star.ExcludeListMutable().insert(QualifiedColumnName(name));
		}
	}
}

void LiftNestedWindows(SelectNode &node) {
	idx_t local = NextHelperRange();
	vector<unique_ptr<ParsedExpression>> lifted;
	for (auto &item : node.select_list) {
		LiftInnerWindows(item, false, lifted, local);
	}
	LiftInnerWindows(node.qualify, false, lifted, local);
	if (lifted.empty()) {
		return;
	}
	if (!node.groups.group_expressions.empty() || !node.groups.grouping_sets.empty() || node.having) {
		throw ParserException("[UNSUPPORTED_FEATURE] spark_dialect does not support a window function inside a window "
		                      "definition in a query with GROUP BY or HAVING; compute the inner window in a subquery");
	}
	// the outer query reads the source's columns through the derived table: it keeps the source's alias
	Identifier alias;
	if (node.from_table) {
		alias = node.from_table->alias;
		if (alias.empty() && node.from_table->type == TableReferenceType::BASE_TABLE) {
			alias = node.from_table->Cast<BaseTableRef>().GetQualifiedName().Name();
		}
	}
	auto inner = make_uniq<SelectNode>();
	inner->select_list.push_back(make_uniq<StarExpression>());
	vector<Identifier> names;
	for (auto &window : lifted) {
		names.push_back(window->GetAlias());
		inner->select_list.push_back(std::move(window));
	}
	if (alias.empty() && node.from_table && node.from_table->type != TableReferenceType::EMPTY_FROM) {
		// a join: qualified references go through helper columns (ORDER BY: LiftOrderByReferences)
		QualifiedReferenceLift lift {*inner, names, local, {}, {}};
		for (auto &item : node.select_list) {
			auto before = item->ToString();
			auto name = item->GetName();
			lift.Rewrite(item);
			if (item->GetAlias().empty() && item->GetExpressionClass() != ExpressionClass::STAR &&
			    item->ToString() != before) {
				// the column keeps the name it had before the rewrite (t.c is named c)
				item->SetAlias(Identifier(name));
			}
		}
		lift.Rewrite(node.qualify);
		for (auto &modifier : node.modifiers) {
			if (modifier->type == ResultModifierType::DISTINCT_MODIFIER) {
				for (auto &target : modifier->Cast<DistinctModifier>().distinct_on_targets) {
					lift.Rewrite(target);
				}
			}
		}
	}
	inner->from_table = std::move(node.from_table);
	inner->where_clause = std::move(node.where_clause);
	LiftNestedWindows(*inner); // windows nested deeper
	auto statement = make_uniq<SelectStatement>();
	statement->node = std::move(inner);
	node.from_table = make_uniq<SubqueryRef>(std::move(statement), alias);
	ExcludeHelperColumns(node, names);
}

//! ORDER BY of a query block that LiftNestedWindows split over a join: its qualified references go through helper
//! columns too (ORDER BY is attached to the query block after the block itself is transformed)
void LiftOrderByReferences(SelectNode &node) {
	if (node.modifiers.empty() || !node.from_table || node.from_table->type != TableReferenceType::SUBQUERY ||
	    !node.from_table->alias.empty()) {
		return;
	}
	auto &subquery = node.from_table->Cast<SubqueryRef>().subquery;
	if (!subquery || !subquery->node || subquery->node->type != QueryNodeType::SELECT_NODE) {
		return;
	}
	auto &inner = subquery->node->Cast<SelectNode>();
	bool split = false;
	for (auto &item : inner.select_list) {
		split = split || StringUtil::StartsWith(item->GetAlias().GetIdentifierName(), "__spark_w");
	}
	if (!split) {
		return;
	}
	idx_t local = NextHelperRange();
	vector<Identifier> names;
	QualifiedReferenceLift lift {inner, names, local, {}, {}};
	lift.Seed();
	for (auto &modifier : node.modifiers) {
		if (modifier->type == ResultModifierType::ORDER_MODIFIER) {
			for (auto &order : modifier->Cast<OrderModifier>().orders) {
				lift.Rewrite(order.expression);
			}
		}
	}
	ExcludeHelperColumns(node, names);
}

arena_ptr<TransformResultValue> FinalizeSimpleSelect(PEGTransformer &t, ParseResult &pr) {
	auto statement = t.Transform<unique_ptr<SelectStatement>>(AsList(pr).GetChild(0));
	if (statement->node && statement->node->type == QueryNodeType::SELECT_NODE) {
		LiftNestedWindows(statement->node->Cast<SelectNode>());
	}
	return t.MakeResult<unique_ptr<SelectStatement>>(std::move(statement));
}

arena_ptr<TransformProcess> SimpleSelectProcess(PEGTransformer &t, ParseResult &pr) {
	return t.MakeProcess<FinalizeTransformProcess>(t, pr, FinalizeSimpleSelect);
}

arena_ptr<TransformResultValue> FinalizeSelectStatementInternal(PEGTransformer &t, ParseResult &pr) {
	auto statement = t.Transform<unique_ptr<SelectStatement>>(AsList(pr).GetChild(0));
	if (statement->node && statement->node->type == QueryNodeType::SELECT_NODE) {
		LiftOrderByReferences(statement->node->Cast<SelectNode>());
	}
	return t.MakeResult<unique_ptr<SelectStatement>>(std::move(statement));
}

arena_ptr<TransformProcess> SelectStatementInternalProcess(PEGTransformer &t, ParseResult &pr) {
	return t.MakeProcess<FinalizeTransformProcess>(t, pr, FinalizeSelectStatementInternal);
}

//! AliasedExpression: `generator AS name` and `generator AS (a, b)` name the generated columns
ExprPtr TransformAliasedHook(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	auto &alt = Unwrap(list.GetChild(0));
	if (StringUtil::CIEquals(alt.Cast<ListParseResult>().Name(), "SpAliasList")) {
		auto &parts = alt.Cast<ListParseResult>();
		auto expr = Expr(t, parts.GetChild(0));
		string aliases;
		for (auto &item : ListItems(parts.GetChild(3))) {
			aliases += (aliases.empty() ? "" : ",") + t.Transform<string>(item.get());
		}
		if (!RenameGeneratorColumns(*expr, aliases)) {
			throw ParserException("Syntax error: an alias list (AS (a, b)) is only allowed after a generator function "
			                      "(explode, posexplode, inline, ...)");
		}
		expr->SetAlias(Identifier(aliases));
		return expr;
	}
	auto expr = Expr(t, alt);
	if (!expr->GetAlias().empty()) {
		RenameGeneratorColumns(*expr, expr->GetAlias().GetIdentifierName());
	} else {
		SetCoalesceColumnName(*expr);
	}
	return expr;
}

//! GROUPING(...) is a BYTE in Spark (DuckDB: BIGINT); GROUPING_ID stays BIGINT
ExprPtr TransformGroupingHook(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	auto &inner = AsList(list.GetChild(0));
	bool is_grouping = StringUtil::CIEquals(Literal(inner.GetChild(0)), "GROUPING");
	auto expr = Expr(t, list.GetChild(0));
	if (is_grouping) {
		return make_uniq<CastExpression>(LogicalType::TINYINT, std::move(expr));
	}
	return expr;
}

//! Transform a function call; a window function inside another window's definition is allowed (Spark evaluates the
//! inner window first, see LiftNestedWindows), so DuckDB's "inside a window definition" state is cleared for it
ExprPtr TransformFunctionCall(PEGTransformer &t, ParseResult &pr) {
	bool in_window_definition = t.in_window_definition;
	t.in_window_definition = false;
	auto expr = Expr(t, pr);
	t.in_window_definition = in_window_definition;
	return expr;
}

ExprPtr TransformFunctionHook(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	return FinalizeFunctionHookResult(TransformFunctionCall(t, list.GetChild(0)));
}

//! Does a parsed expression reference no columns, subqueries or lambda parameters (Spark: "foldable")?
bool IsFoldable(const ParsedExpression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::COLUMN_REF:
	case ExpressionClass::SUBQUERY:
	case ExpressionClass::LAMBDA_REF:
	case ExpressionClass::WINDOW:
	case ExpressionClass::STAR:
		return false;
	default:
		break;
	}
	bool foldable = true;
	ParsedExpressionIterator::EnumerateChildren(expr, [&](const ParsedExpression &child) {
		foldable = foldable && IsFoldable(child);
	});
	return foldable;
}

//! spark_strict: arguments that Spark requires to be constant ([DATATYPE_MISMATCH.NON_FOLDABLE_INPUT] and
//! "needs to be a ... literal" errors at analysis time), by 0-based position
void CheckFoldableArguments(const ParsedExpression &expr) {
	static const unordered_map<string, vector<idx_t>> FOLDABLE = {
	    {"round", {1}},          {"bround", {1}},          {"ceil", {1}},           {"ceiling", {1}},
	    {"floor", {1}},          {"date_part", {0}},       {"datepart", {0}},       {"mask", {1, 2, 3, 4}},
	    {"schema_of_csv", {0}},  {"schema_of_json", {0}},  {"to_binary", {1}},      {"try_to_binary", {1}},
	    {"to_char", {1}},        {"to_varchar", {1}},      {"to_number", {1}},      {"try_to_number", {1}},
	    {"from_json", {1}},      {"from_csv", {1}},        {"xpath", {1}},          {"xpath_boolean", {1}},
	    {"xpath_double", {1}},   {"xpath_float", {1}},     {"xpath_int", {1}},      {"xpath_long", {1}},
	    {"xpath_number", {1}},   {"xpath_short", {1}},     {"xpath_string", {1}},   {"approx_percentile", {1, 2}},
	    {"percentile_approx", {1, 2}}, {"percentile", {1}}, {"histogram_numeric", {1}}, {"hll_sketch_agg", {1}},
	    {"sort_array", {1}}};
	if (expr.GetExpressionClass() != ExpressionClass::FUNCTION) {
		return;
	}
	auto &func = expr.Cast<FunctionExpression>();
	auto entry = FOLDABLE.find(StringUtil::Lower(func.FunctionName().GetIdentifierName()));
	if (entry == FOLDABLE.end()) {
		return;
	}
	auto &args = func.GetArguments();
	for (auto pos : entry->second) {
		if (pos < args.size() && !IsFoldable(args[pos].GetExpression())) {
			throw BinderException("[DATATYPE_MISMATCH.NON_FOLDABLE_INPUT] argument %llu of %s must be a constant",
			                      (unsigned long long)(pos + 1), entry->first);
		}
	}
}

//! spark_strict: an untyped NULL literal where Spark's analyzer wants a typed value (found by calling every Spark
//! function with NULL in each position): the check has to run on the parsed query, as DuckDB types the NULL later
void CheckNullLiteralArguments(const ParsedExpression &expr) {
	// {function, positions, error class}
	struct Rule {
		vector<idx_t> positions;
		const char *error;
	};
	static const unordered_map<string, Rule> RULES = {
	    {"array_contains", {{0, 1}, "NULL_TYPE"}}, {"array_position", {{0}, "NULL_TYPE"}},
	    {"map_contains_key", {{1}, "NULL_TYPE"}},  {"array_append", {{0}, "UNEXPECTED_INPUT_TYPE"}},
	    {"element_at", {{0}, "UNEXPECTED_INPUT_TYPE"}}};
	if (expr.GetExpressionClass() != ExpressionClass::FUNCTION) {
		return;
	}
	auto &func = expr.Cast<FunctionExpression>();
	auto name = StringUtil::Lower(func.FunctionName().GetIdentifierName());
	auto entry = RULES.find(name);
	if (entry == RULES.end()) {
		return;
	}
	auto &args = func.GetArguments();
	for (auto position : entry->second.positions) {
		if (position < args.size() && args[position].GetExpression().GetExpressionClass() == ExpressionClass::CONSTANT &&
		    StringUtil::CIEquals(args[position].GetExpression().ToString(), "NULL")) {
			if (string(entry->second.error) == "NULL_TYPE") {
				throw BinderException("[DATATYPE_MISMATCH.NULL_TYPE] Cannot resolve \"%s\" due to data type mismatch: "
				                      "Null typed values cannot be used as arguments of `%s`.",
				                      func.ToString(), name);
			}
			throw BinderException("[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"%s\" due to data type "
			                      "mismatch: Parameter %llu requires the \"ARRAY\" type, however NULL has the type "
			                      "\"VOID\".",
			                      func.ToString(), (unsigned long long)position);
		}
	}
}

//! spark_strict: arguments whose type Spark's analyzer may reject are wrapped in check_arg(value, 'function',
//! position), checked at bind time against the rules generated from Spark (lambdas and named arguments are left alone)
void WrapArgTypeChecks(ParsedExpression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::FUNCTION) {
		return;
	}
	auto &func = expr.Cast<FunctionExpression>();
	if (!func.GetQualifiedName().Schema().empty()) {
		return;
	}
	auto name = StringUtil::Lower(func.FunctionName().GetIdentifierName());
	auto &args = func.GetArgumentsMutable();
	for (idx_t i = 0; i < args.size(); i++) {
		auto &argument = args[i].GetExpressionMutable();
		if (!HasArgTypeRule(name, i) || argument->GetExpressionClass() == ExpressionClass::LAMBDA ||
		    args[i].HasName()) {
			continue;
		}
		vector<ExprPtr> check;
		check.push_back(std::move(argument));
		check.push_back(ConstantExpression::String(name));
		check.push_back(ConstantExpression::Integer(int64_t(i)));
		argument = SparkCall("check_arg", std::move(check));
	}
}

//! FunctionExpression in spark_strict: the name the user wrote must be a Spark function (checked before DuckDB's
//! own rewrites such as count() -> count_star)
ExprPtr TransformStrictFunctionHook(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	// DuckFunctionExpression <- FunctionIdentifier ...
	auto &function = AsList(list.GetChild(0));
	vector<string> tokens;
	CollectTokens(function.GetChild(0), tokens);
	if (tokens.size() == 1) {
		CheckSparkFunctionName(tokens[0]);
	}
	auto expr = TransformFunctionCall(t, list.GetChild(0));
	CheckFoldableArguments(*expr);
	CheckNullLiteralArguments(*expr);
	WrapArgTypeChecks(*expr);
	return FinalizeFunctionHookResult(std::move(expr));
}

//! CAST(x AS T) has Spark (non-ANSI) semantics: invalid input becomes NULL, numbers truncate/wrap
ExprPtr TransformCastHook(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	auto expr = Expr(t, list.GetChild(0));
	if (expr->GetExpressionClass() != ExpressionClass::CAST) {
		return expr;
	}
	auto &cast = expr->Cast<CastExpression>();
	auto type_expr = unique_ptr_cast<ParsedExpression, TypeExpression>(cast.TargetType().Copy());
	auto null_of_type = make_uniq<CastExpression>(std::move(type_expr), ConstantExpression::Null());
	auto child = std::move(cast.ChildMutable());
	return SparkCall(cast.IsTryCast() ? "try_cast_value" : "cast_value", std::move(child), std::move(null_of_type));
}

//! CASE: Spark coerces the branch values to a common type (a string wins over numbers) - see spark.case_when
ExprPtr TransformCaseHook(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	auto expr = Expr(t, list.GetChild(0));
	if (expr->GetExpressionClass() != ExpressionClass::CASE) {
		return expr;
	}
	auto &case_expr = expr->Cast<CaseExpression>();
	vector<ExprPtr> args;
	for (auto &check : case_expr.CaseChecksMutable()) {
		args.push_back(std::move(check.when_expr));
		args.push_back(std::move(check.then_expr));
	}
	args.push_back(case_expr.ElseMutable() ? std::move(case_expr.ElseMutable()) : ConstantExpression::Null());
	return SparkCall("case_when", std::move(args));
}

//! ORDER BY x: Spark sorts NULLs first for ASC and last for DESC (DuckDB: last for both)
arena_ptr<TransformResultValue> FinalizeOrderByHook(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	auto node = t.Transform<OrderByNode>(list.GetChild(0));
	if (node.type == OrderType::ORDER_DEFAULT) {
		// independent of DuckDB's default_order setting
		node.type = OrderType::ASCENDING;
	}
	if (node.null_order == OrderByNullType::ORDER_DEFAULT) {
		node.null_order = node.type == OrderType::DESCENDING ? OrderByNullType::NULLS_LAST : OrderByNullType::NULLS_FIRST;
	}
	return t.MakeResult<OrderByNode>(std::move(node));
}

arena_ptr<TransformProcess> OrderByHookProcess(PEGTransformer &t, ParseResult &pr) {
	return t.MakeProcess<FinalizeTransformProcess>(t, pr, FinalizeOrderByHook);
}

//===----------------------------------------------------------------------===//
// Types
//===----------------------------------------------------------------------===//
ExprPtr TypeNamed(const string &name, vector<ExprPtr> children = {}) {
	return make_uniq<TypeExpression>(Identifier(name), std::move(children));
}

//! SpArrayType <- 'ARRAY' '<' NestedType '>'
ExprPtr TransformArrayType(PEGTransformer &t, ParseResult &pr) {
	vector<ExprPtr> children;
	children.push_back(Expr(t, AsList(pr).GetChild(2)));
	return TypeNamed("list", std::move(children));
}

//! SpMapType <- 'MAP' '<' NestedType ',' NestedType '>'
ExprPtr TransformMapType(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	vector<ExprPtr> children;
	children.push_back(Expr(t, list.GetChild(2)));
	children.push_back(Expr(t, list.GetChild(4)));
	return TypeNamed("MAP", std::move(children));
}

//! SpStructType <- 'STRUCT' '<' List(SpStructTypeField)? '>' ; SpStructTypeField <- ColLabel ':'? NestedType SpComment?
ExprPtr TransformStructType(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	vector<ExprPtr> children;
	if (HasOptional(list.GetChild(2))) {
		for (auto &item_ref : ListItems(OptionalValue(list.GetChild(2)))) {
			auto &item = AsList(item_ref.get());
			auto name = t.Transform<string>(item.GetChild(0));
			auto type = Expr(t, item.GetChild(2));
			type->SetAlias(Identifier(name));
			children.push_back(std::move(type));
		}
	}
	return TypeNamed("STRUCT", std::move(children));
}

//! spark_strict: CHAR / VARCHAR / CHARACTER without a length is Spark's DATATYPE_MISSING_SIZE
ExprPtr TransformCharTypeStrict(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	if (!HasOptional(list.GetChild(1))) {
		auto name = StringUtil::Upper(Literal(list.GetChild(0)));
		throw ParserException("[DATATYPE_MISSING_SIZE] DataType \"%s\" requires a length parameter, for example "
		                      "\"%s\"(10). Please specify the length.",
		                      name, name);
	}
	return TypeNamed("VARCHAR");
}

//! SpTypeAlias <- 'STRING' / 'LONG' / ...
ExprPtr TransformTypeAlias(PEGTransformer &t, ParseResult &pr) {
	auto name = StringUtil::Upper(Literal(pr));
	static const unordered_map<string, string> aliases = {
	    {"STRING", "VARCHAR"},       {"LONG", "BIGINT"},         {"SHORT", "SMALLINT"},
	    {"BYTE", "TINYINT"},         {"BINARY", "BLOB"},         {"TIMESTAMP_NTZ", "TIMESTAMP"},
	    {"TIMESTAMP_LTZ", "TIMESTAMP"}, {"VOID", "NULL"},        {"INT", "INTEGER"}};
	auto entry = aliases.find(name);
	return TypeNamed(entry == aliases.end() ? name : entry->second);
}

//! SpDecimalType <- ('DECIMAL' / 'DEC' / 'NUMERIC') SpDecimalModifiers?
//! Without modifiers Spark's DECIMAL is DECIMAL(10, 0) (DuckDB's default is (18, 3)).
ExprPtr TransformDecimalType(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	vector<ExprPtr> children;
	auto &mods = list.GetChild(1);
	if (HasOptional(mods)) {
		auto &mod_list = AsList(OptionalValue(mods));
		for (auto &item : ListItems(mod_list.GetChild(1))) {
			children.push_back(Expr(t, item.get()));
		}
		if (children.size() == 1) {
			children.push_back(ConstantExpression::Integer(0));
		}
	} else {
		children.push_back(ConstantExpression::Integer(10));
		children.push_back(ConstantExpression::Integer(0));
	}
	return TypeNamed("DECIMAL", std::move(children));
}

//! SpCharType <- ('CHAR' / 'CHARACTER' / 'VARCHAR') SpCharLength? : Spark strings have no length limit here
ExprPtr TransformCharType(PEGTransformer &t, ParseResult &pr) {
	return TypeNamed("VARCHAR");
}


//===----------------------------------------------------------------------===//
// spark_strict: reject DuckDB-only syntax
//===----------------------------------------------------------------------===//
string RuleName(const char *definition) {
	string text(definition);
	return StringUtil::Replace(text.substr(0, text.find("<-")), " ", "");
}

//! Raise when the rule matches: the dialect has no such construct. The rule keeps DuckDB's recipe.
void RejectRule(ParsedGrammar &g, const char *definition, const string &message) {
	auto name = RuleName(definition);
	auto rule = g.GetRule(name);
	if (!rule) {
		throw InternalException("spark dialect: rule %s not found", name);
	}
	g.SetTransformProcess(
	    name,
	    [message](PEGTransformer &, ParseResult &) -> arena_ptr<TransformProcess> {
		    throw ParserException("%s", message);
	    },
	    false, rule->suppress_suggestions);
}

//! Keep the rule's transform but run `check` on the parse result first (the check raises)
void GuardRule(ParsedGrammar &g, const char *definition, std::function<void(ParseResult &)> check) {
	auto name = RuleName(definition);
	auto rule = g.GetRule(name);
	if (!rule || !rule->transform_process) {
		throw InternalException("spark dialect: cannot guard rule %s", definition);
	}
	auto original = rule->transform_process;
	// not collapsible: the check must see every match
	g.SetTransformProcess(
	    name,
	    [original, check](PEGTransformer &t, ParseResult &pr) -> arena_ptr<TransformProcess> {
		    check(pr);
		    return original(t, pr);
	    },
	    false, rule->suppress_suggestions);
}

//! Remove the alternative `name` from a choice rule
void RemoveReference(ParsedGrammar &g, const char *rule, const char *name) {
	g.RemoveChoice(rule, [name](const PEGExpression &e) {
		return e.type == PEGExpression::Type::REFERENCE && e.text.GetString() == name;
	});
}

bool HasToken(ParseResult &pr, const char *upper_token) {
	vector<string> tokens;
	CollectTokens(pr, tokens);
	for (auto &token : tokens) {
		if (StringUtil::CIEquals(token, upper_token)) {
			return true;
		}
	}
	return false;
}

//! Spark 3.5 type names (a type the user wrote, lower case)
bool IsSparkTypeName(const string &name) {
	static const unordered_set<string> names = {
	    "boolean", "tinyint",   "byte",          "smallint", "short",     "int",       "integer", "bigint",
	    "long",    "float",     "real",          "double",   "decimal",   "dec",       "numeric", "string",
	    "varchar", "char",      "character",     "binary",   "date",      "timestamp", "timestamp_ntz",
	    "timestamp_ltz", "interval", "void"};
	return names.count(name) > 0;
}

//! Spark 3.5 table-valued functions
bool IsSparkTableFunction(const string &name) {
	static const unordered_set<string> names = {"range",         "explode",   "explode_outer", "inline",
	                                            "inline_outer",  "posexplode", "posexplode_outer", "stack",
	                                            "json_tuple",    "sql_keywords"};
	return names.count(name) > 0;
}

void ApplyStrictGrammar(ParsedGrammar &g) {
	// CHAR / VARCHAR without a length is a Spark parse error (DuckDB: a string type)
	g.SetTransformProcess("SpCharType", ExprProcess<TransformCharTypeStrict>, false);
	// ---- expressions ----
	RejectRule(g, "ListExpression <- ArrayBoundedListExpression / ArrayParensSelect",
	           "Spark SQL does not support DuckDB's list literals; use array(...)");
	RejectRule(g, "ListComprehensionExpression <- '[' Expression 'FOR' List(ColIdOrString) 'IN' Expression "
	              "ListComprehensionFilter? ']'",
	           "Spark SQL does not support list comprehensions");
	RejectRule(g, "StructExpression <- '{' List(StructField)? '}'",
	           "Spark SQL does not support DuckDB's struct literals; use struct(...) or named_struct(...)");
	RejectRule(g, "MapExpression <- 'MAP' MapStructExpression",
	           "Spark SQL does not support DuckDB's MAP {...} literals; use map(...)");
	RejectRule(g, "PositionalExpression <- '#' NumberLiteral", "Spark SQL does not support positional references (#1)");
	RejectRule(g, "TryExpression <- 'TRY' Parens(Expression)", "Spark SQL does not support DuckDB's TRY(...)");
	RejectRule(g, "ColumnsExpression <- StarSymbol? 'COLUMNS' Parens(Expression)",
	           "Spark SQL does not support DuckDB's COLUMNS(...)");
	RejectRule(g, "UnpackExpression <- 'UNPACK' Parens(Expression)", "Spark SQL does not support DuckDB's UNPACK(...)");
	RejectRule(g, "LambdaExpression <- 'LAMBDA' List(ColIdOrString) ':' Expression",
	           "Spark SQL does not support DuckDB's lambda syntax; use x -> expr");
	RejectRule(g, "ColIdExpression <- ColId ':' Expression", "Spark SQL does not support the 'alias: expression' syntax");
	RejectRule(g, "ExcludeList <- ExcludeOrExcept ExcludeNames", "Spark SQL does not support * EXCLUDE / * EXCEPT");
	RejectRule(g, "ReplaceList <- 'REPLACE' ReplaceEntries", "Spark SQL does not support * REPLACE");
	RejectRule(g, "RenameList <- 'RENAME' RenameEntries", "Spark SQL does not support * RENAME");

	RemoveReference(g, "SpIndirection", "SpCast");
	RejectRule(g, "SummarizeRule <- 'SUMMARIZE'", "Spark SQL does not support SUMMARIZE");

	// ---- query clauses ----
	RejectRule(g, "QualifyClause <- 'QUALIFY' Expression", "Spark SQL does not support QUALIFY");
	RejectRule(g, "Asof <- 'ASOF'", "Spark SQL does not support ASOF joins");
	RejectRule(g, "PositionalJoinPrefix <- 'POSITIONAL'", "Spark SQL does not support POSITIONAL joins");
	RejectRule(g, "JoinByClause <- 'JOIN' 'BY' Parens('TYPE' ColLabel) TableRef JoinQualifier",
	           "Spark SQL does not support JOIN BY");
	RejectRule(g, "NearestJoinClause <- NearestJoinBare / NearestJoinAliased",
	           "Spark SQL does not support NEAREST BY joins");
	RejectRule(g, "SelectSampleClause <- UsingSample SampleEntry", "Spark SQL does not support USING SAMPLE; use TABLESAMPLE");
	RejectRule(g, "SampleFunction <- ColId", "Spark SQL does not support TABLESAMPLE sampling methods");
	RejectRule(g, "SampleProperties <- ColId (',' SampleSeed)?", "Spark SQL does not support TABLESAMPLE sampling methods");
	RejectRule(g, "LimitLiteralPercent <- NumberLiteral 'PERCENT'", "Spark SQL does not support LIMIT <n> PERCENT");
	GuardRule(g, "LimitExpression <- Expression '%'?", [](ParseResult &pr) {
		if (HasOptional(AsList(pr).GetChild(1))) {
			throw ParserException("Spark SQL does not support LIMIT <n>%");
		}
	});
	GuardRule(g, "SamplePercentage <- '%' / 'PERCENT'", [](ParseResult &pr) {
		if (Literal(pr) == "%") {
			throw ParserException("Spark SQL does not support TABLESAMPLE <n>%; use <n> PERCENT");
		}
	});
	// table functions in FROM are limited to Spark's own
	GuardRule(g, "QualifiedTableFunction <- CatalogQualification? SchemaQualification* TableFunctionName",
	          [](ParseResult &pr) {
		          vector<string> tokens;
		          CollectTokens(pr, tokens);
		          // table functions generated by the tokenizer are qualified (system.spark.sql_keywords)
		          if (tokens.size() >= 3 && StringUtil::CIEquals(tokens[0], SYSTEM_CATALOG) &&
		              StringUtil::CIEquals(tokens[1], SPARK_SCHEMA)) {
			          return;
		          }
		          if (tokens.size() != 1 || !IsSparkTableFunction(StringUtil::Lower(tokens[0]))) {
			          throw BinderException("[UNRESOLVED_ROUTINE] Cannot resolve table-valued function `%s`: it is not a "
			                                "Spark SQL function",
			                                tokens.empty() ? string() : tokens.back());
		          }
	          });

	// ---- statements ----
	for (auto name : {"ExternalResourceStatement", "ExtensionRepositoryStatement", "PragmaStatement", "CallStatement",
	                  "CopyStatement", "PrepareStatement", "ExecuteStatement", "TransactionStatement",
	                  "AttachStatement", "DetachStatement", "CheckpointStatement", "VacuumStatement",
	                  "ExportStatement", "ImportStatement", "DeallocateStatement", "LoadStatement",
	                  "InstallStatement", "UpdateExtensionsStatement", "ConnectStatement", "DisconnectStatement"}) {
		RemoveReference(g, "Statement", name);
	}
	RemoveReference(g, "SelectStatementType", "PivotStatement");
	RemoveReference(g, "SelectStatementType", "UnpivotStatement");
	for (auto name : {"CreateMacroStmt", "CreateSequenceStmt", "CreateTypeStmt", "CreateIndexStmt", "CreateSecretStmt",
	                  "CreateTriggerStmt"}) {
		RemoveReference(g, "CreateStatementVariation", name);
	}
	RejectRule(g, "OrAction <- InsertOrReplace / InsertOrIgnore", "Spark SQL does not support INSERT OR REPLACE / IGNORE");
	RejectRule(g, "OnConflictClause <- 'ON' 'CONFLICT' OnConflictTarget? OnConflictAction",
	           "Spark SQL does not support ON CONFLICT");
	RejectRule(g, "ReturningClause <- 'RETURNING' TargetList", "Spark SQL does not support RETURNING");

	// ---- types ----
	for (auto name : {"BitType", "RowType", "VariantType", "MapType", "TupleType", "GeometryType", "UnionType",
	                  "SetofType"}) {
		RemoveReference(g, "TypeVariations", name);
	}
	RejectRule(g, "ArrayBounds <- ArrayKeywordWithBounds / SquareBracketsArray / ArrayKeyword",
	           "Spark SQL does not support DuckDB's array types (T[]); use ARRAY<T>");
	GuardRule(g, "QualifiedSimpleType <- QualifiedTypeName TypeModifiers?", [](ParseResult &pr) {
		vector<string> tokens;
		CollectTokens(AsList(pr).GetChild(0), tokens);
		// the extension's own system.spark.* types are allowed
		if (tokens.size() >= 3 && StringUtil::CIEquals(tokens[0], SYSTEM_CATALOG) &&
		    StringUtil::CIEquals(tokens[1], SPARK_SCHEMA)) {
			return;
		}
		if (tokens.size() != 1 || !IsSparkTypeName(StringUtil::Lower(tokens[0]))) {
			throw ParserException("[UNSUPPORTED_DATATYPE] Spark SQL does not support the data type `%s`",
			                      tokens.empty() ? string() : tokens.back());
		}
	});
	// DuckDB's DOUBLE PRECISION and FLOAT(n) are not Spark types
	RejectRule(g, "DoubleType <- ('DOUBLE' 'PRECISION')",
	           "[PARSE_SYNTAX_ERROR] Syntax error at or near 'PRECISION': Spark SQL does not support DOUBLE PRECISION");
	GuardRule(g, "FloatType <- 'FLOAT' Parens(NumberLiteral)?", [](ParseResult &pr) {
		if (HasOptional(AsList(pr).GetChild(1))) {
			throw ParserException("[UNSUPPORTED_DATATYPE] Spark SQL does not support the data type FLOAT(n)");
		}
	});
	GuardRule(g, "TimeType <- TimeOrTimestamp TypeModifiers? TimeZone?", [](ParseResult &pr) {
		auto &list = AsList(pr);
		if (!HasToken(list.GetChild(0), "TIMESTAMP") || HasOptional(list.GetChild(2)) || HasOptional(list.GetChild(1))) {
			throw ParserException("[UNSUPPORTED_DATATYPE] Spark SQL only supports TIMESTAMP, TIMESTAMP_NTZ and "
			                      "TIMESTAMP_LTZ");
		}
	});
}

//===----------------------------------------------------------------------===//
// Grammar text
//===----------------------------------------------------------------------===//
struct RuleDef {
	const char *definition;
	grammar_transform_process_function_t transform;
};

} // namespace

void SparkDialect::ApplyGrammarChanges(GrammarChangesInput &input) {
	auto &g = input.parsed_grammar;

	// new words must stay usable as identifiers
	for (auto word : {"RLIKE", "REGEXP", "DIV", "LONG", "SHORT", "BYTE", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "VOID"}) {
		g.AddChoice("UnreservedKeyword", string("'") + word + "'");
	}

	vector<RuleDef> rules = {
	    {"SpAliasList <- Expression 'AS' '(' List(ColLabel) ')'", nullptr},
	    {"SpExpression <- SpOr SpLambdaTail?", ExprProcess<TransformExprMain>},
	    {"SpLambdaTail <- '->' SpExpression", nullptr},
	    {"SpOr <- SpAnd SpOrTail*", ExprProcess<TransformOr>},
	    {"SpOrTail <- 'OR' SpAnd", nullptr},
	    {"SpAnd <- SpNot SpAndTail*", ExprProcess<TransformAnd>},
	    {"SpAndTail <- 'AND' SpNot", nullptr},
	    {"SpNot <- SpNotPrefix / SpPredicated", ExprProcess<TransformForward>},
	    {"SpNotPrefix <- 'NOT' SpNot", ExprProcess<TransformNotPrefix>},
	    {"SpPredicated <- SpComparison SpPredicate?", ExprProcess<TransformPredicated>},
	    {"SpPredicate <- SpBetween / SpInQuery / SpInList / SpRLike / SpLikeQuantified / SpLike / SpIsNull / "
	     "SpIsBool / SpIsDistinct",
	     nullptr},
	    {"SpBetween <- 'NOT'? 'BETWEEN' SpComparison 'AND' SpComparison", nullptr},
	    {"SpInQuery <- 'NOT'? 'IN' '(' SelectStatementInternal ')'", nullptr},
	    {"SpInList <- 'NOT'? 'IN' '(' List(SpExpression) ')'", nullptr},
	    {"SpRLike <- 'NOT'? ('RLIKE' / 'REGEXP') SpComparison", nullptr},
	    {"SpLikeQuantified <- 'NOT'? ('LIKE' / 'ILIKE') ('ANY' / 'SOME' / 'ALL') '(' List(SpExpression)? ')'",
	     nullptr},
	    {"SpLike <- 'NOT'? ('LIKE' / 'ILIKE') SpComparison SpEscape?", nullptr},
	    {"SpEscape <- 'ESCAPE' SpComparison", nullptr},
	    {"SpIsNull <- 'IS' 'NOT'? 'NULL'", nullptr},
	    {"SpIsBool <- 'IS' 'NOT'? ('TRUE' / 'FALSE' / 'UNKNOWN')", nullptr},
	    {"SpIsDistinct <- 'IS' 'NOT'? 'DISTINCT' 'FROM' SpComparison", nullptr},
	    {"SpComparison <- SpBitOr SpComparisonTail*", ExprProcess<TransformComparison>},
	    {"SpComparisonTail <- SpComparisonOp SpBitOr", nullptr},
	    {"SpComparisonOp <- '<=>' / '==' / '=' / '!=' / '<>' / '<=' / '>=' / '<' / '>'", nullptr},
	    {"SpBitOr <- SpBitXor SpBitOrTail*", ExprProcess<TransformBinary>},
	    {"SpBitOrTail <- '|' SpBitXor", nullptr},
	    {"SpBitXor <- SpBitAnd SpBitXorTail*", ExprProcess<TransformBinary>},
	    {"SpBitXorTail <- '^' SpBitAnd", nullptr},
	    {"SpBitAnd <- SpAdditive SpBitAndTail*", ExprProcess<TransformBinary>},
	    {"SpBitAndTail <- '&' SpAdditive", nullptr},
	    {"SpAdditive <- SpMultiplicative SpAdditiveTail*", ExprProcess<TransformBinary>},
	    {"SpAdditiveTail <- ('+' / '-' / '||') SpMultiplicative", nullptr},
	    {"SpMultiplicative <- SpUnary SpMultiplicativeTail*", ExprProcess<TransformBinary>},
	    {"SpMultiplicativeTail <- ('*' / '/' / '%' / 'DIV') SpUnary", nullptr},
	    {"SpUnary <- SpUnaryOp / SpPrimary", ExprProcess<TransformForward>},
	    {"SpUnaryOp <- ('-' / '+' / '~') SpUnary", ExprProcess<TransformUnary>},
	    {"SpPrimary <- SpAtom SpIndirection*", ExprProcess<TransformPrimary>},
	    {"SpIndirection <- SpSubscript / SpField / SpCast", nullptr},
	    {"SpSubscript <- '[' SpExpression ']'", nullptr},
	    {"SpField <- '.' ColLabel", nullptr},
	    {"SpCast <- '::' Type", nullptr},
	    {"SpAtom <- SpArrayCtor / SpMapCtor / SpStructCtor / SpExistsFunction / SpPositionIn / SpKeywordFunction / SpCastFunction / "
	     "SpSubstrFrom / "
	     "SingleExpression",
	     ExprProcess<TransformForward>},
	    {"SpExistsFunction <- 'EXISTS' '(' List(SpExpression) ')'", ExprProcess<TransformExistsFunction>},
	    {"SpKeywordFunction <- ('ANY' / 'SOME' / 'EVERY' / 'CHAR' / 'POSITION' / 'LEFT' / 'RIGHT') '(' List(SpExpression) ')'",
	     ExprProcess<TransformKeywordFunction>},
	    {"SpCastFunction <- SpCastFunctionName '(' SpExpression ')'", ExprProcess<TransformCastFunction>},
	    {"SpSubstrFrom <- ('SUBSTR' / 'SUBSTRING') '(' SpExpression 'FROM' SpExpression SpSubstrFor? ')'",
	     ExprProcess<TransformSubstrFrom>},
	    {"SpSubstrFor <- 'FOR' SpExpression", nullptr},
	    {"SpPositionIn <- 'POSITION' '(' SpComparison 'IN' SpExpression ')'", ExprProcess<TransformPositionIn>},
	    {"SpCastFunctionName <- 'INT' / 'INTEGER' / 'BIGINT' / 'SMALLINT' / 'TINYINT' / 'DOUBLE' / 'FLOAT' / "
	     "'DECIMAL' / 'STRING' / 'BOOLEAN' / 'DATE' / 'TIMESTAMP' / 'BINARY'",
	     nullptr},
	    {"SpArrayCtor <- 'ARRAY' '(' List(SpExpression)? ')'", ExprProcess<TransformArrayCtor>},
	    {"SpMapCtor <- 'MAP' '(' List(SpExpression)? ')'", ExprProcess<TransformMapCtor>},
	    {"SpStructCtor <- 'STRUCT' '(' List(SpStructArg)? ')'", ExprProcess<TransformStructCtor>},
	    {"SpStructArg <- SpExpression SpAsAlias?", nullptr},
	    {"SpAsAlias <- 'AS' ColLabel", nullptr},
	    // types
	    {"SpArrayType <- 'ARRAY' '<' NestedType '>'", ExprProcess<TransformArrayType>},
	    {"SpMapType <- 'MAP' '<' NestedType ',' NestedType '>'", ExprProcess<TransformMapType>},
	    {"SpStructType <- 'STRUCT' '<' List(SpStructTypeField)? '>'", ExprProcess<TransformStructType>},
	    {"SpStructTypeField <- ColLabel ':'? NestedType SpTypeComment?", nullptr},
	    {"SpTypeComment <- 'COMMENT' StringLiteral", nullptr},
	    {"SpTypeAlias <- 'STRING' / 'LONG' / 'SHORT' / 'BYTE' / 'BINARY' / 'TIMESTAMP_NTZ' / 'TIMESTAMP_LTZ' / "
	     "'VOID' / 'INT'",
	     ExprProcess<TransformTypeAlias>},
	    {"SpDecimalType <- ('DECIMAL' / 'DEC' / 'NUMERIC') SpDecimalModifiers?", ExprProcess<TransformDecimalType>},
	    {"SpDecimalModifiers <- '(' List(NumberLiteral) ')'", nullptr},
	    {"SpCharType <- ('CHAR' / 'CHARACTER' / 'VARCHAR') SpCharLength?", ExprProcess<TransformCharType>},
	    {"SpCharLength <- '(' NumberLiteral ')'", nullptr},
	};
	// levels of the precedence ladder that return their only child unchanged when nothing else matched (no
	// operator tail, no predicate, no indirection): collapsible, so the matcher skips building a result for them
	// and a plain operand does not pass through every level's transform (like DuckDB's own expression ladder)
	static const unordered_set<string> collapsible = {
	    "SpExpression", "SpOr",     "SpAnd",      "SpNot",      "SpPredicated",     "SpComparison", "SpBitOr",
	    "SpBitXor",     "SpBitAnd", "SpAdditive", "SpMultiplicative", "SpUnary", "SpPrimary",    "SpAtom"};
	for (auto &rule : rules) {
		g.AddRule(rule.definition, rule.transform);
		string definition = rule.definition;
		auto name = definition.substr(0, definition.find(' '));
		if (rule.transform && collapsible.count(name)) {
			g.SetTransformProcess(name, rule.transform, true);
		}
	}
	g.PrependChoice("TypeVariations", "SpCharType");
	g.PrependChoice("TypeVariations", "SpDecimalType");
	g.PrependChoice("TypeVariations", "SpTypeAlias");
	g.PrependChoice("TypeVariations", "SpStructType");
	g.PrependChoice("TypeVariations", "SpMapType");
	g.PrependChoice("TypeVariations", "SpArrayType");

	// Expression is the entry point used everywhere (select list, WHERE, function arguments, ...)
	g.ReplaceRule("Expression <- SpExpression", ExprProcess<TransformForward>);

	// route function calls through the Spark function hook
	auto function_rule = g.GetRule("FunctionExpression");
	if (!function_rule) {
		throw InternalException("spark dialect: FunctionExpression rule not found");
	}
	g.AddRule("DuckFunctionExpression <- FunctionIdentifier FunctionExpressionArguments WithinGroupClause? "
	          "FilterClause? ExportClause? OverClause?",
	          function_rule->transform_process);
	g.ReplaceRule("FunctionExpression <- DuckFunctionExpression",
	              strict ? ExprProcess<TransformStrictFunctionHook> : ExprProcess<TransformFunctionHook>);

	// SUBSTRING(... FROM ...), OVERLAY(... PLACING ...), EXTRACT(... FROM ...), POSITION(... IN ...), TRIM(...) are
	// parsed by dedicated rules; route their results through the same function hook
	auto special_rule = g.GetRule("SpecialFunctionExpression");
	if (!special_rule) {
		throw InternalException("spark dialect: SpecialFunctionExpression rule not found");
	}
	g.AddRule("DuckSpecialFunctionExpression <- CoalesceExpression / UnpackExpression / TryExpression / "
	          "ColumnsExpression / ExtractExpression / LambdaExpression / NullIfExpression / PositionExpression / "
	          "RowExpression / SubstringExpression / TrimExpression / OverlayExpression",
	          special_rule->transform_process);
	g.ReplaceRule("SpecialFunctionExpression <- DuckSpecialFunctionExpression", ExprProcess<TransformFunctionHook>);

	// window functions inside window definitions: SimpleSelect results go through LiftNestedWindows
	auto simple_select_rule = g.GetRule("SimpleSelect");
	if (!simple_select_rule) {
		throw InternalException("spark dialect: SimpleSelect rule not found");
	}
	g.AddRule("DuckSimpleSelect <- SelectFrom WhereClause? GroupByClause? HavingClause? WindowClause? QualifyClause? "
	          "SelectSampleClause?",
	          simple_select_rule->transform_process);
	g.ReplaceRule("SimpleSelect <- DuckSimpleSelect", SimpleSelectProcess);
	auto select_internal_rule = g.GetRule("SelectStatementInternal");
	if (!select_internal_rule) {
		throw InternalException("spark dialect: SelectStatementInternal rule not found");
	}
	g.AddRule("DuckSelectStatementInternal <- WithClause? SelectSetOpChain ResultModifiers?",
	          select_internal_rule->transform_process);
	g.ReplaceRule("SelectStatementInternal <- DuckSelectStatementInternal", SelectStatementInternalProcess);

	auto aliased_rule = g.GetRule("AliasedExpression");
	if (!aliased_rule) {
		throw InternalException("spark dialect: AliasedExpression rule not found");
	}
	g.AddRule("DuckAliasedExpression <- ColIdExpression / ExpressionAsCollabel / ExpressionOptIdentifier",
	          aliased_rule->transform_process);
	g.ReplaceRule("AliasedExpression <- SpAliasList / DuckAliasedExpression", ExprProcess<TransformAliasedHook>);

	auto grouping_rule = g.GetRule("GroupingExpression");
	if (!grouping_rule) {
		throw InternalException("spark dialect: GroupingExpression rule not found");
	}
	g.AddRule("DuckGroupingExpression <- GroupingOrGroupingId Parens(List(Expression)?)",
	          grouping_rule->transform_process);
	g.ReplaceRule("GroupingExpression <- DuckGroupingExpression", ExprProcess<TransformGroupingHook>);

	auto case_rule = g.GetRule("CaseExpression");
	if (!case_rule) {
		throw InternalException("spark dialect: CaseExpression rule not found");
	}
	g.AddRule("DuckCaseExpression <- 'CASE' Expression? CaseWhenThen+ CaseElse? 'END'", case_rule->transform_process);
	g.ReplaceRule("CaseExpression <- DuckCaseExpression", ExprProcess<TransformCaseHook>);

	auto order_rule = g.GetRule("OrderByExpression");
	if (!order_rule) {
		throw InternalException("spark dialect: OrderByExpression rule not found");
	}
	g.AddRule("DuckOrderByExpression <- Expression DescOrAsc? NullsFirstOrLast?", order_rule->transform_process);
	g.ReplaceRule("OrderByExpression <- DuckOrderByExpression", OrderByHookProcess);

	auto cast_rule = g.GetRule("CastExpression");
	if (!cast_rule) {
		throw InternalException("spark dialect: CastExpression rule not found");
	}
	g.AddRule("DuckCastExpression <- CastOrTryCast Parens(CastArguments)", cast_rule->transform_process);
	g.ReplaceRule("CastExpression <- DuckCastExpression", ExprProcess<TransformCastHook>);

	if (strict) {
		ApplyStrictGrammar(g);
	}
}

} // namespace duckdb
