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

struct HookDepthGuard {
	struct State {
		idx_t depth = 0;
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
				state.budget = ThreadStackSize() / 3;
			}
		}
		state.depth++;
		auto used = state.base > here ? state.base - here : here - state.base;
		if (used > state.budget) {
			state.depth--;
			throw ParserException("Expression is nested too deeply for the Spark dialect");
		}
	}
	~HookDepthGuard() {
		Get().depth--;
	}
};

template <expr_fn_t FN>
arena_ptr<TransformResultValue> FinalizeExpr(PEGTransformer &t, ParseResult &pr) {
	HookDepthGuard guard;
	return t.MakeResult<ExprPtr>(FN(t, pr));
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
		return MaybeNot(negate, make_uniq<BetweenExpression>(std::move(value), std::move(lower), std::move(upper)));
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
		return make_uniq<OperatorExpression>(negate ? ExpressionType::COMPARE_NOT_IN : ExpressionType::COMPARE_IN,
		                                     std::move(children));
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

//! SpLambda <- SpLambdaParams '->' SpExpression
ExprPtr TransformLambda(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	auto &params = Alt(list.GetChild(0));
	vector<string> names;
	if (StringUtil::CIEquals(params.Name(), "SpLambdaParen")) {
		for (auto &item : ListItems(params.Cast<ListParseResult>().GetChild(1))) {
			names.push_back(t.Transform<string>(item.get()));
		}
	} else {
		names.push_back(t.Transform<string>(params));
	}
	auto body = Expr(t, list.GetChild(2));
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
	if (!IsSparkFunction(lower) && !StringUtil::StartsWith(lower, "__spark_")) {
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
	if ((name == "sum" || name == "avg" || name == "mean") && func.GetArguments().size() == 1 && !func.ExportState()) {
		// Spark's result types: sum(decimal(p, s)) is decimal(p + 10, s), avg(decimal(p, s)) decimal(p + 4, s + 4),
		// integer sums are BIGINT; any_value(x) carries the input type to the bind step
		auto sample = DuckCall("any_value", func.GetArguments()[0].GetExpression().Copy());
		if (name == "mean") {
			func.SetFunctionName("avg");
		}
		return SparkCall(name == "sum" ? "sum_typed" : "avg_typed", std::move(expr), std::move(sample));
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
		return DuckCall("printf", std::move(args));
	}
	if (name == "elt") {
		auto args = TakeArguments(func);
		if (args.size() < 2) {
			throw BinderException("elt requires at least two arguments");
		}
		vector<ExprPtr> values;
		for (idx_t i = 1; i < args.size(); i++) {
			values.push_back(SparkCall("as_string", std::move(args[i])));
		}
		return SparkCall("elt", std::move(args[0]), DuckCall("list_value", std::move(values)));
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
		// array_sort(a, (l, r) -> cmp): order each element by how many elements compare below it
		auto args = TakeArguments(func);
		vector<ExprPtr> replacements;
		replacements.push_back(ColRef("__spark_other"));
		replacements.push_back(ColRef("__spark_elem"));
		auto below = InlineLambda(args, 1, "array_sort", std::move(replacements));
		auto is_below = Operator("<", SparkCall("comparator_result", std::move(below)), ConstantExpression::Integer(0));
		auto rank = DuckCall("len", DuckCall("list_filter", args[0]->Copy(),
		                                     MakeLambda({"__spark_other"}, std::move(is_below))));
		vector<pair<string, ExprPtr>> fields;
		fields.emplace_back("r", std::move(rank));
		fields.emplace_back("v", ColRef("__spark_elem"));
		auto keyed = DuckCall("list_transform", args[0]->Copy(), MakeLambda({"__spark_elem"}, StructPack(std::move(fields))));
		auto sorted = DuckCall("list_sort", std::move(keyed));
		return DuckCall("list_transform", std::move(sorted), MakeLambda({"__spark_e"}, StructField("__spark_e", "v")));
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
		return DuckCall("map_from_entries",
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
		return DuckCall("map_from_entries",
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
		return NullIfAnyNull(inputs, DuckCall("map_from_entries", DuckCall("list_transform", std::move(keys),
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
	if (expr->GetExpressionClass() == ExpressionClass::WINDOW) {
		auto &window = expr->Cast<WindowExpression>();
		auto name = StringUtil::Lower(window.FunctionName().GetIdentifierName());
		auto &window_args = window.GetArgumentsMutable();
		if (name == "rank" || name == "dense_rank" || name == "percent_rank" || name == "cume_dist" ||
		    name == "row_number") {
			// Spark accepts (and ignores) arguments here
			window_args.clear();
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
		if (window.GetQualifiedName().Schema().empty() && IsSparkOverride("window_" + name) && IsSparkFunction(name)) {
			window.SetQualifiedName(Identifier(SYSTEM_CATALOG), Identifier(SPARK_SCHEMA),
			                        Identifier("window_" + name));
		}
		return expr;
	}
	return RewriteFunction(std::move(expr));
}

ExprPtr TransformFunctionHook(PEGTransformer &t, ParseResult &pr) {
	auto &list = AsList(pr);
	return FinalizeFunctionHookResult(Expr(t, list.GetChild(0)));
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
	    {"percentile_approx", {1, 2}}, {"percentile", {1}}, {"histogram_numeric", {1}}, {"hll_sketch_agg", {1}}};
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
	auto expr = Expr(t, list.GetChild(0));
	CheckFoldableArguments(*expr);
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

//! SpCharType <- ('CHAR' / 'CHARACTER' / 'VARCHAR') '(' NumberLiteral ')' : Spark strings have no length limit here
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
	    {"SpExpression <- SpLambda / SpOr", ExprProcess<TransformForward>},
	    {"SpLambda <- SpLambdaParams '->' SpExpression", ExprProcess<TransformLambda>},
	    {"SpLambdaParams <- SpLambdaParen / ColId", nullptr},
	    {"SpLambdaParen <- '(' List(ColLabel) ')'", nullptr},
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
	    {"SpCharType <- ('CHAR' / 'CHARACTER' / 'VARCHAR') '(' NumberLiteral ')'", ExprProcess<TransformCharType>},
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
