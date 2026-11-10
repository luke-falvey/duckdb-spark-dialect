//===----------------------------------------------------------------------===//
// Spark function library (C++ part).
//
// Most functions here are "dispatchers": they accept any argument types and, once the argument
// types are known, rewrite themselves (bind_expression) into DuckDB functions or into small
// kernels that implement Spark's semantics for those types.
//===----------------------------------------------------------------------===//

#include "spark_dialect.hpp"

#include "duckdb/common/types/decimal.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include <array>
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include <charconv>
#include <cmath>
#include <mutex>

#include "spark_function_names.hpp"
#include "utf8proc.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// Override registry
//===----------------------------------------------------------------------===//
static std::mutex override_lock;
static unordered_set<string> &Overrides() {
	static unordered_set<string> overrides;
	return overrides;
}

bool IsSparkOverride(const string &lower_name) {
	std::lock_guard<std::mutex> guard(override_lock);
	return Overrides().count(lower_name) > 0;
}

void AddSparkOverride(const string &lower_name) {
	std::lock_guard<std::mutex> guard(override_lock);
	Overrides().insert(lower_name);
}

bool IsSparkFunction(const string &lower_name) {
	static const unordered_set<string> names = [] {
		unordered_set<string> result;
		for (auto name : SPARK_FUNCTION_NAMES) {
			result.insert(name);
		}
		return result;
	}();
	return names.count(lower_name) > 0;
}

namespace {

using ExprP = unique_ptr<Expression>;

//===----------------------------------------------------------------------===//
// Type helpers
//===----------------------------------------------------------------------===//
LogicalType Normalize(const LogicalType &type) {
	if (type.id() == LogicalTypeId::INTEGER_LITERAL) {
		return IntegerLiteral::GetType(type);
	}
	return type;
}

bool IsIntegral(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		return true;
	default:
		return false;
	}
}

bool IsLargeIntegral(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UTINYINT:
		return true;
	default:
		return false;
	}
}

bool IsFloating(const LogicalType &type) {
	return type.id() == LogicalTypeId::FLOAT || type.id() == LogicalTypeId::DOUBLE;
}

bool IsDecimal(const LogicalType &type) {
	return type.id() == LogicalTypeId::DECIMAL;
}

bool IsNumeric(const LogicalType &type) {
	return IsIntegral(type) || IsLargeIntegral(type) || IsFloating(type) || IsDecimal(type);
}

bool IsString(const LogicalType &type) {
	return type.id() == LogicalTypeId::VARCHAR;
}

bool IsNull(const LogicalType &type) {
	return type.id() == LogicalTypeId::SQLNULL;
}

idx_t IntegralRank(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return 1;
	case LogicalTypeId::SMALLINT:
		return 2;
	case LogicalTypeId::INTEGER:
		return 3;
	default:
		return 4;
	}
}

LogicalType AsDecimalType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return LogicalType::DECIMAL(3, 0);
	case LogicalTypeId::SMALLINT:
		return LogicalType::DECIMAL(5, 0);
	case LogicalTypeId::INTEGER:
		return LogicalType::DECIMAL(10, 0);
	case LogicalTypeId::BIGINT:
		return LogicalType::DECIMAL(20, 0);
	case LogicalTypeId::DECIMAL:
		return type;
	default:
		return LogicalType::DECIMAL(38, 0);
	}
}

LogicalType WiderIntegral(const LogicalType &a, const LogicalType &b) {
	return IntegralRank(a) >= IntegralRank(b) ? a : b;
}

string SparkTypeName(const LogicalType &type_p) {
	auto type = Normalize(type_p);
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return "boolean";
	case LogicalTypeId::TINYINT:
		return "tinyint";
	case LogicalTypeId::SMALLINT:
		return "smallint";
	case LogicalTypeId::INTEGER:
		return "int";
	case LogicalTypeId::BIGINT:
		return "bigint";
	case LogicalTypeId::HUGEINT:
		// DuckDB sums integers into HUGEINT where Spark uses BIGINT
		return "bigint";
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
		return "decimal(38,0)";
	case LogicalTypeId::FLOAT:
		return "float";
	case LogicalTypeId::DOUBLE:
		return "double";
	case LogicalTypeId::DECIMAL:
		return "decimal(" + to_string(DecimalType::GetWidth(type)) + "," + to_string(DecimalType::GetScale(type)) +
		       ")";
	case LogicalTypeId::VARCHAR:
		return "string";
	case LogicalTypeId::BLOB:
		return "binary";
	case LogicalTypeId::DATE:
		return "date";
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_NS:
		return "timestamp";
	case LogicalTypeId::INTERVAL:
		return "interval";
	case LogicalTypeId::SQLNULL:
		return "void";
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY:
		return "array<" + SparkTypeName(ListType::GetChildType(type)) + ">";
	case LogicalTypeId::MAP:
		return "map<" + SparkTypeName(MapType::KeyType(type)) + "," + SparkTypeName(MapType::ValueType(type)) + ">";
	case LogicalTypeId::STRUCT: {
		string result = "struct<";
		auto &children = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < children.size(); i++) {
			if (i > 0) {
				result += ",";
			}
			result += children[i].first.GetIdentifierName() + ":" + SparkTypeName(children[i].second);
		}
		return result + ">";
	}
	default:
		return StringUtil::Lower(type.ToString());
	}
}

//===----------------------------------------------------------------------===//
// Bound-expression builders
//===----------------------------------------------------------------------===//
struct Builder {
	explicit Builder(ClientContext &context_p) : context(context_p) {
	}
	ClientContext &context;

	ExprP Call(const string &name, vector<ExprP> args, bool is_operator = false) {
		FunctionBinder binder(context);
		return binder.BindScalarFunction(Identifier(DEFAULT_SCHEMA), Identifier(name), std::move(args), is_operator);
	}
	ExprP Call(const string &name, ExprP a) {
		vector<ExprP> args;
		args.push_back(std::move(a));
		return Call(name, std::move(args));
	}
	ExprP Call(const string &name, ExprP a, ExprP b, bool is_operator = false) {
		vector<ExprP> args;
		args.push_back(std::move(a));
		args.push_back(std::move(b));
		return Call(name, std::move(args), is_operator);
	}
	ExprP Spark(const string &name, vector<ExprP> args) {
		FunctionBinder binder(context);
		return binder.BindScalarFunction(Identifier(SPARK_SCHEMA), Identifier(name), std::move(args));
	}
	ExprP Spark(const string &name, ExprP a) {
		vector<ExprP> args;
		args.push_back(std::move(a));
		return Spark(name, std::move(args));
	}
	ExprP Spark(const string &name, ExprP a, ExprP b) {
		vector<ExprP> args;
		args.push_back(std::move(a));
		args.push_back(std::move(b));
		return Spark(name, std::move(args));
	}
	ExprP Op(const string &op, ExprP a, ExprP b) {
		return Call(op, std::move(a), std::move(b), true);
	}
	ExprP Kernel(const ScalarFunction &function, vector<ExprP> args) {
		return BindSparkKernel(context, function, std::move(args));
	}
	ExprP Kernel(const ScalarFunction &function, ExprP a) {
		vector<ExprP> args;
		args.push_back(std::move(a));
		return Kernel(function, std::move(args));
	}
	ExprP Kernel(const ScalarFunction &function, ExprP a, ExprP b) {
		vector<ExprP> args;
		args.push_back(std::move(a));
		args.push_back(std::move(b));
		return Kernel(function, std::move(args));
	}
	ExprP Cast(ExprP e, const LogicalType &type, bool try_cast = false) {
		return BoundCastExpression::AddCastToType(context, std::move(e), type, try_cast);
	}
	ExprP Constant(Value v) {
		return make_uniq<BoundConstantExpression>(std::move(v));
	}
	ExprP NullOf(const LogicalType &type) {
		return Constant(Value(type));
	}
	ExprP Case(ExprP when, ExprP then, ExprP otherwise) {
		auto type = then->GetReturnType();
		otherwise = Cast(std::move(otherwise), type);
		return make_uniq<BoundCaseExpression>(std::move(when), std::move(then), std::move(otherwise));
	}
	ExprP IsNotNull(ExprP e) {
		auto result = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
		result->GetChildrenMutable().push_back(std::move(e));
		return std::move(result);
	}
	ExprP IsNull(ExprP e) {
		auto result = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN);
		result->GetChildrenMutable().push_back(std::move(e));
		return std::move(result);
	}
	ExprP Or(ExprP a, ExprP b) {
		return make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(a), std::move(b));
	}
	ExprP Compare(ExpressionType type, ExprP a, ExprP b) {
		auto max_type = LogicalType::MaxLogicalType(context, a->GetReturnType(), b->GetReturnType());
		return BoundComparisonExpression::Create(type, Cast(std::move(a), max_type), Cast(std::move(b), max_type));
	}
	ExprP Coalesce(vector<ExprP> args) {
		auto type = args[0]->GetReturnType();
		auto result = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE, type);
		for (auto &arg : args) {
			result->GetChildrenMutable().push_back(Cast(std::move(arg), type));
		}
		return std::move(result);
	}
};

LogicalType ArgType(FunctionBindExpressionInput &input, idx_t i) {
	return Normalize(input.children[i]->GetReturnType());
}

ExprP Arg(FunctionBindExpressionInput &input, idx_t i) {
	auto &child = input.children[i];
	auto type = child->GetReturnType();
	if (type.id() == LogicalTypeId::INTEGER_LITERAL) {
		return BoundCastExpression::AddCastToType(input.context, std::move(child), IntegerLiteral::GetType(type));
	}
	return std::move(child);
}

void NotExecutable(DataChunk &, ExpressionState &, Vector &) {
	throw InternalException("spark dispatch function was not rewritten at bind time");
}

//===----------------------------------------------------------------------===//
// Kernels created at bind time
//===----------------------------------------------------------------------===//
// The dispatchers bind ad-hoc ScalarFunctions (a kernel for the argument types at hand) that are not in the
// catalog. Some optimizers (grouping sets with DISTINCT aggregates, CTE inlining) copy a plan by serializing it,
// and deserializing a function looks it up in the catalog by name. So every kernel is bound as
// system.spark.spark_kernel, which is in the catalog, with a key in its bind data; deserialization restores
// the kernel from a process-wide registry of the kernels bound so far.
struct SparkKernelData : public FunctionData {
	explicit SparkKernelData(string key_p) : key(std::move(key_p)) {
	}
	string key;
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<SparkKernelData>(key);
	}
	bool Equals(const FunctionData &other) const override {
		return key == other.Cast<SparkKernelData>().key;
	}
};

struct SparkKernelEntry {
	scalar_function_t function;
	FunctionNullHandling null_handling;
	FunctionErrors errors;
};

std::mutex &SparkKernelLock() {
	static std::mutex lock;
	return lock;
}

unordered_map<string, SparkKernelEntry> &SparkKernels() {
	static unordered_map<string, SparkKernelEntry> kernels;
	return kernels;
}

void SerializeSparkKernel(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                          const BoundScalarFunction &) {
	serializer.WriteProperty(100, "kernel", bind_data->Cast<SparkKernelData>().key);
}

unique_ptr<FunctionData> DeserializeSparkKernel(Deserializer &deserializer, BoundScalarFunction &function) {
	auto key = deserializer.ReadProperty<string>(100, "kernel");
	SparkKernelEntry entry;
	{
		std::lock_guard<std::mutex> guard(SparkKernelLock());
		auto it = SparkKernels().find(key);
		if (it == SparkKernels().end()) {
			throw SerializationException("Spark dialect kernel %s is not available in this process", key);
		}
		entry = it->second;
	}
	function.SetFunctionCallback(entry.function);
	function.SetNullHandling(entry.null_handling);
	function.SetErrorMode(entry.errors);
	return make_uniq<SparkKernelData>(key);
}

ScalarFunction SparkKernelCatalogEntry() {
	ScalarFunction fun(Identifier("spark_kernel"), {}, LogicalType::ANY, NotExecutable, nullptr, nullptr, nullptr,
	                   LogicalType::ANY);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.SetSerializeCallback(SerializeSparkKernel);
	fun.SetDeserializeCallback(DeserializeSparkKernel);
	return fun;
}

unique_ptr<Expression> BindSparkKernelImpl(ClientContext &context, ScalarFunction function,
                                           vector<unique_ptr<Expression>> args) {
	using raw_fn_t = void (*)(DataChunk &, ExpressionState &, Vector &);
	auto callback = function.GetFunctionCallback();
	auto raw = callback.target<raw_fn_t>();
	string key = function.GetName() + "(";
	for (auto &param : function.GetSignature().GetParameters()) {
		key += param.GetType().ToString() + ",";
	}
	key += ")->" + function.GetReturnType().ToString() + "@" +
	       to_string(raw ? reinterpret_cast<uintptr_t>(*raw) : uintptr_t(0));
	{
		std::lock_guard<std::mutex> guard(SparkKernelLock());
		SparkKernels()[key] = SparkKernelEntry {callback, function.GetNullHandling(), function.GetErrorMode()};
	}
	function.SetQualifiedName(
	    QualifiedName(Identifier(SYSTEM_CATALOG), Identifier(SPARK_SCHEMA), Identifier("spark_kernel")));
	function.SetSerializeCallback(SerializeSparkKernel);
	function.SetDeserializeCallback(DeserializeSparkKernel);
	FunctionBinder binder(context);
	auto expr = binder.BindScalarFunction(function, std::move(args));
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		expr->Cast<BoundFunctionExpression>().BindInfoMutable() = make_uniq<SparkKernelData>(key);
	}
	return expr;
}

//! The bind of every dispatcher, by name (ClassifiedBind looks it up)
unordered_map<string, function_bind_expression_t> &DispatcherBinds() {
	static unordered_map<string, function_bind_expression_t> binds;
	return binds;
}

//! A dispatcher's bind, with DuckDB's unclassified type errors raised inside it (no function for these argument
//! types, no common type, no cast between the types) reported as Spark's analysis error class DATATYPE_MISMATCH.
//! Errors that already carry a class, and value errors (a string that does not convert), are unchanged.
ExprP ClassifiedBind(FunctionBindExpressionInput &input) {
	auto &binds = DispatcherBinds();
	auto entry = binds.find(input.bound_function.GetName().GetIdentifierName());
	if (entry == binds.end()) {
		throw InternalException("spark dispatcher without a bind: %s", input.bound_function.GetName().GetIdentifierName());
	}
	try {
		return entry->second(input);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		auto &message = error.RawMessage();
		bool type_error = StringUtil::Contains(message, "No function matches") ||
		                  StringUtil::Contains(message, "Cannot combine types") ||
		                  StringUtil::Contains(message, "Could not choose a best candidate") ||
		                  StringUtil::Contains(message, "Unimplemented type for cast");
		bool known_type = error.Type() == ExceptionType::BINDER || error.Type() == ExceptionType::NOT_IMPLEMENTED ||
		                  error.Type() == ExceptionType::CONVERSION;
		if (type_error && known_type && !StringUtil::StartsWith(message, "[")) {
			throw BinderException("[DATATYPE_MISMATCH] %s", message);
		}
		throw;
	}
}

ScalarFunction Dispatcher(const string &name, function_bind_expression_t bind_expression, idx_t min_args = 0) {
	ScalarFunction fun(Identifier(name), {}, LogicalType::ANY, NotExecutable, nullptr, nullptr, nullptr,
	                   LogicalType::ANY);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.SetBindExpressionCallback(bind_expression);
	return fun;
}

void CheckArgs(FunctionBindExpressionInput &input, const char *name, idx_t min_args, idx_t max_args) {
	if (input.children.size() < min_args || input.children.size() > max_args) {
		throw BinderException("Invalid number of arguments for %s", name);
	}
}

//===----------------------------------------------------------------------===//
// Integer kernels with Java (two's complement) overflow semantics
//===----------------------------------------------------------------------===//
template <class T>
T WrapAdd(T a, T b) {
	using U = typename std::make_unsigned<T>::type;
	return T(U(a) + U(b));
}
template <class T>
T WrapSub(T a, T b) {
	using U = typename std::make_unsigned<T>::type;
	return T(U(a) - U(b));
}
template <class T>
T WrapMul(T a, T b) {
	using U = typename std::make_unsigned<T>::type;
	return T(U(a) * U(b));
}

struct CheckedAddOp {
	template <class T>
	static bool Op(T a, T b, T &r) {
		return !__builtin_add_overflow(a, b, &r);
	}
};
struct CheckedSubOp {
	template <class T>
	static bool Op(T a, T b, T &r) {
		return !__builtin_sub_overflow(a, b, &r);
	}
};
struct CheckedMulOp {
	template <class T>
	static bool Op(T a, T b, T &r) {
		return !__builtin_mul_overflow(a, b, &r);
	}
};

template <class T, class OP>
void CheckedBinaryKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<T, T, T>(args.data[0], args.data[1], result, args.size(), [](T a, T b) -> optional<T> {
		T r;
		if (!OP::template Op<T>(a, b, r)) {
			return optional<T>();
		}
		return r;
	});
}

struct AddOp {
	template <class T>
	static T Op(T a, T b) {
		return WrapAdd(a, b);
	}
};
struct SubOp {
	template <class T>
	static T Op(T a, T b) {
		return WrapSub(a, b);
	}
};
struct MulOp {
	template <class T>
	static T Op(T a, T b) {
		return WrapMul(a, b);
	}
};

template <class T, class OP>
void WrapBinaryKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<T, T, T>(args.data[0], args.data[1], result, args.size(),
	                                 [](T a, T b) { return OP::template Op<T>(a, b); });
}

template <class OP, template <class, class> class KERNEL>
ScalarFunction IntegralBinaryFunction(const LogicalType &type) {
	scalar_function_t fn;
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		fn = KERNEL<int8_t, OP>::Run;
		break;
	case LogicalTypeId::SMALLINT:
		fn = KERNEL<int16_t, OP>::Run;
		break;
	case LogicalTypeId::INTEGER:
		fn = KERNEL<int32_t, OP>::Run;
		break;
	default:
		fn = KERNEL<int64_t, OP>::Run;
		break;
	}
	return ScalarFunction(Identifier("spark_integral_op"), {type, type}, type, fn);
}

template <class T, class OP>
struct WrapKernel {
	static void Run(DataChunk &args, ExpressionState &state, Vector &result) {
		WrapBinaryKernel<T, OP>(args, state, result);
	}
};
template <class T, class OP>
struct CheckedKernel {
	static void Run(DataChunk &args, ExpressionState &state, Vector &result) {
		CheckedBinaryKernel<T, OP>(args, state, result);
	}
};

template <class T>
void IntRemainderKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<T, T, T>(args.data[0], args.data[1], result, args.size(), [](T a, T b) -> optional<T> {
		if (b == 0) {
			return optional<T>();
		}
		if (b == -1) {
			return T(0);
		}
		return T(a % b);
	});
}

//! NULL where the value is zero (decimal divisors), so callers need not repeat the operand in a CASE:
//! copying an operand makes nested expressions grow exponentially.
template <class T>
void ZeroToNullKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<T, T>(args.data[0], result, args.size(), [](T a) -> optional<T> {
		if (a == T(0)) {
			return optional<T>();
		}
		return a;
	});
}

//! Spark's 0-based array index as DuckDB's 1-based index; NULL for negative indexes (out of range in Spark)
void SparkIndexKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<int64_t, int64_t>(args.data[0], result, args.size(), [](int64_t i) -> optional<int64_t> {
		if (i < 0 || i == NumericLimits<int64_t>::Maximum()) {
			return optional<int64_t>();
		}
		return i + 1;
	});
}

//===----------------------------------------------------------------------===//
// ANSI mode (spark.sql.ansi.enabled): runtime errors instead of NULL / wrap-around
//===----------------------------------------------------------------------===//
bool AnsiEnabled(ClientContext &context) {
	Value setting;
	if (context.TryGetCurrentSetting(Identifier("spark_ansi_enabled"), setting) && !setting.IsNull()) {
		return setting.GetValue<bool>();
	}
	return false;
}

[[noreturn]] void ThrowDivideByZero() {
	throw InvalidInputException("[DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and "
	                            "return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to "
	                            "bypass this error.");
}

//! the divisor itself; an error where it is zero and the dividend is not NULL (ANSI mode)
template <class T>
void AnsiNonZeroKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<bool, T, T>(args.data[0], args.data[1], result, args.size(), [](bool dividend_present, T a) {
		if (dividend_present && a == T(0)) {
			ThrowDivideByZero();
		}
		return a;
	});
}

ExprP AnsiNonZero(Builder &b, ExprP value, ExprP dividend) {
	auto type = value->GetReturnType();
	scalar_function_t fn;
	switch (type.InternalType()) {
	case PhysicalType::INT8:
		fn = AnsiNonZeroKernel<int8_t>;
		break;
	case PhysicalType::INT16:
		fn = AnsiNonZeroKernel<int16_t>;
		break;
	case PhysicalType::INT32:
		fn = AnsiNonZeroKernel<int32_t>;
		break;
	case PhysicalType::INT64:
		fn = AnsiNonZeroKernel<int64_t>;
		break;
	case PhysicalType::INT128:
		fn = AnsiNonZeroKernel<hugeint_t>;
		break;
	case PhysicalType::FLOAT:
		fn = AnsiNonZeroKernel<float>;
		break;
	case PhysicalType::DOUBLE:
		fn = AnsiNonZeroKernel<double>;
		break;
	default:
		return value;
	}
	ScalarFunction function(Identifier("spark_ansi_nonzero"), {LogicalType::BOOLEAN, type}, type, fn);
	function.SetFallible();
	return b.Kernel(function, b.IsNotNull(std::move(dividend)), std::move(value));
}

//! a 0-based array index checked against the array length (ANSI mode): the 1-based index, or an error
void AnsiIndexKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<int64_t, int64_t, int64_t>(
	    args.data[0], args.data[1], result, args.size(), [](int64_t index, int64_t length) -> int64_t {
		    if (index < 0 || index >= length) {
			    throw InvalidInputException("[INVALID_ARRAY_INDEX] The index %lld is out of bounds. The array has "
			                                "%lld elements. Use the SQL function `get()` to tolerate accessing "
			                                "element at invalid index and return NULL instead.",
			                                (long long)index, (long long)length);
		    }
		    return index + 1;
	    });
}

//! sequence(start, stop, step) over integers: the step, after checking that the array stays within Spark's size
//! limit (a huge range is Spark's error, never an attempt to allocate billions of elements)
void SequenceStepKernel(DataChunk &args, ExpressionState &, Vector &result) {
	TernaryExecutor::Execute<int64_t, int64_t, int64_t, int64_t>(
	    args.data[0], args.data[1], args.data[2], result, args.size(), [](int64_t start, int64_t stop, int64_t step) {
		    if (step != 0 && (hugeint_t(stop) - hugeint_t(start)) / hugeint_t(step) >= hugeint_t(0)) {
			    auto count = (hugeint_t(stop) - hugeint_t(start)) / hugeint_t(step) + hugeint_t(1);
			    if (count > hugeint_t(2147483632)) {
				    throw InvalidInputException("[COLLECTION_SIZE_LIMIT_EXCEEDED] Unsuccessful try to create array with "
				                                "%s elements due to exceeding the array size limit 2147483632.",
				                                count.ToString());
			    }
		    }
		    return step;
	    });
}

//! element_at(array, index) in ANSI mode: the index must be within -length..-1 or 1..length
void AnsiElementAtKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<int64_t, int64_t, int64_t>(
	    args.data[0], args.data[1], result, args.size(), [](int64_t index, int64_t length) -> int64_t {
		    if (index == 0 || index > length || index < -length) {
			    throw InvalidInputException("[INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index %lld is out of bounds. The "
			                                "array has %lld elements. Use `try_element_at` to tolerate accessing "
			                                "element at invalid index and return NULL instead.",
			                                (long long)index, (long long)length);
		    }
		    return index;
	    });
}

//! args: a value (the non-ANSI result of an operation) and whether its inputs were all present. The value must not
//! be NULL where the inputs were present.
void AnsiNullCheckKernel(DataChunk &args, ExpressionState &, Vector &result) {
	auto count = args.size();
	UnifiedVectorFormat value_data, present_data;
	args.data[0].ToUnifiedFormat(count, value_data);
	args.data[1].ToUnifiedFormat(count, present_data);
	for (idx_t i = 0; i < count; i++) {
		auto value_index = value_data.sel->get_index(i);
		auto present_index = present_data.sel->get_index(i);
		bool present = present_data.validity.RowIsValid(present_index) &&
		               UnifiedVectorFormat::GetData<bool>(present_data)[present_index];
		if (!value_data.validity.RowIsValid(value_index) && present) {
			throw InvalidInputException("[CAST_INVALID_INPUT] The value cannot be cast to the target type because it "
			                            "is malformed or out of range. Use `try_cast` to tolerate malformed input and "
			                            "return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to "
			                            "\"false\" to bypass this error.");
		}
	}
	result.Reference(args.data[0]);
}

//! `value`, or an error where it is NULL although all of `inputs` are not
ExprP AnsiNullCheck(Builder &b, ExprP value, vector<ExprP> inputs) {
	auto type = value->GetReturnType();
	ExprP present;
	for (auto &input : inputs) {
		auto not_null = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
		not_null->GetChildrenMutable().push_back(std::move(input));
		if (!present) {
			present = std::move(not_null);
		} else {
			present = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(present),
			                                                std::move(not_null));
		}
	}
	vector<ExprP> args;
	args.push_back(std::move(value));
	args.push_back(std::move(present));
	ScalarFunction function(Identifier("spark_ansi_null_check"), {type, LogicalType::BOOLEAN}, type, AnsiNullCheckKernel);
	function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	function.SetFallible();
	return b.Kernel(function, std::move(args));
}

ExprP ZeroToNull(Builder &b, ExprP value) {
	auto type = value->GetReturnType();
	scalar_function_t fn;
	switch (type.InternalType()) {
	case PhysicalType::INT16:
		fn = ZeroToNullKernel<int16_t>;
		break;
	case PhysicalType::INT32:
		fn = ZeroToNullKernel<int32_t>;
		break;
	case PhysicalType::INT64:
		fn = ZeroToNullKernel<int64_t>;
		break;
	case PhysicalType::INT128:
		fn = ZeroToNullKernel<hugeint_t>;
		break;
	default:
		throw InternalException("ZeroToNull: unsupported type %s", type.ToString());
	}
	return b.Kernel(ScalarFunction(Identifier("spark_zero_to_null"), {type}, type, fn), std::move(value));
}

void IntDivKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<int64_t, int64_t, int64_t>(args.data[0], args.data[1], result, args.size(),
	                                                   [](int64_t a, int64_t b) -> optional<int64_t> {
		                                                   if (b == 0) {
			                                                   return optional<int64_t>();
		                                                   }
		                                                   if (b == -1) {
			                                                   return WrapSub<int64_t>(0, a);
		                                                   }
		                                                   return a / b;
	                                                   });
}

// Unsigned 128-bit arithmetic for the decimal division kernel: the compiler's native type where there is one (its
// division is a single runtime call), DuckDB's portable uhugeint_t otherwise (bit-by-bit division, much slower)
#if defined(__SIZEOF_INT128__)
using U128 = unsigned __int128;
inline U128 ToU128(hugeint_t v) { // v >= 0
	return (U128(uint64_t(v.upper)) << 64) | U128(v.lower);
}
inline hugeint_t FromU128(U128 v) { // v < 2^127
	return hugeint_t(int64_t(uint64_t(v >> 64)), uint64_t(v));
}
#else
using U128 = uhugeint_t;
inline U128 ToU128(hugeint_t v) {
	return uhugeint_t(v);
}
inline hugeint_t FromU128(U128 v) {
	return hugeint_t(v);
}
#endif

//! 10^n as an unsigned 128-bit integer, n <= 38
U128 PowerOfTen(idx_t n) {
	static const auto table = [] {
		std::array<U128, 39> t;
		t[0] = U128(1);
		for (idx_t i = 1; i < t.size(); i++) {
			t[i] = t[i - 1] * U128(10);
		}
		return t;
	}();
	return table[n];
}

//! Spark's decimal division: both operands as DECIMAL(38, s1) / DECIMAL(38, s2), the quotient rounded HALF_UP to the
//! scale of the result DECIMAL(38, S) (the caller narrows it to Spark's precision); NULL when the divisor is zero or
//! the quotient has more than `precision` digits. Exact, unlike a division through DOUBLE.
void DecimalDivideKernel(DataChunk &args, ExpressionState &, Vector &result, idx_t precision) {
	auto s1 = idx_t(DecimalType::GetScale(args.data[0].GetType()));
	auto s2 = idx_t(DecimalType::GetScale(args.data[1].GetType()));
	auto scale = idx_t(DecimalType::GetScale(result.GetType()));
	auto shift = int64_t(s2) + int64_t(scale) - int64_t(s1);
	auto limit = PowerOfTen(precision);
	auto digit_limit = PowerOfTen(precision - 1);
	// the scaled dividend fits in 128 bits below this bound (fast path)
	auto fast_bound = (shift >= 0 && shift <= 38) ? PowerOfTen(idx_t(38 - shift)) : U128(0);
	auto scale_factor = (shift >= 0 && shift <= 38) ? PowerOfTen(idx_t(shift)) : U128(1);
	BinaryExecutor::Execute<hugeint_t, hugeint_t, hugeint_t>(
	    args.data[0], args.data[1], result, args.size(), [&](hugeint_t a, hugeint_t b) -> optional<hugeint_t> {
		    if (b == hugeint_t(0)) {
			    return optional<hugeint_t>();
		    }
		    bool negative = (a < hugeint_t(0)) != (b < hugeint_t(0));
		    auto ua = ToU128(a < hugeint_t(0) ? -a : a);
		    auto ub = ToU128(b < hugeint_t(0) ? -b : b);
		    auto finish = [&](U128 q) -> optional<hugeint_t> {
			    if (q >= limit) {
				    return optional<hugeint_t>();
			    }
			    auto value = FromU128(q);
			    return negative ? optional<hugeint_t>(-value) : optional<hugeint_t>(value);
		    };
		    if (ua < fast_bound) {
			    // the scaled dividend fits: one division and a HALF_UP step on the remainder
			    auto n = ua * scale_factor;
			    U128 q, r;
			    if (n <= U128(NumericLimits<uint64_t>::Maximum()) && ub <= U128(NumericLimits<uint64_t>::Maximum())) {
				    auto n64 = uint64_t(n), b64 = uint64_t(ub);
				    q = U128(n64 / b64);
				    r = U128(n64 % b64);
			    } else {
				    q = n / ub;
				    r = n % ub;
			    }
			    if (r * U128(2) >= ub) {
				    q += U128(1);
			    }
			    return finish(q);
		    }
		    auto q = ua / ub;
		    auto r = ua % ub;
		    // the next decimal digit of r / ub, without overflowing: 10 * r = digit * ub + rest
		    auto next_digit = [&]() {
			    U128 acc(0);
			    uint32_t digit = 0;
			    for (int i = 0; i < 10; i++) {
				    acc += r;
				    if (acc >= ub) {
					    acc -= ub;
					    digit++;
				    }
			    }
			    r = acc;
			    return digit;
		    };
		    if (shift >= 0) {
			    // another digit is past `precision` digits once q has precision - 1 of them
			    for (int64_t i = 0; i < shift; i++) {
				    if (q >= digit_limit) {
					    return optional<hugeint_t>();
				    }
				    q = q * U128(10) + U128(next_digit());
			    }
			    if (next_digit() >= 5) {
				    q += U128(1);
			    }
		    } else if (-shift > 38) {
			    q = U128(0);
		    } else {
			    auto p = PowerOfTen(idx_t(-shift));
			    auto rest = q % p;
			    q = q / p;
			    if (rest * U128(2) >= p) {
				    q += U128(1);
			    }
		    }
		    return finish(q);
	    });
}

void DoubleDivideKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<double, double, double>(args.data[0], args.data[1], result, args.size(),
	                                                [](double a, double b) -> optional<double> {
		                                                if (b == 0) {
			                                                return optional<double>();
		                                                }
		                                                return a / b;
	                                                });
}

void DoubleDivTruncKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<double, double, int64_t>(args.data[0], args.data[1], result, args.size(),
	                                                 [](double a, double b) -> optional<int64_t> {
		                                                 if (b == 0 || std::isnan(a / b)) {
			                                                 return optional<int64_t>();
		                                                 }
		                                                 return int64_t(std::trunc(a / b));
	                                                 });
}

//! Java's Math.signum: NaN and signed zeros are returned unchanged
void SignumKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<double, double>(args.data[0], result, args.size(), [](double d) {
		if (d == 0 || std::isnan(d)) {
			return d;
		}
		return d > 0 ? 1.0 : -1.0;
	});
}

void DoubleRemainderKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<double, double, double>(args.data[0], args.data[1], result, args.size(),
	                                                [](double a, double b) -> optional<double> {
		                                                if (b == 0) {
			                                                return optional<double>();
		                                                }
		                                                return std::fmod(a, b);
	                                                });
}

template <class T>
void WrapNegateKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<T, T>(args.data[0], result, args.size(), [](T a) { return WrapSub<T>(0, a); });
}

ScalarFunction NegateFunction(const LogicalType &type) {
	scalar_function_t fn;
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		fn = WrapNegateKernel<int8_t>;
		break;
	case LogicalTypeId::SMALLINT:
		fn = WrapNegateKernel<int16_t>;
		break;
	case LogicalTypeId::INTEGER:
		fn = WrapNegateKernel<int32_t>;
		break;
	default:
		fn = WrapNegateKernel<int64_t>;
		break;
	}
	return ScalarFunction(Identifier("spark_negate"), {type}, type, fn);
}

//! Java narrowing conversion between integral types (wraps)
template <class SRC, class DST>
void NarrowKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<SRC, DST>(args.data[0], result, args.size(), [](SRC a) { return DST(a); });
}

template <class SRC>
scalar_function_t NarrowFrom(const LogicalType &target) {
	switch (target.id()) {
	case LogicalTypeId::TINYINT:
		return NarrowKernel<SRC, int8_t>;
	case LogicalTypeId::SMALLINT:
		return NarrowKernel<SRC, int16_t>;
	case LogicalTypeId::INTEGER:
		return NarrowKernel<SRC, int32_t>;
	default:
		return NarrowKernel<SRC, int64_t>;
	}
}

ScalarFunction NarrowFunction(const LogicalType &source, const LogicalType &target) {
	scalar_function_t fn;
	switch (source.id()) {
	case LogicalTypeId::SMALLINT:
		fn = NarrowFrom<int16_t>(target);
		break;
	case LogicalTypeId::INTEGER:
		fn = NarrowFrom<int32_t>(target);
		break;
	default:
		fn = NarrowFrom<int64_t>(target);
		break;
	}
	return ScalarFunction(Identifier("spark_narrow"), {source}, target, fn);
}

//! Java (long)/(int) conversion of a double: truncate, NaN -> 0, saturate
template <class DST>
void DoubleToIntegralKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<double, DST>(args.data[0], result, args.size(), [](double d) -> DST {
		if (std::isnan(d)) {
			return 0;
		}
		// narrower than int: Java converts to int first, then narrows (wraps)
		using WIDE = typename std::conditional<(sizeof(DST) < 4), int32_t, DST>::type;
		WIDE wide;
		if (d >= double(NumericLimits<WIDE>::Maximum())) {
			wide = NumericLimits<WIDE>::Maximum();
		} else if (d <= double(NumericLimits<WIDE>::Minimum())) {
			wide = NumericLimits<WIDE>::Minimum();
		} else {
			wide = WIDE(std::trunc(d));
		}
		return DST(wide);
	});
}

//! floor / ceil of a double as Spark's BIGINT in one pass: NaN -> 0, saturating
template <bool CEIL>
void FloorCeilToLongKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<double, int64_t>(args.data[0], result, args.size(), [](double d) -> int64_t {
		if (std::isnan(d)) {
			return 0;
		}
		if (d >= double(NumericLimits<int64_t>::Maximum())) {
			return NumericLimits<int64_t>::Maximum();
		}
		if (d <= double(NumericLimits<int64_t>::Minimum())) {
			return NumericLimits<int64_t>::Minimum();
		}
		return int64_t(CEIL ? std::ceil(d) : std::floor(d));
	});
}

ScalarFunction DoubleToIntegralFunction(const LogicalType &target) {
	scalar_function_t fn;
	switch (target.id()) {
	case LogicalTypeId::TINYINT:
		fn = DoubleToIntegralKernel<int8_t>;
		break;
	case LogicalTypeId::SMALLINT:
		fn = DoubleToIntegralKernel<int16_t>;
		break;
	case LogicalTypeId::INTEGER:
		fn = DoubleToIntegralKernel<int32_t>;
		break;
	default:
		fn = DoubleToIntegralKernel<int64_t>;
		break;
	}
	return ScalarFunction(Identifier("spark_double_to_integral"), {LogicalType::DOUBLE}, target, fn);
}

//===----------------------------------------------------------------------===//
// String parsing kernels (Spark's UTF8String.toLong / toInt and friends)
//===----------------------------------------------------------------------===//
bool IsSparkWhitespace(char c) {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v' || (unsigned char)c <= 0x20;
}

string_t TrimView(string_t s) {
	auto data = s.GetData();
	idx_t begin = 0, end = s.GetSize();
	while (begin < end && IsSparkWhitespace(data[begin])) {
		begin++;
	}
	while (end > begin && IsSparkWhitespace(data[end - 1])) {
		end--;
	}
	return string_t(data + begin, UnsafeNumericCast<uint32_t>(end - begin));
}

//! [+-]digits[.digits] -> truncated integer, false on invalid input or overflow
bool ParseSparkIntegral(string_t input, int64_t &result, bool allow_fraction = true) {
	auto s = TrimView(input);
	auto data = s.GetData();
	idx_t n = s.GetSize();
	if (n == 0) {
		return false;
	}
	idx_t i = 0;
	bool negative = false;
	if (data[0] == '+' || data[0] == '-') {
		negative = data[0] == '-';
		i++;
	}
	if (i >= n) {
		return false;
	}
	uint64_t value = 0;
	bool any_digit = false;
	for (; i < n; i++) {
		char c = data[i];
		if (c == '.') {
			if (!allow_fraction) {
				return false;
			}
			break;
		}
		if (c < '0' || c > '9') {
			return false;
		}
		any_digit = true;
		uint64_t digit = uint64_t(c - '0');
		if (value > (NumericLimits<uint64_t>::Maximum() - digit) / 10) {
			return false;
		}
		value = value * 10 + digit;
	}
	if (i < n) {
		// fraction: digits only
		i++;
		for (; i < n; i++) {
			if (data[i] < '0' || data[i] > '9') {
				return false;
			}
		}
	}
	if (!any_digit && (i == 0)) {
		return false;
	}
	const uint64_t limit = uint64_t(NumericLimits<int64_t>::Maximum()) + (negative ? 1 : 0);
	if (value > limit) {
		return false;
	}
	result = negative ? int64_t(uint64_t(0) - value) : int64_t(value);
	return true;
}

template <class DST, bool ALLOW_FRACTION = true>
void StringToIntegralKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<string_t, DST>(args.data[0], result, args.size(), [](string_t s) -> optional<DST> {
		int64_t value;
		if (!ParseSparkIntegral(s, value, ALLOW_FRACTION) || value > int64_t(NumericLimits<DST>::Maximum()) ||
		    value < int64_t(NumericLimits<DST>::Minimum())) {
			return optional<DST>();
		}
		return DST(value);
	});
}

//! `exact`: ANSI mode, where "1.5" is not an integer
ScalarFunction StringToIntegralFunction(const LogicalType &target, bool exact = false) {
	scalar_function_t fn;
	switch (target.id()) {
	case LogicalTypeId::TINYINT:
		fn = exact ? StringToIntegralKernel<int8_t, false> : StringToIntegralKernel<int8_t>;
		break;
	case LogicalTypeId::SMALLINT:
		fn = exact ? StringToIntegralKernel<int16_t, false> : StringToIntegralKernel<int16_t>;
		break;
	case LogicalTypeId::INTEGER:
		fn = exact ? StringToIntegralKernel<int32_t, false> : StringToIntegralKernel<int32_t>;
		break;
	default:
		fn = exact ? StringToIntegralKernel<int64_t, false> : StringToIntegralKernel<int64_t>;
		break;
	}
	return ScalarFunction(Identifier("spark_string_to_integral"), {LogicalType::VARCHAR}, target, fn);
}

void StringToBooleanKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<string_t, bool>(args.data[0], result, args.size(), [](string_t s) -> optional<bool> {
		auto v = StringUtil::Lower(TrimView(s).GetString());
		if (v == "t" || v == "true" || v == "y" || v == "yes" || v == "1") {
			return true;
		}
		if (v == "f" || v == "false" || v == "n" || v == "no" || v == "0") {
			return false;
		}
		return optional<bool>();
	});
}

//! Spark's DateTimeUtils.stringToDate: yyyy, yyyy-[m]m, yyyy-[m]m-[d]d, followed by ' ' or 'T' and anything
bool ParseSparkDate(string_t input, date_t &result) {
	auto s = TrimView(input);
	auto str = s.GetString();
	int32_t segments[3] = {1, 1, 1};
	idx_t segment = 0;
	idx_t digits = 0;
	int64_t current = 0;
	idx_t i = 0;
	bool negative = false;
	if (!str.empty() && (str[0] == '-' || str[0] == '+')) {
		negative = str[0] == '-';
		i++;
	}
	for (; i < str.size(); i++) {
		char c = str[i];
		if (segment < 2 && c == '-') {
			if (digits == 0 || (segment == 0 && digits < 4 && digits > 7)) {
				return false;
			}
			segments[segment] = int32_t(current);
			segment++;
			current = 0;
			digits = 0;
			continue;
		}
		if (segment == 2 && (c == ' ' || c == 'T')) {
			break;
		}
		if (c < '0' || c > '9') {
			return false;
		}
		current = current * 10 + (c - '0');
		digits++;
		if (digits > 7) {
			return false;
		}
	}
	if (digits == 0) {
		return false;
	}
	segments[segment] = int32_t(current);
	if (segment == 0 && (digits < 4 || digits > 7)) {
		return false;
	}
	if (segment > 0 && digits > 2) {
		return false;
	}
	int32_t year = negative ? -segments[0] : segments[0];
	if (!Date::IsValid(year, segments[1], segments[2])) {
		return false;
	}
	result = Date::FromDate(year, segments[1], segments[2]);
	return true;
}

void StringToDateKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<string_t, date_t>(args.data[0], result, args.size(), [](string_t s) -> optional<date_t> {
		date_t d;
		if (!ParseSparkDate(s, d)) {
			return optional<date_t>();
		}
		return d;
	});
}

//! Spark's string -> timestamp: DuckDB's parser first, then Spark's lenient forms that DuckDB rejects:
//! 'yyyy', 'yyyy-[m]m', 'yyyy-[m]m-[d]d' with an optional ' ' / 'T' and time part
bool ParseSparkTimestamp(const string &trimmed, timestamp_t &out) {
	if (trimmed.empty()) {
		return false;
	}
	{
		// a zone designator (`Z`, `+05:30`) is applied: the result is then the instant as a naive UTC timestamp, which
		// the cast's bound expression converts to the session zone (see StringZoneKind); a zone name is left unapplied
		bool has_offset = false;
		string_t tz("", 0);
		if (Timestamp::TryConvertTimestampTZ(trimmed.c_str(), trimmed.size(), out, true, has_offset, tz) ==
		    TimestampCastResult::SUCCESS) {
			return true;
		}
	}
	auto sep = trimmed.find_first_of(" T");
	auto date_part = sep == string::npos ? trimmed : trimmed.substr(0, sep);
	date_t date;
	if (!ParseSparkDate(string_t(date_part.c_str(), UnsafeNumericCast<uint32_t>(date_part.size())), date)) {
		return false;
	}
	dtime_t time(0);
	if (sep != string::npos && sep + 1 < trimmed.size()) {
		auto time_part = trimmed.substr(sep + 1);
		idx_t pos = 0;
		if (!Time::TryConvertTime(time_part.c_str(), time_part.size(), pos, time, false) || pos != time_part.size()) {
			return false;
		}
	}
	return Timestamp::TryFromDatetime(date, time, out);
}

//! 0: no zone designator in the string, 1: an offset (`Z`, `+05:30`; the parsed value is then a UTC instant),
//! 2: a zone name (`America/New_York`; the parsed value is the wall clock time in that zone)
int32_t StringZoneKind(const string &trimmed, string &name) {
	bool has_offset = false;
	string_t tz("", 0);
	timestamp_t ts;
	if (Timestamp::TryConvertTimestampTZ(trimmed.c_str(), trimmed.size(), ts, true, has_offset, tz) !=
	    TimestampCastResult::SUCCESS) {
		return 0;
	}
	if (tz.GetSize() > 0) {
		name = tz.GetString();
		return 2;
	}
	return has_offset ? 1 : 0;
}

void StringZoneKindKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<string_t, int32_t>(args.data[0], result, args.size(), [](string_t s) {
		auto str = s.GetString();
		StringUtil::Trim(str);
		string name;
		return StringZoneKind(str, name);
	});
}

void StringZoneNameKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t s) {
		auto str = s.GetString();
		StringUtil::Trim(str);
		string name;
		StringZoneKind(str, name);
		return StringVector::AddString(result, name);
	});
}

void StringToTimestampKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<string_t, timestamp_t>(args.data[0], result, args.size(),
	                                              [](string_t s) -> optional<timestamp_t> {
		                                              timestamp_t ts;
		                                              auto str = s.GetString();
		                                              StringUtil::Trim(str);
		                                              if (!ParseSparkTimestamp(str, ts)) {
			                                              return optional<timestamp_t>();
		                                              }
		                                              return ts;
	                                              });
}

void StringToBlobKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t s) {
		return StringVector::AddStringOrBlob(result, s);
	});
}

//! CAST(<integral> AS BINARY): the big-endian two's complement bytes of the value (1, 2, 4 or 8 bytes)
template <class T>
void IntegralToBlobKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<T, string_t>(args.data[0], result, args.size(), [&](T v) {
		auto u = typename std::make_unsigned<T>::type(v);
		char bytes[sizeof(T)];
		for (idx_t i = 0; i < sizeof(T); i++) {
			bytes[sizeof(T) - 1 - i] = char(u & 0xFF);
			u = decltype(u)(u >> 8);
		}
		return StringVector::AddStringOrBlob(result, string_t(bytes, uint32_t(sizeof(T))));
	});
}

ScalarFunction IntegralToBlobFunction(const LogicalType &source) {
	scalar_function_t fn;
	switch (source.id()) {
	case LogicalTypeId::TINYINT:
		fn = IntegralToBlobKernel<int8_t>;
		break;
	case LogicalTypeId::SMALLINT:
		fn = IntegralToBlobKernel<int16_t>;
		break;
	case LogicalTypeId::INTEGER:
		fn = IntegralToBlobKernel<int32_t>;
		break;
	default:
		fn = IntegralToBlobKernel<int64_t>;
		break;
	}
	return ScalarFunction(Identifier("spark_integral_to_binary"), {source}, LogicalType::BLOB, fn);
}

//! The low 64 bits of an integer, like Java's BigDecimal.longValue (Spark's Decimal.toLong)
void HugeintLowBitsKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<hugeint_t, int64_t>(args.data[0], result, args.size(),
	                                           [](hugeint_t h) { return int64_t(h.lower); });
}

//===----------------------------------------------------------------------===//
// Spark value -> string (Cast to STRING)
//===----------------------------------------------------------------------===//

//! Java's Double.toString / Float.toString into `out` (at least 48 bytes); returns the length
template <class T>
idx_t JavaFloatingToChars(T value, char *out) {
	auto put = [&](const char *text) {
		idx_t n = idx_t(strlen(text));
		memcpy(out, text, n);
		return n;
	};
	if (std::isnan(value)) {
		return put("NaN");
	}
	if (std::isinf(value)) {
		return put(value > 0 ? "Infinity" : "-Infinity");
	}
	if (value == 0) {
		return put(std::signbit(value) ? "-0.0" : "0.0");
	}
	char buffer[64];
	auto res = std::to_chars(buffer, buffer + sizeof(buffer), value, std::chars_format::scientific);
	// buffer: [-]d[.ddd]e[+-]xx
	const char *p = buffer;
	bool negative = *p == '-';
	if (negative) {
		p++;
	}
	char digits[32];
	idx_t nd = 0;
	for (; p < res.ptr && *p != 'e'; p++) {
		if (*p != '.') {
			digits[nd++] = *p;
		}
	}
	p++;
	bool exp_negative = *p == '-';
	if (*p == '-' || *p == '+') {
		p++;
	}
	int exponent = 0;
	for (; p < res.ptr; p++) {
		exponent = exponent * 10 + (*p - '0');
	}
	if (exp_negative) {
		exponent = -exponent;
	}
	idx_t n = 0;
	if (negative) {
		out[n++] = '-';
	}
	double magnitude = std::fabs(double(value));
	if (magnitude >= 1e-3 && magnitude < 1e7) {
		if (exponent >= 0) {
			idx_t int_digits = idx_t(exponent) + 1;
			for (idx_t i = 0; i < int_digits; i++) {
				out[n++] = i < nd ? digits[i] : '0';
			}
			out[n++] = '.';
			if (nd > int_digits) {
				memcpy(out + n, digits + int_digits, nd - int_digits);
				n += nd - int_digits;
			} else {
				out[n++] = '0';
			}
		} else {
			out[n++] = '0';
			out[n++] = '.';
			for (int i = 0; i < -exponent - 1; i++) {
				out[n++] = '0';
			}
			memcpy(out + n, digits, nd);
			n += nd;
		}
	} else {
		out[n++] = digits[0];
		out[n++] = '.';
		if (nd > 1) {
			memcpy(out + n, digits + 1, nd - 1);
			n += nd - 1;
		} else {
			out[n++] = '0';
		}
		out[n++] = 'E';
		auto exp_text = std::to_chars(out + n, out + n + 8, exponent);
		n = idx_t(exp_text.ptr - out);
	}
	return n;
}

template <class T>
string JavaFloatingToString(T value) {
	char buffer[64];
	return string(buffer, JavaFloatingToChars(value, buffer));
}

string SparkValueToString(const Value &value, bool nested);

} // namespace

//! the Spark string of a scalar value (used by to_json)
string SparkScalarString(const Value &value) {
	return SparkValueToString(value, true);
}

namespace {

//! Bytes as UTF-8 text, each malformed sequence replaced by U+FFFD (like Java's new String(bytes, UTF_8))
string Utf8Lossy(const string &bytes) {
	string out;
	auto data = reinterpret_cast<const utf8proc_uint8_t *>(bytes.data());
	utf8proc_ssize_t pos = 0, len = utf8proc_ssize_t(bytes.size());
	while (pos < len) {
		utf8proc_int32_t cp;
		auto n = utf8proc_iterate(data + pos, len - pos, &cp);
		if (n <= 0) {
			out += "\xEF\xBF\xBD";
			pos++;
			continue;
		}
		out.append(bytes, size_t(pos), size_t(n));
		pos += n;
	}
	return out;
}

string SparkValueToString(const Value &value, bool nested) {
	if (value.IsNull()) {
		return "null";
	}
	auto &type = value.type();
	switch (type.id()) {
	case LogicalTypeId::DOUBLE:
		return JavaFloatingToString(value.GetValue<double>());
	case LogicalTypeId::FLOAT:
		return JavaFloatingToString(value.GetValue<float>());
	case LogicalTypeId::BOOLEAN:
		return value.GetValue<bool>() ? "true" : "false";
	case LogicalTypeId::BLOB:
		return Utf8Lossy(StringValue::Get(value));
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY: {
		auto &children = ListValue::GetChildren(value);
		string result = "[";
		for (idx_t i = 0; i < children.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += SparkValueToString(children[i], true);
		}
		return result + "]";
	}
	case LogicalTypeId::MAP: {
		auto &children = ListValue::GetChildren(value);
		string result = "{";
		for (idx_t i = 0; i < children.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			auto &kv = StructValue::GetChildren(children[i]);
			result += SparkValueToString(kv[0], true) + " -> " + SparkValueToString(kv[1], true);
		}
		return result + "}";
	}
	case LogicalTypeId::STRUCT: {
		auto &children = StructValue::GetChildren(value);
		string result = "{";
		for (idx_t i = 0; i < children.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += SparkValueToString(children[i], true);
		}
		return result + "}";
	}
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ: {
		auto ts = value.GetValue<timestamp_t>();
		date_t date;
		dtime_t time;
		Timestamp::Convert(ts, date, time);
		int32_t hour, minute, second, micros;
		Time::Convert(time, hour, minute, second, micros);
		auto result = Date::ToString(date) + StringUtil::Format(" %02d:%02d:%02d", hour, minute, second);
		if (micros > 0) {
			auto frac = StringUtil::Format("%06d", micros);
			while (!frac.empty() && frac.back() == '0') {
				frac.pop_back();
			}
			result += "." + frac;
		}
		return result;
	}
	default:
		return value.ToString();
	}
}

void ToStringKernel(DataChunk &args, ExpressionState &, Vector &result) {
	auto &input = args.data[0];
	auto count = args.size();
	if (input.GetType().id() == LogicalTypeId::DOUBLE) {
		UnaryExecutor::Execute<double, string_t>(input, result, count, [&](double d) {
			char buffer[64];
			auto n = JavaFloatingToChars(d, buffer);
			return StringVector::AddString(result, buffer, n);
		});
		return;
	}
	if (input.GetType().id() == LogicalTypeId::FLOAT) {
		UnaryExecutor::Execute<float, string_t>(input, result, count, [&](float d) {
			char buffer[64];
			auto n = JavaFloatingToChars(d, buffer);
			return StringVector::AddString(result, buffer, n);
		});
		return;
	}
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto data = FlatVector::GetDataMutable<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		auto value = input.GetValue(i);
		if (value.IsNull()) {
			FlatVector::ValidityMutable(result).SetInvalid(i);
			continue;
		}
		data[i] = StringVector::AddString(result, SparkValueToString(value, false));
	}
	if (count == 1 && input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ExprP ToSparkString(Builder &b, ExprP e) {
	auto type = Normalize(e->GetReturnType());
	switch (type.id()) {
	case LogicalTypeId::VARCHAR:
		return e;
	case LogicalTypeId::SQLNULL:
		return b.NullOf(LogicalType::VARCHAR);
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DATE:
		return b.Cast(std::move(e), LogicalType::VARCHAR);
	default:
		break;
	}
	return b.Kernel(ScalarFunction(Identifier("spark_to_string"), {type}, LogicalType::VARCHAR, ToStringKernel),
	                std::move(e));
}

//===----------------------------------------------------------------------===//
// CAST
//===----------------------------------------------------------------------===//
//! The wall clock TIMESTAMP of the Spark TIMESTAMP type as an instant in the session time zone (unchanged under UTC)
ExprP WallToInstant(Builder &b, ExprP e) {
	Value zone;
	if (b.context.TryGetCurrentSetting(Identifier("TimeZone"), zone) && !zone.IsNull()) {
		auto name = zone.ToString();
		if (name != "UTC" && name != "Etc/UTC" && name != "GMT") {
			return b.Call("timezone", b.Constant(Value(name)), std::move(e));
		}
	}
	return e;
}

bool SessionZoneIsUtc(Builder &b) {
	Value zone;
	if (b.context.TryGetCurrentSetting(Identifier("TimeZone"), zone) && !zone.IsNull()) {
		auto name = zone.ToString();
		return name == "UTC" || name == "Etc/UTC" || name == "GMT";
	}
	return true;
}

ExprP SparkCast(Builder &b, ExprP e, const LogicalType &target) {
	auto source = Normalize(e->GetReturnType());
	if (source == target) {
		return e;
	}
	if (IsNull(source)) {
		return b.NullOf(target);
	}
	if (IsString(target)) {
		return ToSparkString(b, std::move(e));
	}
	if (IsIntegral(target)) {
		if (IsString(source)) {
			return b.Kernel(StringToIntegralFunction(target), std::move(e));
		}
		if (IsFloating(source)) {
			return b.Kernel(DoubleToIntegralFunction(target), b.Cast(std::move(e), LogicalType::DOUBLE));
		}
		if (IsIntegral(source)) {
			if (IntegralRank(source) <= IntegralRank(target)) {
				return b.Cast(std::move(e), target);
			}
			return b.Kernel(NarrowFunction(source, target), std::move(e));
		}
		if (IsDecimal(source)) {
			// Spark's Decimal.toLong keeps the low 64 bits of the integer part; narrower types wrap from there
			auto whole = b.Cast(b.Call("trunc", std::move(e)), LogicalType::HUGEINT);
			auto as_long = b.Kernel(ScalarFunction(Identifier("spark_low_bits"), {LogicalType::HUGEINT},
			                                       LogicalType::BIGINT, HugeintLowBitsKernel),
			                        std::move(whole));
			if (target.id() == LogicalTypeId::BIGINT) {
				return as_long;
			}
			return b.Kernel(NarrowFunction(LogicalType::BIGINT, target), std::move(as_long));
		}
		if (IsDecimal(source) || IsLargeIntegral(source)) {
			auto truncated = IsDecimal(source) ? b.Call("trunc", std::move(e)) : std::move(e);
			auto as_long = b.Cast(std::move(truncated), LogicalType::BIGINT, true);
			if (target.id() == LogicalTypeId::BIGINT) {
				return as_long;
			}
			return b.Kernel(NarrowFunction(LogicalType::BIGINT, target), std::move(as_long));
		}
		if (source.id() == LogicalTypeId::TIMESTAMP || source.id() == LogicalTypeId::TIMESTAMP_TZ) {
			if (source.id() == LogicalTypeId::TIMESTAMP) {
				e = WallToInstant(b, std::move(e));
			}
			auto seconds = b.Call("floor", b.Call("epoch", std::move(e)));
			auto as_long = b.Cast(std::move(seconds), LogicalType::BIGINT, true);
			if (target.id() == LogicalTypeId::BIGINT) {
				return as_long;
			}
			return b.Kernel(NarrowFunction(LogicalType::BIGINT, target), std::move(as_long));
		}
		if (source.id() == LogicalTypeId::DATE) {
			return b.NullOf(target);
		}
	}
	if (IsFloating(target)) {
		if (IsString(source)) {
			return b.Cast(b.Call("trim", std::move(e)), target, true);
		}
		if (source.id() == LogicalTypeId::TIMESTAMP || source.id() == LogicalTypeId::TIMESTAMP_TZ) {
			if (source.id() == LogicalTypeId::TIMESTAMP) {
				e = WallToInstant(b, std::move(e));
			}
			return b.Cast(b.Call("epoch", std::move(e)), target, true);
		}
		if (source.id() == LogicalTypeId::DATE) {
			return b.NullOf(target);
		}
	}
	if (IsDecimal(target)) {
		if (IsString(source)) {
			auto trimmed = b.Call("trim", std::move(e));
			vector<ExprP> options;
			options.push_back(b.Cast(trimmed->Copy(), target, true));
			options.push_back(b.Cast(b.Cast(std::move(trimmed), LogicalType::DOUBLE, true), target, true));
			return b.Coalesce(std::move(options));
		}
		if (source.id() == LogicalTypeId::DATE) {
			return b.NullOf(target);
		}
		if (source.id() == LogicalTypeId::TIMESTAMP || source.id() == LogicalTypeId::TIMESTAMP_TZ) {
			if (source.id() == LogicalTypeId::TIMESTAMP) {
				e = WallToInstant(b, std::move(e));
			}
			return b.Cast(b.Call("epoch", std::move(e)), target, true);
		}
	}
	if (target.id() == LogicalTypeId::BOOLEAN) {
		if (IsString(source)) {
			return b.Kernel(ScalarFunction(Identifier("spark_string_to_boolean"), {LogicalType::VARCHAR},
			                               LogicalType::BOOLEAN, StringToBooleanKernel),
			                std::move(e));
		}
		if (source.id() == LogicalTypeId::DATE) {
			return b.NullOf(target);
		}
	}
	if (target.id() == LogicalTypeId::DATE) {
		if (IsString(source)) {
			return b.Kernel(ScalarFunction(Identifier("spark_string_to_date"), {LogicalType::VARCHAR},
			                               LogicalType::DATE, StringToDateKernel),
			                std::move(e));
		}
		if (IsNumeric(source) || source.id() == LogicalTypeId::BOOLEAN) {
			return b.NullOf(target);
		}
	}
	if (target.id() == LogicalTypeId::TIMESTAMP) {
		if (IsString(source)) {
			auto wall = [&](ExprP arg) {
				return b.Kernel(ScalarFunction(Identifier("spark_string_to_timestamp"), {LogicalType::VARCHAR},
				                               LogicalType::TIMESTAMP, StringToTimestampKernel),
				                std::move(arg));
			};
			// a zone designator in the string (offset or zone name) is applied: the result is the wall clock time of
			// the instant in the session time zone
			auto kind = b.Kernel(ScalarFunction(Identifier("spark_string_zone_kind"), {LogicalType::VARCHAR},
			                                    LogicalType::INTEGER, StringZoneKindKernel),
			                     e->Copy());
			auto name = b.Kernel(ScalarFunction(Identifier("spark_string_zone_name"), {LogicalType::VARCHAR},
			                                    LogicalType::VARCHAR, StringZoneNameKernel),
			                     e->Copy());
			auto offset_case = b.Cast(b.Call("timezone", b.Constant(Value("UTC")), wall(e->Copy())), LogicalType::TIMESTAMP);
			auto named_case = b.Cast(b.Call("timezone", std::move(name), wall(e->Copy())), LogicalType::TIMESTAMP, true);
			auto is_kind = [&](int32_t k) {
				return b.Compare(ExpressionType::COMPARE_EQUAL, kind->Copy(), b.Constant(Value::INTEGER(k)));
			};
			return b.Case(is_kind(0), wall(e->Copy()),
			              b.Case(is_kind(1), std::move(offset_case), std::move(named_case)));
		}
		if (IsIntegral(source) && SessionZoneIsUtc(b)) {
			// seconds since the epoch as the wall clock of a UTC session; the TIMESTAMPTZ path below depends on the
			// ICU calendar setting
			auto micros = b.Op("*", b.Cast(std::move(e), LogicalType::HUGEINT), b.Constant(Value::HUGEINT(1000000)));
			return b.Call("make_timestamp", b.Cast(std::move(micros), LogicalType::BIGINT, true));
		}
		if (IsNumeric(source)) {
			auto tz = b.Call("to_timestamp", b.Cast(std::move(e), LogicalType::DOUBLE));
			return b.Cast(std::move(tz), target, true);
		}
		if (source.id() == LogicalTypeId::BOOLEAN) {
			// true is one microsecond after the epoch, false the epoch itself
			auto micros = b.Case(e->Copy(), b.Constant(Value::BIGINT(1)), b.Constant(Value::BIGINT(0)));
			auto ts = b.Call("make_timestamp", std::move(micros));
			return b.Case(b.IsNull(std::move(e)), b.NullOf(target), std::move(ts));
		}
	}
	if (target.id() == LogicalTypeId::BLOB && IsIntegral(source)) {
		return b.Kernel(IntegralToBlobFunction(source), std::move(e));
	}
	if (target.id() == LogicalTypeId::BLOB && IsString(source)) {
		return b.Kernel(
		    ScalarFunction(Identifier("spark_string_to_binary"), {LogicalType::VARCHAR}, LogicalType::BLOB, StringToBlobKernel),
		    std::move(e));
	}
	if (source.id() == LogicalTypeId::STRUCT && target.id() == LogicalTypeId::STRUCT &&
	    StructType::GetChildCount(source) == StructType::GetChildCount(target)) {
		// Spark casts structs by position: build the target struct field by field
		auto &source_fields = StructType::GetChildTypes(source);
		auto &target_fields = StructType::GetChildTypes(target);
		vector<ExprP> fields;
		for (idx_t i = 0; i < source_fields.size(); i++) {
			auto field = b.Call("struct_extract", e->Copy(), b.Constant(Value(source_fields[i].first.GetIdentifierName())));
			fields.push_back(SparkCast(b, std::move(field), target_fields[i].second));
		}
		auto packed = b.Cast(b.Call("row", std::move(fields)), target);
		return b.Case(b.IsNull(std::move(e)), b.NullOf(target), std::move(packed));
	}
	return b.Cast(std::move(e), target, true);
}

//! typed_literal(CAST(n AS BIGINT)): the value of a typed integral literal (1L, 1S, 1Y) as a constant
ExprP BindTypedLiteral(FunctionBindExpressionInput &input) {
	CheckArgs(input, "typed_literal", 1, 1);
	auto e = Arg(input, 0);
	if (!e->IsFoldable()) {
		return e;
	}
	// a cast of the number as a constant: what DecimalOperandType recognizes as a literal (a CAST the user wrote is
	// not one, see BindCastValue); kept as a cast node so VALUES lists type it like before
	auto target = e->GetReturnType();
	auto value = ExpressionExecutor::EvaluateScalar(input.context, *e);
	auto number = make_uniq<BoundConstantExpression>(value.IsNull() ? Value(LogicalType::BIGINT)
	                                                                : value.DefaultCastAs(LogicalType::BIGINT));
	return BoundCastExpression::AddCastToType(input.context, std::move(number), target);
}

void IdentityKernel(DataChunk &args, ExpressionState &, Vector &result) {
	result.Reference(args.data[0]);
}

//===----------------------------------------------------------------------===//
// spark_strict: argument types Spark's analyzer rejects (generated from Spark's answers, spark_arg_types.inc)
//===----------------------------------------------------------------------===//
namespace ArgKind {
enum : uint32_t {
	NULL_ = 1u << 0, BOOLEAN = 1u << 1, TINYINT = 1u << 2, SMALLINT = 1u << 3, INTEGER = 1u << 4, BIGINT = 1u << 5,
	FLOAT = 1u << 6, DOUBLE = 1u << 7, DECIMAL = 1u << 8, STRING = 1u << 9, BINARY = 1u << 10, DATE = 1u << 11,
	TIMESTAMP = 1u << 12, INTERVAL = 1u << 13, LIST = 1u << 14, MAP = 1u << 15, STRUCT = 1u << 16
};
} // namespace ArgKind
#define NULL NULL_
struct ArgTypeRule {
	const char *function;
	idx_t position;
	uint32_t rejected;
};
const ArgTypeRule ARG_TYPE_RULES[] = {
#include "spark_arg_types.inc"
};
#undef NULL

const unordered_map<string, uint32_t> &ArgTypeRules() {
	static const auto rules = [] {
		unordered_map<string, uint32_t> map;
		for (auto &rule : ARG_TYPE_RULES) {
			map[string(rule.function) + "/" + to_string(rule.position)] = rule.rejected;
		}
		return map;
	}();
	return rules;
}

uint32_t ArgKindOf(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::SQLNULL:
		return ArgKind::NULL_;
	case LogicalTypeId::BOOLEAN:
		return ArgKind::BOOLEAN;
	case LogicalTypeId::TINYINT:
		return ArgKind::TINYINT;
	case LogicalTypeId::SMALLINT:
		return ArgKind::SMALLINT;
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::INTEGER_LITERAL:
		return ArgKind::INTEGER;
	case LogicalTypeId::BIGINT:
		return ArgKind::BIGINT;
	case LogicalTypeId::FLOAT:
		return ArgKind::FLOAT;
	case LogicalTypeId::DOUBLE:
		return ArgKind::DOUBLE;
	case LogicalTypeId::DECIMAL:
		return ArgKind::DECIMAL;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::STRING_LITERAL:
		return ArgKind::STRING;
	case LogicalTypeId::BLOB:
		return ArgKind::BINARY;
	case LogicalTypeId::DATE:
		return ArgKind::DATE;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return ArgKind::TIMESTAMP;
	case LogicalTypeId::INTERVAL:
		return ArgKind::INTERVAL;
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY:
		return ArgKind::LIST;
	case LogicalTypeId::MAP:
		return ArgKind::MAP;
	case LogicalTypeId::STRUCT:
		return ArgKind::STRUCT;
	default:
		return 0;
	}
}

bool HasArgTypeRuleImpl(const string &lower_name, idx_t position) {
	return ArgTypeRules().count(lower_name + "/" + to_string(position)) > 0;
}

//! check_arg(value, 'function', position): the value unchanged, or Spark's analysis error when Spark rejects an
//! argument of its type at that position (spark_strict only wraps arguments that have a rule)
ExprP BindCheckArg(FunctionBindExpressionInput &input) {
	CheckArgs(input, "check_arg", 3, 3);
	auto function = ExpressionExecutor::EvaluateScalar(input.context, *input.children[1]).ToString();
	auto position = ExpressionExecutor::EvaluateScalar(input.context, *input.children[2]).GetValue<int64_t>();
	auto entry = ArgTypeRules().find(function + "/" + to_string(position));
	auto type = input.children[0]->GetReturnType();
	if (entry != ArgTypeRules().end() && (entry->second & ArgKindOf(type))) {
		throw BinderException("[DATATYPE_MISMATCH] Cannot resolve \"%s\" due to data type mismatch: Parameter %lld "
		                      "does not accept the \"%s\" type.",
		                      function, (long long)position + 1, StringUtil::Upper(SparkTypeName(type)));
	}
	return std::move(input.children[0]);
}

//! The spark_strict dialect is selected
bool InStrictDialect(ClientContext &context) {
	auto &dialect = ClientConfig::GetConfig(context).current_dialect;
	return dialect.has_value() && *dialect == "spark_strict";
}

//! Spark 3.5's Cast.canCast (ansi = false) and Cast.canAnsiCast (ansi = true, also TRY_CAST) over the DuckDB types
//! the dialect maps Spark's types to. TIMESTAMP_NTZ and the interval kinds are not distinguishable here: they are
//! checked as TIMESTAMP and as an interval of the matching kind.
bool SparkCanCast(const LogicalType &source_p, const LogicalType &target_p, bool ansi) {
	auto source = Normalize(source_p), target = Normalize(target_p);
	if (IsNull(source) || source == target || IsString(target)) {
		return true;
	}
	auto sid = source.id(), tid = target.id();
	bool s_string = IsString(source), s_bool = sid == LogicalTypeId::BOOLEAN, s_date = sid == LogicalTypeId::DATE;
	bool s_ts = sid == LogicalTypeId::TIMESTAMP || sid == LogicalTypeId::TIMESTAMP_TZ;
	bool s_interval = sid == LogicalTypeId::INTERVAL, s_binary = sid == LogicalTypeId::BLOB;
	bool s_integral = IsIntegral(source), s_numeric = IsNumeric(source);
	auto nested = [](LogicalTypeId id) {
		return id == LogicalTypeId::LIST || id == LogicalTypeId::ARRAY || id == LogicalTypeId::MAP ||
		       id == LogicalTypeId::STRUCT;
	};
	bool s_known = nested(sid) || IsString(source) || sid == LogicalTypeId::BOOLEAN || sid == LogicalTypeId::DATE ||
	               sid == LogicalTypeId::TIMESTAMP || sid == LogicalTypeId::TIMESTAMP_TZ ||
	               sid == LogicalTypeId::INTERVAL || sid == LogicalTypeId::BLOB || IsNumeric(source);
	if (!s_known) {
		return true; // a type the dialect does not map from Spark (rejected elsewhere in spark_strict, if at all)
	}
	if (nested(tid) || nested(sid)) {
		if ((tid == LogicalTypeId::LIST || tid == LogicalTypeId::ARRAY) &&
		    (sid == LogicalTypeId::LIST || sid == LogicalTypeId::ARRAY)) {
			auto child = [](const LogicalType &t) {
				return t.id() == LogicalTypeId::LIST ? ListType::GetChildType(t) : ArrayType::GetChildType(t);
			};
			return SparkCanCast(child(source), child(target), ansi);
		}
		if (tid == LogicalTypeId::MAP && sid == LogicalTypeId::MAP) {
			return SparkCanCast(MapType::KeyType(source), MapType::KeyType(target), ansi) &&
			       SparkCanCast(MapType::ValueType(source), MapType::ValueType(target), ansi);
		}
		if (tid == LogicalTypeId::STRUCT && sid == LogicalTypeId::STRUCT) {
			if (StructType::GetChildCount(source) != StructType::GetChildCount(target)) {
				return false;
			}
			for (idx_t i = 0; i < StructType::GetChildCount(source); i++) {
				if (!SparkCanCast(StructType::GetChildType(source, i), StructType::GetChildType(target, i), ansi)) {
					return false;
				}
			}
			return true;
		}
		return false;
	}
	if (!(s_string || s_bool || s_date || s_ts || s_interval || s_binary || s_numeric)) {
		return true; // a DuckDB-only type: rejected elsewhere in spark_strict
	}
	switch (tid) {
	case LogicalTypeId::BLOB:
		return s_string || (s_integral && !ansi);
	case LogicalTypeId::BOOLEAN:
		return s_string || s_numeric || (!ansi && (s_date || s_ts));
	case LogicalTypeId::DATE:
		return s_string || s_ts;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return s_string || s_date || s_numeric || (!ansi && s_bool);
	case LogicalTypeId::INTERVAL:
		return s_string || s_interval || s_integral || IsDecimal(source);
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return s_string || s_numeric || s_bool || s_ts || (!ansi && s_date);
	default:
		if (IsIntegral(target) || IsDecimal(target)) {
			return s_string || s_numeric || s_bool || s_ts || s_interval || (!ansi && s_date);
		}
		return true;
	}
}

//! spark_strict: Spark's analysis error for a cast its analyzer refuses
void CheckSparkCast(ClientContext &context, const LogicalType &source, const LogicalType &target, bool ansi) {
	if (InStrictDialect(context) && !SparkCanCast(source, target, ansi)) {
		throw BinderException("[DATATYPE_MISMATCH] Cannot resolve the cast due to data type mismatch: cannot cast "
		                      "\"%s\" to \"%s\".",
		                      StringUtil::Upper(SparkTypeName(source)), StringUtil::Upper(SparkTypeName(target)));
	}
}

ExprP BindCastValueInner(FunctionBindExpressionInput &input) {
	CheckArgs(input, "cast", 2, 2);
	Builder b(input.context);
	auto target = input.children[1]->GetReturnType();
	auto e = Arg(input, 0);
	auto source = Normalize(e->GetReturnType());
	CheckSparkCast(input.context, source, target, AnsiEnabled(input.context));
	if (AnsiEnabled(input.context) && source != target && !IsNull(source) &&
	    (IsIntegral(target) || IsFloating(target) || IsDecimal(target) || target.id() == LogicalTypeId::BOOLEAN ||
	     target.id() == LogicalTypeId::DATE || target.id() == LogicalTypeId::TIMESTAMP)) {
		if (IsIntegral(target) && IsString(source)) {
			// a string must be an integer: "1.5" is malformed
			auto copy = e->Copy();
			vector<ExprP> inputs;
			inputs.push_back(std::move(copy));
			auto parsed = b.Kernel(StringToIntegralFunction(target, true), std::move(e));
			return AnsiNullCheck(b, std::move(parsed), std::move(inputs));
		}
		if (IsIntegral(target) && (IsIntegral(source) || IsFloating(source) || IsDecimal(source))) {
			// overflow (and NaN) is an error; fractions are dropped
			if (!IsIntegral(source)) {
				e = b.Call("trunc", b.Cast(std::move(e), IsDecimal(source) ? source : LogicalType::DOUBLE));
			}
			return b.Cast(std::move(e), target);
		}
		auto copy = e->Copy();
		auto result = SparkCast(b, std::move(e), target);
		vector<ExprP> inputs;
		inputs.push_back(std::move(copy));
		return AnsiNullCheck(b, std::move(result), std::move(inputs));
	}
	return SparkCast(b, std::move(e), target);
}

ExprP BindCastValue(FunctionBindExpressionInput &input) {
	auto target = input.children[1]->GetReturnType();
	auto result = BindCastValueInner(input);
	if (result->IsFoldable() && IsIntegral(target)) {
		// a cast is not a literal for Spark's decimal precision rules (CAST(1 AS INT) counts as DECIMAL(10,0)):
		// keep it from looking like one; the optimizer folds it later
		Builder b(input.context);
		return b.Kernel(ScalarFunction(Identifier("spark_cast_result"), {target}, target, IdentityKernel),
		                std::move(result));
	}
	return result;
}

//===----------------------------------------------------------------------===//
// Arithmetic
//===----------------------------------------------------------------------===//
//! Spark's implicit cast of a string operand in arithmetic: DOUBLE
ExprP NumericOperand(Builder &b, ExprP e) {
	auto type = Normalize(e->GetReturnType());
	if (IsString(type)) {
		return b.Cast(b.Call("trim", std::move(e)), LogicalType::DOUBLE, true);
	}
	if (IsNull(type)) {
		return b.Cast(std::move(e), LogicalType::INTEGER);
	}
	return e;
}

//! Spark's implicit cast of a value to a common type: Spark's CAST semantics, and under ANSI a string that does not
//! convert is CAST_INVALID_INPUT when it is evaluated (an untaken CASE branch does not fail)
ExprP SparkImplicitCast(Builder &b, ExprP e, const LogicalType &target) {
	auto source = Normalize(e->GetReturnType());
	if (AnsiEnabled(b.context) && IsString(source) && !IsString(target) && !IsNull(target)) {
		auto copy = e->Copy();
		vector<ExprP> inputs;
		inputs.push_back(std::move(copy));
		return AnsiNullCheck(b, SparkCast(b, std::move(e), target), std::move(inputs));
	}
	return SparkCast(b, std::move(e), target);
}

//! The operands of a binary arithmetic operator: strings convert like Spark's implicit cast (non-ANSI: DOUBLE; ANSI:
//! BIGINT next to an integer, DOUBLE next to a fraction, and two strings are an analysis error)
void NumericOperands(Builder &b, ExprP &left, ExprP &right, const char *op) {
	auto lt = Normalize(left->GetReturnType()), rt = Normalize(right->GetReturnType());
	if (AnsiEnabled(b.context) && (IsString(lt) || IsString(rt))) {
		if (IsString(lt) && IsString(rt)) {
			throw BinderException("[DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE] Cannot resolve \"%s\" due to data type "
			                      "mismatch: the binary operator requires numeric or interval operands, not two "
			                      "strings.",
			                      op);
		}
		auto &other = IsString(lt) ? rt : lt;
		if (IsNumeric(other)) {
			auto target = IsIntegral(other) ? LogicalType::BIGINT : LogicalType::DOUBLE;
			auto &operand = IsString(lt) ? left : right;
			operand = SparkImplicitCast(b, std::move(operand), target);
			return;
		}
	}
	left = NumericOperand(b, std::move(left));
	right = NumericOperand(b, std::move(right));
}

//! Spark's DecimalType.adjustPrecisionScale (allowPrecisionLoss = true)
LogicalType AdjustDecimal(int precision, int scale, bool allow_precision_loss = true) {
	if (precision <= 38) {
		return LogicalType::DECIMAL(uint8_t(precision), uint8_t(scale));
	}
	if (!allow_precision_loss) {
		// DecimalType.bounded: keep the scale, cap the precision (overflow becomes NULL)
		return LogicalType::DECIMAL(38, uint8_t(MinValue(scale, 38)));
	}
	int int_digits = precision - scale;
	int min_scale = MinValue(scale, 6);
	return LogicalType::DECIMAL(38, uint8_t(MaxValue(38 - int_digits, min_scale)));
}

//! The decimal type Spark gives an operand: integer literals get the precision of their digits
LogicalType DecimalOperandType(const Expression &expr, const LogicalType &type) {
	if (IsDecimal(type)) {
		return type;
	}
	// a literal: a constant, or a typed literal (1L, 1S, 1Y: a cast of a constant, see BindTypedLiteral)
	auto *constant = &expr;
	if (BoundCastExpression::IsCast(expr)) {
		constant = &BoundCastExpression::Child(expr.Cast<BoundFunctionExpression>());
	}
	if (constant->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT && IsIntegral(type)) {
		auto &value = constant->Cast<BoundConstantExpression>().GetValue();
		if (!value.IsNull()) {
			auto text = to_string(std::abs(value.GetValue<int64_t>()));
			return LogicalType::DECIMAL(uint8_t(MaxValue<idx_t>(text.size(), 1)), 0);
		}
	}
	return AsDecimalType(type);
}

ExprP SparkDecimalResult(Builder &b, ExprP value, const LogicalType &target);
ExprP SparkWideDecimalMultiply(Builder &b, ExprP left, int s1, ExprP right, int s2, const LogicalType &target);
ExprP SparkExactDecimalMultiply(Builder &b, ExprP left, const LogicalType &lt, ExprP right, const LogicalType &rt,
                                const LogicalType &target);
ExprP SparkExactDecimalAddSub(Builder &b, ExprP left, const LogicalType &lt, ExprP right, const LogicalType &rt,
                              const LogicalType &target, bool subtract);

template <class OP, class CHECKED_OP, bool CHECKED>
ExprP BindAddSubMul(FunctionBindExpressionInput &input, const char *duck_op) {
	CheckArgs(input, duck_op, 2, 2);
	Builder b(input.context);
	auto left_decimal = DecimalOperandType(*input.children[0], ArgType(input, 0));
	auto right_decimal = DecimalOperandType(*input.children[1], ArgType(input, 1));
	if (IsNull(Normalize(input.children[0]->GetReturnType())) && IsNull(Normalize(input.children[1]->GetReturnType()))) {
		// NULL + NULL: both untyped operands are DOUBLE in Spark
		return b.NullOf(LogicalType::DOUBLE);
	}
	auto left = Arg(input, 0);
	auto right = Arg(input, 1);
	NumericOperands(b, left, right, duck_op);
	auto lt = Normalize(left->GetReturnType());
	auto rt = Normalize(right->GetReturnType());
	bool ansi = !CHECKED && AnsiEnabled(input.context);
	if (IsIntegral(lt) && IsIntegral(rt)) {
		auto type = WiderIntegral(lt, rt);
		if (ansi) {
			// DuckDB's own operators raise on overflow
			return b.Op(duck_op, b.Cast(std::move(left), type), b.Cast(std::move(right), type));
		}
		auto fn = CHECKED ? IntegralBinaryFunction<CHECKED_OP, CheckedKernel>(type)
		                  : IntegralBinaryFunction<OP, WrapKernel>(type);
		return b.Kernel(fn, b.Cast(std::move(left), type), b.Cast(std::move(right), type));
	}
	string op = duck_op;
	if (op != "*") {
		// date +/- days
		if (lt.id() == LogicalTypeId::DATE && IsIntegral(rt)) {
			return b.Op(op, std::move(left), b.Cast(std::move(right), LogicalType::INTEGER));
		}
		if (op == "+" && IsIntegral(lt) && rt.id() == LogicalTypeId::DATE) {
			return b.Op("+", std::move(right), b.Cast(std::move(left), LogicalType::INTEGER));
		}
		if (op == "-" && lt.id() == LogicalTypeId::DATE && rt.id() == LogicalTypeId::DATE) {
			// Spark: an interval of days
			return b.Call("to_days", b.Cast(b.Op("-", std::move(left), std::move(right)), LogicalType::INTEGER));
		}
		if (op == "+" && lt.id() == LogicalTypeId::DATE && rt.id() == LogicalTypeId::INTERVAL) {
			return b.Cast(b.Op("+", std::move(left), std::move(right)), LogicalType::DATE);
		}
		if (op == "-" && lt.id() == LogicalTypeId::DATE && rt.id() == LogicalTypeId::INTERVAL) {
			return b.Cast(b.Op("-", std::move(left), std::move(right)), LogicalType::DATE);
		}
	}
	bool float_with_integer = (lt.id() == LogicalTypeId::FLOAT && IsIntegral(rt)) ||
	                          (rt.id() == LogicalTypeId::FLOAT && IsIntegral(lt));
	if ((IsDecimal(lt) && IsFloating(rt)) || (IsFloating(lt) && IsDecimal(rt)) ||
	    (float_with_integer && AnsiEnabled(input.context))) {
		// Spark: decimal with FLOAT or DOUBLE is DOUBLE (DuckDB: FLOAT); under ANSI an integer with FLOAT too
		return b.Op(duck_op, b.Cast(std::move(left), LogicalType::DOUBLE), b.Cast(std::move(right), LogicalType::DOUBLE));
	}
	bool decimal_math = (IsDecimal(lt) || IsDecimal(rt)) && (IsDecimal(lt) || IsIntegral(lt) || IsLargeIntegral(lt)) &&
	                    (IsDecimal(rt) || IsIntegral(rt) || IsLargeIntegral(rt));
	if (decimal_math) {
		// Spark's DecimalPrecision rules; overflow of the result type is NULL (non-ANSI)
		auto ld = IsDecimal(lt) ? lt : left_decimal;
		auto rd = IsDecimal(rt) ? rt : right_decimal;
		int p1 = DecimalType::GetWidth(ld), s1 = DecimalType::GetScale(ld);
		int p2 = DecimalType::GetWidth(rd), s2 = DecimalType::GetScale(rd);
		int precision, scale;
		if (op == "*") {
			precision = p1 + p2 + 1;
			scale = s1 + s2;
		} else {
			scale = MaxValue(s1, s2);
			precision = MaxValue(p1 - s1, p2 - s2) + scale + 1;
		}
		auto result_type = AdjustDecimal(precision, scale, SparkConfBool(input.context, "spark.sql.decimalOperations.allowPrecisionLoss"));
		ExprP left_copy, right_copy;
		if (ansi) {
			left_copy = left->Copy();
			right_copy = right->Copy();
		}
		auto finish = [&](ExprP result) {
			if (!ansi) {
				return result;
			}
			// the overflow of the result type is an error, not NULL
			vector<ExprP> inputs;
			inputs.push_back(std::move(left_copy));
			inputs.push_back(std::move(right_copy));
			return AnsiNullCheck(b, std::move(result), std::move(inputs));
		};
		ExprP value;
		if (op == "*") {
			if (precision <= 38) {
				// the exact product fits Spark's result type: multiply the stored integers directly
				return SparkExactDecimalMultiply(b, std::move(left), ld, std::move(right), rd, result_type);
			}
			if (p1 + p2 <= 38) {
				auto wide_left = LogicalType::DECIMAL(uint8_t(MinValue(38, MaxValue(p1, 38 - p2))), uint8_t(s1));
				value = b.Op("*", b.Cast(std::move(left), wide_left), b.Cast(std::move(right), rd));
			} else {
				return finish(SparkWideDecimalMultiply(b, std::move(left), s1, std::move(right), s2, result_type));
			}
		} else {
			if (precision <= 38 && (op == "+" || op == "-")) {
				// the exact sum fits Spark's result type: align the scales and add the stored integers
				return SparkExactDecimalAddSub(b, std::move(left), ld, std::move(right), rd, result_type, op == "-");
			}
			auto common = LogicalType::DECIMAL(38, uint8_t(MaxValue(s1, s2)));
			value = b.Op(duck_op, b.Cast(std::move(left), common), b.Cast(std::move(right), common));
		}
		return finish(SparkDecimalResult(b, std::move(value), result_type));
	}
	return b.Op(duck_op, std::move(left), std::move(right));
}

//! Decimal result of Spark arithmetic: lower the scale with HALF_UP rounding and NULL on overflow. DuckDB's
//! TRY_CAST does the same but is ~500x slower for 38-digit decimals when the scale goes down.
template <class IN, class OUT>
void DecimalRescaleKernel(DataChunk &args, ExpressionState &, Vector &result, hugeint_t divisor, hugeint_t limit) {
	bool small_divisor = divisor < hugeint_t(NumericLimits<int64_t>::Maximum());
	int64_t divisor64 = small_divisor ? Hugeint::Cast<int64_t>(divisor) : 0;
	bool small_limit = limit < hugeint_t(NumericLimits<int64_t>::Maximum());
	int64_t limit64 = small_limit ? Hugeint::Cast<int64_t>(limit) : 0;
	UnaryExecutor::Execute<IN, OUT>(args.data[0], result, args.size(), [&](IN in) -> optional<OUT> {
		hugeint_t value(in);
		// fast path: the value fits in 63 bits (the common case) - 64-bit division instead of 128-bit
		if (small_divisor && value < hugeint_t(NumericLimits<int64_t>::Maximum()) &&
		    value > hugeint_t(NumericLimits<int64_t>::Minimum())) {
			int64_t v = Hugeint::Cast<int64_t>(value);
			int64_t q = divisor64 > 1 ? v / divisor64 : v;
			if (divisor64 > 1) {
				int64_t r = v % divisor64;
				if ((r < 0 ? -r : r) * 2 >= divisor64) {
					q += v < 0 ? -1 : 1;
				}
			}
			if (small_limit && (q >= limit64 || q <= -limit64)) {
				return optional<OUT>();
			}
			return OUT(q);
		}
		if (divisor > hugeint_t(1)) {
			hugeint_t quotient = value / divisor;
			hugeint_t remainder = value - quotient * divisor;
			if (remainder < hugeint_t(0)) {
				remainder = -remainder;
			}
			if (remainder * hugeint_t(2) >= divisor) {
				quotient += value < hugeint_t(0) ? hugeint_t(-1) : hugeint_t(1);
			}
			value = quotient;
		}
		if (value >= limit || value <= -limit) {
			return optional<OUT>();
		}
		return Hugeint::Cast<OUT>(value);
	});
}

template <class IN>
scalar_function_t DecimalRescaleFor(PhysicalType out, hugeint_t divisor, hugeint_t limit) {
	switch (out) {
	case PhysicalType::INT16:
		return [=](DataChunk &a, ExpressionState &st, Vector &r) { DecimalRescaleKernel<IN, int16_t>(a, st, r, divisor, limit); };
	case PhysicalType::INT32:
		return [=](DataChunk &a, ExpressionState &st, Vector &r) { DecimalRescaleKernel<IN, int32_t>(a, st, r, divisor, limit); };
	case PhysicalType::INT64:
		return [=](DataChunk &a, ExpressionState &st, Vector &r) { DecimalRescaleKernel<IN, int64_t>(a, st, r, divisor, limit); };
	default:
		return [=](DataChunk &a, ExpressionState &st, Vector &r) { DecimalRescaleKernel<IN, hugeint_t>(a, st, r, divisor, limit); };
	}
}

ExprP SparkDecimalResult(Builder &b, ExprP value, const LogicalType &target) {
	auto source = value->GetReturnType();
	if (source == target) {
		return value;
	}
	if (source.id() != LogicalTypeId::DECIMAL || target.id() != LogicalTypeId::DECIMAL ||
	    DecimalType::GetScale(target) > DecimalType::GetScale(source)) {
		return b.Cast(std::move(value), target, true);
	}
	auto divisor = Hugeint::POWERS_OF_TEN[DecimalType::GetScale(source) - DecimalType::GetScale(target)];
	auto limit = Hugeint::POWERS_OF_TEN[DecimalType::GetWidth(target)];
	scalar_function_t fn;
	switch (source.InternalType()) {
	case PhysicalType::INT16:
		fn = DecimalRescaleFor<int16_t>(target.InternalType(), divisor, limit);
		break;
	case PhysicalType::INT32:
		fn = DecimalRescaleFor<int32_t>(target.InternalType(), divisor, limit);
		break;
	case PhysicalType::INT64:
		fn = DecimalRescaleFor<int64_t>(target.InternalType(), divisor, limit);
		break;
	default:
		fn = DecimalRescaleFor<hugeint_t>(target.InternalType(), divisor, limit);
		break;
	}
	return b.Kernel(ScalarFunction(Identifier("spark_decimal_result"), {source}, target, fn), std::move(value));
}

//! Decimal multiplication whose operand precisions add up to more than 38: the exact 128-bit product (NULL when
//! it overflows), then rounded HALF_UP to Spark's result scale (NULL when it does not fit). Both inputs are
//! DECIMAL(38, s).
template <class OUT>
void DecimalMultiplyKernel(DataChunk &args, ExpressionState &, Vector &result, hugeint_t divisor, hugeint_t limit) {
	bool small_divisor = divisor < hugeint_t(NumericLimits<int64_t>::Maximum());
	int64_t divisor64 = small_divisor ? Hugeint::Cast<int64_t>(divisor) : 0;
	bool small_limit = limit < hugeint_t(NumericLimits<int64_t>::Maximum());
	int64_t limit64 = small_limit ? Hugeint::Cast<int64_t>(limit) : 0;
	const hugeint_t int64_max(NumericLimits<int64_t>::Maximum());
	const hugeint_t int64_min(NumericLimits<int64_t>::Minimum());
	BinaryExecutor::Execute<hugeint_t, hugeint_t, OUT>(
	    args.data[0], args.data[1], result, args.size(), [&](hugeint_t a, hugeint_t b) -> optional<OUT> {
		    // fast path: both operands and the product fit in 64 bits (the common case)
		    if (small_divisor && a < int64_max && a > int64_min && b < int64_max && b > int64_min) {
			    int64_t product;
			    if (TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(Hugeint::Cast<int64_t>(a),
			                                                                   Hugeint::Cast<int64_t>(b), product) &&
			        product != NumericLimits<int64_t>::Minimum()) {
				    int64_t q = product;
				    if (divisor64 > 1) {
					    q = product / divisor64;
					    int64_t r = product % divisor64;
					    if ((r < 0 ? -r : r) >= divisor64 - (r < 0 ? -r : r)) {
						    q += product < 0 ? -1 : 1;
					    }
				    }
				    if (small_limit && (q >= limit64 || q <= -limit64)) {
					    return optional<OUT>();
				    }
				    return OUT(q);
			    }
		    }
		    hugeint_t value;
		    if (!Hugeint::TryMultiply(a, b, value)) {
			    return optional<OUT>();
		    }
		    if (divisor > hugeint_t(1)) {
			    hugeint_t quotient = value / divisor;
			    hugeint_t remainder = value - quotient * divisor;
			    if (remainder < hugeint_t(0)) {
				    remainder = -remainder;
			    }
			    if (remainder * hugeint_t(2) >= divisor) {
				    quotient += value < hugeint_t(0) ? hugeint_t(-1) : hugeint_t(1);
			    }
			    value = quotient;
		    }
		    if (value >= limit || value <= -limit) {
			    return optional<OUT>();
		    }
		    return Hugeint::Cast<OUT>(value);
	    });
}

ExprP SparkWideDecimalMultiply(Builder &b, ExprP left, int s1, ExprP right, int s2, const LogicalType &target) {
	auto divisor = Hugeint::POWERS_OF_TEN[s1 + s2 - DecimalType::GetScale(target)];
	auto limit = Hugeint::POWERS_OF_TEN[DecimalType::GetWidth(target)];
	scalar_function_t fn;
	switch (target.InternalType()) {
	case PhysicalType::INT16:
		fn = [=](DataChunk &a, ExpressionState &st, Vector &r) { DecimalMultiplyKernel<int16_t>(a, st, r, divisor, limit); };
		break;
	case PhysicalType::INT32:
		fn = [=](DataChunk &a, ExpressionState &st, Vector &r) { DecimalMultiplyKernel<int32_t>(a, st, r, divisor, limit); };
		break;
	case PhysicalType::INT64:
		fn = [=](DataChunk &a, ExpressionState &st, Vector &r) { DecimalMultiplyKernel<int64_t>(a, st, r, divisor, limit); };
		break;
	default:
		fn = [=](DataChunk &a, ExpressionState &st, Vector &r) { DecimalMultiplyKernel<hugeint_t>(a, st, r, divisor, limit); };
		break;
	}
	auto lt = LogicalType::DECIMAL(38, uint8_t(s1));
	auto rt = LogicalType::DECIMAL(38, uint8_t(s2));
	return b.Kernel(ScalarFunction(Identifier("spark_decimal_multiply"), {lt, rt}, target, fn),
	                b.Cast(std::move(left), lt), b.Cast(std::move(right), rt));
}

template <class OUT, class A, class B>
inline typename std::enable_if<std::is_same<OUT, hugeint_t>::value && (std::is_same<A, hugeint_t>::value || std::is_same<B, hugeint_t>::value), OUT>::type
ExactProduct(A a, B b) {
	return hugeint_t(a) * hugeint_t(b);
}

template <class OUT, class A, class B>
inline typename std::enable_if<std::is_same<OUT, hugeint_t>::value && !(std::is_same<A, hugeint_t>::value || std::is_same<B, hugeint_t>::value), OUT>::type
ExactProduct(A a, B b) {
	__int128 p = static_cast<__int128>(int64_t(a)) * static_cast<__int128>(int64_t(b));
	hugeint_t r;
	r.lower = uint64_t(p);
	r.upper = int64_t(p >> 64);
	return r;
}

template <class OUT, class A, class B>
inline typename std::enable_if<!std::is_same<OUT, hugeint_t>::value, OUT>::type ExactProduct(A a, B b) {
	return OUT(int64_t(a) * int64_t(b));
}

//! Decimal multiplication whose exact result fits the result type (p1 + p2 + 1 <= 38): no rounding and no overflow
//! are possible, so the product of the stored integers is the answer.
template <class A, class B, class OUT>
void ExactDecimalMultiplyKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<A, B, OUT>(args.data[0], args.data[1], result, args.size(),
	                                   [](A a, B b) -> OUT { return ExactProduct<OUT, A, B>(a, b); });
}

template <class A, class B>
scalar_function_t ExactMultiplyFor(PhysicalType out) {
	switch (out) {
	case PhysicalType::INT16:
		return ExactDecimalMultiplyKernel<A, B, int16_t>;
	case PhysicalType::INT32:
		return ExactDecimalMultiplyKernel<A, B, int32_t>;
	case PhysicalType::INT64:
		return ExactDecimalMultiplyKernel<A, B, int64_t>;
	default:
		return ExactDecimalMultiplyKernel<A, B, hugeint_t>;
	}
}

template <class A>
scalar_function_t ExactMultiplyFor(PhysicalType right, PhysicalType out) {
	switch (right) {
	case PhysicalType::INT16:
		return ExactMultiplyFor<A, int16_t>(out);
	case PhysicalType::INT32:
		return ExactMultiplyFor<A, int32_t>(out);
	case PhysicalType::INT64:
		return ExactMultiplyFor<A, int64_t>(out);
	default:
		return ExactMultiplyFor<A, hugeint_t>(out);
	}
}

ExprP SparkExactDecimalMultiply(Builder &b, ExprP left, const LogicalType &lt, ExprP right, const LogicalType &rt,
                                const LogicalType &target) {
	scalar_function_t fn;
	switch (lt.InternalType()) {
	case PhysicalType::INT16:
		fn = ExactMultiplyFor<int16_t>(rt.InternalType(), target.InternalType());
		break;
	case PhysicalType::INT32:
		fn = ExactMultiplyFor<int32_t>(rt.InternalType(), target.InternalType());
		break;
	case PhysicalType::INT64:
		fn = ExactMultiplyFor<int64_t>(rt.InternalType(), target.InternalType());
		break;
	default:
		fn = ExactMultiplyFor<hugeint_t>(rt.InternalType(), target.InternalType());
		break;
	}
	return b.Kernel(ScalarFunction(Identifier("spark_decimal_multiply_exact"), {lt, rt}, target, fn),
	                b.Cast(std::move(left), lt), b.Cast(std::move(right), rt));
}

inline __int128 To128(hugeint_t v) {
	return (static_cast<__int128>(v.upper) << 64) | static_cast<__int128>(v.lower);
}
inline __int128 To128(int16_t v) { return v; }
inline __int128 To128(int32_t v) { return v; }
inline __int128 To128(int64_t v) { return v; }

template <class OUT>
inline OUT From128(__int128 v) {
	return OUT(int64_t(v));
}
template <>
inline hugeint_t From128<hugeint_t>(__int128 v) {
	hugeint_t r;
	r.lower = uint64_t(v);
	r.upper = int64_t(v >> 64);
	return r;
}

//! Decimal + / - whose exact result fits the result type (precision <= 38): align the scales by multiplying with
//! powers of ten and add the stored integers. Nothing can overflow or need rounding.
template <class A, class B, class OUT, bool SUBTRACT>
void ExactDecimalAddSubKernel(DataChunk &args, ExpressionState &, Vector &result, __int128 scale_a, __int128 scale_b) {
	BinaryExecutor::Execute<A, B, OUT>(args.data[0], args.data[1], result, args.size(), [&](A a, B b) -> OUT {
		__int128 x = To128(a) * scale_a, y = To128(b) * scale_b;
		return From128<OUT>(SUBTRACT ? x - y : x + y);
	});
}

template <class A, class B, bool SUBTRACT>
scalar_function_t ExactAddSubFor(PhysicalType out, __int128 sa, __int128 sb) {
	switch (out) {
	case PhysicalType::INT16:
		return [=](DataChunk &a, ExpressionState &st, Vector &r) { ExactDecimalAddSubKernel<A, B, int16_t, SUBTRACT>(a, st, r, sa, sb); };
	case PhysicalType::INT32:
		return [=](DataChunk &a, ExpressionState &st, Vector &r) { ExactDecimalAddSubKernel<A, B, int32_t, SUBTRACT>(a, st, r, sa, sb); };
	case PhysicalType::INT64:
		return [=](DataChunk &a, ExpressionState &st, Vector &r) { ExactDecimalAddSubKernel<A, B, int64_t, SUBTRACT>(a, st, r, sa, sb); };
	default:
		return [=](DataChunk &a, ExpressionState &st, Vector &r) { ExactDecimalAddSubKernel<A, B, hugeint_t, SUBTRACT>(a, st, r, sa, sb); };
	}
}

template <class A, bool SUBTRACT>
scalar_function_t ExactAddSubFor(PhysicalType right, PhysicalType out, __int128 sa, __int128 sb) {
	switch (right) {
	case PhysicalType::INT16:
		return ExactAddSubFor<A, int16_t, SUBTRACT>(out, sa, sb);
	case PhysicalType::INT32:
		return ExactAddSubFor<A, int32_t, SUBTRACT>(out, sa, sb);
	case PhysicalType::INT64:
		return ExactAddSubFor<A, int64_t, SUBTRACT>(out, sa, sb);
	default:
		return ExactAddSubFor<A, hugeint_t, SUBTRACT>(out, sa, sb);
	}
}

template <bool SUBTRACT>
scalar_function_t ExactAddSubDispatch(PhysicalType left, PhysicalType right, PhysicalType out, __int128 sa, __int128 sb) {
	switch (left) {
	case PhysicalType::INT16:
		return ExactAddSubFor<int16_t, SUBTRACT>(right, out, sa, sb);
	case PhysicalType::INT32:
		return ExactAddSubFor<int32_t, SUBTRACT>(right, out, sa, sb);
	case PhysicalType::INT64:
		return ExactAddSubFor<int64_t, SUBTRACT>(right, out, sa, sb);
	default:
		return ExactAddSubFor<hugeint_t, SUBTRACT>(right, out, sa, sb);
	}
}

ExprP SparkExactDecimalAddSub(Builder &b, ExprP left, const LogicalType &lt, ExprP right, const LogicalType &rt,
                              const LogicalType &target, bool subtract) {
	int scale = DecimalType::GetScale(target);
	__int128 sa = 1, sb = 1;
	for (int i = DecimalType::GetScale(lt); i < scale; i++) {
		sa *= 10;
	}
	for (int i = DecimalType::GetScale(rt); i < scale; i++) {
		sb *= 10;
	}
	auto fn = subtract ? ExactAddSubDispatch<true>(lt.InternalType(), rt.InternalType(), target.InternalType(), sa, sb)
	                   : ExactAddSubDispatch<false>(lt.InternalType(), rt.InternalType(), target.InternalType(), sa, sb);
	return b.Kernel(ScalarFunction(Identifier(subtract ? "spark_decimal_subtract_exact" : "spark_decimal_add_exact"),
	                               {lt, rt}, target, fn),
	                b.Cast(std::move(left), lt), b.Cast(std::move(right), rt));
}

ExprP BindAdd(FunctionBindExpressionInput &input) {
	return BindAddSubMul<AddOp, CheckedAddOp, false>(input, "+");
}
ExprP BindSubtract(FunctionBindExpressionInput &input) {
	return BindAddSubMul<SubOp, CheckedSubOp, false>(input, "-");
}
ExprP BindMultiply(FunctionBindExpressionInput &input) {
	return BindAddSubMul<MulOp, CheckedMulOp, false>(input, "*");
}
ExprP BindTryAdd(FunctionBindExpressionInput &input) {
	return BindAddSubMul<AddOp, CheckedAddOp, true>(input, "+");
}
ExprP BindTrySubtract(FunctionBindExpressionInput &input) {
	return BindAddSubMul<SubOp, CheckedSubOp, true>(input, "-");
}
ExprP BindTryMultiply(FunctionBindExpressionInput &input) {
	return BindAddSubMul<MulOp, CheckedMulOp, true>(input, "*");
}

//! Spark decimal division result type: DecimalPrecision rules with allowPrecisionLoss = true
LogicalType DecimalDivideType(const LogicalType &l, const LogicalType &r, bool allow_precision_loss = true) {
	int p1 = DecimalType::GetWidth(l), s1 = DecimalType::GetScale(l);
	int p2 = DecimalType::GetWidth(r), s2 = DecimalType::GetScale(r);
	int scale = MaxValue(6, s1 + p2 + 1);
	int precision = p1 - s1 + s2 + scale;
	if (precision > 38 && !allow_precision_loss) {
		return LogicalType::DECIMAL(38, uint8_t(MinValue(scale, 38)));
	}
	if (precision > 38) {
		int int_digits = precision - scale;
		int min_scale = MinValue(scale, 6);
		scale = MaxValue(38 - int_digits, min_scale);
		precision = 38;
	}
	return LogicalType::DECIMAL(uint8_t(precision), uint8_t(scale));
}

LogicalType AsDecimal(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return LogicalType::DECIMAL(3, 0);
	case LogicalTypeId::SMALLINT:
		return LogicalType::DECIMAL(5, 0);
	case LogicalTypeId::INTEGER:
		return LogicalType::DECIMAL(10, 0);
	case LogicalTypeId::BIGINT:
		return LogicalType::DECIMAL(20, 0);
	case LogicalTypeId::DECIMAL:
		return type;
	default:
		return LogicalType::DECIMAL(38, 0);
	}
}

template <bool TRY>
ExprP BindDivideImpl(FunctionBindExpressionInput &input) {
	CheckArgs(input, "/", 2, 2);
	Builder b(input.context);
	auto left_decimal = DecimalOperandType(*input.children[0], ArgType(input, 0));
	auto right_decimal = DecimalOperandType(*input.children[1], ArgType(input, 1));
	auto left = Arg(input, 0);
	auto right = Arg(input, 1);
	NumericOperands(b, left, right, "/");
	auto lt = Normalize(left->GetReturnType());
	auto rt = Normalize(right->GetReturnType());
	if (lt.id() == LogicalTypeId::INTERVAL && IsNumeric(rt)) {
		return b.Spark("interval_divide", std::move(left), b.Cast(std::move(right), LogicalType::DOUBLE));
	}
	if (!TRY && AnsiEnabled(input.context) && IsNumeric(rt)) {
		right = AnsiNonZero(b, std::move(right), left->Copy());
	}
	if ((IsDecimal(lt) || IsDecimal(rt)) && !IsFloating(lt) && !IsFloating(rt)) {
		auto result_type = DecimalDivideType(IsDecimal(lt) ? lt : left_decimal, IsDecimal(rt) ? rt : right_decimal,
		                                     SparkConfBool(input.context, "spark.sql.decimalOperations.allowPrecisionLoss"));
		auto left_type = IsDecimal(lt) ? lt : left_decimal;
		auto right_type = IsDecimal(rt) ? rt : right_decimal;
		auto wide_left = LogicalType::DECIMAL(38, DecimalType::GetScale(left_type));
		auto wide_right = LogicalType::DECIMAL(38, DecimalType::GetScale(right_type));
		auto precision = idx_t(DecimalType::GetWidth(result_type));
		auto quotient = b.Kernel(
		    ScalarFunction(Identifier("spark_decimal_divide"), {wide_left, wide_right},
		                   LogicalType::DECIMAL(38, DecimalType::GetScale(result_type)),
		                   [precision](DataChunk &args, ExpressionState &state, Vector &out) {
			                   DecimalDivideKernel(args, state, out, precision);
		                   }),
		    b.Cast(std::move(left), wide_left), b.Cast(std::move(right), wide_right));
		return b.Cast(std::move(quotient), result_type);
	}
	return b.Kernel(ScalarFunction(Identifier("spark_divide"), {LogicalType::DOUBLE, LogicalType::DOUBLE},
	                               LogicalType::DOUBLE, DoubleDivideKernel),
	                b.Cast(std::move(left), LogicalType::DOUBLE), b.Cast(std::move(right), LogicalType::DOUBLE));
}

ExprP BindDivide(FunctionBindExpressionInput &input) {
	return BindDivideImpl<false>(input);
}
ExprP BindTryDivide(FunctionBindExpressionInput &input) {
	return BindDivideImpl<true>(input);
}

ExprP BindDiv(FunctionBindExpressionInput &input) {
	CheckArgs(input, "div", 2, 2);
	Builder b(input.context);
	auto left = Arg(input, 0);
	auto right = Arg(input, 1);
	NumericOperands(b, left, right, "div");
	auto lt = Normalize(left->GetReturnType());
	auto rt = Normalize(right->GetReturnType());
	if (lt.id() == LogicalTypeId::INTERVAL && rt.id() == LogicalTypeId::INTERVAL) {
		return b.Spark("interval_div", std::move(left), std::move(right));
	}
	if (AnsiEnabled(input.context) && IsNumeric(rt)) {
		right = AnsiNonZero(b, std::move(right), left->Copy());
	}
	if (IsIntegral(lt) && IsIntegral(rt)) {
		return b.Kernel(ScalarFunction(Identifier("spark_div"), {LogicalType::BIGINT, LogicalType::BIGINT},
		                               LogicalType::BIGINT, IntDivKernel),
		                b.Cast(std::move(left), LogicalType::BIGINT), b.Cast(std::move(right), LogicalType::BIGINT));
	}
	return b.Kernel(ScalarFunction(Identifier("spark_div"), {LogicalType::DOUBLE, LogicalType::DOUBLE},
	                               LogicalType::BIGINT, DoubleDivTruncKernel),
	                b.Cast(std::move(left), LogicalType::DOUBLE), b.Cast(std::move(right), LogicalType::DOUBLE));
}

ExprP BindRemainder(FunctionBindExpressionInput &input) {
	CheckArgs(input, "%", 2, 2);
	Builder b(input.context);
	auto left_decimal = DecimalOperandType(*input.children[0], ArgType(input, 0));
	auto right_decimal = DecimalOperandType(*input.children[1], ArgType(input, 1));
	auto left = Arg(input, 0);
	auto right = Arg(input, 1);
	NumericOperands(b, left, right, "%");
	auto lt = Normalize(left->GetReturnType());
	auto rt = Normalize(right->GetReturnType());
	if (AnsiEnabled(input.context) && IsNumeric(rt)) {
		right = AnsiNonZero(b, std::move(right), left->Copy());
	}
	if (IsIntegral(lt) && IsIntegral(rt)) {
		auto type = WiderIntegral(lt, rt);
		scalar_function_t fn;
		switch (type.id()) {
		case LogicalTypeId::TINYINT:
			fn = IntRemainderKernel<int8_t>;
			break;
		case LogicalTypeId::SMALLINT:
			fn = IntRemainderKernel<int16_t>;
			break;
		case LogicalTypeId::INTEGER:
			fn = IntRemainderKernel<int32_t>;
			break;
		default:
			fn = IntRemainderKernel<int64_t>;
			break;
		}
		return b.Kernel(ScalarFunction(Identifier("spark_remainder"), {type, type}, type, fn),
		                b.Cast(std::move(left), type), b.Cast(std::move(right), type));
	}
	if (IsFloating(lt) || IsFloating(rt)) {
		// FLOAT with FLOAT or an integer stays FLOAT in Spark; DOUBLE or a decimal makes it DOUBLE
		bool stays_float = (lt.id() == LogicalTypeId::FLOAT || IsIntegral(lt)) && (rt.id() == LogicalTypeId::FLOAT || IsIntegral(rt));
		if (AnsiEnabled(input.context) && !(lt.id() == LogicalTypeId::FLOAT && rt.id() == LogicalTypeId::FLOAT)) {
			stays_float = false; // AnsiTypeCoercion: an integer with FLOAT widens to DOUBLE
		}
		auto result = b.Kernel(ScalarFunction(Identifier("spark_remainder"), {LogicalType::DOUBLE, LogicalType::DOUBLE},
		                                      LogicalType::DOUBLE, DoubleRemainderKernel),
		                       b.Cast(std::move(left), LogicalType::DOUBLE), b.Cast(std::move(right), LogicalType::DOUBLE));
		return stays_float ? b.Cast(std::move(result), LogicalType::FLOAT) : std::move(result);
	}
	// integers mixed with decimals; anything else (intervals, ...) is a type error as in Spark
	if (!(IsDecimal(lt) || IsIntegral(lt)) || !(IsDecimal(rt) || IsIntegral(rt))) {
		throw BinderException("[DATATYPE_MISMATCH] %% requires numeric operands, got %s and %s", SparkTypeName(lt),
		                      SparkTypeName(rt));
	}
	// decimals: NULL when dividing by zero; the type is Spark's DecimalPrecision rule for `%`:
	// scale max(s1, s2), precision min(p1 - s1, p2 - s2) + scale, reduced to 38 digits
	auto ld = IsDecimal(lt) ? lt : left_decimal;
	auto rd = IsDecimal(rt) ? rt : right_decimal;
	int s1 = DecimalType::GetScale(ld), s2 = DecimalType::GetScale(rd);
	int scale = MaxValue(s1, s2);
	int precision = MinValue(DecimalType::GetWidth(ld) - s1, DecimalType::GetWidth(rd) - s2) + scale;
	auto result_type = AdjustDecimal(precision, scale);
	auto common = LogicalType::DECIMAL(38, uint8_t(scale));
	auto remainder = b.Op("%", b.Cast(std::move(left), common), ZeroToNull(b, b.Cast(std::move(right), common)));
	return b.Cast(std::move(remainder), result_type);
}

ExprP BindNegative(FunctionBindExpressionInput &input) {
	CheckArgs(input, "negative", 1, 1);
	Builder b(input.context);
	auto child = NumericOperand(b, Arg(input, 0));
	auto type = Normalize(child->GetReturnType());
	if (IsIntegral(type) && !AnsiEnabled(input.context)) {
		return b.Kernel(NegateFunction(type), std::move(child));
	}
	vector<ExprP> args;
	args.push_back(std::move(child));
	return b.Call("-", std::move(args), true);
}

//! Comparisons with Spark's implicit casts (TypeCoercion.findCommonTypeForBinaryComparison, non-ANSI):
//! a string compared with a decimal compares as DOUBLE; a string compared with another atomic type is cast to
//! that type (NULL when it does not parse). Everything else is DuckDB's native comparison, so filters on
//! columns still push down.
ExprP BindComparisonAs(FunctionBindExpressionInput &input, ExpressionType type) {
	CheckArgs(input, "comparison", 2, 2);
	Builder b(input.context);
	auto l = Arg(input, 0);
	auto r = Arg(input, 1);
	auto lt = Normalize(l->GetReturnType());
	auto rt = Normalize(r->GetReturnType());
	bool ls = IsString(lt), rs = IsString(rt);
	if (ls != rs && !IsNull(lt) && !IsNull(rt)) {
		auto &other = ls ? rt : lt;
		auto id = other.id();
		bool atomic = IsNumeric(other) || id == LogicalTypeId::BOOLEAN || id == LogicalTypeId::DATE ||
		              id == LogicalTypeId::TIMESTAMP || id == LogicalTypeId::TIMESTAMP_TZ || id == LogicalTypeId::BLOB;
		if (IsDecimal(other)) {
			l = SparkCast(b, std::move(l), LogicalType::DOUBLE);
			r = SparkCast(b, std::move(r), LogicalType::DOUBLE);
		} else if (atomic) {
			auto target = other;
			if (ls) {
				l = SparkCast(b, std::move(l), target);
			} else {
				r = SparkCast(b, std::move(r), target);
			}
		}
	}
	// equality of a boolean with a number (BooleanEquality): the boolean is cast to the number's type
	bool equality = type == ExpressionType::COMPARE_EQUAL || type == ExpressionType::COMPARE_NOTEQUAL ||
	                type == ExpressionType::COMPARE_NOT_DISTINCT_FROM || type == ExpressionType::COMPARE_DISTINCT_FROM;
	auto fractional = [](const LogicalType &t) {
		return IsFloating(t) || IsDecimal(t) || IsIntegral(t);
	};
	if (equality && lt.id() == LogicalTypeId::BOOLEAN && fractional(rt)) {
		l = b.Cast(std::move(l), rt);
	} else if (equality && rt.id() == LogicalTypeId::BOOLEAN && fractional(lt)) {
		r = b.Cast(std::move(r), lt);
	}
	return b.Compare(type, std::move(l), std::move(r));
}

template <ExpressionType TYPE>
ExprP BindComparison(FunctionBindExpressionInput &input) {
	return BindComparisonAs(input, TYPE);
}

//! The comparator of array_sort(arr, (l, r) -> ...) must return INT, like in Spark
ExprP BindComparatorResult(FunctionBindExpressionInput &input) {
	CheckArgs(input, "array_sort", 1, 1);
	Builder b(input.context);
	auto result = Arg(input, 0);
	auto type = result->GetReturnType();
	if (type.id() != LogicalTypeId::INTEGER) {
		throw BinderException("[DATATYPE_MISMATCH.UNEXPECTED_RETURN_TYPE] The array_sort comparator must return INT, "
		                      "but returns %s",
		                      SparkTypeName(type));
	}
	return result;
}

//! zero_key(x): for DOUBLE / FLOAT, -0.0 mapped to a value no other number has in practice (-denorm_min), so that
//! equality-based list functions tell it from 0.0 like Spark does; other types unchanged
ExprP BindZeroKey(FunctionBindExpressionInput &input) {
	CheckArgs(input, "zero_key", 1, 1);
	Builder b(input.context);
	auto value = Arg(input, 0);
	auto type = Normalize(value->GetReturnType());
	if (type.id() != LogicalTypeId::DOUBLE && type.id() != LogicalTypeId::FLOAT) {
		return value;
	}
	auto as_double = b.Cast(value->Copy(), LogicalType::DOUBLE);
	auto is_negative_zero = make_uniq<BoundConjunctionExpression>(
	    ExpressionType::CONJUNCTION_AND,
	    b.Compare(ExpressionType::COMPARE_EQUAL, as_double->Copy(), b.Constant(Value::DOUBLE(0))),
	    b.Compare(ExpressionType::COMPARE_LESSTHAN, b.Call("atan2", as_double->Copy(), b.Constant(Value::DOUBLE(-1))),
	              b.Constant(Value::DOUBLE(0))));
	return b.Case(std::move(is_negative_zero), b.Constant(Value::DOUBLE(-4.9406564584124654e-324)),
	              std::move(as_double));
}

//! narrow_int(x): a BIGINT (or wider) argument wraps to INT like Spark's implicit cast; other types are unchanged
ExprP BindNarrowInt(FunctionBindExpressionInput &input) {
	CheckArgs(input, "narrow_int", 1, 2);
	Builder b(input.context);
	auto value = Arg(input, 0);
	auto type = Normalize(value->GetReturnType());
	if (IsString(type) && input.children.size() == 1) {
		// a string argument is read as an INT (NULL when malformed); with a second argument it may be a format and stays
		return SparkCast(b, std::move(value), LogicalType::INTEGER);
	}
	if (type.id() == LogicalTypeId::BIGINT || type.id() == LogicalTypeId::HUGEINT || IsFloating(type) || IsDecimal(type)) {
		// Spark's implicit cast to INT: wraps a BIGINT, truncates a fraction (a string argument may be a format: unchanged)
		return SparkCast(b, std::move(value), LogicalType::INTEGER);
	}
	return value;
}

//! simple_sort_key(list): true when DuckDB's sort order of the list's elements is Spark's comparison order
//! (numbers, strings, booleans, dates and timestamps; nested values may contain NULLs and are left to the slow path)
ExprP BindSimpleSortKey(FunctionBindExpressionInput &input) {
	CheckArgs(input, "simple_sort_key", 1, 2);
	Builder b(input.context);
	auto type = Normalize(Arg(input, 0)->GetReturnType());
	// the second argument asks for exact types: equal values are indistinguishable (no floating point zeros)
	bool exact = input.children.size() > 1;
	bool simple = false;
	if (type.id() == LogicalTypeId::LIST) {
		switch (Normalize(ListType::GetChildType(type)).id()) {
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::HUGEINT:
		case LogicalTypeId::DECIMAL:
		case LogicalTypeId::VARCHAR:
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_TZ:
			simple = true;
			break;
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
			simple = !exact;
			break;
		default:
			break;
		}
	}
	return b.Constant(Value::BOOLEAN(simple));
}

//! `s [NOT] LIKE p` / `ILIKE` without an ESCAPE clause. With a constant pattern that has no backslash, Spark's
//! escape character never applies and DuckDB's own LIKE operators (`~~`, `!~~`, `~~*`, `!~~*`) give the same result.
//! They are not fallible, unlike like_escape: DuckDB moves an ON-clause filter below an outer join, and pushes
//! aggregates below it (TPC-H Q13), only when the predicate cannot fail. Other patterns keep the checked form.
template <bool NEGATE, bool CASE_INSENSITIVE>
ExprP BindLikeOperator(FunctionBindExpressionInput &input) {
	CheckArgs(input, "like", 2, 2);
	Builder b(input.context);
	auto subject = b.Spark("as_string", Arg(input, 0));
	auto pattern_arg = Arg(input, 1);
	auto pattern = b.Spark("as_string", pattern_arg->Copy());
	bool simple = false;
	if (pattern->IsFoldable()) {
		auto value = ExpressionExecutor::EvaluateScalar(input.context, *pattern);
		simple = !value.IsNull() && value.ToString().find('\\') == string::npos;
	}
	if (simple) {
		const char *name = NEGATE ? (CASE_INSENSITIVE ? "!~~*" : "!~~") : (CASE_INSENSITIVE ? "~~*" : "~~");
		return b.Call(name, std::move(subject), std::move(pattern), true);
	}
	vector<ExprP> args;
	args.push_back(std::move(subject));
	args.push_back(b.Spark("check_like_pattern", std::move(pattern)));
	args.push_back(b.Constant(Value("\\")));
	ExprP checked = b.Call(CASE_INSENSITIVE ? "ilike_escape" : "like_escape", std::move(args));
	if (!NEGATE) {
		return checked;
	}
	auto negated = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_NOT, LogicalType::BOOLEAN);
	negated->GetChildrenMutable().push_back(std::move(checked));
	return std::move(negated);
}

ExprP BindPositive(FunctionBindExpressionInput &input) {
	CheckArgs(input, "positive", 1, 1);
	Builder b(input.context);
	return NumericOperand(b, Arg(input, 0));
}

ExprP BitwiseOperands(Builder &b, FunctionBindExpressionInput &input, ExprP &right) {
	auto left = Arg(input, 0);
	right = Arg(input, 1);
	auto lt = Normalize(left->GetReturnType());
	auto rt = Normalize(right->GetReturnType());
	if (IsNull(lt)) {
		lt = LogicalType::INTEGER;
	}
	if (IsNull(rt)) {
		rt = LogicalType::INTEGER;
	}
	if (!IsIntegral(lt) || !IsIntegral(rt)) {
		throw BinderException("Bitwise operators require integral operands");
	}
	auto type = WiderIntegral(lt, rt);
	right = b.Cast(std::move(right), type);
	return b.Cast(std::move(left), type);
}

ExprP BindBitAnd(FunctionBindExpressionInput &input) {
	CheckArgs(input, "&", 2, 2);
	Builder b(input.context);
	ExprP right;
	auto left = BitwiseOperands(b, input, right);
	return b.Op("&", std::move(left), std::move(right));
}
ExprP BindBitOr(FunctionBindExpressionInput &input) {
	CheckArgs(input, "|", 2, 2);
	Builder b(input.context);
	ExprP right;
	auto left = BitwiseOperands(b, input, right);
	return b.Op("|", std::move(left), std::move(right));
}
ExprP BindBitXor(FunctionBindExpressionInput &input) {
	CheckArgs(input, "^", 2, 2);
	Builder b(input.context);
	ExprP right;
	auto left = BitwiseOperands(b, input, right);
	return b.Call("xor", std::move(left), std::move(right));
}
ExprP BindBitNot(FunctionBindExpressionInput &input) {
	CheckArgs(input, "~", 1, 1);
	Builder b(input.context);
	auto child = Arg(input, 0);
	auto type = Normalize(child->GetReturnType());
	if (IsNull(type)) {
		child = b.Cast(std::move(child), LogicalType::INTEGER);
	} else if (!IsIntegral(type)) {
		throw BinderException("Bitwise NOT requires an integral operand");
	}
	vector<ExprP> args;
	args.push_back(std::move(child));
	return b.Call("~", std::move(args), true);
}

LogicalType SparkWiderType(ClientContext &context, const LogicalType &a_p, const LogicalType &b_p);

//! `a || b` and concat(): NULL if any input is NULL; arrays concatenate
ExprP BindConcat(FunctionBindExpressionInput &input) {
	Builder b(input.context);
	if (input.children.empty()) {
		return b.Constant(Value(""));
	}
	bool all_lists = true, all_blobs = true;
	for (idx_t i = 0; i < input.children.size(); i++) {
		auto type = ArgType(input, i);
		all_lists = all_lists && (type.id() == LogicalTypeId::LIST || IsNull(type));
		all_blobs = all_blobs && (type.id() == LogicalTypeId::BLOB || IsNull(type));
	}
	// spark.sql.function.concatBinaryAsString: binary inputs concatenate as strings
	if (all_blobs && SparkConfBool(input.context, "spark.sql.function.concatBinaryAsString")) {
		all_blobs = false;
	}
	vector<ExprP> args;
	for (idx_t i = 0; i < input.children.size(); i++) {
		args.push_back(Arg(input, i));
	}
	if (all_lists) {
		// Spark coerces the element types to a common type, e.g. array<int> and array<string> -> array<string>
		LogicalType element = LogicalType::SQLNULL;
		for (auto &arg : args) {
			auto type = arg->GetReturnType();
			if (type.id() == LogicalTypeId::LIST) {
				element = SparkWiderType(input.context, element, ListType::GetChildType(type));
			}
		}
		if (!IsNull(element)) {
			for (auto &arg : args) {
				auto type = arg->GetReturnType();
				if (type.id() != LogicalTypeId::LIST || ListType::GetChildType(type) != element) {
					arg = b.Cast(std::move(arg), LogicalType::LIST(element));
				}
			}
		}
	}
	// `||` returns NULL if either side is NULL, for strings, lists and blobs alike (unlike DuckDB's concat())
	ExprP result = all_lists || all_blobs ? std::move(args[0]) : ToSparkString(b, std::move(args[0]));
	for (idx_t i = 1; i < args.size(); i++) {
		auto next = all_lists || all_blobs ? std::move(args[i]) : ToSparkString(b, std::move(args[i]));
		result = b.Op("||", std::move(result), std::move(next));
	}
	return result;
}

//===----------------------------------------------------------------------===//
// Access
//===----------------------------------------------------------------------===//
ExprP BindSubscript(FunctionBindExpressionInput &input) {
	CheckArgs(input, "[]", 2, 2);
	Builder b(input.context);
	auto base = Arg(input, 0);
	auto index = Arg(input, 1);
	auto type = Normalize(base->GetReturnType());
	if (type.id() == LogicalTypeId::LIST || type.id() == LogicalTypeId::ARRAY) {
		// 0-based; out of range (including negative) is NULL
		auto idx = b.Cast(std::move(index), LogicalType::BIGINT);
		if (AnsiEnabled(input.context)) {
			ScalarFunction check(Identifier("spark_ansi_index"), {LogicalType::BIGINT, LogicalType::BIGINT},
			                     LogicalType::BIGINT, AnsiIndexKernel);
			check.SetFallible();
			auto checked = b.Kernel(check, std::move(idx), b.Cast(b.Call("len", base->Copy()), LogicalType::BIGINT));
			return b.Call("list_extract", std::move(base), std::move(checked));
		}
		auto one_based = b.Kernel(ScalarFunction(Identifier("spark_index"), {LogicalType::BIGINT}, LogicalType::BIGINT,
		                                         SparkIndexKernel),
		                          std::move(idx));
		return b.Call("list_extract", std::move(base), std::move(one_based));
	}
	if (type.id() == LogicalTypeId::MAP) {
		return b.Call("map_extract_value", std::move(base), std::move(index));
	}
	if (type.id() == LogicalTypeId::STRUCT) {
		return b.Call("struct_extract", std::move(base), std::move(index));
	}
	if (IsNull(type)) {
		return b.NullOf(LogicalType::SQLNULL);
	}
	throw BinderException("[] is not supported for %s", SparkTypeName(type));
}

ExprP BindGetField(FunctionBindExpressionInput &input) {
	CheckArgs(input, ".", 2, 2);
	Builder b(input.context);
	auto base = Arg(input, 0);
	auto type = Normalize(base->GetReturnType());
	if (type.id() == LogicalTypeId::STRUCT && input.children[1]->IsFoldable()) {
		// Spark field names are case-insensitive
		auto name = ExpressionExecutor::EvaluateScalar(input.context, *input.children[1]).ToString();
		auto &children = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < children.size(); i++) {
			if (StringUtil::CIEquals(children[i].first.GetIdentifierName(), name)) {
				return b.Call("struct_extract", std::move(base), b.Constant(Value(children[i].first.GetIdentifierName())));
			}
		}
		throw BinderException("No such struct field %s in %s", name, SparkTypeName(type));
	}
	return b.Call("struct_extract", std::move(base), Arg(input, 1));
}

//===----------------------------------------------------------------------===//
// typeof / array / map
//===----------------------------------------------------------------------===//
ExprP BindTypeOf(FunctionBindExpressionInput &input) {
	CheckArgs(input, "typeof", 1, 1);
	Builder b(input.context);
	return b.Constant(Value(SparkTypeName(input.children[0]->GetReturnType())));
}

LogicalType SparkWiderType(ClientContext &context, const LogicalType &a_p, const LogicalType &b_p);

//! The operands cast to their common type, found pairwise from the left like Spark's findWiderTypeForTwo
void CastToCommonType(Builder &b, vector<ExprP> &args) {
	LogicalType common = LogicalType::SQLNULL;
	for (auto &arg : args) {
		common = SparkWiderType(b.context, common, arg->GetReturnType());
	}
	if (IsNull(common)) {
		return;
	}
	for (auto &arg : args) {
		if (arg->GetReturnType() != common) {
			arg = AnsiEnabled(b.context) ? SparkImplicitCast(b, std::move(arg), common) : b.Cast(std::move(arg), common);
		}
	}
}

//! value IN (items...) after the common type of all operands. Structs compare by position like Spark; operands of
//! different kinds (a struct with a scalar, an array with a struct, ...) are Spark's analysis error, never a
//! comparison of mismatched types.
ExprP BindInListValue(FunctionBindExpressionInput &input) {
	Builder b(input.context);
	vector<ExprP> args;
	for (idx_t i = 0; i < input.children.size(); i++) {
		args.push_back(Arg(input, i));
	}
	auto kind = [](const LogicalType &type) {
		switch (type.id()) {
		case LogicalTypeId::STRUCT:
		case LogicalTypeId::LIST:
		case LogicalTypeId::MAP:
			return type.id();
		case LogicalTypeId::ARRAY:
			return LogicalTypeId::LIST;
		default:
			return LogicalTypeId::INVALID;
		}
	};
	auto mismatch = [&]() {
		string types;
		for (auto &arg : args) {
			types += (types.empty() ? "" : ", ") + SparkTypeName(arg->GetReturnType());
		}
		return BinderException("[DATATYPE_MISMATCH.DATA_DIFF_TYPES] Cannot resolve IN due to data type mismatch: Input "
		                       "to `in` should all be the same type, but it's [%s]",
		                       types);
	};
	bool nested = false;
	optional_idx first;
	for (idx_t i = 0; i < args.size(); i++) {
		auto type = Normalize(args[i]->GetReturnType());
		if (IsNull(type)) {
			continue;
		}
		nested = nested || kind(type) != LogicalTypeId::INVALID;
		if (!first.IsValid()) {
			first = i;
			continue;
		}
		auto first_type = Normalize(args[first.GetIndex()]->GetReturnType());
		if (kind(type) != kind(first_type) ||
		    (type.id() == LogicalTypeId::STRUCT &&
		     StructType::GetChildCount(type) != StructType::GetChildCount(first_type))) {
			throw mismatch();
		}
	}
	if (!nested) {
		CastToCommonType(b, args);
	} else {
		// operands of the same field types compare as they are (field names do not matter); once a cast is needed,
		// Spark's common struct type requires the same field names (case-insensitive)
		std::function<bool(const LogicalType &, const LogicalType &, bool)> same;
		same = [&](const LogicalType &x, const LogicalType &y, bool names) -> bool {
			auto l = Normalize(x), r = Normalize(y);
			if (IsNull(l) || IsNull(r)) {
				return true;
			}
			if (l.id() != r.id()) {
				// with a cast, atomic types of different ids widen; nested kinds must agree
				auto is_nested = [](const LogicalType &t) {
					return t.id() == LogicalTypeId::STRUCT || t.id() == LogicalTypeId::LIST || t.id() == LogicalTypeId::MAP;
				};
				return names && !is_nested(l) && !is_nested(r);
			}
			switch (l.id()) {
			case LogicalTypeId::STRUCT: {
				if (StructType::GetChildCount(l) != StructType::GetChildCount(r)) {
					return false;
				}
				for (idx_t i = 0; i < StructType::GetChildCount(l); i++) {
					if (names && !StringUtil::CIEquals(StructType::GetChildName(l, i).GetIdentifierName(), StructType::GetChildName(r, i).GetIdentifierName())) {
						return false;
					}
					if (!same(StructType::GetChildType(l, i), StructType::GetChildType(r, i), names)) {
						return false;
					}
				}
				return true;
			}
			case LogicalTypeId::LIST:
				return same(ListType::GetChildType(l), ListType::GetChildType(r), names);
			case LogicalTypeId::MAP:
				return same(MapType::KeyType(l), MapType::KeyType(r), names) &&
				       same(MapType::ValueType(l), MapType::ValueType(r), names);
			default:
				return names || l == r;
			}
		};
		bool structurally_equal = true;
		for (auto &arg : args) {
			structurally_equal = structurally_equal && same(args[0]->GetReturnType(), arg->GetReturnType(), false);
		}
		if (!structurally_equal) {
			for (auto &arg : args) {
				if (!same(args[0]->GetReturnType(), arg->GetReturnType(), true)) {
					throw mismatch();
				}
			}
		}
		LogicalType common = LogicalType::SQLNULL;
		try {
			for (auto &arg : args) {
				common = SparkWiderType(b.context, common, arg->GetReturnType());
			}
		} catch (std::exception &) {
			throw mismatch();
		}
		for (auto &arg : args) {
			if (arg->GetReturnType() != common) {
				arg = SparkCast(b, std::move(arg), common);
			}
		}
	}
	auto result = make_uniq<BoundOperatorExpression>(ExpressionType::COMPARE_IN, LogicalType::BOOLEAN);
	for (auto &arg : args) {
		result->GetChildrenMutable().push_back(std::move(arg));
	}
	return std::move(result);
}

ExprP BindCoalesceValue(FunctionBindExpressionInput &input) {
	Builder b(input.context);
	vector<ExprP> args;
	for (idx_t i = 0; i < input.children.size(); i++) {
		args.push_back(Arg(input, i));
	}
	CastToCommonType(b, args);
	return b.Coalesce(std::move(args));
}

ExprP BindArray(FunctionBindExpressionInput &input) {
	Builder b(input.context);
	vector<ExprP> args;
	for (idx_t i = 0; i < input.children.size(); i++) {
		args.push_back(Arg(input, i));
	}
	CastToCommonType(b, args);
	if (args.empty()) {
		return b.Constant(Value::LIST(LogicalType::SQLNULL, {}));
	}
	return b.Call("list_value", std::move(args));
}

// spark.sql.mapKeyDedupPolicy=LAST_WIN: the 1-based positions of the entries a map keeps, in the order of the first
// occurrence of every key (LAST = false: the position of the first entry of the key, LAST = true: of its last one)
template <bool LAST, bool ENTRIES = false>
void MapDedupIndexKernel(DataChunk &args, ExpressionState &, Vector &result) {
	idx_t count = args.size();
	vector<vector<Value>> out(count);
	vector<bool> nulls(count, false);
	for (idx_t row = 0; row < count; row++) {
		auto list = args.data[0].GetValue(row);
		if (list.IsNull()) {
			nulls[row] = true;
			continue;
		}
		auto &items = ListValue::GetChildren(list);
		vector<Value> keys;
		for (auto &item : items) {
			keys.push_back(ENTRIES ? (item.IsNull() ? item : StructValue::GetChildren(item)[0]) : item);
		}
		vector<idx_t> first, last;
		for (idx_t i = 0; i < keys.size(); i++) {
			idx_t found = first.size();
			for (idx_t j = 0; j < first.size(); j++) {
				if (ValueOperations::NotDistinctFrom(keys[first[j]], keys[i])) {
					found = j;
					break;
				}
			}
			if (found == first.size()) {
				first.push_back(i);
				last.push_back(i);
			} else {
				last[found] = i;
			}
		}
		for (idx_t j = 0; j < first.size(); j++) {
			out[row].push_back(Value::BIGINT(int64_t((LAST ? last[j] : first[j]) + 1)));
		}
	}
	for (idx_t row = 0; row < count; row++) {
		result.SetValue(row, nulls[row] ? Value(LogicalType::LIST(LogicalType::BIGINT))
		                               : Value::LIST(LogicalType::BIGINT, out[row]));
	}
}

// map_from_entries(entries) and the other builders of a map from a list of {key, value} structs
ExprP BindMapFromEntries(FunctionBindExpressionInput &input) {
	CheckArgs(input, "map_from_entries", 1, 1);
	Builder b(input.context);
	auto entries = Arg(input, 0);
	if (SparkConfEquals(input.context, "spark.sql.mapKeyDedupPolicy", "LAST_WIN") &&
	    entries->GetReturnType().id() == LogicalTypeId::LIST &&
	    ListType::GetChildType(entries->GetReturnType()).id() == LogicalTypeId::STRUCT) {
		ScalarFunction last_fn(Identifier("map_dedup_entries"), {entries->GetReturnType()},
		                       LogicalType::LIST(LogicalType::BIGINT), MapDedupIndexKernel<true, true>);
		auto last = b.Kernel(last_fn, entries->Copy());
		entries = b.Call("list_select", std::move(entries), std::move(last));
	}
	return b.Call("map_from_entries", std::move(entries));
}

// elt_list(values...): the candidates of elt(); binary stays binary unless spark.sql.function.eltOutputAsString
ExprP BindEltList(FunctionBindExpressionInput &input) {
	Builder b(input.context);
	bool all_binary = !input.children.empty();
	for (idx_t i = 0; i < input.children.size(); i++) {
		all_binary = all_binary && Normalize(ArgType(input, i)).id() == LogicalTypeId::BLOB;
	}
	all_binary = all_binary && !SparkConfBool(input.context, "spark.sql.function.eltOutputAsString");
	vector<ExprP> args;
	for (idx_t i = 0; i < input.children.size(); i++) {
		auto arg = Arg(input, i);
		args.push_back(all_binary ? std::move(arg) : b.Spark("as_string", std::move(arg)));
	}
	return b.Call("list_value", std::move(args));
}

// conf_bool('spark.sql.key'): the value of a boolean setting, as a constant at bind time (macros use it to branch)
ExprP BindConfBool(FunctionBindExpressionInput &input) {
	CheckArgs(input, "conf_bool", 1, 1);
	auto &child = *input.children[0];
	if (child.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		throw BinderException("conf_bool: the key must be a constant");
	}
	auto key = child.Cast<BoundConstantExpression>().GetValue().ToString();
	return make_uniq<BoundConstantExpression>(Value::BOOLEAN(SparkConfBool(input.context, key)));
}

// map_concat: a NULL map makes the result NULL (DuckDB treats it as empty)
ExprP BindMapConcat(FunctionBindExpressionInput &input) {
	Builder b(input.context);
	vector<ExprP> args;
	ExprP any_null;
	for (idx_t i = 0; i < input.children.size(); i++) {
		auto arg = Arg(input, i);
		auto is_null = b.IsNull(arg->Copy());
		any_null = any_null ? b.Or(std::move(any_null), std::move(is_null)) : std::move(is_null);
		args.push_back(std::move(arg));
	}
	if (args.empty()) {
		vector<ExprP> none;
		return b.Call("map", std::move(none));
	}
	auto result = b.Call("map_concat", std::move(args));
	auto type = result->GetReturnType();
	return b.Case(std::move(any_null), b.NullOf(type), std::move(result));
}

ExprP BindMapFromArrays(FunctionBindExpressionInput &input) {
	CheckArgs(input, "map_from_arrays", 2, 2);
	Builder b(input.context);
	auto keys = Arg(input, 0);
	auto values = Arg(input, 1);
	if (keys->GetReturnType().id() == LogicalTypeId::LIST &&
	    ListType::GetChildType(keys->GetReturnType()).id() == LogicalTypeId::SQLNULL) {
		// MAP(): an empty map
		vector<ExprP> none;
		return b.Call("map", std::move(none));
	}
	if (SparkConfEquals(input.context, "spark.sql.mapKeyDedupPolicy", "LAST_WIN") &&
	    keys->GetReturnType().id() == LogicalTypeId::LIST) {
		auto list_type = LogicalType::LIST(LogicalType::BIGINT);
		ScalarFunction first_fn(Identifier("map_dedup_first"), {keys->GetReturnType()}, list_type,
		                        MapDedupIndexKernel<false>);
		ScalarFunction last_fn(Identifier("map_dedup_last"), {keys->GetReturnType()}, list_type,
		                       MapDedupIndexKernel<true>);
		auto first = b.Kernel(first_fn, keys->Copy());
		auto last = b.Kernel(last_fn, keys->Copy());
		auto new_keys = b.Call("list_select", std::move(keys), std::move(first));
		auto new_values = b.Call("list_select", std::move(values), std::move(last));
		return b.Call("map", std::move(new_keys), std::move(new_values));
	}
	return b.Call("map", std::move(keys), std::move(values));
}

//===----------------------------------------------------------------------===//
// Generators: explode / posexplode (+ _outer) rows as a list of structs, unnested by the caller
//===----------------------------------------------------------------------===//
struct ExplodeBindData : public FunctionData {
	ExplodeBindData(bool pos_p, bool outer_p) : pos(pos_p), outer(outer_p) {
	}
	bool pos;
	bool outer;
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ExplodeBindData>(pos, outer);
	}
	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<ExplodeBindData>();
		return pos == o.pos && outer == o.outer;
	}
};

LogicalType ExplodeRowType(const LogicalType &input, bool pos, const string &aliases = "") {
	vector<std::pair<string, LogicalType>> columns;
	if (pos) {
		columns.emplace_back("pos", LogicalType::INTEGER);
	}
	if (input.id() == LogicalTypeId::MAP) {
		columns.emplace_back("key", MapType::KeyType(input));
		columns.emplace_back("value", MapType::ValueType(input));
	} else {
		columns.emplace_back("col", ListType::GetChildType(input));
	}
	if (!aliases.empty()) {
		// `generator AS name` / `generator AS (a, b)`: one alias per output column
		auto names = StringUtil::Split(aliases, ',');
		if (names.size() != columns.size()) {
			throw BinderException("[UDTF_ALIAS_NUMBER_MISMATCH] The number of aliases supplied in the AS clause does not "
			                      "match the number of columns output by the UDTF: expected %llu aliases, got %llu",
			                      (unsigned long long)columns.size(), (unsigned long long)names.size());
		}
		for (idx_t i = 0; i < columns.size(); i++) {
			columns[i].first = names[i];
		}
	}
	child_list_t<LogicalType> fields;
	for (auto &column : columns) {
		fields.emplace_back(Identifier(column.first), column.second);
	}
	return LogicalType::LIST(LogicalType::STRUCT(std::move(fields)));
}

void ExplodeRowsKernel(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.BindInfo()->Cast<ExplodeBindData>();
	auto count = args.size();
	auto &input = args.data[0];
	bool is_map = input.GetType().id() == LogicalTypeId::MAP;
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(format);
	auto entries = UnifiedVectorFormat::GetData<list_entry_t>(format);
	auto &input_child = ListVector::GetChild(input);
	auto input_child_size = ListVector::GetListSize(input);

	idx_t total = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = format.sel->get_index(i);
		bool valid = format.validity.RowIsValid(idx);
		idx_t length = valid ? entries[idx].length : 0;
		total += (length == 0 && info.outer) ? 1 : length;
	}
	result.SetVectorType(VectorType::FLAT_VECTOR);
	ListVector::Reserve(result, total);
	auto &result_child = ListVector::GetChildMutable(result);
	auto &fields = StructVector::GetEntries(result_child);
	auto result_entries = FlatVector::GetDataMutable<list_entry_t>(result);
	SelectionVector sel(MaxValue<idx_t>(total, 1));
	vector<idx_t> null_positions;
	vector<int32_t> positions(total);
	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = format.sel->get_index(i);
		bool valid = format.validity.RowIsValid(idx);
		idx_t length = valid ? entries[idx].length : 0;
		if (length == 0) {
			if (info.outer) {
				result_entries[i] = list_entry_t(offset, 1);
				sel.set_index(offset, 0);
				null_positions.push_back(offset);
				offset++;
			} else {
				result_entries[i] = list_entry_t(offset, 0);
			}
			continue;
		}
		result_entries[i] = list_entry_t(offset, length);
		for (idx_t j = 0; j < length; j++) {
			sel.set_index(offset + j, entries[idx].offset + j);
			positions[offset + j] = int32_t(j);
		}
		offset += length;
	}
	idx_t field = 0;
	if (info.pos) {
		auto &pos_vector = fields[field++];
		auto pos_data = FlatVector::GetDataMutable<int32_t>(pos_vector);
		for (idx_t k = 0; k < total; k++) {
			pos_data[k] = positions[k];
		}
	}
	vector<reference<const Vector>> sources;
	if (is_map) {
		auto &kv = StructVector::GetEntries(input_child);
		sources.push_back(kv[0]);
		sources.push_back(kv[1]);
	} else {
		sources.push_back(input_child);
	}
	for (auto &source : sources) {
		auto &target = fields[field++];
		if (input_child_size > 0 && total > 0) {
			VectorOperations::Copy(source.get(), target, sel, total, 0, 0);
		}
		if (input_child_size == 0) {
			for (idx_t k = 0; k < total; k++) {
				FlatVector::ValidityMutable(target).SetInvalid(k);
			}
		}
	}
	for (auto position : null_positions) {
		for (idx_t f = 0; f < fields.size(); f++) {
			FlatVector::ValidityMutable(fields[f]).SetInvalid(position);
		}
	}
	ListVector::SetListSize(result, total);
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

template <bool POS, bool OUTER>
unique_ptr<FunctionData> ExplodeBind(BindScalarFunctionInput &input) {
	auto &arguments = input.GetArguments();
	auto type = arguments[0]->GetReturnType();
	// an untyped NULL is rejected like in Spark (explode(CAST(NULL AS ARRAY<INT>)) is fine)
	if (type.id() != LogicalTypeId::LIST && type.id() != LogicalTypeId::MAP) {
		throw BinderException("explode: input must be an array or a map, not %s", SparkTypeName(type));
	}
	string column;
	if (arguments.size() > 1) {
		// `explode(x) AS name`: the output column is named by the alias
		if (!arguments[1]->IsFoldable()) {
			throw BinderException("explode: the column name must be a constant");
		}
		column = ExpressionExecutor::EvaluateScalar(input.GetClientContext(), *arguments[1]).ToString();
	}
	input.GetBoundFunction().SetReturnType(ExplodeRowType(type, POS, column));
	return make_uniq<ExplodeBindData>(POS, OUTER);
}

template <bool POS, bool OUTER>
ScalarFunction ExplodeRowsFunction(const string &name, bool named = false) {
	vector<LogicalType> args {LogicalType::ANY};
	if (named) {
		args.push_back(LogicalType::VARCHAR);
	}
	ScalarFunction fun(Identifier(name), args, LogicalType::ANY, ExplodeRowsKernel, ExplodeBind<POS, OUTER>);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

//! inline(array<struct>) rows: the array itself; inline_outer: a single NULL struct for NULL/empty input
void CheckInlineInput(const LogicalType &type) {
	if (type.id() != LogicalTypeId::LIST || ListType::GetChildType(type).id() != LogicalTypeId::STRUCT) {
		throw BinderException("[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] inline requires an array of structs, not %s",
		                      SparkTypeName(type));
	}
}

ExprP BindInlineRows(FunctionBindExpressionInput &input) {
	CheckArgs(input, "inline", 1, 1);
	auto e = Arg(input, 0);
	CheckInlineInput(e->GetReturnType());
	return e;
}

ExprP BindInlineOuterRows(FunctionBindExpressionInput &input) {
	CheckArgs(input, "inline_outer", 1, 1);
	Builder b(input.context);
	auto arr = Arg(input, 0);
	auto type = arr->GetReturnType();
	CheckInlineInput(type);
	auto empty = b.Or(b.IsNull(arr->Copy()),
	                  b.Compare(ExpressionType::COMPARE_EQUAL, b.Call("len", arr->Copy()), b.Constant(Value::BIGINT(0))));
	auto null_row = b.Constant(Value::LIST(ListType::GetChildType(type), {Value(ListType::GetChildType(type))}));
	return b.Case(std::move(empty), std::move(null_row), std::move(arr));
}

//===----------------------------------------------------------------------===//
// Conversions used by the macro library (Spark's implicit casts of function arguments)
//===----------------------------------------------------------------------===//
template <LogicalTypeId TARGET>
ExprP BindAsType(FunctionBindExpressionInput &input) {
	CheckArgs(input, "conversion", 1, 1);
	Builder b(input.context);
	return SparkCast(b, Arg(input, 0), LogicalType(TARGET));
}

//! A DOUBLE parameter of a math or statistics function: numbers and strings are cast (Spark's implicit cast, NULL when
//! a string is malformed), any other type is Spark's analysis error
ExprP BindAsDoubleParam(FunctionBindExpressionInput &input) {
	CheckArgs(input, "as_double_param", 1, 1);
	Builder b(input.context);
	auto e = Arg(input, 0);
	auto type = Normalize(e->GetReturnType());
	if (!(IsNull(type) || IsString(type) || IsNumeric(type))) {
		throw BinderException("[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve the function call due to data "
		                      "type mismatch: the parameter requires the \"DOUBLE\" type, however the argument has the "
		                      "type \"%s\".",
		                      StringUtil::Upper(SparkTypeName(type)));
	}
	return SparkCast(b, std::move(e), LogicalType::DOUBLE);
}

//! The argument of sum / avg / try_sum / try_avg: numbers and intervals unchanged, a string or an untyped NULL as
//! DOUBLE (NULL when it is not a number), anything else Spark's analysis error
ExprP BindAsSumOperand(FunctionBindExpressionInput &input) {
	CheckArgs(input, "as_sum_operand", 1, 1);
	Builder b(input.context);
	auto e = Arg(input, 0);
	auto type = Normalize(e->GetReturnType());
	if (IsNull(type) || IsString(type)) {
		return SparkCast(b, std::move(e), LogicalType::DOUBLE);
	}
	if (!(IsNumeric(type) || type.id() == LogicalTypeId::INTERVAL)) {
		throw BinderException("[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve the aggregate due to data type "
		                      "mismatch: Parameter 1 requires the \"NUMERIC\" or \"ANSI INTERVAL\" type, however "
		                      "the argument has the type \"%s\".",
		                      StringUtil::Upper(SparkTypeName(type)));
	}
	return e;
}

//! to_date / to_timestamp* (value, format): Spark parses only a string with the format; a DATE or TIMESTAMP value is
//! cast and the format ignored (ParseToDate / ParseToTimestamp). The third argument carries the result type.
ExprP BindParseWithFormat(FunctionBindExpressionInput &input) {
	CheckArgs(input, "parse_with_format", 3, 3);
	Builder b(input.context);
	auto target = input.children[2]->GetReturnType();
	auto value = Arg(input, 0);
	auto type = Normalize(value->GetReturnType());
	if (type.id() == LogicalTypeId::DATE || type.id() == LogicalTypeId::TIMESTAMP) {
		return SparkCast(b, std::move(value), target);
	}
	if (type.id() == LogicalTypeId::TIMESTAMP_TZ) {
		return b.Cast(std::move(value), target);
	}
	auto parsed = b.Spark("parse_datetime", b.Spark("as_string", std::move(value)), Arg(input, 1));
	return target.id() == LogicalTypeId::DATE ? b.Cast(std::move(parsed), LogicalType::DATE) : std::move(parsed);
}

//! pivot_key(x): the text PIVOT compares a value by (see RewritePivotValues): numbers as canonical decimals (1, 1.0
//! and 1.00 are the same key, like Spark's comparison after casting the value to the column's type), other values as
//! DuckDB renders them
void CanonicalNumberKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input) {
		auto text = input.GetString();
		if (text.find('.') != string::npos && text.find_first_of("eE") == string::npos) {
			auto last = text.find_last_not_of('0');
			text = text.substr(0, last + 1);
			if (!text.empty() && text.back() == '.') {
				text.pop_back();
			}
			if (text == "-0") {
				text = "0";
			}
		}
		return StringVector::AddString(result, text);
	});
}

ExprP BindPivotKey(FunctionBindExpressionInput &input) {
	CheckArgs(input, "pivot_key", 1, 1);
	Builder b(input.context);
	auto e = Arg(input, 0);
	auto type = Normalize(e->GetReturnType());
	auto text = b.Cast(std::move(e), LogicalType::VARCHAR);
	if (IsDecimal(type) || IsFloating(type)) {
		return b.Kernel(ScalarFunction(Identifier("spark_canonical_number"), {LogicalType::VARCHAR},
		                               LogicalType::VARCHAR, CanonicalNumberKernel),
		                std::move(text));
	}
	return text;
}

//! the value argument of round / bround: an untyped NULL or a string is a DOUBLE (Spark's implicit cast), numbers are
//! unchanged
ExprP BindAsRoundOperand(FunctionBindExpressionInput &input) {
	CheckArgs(input, "round", 1, 1);
	Builder b(input.context);
	auto e = Arg(input, 0);
	auto type = Normalize(e->GetReturnType());
	if (IsNull(type) || IsString(type)) {
		return SparkCast(b, std::move(e), LogicalType::DOUBLE);
	}
	return e;
}

//! a timestamp; dates and strings convert like Spark's implicit casts
ExprP BindAsTimestamp(FunctionBindExpressionInput &input) {
	CheckArgs(input, "as_timestamp", 1, 1);
	Builder b(input.context);
	auto e = Arg(input, 0);
	auto type = Normalize(e->GetReturnType());
	if (type.id() == LogicalTypeId::TIMESTAMP_TZ) {
		return b.Cast(std::move(e), LogicalType::TIMESTAMP);
	}
	return SparkCast(b, std::move(e), LogicalType::TIMESTAMP);
}

//! a value as BLOB: strings are their UTF-8 bytes
ExprP BindAsBinary(FunctionBindExpressionInput &input) {
	CheckArgs(input, "as_binary", 1, 1);
	Builder b(input.context);
	auto e = Arg(input, 0);
	auto type = Normalize(e->GetReturnType());
	if (type.id() == LogicalTypeId::BLOB) {
		return e;
	}
	return SparkCast(b, ToSparkString(b, std::move(e)), LogicalType::BLOB);
}

//! size of an array or map in one pass: NULL input is -1 (spark.sql.legacy.sizeOfNull) or NULL (ANSI)
template <bool NULL_IS_MINUS_ONE>
void SizeKernel(DataChunk &args, ExpressionState &, Vector &result) {
	auto &input = args.data[0];
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(format);
	auto entries = UnifiedVectorFormat::GetData<list_entry_t>(format);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetDataMutable<int32_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		auto idx = format.sel->get_index(i);
		if (format.validity.RowIsValid(idx)) {
			out[i] = int32_t(entries[idx].length);
		} else if (NULL_IS_MINUS_ONE) {
			out[i] = -1;
		} else {
			FlatVector::ValidityMutable(result).SetInvalid(i);
		}
	}
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//! size / cardinality: -1 for NULL (spark.sql.legacy.sizeOfNull = true)
ExprP BindSize(FunctionBindExpressionInput &input) {
	CheckArgs(input, "size", 1, 1);
	Builder b(input.context);
	auto e = Arg(input, 0);
	auto type = Normalize(e->GetReturnType());
	bool legacy = SparkConfBool(input.context, "spark.sql.legacy.sizeOfNull");
	bool ansi = AnsiEnabled(input.context) || !legacy;
	if (IsNull(type)) {
		return b.Constant(ansi ? Value(LogicalType::INTEGER) : Value::INTEGER(-1));
	}
	if (type.id() != LogicalTypeId::MAP && type.id() != LogicalTypeId::LIST && type.id() != LogicalTypeId::ARRAY) {
		throw BinderException("size: argument must be an array or a map, not %s", SparkTypeName(type));
	}
	if (type.id() == LogicalTypeId::ARRAY) {
		e = b.Cast(std::move(e), LogicalType::LIST(ArrayType::GetChildType(type)));
	}
	auto function = ScalarFunction(Identifier(ansi ? "spark_size_ansi" : "spark_size"), {e->GetReturnType()},
	                               LogicalType::INTEGER, ansi ? SizeKernel<false> : SizeKernel<true>);
	function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return b.Kernel(function, std::move(e));
}

ExprP BindReverse(FunctionBindExpressionInput &input) {
	CheckArgs(input, "reverse", 1, 1);
	Builder b(input.context);
	auto e = Arg(input, 0);
	auto type = Normalize(e->GetReturnType());
	if (type.id() == LogicalTypeId::LIST) {
		auto length = b.Call("len", e->Copy());
		vector<ExprP> slice_args;
		slice_args.push_back(std::move(e));
		slice_args.push_back(std::move(length));
		slice_args.push_back(b.Constant(Value::BIGINT(1)));
		slice_args.push_back(b.Constant(Value::BIGINT(-1)));
		return b.Call("list_slice", std::move(slice_args));
	}
	return b.Call("reverse", ToSparkString(b, std::move(e)));
}

//! length / char_length: characters for strings, bytes for binary
ExprP BindLength(FunctionBindExpressionInput &input) {
	CheckArgs(input, "length", 1, 1);
	Builder b(input.context);
	auto e = Arg(input, 0);
	auto type = Normalize(e->GetReturnType());
	if (type.id() == LogicalTypeId::BLOB) {
		return b.Cast(b.Call("octet_length", std::move(e)), LogicalType::INTEGER);
	}
	return b.Cast(b.Call("length", ToSparkString(b, std::move(e))), LogicalType::INTEGER);
}

//! octet_length / bit_length helper: bytes of the string or binary value
ExprP BindByteLength(FunctionBindExpressionInput &input) {
	CheckArgs(input, "octet_length", 1, 1);
	Builder b(input.context);
	auto e = Arg(input, 0);
	auto type = Normalize(e->GetReturnType());
	if (type.id() == LogicalTypeId::BLOB) {
		return b.Cast(b.Call("octet_length", std::move(e)), LogicalType::INTEGER);
	}
	return b.Cast(b.Call("strlen", ToSparkString(b, std::move(e))), LogicalType::INTEGER);
}

//! element_at(array, 1-based index) / element_at(map, key); NULL when out of range
template <bool TRY>
ExprP BindElementAt(FunctionBindExpressionInput &input) {
	CheckArgs(input, "element_at", 2, 2);
	Builder b(input.context);
	auto base = Arg(input, 0);
	auto index = Arg(input, 1);
	auto type = Normalize(base->GetReturnType());
	if (type.id() == LogicalTypeId::MAP) {
		return b.Call("map_extract_value", std::move(base), std::move(index));
	}
	if (type.id() == LogicalTypeId::LIST) {
		auto idx = b.Cast(std::move(index), LogicalType::BIGINT);
		if (idx->IsFoldable()) {
			auto value = ExpressionExecutor::EvaluateScalar(input.context, *idx);
			if (!value.IsNull() && value.GetValue<int64_t>() == 0) {
				throw InvalidInputException("[INVALID_INDEX_OF_ZERO] The index 0 is invalid. An index shall be "
				                            "either < 0 or > 0 (the first element has index 1).");
			}
		}
		if (!TRY && AnsiEnabled(input.context)) {
			ScalarFunction check(Identifier("spark_ansi_element_at"), {LogicalType::BIGINT, LogicalType::BIGINT},
			                     LogicalType::BIGINT, AnsiElementAtKernel);
			check.SetFallible();
			idx = b.Kernel(check, std::move(idx), b.Cast(b.Call("len", base->Copy()), LogicalType::BIGINT));
		}
		return b.Call("list_extract", std::move(base), std::move(idx));
	}
	if (IsNull(type)) {
		return b.NullOf(LogicalType::SQLNULL);
	}
	throw BinderException("element_at: argument must be an array or a map");
}

//! sum: Spark sums integers into BIGINT (wrapping on overflow), DuckDB into HUGEINT
ExprP BindSumResult(FunctionBindExpressionInput &input) {
	CheckArgs(input, "sum", 1, 1);
	Builder b(input.context);
	auto e = Arg(input, 0);
	auto type = e->GetReturnType();
	if (type.id() == LogicalTypeId::HUGEINT) {
		return b.Cast(std::move(e), LogicalType::BIGINT, true);
	}
	return e;
}

ExprP BindCurrentDatabase(FunctionBindExpressionInput &input) {
	Builder b(input.context);
	return b.Constant(Value("default"));
}

//! concat_ws argument: arrays contribute their non-NULL elements, everything else its string value
ExprP BindConcatWsArg(FunctionBindExpressionInput &input) {
	CheckArgs(input, "concat_ws", 2, 2);
	Builder b(input.context);
	auto e = Arg(input, 0);
	auto sep = Arg(input, 1);
	auto type = Normalize(e->GetReturnType());
	if (type.id() == LogicalTypeId::LIST) {
		// the array's non-NULL elements joined by the separator; NULL (skipped) when there are none
		auto strings = b.Cast(std::move(e), LogicalType::LIST(LogicalType::VARCHAR));
		return b.Spark("concat_ws_array", std::move(strings), b.Cast(std::move(sep), LogicalType::VARCHAR));
	}
	return ToSparkString(b, std::move(e));
}

ExprP BindConcatWs(FunctionBindExpressionInput &input) {
	Builder b(input.context);
	vector<ExprP> args;
	args.push_back(ToSparkString(b, Arg(input, 0)));
	for (idx_t i = 1; i < input.children.size(); i++) {
		args.push_back(b.Cast(Arg(input, i), LogicalType::VARCHAR));
	}
	return b.Call("concat_ws", std::move(args));
}

//! sequence(start, stop[, step]): inclusive; integers, dates or timestamps
ExprP BindSequence(FunctionBindExpressionInput &input) {
	CheckArgs(input, "sequence", 2, 3);
	Builder b(input.context);
	auto start = Arg(input, 0);
	auto stop = Arg(input, 1);
	auto type = Normalize(start->GetReturnType());
	if (type.id() == LogicalTypeId::DATE || type.id() == LogicalTypeId::TIMESTAMP) {
		auto start_ts = b.Cast(start->Copy(), LogicalType::TIMESTAMP);
		auto stop_ts = b.Cast(SparkCast(b, stop->Copy(), type), LogicalType::TIMESTAMP);
		ExprP step;
		if (input.children.size() == 3) {
			step = b.Cast(Arg(input, 2), LogicalType::INTERVAL);
		} else {
			auto ascending =
			    b.Compare(ExpressionType::COMPARE_LESSTHANOREQUALTO, start->Copy(), SparkCast(b, std::move(stop), type));
			step = b.Case(std::move(ascending), b.Constant(Value::INTERVAL(0, 1, 0)), b.Constant(Value::INTERVAL(0, -1, 0)));
		}
		vector<ExprP> args;
		args.push_back(std::move(start_ts));
		args.push_back(std::move(stop_ts));
		args.push_back(std::move(step));
		auto series = b.Call("generate_series", std::move(args));
		return b.Cast(std::move(series), LogicalType::LIST(type));
	}
	auto result_type = WiderIntegral(IsIntegral(type) ? type : LogicalType::BIGINT,
	                                 IsIntegral(Normalize(stop->GetReturnType())) ? Normalize(stop->GetReturnType())
	                                                                             : LogicalType::BIGINT);
	ExprP step;
	if (input.children.size() == 3) {
		step = b.Cast(Arg(input, 2), LogicalType::BIGINT);
	} else {
		auto ascending = b.Compare(ExpressionType::COMPARE_LESSTHANOREQUALTO, start->Copy(), stop->Copy());
		step = b.Case(std::move(ascending), b.Constant(Value::BIGINT(1)), b.Constant(Value::BIGINT(-1)));
	}
	auto start_long = b.Cast(std::move(start), LogicalType::BIGINT);
	auto stop_long = b.Cast(std::move(stop), LogicalType::BIGINT);
	ScalarFunction check(Identifier("spark_sequence_step"), {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                     LogicalType::BIGINT, SequenceStepKernel);
	check.SetFallible();
	vector<ExprP> check_args;
	check_args.push_back(start_long->Copy());
	check_args.push_back(stop_long->Copy());
	check_args.push_back(std::move(step));
	auto checked_step = b.Kernel(check, std::move(check_args));
	vector<ExprP> args;
	args.push_back(std::move(start_long));
	args.push_back(std::move(stop_long));
	args.push_back(std::move(checked_step));
	return b.Cast(b.Call("generate_series", std::move(args)), LogicalType::LIST(result_type));
}

//===----------------------------------------------------------------------===//
// CASE / IF with Spark's type coercion
//===----------------------------------------------------------------------===//
bool IsDatetime(const LogicalType &t) {
	return t.id() == LogicalTypeId::DATE || t.id() == LogicalTypeId::TIMESTAMP || t.id() == LogicalTypeId::TIMESTAMP_TZ;
}

//! Spark's findWiderTypeForTwo with string promotion (TypeCoercion for CaseWhen / If / Coalesce)
LogicalType SparkWiderType(ClientContext &context, const LogicalType &a_p, const LogicalType &b_p) {
	auto a = Normalize(a_p), b = Normalize(b_p);
	if (IsNull(a)) {
		return b;
	}
	if (IsNull(b) || a == b) {
		return a;
	}
	if (IsNumeric(a) && IsNumeric(b)) {
		if (a.id() == LogicalTypeId::DOUBLE || b.id() == LogicalTypeId::DOUBLE) {
			return LogicalType::DOUBLE;
		}
		if (a.id() == LogicalTypeId::FLOAT || b.id() == LogicalTypeId::FLOAT) {
			if (AnsiEnabled(context) && !(a.id() == LogicalTypeId::FLOAT && b.id() == LogicalTypeId::FLOAT)) {
				return LogicalType::DOUBLE; // AnsiTypeCoercion: an integer or a decimal with FLOAT widens to DOUBLE
			}
			return (IsDecimal(a) || IsDecimal(b)) ? LogicalType::DOUBLE : LogicalType::FLOAT;
		}
		if (IsIntegral(a) && IsIntegral(b)) {
			return WiderIntegral(a, b);
		}
		auto da = AsDecimalType(a), db = AsDecimalType(b);
		int s = MaxValue(DecimalType::GetScale(da), DecimalType::GetScale(db));
		int int_digits = MaxValue(DecimalType::GetWidth(da) - DecimalType::GetScale(da),
		                          DecimalType::GetWidth(db) - DecimalType::GetScale(db));
		return LogicalType::DECIMAL(uint8_t(MinValue(38, int_digits + s)), uint8_t(MinValue(38, s)));
	}
	if (IsDatetime(a) && IsDatetime(b)) {
		return LogicalType::TIMESTAMP;
	}
	// nested types widen element by element (array<int> with array<string> is array<string>)
	if (a.id() == LogicalTypeId::LIST && b.id() == LogicalTypeId::LIST) {
		return LogicalType::LIST(SparkWiderType(context, ListType::GetChildType(a), ListType::GetChildType(b)));
	}
	if (a.id() == LogicalTypeId::MAP && b.id() == LogicalTypeId::MAP) {
		return LogicalType::MAP(SparkWiderType(context, MapType::KeyType(a), MapType::KeyType(b)),
		                        SparkWiderType(context, MapType::ValueType(a), MapType::ValueType(b)));
	}
	if (a.id() == LogicalTypeId::STRUCT && b.id() == LogicalTypeId::STRUCT &&
	    StructType::GetChildCount(a) == StructType::GetChildCount(b)) {
		child_list_t<LogicalType> fields;
		for (idx_t i = 0; i < StructType::GetChildCount(a); i++) {
			fields.emplace_back(StructType::GetChildName(a, i),
			                    SparkWiderType(context, StructType::GetChildType(a, i), StructType::GetChildType(b, i)));
		}
		return LogicalType::STRUCT(std::move(fields));
	}
	bool a_atomic = IsNumeric(a) || IsDatetime(a) || a.id() == LogicalTypeId::BOOLEAN;
	bool b_atomic = IsNumeric(b) || IsDatetime(b) || b.id() == LogicalTypeId::BOOLEAN;
	if ((IsString(a) && b_atomic) || (IsString(b) && a_atomic)) {
		if (AnsiEnabled(context)) {
			// AnsiTypeCoercion: the string takes the other side's type (an integer as BIGINT, a fraction as DOUBLE)
			auto &other = IsString(a) ? b : a;
			if (IsIntegral(other)) {
				return LogicalType::BIGINT;
			}
			if (IsNumeric(other)) {
				return LogicalType::DOUBLE;
			}
			return other;
		}
		return LogicalType::VARCHAR;
	}
	return LogicalType::MaxLogicalType(context, a, b);
}

//! case_when(cond1, value1, cond2, value2, ..., else)
ExprP BindCaseWhen(FunctionBindExpressionInput &input) {
	if (input.children.size() < 3 || input.children.size() % 2 == 0) {
		throw BinderException("Invalid number of arguments for CASE");
	}
	Builder b(input.context);
	vector<ExprP> args;
	for (idx_t i = 0; i < input.children.size(); i++) {
		args.push_back(Arg(input, i));
	}
	LogicalType type = LogicalType::SQLNULL;
	for (idx_t i = 1; i < args.size(); i += 2) {
		type = SparkWiderType(input.context, type, args[i]->GetReturnType());
	}
	type = SparkWiderType(input.context, type, args.back()->GetReturnType());
	if (IsNull(type)) {
		type = LogicalType::INTEGER;
	}
	auto result = make_uniq<BoundCaseExpression>(type);
	for (idx_t i = 0; i + 1 < args.size(); i += 2) {
		BoundCaseCheck check;
		check.when_expr = b.Cast(std::move(args[i]), LogicalType::BOOLEAN);
		check.then_expr = SparkImplicitCast(b, std::move(args[i + 1]), type);
		result->CaseChecksMutable().push_back(std::move(check));
	}
	result->ElseMutable() = SparkImplicitCast(b, std::move(args.back()), type);
	return std::move(result);
}

//! binary values stay binary, everything else becomes its Spark string
ExprP BindAsTextOrBinary(FunctionBindExpressionInput &input) {
	CheckArgs(input, "as_text_or_binary", 1, 1);
	Builder b(input.context);
	auto e = Arg(input, 0);
	if (Normalize(e->GetReturnType()).id() == LogicalTypeId::BLOB) {
		return e;
	}
	return ToSparkString(b, std::move(e));
}

//! lpad / rpad default padding: a space for strings, a zero byte for binary
ExprP BindDefaultPad(FunctionBindExpressionInput &input) {
	CheckArgs(input, "default_pad", 1, 1);
	Builder b(input.context);
	if (ArgType(input, 0).id() == LogicalTypeId::BLOB) {
		return b.Constant(Value::BLOB(string(1, '\0')));
	}
	return b.Constant(Value(" "));
}

//! abs: Java semantics for integers (abs(MIN_VALUE) = MIN_VALUE), intervals supported
template <class T>
void WrapAbsKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<T, T>(args.data[0], result, args.size(), [](T a) { return a < 0 ? WrapSub<T>(0, a) : a; });
}

ExprP BindAbs(FunctionBindExpressionInput &input) {
	CheckArgs(input, "abs", 1, 1);
	Builder b(input.context);
	auto e = NumericOperand(b, Arg(input, 0));
	auto type = Normalize(e->GetReturnType());
	if (type.id() == LogicalTypeId::INTERVAL) {
		return b.Spark("interval_abs", std::move(e));
	}
	if (IsIntegral(type) && AnsiEnabled(input.context)) {
		// DuckDB's abs raises on the minimum value
		return b.Call("abs", std::move(e));
	}
	if (IsIntegral(type)) {
		scalar_function_t fn;
		switch (type.id()) {
		case LogicalTypeId::TINYINT:
			fn = WrapAbsKernel<int8_t>;
			break;
		case LogicalTypeId::SMALLINT:
			fn = WrapAbsKernel<int16_t>;
			break;
		case LogicalTypeId::INTEGER:
			fn = WrapAbsKernel<int32_t>;
			break;
		default:
			fn = WrapAbsKernel<int64_t>;
			break;
		}
		return b.Kernel(ScalarFunction(Identifier("spark_abs"), {type}, type, fn), std::move(e));
	}
	return b.Call("abs", std::move(e));
}

ExprP BindSign(FunctionBindExpressionInput &input) {
	CheckArgs(input, "sign", 1, 1);
	Builder b(input.context);
	auto e = NumericOperand(b, Arg(input, 0));
	if (Normalize(e->GetReturnType()).id() == LogicalTypeId::INTERVAL) {
		return b.Spark("interval_sign", std::move(e));
	}
	return b.Kernel(ScalarFunction(Identifier("spark_signum"), {LogicalType::DOUBLE}, LogicalType::DOUBLE, SignumKernel),
	                b.Cast(std::move(e), LogicalType::DOUBLE));
}

//! floor / ceil with one argument: BIGINT for integral and floating input (Java's (long) of Math.floor, so NaN
//! is 0 and huge values saturate), DECIMAL(p - s + 1, 0) for decimals
//! date_part / datepart / extract(field FROM source): the field must be a foldable string (like in Spark), which makes
//! the result type known at bind time: INT for calendar fields, DECIMAL(8,6) for the seconds
ExprP BindDatePart(FunctionBindExpressionInput &input) {
	CheckArgs(input, "date_part", 2, 2);
	Builder b(input.context);
	if (!input.children[0]->IsFoldable()) {
		throw BinderException("[NON_FOLDABLE_INPUT] The input field must be a foldable string expression");
	}
	auto field = StringUtil::Upper(ExpressionExecutor::EvaluateScalar(input.context, *input.children[0]).ToString());
	auto source = Arg(input, 1);
	static const unordered_map<string, string> CALENDAR = {
	    {"YEAR", "year"}, {"Y", "year"}, {"YEARS", "year"}, {"YR", "year"}, {"YRS", "year"}, {"YEAROFWEEK", "isoyear"},
	    {"QUARTER", "quarter"}, {"QTR", "quarter"}, {"MONTH", "month"}, {"MON", "month"}, {"MONS", "month"},
	    {"MONTHS", "month"}, {"WEEK", "weekofyear"}, {"W", "weekofyear"}, {"WEEKS", "weekofyear"}, {"DAY", "day"},
	    {"D", "day"}, {"DAYS", "day"}, {"DAYOFWEEK_ISO", "isodow"}, {"DOW_ISO", "isodow"}, {"DOY", "dayofyear"},
	    {"HOUR", "hour"}, {"H", "hour"}, {"HOURS", "hour"}, {"HR", "hour"}, {"HRS", "hour"}, {"MINUTE", "minute"},
	    {"M", "minute"}, {"MIN", "minute"}, {"MINS", "minute"}, {"MINUTES", "minute"}};
	static const unordered_set<string> SECONDS = {"SECOND", "S", "SEC", "SECONDS", "SECS"};
	auto found = CALENDAR.find(field);
	if (found != CALENDAR.end()) {
		return b.Cast(b.Call(found->second, std::move(source)), LogicalType::INTEGER);
	}
	if (field == "DAYOFWEEK" || field == "DOW") {
		return b.Cast(b.Op("+", b.Call("dayofweek", std::move(source)), b.Constant(Value::BIGINT(1))), LogicalType::INTEGER);
	}
	if (SECONDS.count(field)) {
		auto micros = b.Cast(b.Call("microsecond", std::move(source)), LogicalType::DECIMAL(14, 0));
		auto seconds = b.Op("/", std::move(micros), b.Constant(Value::BIGINT(1000000)));
		return b.Cast(std::move(seconds), LogicalType::DECIMAL(8, 6));
	}
	throw BinderException("[INVALID_EXTRACT_FIELD] Cannot extract %s from the source", field);
}

template <bool CEIL>
ExprP BindFloorCeil(FunctionBindExpressionInput &input) {
	CheckArgs(input, CEIL ? "ceil" : "floor", 1, 1);
	Builder b(input.context);
	auto e = NumericOperand(b, Arg(input, 0));
	auto type = Normalize(e->GetReturnType());
	if (IsIntegral(type)) {
		return b.Cast(std::move(e), LogicalType::BIGINT);
	}
	if (IsDecimal(type)) {
		// Spark: scale 0 keeps the type, otherwise DECIMAL(p - s + 1, 0)
		int scale = DecimalType::GetScale(type);
		if (scale == 0) {
			return e;
		}
		int digits = MinValue(38, DecimalType::GetWidth(type) - scale + 1);
		return b.Cast(b.Call(CEIL ? "ceil" : "floor", std::move(e)), LogicalType::DECIMAL(uint8_t(digits), 0));
	}
	return b.Kernel(ScalarFunction(Identifier(CEIL ? "spark_ceil_to_long" : "spark_floor_to_long"), {LogicalType::DOUBLE},
	                               LogicalType::BIGINT, FloorCeilToLongKernel<CEIL>),
	                b.Cast(std::move(e), LogicalType::DOUBLE));
}

//! try_cast: like cast, but numeric conversions that overflow are NULL (Spark's ANSI cast semantics)
ExprP BindTryCastValue(FunctionBindExpressionInput &input) {
	CheckArgs(input, "try_cast", 2, 2);
	Builder b(input.context);
	auto target = input.children[1]->GetReturnType();
	auto e = Arg(input, 0);
	auto source = Normalize(e->GetReturnType());
	CheckSparkCast(input.context, source, target, true);
	if (IsIntegral(target) && IsString(source) && AnsiEnabled(input.context)) {
		return b.Kernel(StringToIntegralFunction(target, true), std::move(e));
	}
	if (IsIntegral(target) && IsNumeric(source) && source != target) {
		if (!IsIntegral(source)) {
			e = b.Call("trunc", b.Cast(std::move(e), IsDecimal(source) ? source : LogicalType::DOUBLE));
		}
		return b.Cast(std::move(e), target, true);
	}
	return SparkCast(b, std::move(e), target);
}

//! intervals as numbers (months or microseconds), everything else unchanged
ExprP BindAsNumber(FunctionBindExpressionInput &input) {
	CheckArgs(input, "as_number", 1, 1);
	Builder b(input.context);
	auto e = NumericOperand(b, Arg(input, 0));
	if (Normalize(e->GetReturnType()).id() == LogicalTypeId::INTERVAL) {
		return b.Spark("interval_number", std::move(e));
	}
	return e;
}

//! from_number(n, sample): back to an interval when the sample is one, DOUBLE otherwise
ExprP BindFromNumber(FunctionBindExpressionInput &input) {
	CheckArgs(input, "from_number", 2, 2);
	Builder b(input.context);
	auto n = Arg(input, 0);
	auto sample = Arg(input, 1);
	if (Normalize(sample->GetReturnType()).id() == LogicalTypeId::INTERVAL) {
		if (n->GetReturnType().id() == LogicalTypeId::LIST) {
			return b.Spark("numbers_to_intervals", b.Cast(std::move(n), LogicalType::LIST(LogicalType::DOUBLE)),
			               std::move(sample));
		}
		return b.Spark("number_to_interval", b.Cast(std::move(n), LogicalType::DOUBLE), std::move(sample));
	}
	// Spark's median / percentile of numbers (decimals too) are DOUBLE
	if (n->GetReturnType().id() == LogicalTypeId::LIST) {
		return b.Cast(std::move(n), LogicalType::LIST(LogicalType::DOUBLE));
	}
	return b.Cast(std::move(n), LogicalType::DOUBLE);
}

//! try_avg: NULL when Spark's sum would overflow its type
ExprP BindTryAvgResult(FunctionBindExpressionInput &input) {
	CheckArgs(input, "try_avg", 3, 3);
	Builder b(input.context);
	auto sum = Arg(input, 0);
	auto count = Arg(input, 1);
	auto sample = Arg(input, 2);
	auto sample_type = Normalize(sample->GetReturnType());
	double limit;
	if (sample_type.id() == LogicalTypeId::INTERVAL) {
		limit = 2147483647.0; // months; day-time sums are checked against BIGINT microseconds below
	} else if (IsIntegral(sample_type)) {
		limit = 9223372036854775807.0;
	} else {
		limit = std::numeric_limits<double>::infinity();
	}
	auto sum_double = b.Cast(sum->Copy(), LogicalType::DOUBLE);
	auto overflow = b.Compare(ExpressionType::COMPARE_GREATERTHAN, b.Call("abs", sum_double->Copy()),
	                          b.Constant(Value::DOUBLE(limit)));
	if (sample_type.id() == LogicalTypeId::INTERVAL) {
		// year-month sums overflow an INT of months, day-time sums a BIGINT of microseconds
		auto year_month = b.Spark("interval_is_year_month", sample->Copy());
		auto ym_overflow = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(year_month),
		                                                        std::move(overflow));
		auto dt_overflow = b.Compare(ExpressionType::COMPARE_GREATERTHAN, b.Call("abs", std::move(sum_double)),
		                             b.Constant(Value::DOUBLE(9223372036854775807.0)));
		overflow = b.Or(std::move(ym_overflow), std::move(dt_overflow));
	}
	ExprP average = b.Op("/", b.Cast(std::move(sum), LogicalType::DOUBLE), b.Cast(std::move(count), LogicalType::DOUBLE));
	if (sample_type.id() == LogicalTypeId::INTERVAL) {
		average = b.Spark("number_to_interval", std::move(average), std::move(sample));
	} else if (IsDecimal(sample_type)) {
		average = b.Cast(std::move(average),
		                 LogicalType::DECIMAL(uint8_t(MinValue(38, DecimalType::GetWidth(sample_type) + 4)),
		                                      uint8_t(MinValue(38, DecimalType::GetScale(sample_type) + 4))),
		                 true);
	}
	auto type = average->GetReturnType();
	return b.Case(std::move(overflow), b.NullOf(type), std::move(average));
}

} // namespace

void RegisterSparkFunctions(ExtensionLoader &loader) {
	vector<pair<string, function_bind_expression_t>> dispatchers = {
	    {"add", BindAdd},
	    {"subtract", BindSubtract},
	    {"multiply", BindMultiply},
	    {"divide", BindDivide},
	    {"try_divide_impl", BindTryDivide},
	    {"div", BindDiv},
	    {"remainder", BindRemainder},
	    {"mod", BindRemainder},
	    {"negative", BindNegative},
	    {"positive", BindPositive},
	    {"zero_key", BindZeroKey},
	    {"like_op", BindLikeOperator<false, false>},
	    {"not_like_op", BindLikeOperator<true, false>},
	    {"ilike_op", BindLikeOperator<false, true>},
	    {"not_ilike_op", BindLikeOperator<true, true>},
	    {"simple_sort_key", BindSimpleSortKey},
	    {"narrow_int", BindNarrowInt},
	    {"comparator_result", BindComparatorResult},
	    {"cmp_eq", BindComparison<ExpressionType::COMPARE_EQUAL>},
	    {"cmp_ne", BindComparison<ExpressionType::COMPARE_NOTEQUAL>},
	    {"cmp_lt", BindComparison<ExpressionType::COMPARE_LESSTHAN>},
	    {"cmp_gt", BindComparison<ExpressionType::COMPARE_GREATERTHAN>},
	    {"cmp_le", BindComparison<ExpressionType::COMPARE_LESSTHANOREQUALTO>},
	    {"cmp_ge", BindComparison<ExpressionType::COMPARE_GREATERTHANOREQUALTO>},
	    {"cmp_nsafe", BindComparison<ExpressionType::COMPARE_NOT_DISTINCT_FROM>},
	    {"op_bit_and", BindBitAnd},
	    {"op_bit_or", BindBitOr},
	    {"op_bit_xor", BindBitXor},
	    {"op_bit_not", BindBitNot},
	    {"as_date", BindAsType<LogicalTypeId::DATE>},
	    {"as_timestamp", BindAsTimestamp},
	    {"as_double", BindAsType<LogicalTypeId::DOUBLE>},
	    {"as_round_operand", BindAsRoundOperand},
	    {"typed_literal", BindTypedLiteral},
	    {"check_arg", BindCheckArg},
	    {"as_double_param", BindAsDoubleParam},
	    {"as_sum_operand", BindAsSumOperand},
	    {"parse_with_format", BindParseWithFormat},
	    {"pivot_key", BindPivotKey},
	    {"as_long", BindAsType<LogicalTypeId::BIGINT>},
	    {"as_int", BindAsType<LogicalTypeId::INTEGER>},
	    {"as_string", BindAsType<LogicalTypeId::VARCHAR>},
	    {"as_boolean", BindAsType<LogicalTypeId::BOOLEAN>},
	    {"as_binary", BindAsBinary},
	    {"size", BindSize},
	    {"date_part_value", BindDatePart},
	    {"cardinality", BindSize},
	    {"reverse", BindReverse},
	    {"length", BindLength},
	    {"len", BindLength},
	    {"char_length", BindLength},
	    {"character_length", BindLength},
	    {"octet_length_impl", BindByteLength},
	    {"element_at", BindElementAt<false>},
	    {"try_element_at", BindElementAt<true>},
	    {"sum_result", BindSumResult},
	    {"current_database", BindCurrentDatabase},
	    {"current_schema", BindCurrentDatabase},
	    {"concat_ws_arg", BindConcatWsArg},
	    {"sequence", BindSequence},
	    {"case_when", BindCaseWhen},
	    {"as_text_or_binary", BindAsTextOrBinary},
	    {"default_pad", BindDefaultPad},
	    {"abs", BindAbs},
	    {"sign", BindSign},
	    {"signum", BindSign},
	    {"as_number", BindAsNumber},
	    {"from_number", BindFromNumber},
	    {"try_avg_result", BindTryAvgResult},
	    {"try_add", BindTryAdd},
	    {"try_subtract", BindTrySubtract},
	    {"try_multiply", BindTryMultiply},
	    {"concat_ws_impl", BindConcatWs},
	    {"concat_op", BindConcat},
	    {"concat", BindConcat},
	    {"subscript", BindSubscript},
	    {"get_field", BindGetField},
	    {"cast_value", BindCastValue},
	    {"try_cast_value", BindTryCastValue},
	    {"floor_value", BindFloorCeil<false>},
	    {"ceil_value", BindFloorCeil<true>},
	    {"typeof", BindTypeOf},
	    {"array", BindArray},
    {"coalesce_value", BindCoalesceValue},
    {"in_list_value", BindInListValue},
	    {"map_from_arrays", BindMapFromArrays},
    {"map_concat", BindMapConcat},
    {"conf_bool", BindConfBool},
    {"elt_list", BindEltList},
    {"map_from_entries_value", BindMapFromEntries},
    {"map_from_entries", BindMapFromEntries},
	    {"inline_rows", BindInlineRows},
	    {"inline_outer_rows", BindInlineOuterRows},
	};
	loader.RegisterFunction(SparkKernelCatalogEntry());
	for (auto &entry : dispatchers) {
		DispatcherBinds()[entry.first] = entry.second;
		RegisterFallible(loader, Dispatcher(entry.first, ClassifiedBind));
		AddSparkOverride(entry.first);
	}
	RegisterFallible(loader, ExplodeRowsFunction<false, false>("explode_rows"));
	RegisterFallible(loader, ExplodeRowsFunction<false, true>("explode_outer_rows"));
	RegisterFallible(loader, ExplodeRowsFunction<true, false>("posexplode_rows"));
	RegisterFallible(loader, ExplodeRowsFunction<true, true>("posexplode_outer_rows"));
	RegisterFallible(loader, ExplodeRowsFunction<false, false>("explode_as_rows", true));
	RegisterFallible(loader, ExplodeRowsFunction<false, true>("explode_outer_as_rows", true));
	RegisterFallible(loader, ExplodeRowsFunction<true, false>("posexplode_as_rows", true));
	RegisterFallible(loader, ExplodeRowsFunction<true, true>("posexplode_outer_as_rows", true));
}

bool SparkAnsiEnabled(ClientContext &context) {
	return AnsiEnabled(context);
}

unique_ptr<Expression> BindSparkKernel(ClientContext &context, ScalarFunction function,
                                       vector<unique_ptr<Expression>> args) {
	return BindSparkKernelImpl(context, std::move(function), std::move(args));
}

bool HasArgTypeRule(const string &lower_name, idx_t position) {
	return HasArgTypeRuleImpl(lower_name, position);
}

} // namespace duckdb
