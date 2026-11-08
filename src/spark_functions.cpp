//===----------------------------------------------------------------------===//
// Spark function library (C++ part).
//
// Most functions here are "dispatchers": they accept any argument types and, once the argument
// types are known, rewrite themselves (bind_expression) into DuckDB functions or into small
// kernels that implement Spark's semantics for those types.
//===----------------------------------------------------------------------===//

#include "spark_dialect.hpp"

#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
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
	hugeint_t value = 0;
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
		value = value * 10 + (c - '0');
		if (value > hugeint_t(NumericLimits<int64_t>::Maximum()) + 1) {
			return false;
		}
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
	if (negative) {
		value = -value;
	}
	if (value > hugeint_t(NumericLimits<int64_t>::Maximum()) || value < hugeint_t(NumericLimits<int64_t>::Minimum())) {
		return false;
	}
	result = Hugeint::Cast<int64_t>(value);
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
	if (Timestamp::TryConvertTimestamp(trimmed.c_str(), trimmed.size(), out, true) == TimestampCastResult::SUCCESS) {
		return true;
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

//===----------------------------------------------------------------------===//
// Spark value -> string (Cast to STRING)
//===----------------------------------------------------------------------===//

//! Java's Double.toString / Float.toString
template <class T>
string JavaFloatingToString(T value) {
	if (std::isnan(value)) {
		return "NaN";
	}
	if (std::isinf(value)) {
		return value > 0 ? "Infinity" : "-Infinity";
	}
	if (value == 0) {
		return std::signbit(value) ? "-0.0" : "0.0";
	}
	char buffer[64];
	auto res = std::to_chars(buffer, buffer + sizeof(buffer), value, std::chars_format::scientific);
	string sci(buffer, res.ptr);
	// sci: [-]d[.ddd]e[+-]xx
	bool negative = sci[0] == '-';
	if (negative) {
		sci = sci.substr(1);
	}
	auto e_pos = sci.find('e');
	string mantissa = sci.substr(0, e_pos);
	int exponent = std::stoi(sci.substr(e_pos + 1));
	string digits;
	for (auto c : mantissa) {
		if (c != '.') {
			digits += c;
		}
	}
	string result;
	double magnitude = std::fabs(double(value));
	if (magnitude >= 1e-3 && magnitude < 1e7) {
		// plain decimal notation
		if (exponent >= 0) {
			string int_part = digits.substr(0, MinValue<idx_t>(digits.size(), idx_t(exponent) + 1));
			while (int_part.size() < idx_t(exponent) + 1) {
				int_part += '0';
			}
			string frac = digits.size() > idx_t(exponent) + 1 ? digits.substr(exponent + 1) : "0";
			result = int_part + "." + frac;
		} else {
			result = "0." + string(-exponent - 1, '0') + digits;
		}
	} else {
		string frac = digits.size() > 1 ? digits.substr(1) : "0";
		result = digits.substr(0, 1) + "." + frac + "E" + to_string(exponent);
	}
	return negative ? "-" + result : result;
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
		UnaryExecutor::Execute<double, string_t>(
		    input, result, count, [&](double d) { return StringVector::AddString(result, JavaFloatingToString(d)); });
		return;
	}
	if (input.GetType().id() == LogicalTypeId::FLOAT) {
		UnaryExecutor::Execute<float, string_t>(
		    input, result, count, [&](float d) { return StringVector::AddString(result, JavaFloatingToString(d)); });
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
		if (IsDecimal(source) || IsLargeIntegral(source)) {
			auto truncated = IsDecimal(source) ? b.Call("trunc", std::move(e)) : std::move(e);
			auto as_long = b.Cast(std::move(truncated), LogicalType::BIGINT, true);
			if (target.id() == LogicalTypeId::BIGINT) {
				return as_long;
			}
			return b.Kernel(NarrowFunction(LogicalType::BIGINT, target), std::move(as_long));
		}
		if (source.id() == LogicalTypeId::TIMESTAMP || source.id() == LogicalTypeId::TIMESTAMP_TZ) {
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
			return b.Kernel(ScalarFunction(Identifier("spark_string_to_timestamp"), {LogicalType::VARCHAR},
			                               LogicalType::TIMESTAMP, StringToTimestampKernel),
			                std::move(e));
		}
		if (IsNumeric(source)) {
			auto tz = b.Call("to_timestamp", b.Cast(std::move(e), LogicalType::DOUBLE));
			return b.Cast(std::move(tz), target, true);
		}
		if (source.id() == LogicalTypeId::BOOLEAN) {
			return b.NullOf(target);
		}
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

ExprP BindCastValue(FunctionBindExpressionInput &input) {
	CheckArgs(input, "cast", 2, 2);
	Builder b(input.context);
	auto target = input.children[1]->GetReturnType();
	auto e = Arg(input, 0);
	auto source = Normalize(e->GetReturnType());
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

//! Spark's DecimalType.adjustPrecisionScale (allowPrecisionLoss = true)
LogicalType AdjustDecimal(int precision, int scale) {
	if (precision <= 38) {
		return LogicalType::DECIMAL(uint8_t(precision), uint8_t(scale));
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
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT && IsIntegral(type)) {
		auto &value = expr.Cast<BoundConstantExpression>().GetValue();
		if (!value.IsNull()) {
			auto text = to_string(std::abs(value.GetValue<int64_t>()));
			return LogicalType::DECIMAL(uint8_t(MaxValue<idx_t>(text.size(), 1)), 0);
		}
	}
	return AsDecimalType(type);
}

ExprP SparkDecimalResult(Builder &b, ExprP value, const LogicalType &target);
ExprP SparkWideDecimalMultiply(Builder &b, ExprP left, int s1, ExprP right, int s2, const LogicalType &target);

template <class OP, class CHECKED_OP, bool CHECKED>
ExprP BindAddSubMul(FunctionBindExpressionInput &input, const char *duck_op) {
	CheckArgs(input, duck_op, 2, 2);
	Builder b(input.context);
	auto left_decimal = DecimalOperandType(*input.children[0], ArgType(input, 0));
	auto right_decimal = DecimalOperandType(*input.children[1], ArgType(input, 1));
	auto left = NumericOperand(b, Arg(input, 0));
	auto right = NumericOperand(b, Arg(input, 1));
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
		auto result_type = AdjustDecimal(precision, scale);
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
			if (p1 + p2 <= 38) {
				auto wide_left = LogicalType::DECIMAL(uint8_t(MinValue(38, MaxValue(p1, 38 - p2))), uint8_t(s1));
				value = b.Op("*", b.Cast(std::move(left), wide_left), b.Cast(std::move(right), rd));
			} else {
				return finish(SparkWideDecimalMultiply(b, std::move(left), s1, std::move(right), s2, result_type));
			}
		} else {
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
LogicalType DecimalDivideType(const LogicalType &l, const LogicalType &r) {
	int p1 = DecimalType::GetWidth(l), s1 = DecimalType::GetScale(l);
	int p2 = DecimalType::GetWidth(r), s2 = DecimalType::GetScale(r);
	int scale = MaxValue(6, s1 + p2 + 1);
	int precision = p1 - s1 + s2 + scale;
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
	auto left = NumericOperand(b, Arg(input, 0));
	auto right = NumericOperand(b, Arg(input, 1));
	auto lt = Normalize(left->GetReturnType());
	auto rt = Normalize(right->GetReturnType());
	if (lt.id() == LogicalTypeId::INTERVAL && IsNumeric(rt)) {
		return b.Spark("interval_divide", std::move(left), b.Cast(std::move(right), LogicalType::DOUBLE));
	}
	if (!TRY && AnsiEnabled(input.context) && IsNumeric(rt)) {
		right = AnsiNonZero(b, std::move(right), left->Copy());
	}
	if ((IsDecimal(lt) || IsDecimal(rt)) && !IsFloating(lt) && !IsFloating(rt)) {
		auto result_type = DecimalDivideType(IsDecimal(lt) ? lt : left_decimal, IsDecimal(rt) ? rt : right_decimal);
		auto quotient = b.Kernel(ScalarFunction(Identifier("spark_divide"), {LogicalType::DOUBLE, LogicalType::DOUBLE},
		                                        LogicalType::DOUBLE, DoubleDivideKernel),
		                         b.Cast(std::move(left), LogicalType::DOUBLE), b.Cast(std::move(right), LogicalType::DOUBLE));
		return b.Cast(std::move(quotient), result_type, true);
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
	auto left = NumericOperand(b, Arg(input, 0));
	auto right = NumericOperand(b, Arg(input, 1));
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
	auto left = NumericOperand(b, Arg(input, 0));
	auto right = NumericOperand(b, Arg(input, 1));
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
		return b.Kernel(ScalarFunction(Identifier("spark_remainder"), {LogicalType::DOUBLE, LogicalType::DOUBLE},
		                               LogicalType::DOUBLE, DoubleRemainderKernel),
		                b.Cast(std::move(left), LogicalType::DOUBLE), b.Cast(std::move(right), LogicalType::DOUBLE));
	}
	// integers mixed with decimals; anything else (intervals, ...) is a type error as in Spark
	if (!(IsDecimal(lt) || IsIntegral(lt)) || !(IsDecimal(rt) || IsIntegral(rt))) {
		throw BinderException("[DATATYPE_MISMATCH] %% requires numeric operands, got %s and %s", SparkTypeName(lt),
		                      SparkTypeName(rt));
	}
	// decimals: NULL when dividing by zero
	return b.Op("%", std::move(left), ZeroToNull(b, std::move(right)));
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
	CheckArgs(input, "narrow_int", 1, 1);
	Builder b(input.context);
	auto value = Arg(input, 0);
	auto type = Normalize(value->GetReturnType());
	if (type.id() == LogicalTypeId::BIGINT || type.id() == LogicalTypeId::HUGEINT) {
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

ExprP BindArray(FunctionBindExpressionInput &input) {
	Builder b(input.context);
	vector<ExprP> args;
	for (idx_t i = 0; i < input.children.size(); i++) {
		args.push_back(Arg(input, i));
	}
	if (args.empty()) {
		return b.Constant(Value::LIST(LogicalType::SQLNULL, {}));
	}
	return b.Call("list_value", std::move(args));
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

LogicalType ExplodeRowType(const LogicalType &input, bool pos) {
	child_list_t<LogicalType> fields;
	if (pos) {
		fields.emplace_back(Identifier("pos"), LogicalType::INTEGER);
	}
	if (input.id() == LogicalTypeId::MAP) {
		fields.emplace_back(Identifier("key"), MapType::KeyType(input));
		fields.emplace_back(Identifier("value"), MapType::ValueType(input));
	} else {
		fields.emplace_back(Identifier("col"), ListType::GetChildType(input));
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
	input.GetBoundFunction().SetReturnType(ExplodeRowType(type, POS));
	return make_uniq<ExplodeBindData>(POS, OUTER);
}

template <bool POS, bool OUTER>
ScalarFunction ExplodeRowsFunction(const string &name) {
	ScalarFunction fun(Identifier(name), {LogicalType::ANY}, LogicalType::ANY, ExplodeRowsKernel, ExplodeBind<POS, OUTER>);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

//! inline(array<struct>) rows: the array itself; inline_outer: a single NULL struct for NULL/empty input
ExprP BindInlineRows(FunctionBindExpressionInput &input) {
	CheckArgs(input, "inline", 1, 1);
	return Arg(input, 0);
}

ExprP BindInlineOuterRows(FunctionBindExpressionInput &input) {
	CheckArgs(input, "inline_outer", 1, 1);
	Builder b(input.context);
	auto arr = Arg(input, 0);
	auto type = arr->GetReturnType();
	if (type.id() != LogicalTypeId::LIST) {
		throw BinderException("inline_outer: input must be an array of structs");
	}
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

//! size / cardinality: -1 for NULL (spark.sql.legacy.sizeOfNull = true)
ExprP BindSize(FunctionBindExpressionInput &input) {
	CheckArgs(input, "size", 1, 1);
	Builder b(input.context);
	auto e = Arg(input, 0);
	auto type = Normalize(e->GetReturnType());
	ExprP length;
	if (type.id() == LogicalTypeId::MAP) {
		length = b.Call("cardinality", e->Copy());
	} else if (type.id() == LogicalTypeId::LIST || type.id() == LogicalTypeId::ARRAY) {
		length = b.Call("len", e->Copy());
	} else if (IsNull(type)) {
		return b.Constant(AnsiEnabled(input.context) ? Value(LogicalType::INTEGER) : Value::INTEGER(-1));
	} else {
		throw BinderException("size: argument must be an array or a map, not %s", SparkTypeName(type));
	}
	if (AnsiEnabled(input.context)) {
		// size(NULL) is NULL in ANSI mode
		return b.Cast(std::move(length), LogicalType::INTEGER);
	}
	vector<ExprP> options;
	options.push_back(b.Cast(std::move(length), LogicalType::INTEGER));
	options.push_back(b.Constant(Value::INTEGER(-1)));
	return b.Coalesce(std::move(options));
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
	vector<ExprP> args;
	args.push_back(b.Cast(std::move(start), LogicalType::BIGINT));
	args.push_back(b.Cast(std::move(stop), LogicalType::BIGINT));
	args.push_back(std::move(step));
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
	bool a_atomic = IsNumeric(a) || IsDatetime(a) || a.id() == LogicalTypeId::BOOLEAN;
	bool b_atomic = IsNumeric(b) || IsDatetime(b) || b.id() == LogicalTypeId::BOOLEAN;
	if ((IsString(a) && b_atomic) || (IsString(b) && a_atomic)) {
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
		check.then_expr = SparkCast(b, std::move(args[i + 1]), type);
		result->CaseChecksMutable().push_back(std::move(check));
	}
	result->ElseMutable() = SparkCast(b, std::move(args.back()), type);
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
		return b.Call(CEIL ? "ceil" : "floor", std::move(e));
	}
	auto rounded = b.Call(CEIL ? "ceil" : "floor", b.Cast(std::move(e), LogicalType::DOUBLE));
	return b.Kernel(DoubleToIntegralFunction(LogicalType::BIGINT), b.Cast(std::move(rounded), LogicalType::DOUBLE));
}

//! try_cast: like cast, but numeric conversions that overflow are NULL (Spark's ANSI cast semantics)
ExprP BindTryCastValue(FunctionBindExpressionInput &input) {
	CheckArgs(input, "try_cast", 2, 2);
	Builder b(input.context);
	auto target = input.children[1]->GetReturnType();
	auto e = Arg(input, 0);
	auto source = Normalize(e->GetReturnType());
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

//! from_number(n, sample): back to an interval when the sample is one
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
	return n;
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
	    {"as_long", BindAsType<LogicalTypeId::BIGINT>},
	    {"as_int", BindAsType<LogicalTypeId::INTEGER>},
	    {"as_string", BindAsType<LogicalTypeId::VARCHAR>},
	    {"as_boolean", BindAsType<LogicalTypeId::BOOLEAN>},
	    {"as_binary", BindAsBinary},
	    {"size", BindSize},
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
	    {"map_from_arrays", BindMapFromArrays},
	    {"inline_rows", BindInlineRows},
	    {"inline_outer_rows", BindInlineOuterRows},
	};
	loader.RegisterFunction(SparkKernelCatalogEntry());
	for (auto &entry : dispatchers) {
		RegisterFallible(loader, Dispatcher(entry.first, entry.second));
		AddSparkOverride(entry.first);
	}
	RegisterFallible(loader, ExplodeRowsFunction<false, false>("explode_rows"));
	RegisterFallible(loader, ExplodeRowsFunction<false, true>("explode_outer_rows"));
	RegisterFallible(loader, ExplodeRowsFunction<true, false>("posexplode_rows"));
	RegisterFallible(loader, ExplodeRowsFunction<true, true>("posexplode_outer_rows"));
}

bool SparkAnsiEnabled(ClientContext &context) {
	return AnsiEnabled(context);
}

unique_ptr<Expression> BindSparkKernel(ClientContext &context, ScalarFunction function,
                                       vector<unique_ptr<Expression>> args) {
	return BindSparkKernelImpl(context, std::move(function), std::move(args));
}

} // namespace duckdb
