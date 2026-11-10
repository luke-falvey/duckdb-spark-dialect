// Spark's sum and avg as real aggregate functions (system.spark.sum / avg).
//
// The result types are Spark's, chosen from the argument type at bind time:
//   sum: integral -> BIGINT (wraps on overflow, like Java's long), DECIMAL(p, s) -> DECIMAL(min(38, p + 10), s)
//        (NULL on overflow), floating point -> DOUBLE
//   avg: integral and floating point -> DOUBLE (summed as doubles), DECIMAL(p, s) -> DECIMAL(min(38, p + 4),
//        min(38, s + 4)) with HALF_UP rounding (NULL on overflow)
// An empty or all-NULL input gives NULL.
//
// They replace the earlier rewrite sum(x) -> sum_typed(sum(x), any_value(x)), which needed the extra aggregate only
// to learn the argument type.

#include "spark_dialect.hpp"

#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

namespace {

// ---- helpers ----------------------------------------------------------------------------------

//! The low 64 bits of an integral input: summing them wraps like a Java long
template <class T>
inline uint64_t LowBits(const T &value) {
	if constexpr (std::is_same<T, hugeint_t>::value) {
		return value.lower;
	} else {
		return uint64_t(int64_t(value));
	}
}

//! the value as Spark's implicit cast to DOUBLE sees it: timestamps are seconds since the epoch
template <class T>
inline double AsDouble(const T &value) {
	if constexpr (std::is_same<T, timestamp_t>::value) {
		return double(value.value) / 1000000.0;
	} else if constexpr (std::is_same<T, hugeint_t>::value) {
		return Cast::Operation<T, double>(value);
	} else {
		return static_cast<double>(value);
	}
}

template <class T>
inline hugeint_t ToHugeint(const T &value) {
	if constexpr (std::is_same<T, hugeint_t>::value) {
		return value;
	} else {
		return Hugeint::Convert(int64_t(value));
	}
}

template <class T>
inline T FromHugeint(const hugeint_t &value) {
	if constexpr (std::is_same<T, hugeint_t>::value) {
		return value;
	} else {
		// the caller checked that the value fits the decimal's width, so it fits the physical type
		return T(int64_t(value.lower));
	}
}

//! |value| < 10^width
inline bool FitsWidth(const hugeint_t &value, uint8_t width) {
	const auto &limit = Hugeint::POWERS_OF_TEN[width];
	return value < limit && value > -limit;
}

//! DuckDB updates a copy of the state ("clustered local") per run of rows with the same state, which is what makes
//! ungrouped and sorted aggregation fast; each operation below adds its Update/Flush functions to this.
struct ClusteredCopy {
	template <class STATE>
	struct ClusteredLocalState {
		typedef STATE Type;
	};

	template <class STATE>
	static void InitializeClusteredLocal(STATE &local, const STATE &state) {
		local = state;
	}
};

// ---- sum of integers: a BIGINT that wraps -------------------------------------------------------

struct IntegerSumState {
	// exported state layout: lets DuckDB cascade grouping sets and push the aggregate below joins
	static constexpr const char *STATE_NAMES[] = {"value", "is_set"};
	using STATE_TYPE = StructStateType<int64_t, bool>;

	int64_t value;
	bool is_set;
};

[[noreturn]] void ThrowAggregateOverflow() {
	throw InvalidInputException("[ARITHMETIC_OVERFLOW] long overflow. If necessary set \"spark.sql.ansi.enabled\" to "
	                            "\"false\" to bypass this error.");
}

//! CHECKED (ANSI mode): an overflow of the BIGINT sum is an error instead of wrapping around
inline int64_t AddInt64(int64_t a, int64_t b, bool checked) {
	if (checked) {
		int64_t result;
		if (!TryAddOperator::Operation<int64_t, int64_t, int64_t>(a, b, result)) {
			ThrowAggregateOverflow();
		}
		return result;
	}
	return int64_t(uint64_t(a) + uint64_t(b));
}

inline int64_t MultiplyInt64(int64_t a, uint64_t count, bool checked) {
	if (checked) {
		int64_t result;
		if (!TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(a, int64_t(count), result)) {
			ThrowAggregateOverflow();
		}
		return result;
	}
	return int64_t(uint64_t(a) * count);
}

template <bool CHECKED>
struct IntegerSumOperation : public ClusteredCopy {
	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.is_set) {
			return;
		}
		target.value = AddInt64(target.value, source.value, CHECKED);
		target.is_set = true;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		state.value = AddInt64(state.value, int64_t(LowBits(input)), CHECKED);
		state.is_set = true;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &, idx_t count) {
		state.value = AddInt64(state.value, MultiplyInt64(int64_t(LowBits(input)), count, CHECKED), CHECKED);
		state.is_set = true;
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set) {
			finalize_data.ReturnNull();
		} else {
			target = state.value;
		}
	}

	template <class INPUT_TYPE, class STATE>
	static void UpdateClusteredLocal(STATE &local, const INPUT_TYPE &input) {
		local.value = AddInt64(local.value, int64_t(LowBits(input)), CHECKED);
	}

	template <class INPUT_TYPE, class STATE>
	static void UpdateClusteredLocal(STATE &local, const INPUT_TYPE &input, idx_t count) {
		local.value = AddInt64(local.value, MultiplyInt64(int64_t(LowBits(input)), count, CHECKED), CHECKED);
	}

	template <class STATE>
	static void FlushClusteredLocal(STATE &state, STATE &local, bool saw_value) {
		if (saw_value) {
			local.is_set = true;
		}
		state = local;
	}


	static bool IgnoreNull() {
		return true;
	}
};

// ---- sum of doubles ------------------------------------------------------------------------------

struct DoubleSumState {
	static constexpr const char *STATE_NAMES[] = {"value", "is_set"};
	using STATE_TYPE = StructStateType<double, bool>;

	double value;
	bool is_set;
};

struct DoubleSumOperation : public ClusteredCopy {
	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.is_set) {
			return;
		}
		target.value += source.value;
		target.is_set = true;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		state.value += AsDouble(input);
		state.is_set = true;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &, idx_t count) {
		state.value += AsDouble(input) * double(count);
		state.is_set = true;
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set) {
			finalize_data.ReturnNull();
		} else {
			target = state.value;
		}
	}

	template <class INPUT_TYPE, class STATE>
	static void UpdateClusteredLocal(STATE &local, const INPUT_TYPE &input) {
		local.value += AsDouble(input);
	}

	template <class INPUT_TYPE, class STATE>
	static void UpdateClusteredLocal(STATE &local, const INPUT_TYPE &input, idx_t count) {
		local.value += AsDouble(input) * double(count);
	}

	template <class STATE>
	static void FlushClusteredLocal(STATE &state, STATE &local, bool saw_value) {
		if (saw_value) {
			local.is_set = true;
		}
		state = local;
	}


	static bool IgnoreNull() {
		return true;
	}
};

// ---- avg of integers and doubles: a double sum and a count ---------------------------------------

struct DoubleAvgState {
	static constexpr const char *STATE_NAMES[] = {"value", "count"};
	using STATE_TYPE = StructStateType<double, int64_t>;

	double value;
	int64_t count;
};

struct DoubleAvgOperation : public ClusteredCopy {
	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		target.value += source.value;
		target.count += source.count;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		state.value += AsDouble(input);
		state.count++;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &, idx_t count) {
		state.value += AsDouble(input) * double(count);
		state.count += int64_t(count);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.count == 0) {
			finalize_data.ReturnNull();
		} else {
			target = state.value / double(state.count);
		}
	}

	template <class INPUT_TYPE, class STATE>
	static void UpdateClusteredLocal(STATE &local, const INPUT_TYPE &input) {
		local.value += AsDouble(input);
		local.count++;
	}

	template <class INPUT_TYPE, class STATE>
	static void UpdateClusteredLocal(STATE &local, const INPUT_TYPE &input, idx_t count) {
		local.value += AsDouble(input) * double(count);
		local.count += int64_t(count);
	}

	template <class STATE>
	static void FlushClusteredLocal(STATE &state, STATE &local, bool) {
		state = local;
	}


	static bool IgnoreNull() {
		return true;
	}
};

// ---- sum of decimals: an exact 128-bit sum, NULL when it does not fit DECIMAL(min(38, p + 10), s) --

//! The sum is `value + small`: narrow inputs are added to the 64-bit `small` with an overflow check, and only a
//! carry out of it (rare) is moved to the 128-bit `value`, which keeps the hot loop at one add and one branch.
struct DecimalSumState {
	static constexpr const char *STATE_NAMES[] = {"value", "small", "is_set", "overflow"};
	using STATE_TYPE = StructStateType<hugeint_t, int64_t, bool, bool>;

	hugeint_t value;
	int64_t small;
	bool is_set;
	bool overflow;
};

//! Adds `count` times `input` to the state; a 128-bit overflow marks the state as overflowed
inline void AddToDecimalState(hugeint_t &sum, bool &overflow, const hugeint_t &input, idx_t count) {
	if (overflow) {
		return;
	}
	hugeint_t addend = input;
	if (count != 1 && !Hugeint::TryMultiply(input, Hugeint::Convert(count), addend)) {
		overflow = true;
		return;
	}
	if (!Hugeint::TryAddInPlace(sum, addend)) {
		overflow = true;
	}
}

//! Moves a 64-bit partial sum into the 128-bit one (sign-extended, branch-free carry); 128 bits cannot overflow from
//! 64-bit additions (that would take 2^64 spills).
inline void SpillSmall(hugeint_t &sum, int64_t small) {
	auto old = sum.lower;
	sum.lower += uint64_t(small);
	sum.upper += (small >> 63) + int64_t(sum.lower < old);
}

template <class STATE>
inline hugeint_t TotalOf(const STATE &state) {
	hugeint_t total = state.value;
	SpillSmall(total, state.small);
	return total;
}

//! Adds an input to the sum of a state; a hugeint input (or a repeated one) is checked against 128-bit overflow.
template <class STATE, class T>
inline void AddDecimalInput(STATE &state, const T &input, idx_t count) {
	if constexpr (!std::is_same<T, hugeint_t>::value) {
		if (count == 1) {
			auto value = int64_t(input);
			int64_t next;
			if (DUCKDB_LIKELY(!__builtin_add_overflow(state.small, value, &next))) {
				state.small = next;
			} else {
				SpillSmall(state.value, state.small);
				state.small = value;
			}
			return;
		}
	}
	AddToDecimalState(state.value, state.overflow, ToHugeint(input), count);
}

template <class IN, class OUT, bool ANSI = false>
struct DecimalSumOperation : public ClusteredCopy {
	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.is_set) {
			return;
		}
		target.is_set = true;
		target.overflow = target.overflow || source.overflow;
		AddToDecimalState(target.value, target.overflow, TotalOf(source), 1);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		state.is_set = true;
		AddDecimalInput(state, input, 1);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &, idx_t count) {
		state.is_set = true;
		AddDecimalInput(state, input, count);
	}

	template <class INPUT_TYPE, class STATE>
	static void UpdateClusteredLocal(STATE &local, const INPUT_TYPE &input) {
		AddDecimalInput(local, input, 1);
	}

	template <class INPUT_TYPE, class STATE>
	static void UpdateClusteredLocal(STATE &local, const INPUT_TYPE &input, idx_t count) {
		AddDecimalInput(local, input, count);
	}

	template <class STATE>
	static void FlushClusteredLocal(STATE &state, STATE &local, bool saw_value) {
		if (saw_value) {
			local.is_set = true;
		}
		state = local;
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		auto width = DecimalType::GetWidth(finalize_data.input.function.GetReturnType());
		if (!state.is_set) {
			finalize_data.ReturnNull();
		} else if (state.overflow || !FitsWidth(TotalOf(state), width)) {
			if (ANSI) {
				throw InvalidInputException("[NUMERIC_VALUE_OUT_OF_RANGE] The sum does not fit the decimal type. If "
				                            "necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this "
				                            "error.");
			}
			finalize_data.ReturnNull();
		} else {
			target = FromHugeint<T>(TotalOf(state));
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

// ---- avg of decimals: exact, HALF_UP, NULL on overflow -------------------------------------------

struct DecimalAvgState {
	static constexpr const char *STATE_NAMES[] = {"value", "small", "count", "overflow"};
	using STATE_TYPE = StructStateType<hugeint_t, int64_t, int64_t, bool>;

	hugeint_t value;
	int64_t small;
	int64_t count;
	bool overflow;
};

template <class IN, class OUT>
struct DecimalAvgOperation : public ClusteredCopy {
	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		target.count += source.count;
		target.overflow = target.overflow || source.overflow;
		AddToDecimalState(target.value, target.overflow, TotalOf(source), 1);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		state.count++;
		AddDecimalInput(state, input, 1);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &, idx_t count) {
		state.count += int64_t(count);
		AddDecimalInput(state, input, count);
	}

	template <class INPUT_TYPE, class STATE>
	static void UpdateClusteredLocal(STATE &local, const INPUT_TYPE &input) {
		local.count++;
		AddDecimalInput(local, input, 1);
	}

	template <class INPUT_TYPE, class STATE>
	static void UpdateClusteredLocal(STATE &local, const INPUT_TYPE &input, idx_t count) {
		local.count += int64_t(count);
		AddDecimalInput(local, input, count);
	}

	template <class STATE>
	static void FlushClusteredLocal(STATE &state, STATE &local, bool) {
		state = local;
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		auto &function = finalize_data.input.function;
		auto &input_type = function.GetArguments()[0];
		auto &result_type = function.GetReturnType();
		auto sum_width = uint8_t(MinValue<int>(38, DecimalType::GetWidth(input_type) + 10));
		// Spark: the sum is a DECIMAL(min(38, p + 10), s); when it overflows, the average is NULL
		auto sum = TotalOf(state);
		if (state.count == 0 || state.overflow || !FitsWidth(sum, sum_width)) {
			finalize_data.ReturnNull();
			return;
		}
		// Spark evaluates sum / CAST(count AS DECIMAL(20, 0)) first. That quotient's type is the decimal division
		// type with adjusted precision: scale max(6, s + 21), reduced when the precision would exceed 38. A quotient
		// that does not fit it is NULL. The quotient is then cast to the result type (HALF_UP).
		int input_scale = DecimalType::GetScale(input_type);
		int quotient_scale = MaxValue(6, input_scale + 21);
		int quotient_precision = sum_width - input_scale + quotient_scale;
		if (quotient_precision > 38) {
			quotient_scale = MaxValue(38 - (sum_width - input_scale), 6);
			quotient_precision = 38;
		}
		hugeint_t quotient;
		if (!DivideRounded(sum, state.count, quotient_scale - input_scale, quotient) ||
		    !FitsWidth(quotient, uint8_t(quotient_precision))) {
			finalize_data.ReturnNull();
			return;
		}
		hugeint_t result;
		if (!Rescale(quotient, quotient_scale, DecimalType::GetScale(result_type), result) ||
		    !FitsWidth(result, DecimalType::GetWidth(result_type))) {
			finalize_data.ReturnNull();
			return;
		}
		target = FromHugeint<T>(result);
	}

	//! change the scale of an unscaled decimal value; dropping digits rounds half up (away from zero)
	static bool Rescale(const hugeint_t &value, int from, int to, hugeint_t &result) {
		if (to >= from) {
			result = value;
			for (int i = from; i < to; i++) {
				if (!Hugeint::TryMultiply(result, hugeint_t(10), result)) {
					return false;
				}
			}
			return true;
		}
		bool negative = value < hugeint_t(0);
		hugeint_t magnitude = negative ? -value : value;
		hugeint_t divisor = Hugeint::POWERS_OF_TEN[from - to];
		hugeint_t quotient = magnitude / divisor;
		hugeint_t remainder = magnitude % divisor;
		if (remainder >= divisor - remainder) {
			quotient += hugeint_t(1);
		}
		result = negative ? -quotient : quotient;
		return true;
	}

	//! sum / count at `shift` more decimal digits, rounded half up (away from zero); false on 128-bit overflow
	static bool DivideRounded(const hugeint_t &sum, int64_t count, int shift, hugeint_t &result) {
		bool negative = sum < hugeint_t(0);
		hugeint_t magnitude = negative ? -sum : sum;
		hugeint_t divisor = Hugeint::Convert(count);
		hugeint_t quotient = magnitude / divisor;
		hugeint_t remainder = magnitude % divisor;
		for (int i = 0; i < shift; i++) {
			// remainder < count <= 2^63, so remainder * 10 fits
			remainder = remainder * hugeint_t(10);
			hugeint_t digit = remainder / divisor;
			remainder = remainder % divisor;
			if (!Hugeint::TryMultiply(quotient, hugeint_t(10), quotient) || !Hugeint::TryAddInPlace(quotient, digit)) {
				return false;
			}
		}
		if (remainder * hugeint_t(2) >= divisor && !Hugeint::TryAddInPlace(quotient, hugeint_t(1))) {
			return false;
		}
		result = negative ? -quotient : quotient;
		return true;
	}

	static bool IgnoreNull() {
		return true;
	}
};

// ---- construction ----------------------------------------------------------------------------------

template <class F>
AggregateFunction WithPhysicalType(PhysicalType type, F &&make) {
	switch (type) {
	case PhysicalType::INT16:
		return make(int16_t());
	case PhysicalType::INT32:
		return make(int32_t());
	case PhysicalType::INT64:
		return make(int64_t());
	case PhysicalType::INT128:
		return make(hugeint_t());
	default:
		throw InternalException("spark sum/avg: unsupported decimal representation");
	}
}

template <class STATE, class IN, class OUT, class OP>
AggregateFunction MakeDecimal(const LogicalType &input, const LogicalType &result) {
	auto function = AggregateFunction::UnaryAggregate<STATE, IN, OUT, OP>(input, result);
	function.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
	return function;
}

//! The decimal overloads are chosen at bind time: the implementation depends on the precision
template <bool IS_SUM>
unique_ptr<FunctionData> BindDecimal(BindAggregateFunctionInput &input) {
	auto &function = input.GetBoundFunction();
	auto decimal_type = input.GetArguments()[0]->GetReturnType();
	auto width = DecimalType::GetWidth(decimal_type);
	auto scale = DecimalType::GetScale(decimal_type);
	LogicalType result_type =
	    IS_SUM ? LogicalType::DECIMAL(uint8_t(MinValue<int>(38, width + 10)), scale)
	           : LogicalType::DECIMAL(uint8_t(MinValue<int>(38, width + 4)), uint8_t(MinValue<int>(38, scale + 4)));
	bool ansi = SparkAnsiEnabled(input.GetClientContext());
	auto implementation = WithPhysicalType(decimal_type.InternalType(), [&](auto in) {
		return WithPhysicalType(result_type.InternalType(), [&](auto out) {
			using IN = decltype(in);
			using OUT = decltype(out);
			if constexpr (IS_SUM) {
				if (ansi) {
					auto function = MakeDecimal<DecimalSumState, IN, OUT, DecimalSumOperation<IN, OUT, true>>(
					    decimal_type, result_type);
					function.SetFallible();
					return function;
				}
				return MakeDecimal<DecimalSumState, IN, OUT, DecimalSumOperation<IN, OUT>>(decimal_type, result_type);
			} else {
				return MakeDecimal<DecimalAvgState, IN, OUT, DecimalAvgOperation<IN, OUT>>(decimal_type, result_type);
			}
		});
	});
	function.ReplaceImplementation(implementation);
	function.SetName(Identifier(IS_SUM ? "spark_sum" : "spark_avg"));
	function.GetArguments()[0] = decimal_type;
	function.SetReturnType(result_type);
	function.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
	return nullptr;
}

AggregateFunction MakeDoubleSum();
AggregateFunction MakeDoubleAvg();

//! sum / avg of a string or an untyped NULL: Spark casts the argument to DOUBLE
template <bool IS_SUM>
unique_ptr<FunctionData> BindCastToDouble(BindAggregateFunctionInput &input) {
	auto &function = input.GetBoundFunction();
	// Spark's implicit cast: a string that is not a number is NULL (non-ANSI); under ANSI it is an error
	bool try_cast = !SparkAnsiEnabled(input.GetClientContext());
	input.GetArguments()[0] = BoundCastExpression::AddCastToType(
	    input.GetClientContext(), std::move(input.GetArguments()[0]), LogicalType::DOUBLE, try_cast);
	function.ReplaceImplementation(IS_SUM ? MakeDoubleSum() : MakeDoubleAvg());
	function.SetName(Identifier(IS_SUM ? "spark_sum" : "spark_avg"));
	function.GetArguments()[0] = LogicalType::DOUBLE;
	function.SetReturnType(LogicalType::DOUBLE);
	return nullptr;
}

AggregateFunction CastingOverload(const LogicalType &type, bind_aggregate_function_t bind) {
	AggregateFunction function({type}, LogicalType::DOUBLE, nullptr, nullptr, nullptr, nullptr, nullptr,
	                           FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, bind);
	function.GetSignature().GetParameter(0).SetName("arg");
	return function;
}

//! any other argument type: Spark's error instead of "no function matches spark_sum(...)"
template <bool IS_SUM>
unique_ptr<FunctionData> BindUnsupported(BindAggregateFunctionInput &input) {
	throw BinderException("[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"%s(%s)\" due to data type mismatch: "
	                      "Parameter 1 requires the (\"NUMERIC\" or \"INTERVAL DAY TO SECOND\" or \"INTERVAL YEAR TO "
	                      "MONTH\") type, however the argument has the type \"%s\".",
	                      IS_SUM ? "sum" : "avg", input.GetArguments()[0]->ToString(),
	                      input.GetArguments()[0]->GetReturnType().ToString());
}

AggregateFunction DecimalWildcard(bind_aggregate_function_t bind) {
	AggregateFunction function({}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr, nullptr, nullptr,
	                           FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, bind);
	function.GetSignature().AddParameter("arg", LogicalTypeId::DECIMAL);
	return function;
}

template <class INPUT, bool CHECKED>
AggregateFunction IntegerSumImpl(const LogicalType &type);

//! ANSI mode: the BIGINT sum raises on overflow, chosen when the call is bound
template <class INPUT>
unique_ptr<FunctionData> BindIntegerSum(BindAggregateFunctionInput &input) {
	if (SparkAnsiEnabled(input.GetClientContext())) {
		auto &function = input.GetBoundFunction();
		auto type = input.GetArguments()[0]->GetReturnType();
		function.ReplaceImplementation(IntegerSumImpl<INPUT, true>(type));
		function.SetName(Identifier("spark_sum"));
		function.GetArguments()[0] = type;
		function.SetReturnType(LogicalType::BIGINT);
		function.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
	}
	return nullptr;
}

template <class INPUT, bool CHECKED>
AggregateFunction IntegerSumImpl(const LogicalType &type) {
	auto function =
	    AggregateFunction::UnaryAggregate<IntegerSumState, INPUT, int64_t, IntegerSumOperation<CHECKED>>(
	        type, LogicalType::BIGINT);
	function.GetSignature().GetParameter(0).SetName("arg");
	function.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
	if (CHECKED) {
		function.SetFallible();
	} else {
		function.SetBindCallback(BindIntegerSum<INPUT>);
	}
	return function;
}

template <class INPUT>
AggregateFunction IntegerSum(const LogicalType &type) {
	return IntegerSumImpl<INPUT, false>(type);
}

template <class INPUT>
AggregateFunction DoubleAvg(const LogicalType &type) {
	auto function =
	    AggregateFunction::UnaryAggregate<DoubleAvgState, INPUT, double, DoubleAvgOperation>(type, LogicalType::DOUBLE);
	function.GetSignature().GetParameter(0).SetName("arg");
	return function;
}

AggregateFunction MakeDoubleSum() {
	auto function = AggregateFunction::UnaryAggregate<DoubleSumState, double, double, DoubleSumOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE);
	function.GetSignature().GetParameter(0).SetName("arg");
	return function;
}

AggregateFunction MakeDoubleAvg() {
	return DoubleAvg<double>(LogicalType::DOUBLE);
}

} // namespace

//! DuckDB re-binds an exported aggregate state (ROLLUP / CUBE cascades, aggregate pushdown below joins) by looking the
//! aggregate up by name in system.main, so the Spark aggregates are also registered there. The names are internal
//! (`spark_sum` / `spark_avg`); the default dialect has no use for them.
static void RegisterInMain(ExtensionLoader &loader, const AggregateFunctionSet &set) {
	loader.UseDefaultSchema(Identifier(DEFAULT_SCHEMA));
	loader.RegisterFunction(set);
	loader.UseDefaultSchema(Identifier(SPARK_SCHEMA));
}

void RegisterSparkAggregates(ExtensionLoader &loader) {
	AggregateFunctionSet sum {Identifier("spark_sum")};
	sum.AddFunction(DecimalWildcard(BindDecimal<true>));
	sum.AddFunction(IntegerSum<int16_t>(LogicalType::SMALLINT));
	sum.AddFunction(IntegerSum<int32_t>(LogicalType::INTEGER));
	sum.AddFunction(IntegerSum<int64_t>(LogicalType::BIGINT));
	sum.AddFunction(IntegerSum<hugeint_t>(LogicalType::HUGEINT));
	sum.AddFunction(MakeDoubleSum());
	{
		auto timestamp_sum = AggregateFunction::UnaryAggregate<DoubleSumState, timestamp_t, double, DoubleSumOperation>(
		    LogicalType::TIMESTAMP, LogicalType::DOUBLE);
		timestamp_sum.GetSignature().GetParameter(0).SetName("arg");
		sum.AddFunction(timestamp_sum);
	}
	sum.AddFunction(CastingOverload(LogicalType::VARCHAR, BindCastToDouble<true>));
	sum.AddFunction(CastingOverload(LogicalType::SQLNULL, BindCastToDouble<true>));
	sum.AddFunction(IntegerSum<int8_t>(LogicalType::TINYINT));
	sum.AddFunction(CastingOverload(LogicalType::FLOAT, BindCastToDouble<true>));
	for (auto &type : {LogicalType::DATE, LogicalType::TIMESTAMP_TZ, LogicalType::TIME, LogicalType::BLOB, LogicalType::BOOLEAN}) {
		sum.AddFunction(CastingOverload(type, BindUnsupported<true>));
	}
	loader.RegisterFunction(sum);
	RegisterInMain(loader, sum);

	AggregateFunctionSet avg {Identifier("spark_avg")};
	avg.AddFunction(DecimalWildcard(BindDecimal<false>));
	avg.AddFunction(DoubleAvg<int16_t>(LogicalType::SMALLINT));
	avg.AddFunction(DoubleAvg<int32_t>(LogicalType::INTEGER));
	avg.AddFunction(DoubleAvg<int64_t>(LogicalType::BIGINT));
	avg.AddFunction(DoubleAvg<hugeint_t>(LogicalType::HUGEINT));
	avg.AddFunction(MakeDoubleAvg());
	avg.AddFunction(DoubleAvg<timestamp_t>(LogicalType::TIMESTAMP));
	avg.AddFunction(CastingOverload(LogicalType::VARCHAR, BindCastToDouble<false>));
	avg.AddFunction(CastingOverload(LogicalType::SQLNULL, BindCastToDouble<false>));
	avg.AddFunction(DoubleAvg<int8_t>(LogicalType::TINYINT));
	avg.AddFunction(CastingOverload(LogicalType::FLOAT, BindCastToDouble<false>));
	for (auto &type : {LogicalType::DATE, LogicalType::TIMESTAMP_TZ, LogicalType::TIME, LogicalType::BLOB, LogicalType::BOOLEAN}) {
		avg.AddFunction(CastingOverload(type, BindUnsupported<false>));
	}
	loader.RegisterFunction(avg);
	RegisterInMain(loader, avg);
}

} // namespace duckdb
