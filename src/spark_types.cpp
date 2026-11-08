// Type unification rules (LogicalType::TryGetMaxLogicalType) that follow Spark in the spark / spark_strict dialects
// and DuckDB everywhere else:
//  - two DECIMALs widen to a type that keeps the fraction digits (DECIMAL(38,0) with DECIMAL(2,1) is DECIMAL(38,1)),
//    where DuckDB keeps the integer digits and drops fraction digits when the width exceeds 38
//  - an integer literal next to TINYINT / SMALLINT is an INT (DuckDB lets the literal adapt to the narrower type)
// They matter for VALUES lists, UNION, IN lists and everything else DuckDB binds with a common type.

#include "spark_dialect.hpp"

#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/combine_types_rule.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

namespace {

bool InSparkDialect(optional_ptr<ClientContext> context) {
	if (!context) {
		return false;
	}
	auto &dialect = ClientConfig::GetConfig(*context).current_dialect;
	return dialect.has_value() && (*dialect == "spark" || *dialect == "spark_strict");
}

bool MatchesDecimals(const LogicalType &left, const LogicalType &right) {
	return left.id() == LogicalTypeId::DECIMAL && right.id() == LogicalTypeId::DECIMAL;
}

bool CombineDecimals(LogicalTypeResolver &resolver, const LogicalType &left, const LogicalType &right,
                     LogicalType &result) {
	int left_range = DecimalType::GetWidth(left) - DecimalType::GetScale(left);
	int right_range = DecimalType::GetWidth(right) - DecimalType::GetScale(right);
	int range = MaxValue(left_range, right_range);
	int scale = MaxValue<int>(DecimalType::GetScale(left), DecimalType::GetScale(right));
	int width = range + scale;
	if (width > DecimalType::MaxWidth()) {
		if (InSparkDialect(resolver.context)) {
			// Spark's adjustPrecisionScale: keep the integer digits but at least 6 (or the scale if smaller) fraction
			// digits
			scale = MaxValue<int>(DecimalType::MaxWidth() - range, MinValue(scale, 6));
		} else {
			// DuckDB: keep the integer digits, truncate the scale
			scale = DecimalType::MaxWidth() - range;
		}
		width = DecimalType::MaxWidth();
	}
	result = LogicalType::DECIMAL(uint8_t(width), uint8_t(scale));
	return true;
}

bool MatchesLiteralWithSmallInteger(const LogicalType &left, const LogicalType &right) {
	auto small = [](const LogicalType &type) {
		return type.id() == LogicalTypeId::TINYINT || type.id() == LogicalTypeId::SMALLINT;
	};
	return (left.id() == LogicalTypeId::INTEGER_LITERAL && small(right)) ||
	       (right.id() == LogicalTypeId::INTEGER_LITERAL && small(left));
}

bool CombineLiteralWithSmallInteger(LogicalTypeResolver &resolver, const LogicalType &left, const LogicalType &right,
                                    LogicalType &result) {
	auto &literal = left.id() == LogicalTypeId::INTEGER_LITERAL ? left : right;
	auto &other = left.id() == LogicalTypeId::INTEGER_LITERAL ? right : left;
	if (InSparkDialect(resolver.context)) {
		// Spark's literal 1 is an INT, and INT is wider than TINYINT / SMALLINT
		result = LogicalType::INTEGER;
		return true;
	}
	// DuckDB: the literal adapts to the narrower type when it fits, otherwise the underlying type is combined
	if (resolver.context &&
	    CastFunctionSet::Get(*resolver.context).ImplicitCastCost(resolver.context, literal, other) >= 0) {
		result = other;
		return true;
	}
	return resolver.Operation(IntegerLiteral::GetType(literal), other, result);
}

} // namespace

void RegisterSparkTypeRules(ExtensionLoader &loader) {
	loader.RegisterCombineTypesRule(CombineTypesRule {MatchesDecimals, CombineDecimals});
	loader.RegisterCombineTypesRule(CombineTypesRule {MatchesLiteralWithSmallInteger, CombineLiteralWithSmallInteger});
}

} // namespace duckdb
