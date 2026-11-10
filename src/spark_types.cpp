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

//! BIGINT with a DECIMAL: Spark widens through DECIMAL(20,0), DuckDB through DECIMAL(19,0) (the other integer types
//! have the same digit counts in both)
bool MatchesBigintWithDecimal(const LogicalType &left, const LogicalType &right) {
	return (left.id() == LogicalTypeId::BIGINT && right.id() == LogicalTypeId::DECIMAL) ||
	       (right.id() == LogicalTypeId::BIGINT && left.id() == LogicalTypeId::DECIMAL);
}

bool CombineBigintWithDecimal(LogicalTypeResolver &resolver, const LogicalType &left, const LogicalType &right,
                              LogicalType &result) {
	auto &decimal = left.id() == LogicalTypeId::DECIMAL ? left : right;
	auto as_decimal = LogicalType::DECIMAL(InSparkDialect(resolver.context) ? 20 : 19, 0);
	return resolver.Operation(as_decimal, decimal, result);
}

bool InStrictDialect(optional_ptr<ClientContext> context) {
	if (!context) {
		return false;
	}
	auto &dialect = ClientConfig::GetConfig(*context).current_dialect;
	return dialect.has_value() && *dialect == "spark_strict";
}

enum class SparkKind { NONE, NUMBER, BOOLEAN, DATETIME, BINARY, INTERVAL, STRING, LIST, MAP, STRUCT, OTHER };

SparkKind KindOf(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::INTEGER_LITERAL:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return SparkKind::NUMBER;
	case LogicalTypeId::BOOLEAN:
		return SparkKind::BOOLEAN;
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return SparkKind::DATETIME;
	case LogicalTypeId::BLOB:
		return SparkKind::BINARY;
	case LogicalTypeId::INTERVAL:
		return SparkKind::INTERVAL;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::STRING_LITERAL:
		return SparkKind::STRING;
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY:
		return SparkKind::LIST;
	case LogicalTypeId::MAP:
		return SparkKind::MAP;
	case LogicalTypeId::STRUCT:
		return SparkKind::STRUCT;
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::UNKNOWN:
		return SparkKind::NONE;
	default:
		return SparkKind::OTHER;
	}
}

//! Set while the rule below asks DuckDB's built-in rules (outside spark_strict), so it does not match again
thread_local bool combining_without_spark_rule = false;

//! Two types that have no common type in Spark in any context (findWiderTypeForTwo and the stricter variants
//! alike): a boolean with a number, a number or a datetime with binary data, a struct with a scalar, ... Pairs with
//! a string are left out: whether a string promotes depends on the context (UNION yes, VALUES no).
bool MatchesNoSparkCommonType(const LogicalType &left, const LogicalType &right) {
	if (combining_without_spark_rule) {
		return false;
	}
	auto a = KindOf(left), b = KindOf(right);
	if (a == b || a == SparkKind::NONE || b == SparkKind::NONE || a == SparkKind::OTHER || b == SparkKind::OTHER ||
	    a == SparkKind::STRING || b == SparkKind::STRING) {
		return false;
	}
	return true;
}

bool CombineNoSparkCommonType(LogicalTypeResolver &resolver, const LogicalType &left, const LogicalType &right,
                              LogicalType &result) {
	if (InStrictDialect(resolver.context)) {
		return false; // Spark's analysis error ("Cannot combine types")
	}
	combining_without_spark_rule = true;
	bool ok = LogicalType::DefaultTryGetMaxLogicalTypeUnchecked(left, right, result);
	combining_without_spark_rule = false;
	return ok;
}

} // namespace

void RegisterSparkTypeRules(ExtensionLoader &loader) {
	loader.RegisterCombineTypesRule(CombineTypesRule {MatchesNoSparkCommonType, CombineNoSparkCommonType});
	loader.RegisterCombineTypesRule(CombineTypesRule {MatchesBigintWithDecimal, CombineBigintWithDecimal});
	loader.RegisterCombineTypesRule(CombineTypesRule {MatchesDecimals, CombineDecimals});
	loader.RegisterCombineTypesRule(CombineTypesRule {MatchesLiteralWithSmallInteger, CombineLiteralWithSmallInteger});
}

} // namespace duckdb
