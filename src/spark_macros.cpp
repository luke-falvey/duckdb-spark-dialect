//===----------------------------------------------------------------------===//
// Spark functions defined as DuckDB SQL macros, registered in the `spark` schema.
//
// Macro bodies are DuckDB SQL. Unqualified calls resolve to DuckDB's built-in functions; calls
// to Spark functions are written system.spark.<name>. The as_* helpers apply Spark's implicit
// casts of function arguments (e.g. a string where a date is expected).
//===----------------------------------------------------------------------===//

#include "spark_dialect.hpp"

#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

namespace {

struct SparkMacro {
	const char *name;
	const char *definition;
};

#define S "system.spark."

// clang-format off
const SparkMacro SPARK_MACROS[] = {
    // ---- predicates used by the grammar ----
    {"like", "(s, p) AS like_escape(" S "as_string(s), " S "check_like_pattern(" S "as_string(p)), '\\'), (s, p, e) AS like_escape(" S "as_string(s), " S "as_string(p), CASE WHEN e IS NULL THEN error('[INVALID_ESCAPE] the escape character must be a single-character string literal') ELSE e END)"},
    {"ilike", "(s, p) AS ilike_escape(" S "as_string(s), " S "check_like_pattern(" S "as_string(p)), '\\'), (s, p, e) AS ilike_escape(" S "as_string(s), " S "as_string(p), CASE WHEN e IS NULL THEN error('[INVALID_ESCAPE] the escape character must be a single-character string literal') ELSE e END)"},
    {"rlike", "(s, p) AS regexp_matches(" S "as_string(s), p)"},
    {"regexp", "(s, p) AS regexp_matches(" S "as_string(s), p)"},
    {"regexp_like", "(s, p) AS regexp_matches(" S "as_string(s), p)"},
    {"json_tuple_field", "(j, k) AS json_extract_string(TRY(CAST(j AS JSON)), '$.\"' || k || '\"')"},

    // ---- conditional / null handling ----
    {"nvl", "(a, b) AS coalesce(a, b)"},
    {"ifnull", "(a, b) AS coalesce(a, b)"},
    {"nvl2", "(a, b, c) AS CASE WHEN a IS NOT NULL THEN b ELSE c END"},
    {"isnull", "(a) AS a IS NULL"},
    // 0 (not NULL) when no row is true
    {"count_if", "(x) AS count(CASE WHEN x THEN 1 END)"},
    {"isnan", "(a) AS coalesce(isnan(" S "as_double(a)), false)"},
    {"flatten", "(a) AS CASE WHEN list_bool_or(list_transform(a, lambda x: x IS NULL)) THEN NULL ELSE flatten(a) END"},
    // Spark's max_by / min_by keep a NULL value of the maximum row (DuckDB's arg_max skips NULL values)
    {"max_by", "(x, y) AS arg_max_null(x, y)"},
    {"min_by", "(x, y) AS arg_min_null(x, y)"},
    // Catalyst's Divide yields NULL when the variance product is zero, where DuckDB's corr gives NaN
    {"corr", "(x, y) AS CASE WHEN var_pop(CASE WHEN y IS NOT NULL THEN " S "as_double(x) END) * var_pop(CASE WHEN x IS NOT NULL THEN " S "as_double(y) END) = 0 THEN NULL ELSE corr(" S "as_double(x), " S "as_double(y)) END"},
    {"isnotnull", "(a) AS a IS NOT NULL"},
    {"equal_null", "(a, b) AS " S "cmp_nsafe(a, b)"},
    {"nanvl", "(a, b) AS CASE WHEN isnan(" S "as_double(a)) THEN " S "as_double(b) ELSE " S "as_double(a) END"},
    {"assert_true", "(c) AS CASE WHEN c THEN NULL ELSE error('''' || CAST(c AS VARCHAR) || ''' is not true!') END, "
                    "(c, m) AS CASE WHEN c THEN NULL ELSE error(m) END"},

    // ---- strings ----
    {"ascii", "(s) AS CASE WHEN s IS NULL THEN NULL WHEN length(" S "as_string(s)) = 0 THEN 0 ELSE ascii(" S "as_string(s)) END"},
    {"char", "(n) AS CASE WHEN n IS NULL THEN NULL WHEN n < 0 THEN '' ELSE chr(CAST(((n % 256) + 256) % 256 AS INTEGER)) END"},
    {"chr", "(n) AS " S "char(n)"},
    {"bit_length", "(s) AS " S "octet_length_impl(s) * 8"},
    {"octet_length", "(s) AS " S "octet_length_impl(s)"},
    {"btrim", "(s) AS trim(" S "as_string(s)), (s, c) AS trim(" S "as_string(s), " S "as_string(c))"},
    {"contains", "(a, b) AS CASE WHEN a IS NULL OR b IS NULL THEN NULL ELSE contains(" S "as_string(a), " S "as_string(b)) END"},
    {"startswith", "(a, b) AS CASE WHEN a IS NULL OR b IS NULL THEN NULL ELSE starts_with(" S "as_string(a), " S "as_string(b)) END"},
    {"endswith", "(a, b) AS CASE WHEN a IS NULL OR b IS NULL THEN NULL ELSE ends_with(" S "as_string(a), " S "as_string(b)) END"},
    {"encode", "(s, c) AS " S "encode_charset(" S "as_string(s), " S "as_string(c))"},
    {"decode", "(b, c) AS " S "decode_charset(" S "as_binary(b), " S "as_string(c))"},
    {"elt", "(n, l) AS CASE WHEN n >= 1 THEN list_extract(l, n) END"},
    {"find_in_set", "(s, l) AS CASE WHEN s IS NULL OR l IS NULL THEN NULL WHEN contains(s, ',') THEN 0 ELSE coalesce(CAST(list_position(string_split(l, ','), s) AS INTEGER), 0) END"},
    {"format_number", "(x, d) AS " S "format_number_impl(CASE WHEN typeof(x) IN ('FLOAT', 'DOUBLE') THEN " S "double_to_decimal_string(CAST(x AS DOUBLE)) ELSE CAST(x AS VARCHAR) END, d)"},
    {"instr", "(s, sub) AS CAST(instr(" S "as_string(s), " S "as_string(sub)) AS INTEGER)"},
    {"left", "(s, n) AS " S "substr_impl(" S "as_text_or_binary(s), 1, CASE WHEN n <= 0 THEN 0 ELSE CAST(n AS BIGINT) END)"},
    {"right", "(s, n) AS CASE WHEN n <= 0 THEN '' ELSE right(" S "as_string(s), CAST(n AS BIGINT)) END"},
    {"levenshtein", "(a, b) AS " S "levenshtein_impl(a, b), "
                    "(a, b, t) AS CASE WHEN " S "levenshtein_impl(a, b) > t THEN -1 ELSE " S "levenshtein_impl(a, b) END"},
    {"locate", "(sub, s) AS CAST(instr(s, sub) AS INTEGER), "
               "(sub, s, p) AS CASE WHEN sub IS NULL OR s IS NULL THEN NULL WHEN p IS NULL OR p < 1 THEN 0 "
               "WHEN sub = '' THEN 1 WHEN instr(substr(s, p), sub) = 0 THEN 0 ELSE CAST(instr(substr(s, p), sub) + p - 1 AS INTEGER) END"},
    {"position", "(sub, s) AS " S "locate(sub, s), (sub, s, p) AS " S "locate(sub, s, p)"},
    {"lpad", "(s, n) AS " S "lpad_impl(" S "as_text_or_binary(s), CAST(n AS BIGINT), " S "default_pad(s)), "
             "(s, n, p) AS " S "lpad_impl(" S "as_text_or_binary(s), CAST(n AS BIGINT), p)"},
    {"rpad", "(s, n) AS " S "rpad_impl(" S "as_text_or_binary(s), CAST(n AS BIGINT), " S "default_pad(s)), "
             "(s, n, p) AS " S "rpad_impl(" S "as_text_or_binary(s), CAST(n AS BIGINT), p)"},
    {"mask", "(s) AS " S "mask_impl(s, 'X', 'x', 'n', NULL), (s, u) AS " S "mask_impl(s, u, 'x', 'n', NULL), "
             "(s, u, l) AS " S "mask_impl(s, u, l, 'n', NULL), (s, u, l, d) AS " S "mask_impl(s, u, l, d, NULL), "
             "(s, u, l, d, o) AS " S "mask_impl(s, u, l, d, o)"},
    {"overlay", "(s, r, p) AS " S "substr_impl(" S "as_text_or_binary(s), 1, CAST(p AS BIGINT) - 1) || " S "as_text_or_binary(r) || " S "substr_impl(" S "as_text_or_binary(s), CAST(p AS BIGINT) + length(" S "as_string(r)), 9223372036854775807), "
                "(s, r, p, l) AS " S "substr_impl(" S "as_text_or_binary(s), 1, CAST(p AS BIGINT) - 1) || " S "as_text_or_binary(r) || " S "substr_impl(" S "as_text_or_binary(s), CAST(p AS BIGINT) + CAST(CASE WHEN l < 0 THEN length(" S "as_string(r)) ELSE l END AS BIGINT), 9223372036854775807)"},
    {"regexp_replace", "(s, p, r) AS regexp_replace(" S "as_string(s), p, regexp_replace(r, '\\$(\\d)', '\\\\\\1', 'g'), 'g'), "
                       "(s, p, r, pos) AS substring(" S "as_string(s), 1, CAST(pos AS BIGINT) - 1) || regexp_replace(substring(" S "as_string(s), CAST(pos AS BIGINT)), p, regexp_replace(r, '\\$(\\d)', '\\\\\\1', 'g'), 'g')"},
    {"regexp_count", "(s, p) AS CAST(len(regexp_extract_all(s, p)) AS INTEGER)"},
    {"regexp_substr", "(s, p) AS CASE WHEN regexp_matches(s, p) THEN nullif(regexp_extract(s, p), '') END"},
    {"regexp_instr", "(s, p) AS CASE WHEN s IS NULL OR p IS NULL THEN NULL WHEN regexp_matches(s, p) THEN CAST(length(regexp_replace(s, '(?s)(' || p || ').*', '')) + 1 AS INTEGER) ELSE 0 END, "
                     "(s, p, i) AS CASE WHEN s IS NULL OR p IS NULL THEN NULL WHEN regexp_matches(s, p) THEN CAST(length(regexp_replace(s, '(?s)(' || p || ').*', '')) + 1 AS INTEGER) ELSE 0 END"},
    {"regexp_extract", "(s, p) AS regexp_extract(s, p, 1), (s, p, i) AS regexp_extract(s, p, CAST(i AS INTEGER))"},
    {"regexp_extract_all", "(s, p) AS regexp_extract_all(s, p, 1), (s, p, i) AS regexp_extract_all(s, p, CAST(i AS INTEGER))"},
    {"replace", "(s, f) AS replace(s, f, ''), (s, f, r) AS replace(s, f, r)"},
    {"space", "(n) AS CASE WHEN n IS NULL THEN NULL WHEN n <= 0 THEN '' ELSE repeat(' ', CAST(n AS BIGINT)) END"},
    {"split", "(s, p) AS " S "split_impl(s, p, -1), (s, p, l) AS " S "split_impl(s, p, CAST(l AS INTEGER))"},
    {"substr", "(s, p) AS " S "substr_impl(" S "as_text_or_binary(s), CAST(p AS BIGINT), 9223372036854775807), "
               "(s, p, l) AS " S "substr_impl(" S "as_text_or_binary(s), CAST(p AS BIGINT), CAST(l AS BIGINT))"},
    {"substring_index", "(s, d, c) AS " S "substring_index_impl(" S "as_string(s), " S "as_string(d), CAST(c AS BIGINT))"},
    {"to_binary", "(s) AS unhex(s), (s, f) AS CASE lower(f) WHEN 'hex' THEN unhex(s) WHEN 'base64' THEN from_base64(s) WHEN 'utf-8' THEN encode(s) WHEN 'utf8' THEN encode(s) END"},
    {"try_to_binary", "(s) AS TRY(unhex(s)), (s, f) AS TRY(" S "to_binary(s, f))"},
    // Java's MIME decoder skips characters outside the base64 alphabet (whitespace, line breaks)
    {"unbase64", "(s) AS from_base64(regexp_replace(" S "as_string(s), '[^A-Za-z0-9+/=]', '', 'g'))"},
    {"base64", "(b) AS to_base64(" S "as_binary(b))"},
    {"hex", "(x) AS CASE WHEN typeof(x) IN ('VARCHAR', 'BLOB') THEN hex(" S "as_binary(x)) ELSE hex(CAST(x AS BIGINT)) END"},
    {"unhex", "(s) AS TRY(unhex(s))"},
    {"parse_url", "(u, p) AS " S "parse_url_impl(u, p), (u, p, k) AS " S "parse_url_impl(u, p, k)"},
    {"crc32", "(b) AS " S "crc32_impl(" S "as_binary(b))"},
    {"sha", "(b) AS sha1(" S "as_binary(b))"},
    {"sha1", "(b) AS sha1(" S "as_binary(b))"},
    // SHA-256 (also bit length 0) is DuckDB's; the other lengths use the dialect's implementation. Both hash the
    // bytes (binary input need not be valid UTF-8)
    {"sha2", "(b, n) AS CASE WHEN CAST(n AS INTEGER) IN (0, 256) THEN sha256(" S "as_binary(b)) ELSE " S "sha2_impl(" S "as_binary(b), CAST(n AS INTEGER)) END"},
    {"md5", "(b) AS md5(" S "as_binary(b))"},
    {"string", "(x) AS " S "as_string(x)"},

    // ---- math ----
    {"e", "() AS exp(1)"},
    {"ln", "(x) AS CASE WHEN " S "as_double(x) <= 0 THEN NULL ELSE ln(" S "as_double(x)) END"},
    {"log", "(x) AS " S "ln(x), (b, x) AS CASE WHEN " S "as_double(b) <= 0 OR " S "as_double(x) <= 0 THEN NULL ELSE ln(" S "as_double(x)) / ln(" S "as_double(b)) END"},
    {"log10", "(x) AS CASE WHEN " S "as_double(x) <= 0 THEN NULL ELSE log10(" S "as_double(x)) END"},
    {"log2", "(x) AS CASE WHEN " S "as_double(x) <= 0 THEN NULL ELSE log2(" S "as_double(x)) END"},
    {"log1p", "(x) AS CASE WHEN " S "as_double(x) <= -1 THEN NULL ELSE " S "log1p_impl(" S "as_double(x)) END"},
    {"sqrt", "(x) AS CASE WHEN " S "as_double(x) < 0 THEN CAST('NaN' AS DOUBLE) ELSE sqrt(" S "as_double(x)) END"},
    {"csc", "(x) AS 1 / sin(" S "as_double(x))"},
    {"sec", "(x) AS 1 / cos(" S "as_double(x))"},
    {"cot", "(x) AS 1 / tan(" S "as_double(x))"},
    {"bround", "(x) AS round_even(x, 0), (x, d) AS round_even(x, CAST(d AS INTEGER))"},
    {"round", "(x) AS round(x, 0), (x, d) AS round(x, CAST(d AS INTEGER))"},
    {"ceil", "(x) AS " S "ceil_value(x), (x, d) AS ceil(x * pow(10, d)) / pow(10, d)"},
    {"ceiling", "(x) AS " S "ceil_value(x), (x, d) AS ceil(x * pow(10, d)) / pow(10, d)"},
    {"floor", "(x) AS " S "floor_value(x), (x, d) AS floor(x * pow(10, d)) / pow(10, d)"},
    {"conv", "(n, f, t) AS " S "conv_impl(" S "as_string(n), CAST(f AS INTEGER), CAST(t AS INTEGER))"},
    {"pmod", "(a, b) AS CASE WHEN " S "remainder(a, b) < 0 THEN " S "remainder(" S "add(" S "remainder(a, b), b), b) ELSE " S "remainder(a, b) END"},
    {"width_bucket",
     "(v, lo, hi, n) AS CASE WHEN " S "as_number(v) IS NULL OR " S "as_number(lo) IS NULL OR " S "as_number(hi) IS NULL OR n IS NULL OR n <= 0 OR " S "as_number(lo) = " S "as_number(hi) THEN NULL "
     "WHEN " S "as_number(lo) < " S "as_number(hi) AND " S "as_number(v) < " S "as_number(lo) THEN 0 WHEN " S "as_number(lo) < " S "as_number(hi) AND " S "as_number(v) >= " S "as_number(hi) THEN n + 1 "
     "WHEN " S "as_number(lo) > " S "as_number(hi) AND " S "as_number(v) > " S "as_number(lo) THEN 0 WHEN " S "as_number(lo) > " S "as_number(hi) AND " S "as_number(v) <= " S "as_number(hi) THEN n + 1 "
     "ELSE CAST(floor((" S "as_number(v) - " S "as_number(lo)) / (" S "as_number(hi) - " S "as_number(lo)) * n) + 1 AS BIGINT) END"},
    {"rand", "() AS random(), (s) AS random()"},
    {"random", "() AS random(), (s) AS random()"},
    {"randn", "() AS sqrt(-2 * ln(random())) * cos(2 * pi() * random()), (s) AS sqrt(-2 * ln(random())) * cos(2 * pi() * random())"},
    {"try_divide", "(a, b) AS " S "try_divide_impl(a, b)"},
    {"factorial", "(n) AS CASE WHEN n < 0 OR n > 20 THEN NULL ELSE CAST(factorial(CAST(n AS INTEGER)) AS BIGINT) END"},
    {"bin", "(n) AS CASE WHEN n IS NULL THEN NULL WHEN " S "as_long(n) >= 0 THEN bin(" S "as_long(n)) ELSE bin(" S "as_long(n) + 18446744073709551616::HUGEINT) END"},
    {"bit_count", "(n) AS CAST(bit_count(CAST(n AS BIGINT)) AS INTEGER)"},
    {"bit_get", "(n, p) AS CAST((CAST(n AS BIGINT) >> CAST(p AS INTEGER)) & 1 AS TINYINT)"},
    {"getbit", "(n, p) AS " S "bit_get(n, p)"},

    // ---- date / time ----
    {"current_date", "() AS current_date"},
    {"curdate", "() AS current_date"},
    {"current_timestamp", "() AS CAST(now() AS TIMESTAMP)"},
    {"now", "() AS CAST(now() AS TIMESTAMP)"},
    {"localtimestamp", "() AS CAST(now() AS TIMESTAMP)"},
    {"current_timezone", "() AS current_setting('TimeZone')"},
    {"date", "(x) AS " S "as_date(x)"},
    {"timestamp", "(x) AS " S "as_timestamp(x)"},
    {"to_date", "(s) AS " S "as_date(s), (s, f) AS CAST(" S "parse_datetime(" S "as_string(s), f) AS DATE)"},
    {"to_timestamp", "(s) AS " S "as_timestamp(s), (s, f) AS " S "parse_datetime(" S "as_string(s), f)"},
    {"to_timestamp_ltz", "(s) AS " S "as_timestamp(s), (s, f) AS " S "parse_datetime(" S "as_string(s), f)"},
    {"to_timestamp_ntz", "(s) AS " S "as_timestamp(s), (s, f) AS " S "parse_datetime(" S "as_string(s), f)"},
    {"try_to_timestamp", "(s) AS " S "as_timestamp(s), (s, f) AS " S "parse_datetime(" S "as_string(s), f)"},
    {"date_format", "(t, f) AS " S "format_datetime(" S "as_timestamp(t), f)"},
    {"from_unixtime", "(t) AS " S "format_datetime(CASE WHEN typeof(t) IN ('BIGINT', 'INTEGER', 'SMALLINT', 'TINYINT') THEN " S "epoch_seconds_wrap(CAST(t AS BIGINT)) ELSE CAST(to_timestamp(CAST(t AS DOUBLE)) AS TIMESTAMP) END, 'yyyy-MM-dd HH:mm:ss'), "
                      "(t, f) AS " S "format_datetime(CASE WHEN typeof(t) IN ('BIGINT', 'INTEGER', 'SMALLINT', 'TINYINT') THEN " S "epoch_seconds_wrap(CAST(t AS BIGINT)) ELSE CAST(to_timestamp(CAST(t AS DOUBLE)) AS TIMESTAMP) END, f)"},
    {"unix_timestamp", "() AS CAST(floor(epoch(now())) AS BIGINT), "
                       "(t) AS CAST(floor(epoch(CASE WHEN typeof(t) = 'VARCHAR' THEN " S "parse_datetime(" S "as_string(t), 'yyyy-MM-dd HH:mm:ss') ELSE " S "as_timestamp(t) END)) AS BIGINT), "
                       "(t, f) AS CAST(floor(epoch(CASE WHEN typeof(t) = 'VARCHAR' THEN " S "parse_datetime(" S "as_string(t), f) ELSE " S "as_timestamp(t) END)) AS BIGINT)"},
    {"to_unix_timestamp", "(t) AS " S "unix_timestamp(t), (t, f) AS " S "unix_timestamp(t, f)"},
    {"add_months", "(d, n) AS CAST(" S "as_date(d) + to_months(CAST(n AS INTEGER)) AS DATE)"},
    {"date_add", "(d, n) AS " S "as_date(d) + CAST(n AS INTEGER)"},
    {"dateadd", "(d, n) AS " S "as_date(d) + CAST(n AS INTEGER)"},
    {"date_sub", "(d, n) AS " S "as_date(d) - CAST(n AS INTEGER)"},
    {"datediff", "(e, s) AS CAST(" S "as_date(e) - " S "as_date(s) AS INTEGER)"},
    {"date_diff", "(e, s) AS CAST(" S "as_date(e) - " S "as_date(s) AS INTEGER)"},
    {"date_from_unix_date", "(n) AS DATE '1970-01-01' + CAST(n AS INTEGER)"},
    {"unix_date", "(d) AS CAST(" S "as_date(d) - DATE '1970-01-01' AS INTEGER)"},
    {"unix_seconds", "(t) AS CAST(floor(epoch(" S "as_timestamp(t))) AS BIGINT)"},
    {"unix_millis", "(t) AS CAST(floor(epoch_us(" S "as_timestamp(t)) / 1000) AS BIGINT)"},
    {"unix_micros", "(t) AS epoch_us(" S "as_timestamp(t))"},
    {"timestamp_seconds", "(n) AS CAST(to_timestamp(CAST(n AS DOUBLE)) AS TIMESTAMP)"},
    {"timestamp_millis", "(n) AS make_timestamp(CAST(n AS BIGINT) * 1000)"},
    {"timestamp_micros", "(n) AS make_timestamp(CAST(n AS BIGINT))"},
    {"year", "(d) AS CAST(year(" S "as_date(d)) AS INTEGER)"},
    {"quarter", "(d) AS CAST(quarter(" S "as_date(d)) AS INTEGER)"},
    {"month", "(d) AS CAST(month(" S "as_date(d)) AS INTEGER)"},
    {"day", "(d) AS CAST(day(" S "as_date(d)) AS INTEGER)"},
    {"dayofmonth", "(d) AS CAST(day(" S "as_date(d)) AS INTEGER)"},
    {"dayofyear", "(d) AS CAST(dayofyear(" S "as_date(d)) AS INTEGER)"},
    {"dayofweek", "(d) AS CAST(dayofweek(" S "as_date(d)) + 1 AS INTEGER)"},
    {"weekday", "(d) AS CAST(isodow(" S "as_date(d)) - 1 AS INTEGER)"},
    {"weekofyear", "(d) AS CAST(weekofyear(" S "as_date(d)) AS INTEGER)"},
    {"hour", "(t) AS CAST(hour(" S "as_timestamp(t)) AS INTEGER)"},
    {"minute", "(t) AS CAST(minute(" S "as_timestamp(t)) AS INTEGER)"},
    {"second", "(t) AS CAST(second(" S "as_timestamp(t)) AS INTEGER)"},
    {"last_day", "(d) AS last_day(" S "as_date(d))"},
    {"next_day",
     "(d, w) AS " S "as_date(d) + CAST(((CASE WHEN upper(w) IN ('MO', 'MON', 'MONDAY') THEN 1 WHEN upper(w) IN ('TU', 'TUE', 'TUESDAY') THEN 2 "
     "WHEN upper(w) IN ('WE', 'WED', 'WEDNESDAY') THEN 3 WHEN upper(w) IN ('TH', 'THU', 'THURSDAY') THEN 4 "
     "WHEN upper(w) IN ('FR', 'FRI', 'FRIDAY') THEN 5 WHEN upper(w) IN ('SA', 'SAT', 'SATURDAY') THEN 6 "
     "WHEN upper(w) IN ('SU', 'SUN', 'SUNDAY') THEN 7 END) - isodow(" S "as_date(d)) + 6) % 7 + 1 AS INTEGER)"},
    {"months_between", "(a, b) AS " S "months_between_impl(" S "as_timestamp(a), " S "as_timestamp(b), true), "
                       "(a, b, r) AS " S "months_between_impl(" S "as_timestamp(a), " S "as_timestamp(b), r)"},
    {"trunc",
     "(d, f) AS CASE upper(f) WHEN 'YEAR' THEN CAST(date_trunc('year', " S "as_date(d)) AS DATE) WHEN 'YYYY' THEN CAST(date_trunc('year', " S "as_date(d)) AS DATE) "
     "WHEN 'YY' THEN CAST(date_trunc('year', " S "as_date(d)) AS DATE) WHEN 'QUARTER' THEN CAST(date_trunc('quarter', " S "as_date(d)) AS DATE) "
     "WHEN 'MONTH' THEN CAST(date_trunc('month', " S "as_date(d)) AS DATE) WHEN 'MM' THEN CAST(date_trunc('month', " S "as_date(d)) AS DATE) "
     "WHEN 'MON' THEN CAST(date_trunc('month', " S "as_date(d)) AS DATE) WHEN 'WEEK' THEN CAST(date_trunc('week', " S "as_date(d)) AS DATE) END"},
    {"date_trunc",
     "(f, t) AS CASE upper(f) WHEN 'YEAR' THEN date_trunc('year', " S "as_timestamp(t)) WHEN 'YYYY' THEN date_trunc('year', " S "as_timestamp(t)) "
     "WHEN 'YY' THEN date_trunc('year', " S "as_timestamp(t)) WHEN 'QUARTER' THEN date_trunc('quarter', " S "as_timestamp(t)) "
     "WHEN 'MONTH' THEN date_trunc('month', " S "as_timestamp(t)) WHEN 'MM' THEN date_trunc('month', " S "as_timestamp(t)) "
     "WHEN 'MON' THEN date_trunc('month', " S "as_timestamp(t)) WHEN 'WEEK' THEN date_trunc('week', " S "as_timestamp(t)) "
     "WHEN 'DAY' THEN date_trunc('day', " S "as_timestamp(t)) WHEN 'DD' THEN date_trunc('day', " S "as_timestamp(t)) "
     "WHEN 'HOUR' THEN date_trunc('hour', " S "as_timestamp(t)) WHEN 'MINUTE' THEN date_trunc('minute', " S "as_timestamp(t)) "
     "WHEN 'SECOND' THEN date_trunc('second', " S "as_timestamp(t)) WHEN 'MILLISECOND' THEN date_trunc('millisecond', " S "as_timestamp(t)) "
     "WHEN 'MICROSECOND' THEN date_trunc('microsecond', " S "as_timestamp(t)) END"},
    {"date_part",
     "(f, t) AS CASE upper(f) WHEN 'YEAR' THEN year(t) WHEN 'Y' THEN year(t) WHEN 'YEARS' THEN year(t) WHEN 'YR' THEN year(t) WHEN 'YRS' THEN year(t) "
     "WHEN 'YEAROFWEEK' THEN isoyear(t) WHEN 'QUARTER' THEN quarter(t) WHEN 'QTR' THEN quarter(t) "
     "WHEN 'MONTH' THEN month(t) WHEN 'MON' THEN month(t) WHEN 'MONS' THEN month(t) WHEN 'MONTHS' THEN month(t) "
     "WHEN 'WEEK' THEN weekofyear(t) WHEN 'W' THEN weekofyear(t) WHEN 'WEEKS' THEN weekofyear(t) "
     "WHEN 'DAY' THEN day(t) WHEN 'D' THEN day(t) WHEN 'DAYS' THEN day(t) "
     "WHEN 'DAYOFWEEK' THEN dayofweek(t) + 1 WHEN 'DOW' THEN dayofweek(t) + 1 "
     "WHEN 'DAYOFWEEK_ISO' THEN isodow(t) WHEN 'DOW_ISO' THEN isodow(t) WHEN 'DOY' THEN dayofyear(t) "
     "WHEN 'HOUR' THEN hour(t) WHEN 'H' THEN hour(t) WHEN 'HOURS' THEN hour(t) WHEN 'HR' THEN hour(t) WHEN 'HRS' THEN hour(t) "
     "WHEN 'MINUTE' THEN minute(t) WHEN 'M' THEN minute(t) WHEN 'MIN' THEN minute(t) WHEN 'MINS' THEN minute(t) WHEN 'MINUTES' THEN minute(t) "
     "WHEN 'SECOND' THEN CAST(CAST(microsecond(t) AS DECIMAL(14, 0)) / 1000000 AS DECIMAL(8, 6)) WHEN 'S' THEN CAST(CAST(microsecond(t) AS DECIMAL(14, 0)) / 1000000 AS DECIMAL(8, 6)) "
     "WHEN 'SEC' THEN CAST(CAST(microsecond(t) AS DECIMAL(14, 0)) / 1000000 AS DECIMAL(8, 6)) WHEN 'SECONDS' THEN CAST(CAST(microsecond(t) AS DECIMAL(14, 0)) / 1000000 AS DECIMAL(8, 6)) "
     "WHEN 'SECS' THEN CAST(CAST(microsecond(t) AS DECIMAL(14, 0)) / 1000000 AS DECIMAL(8, 6)) END"},
    {"datepart", "(f, t) AS " S "date_part(f, t)"},
    {"make_timestamp", "(y, mo, d, h, mi, s) AS TRY(make_timestamp(CAST(y AS BIGINT), CAST(mo AS BIGINT), CAST(d AS BIGINT), CAST(h AS BIGINT), CAST(mi AS BIGINT), CAST(s AS DOUBLE))), "
                       "(y, mo, d, h, mi, s, tz) AS CAST(timezone(current_setting('TimeZone'), timezone(tz, TRY(make_timestamp(CAST(y AS BIGINT), CAST(mo AS BIGINT), CAST(d AS BIGINT), CAST(h AS BIGINT), CAST(mi AS BIGINT), CAST(s AS DOUBLE))))) AS TIMESTAMP)"},
    
    {"make_timestamp_ltz", "(y, mo, d, h, mi, s) AS TRY(make_timestamp(CAST(y AS BIGINT), CAST(mo AS BIGINT), CAST(d AS BIGINT), CAST(h AS BIGINT), CAST(mi AS BIGINT), CAST(s AS DOUBLE))), "
                           "(y, mo, d, h, mi, s, tz) AS CAST(timezone(current_setting('TimeZone'), timezone(tz, TRY(make_timestamp(CAST(y AS BIGINT), CAST(mo AS BIGINT), CAST(d AS BIGINT), CAST(h AS BIGINT), CAST(mi AS BIGINT), CAST(s AS DOUBLE))))) AS TIMESTAMP)"},
    {"make_timestamp_ntz", "(y, mo, d, h, mi, s) AS " S "make_timestamp(y, mo, d, h, mi, s)"},
    {"make_date", "(y, m, d) AS TRY(make_date(CAST(y AS BIGINT), CAST(m AS BIGINT), CAST(d AS BIGINT)))"},
    {"make_dt_interval", "() AS INTERVAL 0 SECOND, (d) AS to_days(CAST(d AS INTEGER)), (d, h) AS to_days(CAST(d AS INTEGER)) + to_hours(CAST(h AS BIGINT)), "
                         "(d, h, m) AS to_days(CAST(d AS INTEGER)) + to_hours(CAST(h AS BIGINT)) + to_minutes(CAST(m AS BIGINT)), "
                         "(d, h, m, s) AS to_days(CAST(d AS INTEGER)) + to_hours(CAST(h AS BIGINT)) + to_minutes(CAST(m AS BIGINT)) + to_microseconds(CAST(round(CAST(s AS DECIMAL(18, 6)) * 1000000) AS BIGINT))"},
    {"make_ym_interval", "() AS INTERVAL 0 MONTH, (y) AS to_years(CAST(y AS INTEGER)), (y, m) AS to_years(CAST(y AS INTEGER)) + to_months(CAST(m AS INTEGER))"},
    {"from_utc_timestamp", "(t, tz) AS CAST(timezone(tz, timezone('UTC', " S "as_timestamp(t))) AS TIMESTAMP)"},
    {"to_utc_timestamp", "(t, tz) AS CAST(timezone('UTC', timezone(tz, " S "as_timestamp(t))) AS TIMESTAMP)"},
    {"convert_timezone", "(tgt, t) AS CAST(timezone(tgt, timezone(current_setting('TimeZone'), " S "as_timestamp(t))) AS TIMESTAMP), "
                         "(src, tgt, t) AS CAST(timezone(tgt, timezone(src, " S "as_timestamp(t))) AS TIMESTAMP)"},
    {"window", "(t, d) AS {'start': time_bucket(CAST(d AS INTERVAL), " S "as_timestamp(t), TIMESTAMP '1970-01-01'), "
               "'end': time_bucket(CAST(d AS INTERVAL), " S "as_timestamp(t), TIMESTAMP '1970-01-01') + CAST(d AS INTERVAL)}"},
    {"__spark_window", "(t, d) AS {'start': time_bucket(CAST(d AS INTERVAL), " S "as_timestamp(t), TIMESTAMP '1970-01-01'), "
               "'end': time_bucket(CAST(d AS INTERVAL), " S "as_timestamp(t), TIMESTAMP '1970-01-01') + CAST(d AS INTERVAL)}"},
    {"window_time", "(w) AS w['end'] - INTERVAL 1 MICROSECOND"},

    // ---- collections ----
    {"array_contains", "(a, v) AS CASE WHEN a IS NULL OR v IS NULL THEN NULL WHEN list_contains(a, v) THEN true WHEN " S "list_has_null(a) THEN NULL ELSE false END"},
    {"array_append", "(a, v) AS CASE WHEN a IS NULL THEN NULL ELSE list_append(a, v) END"},
    {"array_prepend", "(a, v) AS CASE WHEN a IS NULL THEN NULL ELSE list_prepend(v, a) END"},
    {"array_compact", "(a) AS list_filter(a, lambda x: x IS NOT NULL)"},
    {"array_distinct", "(a) AS list_filter(a, lambda x, i: list_position(list_transform(a, lambda y: " S "zero_key(y)), " S "zero_key(x)) = i + 1)"},
    // NULL if either array is NULL (DuckDB's list functions treat a NULL list as empty)
    {"array_union", "(a, b) AS CASE WHEN a IS NULL OR b IS NULL THEN NULL ELSE " S "array_distinct(list_concat(a, b)) END"},
    {"array_intersect", "(a, b) AS CASE WHEN a IS NULL OR b IS NULL THEN NULL ELSE list_filter(" S "array_distinct(a), lambda x: list_position(list_transform(b, lambda y: " S "zero_key(y)), " S "zero_key(x)) IS NOT NULL) END"},
    {"array_except", "(a, b) AS CASE WHEN a IS NULL OR b IS NULL THEN NULL ELSE list_filter(" S "array_distinct(a), lambda x: list_position(list_transform(b, lambda y: " S "zero_key(y)), " S "zero_key(x)) IS NULL) END"},
    {"array_join", "(a, d) AS " S "array_join_impl(list_transform(a, lambda x: " S "as_string(x)), d), "
                   "(a, d, r) AS " S "array_join_impl(list_transform(a, lambda x: " S "as_string(x)), d, r)"},
    {"array_max", "(a) AS list_max(a)"},
    {"array_min", "(a) AS list_min(a)"},
    {"array_position", "(a, v) AS CASE WHEN a IS NULL OR v IS NULL THEN NULL ELSE coalesce(CAST(list_position(a, v) AS BIGINT), 0) END"},
    {"array_remove", "(a, v) AS CASE WHEN v IS NULL THEN NULL ELSE list_filter(a, lambda x: x IS NULL OR x != v) END"},
    {"array_repeat", "(v, n) AS CASE WHEN n IS NULL THEN NULL ELSE list_transform(range(greatest(CAST(n AS BIGINT), 0)), lambda i: v) END"},
    {"array_size", "(a) AS CAST(len(a) AS INTEGER)"},
    {"array_sort", "(a) AS list_sort(a, 'ASC', 'NULLS LAST')"},
    {"sort_array", "(a) AS list_sort(a, 'ASC', 'NULLS FIRST'), (a, ascending) AS CASE WHEN ascending THEN list_sort(a, 'ASC', 'NULLS FIRST') ELSE list_sort(a, 'DESC', 'NULLS LAST') END"},
    {"arrays_overlap", "(a, b) AS CASE WHEN a IS NULL OR b IS NULL THEN NULL WHEN list_has_any(a, b) THEN true "
                       "WHEN len(a) > 0 AND len(b) > 0 AND (list_count(a) < len(a) OR list_count(b) < len(b)) THEN NULL ELSE false END"},
    {"array_insert",
     "(a, p, v) AS CASE WHEN a IS NULL OR p IS NULL THEN NULL "
     "WHEN p > 0 AND p <= len(a) + 1 THEN list_concat(list_slice(a, 1, p - 1), [v], list_slice(a, p, len(a))) "
     "WHEN p > 0 THEN list_concat(a, list_transform(range(p - 1 - len(a)), lambda i: NULL), [v]) "
     "WHEN p < 0 AND -p <= len(a) THEN list_concat(list_slice(a, 1, len(a) + p + 1), [v], list_slice(a, len(a) + p + 2, len(a))) "
     "WHEN p < 0 THEN list_concat([v], list_transform(range(-p - len(a) - 1), lambda i: NULL), a) "
     "ELSE error('[INVALID_INDEX_OF_ZERO] The index 0 is invalid.') END"},
    {"slice", "(a, s, l) AS CASE WHEN a IS NULL OR s IS NULL OR l IS NULL THEN NULL WHEN l < 0 THEN error('Unexpected value for length in function slice: length must be greater than or equal to 0.') "
              "WHEN s > 0 THEN list_slice(a, s, s + l - 1) WHEN s < 0 AND len(a) + s + 1 >= 1 THEN list_slice(a, len(a) + s + 1, len(a) + s + l) "
              "WHEN s < 0 THEN [] ELSE error('Unexpected value for start in function slice: SQL array indices start at 1.') END"},
    {"shuffle", "(a) AS list_transform(list_sort(list_transform(a, lambda x: {'r': random(), 'v': x})), lambda e: e.v)"},
    {"get", "(a, i) AS " S "subscript(a, i)"},
    {"exists", "(a, f) AS CASE WHEN a IS NULL THEN NULL WHEN list_contains(list_transform(a, f), true) THEN true "
               "WHEN list_count(list_transform(a, f)) < len(a) THEN NULL ELSE false END"},
    {"forall", "(a, f) AS CASE WHEN a IS NULL THEN NULL WHEN list_contains(list_transform(a, f), false) THEN false "
               "WHEN list_count(list_transform(a, f)) < len(a) THEN NULL ELSE true END"},
    {"aggregate", "(a, z, m) AS list_reduce(a, m, z), (a, z, m, f) AS list_transform([list_reduce(a, m, z)], f)[1]"},
    {"reduce", "(a, z, m) AS list_reduce(a, m, z), (a, z, m, f) AS list_transform([list_reduce(a, m, z)], f)[1]"},
    {"map_contains_key", "(m, k) AS map_contains(m, k)"},
    {"map_keys", "(m) AS map_keys(m)"},
    {"map_values", "(m) AS map_values(m)"},
    {"map_entries", "(m) AS map_entries(m)"},
    {"str_to_map", "(s, p, k) AS map_from_entries(list_transform(" S "split(s, p), lambda kv: {'key': " S "split(kv, k, 2)[1], 'value': " S "split(kv, k, 2)[2]}))"},
    {"str_to_map_2", "(s, p) AS map_from_entries(list_transform(" S "split(s, p), lambda kv: {'key': " S "split(kv, ':', 2)[1], 'value': " S "split(kv, ':', 2)[2]}))"},
    {"array_agg", "(x) AS coalesce(list(x) FILTER (WHERE x IS NOT NULL), [])"},

    // ---- json ----
    {"get_json_object", "(j, p) AS CASE WHEN NOT starts_with(p, '$') THEN NULL ELSE json_extract_string(TRY(CAST(j AS JSON)), p) END"},
    {"json_array_length", "(j) AS CASE WHEN json_type(TRY(CAST(j AS JSON))) = 'ARRAY' THEN CAST(json_array_length(TRY(CAST(j AS JSON))) AS INTEGER) END"},
    {"json_object_keys", "(j) AS CASE WHEN json_type(TRY(CAST(j AS JSON))) = 'OBJECT' THEN json_keys(TRY(CAST(j AS JSON))) END"},

    // ---- aggregates ----
    {"collect_list", "(x) AS coalesce(list(x) FILTER (WHERE x IS NOT NULL), [])"},
    {"collect_set", "(x) AS coalesce(list_sort(list(DISTINCT x) FILTER (WHERE x IS NOT NULL)), [])"},
    {"try_sum", "(x) AS " S "sum_result(sum(x))"},
    {"try_avg", "(x) AS " S "try_avg_result(sum(" S "as_number(x)), count(x), max(x))"},
    {"__pivot_sum", "(x) AS sum(x)"},
    {"__pivot_avg", "(x) AS avg(x)"},
    {"sliding_windows", "(t, d, s) AS list_filter(list_transform(range(CAST(ceil(epoch(CAST(d AS INTERVAL)) / epoch(CAST(s AS INTERVAL))) AS BIGINT)), "
                        "lambda k: {'start': time_bucket(CAST(s AS INTERVAL), " S "as_timestamp(t), TIMESTAMP '1970-01-01') - k * CAST(s AS INTERVAL), "
                        "'end': time_bucket(CAST(s AS INTERVAL), " S "as_timestamp(t), TIMESTAMP '1970-01-01') - k * CAST(s AS INTERVAL) + CAST(d AS INTERVAL)}), "
                        "lambda w: w.start <= " S "as_timestamp(t) AND " S "as_timestamp(t) < w['end'])"},
    {"every", "(x) AS bool_and(x)"},
    {"some", "(x) AS bool_or(x)"},
    {"any", "(x) AS bool_or(x)"},
    {"bool_and", "(x) AS bool_and(x)"},
    {"bool_or", "(x) AS bool_or(x)"},
    {"first", "(x) AS first(x), (x, n) AS CASE WHEN n THEN first(x) FILTER (WHERE x IS NOT NULL) ELSE first(x) END"},
    {"first_value", "(x) AS first(x), (x, n) AS CASE WHEN n THEN first(x) FILTER (WHERE x IS NOT NULL) ELSE first(x) END"},
    {"any_value", "(x) AS first(x), (x, n) AS CASE WHEN n THEN first(x) FILTER (WHERE x IS NOT NULL) ELSE first(x) END"},
    {"last", "(x) AS last(x), (x, n) AS CASE WHEN n THEN last(x) FILTER (WHERE x IS NOT NULL) ELSE last(x) END"},
    {"last_value", "(x) AS last(x), (x, n) AS CASE WHEN n THEN last(x) FILTER (WHERE x IS NOT NULL) ELSE last(x) END"},
    {"std", "(x) AS stddev_samp(x)"},
    {"stddev", "(x) AS stddev_samp(x)"},
    {"median", "(x) AS " S "from_number(quantile_cont(" S "as_number(x), 0.5), max(x))"},
    {"percentile", "(x, p) AS " S "from_number(quantile_cont(" S "as_number(x), p), max(x)), (x, p, f) AS " S "from_number(quantile_cont(" S "as_number(x), p), max(x))"},
    {"percentile_approx", "(x, p) AS quantile_disc(x, p), (x, p, a) AS quantile_disc(x, p)"},
    {"approx_percentile", "(x, p) AS quantile_disc(x, p), (x, p, a) AS quantile_disc(x, p)"},
    {"kurtosis", "(x) AS kurtosis_pop(x)"},
    {"skewness", "(x) AS (avg(power(x, 3)) - 3 * avg(x) * avg(power(x, 2)) + 2 * power(avg(x), 3)) / power(var_pop(x), 1.5)"},
    {"histogram_numeric", "(x, n) AS list_transform(list_sort(map_entries(histogram(x))), lambda e: {'x': e.key, 'y': CAST(e.value AS DOUBLE)})"},

    // ---- misc ----
    {"aes_encrypt", "(i, k) AS " S "aes_encrypt_impl(" S "as_binary(i), " S "as_binary(k), 'GCM', 'DEFAULT', ''::BLOB, ''::BLOB), "
                    "(i, k, m) AS " S "aes_encrypt_impl(" S "as_binary(i), " S "as_binary(k), m, 'DEFAULT', ''::BLOB, ''::BLOB), "
                    "(i, k, m, p) AS " S "aes_encrypt_impl(" S "as_binary(i), " S "as_binary(k), m, p, ''::BLOB, ''::BLOB), "
                    "(i, k, m, p, iv) AS " S "aes_encrypt_impl(" S "as_binary(i), " S "as_binary(k), m, p, " S "as_binary(iv), ''::BLOB), "
                    "(i, k, m, p, iv, a) AS " S "aes_encrypt_impl(" S "as_binary(i), " S "as_binary(k), m, p, " S "as_binary(iv), " S "as_binary(a))"},
    {"aes_decrypt", "(i, k) AS " S "aes_decrypt_impl(" S "as_binary(i), " S "as_binary(k), 'GCM', 'DEFAULT', ''::BLOB), "
                    "(i, k, m) AS " S "aes_decrypt_impl(" S "as_binary(i), " S "as_binary(k), m, 'DEFAULT', ''::BLOB), "
                    "(i, k, m, p) AS " S "aes_decrypt_impl(" S "as_binary(i), " S "as_binary(k), m, p, ''::BLOB), "
                    "(i, k, m, p, a) AS " S "aes_decrypt_impl(" S "as_binary(i), " S "as_binary(k), m, p, " S "as_binary(a))"},
    {"try_aes_decrypt", "(i, k) AS " S "try_aes_decrypt_impl(" S "as_binary(i), " S "as_binary(k), 'GCM', 'DEFAULT', ''::BLOB), "
                        "(i, k, m) AS " S "try_aes_decrypt_impl(" S "as_binary(i), " S "as_binary(k), m, 'DEFAULT', ''::BLOB), "
                        "(i, k, m, p) AS " S "try_aes_decrypt_impl(" S "as_binary(i), " S "as_binary(k), m, p, ''::BLOB), "
                        "(i, k, m, p, a) AS " S "try_aes_decrypt_impl(" S "as_binary(i), " S "as_binary(k), m, p, " S "as_binary(a))"},
    {"count_min_sketch", "(x, e, c, s) AS " S "count_min_sketch_impl(list(x), CAST(e AS DOUBLE), CAST(c AS DOUBLE), CAST(s AS INTEGER))"},
    {"to_char", "(x, f) AS " S "to_char_impl(CAST(x AS VARCHAR), f)"},
    {"to_varchar", "(x, f) AS " S "to_char_impl(CAST(x AS VARCHAR), f)"},
    {"to_number", "(s, f) AS CAST(" S "to_number_impl(s, f) AS DECIMAL(38, 10))"},
    {"try_to_number", "(s, f) AS CAST(" S "try_to_number_impl(s, f) AS DECIMAL(38, 10))"},
    {"xpath_double", "(x, p) AS " S "xpath_number_impl(x, p)"},
    {"xpath_number", "(x, p) AS " S "xpath_number_impl(x, p)"},
    {"xpath_float", "(x, p) AS CAST(" S "xpath_number_impl(x, p) AS FLOAT)"},
    {"xpath_int", "(x, p) AS CAST(CASE WHEN isnan(" S "xpath_number_impl(x, p)) THEN 0 ELSE trunc(" S "xpath_number_impl(x, p)) END AS INTEGER)"},
    {"xpath_long", "(x, p) AS CAST(CASE WHEN isnan(" S "xpath_number_impl(x, p)) THEN 0 ELSE trunc(" S "xpath_number_impl(x, p)) END AS BIGINT)"},
    {"xpath_short", "(x, p) AS CAST(CASE WHEN isnan(" S "xpath_number_impl(x, p)) THEN 0 ELSE trunc(" S "xpath_number_impl(x, p)) END AS SMALLINT)"},
    {"to_json", "(x) AS " S "to_json_impl(x, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX', 'yyyy-MM-dd'), "
                "(x, o) AS " S "to_json_impl(x, coalesce(o['timestampFormat'], 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX'), coalesce(o['dateFormat'], 'yyyy-MM-dd'))"},
    {"schema_of_json", "(j) AS " S "schema_of_json_impl(j), (j, o) AS " S "schema_of_json_impl(j)"},
    {"current_catalog", "() AS 'spark_catalog'"},
    {"current_user", "() AS current_user"},
    {"user", "() AS current_user"},
    {"session_user", "() AS current_user"},
    {"input_file_name", "() AS ''"},
    {"input_file_block_start", "() AS CAST(-1 AS BIGINT)"},
    {"input_file_block_length", "() AS CAST(-1 AS BIGINT)"},
    {"spark_partition_id", "() AS 0"},
    {"bitmap_bucket_number", "(n) AS CASE WHEN n > 0 THEN (CAST(n AS BIGINT) - 1) // 32768 + 1 ELSE CAST(n AS BIGINT) // 32768 END"},
    {"bitmap_bit_position", "(n) AS CASE WHEN n > 0 THEN (CAST(n AS BIGINT) - 1) % 32768 ELSE (-CAST(n AS BIGINT)) % 32768 END"},
    {"bitmap_construct_agg", "(p) AS " S "bitmap_from_positions(list(p))"},
    {"bitmap_or_agg", "(b) AS " S "bitmap_or_list(list(b))"},
    {"bitmap_count", "(b) AS " S "bitmap_count_impl(b)"},
    {"hll_sketch_agg", "(x) AS " S "hll_from_list(list(DISTINCT CAST(hash(x) >> 1 AS BIGINT)) FILTER (WHERE x IS NOT NULL)), "
                       "(x, k) AS " S "hll_from_list(list(DISTINCT CAST(hash(x) >> 1 AS BIGINT)) FILTER (WHERE x IS NOT NULL))"},
    {"hll_union_agg", "(s) AS " S "hll_union_list(list(s)), (s, b) AS " S "hll_union_list(list(s))"},
    {"hll_union", "(a, b) AS " S "hll_union_list([a, b]), (a, b, c) AS " S "hll_union_list([a, b])"},
    {"hll_sketch_estimate", "(s) AS " S "hll_estimate(s)"},
    {"version", "() AS '3.5.9 duckdb'"},
};
// clang-format on

#undef S

} // namespace

void RegisterSparkMacros(ExtensionLoader &loader) {
	for (auto &macro : SPARK_MACROS) {
		DefaultMacro default_macro {SPARK_SCHEMA, macro.name, macro.definition};
		unique_ptr<CreateMacroInfo> info;
		try {
			info = DefaultFunctionGenerator::CreateInternalMacroInfo(default_macro);
		} catch (std::exception &ex) {
			throw InternalException("spark dialect: failed to parse macro %s: %s", macro.name, ex.what());
		}
		loader.RegisterFunction(*info);
		AddSparkOverride(macro.name);
	}
}

} // namespace duckdb
