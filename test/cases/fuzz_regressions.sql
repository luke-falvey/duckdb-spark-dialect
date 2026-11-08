-- Reduced reproducers of wrong results found by the fuzzers (test/fuzz/), checked against Spark 3.5.

-- ===== the minimum values of suffixed literals (the minus is part of the literal in Spark) =====
SELECT -9223372036854775808L, typeof(-9223372036854775808L), -128Y, typeof(-128Y), -32768S, typeof(-32768S);

SELECT 1 + -9223372036854775808L, abs(-9223372036854775808L), - 9223372036854775808L, array(-9223372036854775808L);

SELECT x = 2, x + 1 FROM VALUES (-9223372036854775808L), (2), (NULL) AS t(x);

-- ===== comparisons between strings and other types use Spark's implicit casts =====
SELECT 1 = 'abc', 1 <=> 'abc', 1 == ' ', 1 = '1', 1.1 < 'abc', 1.0 <= '1', 2.0 >= '2.1';

SELECT '2024-01-02' > DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00' = '2024-01-01', true = 'true', 1 = '1.5';

SELECT x, x = 1, x < 1.5, x <=> 1 FROM VALUES ('1'), ('abc'), (' '), (NULL) AS t(x);

-- ===== string -> timestamp accepts Spark's short forms =====
SELECT hour('2024'), minute('2024'), date_format('2024', 'y'), date_trunc('MM', '2024'), months_between('2024', '1996-10-30');

SELECT CAST('2024-1-5 3:04:05' AS TIMESTAMP), CAST(' 2024-02-29T12:00 ' AS TIMESTAMP), CAST('2024-01' AS TIMESTAMP), CAST('2024-02-30' AS TIMESTAMP), CAST('x' AS TIMESTAMP);

-- ===== concat, % and [] without repeated operands =====
SELECT concat(NULL, NULL), concat('a', NULL, 1), concat(), concat(array(1), CAST(NULL AS ARRAY<INT>)), concat(1, 2.5, true), concat(X'01', X'02');

SELECT 7.25BD % 2, 2 % 1.8, 1.5 % 0.0, mod(2, 1.8), array(1, 2)[-1], array(1, 2)[0], array(1, 2)[5];

SELECT array_sort(array(3, 1, 2), (l, r) -> CASE WHEN l < r THEN -1 WHEN l > r THEN 1 ELSE 0 END);

-- ===== functions =====
SELECT initcap('é sPARK ünï'), levenshtein('kitten', '日本語'), levenshtein('kitten', 'sitting'), conv(' ', 2, 10), conv('11', 2, 10);

SELECT asinh(-2147483648), asinh(1.0), acosh(2.0), isnan(NULL), isnan(CAST('x' AS DOUBLE)), isnan(CAST('NaN' AS DOUBLE));

SELECT locate('', 'foobarbar', 5), locate('bar', 'foobarbar', 5), encode('abc', NULL), get_json_object('{"a":"b"}', ''), get_json_object('{"a":"b"}', '$.a');

SELECT array_union(CAST(NULL AS ARRAY<INT>), array(1)), array_union(array(1), CAST(NULL AS ARRAY<INT>)), array_except(array(1), CAST(NULL AS ARRAY<INT>)), array_intersect(array(1), CAST(NULL AS ARRAY<INT>)), array_join(array('a'), NULL);

SELECT flatten(array(array(1), NULL)), flatten(array(array(1), array(2))), flatten(CAST(array() AS ARRAY<ARRAY<INT>>));

SELECT max_by(x, y), min_by(x, y) FROM VALUES ('a', 10), (NULL, 50), ('c', 20), (NULL, 5) AS t(x, y);

SELECT corr(c1, c2) FROM VALUES (3, 2), (3, 3), (NULL, 4) AS t(c1, c2);

SELECT corr(c1, c2) FROM VALUES (1, 2), (2, 3), (4, 9) AS t(c1, c2);
