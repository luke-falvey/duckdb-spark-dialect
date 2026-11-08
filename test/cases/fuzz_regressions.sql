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

SELECT unbase64(' '), unbase64('U3Bh cmsg\nU1FM'), url_encode('  a b*~é'), regexp_substr('abc', ''), bin(-1.5), bin(13);

SELECT substring_index('www.apache.org', '.', 2), substring_index('www.apache.org', '.', -2), substring_index('aaa', 'aa', 1), substring_index('a.b', '', 1), substring_index('a.b.c', x, 1) FROM VALUES ('.'), ('b'), (NULL) AS t(x);

SELECT array_join(array('hello', NULL, 'world'), x), array_join(array('a', NULL), ',', 'N'), array_join(array(1.5D, CAST('NaN' AS DOUBLE)), ' ') FROM VALUES ('-'), (' '), (NULL) AS t(x);

SELECT equal_null(1, '%_'), equal_null(1, '1'), equal_null(NULL, NULL), levenshtein('kitten', 'sitting', 2);

SELECT hll_sketch_estimate(hll_sketch_agg(col)) FROM VALUES (1), (2), (3), (NULL), (2), (9223372036854775807L) AS t(col);

SELECT concat(array(1, 2), array('b', NULL), array(6)), concat(array(1), array(2.5D));

SELECT arrays_zip(CAST(NULL AS ARRAY<INT>), array(1)), arrays_zip(array(1), array(2, 3)), zip_with(CAST(NULL AS ARRAY<INT>), array(1), (x, y) -> x), map_zip_with(CAST(NULL AS MAP<INT,INT>), map(1, 2), (k, a, b) -> a);

SELECT covar_pop(c1, c2), covar_samp(c1, c2), regr_slope(c1, c2) FROM VALUES (3, 1), (2, 2), (3, 3) AS tab(c1, c2);

SELECT a, session_window.start, session_window.end, count(*) AS cnt FROM VALUES ('A1', '2021-01-01 00:00:00'), ('A1', 'not a date'), ('A1', '2021-01-01 00:10:00'), ('A2', NULL) AS tab(a, b) GROUP BY a, session_window(b, '5 minutes') ORDER BY a, start;

SELECT a, count(*) FROM VALUES ('A1', '2021-01-01 00:00:00') AS tab(a, b) GROUP BY a, session_window(b, '%_');

SELECT a, window.start, count(*) FROM VALUES ('A1', '2021-01-01 00:00:00'), ('A1', NULL), ('A2', 'x') AS tab(a, b) GROUP BY a, window(b, '5 minutes');

-- ===== plans copied by serialization (grouping sets with DISTINCT aggregates) used to lose bind-time kernels =====
-- @setup
CREATE OR REPLACE TEMPORARY VIEW fz_cube AS SELECT CAST(c0 AS TINYINT) AS k, c1 AS g, CAST(c2 AS BOOLEAN) AS b FROM VALUES (1, 'x', 'true'), (2, 'y', 'false'), (3, 'x', NULL) AS v(c0, c1, c2);

SELECT g, b, array_sort(collect_set(k)) AS a0, grouping(g) AS gid FROM fz_cube GROUP BY CUBE(g, b);

SELECT g, count(DISTINCT k), sum(k) FROM fz_cube GROUP BY ROLLUP(g);

-- ===== range() has a BIGINT column named id =====
SELECT id, typeof(id) FROM range(3);

SELECT r.id FROM range(1, 7, 2) AS r JOIN range(10) AS s ON r.id = s.id;

SELECT count(*), sum(id) FROM range(0, 10, 3, 4);

-- ===== found by the generated-query fuzzer (test/fuzz/differential.py) =====
SELECT signum(CAST('NaN' AS DOUBLE)), signum(-0.0D), signum(-2.5), signum(3), 5 % signum(CAST('NaN' AS DOUBLE));

SELECT floor(CAST('NaN' AS DOUBLE)), typeof(floor(1.5D)), floor(1e30D), ceil(-1.5D), typeof(ceil(1.5BD)), floor(-0.5BD), floor(5), typeof(floor(5)), floor(-0.1);

SELECT try_cast(9223372036854775807L AS INT), CAST(9223372036854775807L AS INT), try_cast(300 AS TINYINT), try_cast(1.9D AS INT), try_cast(CAST('NaN' AS DOUBLE) AS INT), try_cast(1e20D AS BIGINT), try_cast(12.7BD AS TINYINT);

SELECT CAST(X'00FF61' AS STRING), crc32(X'FF00'), crc32('abc');

SELECT count_if(b), count_if(CAST(NULL AS BOOLEAN)) FROM VALUES (CAST(NULL AS BOOLEAN)), (NULL) AS t(b);

SELECT to_char(2, '999'), to_char(1.5, '999.99'), to_char(12454, '99G999'), to_char(2, '99G999'), to_char(-12, '9999'), to_char(-12454.8, '99G999D9S'), to_char(12345, '999.99'), to_char(454, '000D00');
