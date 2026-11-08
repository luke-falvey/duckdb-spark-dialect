-- Cases for the README limitations that were removed or reduced (docs/plans/readme-limitations.md).
-- One section per limitation; golden results come from Spark 3.5 (test/make_golden.py).

-- ===== L2: sum / avg are real aggregates (no any_value) =====

-- @setup
CREATE TEMPORARY VIEW l2_ints AS SELECT * FROM VALUES
  ('a', 1, 10L, 1.50), ('a', 2, 20L, 2.25), ('a', NULL, NULL, NULL), ('b', 2147483647, 9223372036854775807L, 99999999.99),
  ('b', 1, 1L, 0.01), ('c', NULL, NULL, NULL) AS l2_ints(g, i, l, d);

SELECT g, sum(i), sum(l), sum(d), avg(i), avg(l), avg(d) FROM l2_ints GROUP BY g ORDER BY g;

SELECT sum(i), sum(l), sum(d), avg(i), avg(l), avg(d) FROM l2_ints;

SELECT typeof(sum(i)), typeof(sum(l)), typeof(sum(d)), typeof(avg(i)), typeof(avg(l)), typeof(avg(d)) FROM l2_ints;

SELECT sum(CAST(i AS TINYINT)), sum(CAST(i AS SMALLINT)), typeof(sum(CAST(i AS TINYINT))), avg(CAST(i AS SMALLINT)) FROM l2_ints WHERE i < 10;

SELECT sum(i), avg(i), sum(d), avg(d) FROM l2_ints WHERE false;

SELECT sum(i), avg(i), sum(d), avg(d) FROM l2_ints WHERE i IS NULL;

SELECT sum(CAST(f AS FLOAT)), avg(CAST(f AS FLOAT)), typeof(sum(CAST(f AS FLOAT))) FROM VALUES (1.5), (2.5), (NULL) AS t(f);

SELECT mean(i), mean(d), typeof(mean(d)) FROM l2_ints;

SELECT sum(DISTINCT i), avg(DISTINCT i), sum(DISTINCT d), avg(DISTINCT d), sum(i) FILTER (WHERE l > 5), avg(d) FILTER (WHERE g = 'a') FROM l2_ints;

SELECT g, sum(d) FILTER (WHERE d > 1), count(*) FROM l2_ints GROUP BY g ORDER BY g;

-- BIGINT sums wrap like Java's long
SELECT sum(l) FROM VALUES (9223372036854775807L), (1L) AS t(l);

SELECT g, sum(l) FROM VALUES ('x', 9223372036854775807L), ('x', 9223372036854775807L), ('y', -9223372036854775807L), ('y', -9223372036854775807L), ('y', -9223372036854775807L) AS t(g, l) GROUP BY g ORDER BY g;

SELECT sum(i) FROM VALUES (2147483647), (2147483647), (2147483647) AS t(i);

SELECT avg(l) FROM VALUES (9223372036854775807L), (9223372036854775807L), (1L) AS t(l);

-- decimals: result types, rounding, overflow to NULL
SELECT typeof(sum(d)), typeof(avg(d)), sum(d), avg(d) FROM VALUES (CAST(1.5 AS DECIMAL(3, 1))), (CAST(2.25 AS DECIMAL(3, 1))), (CAST(1 AS DECIMAL(3, 1))) AS t(d);

SELECT avg(d) FROM VALUES (CAST(1 AS DECIMAL(10, 0))), (CAST(2 AS DECIMAL(10, 0))) AS t(d);

SELECT avg(d), typeof(avg(d)) FROM VALUES (CAST(1 AS DECIMAL(10, 2))), (CAST(1 AS DECIMAL(10, 2))), (CAST(2 AS DECIMAL(10, 2))) AS t(d);

SELECT avg(d) FROM VALUES (CAST(-1 AS DECIMAL(10, 2))), (CAST(-1 AS DECIMAL(10, 2))), (CAST(-2 AS DECIMAL(10, 2))) AS t(d);

SELECT avg(d) FROM VALUES (CAST(0.00005 AS DECIMAL(10, 5))), (CAST(0 AS DECIMAL(10, 5))) AS t(d);

SELECT sum(d), typeof(sum(d)) FROM VALUES (CAST(99999999999999999999999999999999999999 AS DECIMAL(38, 0))), (CAST(1 AS DECIMAL(38, 0))) AS t(d);

SELECT sum(d), avg(d), typeof(sum(d)), typeof(avg(d)) FROM VALUES (CAST(99999999999999999999999999999999999998 AS DECIMAL(38, 0))), (CAST(1 AS DECIMAL(38, 0))) AS t(d);

SELECT sum(d), typeof(sum(d)) FROM VALUES (CAST(9999999999 AS DECIMAL(10, 0))), (CAST(9999999999 AS DECIMAL(10, 0))) AS t(d);

SELECT sum(d), avg(d), typeof(sum(d)), typeof(avg(d)) FROM VALUES (CAST(99999.5 AS DECIMAL(28, 18))), (CAST(1.25 AS DECIMAL(28, 18))) AS t(d);

SELECT sum(d), avg(d), sum(CAST(d AS DECIMAL(5, 1))), avg(CAST(d AS DECIMAL(18, 4))) FROM VALUES (1.25), (2.5), (NULL) AS t(d);

-- window aggregates, frames
SELECT g, i, sum(i) OVER (PARTITION BY g ORDER BY i), avg(d) OVER (PARTITION BY g ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM l2_ints ORDER BY g, i;

SELECT g, i, sum(l) OVER (PARTITION BY g), sum(d) OVER (ORDER BY g, i ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) FROM l2_ints ORDER BY g, i;

SELECT l, sum(l) OVER (ORDER BY l ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), avg(l) OVER (ORDER BY l ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM VALUES (9223372036854775807L), (1L), (2L), (3L) AS t(l) ORDER BY l;

-- other aggregate contexts
SELECT g, sum(i), avg(d) FROM l2_ints GROUP BY ROLLUP (g) ORDER BY g;

SELECT g, sum(i) FROM l2_ints GROUP BY g HAVING sum(l) > 10 ORDER BY g;

SELECT g, sum(i) FROM l2_ints GROUP BY g ORDER BY sum(d) DESC NULLS LAST, g;

SELECT * FROM (SELECT g, i FROM l2_ints) PIVOT (sum(i) AS s, avg(i) AS a FOR g IN ('a', 'b', 'c'));

SELECT * FROM (SELECT g, d FROM l2_ints) PIVOT (sum(d) FOR g IN ('a', 'b'));

SELECT sum(x) FROM VALUES ('1'), ('2.5'), (NULL) AS t(x);

SELECT sum(b) FROM VALUES (true), (false) AS t(b);

SELECT sum(1), sum(1.5), sum(NULL), avg(1), avg(1.5), avg(NULL), typeof(avg(NULL));

SELECT sum(x), avg(x) FROM range(1, 5001) AS t(x);

SELECT g, sum(x), avg(x) FROM (SELECT id % 7 AS g, id AS x FROM range(1, 5001)) GROUP BY g ORDER BY g;

-- ===== L9: format_string / printf follow java.util.Formatter =====

SELECT format_string('%s', CAST(NULL AS STRING)), format_string('%d', CAST(NULL AS INT)), format_string('%5s|%-5s|', CAST(NULL AS STRING), CAST(NULL AS STRING)), format_string('%b', CAST(NULL AS INT)), format_string('%f', CAST(NULL AS DOUBLE));

SELECT format_string(CAST(NULL AS STRING), 1), printf(CAST(NULL AS STRING));

SELECT format_string('Hello %s, you are %d years old', 'Bob', 30), printf('%s-%s', 'a', 'b'), format_string('no args');

SELECT format_string('%5d|%-5d|%05d|%+d|% d|%,d|%(d', 42, 42, 42, 42, 42, 1234567, -5);

SELECT format_string('%d %d %d %d', CAST(1 AS TINYINT), CAST(2 AS SMALLINT), 3, 4L);

SELECT format_string('%x|%X|%o|%08x|%#x', 255, 255, 8, 255, 255), format_string('%x', -1), format_string('%x', CAST(-1 AS TINYINT)), format_string('%x', -1L);

SELECT format_string('%.2f|%10.3f|%-10.1f|%+.1f|%,.2f|%f', 3.14159D, 2.5D, 0.25D, 1.05D, 1234567.891D, 1.5D);

SELECT format_string('%.1f|%.1f|%.1f|%.0f|%.0f|%.0f', 0.25D, 0.35D, 0.15D, 0.5D, 1.5D, 2.5D);

SELECT format_string('%s|%s', CAST(1.005 AS DECIMAL(10, 3)), CAST(2.0005 AS DECIMAL(10, 4)));

SELECT format_string('%e|%.2e|%E|%g|%.3g|%g', 12345.6789D, 0.000123D, 1.5D, 12345.6789D, 0.0001234D, 1234567.0D);

SELECT format_string('%f|%e|%s|%.1f', CAST('NaN' AS DOUBLE), CAST('Infinity' AS DOUBLE), CAST('-Infinity' AS DOUBLE), CAST('Infinity' AS DOUBLE));

SELECT format_string('%s|%s|%s|%s|%s', 1.0D, 100.0D, 1.0E10D, 0.00012D, 123456.789D), format_string('%s|%s', CAST(1.5 AS FLOAT), true);

SELECT format_string('%s|%10s|%-10s|%.2s|%S', 'abc', 'abc', 'abc', 'abc', 'abc');

SELECT format_string('%b|%b|%B', true, false, 'x'), format_string('%c|%c', 65, 233);

SELECT format_string('%2$s %1$s %2$s', 'a', 'b'), format_string('%s %<s %s', 'a', 'b'), format_string('100%%'), format_string('a%nb');

SELECT format_string('%d', 'x');

SELECT format_string('%s %s', 'only-one');

SELECT format_string('%y', 1);

SELECT format_string('%d', 1.5D);

SELECT format_string('%s', array(1, 2)), format_string('%s', DATE '2020-01-02'), format_string('%s', CAST('2020-01-02 03:04:05' AS TIMESTAMP));

SELECT format_string('%s|%d', s, i) FROM VALUES ('a', 1), (CAST(NULL AS STRING), 2), ('c', CAST(NULL AS INT)) AS t(s, i);

-- ===== L9: -0.0 and 0.0 are different values in array set functions =====

SELECT array_distinct(array(-0.0D, 0.0D)), array_distinct(array(0.0D, -0.0D, 0.0D, -0.0D)), array_distinct(array(1.0D, NULL, 1.0D, NULL, -0.0D));

SELECT array_union(array(0.0D), array(-0.0D)), array_union(array(1.0D, -0.0D), array(0.0D, 2.0D));

SELECT array_except(array(0.0D, 1.0D), array(-0.0D)), array_except(array(-0.0D, 1.0D), array(-0.0D)), array_except(array(0.0D), array(0.0D));

SELECT array_intersect(array(0.0D, 1.0D), array(-0.0D, 1.0D)), array_intersect(array(-0.0D, 1.0D), array(-0.0D));

SELECT array_distinct(array(CAST(0.0 AS FLOAT), CAST(-0.0 AS FLOAT))), array_distinct(array(CAST('NaN' AS DOUBLE), CAST('NaN' AS DOUBLE), 1.0D));

SELECT array_distinct(array(1, 2, 2, NULL, NULL)), array_union(array(1, 2), array(2, 3)), array_except(array(1, 2, 3), array(2)), array_intersect(array(1, 2, 3), array(2, 3, 4));

-- ===== L3: window / session_window keep the source's table alias =====

-- @setup
CREATE TEMPORARY VIEW l3_ev AS SELECT * FROM VALUES
  ('u1', TIMESTAMP '2024-01-01 00:00:00', 1), ('u1', TIMESTAMP '2024-01-01 00:03:00', 2),
  ('u1', TIMESTAMP '2024-01-01 00:20:00', 3), ('u2', TIMESTAMP '2024-01-01 00:01:00', 4),
  ('u2', TIMESTAMP '2024-01-01 00:02:00', NULL), ('u3', TIMESTAMP '2024-01-01 00:30:00', 6),
  ('u3', CAST(NULL AS TIMESTAMP), 7) AS l3_ev(u, ts, v);

-- @setup
CREATE TEMPORARY VIEW l3_users AS SELECT * FROM VALUES ('u1', 'Ann'), ('u2', 'Bob'), ('u3', 'Cy') AS l3_users(u, name);

SELECT window.start, count(*), sum(e.v) FROM l3_ev e GROUP BY window(e.ts, '10 minutes') ORDER BY start;

SELECT e.u, window.start, sum(e.v) FROM l3_ev AS e WHERE e.v > 1 GROUP BY e.u, window(e.ts, '10 minutes') HAVING sum(e.v) > 1 ORDER BY e.u, start;

SELECT l3_ev.u, window.end, count(l3_ev.v) FROM l3_ev GROUP BY l3_ev.u, window(l3_ev.ts, '15 minutes', '5 minutes') ORDER BY l3_ev.u, 2;

SELECT window, count(*) FROM l3_ev GROUP BY window(ts, '10 minutes') ORDER BY window.start;

SELECT window.start AS s, count(*) FROM l3_ev WHERE u = 'u1' OR v IS NULL GROUP BY window(ts, '10 minutes') ORDER BY s;

SELECT e.u, window.start, count(*) FROM l3_ev e GROUP BY e.u, window(e.ts, '10 minutes') ORDER BY e.u, window.start;

SELECT count(*) FROM l3_ev e GROUP BY window(e.ts, '10 minutes', '5 minutes') ORDER BY window.start;

SELECT window.start, count(*) FROM l3_ev GROUP BY window(ts, '10 minutes') HAVING window.start >= TIMESTAMP '2024-01-01 00:10:00' ORDER BY 1;

SELECT x.start, x.n FROM (SELECT window.start AS start, count(*) AS n FROM l3_ev e GROUP BY window(e.ts, '10 minutes')) x ORDER BY x.start;

WITH w AS (SELECT window.start AS start, sum(e.v) AS total FROM l3_ev e GROUP BY window(e.ts, '10 minutes')) SELECT * FROM w ORDER BY start;

SELECT u.name, window.start, count(*) FROM l3_ev e JOIN l3_users u ON e.u = u.u GROUP BY u.name, window(e.ts, '10 minutes') ORDER BY u.name, start;

SELECT e.u, session_window.start, session_window.end, count(*), sum(e.v) FROM l3_ev e GROUP BY e.u, session_window(e.ts, '5 minutes') ORDER BY e.u, start;

SELECT l3_ev.u, session_window.start, count(l3_ev.v) FROM l3_ev WHERE l3_ev.v IS NOT NULL GROUP BY l3_ev.u, session_window(l3_ev.ts, '5 minutes') ORDER BY l3_ev.u, start;

SELECT e.u, session_window.start FROM (SELECT * FROM l3_ev) AS e GROUP BY e.u, session_window(e.ts, '5 minutes') ORDER BY e.u, start;

SELECT window.start, count(*) FROM (SELECT * FROM l3_ev WHERE v > 2) GROUP BY window(ts, '10 minutes') ORDER BY start;

-- ===== L9: BIGINT arguments of INT parameters wrap like Spark's implicit cast =====

SELECT add_months('2016-08-31', 9223372036854775807L), add_months('2016-08-31', -9223372036854775808L), add_months('2016-08-31', 4294967297L);

SELECT bround(2.5D, 4294967297L), round(2.5D, 4294967297L), bround(25, -9223372036854775808L);

SELECT date_from_unix_date(9223372036854775807L), date_from_unix_date(x) FROM VALUES (4294967297L) AS t(x);

SELECT left('abcdef', x), right('abcdef', x), repeat('ab', x), substring('hello', x, 2), lpad('a', x, 'x'), rpad('a', x, 'x') FROM VALUES (4294967299L) AS t(x);

SELECT conv('100', 2, 9223372036854775807L), conv('100', -9223372036854775808L, 10), bit_get(11, -9223372036854775808L), getbit(11, 4294967296L);

SELECT make_ym_interval(1, 9223372036854775807L), make_dt_interval(9223372036854775807L), make_date(2020, 4294967297L, 1);

SELECT shiftleft(1, 4294967297L), shiftright(8, 4294967297L), shiftrightunsigned(8, 4294967297L);

SELECT slice(array(1, 2, 3), x, 2) FROM VALUES (4294967297L) AS t(x);

SELECT array_repeat('a', x) FROM VALUES (4294967297L) AS t(x);

SELECT add_months('2016-08-31', x), left('abcdef', x) FROM VALUES (2L), (CAST(NULL AS BIGINT)), (-4294967295L) AS t(x);

-- ===== L7: deep nesting =====

SELECT ((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((1))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))));

SELECT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT true;

SELECT - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 1;

-- ===== L6: array_sort comparator shapes use a sort =====

SELECT array_sort(array(5, 6, 1, 3, 2), (l, r) -> CASE WHEN l < r THEN -1 WHEN l > r THEN 1 ELSE 0 END);

SELECT array_sort(array(5, 6, 1, 3, 2), (l, r) -> CASE WHEN l < r THEN 1 WHEN l > r THEN -1 ELSE 0 END);

SELECT array_sort(array(5, 6, 1, 3, 2), (l, r) -> CASE WHEN l > r THEN 1 WHEN l < r THEN -1 ELSE 0 END), array_sort(array(5, 6, 1, 3, 2), (l, r) -> CASE WHEN l > r THEN -1 WHEN l < r THEN 1 ELSE 0 END);

SELECT array_sort(array(5, 6, 1, 3, 2), (x, y) -> CASE WHEN y < x THEN -1 WHEN y > x THEN 1 ELSE 0 END), array_sort(array(5, 6, 1, 3, 2), (x, y) -> CASE WHEN y > x THEN 1 WHEN y < x THEN -1 ELSE 0 END);

SELECT array_sort(array(3, 1, 3, 2, 1, 2), (l, r) -> CASE WHEN l < r THEN -1 WHEN l > r THEN 1 ELSE 0 END), array_sort(array(3, 1, 3, 2, 1, 2), (l, r) -> CASE WHEN l < r THEN 1 WHEN l > r THEN -1 ELSE 0 END);

SELECT array_sort(array('bc', 'ab', 'a', 'abc', ''), (l, r) -> CASE WHEN l < r THEN -1 WHEN l > r THEN 1 ELSE 0 END), array_sort(array('bc', 'ab', 'a', 'abc', ''), (l, r) -> CASE WHEN length(l) < length(r) THEN -1 WHEN length(l) > length(r) THEN 1 ELSE 0 END);

SELECT array_sort(array('bb', 'a', 'cc', 'd', 'eee', 'ff'), (l, r) -> CASE WHEN length(l) < length(r) THEN 1 WHEN length(l) > length(r) THEN -1 ELSE 0 END);

SELECT array_sort(array(1.5D, -0.5D, CAST('NaN' AS DOUBLE), 0.0D, 2.5D), (l, r) -> CASE WHEN l < r THEN -1 WHEN l > r THEN 1 ELSE 0 END), array_sort(array(true, false, true), (l, r) -> CASE WHEN l < r THEN -1 WHEN l > r THEN 1 ELSE 0 END);

SELECT array_sort(array(DATE '2020-03-01', DATE '2019-01-01', DATE '2021-01-01'), (l, r) -> CASE WHEN l < r THEN -1 WHEN l > r THEN 1 ELSE 0 END);

SELECT array_sort(array(struct(2, 'b'), struct(1, 'z'), struct(2, 'a')), (l, r) -> CASE WHEN l.col1 < r.col1 THEN -1 WHEN l.col1 > r.col1 THEN 1 ELSE 0 END);

SELECT array_sort(array(struct(2, 'b'), struct(1, 'z'), struct(2, 'a')), (l, r) -> CASE WHEN l.col2 < r.col2 THEN 1 WHEN l.col2 > r.col2 THEN -1 ELSE 0 END);

SELECT array_sort(array(5, NULL, 1, 3), (l, r) -> CASE WHEN l IS NULL AND r IS NULL THEN 0 WHEN l IS NULL THEN -1 WHEN r IS NULL THEN 1 WHEN l < r THEN -1 WHEN l > r THEN 1 ELSE 0 END);

SELECT array_sort(array(5, NULL, 1, 3), (l, r) -> CASE WHEN l IS NULL AND r IS NULL THEN 0 WHEN l IS NULL THEN 1 WHEN r IS NULL THEN -1 WHEN l < r THEN 1 WHEN l > r THEN -1 ELSE 0 END);

SELECT array_sort(array(5, 6, 1), (l, r) -> r - l), array_sort(array(5, 6, 1), (l, r) -> l - r), array_sort(array(2, 4, 1, 3), (l, r) -> CASE WHEN l % 2 < r % 2 THEN -1 WHEN l % 2 > r % 2 THEN 1 ELSE 0 END);

SELECT array_sort(array(CAST(NULL AS INT)), (l, r) -> l - r), array_sort(array(1), (l, r) -> CAST(NULL AS INT)), array_sort(CAST(array() AS ARRAY<INT>), (l, r) -> CAST(NULL AS INT));

SELECT array_sort(array(1, 2, 3), (l, r) -> CAST(NULL AS INT));

SELECT array_sort(array(5, NULL, 1), (l, r) -> l - r);

SELECT array_sort(CAST(NULL AS ARRAY<INT>), (l, r) -> CASE WHEN l < r THEN -1 WHEN l > r THEN 1 ELSE 0 END);

SELECT a, array_sort(a, (l, r) -> CASE WHEN l < r THEN -1 WHEN l > r THEN 1 ELSE 0 END) FROM VALUES (array(3, 1, 2)), (array(CAST(NULL AS INT), 2)), (array(9, 8)), (CAST(NULL AS ARRAY<INT>)) AS t(a);

SELECT size(array_sort(sequence(2000, 1, -1), (l, r) -> CASE WHEN l < r THEN -1 WHEN l > r THEN 1 ELSE 0 END)), array_sort(sequence(2000, 1, -1), (l, r) -> CASE WHEN l < r THEN -1 WHEN l > r THEN 1 ELSE 0 END)[0];

-- ===== L4: CHAR(n) for any n =====

-- @setup
CREATE TABLE l4_t (a CHAR(1), b CHAR(255), c CHAR(256), d CHAR(1000), e CHAR(65535), f VARCHAR(10), g CHAR(3)) USING parquet;

INSERT INTO l4_t VALUES ('x', 'y', 'z', 'w', 'v', 'u', 'ab');

INSERT INTO l4_t VALUES ('p', repeat('q', 255), repeat('r', 256), repeat('s', 300), 'e', 'f', NULL);

SELECT a, length(b), length(c), length(d), length(e), f, length(g), g = 'ab ', g = 'ab' FROM l4_t ORDER BY a;

SELECT length(c), c = repeat('z', 1) || repeat(' ', 255), length(d), length(e) FROM l4_t WHERE a = 'x';

SELECT concat(g, '|'), upper(g), trim(g), g IS NULL, rpad(g, 5, '-') FROM l4_t ORDER BY a;

INSERT INTO l4_t VALUES ('too long', 'y', 'z', 'w', 'v', 'u', 'ab');

INSERT INTO l4_t VALUES ('x', 'y', repeat('z', 257), 'w', 'v', 'u', 'ab');

INSERT INTO l4_t VALUES ('x', 'y', 'z', 'w', 'v', 'u', 'abcd');

CREATE TABLE l4_zero (a CHAR(0)) USING parquet;

INSERT INTO l4_zero VALUES ('');

SELECT length(a) FROM l4_zero;

CREATE TABLE l4_t2 (a CHAR(300)) USING parquet;

INSERT INTO l4_t2 SELECT 'abc';

SELECT length(a), a = 'abc' FROM l4_t2;

SELECT cast('ab' AS CHAR(300)), length(cast('ab' AS CHAR(3))), typeof(cast('ab' AS CHAR(5)));

-- ===== L5: struct values are stored by position =====

-- @setup
CREATE TABLE l5_t (s STRUCT<a: INT, b: STRING>, n INT) USING parquet;

INSERT INTO l5_t VALUES (named_struct('x', 1, 'y', 'q'), 1);

INSERT INTO l5_t SELECT named_struct('p', 2, 'q', 'r'), 2;

INSERT INTO l5_t SELECT struct(3, 's'), 3;

INSERT INTO l5_t VALUES (named_struct('a', 4, 'b', 't'), 4), (named_struct('a', 5, 'b', 'u'), 5);

INSERT INTO l5_t SELECT named_struct('x', n, 'y', concat('n', n)), n + 10 FROM l5_t WHERE n < 3;

SELECT s, s.a, s.b, n FROM l5_t ORDER BY n;

SELECT named_struct('a', 1, 'b', 2) = named_struct('a', 1, 'b', 2), struct(1, 2) = struct(1, 2L), array(struct(1, 2), struct(1, 2L));

-- ===== L9: VALUES and UNION unify types like Spark =====

SELECT typeof(x), x FROM VALUES (-128Y), (1) AS t(x);

SELECT typeof(x), x FROM VALUES (1), (-128Y) AS t(x);

SELECT typeof(x), x FROM VALUES (1S), (1) AS t(x);

SELECT typeof(x), x FROM VALUES (127Y), (1), (200S) AS t(x);

SELECT typeof(x), x FROM VALUES (1Y), (2Y) AS t(x);

SELECT typeof(x), x FROM VALUES (CAST(1 AS DECIMAL(38, 0))), (1.8) AS t(x);

SELECT typeof(x), x FROM VALUES (1.8), (CAST(1 AS DECIMAL(38, 0))) AS t(x);

SELECT typeof(x), x FROM VALUES (CAST(1 AS DECIMAL(10, 2))), (1), (CAST(1.5 AS DECIMAL(5, 4))) AS t(x);

SELECT typeof(a), typeof(b), a, b FROM VALUES (1, 1.5), (2147483648, 2) AS t(a, b);

SELECT typeof(x), x FROM (SELECT CAST(1 AS DECIMAL(38, 0)) AS x UNION ALL SELECT 1.8);

SELECT typeof(x), x FROM (SELECT 1 AS x UNION ALL SELECT 2Y UNION ALL SELECT 3S);

SELECT CAST(1 AS DECIMAL(38, 0)) = 1.8, CAST(1 AS DECIMAL(38, 0)) < 1.8, CAST(5 AS DECIMAL(38, 0)) IN (4.5, 5.0);

-- ===== L9: untyped NULL arguments that Spark rejects (spark_strict) =====

SELECT array_contains(NULL, NULL);

SELECT array_contains(array(1), NULL);

SELECT array_contains(NULL, 1);

SELECT array_position(NULL, 1);

SELECT map_contains_key(map(1, 2), NULL);

SELECT array_append(NULL, 1);

SELECT element_at(NULL, 1);

SELECT array_contains(array(1, NULL), CAST(NULL AS INT)), array_position(array(1, 2), CAST(NULL AS INT)), map_contains_key(map(1, 2), 1);

-- ===== L9: corpus tail (Spark's own sql-tests inputs) =====

SELECT count(DISTINCT 3, 2), count(1, 2), count(DISTINCT 'a', 'a');

SELECT count(DISTINCT a, b), count(a, b), count(DISTINCT a), count(*), count(a, b, a) FROM VALUES (1, 2), (1, 2), (CAST(NULL AS INT), 1), (2, CAST(NULL AS INT)), (3, 4) AS t(a, b);

SELECT g, count(DISTINCT a, b) FILTER (WHERE a > 0), count(a, b) FILTER (WHERE a > 1) FROM VALUES ('x', 1, 2), ('x', 1, 2), ('y', 1, 3), ('y', 2, 3) AS t(g, a, b) GROUP BY g ORDER BY g;

SELECT a, count(a, b) OVER (ORDER BY a), count(a) OVER (ORDER BY a) FROM VALUES (1, 2), (2, CAST(NULL AS INT)), (3, 3) AS t(a, b) ORDER BY a;

SELECT count(DISTINCT a, b) FROM VALUES (1, 'x'), (1, 'x'), (1, 'y') AS t(a, b);

SELECT percentile_disc(0.25) WITHIN GROUP (ORDER BY v), percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FROM VALUES (0), (1), (2), (10) AS t(v);

SELECT avg(x), typeof(avg(x)), sum(x) FROM VALUES (TIMESTAMP '2020-01-01 00:00:00.5'), (TIMESTAMP '2020-01-02 00:00:00') AS t(x);

SELECT avg(x) FROM VALUES (DATE '2020-01-01') AS t(x);

SELECT sum(x) FROM VALUES (DATE '2020-01-01') AS t(x);
