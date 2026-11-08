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
