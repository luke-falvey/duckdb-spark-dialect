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
