-- Extra Spark-vs-DuckDB checks for features fixed after the first full run.

-- @setup
CREATE TEMPORARY VIEW ev AS SELECT * FROM VALUES
  ('u1', TIMESTAMP '2024-01-01 00:00:00', 1), ('u1', TIMESTAMP '2024-01-01 00:03:00', 2),
  ('u1', TIMESTAMP '2024-01-01 00:20:00', 3), ('u2', TIMESTAMP '2024-01-01 00:01:00', 4),
  ('u2', TIMESTAMP '2024-01-01 00:02:00', NULL), ('u3', TIMESTAMP '2024-01-01 00:30:00', 6)
  AS ev(u, ts, v);

SELECT u, session_window.start, session_window.end, count(*), sum(v) FROM ev WHERE v IS NOT NULL OR u = 'u2' GROUP BY u, session_window(ts, '5 minutes') ORDER BY u, start;

SELECT session_window.start, count(*) FROM ev GROUP BY session_window(ts, '10 minutes') ORDER BY start;

SELECT u, window.start, window.end, count(*) FROM ev GROUP BY u, window(ts, '10 minutes', '5 minutes') ORDER BY u, start;

SELECT window.start, sum(v) FROM ev GROUP BY window(ts, '15 minutes') ORDER BY start;

SELECT name, count(*) OVER w FROM VALUES ('a'), ('b') AS t(name) WINDOW w AS (PARTITION BY name) ORDER BY name;

SELECT typeof(sum(CAST(v AS DECIMAL(5, 2)))), typeof(avg(CAST(v AS DECIMAL(5, 2)))), sum(CAST(v AS DECIMAL(5, 2))), avg(CAST(v AS DECIMAL(5, 2))) FROM ev;

SELECT u, sum(v), avg(v), typeof(sum(v)), typeof(avg(v)) FROM ev GROUP BY u;

SELECT sum(DISTINCT v), avg(DISTINCT v), sum(v) FILTER (WHERE v > 2) FROM ev;

SELECT try_avg(v), try_avg(CAST(v AS BIGINT)), try_sum(v) FROM ev;

SELECT try_avg(col) FROM VALUES (9223372036854775807L), (1L) AS tab(col);

SELECT try_avg(col) FROM VALUES (INTERVAL '10' DAY), (INTERVAL '20' DAY) AS tab(col);

SELECT median(col), percentile(col, array(0.25, 0.75)) FROM VALUES (INTERVAL '1' MONTH), (INTERVAL '5' MONTH) AS tab(col);

SELECT * FROM (SELECT u, v, hour(ts) AS h FROM ev) PIVOT (count(v) AS c, sum(v) AS s FOR u IN ('u1', 'u2', 'u4'));

SELECT * FROM (SELECT u, v FROM ev) PIVOT (count(*) FOR u IN ('u1', 'u4'));

-- @setup
CREATE TABLE ch (c CHAR(4), s STRUCT<a: INT, b: STRING>, n INT) USING parquet;

INSERT INTO ch VALUES ('ab', STRUCT(1, 'x'), 1), ('abcd', STRUCT(2, 'y'), 2), ('a  ', NULL, 3);

SELECT c, length(c), c = 'ab  ', s.a, s.b, n FROM ch ORDER BY n;

SELECT array_sort(array(3, 1, 2), (l, r) -> CASE WHEN l < r THEN 1 WHEN l > r THEN -1 ELSE 0 END);

SELECT array_sort(array('b', NULL, 'a'), (left, right) -> CASE WHEN left IS NULL THEN 1 WHEN right IS NULL THEN -1 WHEN left < right THEN -1 WHEN left > right THEN 1 ELSE 0 END);

-- exact decimal division (it went through DOUBLE): HALF_UP to Spark's result scale, NULL past the result precision
SELECT CAST(1 AS TINYINT) / CAST(1.5 AS DECIMAL(38,18)), CAST(12345678901234567890123456789 AS DECIMAL(38,0)) / CAST(7 AS DECIMAL(10,0)), CAST(-2 AS DECIMAL(5,0)) / CAST(3 AS DECIMAL(5,0)), CAST('-70016626334787.1' AS DECIMAL(18,1)) / CAST('-0.00000000000000000002' AS DECIMAL(20,20));

SELECT x / y, typeof(x / y) FROM VALUES (CAST(10 AS DECIMAL(20,2)), CAST(3 AS DECIMAL(10,4))), (CAST(-1 AS DECIMAL(20,2)), CAST(0 AS DECIMAL(10,4))), (CAST(5 AS DECIMAL(20,2)), CAST(-6 AS DECIMAL(10,4))) AS t(x, y);

-- decimal to integral casts wrap like Decimal.toLong; integral to BINARY is the big-endian bytes
SELECT CAST(CAST(300.7 AS DECIMAL(10,1)) AS TINYINT), CAST(CAST(99999999999 AS DECIMAL(20,0)) AS INT), CAST(CAST(1e20 AS DECIMAL(38,0)) AS BIGINT), CAST(CAST(-1.9 AS DECIMAL(3,1)) AS INT);

SELECT hex(CAST(CAST(1 AS TINYINT) AS BINARY)), hex(CAST(CAST(-2 AS SMALLINT) AS BINARY)), hex(CAST(258 AS BINARY)), hex(CAST(-1L AS BINARY));

-- result types: median / percentile DOUBLE, round of NULL and of strings DOUBLE
SELECT median(x), percentile(x, 0.5), round(NULL), bround(NULL), round('1.55', 1), bround('2.5') FROM VALUES (CAST(1 AS DECIMAL(5,2))), (CAST(2 AS DECIMAL(5,2))) AS t(x);

-- skewness / kurtosis of a constant column are NULL
SELECT skewness(v), kurtosis(v) FROM VALUES (1), (1), (1) AS t(v);

-- a boolean equals a fractional number cast from the boolean
SELECT true = 1.5D, true = 1.0D, true = CAST(1 AS DECIMAL(10,0)), true <=> CAST(1 AS FLOAT), CAST(2 AS DECIMAL(5,1)) = true, true <> 1.5D;

-- IN over structs compares by position; operands of different kinds are an analysis error (they crashed before)
SELECT named_struct('a', 1, 'b', 'x') IN (named_struct('c', 1, 'd', 'x')), named_struct('a', 1, 'b', 'x') IN (CAST(named_struct('a', 1, 'b', 'x') AS STRUCT<a:BIGINT,b:STRING>));

SELECT true IN (named_struct('a', 1, 'b', 'x'), true);

SELECT array(1, 2) IN (named_struct('a', 1, 'b', 'x'), array(1, 2));

SELECT named_struct('a', 1) IN (map(1, 'a'), named_struct('a', 1));

-- array_insert past Spark's array size limit is an error (it exhausted memory before)
SELECT array_insert(array(1, 2, 3, 4), 2147483647, 5);

-- string arguments of INT / DOUBLE parameters are cast (NULL when malformed)
SELECT regexp_extract('100-200', '(\\d+)-(\\d+)', 'a'), regexp_extract('100-200', '(\\d+)-(\\d+)', '2'), sha2('Spark', 'a'), atan2(0, 'a'), power('2', 0.5);

-- DOUBLE parameters take numbers and strings; other types are an analysis error
SELECT sqrt('4'), pow('2', 2), skewness(x), kurtosis(x) FROM VALUES ('1'), ('2'), ('10') AS t(x);

SELECT sqrt(true);

SELECT pow(2, DATE'2024-01-01');

SELECT skewness(v) FROM VALUES (true) AS t(v);

-- IN over structs that need a cast: the field names must match (case-insensitive)
SELECT named_struct('A', 1, 'B', 'x') IN (CAST(named_struct('a', 1, 'b', 'x') AS STRUCT<a:BIGINT,b:STRING>));

SELECT named_struct('x', 1, 'y', 'x') IN (CAST(named_struct('a', 1, 'b', 'x') AS STRUCT<a:BIGINT,b:STRING>));

-- sequence past Spark's array size limit is an error before anything is allocated (it asked for 32 GiB before)
SELECT sequence(4294967297L, 5);

SELECT sequence(1, 4294967297L);

SELECT sequence(1, 5), sequence(5, 1), sequence(1, 10, 3), sequence(10, 1, -4), size(sequence(1, 100000)), sequence(1, 1);
