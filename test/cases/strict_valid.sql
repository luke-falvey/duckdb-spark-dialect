-- Valid Spark 3.5 queries that a too-aggressive spark_strict could break. Both dialects must return Spark's rows.

-- @setup
CREATE TABLE sv_t (a INT, b STRING, ts TIMESTAMP) USING parquet;

-- @setup
INSERT INTO sv_t VALUES (1, 'x', TIMESTAMP '2024-01-01 00:00:00'), (2, 'y', TIMESTAMP '2024-01-01 00:03:00'), (2, 'z', TIMESTAMP '2024-01-01 00:20:00');

FROM sv_t SELECT a ORDER BY a;

SELECT a, count(*) AS c FROM sv_t GROUP BY ALL ORDER BY a;

SELECT a FROM sv_t ORDER BY 1 LIMIT ALL;

-- @nondeterministic
SELECT a FROM sv_t TABLESAMPLE (50 PERCENT);

-- @nondeterministic
SELECT a FROM sv_t TABLESAMPLE (2 ROWS);

SELECT * FROM (SELECT a, b FROM sv_t) PIVOT (count(b) FOR a IN (1, 2));

SELECT ARRAY(1, 2)[0];

SELECT MAP('a', 1)['a'];

SELECT STRUCT(1 AS x).x;

SELECT transform(array(1, 2), x -> x + 1);

SELECT transform(array(1, 2), (x, i) -> x + i);

SELECT filter(array(1, 2, 3), x -> x > 1);

SELECT try_cast('a' AS INT);

SELECT CAST(NULL AS STRUCT<a: INT>);

SELECT CAST('2024-01-01' AS DATE);

SELECT CAST(1 AS DECIMAL(10, 2));

SELECT CAST(TIMESTAMP '2024-01-01' AS TIMESTAMP_NTZ);

SELECT CAST(ARRAY(1) AS ARRAY<BIGINT>);

SELECT CAST(MAP('a', 1) AS MAP<STRING, INT>);

SELECT IF(1 < 2, 'a', 'b');

SELECT `a` FROM sv_t ORDER BY `a`;

SELECT a, v FROM sv_t LATERAL VIEW explode(array(1, 2)) e AS v ORDER BY a, v;

SELECT a FROM sv_t SORT BY a;

SELECT a FROM sv_t CLUSTER BY a;

SELECT a FROM sv_t DISTRIBUTE BY a;

-- @setup
CREATE TABLE sv_u (a INT) USING parquet;

-- @setup
INSERT OVERWRITE sv_u SELECT a FROM sv_t;

SELECT count(*) FROM sv_u;

CACHE TABLE sv_u;

SET spark.sql.shuffle.partitions=4;

SET foo.bar=baz;

SELECT 1 <=> NULL, 7 DIV 2, !true, NOT true;

SELECT 1L, 1.5BD, 2S, 3Y, 1.5F, 1.5D;

SELECT X'1F', "double quoted string", r'raw\n', 'it''s';

SELECT INTERVAL '1' DAY;

SELECT INTERVAL 1 DAY 2 HOURS;

SELECT window.start, count(*) FROM sv_t GROUP BY window(ts, '5 minutes') ORDER BY 1;

SELECT session_window.start, count(*) FROM sv_t GROUP BY session_window(ts, '5 minutes') ORDER BY 1;

SELECT a, sum(a) OVER (ORDER BY a) AS s, rank() OVER (ORDER BY a) AS r FROM sv_t ORDER BY a, s;

SELECT count(*), count(DISTINCT a), sum(a), avg(a), max(b) FROM sv_t;

SELECT concat(b, 'x'), upper(b), substring(b, 1, 1), substr(b FROM 1 FOR 1), trim(' x '), position('x' IN b) FROM sv_t ORDER BY b;

SELECT extract(YEAR FROM ts), coalesce(NULL, a), nullif(a, 1), cast(a AS STRING) FROM sv_t ORDER BY a;

SELECT * FROM range(3);

SELECT * FROM explode(array(1, 2));

SELECT * FROM stack(2, 'a', 1, 'b', 2);

SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(x, y);

SELECT a FROM sv_t WHERE a IN (SELECT a FROM sv_u) AND a BETWEEN 1 AND 2 AND b LIKE '%' ORDER BY a;

SELECT x.a, y.a FROM sv_t x JOIN sv_u y ON x.a = y.a ORDER BY 1, 2;

SELECT a FROM sv_t UNION ALL SELECT a FROM sv_u ORDER BY a;

SELECT a FROM sv_t EXCEPT SELECT 1;

WITH c AS (SELECT a FROM sv_t) SELECT sum(a) FROM c;

SELECT CASE WHEN a = 1 THEN 'one' ELSE 'other' END FROM sv_t ORDER BY a;

SELECT array_sort(array(3, 1, 2)), sort_array(array(3, 1, 2)), size(array(1, 2)), element_at(array(1, 2), 1);

SELECT named_struct('a', 1, 'b', 2).b;

SELECT date_add(DATE '2024-01-01', 1), date_format(TIMESTAMP '2024-01-01 10:00:00', 'yyyy-MM'), to_date('2024-01-01');

SELECT explode(array(1, 2)) AS v;

SELECT inline(array(struct(1 AS a, 'x' AS b)));

SELECT * FROM sql_keywords() LIMIT 2;

-- @setup
CREATE TABLE sv_c (c CHAR(4), n INT) USING parquet;

-- @setup
INSERT INTO sv_c VALUES ('ab', 1);

SELECT c, length(c) FROM sv_c;

-- @setup
CREATE TABLE sv_nested (a ARRAY<INT>, m MAP<STRING, INT>, s STRUCT<x: INT, y: STRING>) USING parquet;

-- @setup
INSERT INTO sv_nested VALUES (ARRAY(1, 2), MAP('k', 1), STRUCT(5, 'w'));

SELECT a[0], m['k'], s.x, s.y FROM sv_nested;

-- @nondeterministic
DESCRIBE sv_nested;

-- @nondeterministic
SHOW TABLES;

-- @nondeterministic
EXPLAIN SELECT 1;

CREATE TEMPORARY VIEW sv_view AS SELECT 1 AS a;

SELECT * FROM sv_view;

USE default;

TRUNCATE TABLE sv_u;
