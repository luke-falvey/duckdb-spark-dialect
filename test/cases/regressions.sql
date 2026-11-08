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
