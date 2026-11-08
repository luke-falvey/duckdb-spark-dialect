-- Query syntax: joins, grouping, windows, set operations, Spark-specific clauses.

-- @setup
CREATE TEMPORARY VIEW emp AS SELECT * FROM VALUES
  (1, 'Alice', 'eng', 100.0D, DATE '2020-01-15', ARRAY('java', 'scala'), MAP('level', 3)),
  (2, 'Bob', 'eng', 80.0D, DATE '2021-06-01', ARRAY('python'), MAP('level', 2)),
  (3, 'Cara', 'ops', 70.0D, DATE '2019-03-20', ARRAY(), MAP()),
  (4, 'Dan', 'ops', 70.0D, DATE '2022-11-11', NULL, NULL),
  (5, 'Eve', NULL, 120.0D, DATE '2018-07-04', ARRAY('go', 'rust', 'c'), MAP('level', 5, 'mgr', 1)),
  (6, 'Fay', 'sales', NULL, NULL, ARRAY(NULL), MAP('level', NULL))
  AS emp(id, name, dept, salary, hired, skills, attrs);

-- @setup
CREATE TEMPORARY VIEW dept AS SELECT * FROM VALUES
  ('eng', 'Engineering', 1), ('ops', 'Operations', 2), ('hr', 'People', 3)
  AS dept(code, title, floor);

-- @setup
CREATE OR REPLACE TEMPORARY VIEW sales AS SELECT * FROM VALUES
  ('2024-01', 'north', 'a', 10), ('2024-01', 'south', 'a', 20), ('2024-02', 'north', 'b', 30),
  ('2024-02', 'north', 'a', 5), ('2024-03', 'south', 'b', NULL), ('2024-03', 'east', 'a', 7)
  AS sales(month, region, product, amount);

-- ===== basic select =====
SELECT * FROM emp;

SELECT id, name AS n, salary * 1.1 AS raised FROM emp WHERE salary > 75 ORDER BY id;

SELECT DISTINCT dept FROM emp;

SELECT `name`, `dept` FROM emp AS `e` WHERE `e`.`id` = 1;

SELECT e.* FROM emp e WHERE e.id < 3;

SELECT name FROM emp ORDER BY salary DESC NULLS LAST, name LIMIT 3;

SELECT name FROM emp ORDER BY salary ASC, name LIMIT 2 OFFSET 2;

SELECT name FROM emp ORDER BY salary ASC NULLS LAST, name LIMIT 2 OFFSET 1;

SELECT name, salary FROM emp ORDER BY salary, name;

SELECT name, salary FROM emp ORDER BY salary DESC, name;

SELECT name, dept FROM emp ORDER BY dept NULLS FIRST, name;

SELECT id, name FROM emp ORDER BY 2 DESC;

SELECT 1 AS one, 'x' AS `weird name`, NULL AS n;

SELECT * FROM (SELECT id, name FROM emp) t WHERE t.id > 4;

SELECT * FROM VALUES (1, 'a'), (2, 'b') AS v(x, y);

SELECT * FROM VALUES (1), (2), (3);

SELECT col1, col2 FROM VALUES (1, 'a'), (2, 'b');

-- ===== Spark-only ordering/partition clauses =====
-- @unordered
SELECT id FROM emp SORT BY id;

SELECT id FROM emp DISTRIBUTE BY dept;

-- @unordered
SELECT id FROM emp CLUSTER BY id;

-- @unordered
SELECT id, dept FROM emp DISTRIBUTE BY dept SORT BY id;

-- ===== hints =====
SELECT /*+ BROADCAST(d) */ e.name, d.title FROM emp e JOIN dept d ON e.dept = d.code;

SELECT /*+ REPARTITION(4), COALESCE(1) */ count(*) FROM emp;

-- ===== joins =====
SELECT e.name, d.title FROM emp e JOIN dept d ON e.dept = d.code;

SELECT e.name, d.title FROM emp e INNER JOIN dept d ON e.dept = d.code;

SELECT e.name, d.title FROM emp e LEFT JOIN dept d ON e.dept = d.code;

SELECT e.name, d.title FROM emp e LEFT OUTER JOIN dept d ON e.dept = d.code;

SELECT e.name, d.title FROM emp e RIGHT JOIN dept d ON e.dept = d.code;

SELECT e.name, d.title FROM emp e RIGHT OUTER JOIN dept d ON e.dept = d.code;

SELECT e.name, d.title FROM emp e FULL JOIN dept d ON e.dept = d.code;

SELECT e.name, d.title FROM emp e FULL OUTER JOIN dept d ON e.dept = d.code;

SELECT e.name FROM emp e LEFT SEMI JOIN dept d ON e.dept = d.code;

SELECT e.name FROM emp e SEMI JOIN dept d ON e.dept = d.code;

SELECT e.name FROM emp e LEFT ANTI JOIN dept d ON e.dept = d.code;

SELECT e.name FROM emp e ANTI JOIN dept d ON e.dept = d.code;

SELECT count(*) FROM emp CROSS JOIN dept;

SELECT count(*) FROM emp, dept;

SELECT e.name, d.title FROM emp e, dept d WHERE e.dept = d.code;

SELECT * FROM (SELECT 'eng' AS code, 1 AS a) x JOIN dept USING (code);

SELECT * FROM (SELECT 'eng' AS code, 1 AS a) x NATURAL JOIN dept;

SELECT * FROM (SELECT 'eng' AS code) x LEFT JOIN dept USING (code);

SELECT e.name, d.title FROM emp e JOIN dept d ON e.dept = d.code AND d.floor > 1;

SELECT a.id, b.id FROM emp a JOIN emp b ON a.salary = b.salary AND a.id < b.id;

SELECT e.name, d.title FROM emp e JOIN dept d ON e.dept <=> d.code;

SELECT name, (SELECT max(title) FROM dept WHERE code = e.dept) FROM emp e;

-- ===== LATERAL VIEW =====
SELECT id, skill FROM emp LATERAL VIEW explode(skills) t AS skill;

SELECT id, skill FROM emp LATERAL VIEW OUTER explode(skills) t AS skill;

SELECT id, pos, skill FROM emp LATERAL VIEW posexplode(skills) t AS pos, skill;

SELECT id, pos, skill FROM emp LATERAL VIEW OUTER posexplode(skills) t AS pos, skill;

SELECT id, k, v FROM emp LATERAL VIEW explode(attrs) t AS k, v;

SELECT id, x, y FROM emp LATERAL VIEW explode(ARRAY(1, 2)) a AS x LATERAL VIEW explode(ARRAY('p', 'q')) b AS y WHERE id = 1;

SELECT t.col FROM emp LATERAL VIEW explode(skills) t WHERE id = 1;

SELECT id, a, b FROM emp LATERAL VIEW inline(ARRAY(STRUCT(1, 'x'), STRUCT(2, 'y'))) t AS a, b WHERE id < 3;

SELECT id, s1, s2 FROM emp LATERAL VIEW stack(2, 'a', 'b', 'c', 'd') t AS s1, s2 WHERE id = 1;

SELECT id, j1, j2 FROM emp LATERAL VIEW json_tuple('{"a":1,"b":"x"}', 'a', 'b') t AS j1, j2 WHERE id = 1;

-- ===== generators in the select list =====
SELECT id, explode(skills) FROM emp;

SELECT id, explode(skills) AS skill FROM emp;

SELECT id, posexplode(skills) FROM emp;

SELECT explode(ARRAY(1, 2, 3));

SELECT explode(MAP('a', 1, 'b', 2));

SELECT id, explode_outer(skills) FROM emp;

SELECT inline(ARRAY(STRUCT(1, 'a'), STRUCT(2, 'b')));

SELECT stack(3, 1, 2, 3);

SELECT * FROM explode(ARRAY(10, 20));

SELECT * FROM range(3);

SELECT * FROM range(1, 10, 3);

-- ===== aggregation =====
SELECT dept, count(*), count(salary), sum(salary), avg(salary), min(salary), max(salary) FROM emp GROUP BY dept;

SELECT dept, count(*) FROM emp GROUP BY dept HAVING count(*) > 1;

SELECT dept, count(*) c FROM emp GROUP BY 1 ORDER BY c DESC, dept;

SELECT count(*), count(DISTINCT dept), sum(DISTINCT salary), approx_count_distinct(dept) FROM emp;

SELECT dept, collect_list(name), sort_array(collect_set(salary)) FROM emp WHERE dept = 'eng' GROUP BY dept;

SELECT sort_array(collect_list(name)), sort_array(collect_set(dept)) FROM emp;

SELECT dept, max_by(name, salary), min_by(name, salary) FROM emp WHERE dept = 'eng' GROUP BY dept;

SELECT first(name), last(name) FROM (SELECT name FROM emp ORDER BY id);

SELECT dept, sum(salary) FILTER (WHERE id > 1), count(*) FILTER (WHERE salary > 75) FROM emp GROUP BY dept;

SELECT bool_and(salary > 50), bool_or(salary > 110), every(id > 0), some(id > 5), any(id > 6) FROM emp;

SELECT stddev(salary), stddev_pop(salary), variance(salary), var_pop(salary), corr(id, salary), covar_samp(id, salary) FROM emp;

SELECT percentile(salary, 0.5), percentile_approx(salary, 0.5), median(salary) FROM emp;

SELECT count(*) FROM emp WHERE false;

SELECT sum(salary), avg(salary), max(name) FROM emp WHERE false;

SELECT dept, region FROM emp, (SELECT 'north' AS region) GROUP BY dept, region;

-- ===== grouping sets / rollup / cube =====
SELECT month, region, sum(amount) FROM sales GROUP BY month, region WITH ROLLUP;

SELECT month, region, sum(amount) FROM sales GROUP BY ROLLUP (month, region);

SELECT month, region, sum(amount) FROM sales GROUP BY month, region WITH CUBE;

SELECT month, region, sum(amount) FROM sales GROUP BY CUBE (month, region);

SELECT month, region, sum(amount), grouping(month), grouping_id(month, region) FROM sales GROUP BY GROUPING SETS ((month, region), (month), ());

SELECT region, sum(amount) FROM sales GROUP BY region GROUPING SETS ((region), ());

-- ===== window functions =====
SELECT name, dept, salary, rank() OVER (PARTITION BY dept ORDER BY salary DESC) FROM emp;

SELECT name, dense_rank() OVER (ORDER BY salary DESC), row_number() OVER (ORDER BY id), percent_rank() OVER (ORDER BY id), cume_dist() OVER (ORDER BY id), ntile(3) OVER (ORDER BY id) FROM emp;

SELECT name, lag(name) OVER (ORDER BY id), lead(name, 2, 'none') OVER (ORDER BY id), lag(salary, 1, 0D) OVER (ORDER BY id) FROM emp;

SELECT name, sum(salary) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM emp;

SELECT name, sum(salary) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), avg(salary) OVER (PARTITION BY dept) FROM emp;

SELECT name, sum(salary) OVER (ORDER BY salary RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) FROM emp;

SELECT name, sum(salary) OVER (ORDER BY salary, id) FROM emp;

SELECT name, first_value(name) OVER (PARTITION BY dept ORDER BY id), last_value(name) OVER (PARTITION BY dept ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), nth_value(name, 2) OVER (ORDER BY id) FROM emp;

SELECT name, count(*) OVER w, max(salary) OVER w FROM emp WINDOW w AS (PARTITION BY dept);

SELECT * FROM (SELECT name, dept, row_number() OVER (PARTITION BY dept ORDER BY salary DESC, id) rn FROM emp) WHERE rn = 1;

-- ===== set operations =====
SELECT dept FROM emp UNION SELECT code FROM dept;

SELECT dept FROM emp UNION ALL SELECT code FROM dept;

SELECT dept FROM emp UNION DISTINCT SELECT code FROM dept;

SELECT dept FROM emp INTERSECT SELECT code FROM dept;

SELECT dept FROM emp INTERSECT ALL SELECT dept FROM emp WHERE id < 4;

SELECT dept FROM emp EXCEPT SELECT code FROM dept;

SELECT dept FROM emp MINUS SELECT code FROM dept;

SELECT dept FROM emp EXCEPT ALL SELECT dept FROM emp WHERE id = 1;

SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 1 ORDER BY x;

(SELECT 1 AS x) UNION (SELECT 2) ORDER BY x DESC LIMIT 1;

SELECT 1, 'a' UNION ALL SELECT 2.5, NULL;

-- ===== subqueries =====
SELECT name FROM emp WHERE EXISTS (SELECT 1 FROM dept WHERE code = emp.dept);

SELECT name FROM emp WHERE NOT EXISTS (SELECT 1 FROM dept WHERE code = emp.dept);

SELECT name FROM emp WHERE salary > (SELECT avg(salary) FROM emp);

SELECT name FROM emp WHERE dept IN (SELECT code FROM dept WHERE floor < 3);

SELECT name, salary - (SELECT min(salary) FROM emp e2 WHERE e2.dept = e1.dept) FROM emp e1;

-- ===== CTEs =====
WITH t AS (SELECT dept, sum(salary) s FROM emp GROUP BY dept) SELECT * FROM t WHERE s > 100;

WITH a AS (SELECT 1 AS x), b AS (SELECT x + 1 AS y FROM a) SELECT * FROM a, b;

WITH t(x, y) AS (SELECT 1, 2) SELECT x + y FROM t;

-- ===== PIVOT / UNPIVOT =====
SELECT * FROM (SELECT month, region, amount FROM sales) PIVOT (sum(amount) FOR region IN ('north', 'south', 'east'));

SELECT * FROM (SELECT month, region, amount FROM sales) PIVOT (sum(amount) AS total, count(amount) AS cnt FOR region IN ('north' AS n, 'south' AS s));

SELECT * FROM (SELECT product, month, amount FROM sales) PIVOT (max(amount) FOR (product, month) IN (('a', '2024-01') AS a1, ('b', '2024-02') AS b2));

SELECT * FROM (SELECT 1 AS id, 10 AS q1, 20 AS q2) UNPIVOT (val FOR quarter IN (q1, q2));

SELECT * FROM (SELECT 1 AS id, 10 AS q1, CAST(NULL AS INT) AS q2) UNPIVOT INCLUDE NULLS (val FOR quarter IN (q1 AS `first`, q2 AS `second`));

-- ===== TABLESAMPLE =====
-- @nondeterministic
SELECT count(*) FROM emp TABLESAMPLE (50 PERCENT);

SELECT count(*) FROM emp TABLESAMPLE (100 PERCENT);

SELECT count(*) FROM emp TABLESAMPLE (3 ROWS);

-- @nondeterministic
SELECT count(*) FROM emp TABLESAMPLE (BUCKET 1 OUT OF 2);

-- ===== misc expressions over tables =====
SELECT name, skills[0], attrs['level'], size(skills), size(attrs), array_contains(skills, 'go') FROM emp;

SELECT name, IF(salary > 90, 'high', 'low'), nvl(dept, 'none'), coalesce(salary, 0D), nullif(dept, 'eng') FROM emp;

SELECT upper(name), lower(name), length(name), substr(name, 2), substring(name, 1, 2), concat(name, '-', dept), concat_ws('/', name, dept) FROM emp;

SELECT name, year(hired), month(hired), dayofmonth(hired), date_format(hired, 'yyyy/MM/dd'), datediff(DATE '2024-01-01', hired), date_add(hired, 10) FROM emp;

SELECT name, struct(id, name).name, named_struct('i', id).i FROM emp;

SELECT name, CASE dept WHEN 'eng' THEN 1 ELSE 0 END * salary FROM emp;
