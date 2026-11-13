-- Reproducers from a user project compared with Spark 3.5.0 (docs/plans/user-feedback-fixes.md, section 2).

-- 2.1 an aliased rewrite in a function argument (substring / left / date_add over coalesce / ifnull / nvl)
SELECT substring(coalesce(x, 'abc'), 1, 2), left(ifnull(x, 'abc'), 2), substr(coalesce(x, 'abc'), 2) FROM VALUES ('hello'), (NULL) t(x);

SELECT upper(coalesce(x, 'abc')), right(nvl(x, 'abc'), 2), lpad(coalesce(x, 'a'), 3, '*'), length(coalesce(x, 'abc')) FROM VALUES ('hello'), (NULL) t(x);

SELECT date_add(coalesce(d, DATE'2024-01-01'), 1), round(coalesce(n, 1.5), 1), nvl2(coalesce(x, 'a'), 1, 2), md5(coalesce(x, 'a')), sha2(nvl(x, 'a'), 256) FROM VALUES ('hello', DATE'2024-02-02', 2.25) t(x, d, n);

SELECT struct(coalesce(x, 'abc')), named_struct('a', coalesce(x, 'b')), array(coalesce(x, 'a')), map('k', coalesce(x, 'a')) FROM VALUES ('hello') t(x);

-- 2.2 window functions inside window definitions
SELECT x, rank() OVER (PARTITION BY g ORDER BY sum(x) OVER (PARTITION BY g)) FROM VALUES (1, 1), (2, 1), (5, 2) t(x, g) ORDER BY x;

SELECT x, row_number() OVER (PARTITION BY rank() OVER (ORDER BY x) ORDER BY x) FROM VALUES (1), (2), (2) t(x) ORDER BY x;

SELECT x, sum(x) OVER (ORDER BY lag(x) OVER (ORDER BY x) NULLS FIRST) FROM VALUES (1), (2), (4) t(x) ORDER BY x;

SELECT x, sum(x) OVER w FROM VALUES (1, 1), (2, 1) t(x, g) WINDOW w AS (PARTITION BY g ORDER BY rank() OVER (ORDER BY x)) ORDER BY x;

SELECT *, rank() OVER (ORDER BY count(x) OVER (PARTITION BY g) DESC, x) AS r FROM VALUES (1, 1), (2, 1), (5, 2) t(x, g) ORDER BY x;

SELECT t.x, rank() OVER (ORDER BY sum(t.x) OVER (PARTITION BY t.g), t.x) FROM VALUES (1, 1), (2, 1), (5, 2) t(x, g) WHERE x > 1 ORDER BY 1;

WITH c AS (SELECT x, g FROM VALUES (1, 1), (2, 1), (5, 2) t(x, g)) SELECT x, dense_rank() OVER (ORDER BY max(x) OVER (PARTITION BY g)) FROM c ORDER BY x;

SELECT x, rank() OVER (ORDER BY rank() OVER (ORDER BY row_number() OVER (ORDER BY x DESC))) FROM VALUES (1), (2), (3) t(x) ORDER BY x;

SELECT (SELECT max(y) FROM VALUES (1) u(y)), x, rank() OVER (ORDER BY sum(x) OVER ()) FROM VALUES (1), (2) t(x) ORDER BY x;

-- over a join with table-qualified columns (rld_application_base_ice, rld_rwa_rpt_data_prep)
SELECT aps.id, aps.cat, rank() OVER (PARTITION BY aps.id ORDER BY (row_number() OVER (ORDER BY aps.id, CASE WHEN aps.cat = 'Home' THEN '1' ELSE aps.cat END))) AS rank FROM VALUES (1, 'Home', 10), (1, 'Cards', 5), (2, 'Auto', 7) aps(id, cat, lim) INNER JOIN (SELECT a.id, max(a.lim) AS m FROM VALUES (1, 'Home', 10), (1, 'Cards', 5), (2, 'Auto', 7) a(id, cat, lim) GROUP BY a.id) AS apmax ON aps.id = apmax.id ORDER BY 1, 3;

SELECT s.cid, s.num FROM (SELECT ct.cid, cust.num, rank() OVER (PARTITION BY ct.cid ORDER BY cust.created DESC, row_number() OVER (ORDER BY cust.num DESC)) AS ranknum FROM VALUES (100, 'NZ012'), (101, 'NZ034') ct(cid, tick) INNER JOIN VALUES ('012', 1), ('012', 1), ('034', 2) cust(num, created) ON substring(ct.tick, 3, 10) = cust.num WHERE substring(ct.tick, 1, 2) = 'NZ') AS s WHERE s.ranknum = 1 ORDER BY 1;

SELECT *, rank() OVER (PARTITION BY a.k ORDER BY row_number() OVER (ORDER BY b.v DESC)) AS r FROM VALUES (1, 'x'), (2, 'y') a(k, s) JOIN VALUES (1, 10), (1, 20), (2, 30) b(k, v) ON a.k = b.k ORDER BY r, v;

SELECT b.*, a.s, rank() OVER (ORDER BY row_number() OVER (ORDER BY b.v)) AS r FROM VALUES (1, 'x'), (2, 'y') a(k, s) LEFT JOIN VALUES (1, 10), (2, 30) b(k, v) ON a.k = b.k ORDER BY b.v DESC;

SELECT a.k, b.v + 1, transform(array(1, 2), x -> x + a.k) AS t FROM VALUES (1), (2) a(k), VALUES (1, 10), (2, 30) b(k, v) WHERE a.k = b.k QUALIFY rank() OVER (ORDER BY sum(b.v) OVER (PARTITION BY a.k)) > 1 ORDER BY a.k;

-- 2.3 aggregates over strings: a string that is not a number is NULL
SELECT sum(x), avg(x), typeof(sum(x)) FROM VALUES ('1'), ('abc'), ('2.5') t(x);

SELECT stddev(x), variance(x), var_pop(x), stddev_pop(x) FROM VALUES ('1'), ('abc'), ('3') t(x);

SELECT g, sum(x), avg(x) FROM VALUES (1, '1'), (1, ''), (2, ' 4 ') t(g, x) GROUP BY g ORDER BY g;

SELECT sum(x) OVER (ORDER BY g), avg(x) OVER () FROM VALUES (1, '1'), (2, 'abc') t(g, x) ORDER BY g;

SELECT try_sum(x), try_avg(x), corr(x, y), covar_pop(x, y), skewness(x), kurtosis(x) FROM VALUES ('1', '2'), ('abc', '3'), ('3', '5') t(x, y);

SELECT sum(x) FILTER (WHERE x <> 'abc'), sum(DISTINCT x), avg(DISTINCT x) FROM VALUES ('1'), ('abc'), ('1') t(x);

SELECT var_pop(x), covar_samp(x, y), regr_avgx(y, x), regr_slope(y, x), regr_sxx(y, x), mean(x) FROM VALUES ('1', '2'), ('abc', '3'), ('3', '5') t(x, y);

SELECT sum(x) FROM VALUES ('abc'), ('') t(x);

SELECT sum(b) FROM VALUES (true) t(b);

SELECT try_sum(b) FROM VALUES (true) t(b);

SELECT var_pop(b) FROM VALUES (true) t(b);

-- 2.4 PIVOT values match by value
SELECT * FROM (SELECT CAST(k AS DECIMAL(10,2)) k, v FROM VALUES (1, 10), (2, 20) t(k, v)) PIVOT (sum(v) FOR k IN (1 AS a, 2 AS b));

SELECT * FROM (SELECT CAST(k AS DOUBLE) k, v FROM VALUES (1, 10), (2, 20) t(k, v)) PIVOT (sum(v) FOR k IN (1 AS a, 2 AS b));

SELECT * FROM (SELECT CAST(k AS DECIMAL(10,2)) k, v FROM VALUES (1, 10), (2, 20) t(k, v)) PIVOT (sum(v) FOR k IN (1.0 AS a, '2' AS b));

SELECT * FROM (SELECT CAST(k AS FLOAT) k, v FROM VALUES (1.5, 10), (2, 20) t(k, v)) PIVOT (sum(v) FOR k IN (1.5 AS a, 2 AS b));

SELECT * FROM (SELECT CAST(k AS DECIMAL(10,2)) k, g, v FROM VALUES (1, 'x', 10), (2, 'y', 20) t(k, g, v)) PIVOT (sum(v) FOR (k, g) IN ((1, 'x') AS a, (2, 'y') AS b));

SELECT * FROM (SELECT CAST(k AS DECIMAL(10,2)) k, v FROM VALUES (1, 10), (2, 20) t(k, v)) PIVOT (sum(v) AS s, max(v) AS m FOR k IN (1 AS a, 3 AS c));

-- 2.5 to_date / to_timestamp* with a format: a date or timestamp value ignores the format
SELECT to_date(TIMESTAMP'2024-03-05 10:11:12', 'dd/MM/yyyy'), to_date(DATE'2024-03-05', 'dd/MM/yyyy'), to_date('05/03/2024', 'dd/MM/yyyy');

SELECT to_timestamp(TIMESTAMP'2024-03-05 10:11:12', 'dd/MM/yyyy'), to_timestamp(DATE'2024-03-05', 'dd/MM/yyyy'), try_to_timestamp(TIMESTAMP'2024-03-05 10:11:12', 'dd');

SELECT to_timestamp_ltz(DATE'2024-03-05', 'dd'), to_date(NULL, 'yyyy'), to_date(x, 'dd/MM/yyyy') FROM VALUES ('05/03/2024'), ('bad') t(x);

-- 2.6 CHAR / VARCHAR without a length (Spark: DATATYPE_MISSING_SIZE; the lenient dialect accepts it)
SELECT CAST(1 AS VARCHAR);

SELECT CAST(1 AS CHAR);

SELECT CAST(array(1) AS ARRAY<VARCHAR>);

SELECT CAST(1 AS VARCHAR(5)), CAST(1 AS CHAR(3)), CAST(1 AS STRING);
