-- Values matched by value, not by their text (docs/plans/user-feedback-fixes.md 3.4)

SELECT map(1.0, 'a')[1], map(1, 'a')[1.0], element_at(map(1L, 'a'), 1), element_at(map(CAST(1 AS TINYINT), 'a'), 1);

SELECT map(CAST(1.5 AS DOUBLE), 'a')[1.5], map(CAST(1.50 AS DECIMAL(5,2)), 'a')[1.5];

SELECT array_contains(array(1.0, 2.0), 1), array_contains(array(1, 2), 1.0), array_contains(array(1L), 1);

SELECT array_position(array(1.0, 2.0), 2), array_position(array(1, 2), 2.0), array_remove(array(1.0, 2.0), 1);

SELECT find_in_set('1', '1.0,1'), find_in_set('a', 'b,a');

SELECT str_to_map('1:a,2:b')['1'], str_to_map('1.0:a')['1.0'];

SELECT 1 IN (1.0, 2.0), 1.0 IN (1, 2), CAST(1 AS DECIMAL(5,2)) IN (1, 2), 1 IN (CAST(1 AS DOUBLE));

SELECT CAST(1.50 AS DECIMAL(5,2)) = 1.5, CAST(1 AS DOUBLE) = CAST(1 AS DECIMAL(10,0)), CAST(1 AS FLOAT) = 1;

SELECT * FROM (SELECT CAST(k AS DECIMAL(10,2)) k, v FROM VALUES (1, 10), (2, 20) t(k, v)) PIVOT (sum(v) FOR k IN (1 AS a, 2 AS b));

SELECT * FROM (SELECT CAST(k AS BIGINT) k, v FROM VALUES (1, 10), (2, 20) t(k, v)) PIVOT (sum(v) FOR k IN (1.0 AS a, '2' AS b));

SELECT * FROM (SELECT CAST(k AS STRING) k, v FROM VALUES ('1', 10), ('2', 20) t(k, v)) PIVOT (sum(v) FOR k IN (1 AS a, 2 AS b));

SELECT * FROM (SELECT k, v FROM VALUES (DATE'2024-01-01', 10) t(k, v)) PIVOT (sum(v) FOR k IN ('2024-01-01' AS a, DATE'2024-01-02' AS b));

SELECT * FROM (SELECT k, v FROM VALUES (true, 10), (false, 20) t(k, v)) PIVOT (sum(v) FOR k IN (true AS t, false AS f));

SELECT l.k, r.k FROM VALUES (CAST(1 AS DECIMAL(5,2))) l(k) JOIN VALUES (1) r(k) ON l.k = r.k;

SELECT k FROM VALUES (CAST(1 AS DECIMAL(5,2))) l(k) JOIN VALUES (CAST(1 AS DOUBLE)) r(k) USING (k);

SELECT count(DISTINCT x) FROM VALUES (CAST(1 AS DECIMAL(5,2))), (CAST(1.00 AS DECIMAL(5,2))) t(x);

SELECT x, count(*) FROM VALUES (1.0D), (1.00D), (CAST(1 AS DOUBLE)) t(x) GROUP BY x;

-- a map key is cast to the map's key type (a string that is not a number is NULL); an empty map takes only a NULL key
SELECT map(1, 2)['b'], map(1, 2)['1'], map('a', 1)['a'];

SELECT map()['b'];

SELECT map()[NULL];

-- NULL has no fields or elements
SELECT NULL[0];
