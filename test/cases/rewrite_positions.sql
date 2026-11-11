-- A rewritten expression must behave like the bare expression wherever it is used (docs/plans/user-feedback-fixes.md 3.2):
-- as a function / macro argument, a struct field, a GROUP BY / ORDER BY key, an IN value, a PIVOT value.

SELECT substring(coalesce(x, 'z'), 1, 1), concat_ws('-', coalesce(x, 'z'), nvl(x, 'y')) FROM VALUES ('ab'), (NULL) t(x);

SELECT struct(coalesce(x, 'z'), ifnull(x, 'y')), struct(coalesce(x, 'z') AS c) FROM VALUES ('ab') t(x);

SELECT coalesce(x, 'z'), count(*) FROM VALUES ('ab'), (NULL), (NULL) t(x) GROUP BY coalesce(x, 'z') ORDER BY 1;

SELECT x FROM VALUES ('b'), (NULL), ('a') t(x) ORDER BY coalesce(x, 'z');

SELECT coalesce(x, 'z') IN ('z', 'ab'), 'z' IN (coalesce(x, 'z')) FROM VALUES ('ab'), (NULL) t(x);

SELECT * FROM (SELECT coalesce(k, 0) k, v FROM VALUES (1, 10), (NULL, 20) t(k, v)) PIVOT (sum(v) FOR k IN (0 AS z, 1 AS o));

SELECT transform(array(coalesce(x, 'z')), e -> upper(e)), filter(array(nvl(x, 'z')), e -> e <> '') FROM VALUES ('ab') t(x);

SELECT date_format(coalesce(d, DATE'2024-01-01'), 'yyyy'), datediff(coalesce(d, DATE'2024-01-01'), DATE'2023-12-31') FROM VALUES (CAST(NULL AS DATE)) t(d);

SELECT element_at(map('a', 1), coalesce(x, 'a')), array_contains(array('a'), nvl(x, 'a')) FROM VALUES (CAST(NULL AS STRING)) t(x);

SELECT CAST(coalesce(x, '1') AS INT) + 1, coalesce(x, '1') || 'z' FROM VALUES (CAST(NULL AS STRING)) t(x);

SELECT nvl2(x, 'a', 'b'), nullif(coalesce(x, 'a'), 'a'), if(coalesce(x, 'a') = 'a', 1, 2) FROM VALUES (CAST(NULL AS STRING)) t(x);

SELECT x, rank() OVER (ORDER BY coalesce(x, 0)), sum(coalesce(x, 0)) OVER () FROM VALUES (1), (NULL) t(x) ORDER BY x NULLS FIRST;
