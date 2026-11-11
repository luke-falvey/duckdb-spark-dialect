-- Analytic constructs inside each other (docs/plans/user-feedback-fixes.md 3.6)

SELECT x, rank() OVER (ORDER BY sum(x) OVER ()) FROM VALUES (1), (2) t(x) ORDER BY x;

SELECT sum(rank() OVER (ORDER BY x)) FROM VALUES (1), (2) t(x);

SELECT g, rank() OVER (ORDER BY sum(x)) FROM VALUES (1, 1), (2, 1), (5, 2) t(x, g) GROUP BY g ORDER BY g;

SELECT g, rank() OVER (ORDER BY sum(x) DESC), sum(sum(x)) OVER () FROM VALUES (1, 1), (2, 1), (5, 2) t(x, g) GROUP BY g ORDER BY g;

SELECT x FROM VALUES (1), (2) t(x) WHERE rank() OVER (ORDER BY x) = 1;

SELECT g FROM VALUES (1, 1), (2, 2) t(x, g) GROUP BY g HAVING rank() OVER (ORDER BY g) = 1;

SELECT transform(array(1, 2), e -> e + sum(x) OVER ()) FROM VALUES (1) t(x);

SELECT x, CASE WHEN rank() OVER (ORDER BY x) = 1 THEN 'first' ELSE 'other' END, coalesce(lag(x) OVER (ORDER BY x), 0) FROM VALUES (1), (2) t(x) ORDER BY x;

SELECT id, s, rank() OVER (PARTITION BY id ORDER BY s) FROM VALUES (1, array('b', 'a')) t(id, arr) LATERAL VIEW explode(arr) e AS s ORDER BY s;

SELECT explode(array(rank() OVER (ORDER BY x))) FROM VALUES (1) t(x);

SELECT x, rank() OVER (ORDER BY (SELECT max(y) FROM VALUES (1) u(y))) FROM VALUES (1), (2) t(x) ORDER BY x;

SELECT x, avg(x) OVER (PARTITION BY x % 2 ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM VALUES (1), (2), (3), (4) t(x) ORDER BY x;
