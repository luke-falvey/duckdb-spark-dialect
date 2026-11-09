-- spark
SELECT count(*) AS n, sum(try_cast(code AS INT)) AS s, sum(CASE WHEN try_cast(code AS INT) IS NULL THEN 1 ELSE 0 END) AS bad FROM events;
-- native
SELECT count(*) AS n, sum(try_cast(code AS INT)) AS s, sum(CASE WHEN try_cast(code AS INT) IS NULL THEN 1 ELSE 0 END) AS bad FROM events;
