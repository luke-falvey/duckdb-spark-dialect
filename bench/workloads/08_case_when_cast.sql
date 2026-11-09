-- spark
SELECT CASE WHEN amount < 10 THEN 'small' WHEN amount < 100 THEN 'medium' ELSE 'large' END AS bucket,
       CAST(amount AS INT) % 7 AS k, count(*) AS n, sum(CAST(amount AS DOUBLE)) AS s, avg(coalesce(retries, 0)) AS r
FROM events GROUP BY 1, 2 ORDER BY 1, 2;
