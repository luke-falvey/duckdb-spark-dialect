-- spark
SELECT d.region, e.country, count(*) AS n, sum(e.amount * (1 - d.discount)) AS net
FROM events e JOIN dim d ON e.country = d.country WHERE e.status = 'ok' GROUP BY d.region, e.country ORDER BY d.region, e.country;
