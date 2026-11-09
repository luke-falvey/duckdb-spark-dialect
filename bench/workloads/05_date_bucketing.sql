-- spark
SELECT year(ts) AS y, month(ts) AS m, dayofweek(ts) AS dow, date_format(ts, 'yyyy-MM') AS ym, count(*) AS n, sum(amount) AS s
FROM events GROUP BY year(ts), month(ts), dayofweek(ts), date_format(ts, 'yyyy-MM') ORDER BY y, m, dow;
-- native
SELECT year(ts) AS y, month(ts) AS m, dayofweek(ts) + 1 AS dow, strftime(ts, '%Y-%m') AS ym, count(*) AS n, sum(amount) AS s
FROM events GROUP BY year(ts), month(ts), dayofweek(ts) + 1, strftime(ts, '%Y-%m') ORDER BY y, m, dow;
