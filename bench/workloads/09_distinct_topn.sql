-- spark
SELECT user_id, count(*) AS n, collect_set(country) AS countries FROM events GROUP BY user_id HAVING count(*) > 20 ORDER BY n DESC, user_id LIMIT 100;
-- native
SELECT user_id, count(*) AS n, list(DISTINCT country) AS countries FROM events GROUP BY user_id HAVING count(*) > 20 ORDER BY n DESC, user_id LIMIT 100;
