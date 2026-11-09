-- spark
SELECT tag, count(*) AS n, count(DISTINCT user_id) AS users FROM (SELECT user_id, explode(tags) AS tag FROM events WHERE size(tags) > 1)
GROUP BY tag ORDER BY n DESC, tag LIMIT 50;
-- native
SELECT tag, count(*) AS n, count(DISTINCT user_id) AS users FROM (SELECT user_id, unnest(tags) AS tag FROM events WHERE len(tags) > 1)
GROUP BY tag ORDER BY n DESC, tag LIMIT 50;
