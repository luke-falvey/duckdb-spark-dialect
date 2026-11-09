-- spark
SELECT date_trunc('day', ts) AS day, country, count(*) AS events, count(DISTINCT user_id) AS users,
       sum(amount) AS revenue, avg(amount) AS avg_amount, max(amount) AS max_amount
FROM events WHERE ts >= TIMESTAMP '2024-02-01 00:00:00' AND status IN ('ok', 'refund')
GROUP BY date_trunc('day', ts), country ORDER BY day, country;
