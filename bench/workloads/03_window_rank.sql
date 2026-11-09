-- spark
SELECT user_id, ts, amount, row_number() OVER (PARTITION BY user_id ORDER BY ts) AS rn,
       sum(amount) OVER (PARTITION BY user_id ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rolling,
       lag(amount, 1) OVER (PARTITION BY user_id ORDER BY ts) AS prev
FROM events WHERE user_id % 50 = 0 ORDER BY user_id, ts LIMIT 1000;
