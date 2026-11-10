-- Session time zone: every function that turns an instant into a wall-clock rendering or back.

SELECT current_timezone();

SELECT CAST(timestamp_seconds(0) AS STRING), CAST(timestamp_millis(1700000000123) AS STRING), CAST(timestamp_micros(1700000000123456) AS STRING);

SELECT from_unixtime(0), from_unixtime(1700000000), from_unixtime(1700000000, 'yyyy-MM-dd HH:mm:ss z'), from_unixtime(1700000000, 'yyyy/MM/dd HH:mm XXX');

SELECT unix_timestamp(TIMESTAMP '2024-03-10 12:00:00'), unix_timestamp('2024-03-10 12:00:00'), unix_timestamp('2024-03-10', 'yyyy-MM-dd'), to_unix_timestamp(TIMESTAMP '2024-07-01 00:00:00');

SELECT CAST(TIMESTAMP '2024-07-01 00:00:00 UTC' AS STRING), CAST(TIMESTAMP '2024-07-01 12:30:00+05:30' AS STRING), CAST(TIMESTAMP '2024-07-01 00:00:00' AS STRING);

SELECT CAST(CAST('2024-07-01 12:00:00+00:00' AS TIMESTAMP) AS STRING), CAST(CAST('2024-07-01T12:00:00Z' AS TIMESTAMP) AS STRING), CAST(CAST('2024-07-01 12:00:00 America/New_York' AS TIMESTAMP) AS STRING);

SELECT CAST(TIMESTAMP '2024-03-10 12:00:00' AS BIGINT), CAST(TIMESTAMP '2024-03-10 12:00:00' AS DOUBLE), CAST(CAST(1710072000 AS TIMESTAMP) AS STRING), CAST(CAST(1710072000.5D AS TIMESTAMP) AS STRING);

SELECT CAST(TIMESTAMP '2024-03-10 23:30:00' AS DATE), CAST(DATE '2024-03-10' AS TIMESTAMP), CAST(CAST(DATE '2024-03-10' AS TIMESTAMP) AS STRING);

SELECT hour(timestamp_seconds(1700000000)), minute(timestamp_seconds(1700000000)), day(timestamp_seconds(1700000000)), dayofweek(timestamp_seconds(1700000000)), dayofyear(timestamp_seconds(1700000000)), weekofyear(timestamp_seconds(1700000000)), month(timestamp_seconds(1700000000)), year(timestamp_seconds(1700000000));

SELECT date_trunc('DAY', timestamp_seconds(1700000000)), date_trunc('HOUR', timestamp_seconds(1700000000)), date_trunc('MONTH', timestamp_seconds(1700000000)), trunc(timestamp_seconds(1700000000), 'MM'), CAST(date_trunc('WEEK', timestamp_seconds(1700000000)) AS STRING);

SELECT from_utc_timestamp(TIMESTAMP '2024-07-01 00:00:00', 'America/Los_Angeles'), to_utc_timestamp(TIMESTAMP '2024-07-01 00:00:00', 'Asia/Kolkata'), CAST(from_utc_timestamp(timestamp_seconds(0), 'Australia/Lord_Howe') AS STRING);

SELECT date_format(timestamp_seconds(1700000000), 'yyyy-MM-dd HH:mm:ss'), date_format(timestamp_seconds(1700000000), 'yyyy-MM-dd HH:mm:ss z'), date_format(timestamp_seconds(1700000000), 'HH:mm:ss Z'), date_format(timestamp_seconds(1700000000), 'HH:mm XXX'), date_format(timestamp_seconds(1700000000), 'HH:mm xx'), date_format(timestamp_seconds(1700000000), 'VV'), date_format(timestamp_seconds(1700000000), 'zzzz'), date_format(timestamp_seconds(1700000000), 'O');

SELECT to_timestamp('2024-03-10 12:00:00'), to_timestamp('2024-03-10 12:00:00 +0530', 'yyyy-MM-dd HH:mm:ss Z'), to_timestamp('2024-03-10', 'yyyy-MM-dd'), CAST(to_timestamp(1710072000) AS STRING), CAST(to_timestamp('2024-03-10 12:00:00') AS STRING);

SELECT make_timestamp(2024, 3, 10, 12, 0, 0), make_timestamp(2024, 3, 10, 12, 0, 0, 'UTC'), make_timestamp(2024, 3, 10, 12, 0, 0, 'Asia/Tokyo'), CAST(make_timestamp(2024, 7, 1, 0, 0, 0, 'UTC') AS STRING);

-- DST gap and overlap in America/Los_Angeles (2024-03-10 02:30 does not exist; 2024-11-03 01:30 happens twice)
SELECT CAST(TIMESTAMP '2024-03-10 02:30:00' AS STRING), CAST(TIMESTAMP '2024-11-03 01:30:00' AS STRING), unix_timestamp(TIMESTAMP '2024-03-10 02:30:00'), unix_timestamp(TIMESTAMP '2024-11-03 01:30:00');

SELECT CAST(timestamp_seconds(1710054600) AS STRING), CAST(timestamp_seconds(1710054600 + 3600) AS STRING), CAST(timestamp_seconds(1730619000) AS STRING), CAST(timestamp_seconds(1730619000 + 3600) AS STRING);

SELECT ts, CAST(ts AS STRING), CAST(ts AS DATE), hour(ts), unix_timestamp(ts), date_format(ts, 'yyyy-MM-dd HH:mm:ss') FROM VALUES (timestamp_seconds(0)), (timestamp_seconds(1700000000)), (TIMESTAMP '2024-03-10 12:00:00'), (CAST(NULL AS TIMESTAMP)) AS t(ts) ORDER BY unix_timestamp(ts);

SELECT window.start, window.end, c FROM (SELECT window(ts, '1 day') AS window, count(*) AS c FROM VALUES (timestamp_seconds(1700000000)), (timestamp_seconds(1700050000)), (timestamp_seconds(1700100000)) AS t(ts) GROUP BY window(ts, '1 day')) ORDER BY 1;

SELECT current_timestamp() IS NOT NULL, typeof(current_timestamp()), now() IS NOT NULL, CAST(current_date() AS STRING) = date_format(current_timestamp(), 'yyyy-MM-dd');

SELECT date_add(timestamp_seconds(1700000000), 1) , add_months(timestamp_seconds(1700000000), 1), months_between(timestamp_seconds(1700000000), timestamp_seconds(1690000000)), datediff(timestamp_seconds(1700000000), timestamp_seconds(1690000000)), last_day(timestamp_seconds(1700000000)), next_day(timestamp_seconds(1700000000), 'Mon');

SELECT extract(HOUR FROM timestamp_seconds(1700000000)), date_part('DAY', timestamp_seconds(1700000000)), CAST(timestamp_seconds(1700000000) AS DATE), CAST(timestamp_seconds(1700000000) AS STRING);

-- negative control: nothing here depends on the zone
SELECT DATE '2024-03-10' + 1, CAST(DATE '2024-03-10' AS STRING), TIMESTAMP_NTZ '2024-03-10 12:00:00' + INTERVAL 1 DAY;
