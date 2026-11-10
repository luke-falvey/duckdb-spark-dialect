-- session.timeZone with timestampType and the datetime parser (plan B4).
-- @matrix spark.sql.session.timeZone=UTC,America/Los_Angeles,Asia/Kolkata
-- @matrix spark.sql.timestampType=TIMESTAMP_LTZ,TIMESTAMP_NTZ

SELECT typeof(TIMESTAMP'2024-03-10 02:30:00'), TIMESTAMP'2024-03-10 02:30:00';

SELECT CAST('2024-03-10 10:30:00' AS TIMESTAMP);

SELECT CAST(TIMESTAMP'2024-03-10 10:30:00' AS DATE);

SELECT CAST(DATE'2024-03-10' AS TIMESTAMP);

SELECT TIMESTAMP'2024-03-10 10:30:00' = DATE'2024-03-10';

SELECT unix_timestamp(TIMESTAMP'2024-03-10 10:30:00');

SELECT to_timestamp('2024-03-10 10:30:00');

SELECT date_format(TIMESTAMP'2024-03-10 10:30:00', 'yyyy-MM-dd HH:mm:ss');

SELECT hour(TIMESTAMP'2024-03-10 10:30:00'), date_trunc('day', TIMESTAMP'2024-03-10 10:30:00');

SELECT current_timezone();
