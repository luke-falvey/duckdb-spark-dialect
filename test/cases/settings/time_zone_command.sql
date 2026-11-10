-- SET TIME ZONE and the session time zone setting: forms, echo, validation (plan B2).

SET TIME ZONE 'America/Los_Angeles';

SET spark.sql.session.timeZone;

SELECT current_timezone(), CAST(timestamp_seconds(0) AS STRING);

SET TIME ZONE 'Asia/Kolkata';

SELECT current_timezone(), CAST(timestamp_seconds(0) AS STRING);

SET TIME ZONE '+05:30';

SET spark.sql.session.timeZone;

SELECT current_timezone(), CAST(timestamp_seconds(0) AS STRING);

SET TIME ZONE INTERVAL 8 HOUR;

SET spark.sql.session.timeZone;

SELECT current_timezone(), CAST(timestamp_seconds(0) AS STRING);

SET TIME ZONE INTERVAL -3 HOUR;

SELECT current_timezone(), CAST(timestamp_seconds(0) AS STRING);

SET TIME ZONE INTERVAL '15:40:32' HOUR TO SECOND;

SET spark.sql.session.timeZone;

SET TIME ZONE INTERVAL '-02:30' HOUR TO MINUTE;

SELECT current_timezone(), CAST(timestamp_seconds(0) AS STRING);

SET TIME ZONE INTERVAL 19 HOUR;

SET TIME ZONE 'Not/A_Zone';

SET spark.sql.session.timeZone=Invalid;

SET spark.sql.session.timeZone=GMT+8;

SELECT current_timezone(), CAST(timestamp_seconds(0) AS STRING);

SET spark.sql.session.timeZone=UTC+05:30;

SELECT current_timezone(), CAST(timestamp_seconds(0) AS STRING);

SET spark.sql.session.timeZone=PST;

SELECT current_timezone(), CAST(timestamp_seconds(0) AS STRING);

SET spark.sql.session.timeZone=Z;

SELECT current_timezone(), CAST(timestamp_seconds(0) AS STRING);

SET TIME ZONE LOCAL;

SELECT current_timezone() IS NOT NULL;

RESET spark.sql.session.timeZone;

SELECT current_timezone(), CAST(timestamp_seconds(0) AS STRING);
