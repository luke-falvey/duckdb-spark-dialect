-- Spark literal syntax, type names and casts (non-ANSI mode, Spark 3.5 default).

-- ===== string literals =====
SELECT 'single', "double", 'it''s', "say ""hi""", 'it\'s', "dq \" inside";

SELECT 'tab\there', 'nl\nhere', 'back\\slash', 'unicode A', 'q\%', 'a\zb';

SELECT 'con' 'cat' "enated";

SELECT r'raw\nstring', R"raw\tdq";

SELECT length('\n'), length(r'\n'), '\'' = "'";

-- ===== numeric literals =====
SELECT 10L, 10S, 10Y, 1.5BD, 2.5D, 3.5F, 1E3, 1.5e-2, -1e2D, .5, 5.;

SELECT typeof(10L), typeof(10S), typeof(10Y), typeof(1.5BD), typeof(2.5D), typeof(3.5F), typeof(1E3), typeof(10), typeof(1.5);

SELECT typeof(2147483648), typeof(9223372036854775808), 9223372036854775808;

SELECT 123456789012345678901234567890.5;

-- ===== other literals =====
SELECT X'1F2A', true, false, NULL, typeof(NULL);

SELECT DATE '2024-02-29', DATE'2024-1-5', TIMESTAMP '2024-02-29 13:45:10.123', TIMESTAMP '2024-01-01';

SELECT TIMESTAMP_NTZ '2024-01-01 10:00:00', TIMESTAMP_LTZ '2024-01-01 10:00:00';

SELECT INTERVAL '1' DAY, INTERVAL 2 HOURS, INTERVAL '1 2:03:04' DAY TO SECOND, INTERVAL 90 MINUTES;

SELECT DATE '2024-01-31' + INTERVAL '1' DAY, TIMESTAMP '2024-01-01 00:00:00' + INTERVAL 36 HOURS;

SELECT TIMESTAMP '2024-01-01 00:00:00' - INTERVAL '1 01:00:00' DAY TO SECOND;

SELECT DATE '2024-01-31' + INTERVAL 1 MONTH, DATE '2024-03-31' - INTERVAL '1' MONTH;

SELECT ARRAY(1, 2, 3), ARRAY(), ARRAY('a', NULL), ARRAY(1, 2.5), MAP('a', 1, 'b', 2), MAP(), STRUCT(1, 'a'), STRUCT(1 AS x);

SELECT NAMED_STRUCT('a', 1, 'b', ARRAY(1)), ARRAY(STRUCT(1 AS a), STRUCT(2 AS a)), MAP(1, ARRAY('x'));

-- ===== CAST to every type name =====
SELECT CAST('12' AS INT), CAST('12' AS INTEGER), CAST('12' AS BIGINT), CAST('12' AS LONG), CAST('12' AS SMALLINT), CAST('12' AS SHORT), CAST('12' AS TINYINT), CAST('12' AS BYTE);

SELECT CAST('1.5' AS DOUBLE), CAST('1.5' AS FLOAT), CAST('1.5' AS REAL), CAST('1.567' AS DECIMAL(5,2)), CAST('1.5' AS DEC(4,1)), CAST('1.5' AS NUMERIC(4,1)), CAST('7' AS DECIMAL);

SELECT CAST(12 AS STRING), CAST(12 AS VARCHAR(5)), CAST(12 AS CHAR(5)), CAST(1.5D AS STRING), CAST(true AS STRING), CAST(DATE '2024-01-02' AS STRING);

SELECT CAST('true' AS BOOLEAN), CAST('false' AS BOOLEAN), CAST('yes' AS BOOLEAN), CAST('0' AS BOOLEAN), CAST('abc' AS BOOLEAN), CAST(0 AS BOOLEAN), CAST(5 AS BOOLEAN);

SELECT CAST('2024-01-02' AS DATE), CAST('2024-01-02 03:04:05' AS TIMESTAMP), CAST('2024-01-02 03:04:05' AS DATE), CAST(TIMESTAMP '2024-01-02 03:04:05' AS DATE);

SELECT CAST('abc' AS BINARY), CAST(X'616263' AS STRING);

SELECT CAST(NULL AS INT), CAST(NULL AS ARRAY<INT>), CAST(NULL AS MAP<STRING, INT>), CAST(NULL AS STRUCT<a: INT, b: STRING>);

SELECT CAST(ARRAY(1, 2) AS ARRAY<STRING>), CAST(ARRAY('1', 'x') AS ARRAY<INT>), CAST(MAP('a', 1) AS MAP<STRING, BIGINT>);

SELECT CAST(STRUCT(1, 'a') AS STRUCT<x: BIGINT, y: STRING>), CAST(ARRAY(ARRAY(1)) AS ARRAY<ARRAY<DOUBLE>>);

SELECT typeof(CAST(NULL AS ARRAY<MAP<STRING, ARRAY<INT>>>)), typeof(CAST(NULL AS STRUCT<a: STRUCT<b: INT>>));

-- ===== non-ANSI cast behaviour: invalid input becomes NULL =====
SELECT CAST('abc' AS INT), CAST('1.9' AS INT), CAST('-1.9' AS INT), CAST(' 42 ' AS INT), CAST('' AS INT);

SELECT CAST(300 AS TINYINT), CAST(-129 AS TINYINT), CAST(70000 AS SMALLINT), CAST(3000000000 AS INT), CAST(1e20D AS BIGINT);

SELECT CAST(1.9D AS INT), CAST(-1.9D AS INT), CAST(2.5 AS INT), CAST(CAST('NaN' AS DOUBLE) AS INT);

SELECT CAST('2024-13-45' AS DATE), CAST('not a date' AS TIMESTAMP), CAST('2024-02-30' AS DATE);

SELECT CAST(12345.678 AS DECIMAL(4,1)), CAST('1e3' AS DECIMAL(10,2)), CAST(1.255 AS DECIMAL(4,2)), CAST(-1.255 AS DECIMAL(4,2));

SELECT CAST(1.5D AS STRING), CAST(1.0D AS STRING), CAST(1e20D AS STRING), CAST(1e-5D AS STRING), CAST(0.1F AS STRING), CAST(100.0F AS DOUBLE);

SELECT CAST(TIMESTAMP '2024-01-02 03:04:05.5' AS STRING), CAST(TIMESTAMP '2024-01-02 03:04:05' AS STRING), CAST(CAST(1.50 AS DECIMAL(5,2)) AS STRING);

SELECT CAST(ARRAY(1, NULL) AS STRING), CAST(MAP('a', 1) AS STRING), CAST(STRUCT(1, 'x') AS STRING);

SELECT CAST(DATE '2024-01-02' AS TIMESTAMP), CAST(1704153600 AS TIMESTAMP), CAST(TIMESTAMP '2024-01-02 00:00:00' AS LONG), CAST(TIMESTAMP '1970-01-01 00:00:01.5' AS DOUBLE);

-- ===== shorthand cast functions =====
SELECT int('5'), bigint('5'), smallint('5'), tinyint('5'), double('1.5'), float('1.5'), decimal('1.5'), string(5), boolean('true'), date('2024-01-01'), timestamp('2024-01-01 01:02:03'), binary('ab');

-- ===== try_cast =====
SELECT try_cast('5' AS INT), try_cast('x' AS INT), try_cast('2024-01-01' AS DATE);

-- ===== implicit coercion =====
SELECT 1 + 1.5D, typeof(1 + 1.5D), typeof(1 + 1L), typeof(1 + 1.5), typeof(CAST(1 AS TINYINT) + 1Y);

SELECT typeof(1 / 2), typeof(1L / 2L), typeof(1.5 / 2), typeof(1 div 2), typeof(5 % 3), typeof(1.5D % 1);

SELECT typeof('1' + 1), typeof(1 || 2), typeof(DATE '2024-01-01' + 1), typeof(ARRAY(1, 2L)), typeof(COALESCE(1, 2.5));

SELECT CASE WHEN true THEN 1 ELSE 2L END, typeof(CASE WHEN true THEN 1 ELSE 2L END), typeof(IF(true, 1, 'a'));

SELECT typeof(sum(1)), typeof(avg(1)), typeof(sum(1.5)), typeof(avg(1.5)), typeof(sum(1.5D)), typeof(count(1));

SELECT typeof(MAP('a', 1)), typeof(STRUCT(1 AS a, 'b' AS b)), typeof(ARRAY(DATE '2024-01-01')), typeof(X'00');
