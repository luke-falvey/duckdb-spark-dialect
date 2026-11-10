-- @config spark.sql.ansi.enabled=true
-- ANSI mode's implicit casts (AnsiTypeCoercion): a string takes the other operand's type

SELECT typeof(CAST(1 AS FLOAT) + '1'), typeof(CAST(1 AS TINYINT) + '1'), typeof(1 - '1'), typeof('3' / 2), typeof(5 % '2'), 1 + '2';

SELECT 1 + 'abc';

SELECT '1' + '1';

SELECT typeof(CASE WHEN true THEN CAST(1 AS TINYINT) ELSE 'abc' END), CASE WHEN true THEN CAST(1 AS TINYINT) ELSE 'abc' END;

SELECT typeof(coalesce(CAST(1 AS FLOAT), '1')), typeof(coalesce(CAST(1.5 AS DECIMAL(5,2)), '1')), typeof(coalesce(true, 'true')), typeof(coalesce(DATE'2024-01-01', '2024-01-02')), typeof(array(1, '2'));

SELECT typeof(CAST(1 AS TINYINT) + CAST(1.5 AS FLOAT)), typeof(CAST(1 AS INT) % CAST(1.5 AS FLOAT)), typeof(CAST(1 AS FLOAT) * CAST(2 AS FLOAT)), typeof(array(CAST(1 AS TINYINT), CAST(1.5 AS FLOAT)));

SELECT TRY_CAST(1 AS BINARY);
