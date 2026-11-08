-- Queries that Apache Spark 3.5 rejects. spark_strict must reject all of them; the lenient spark dialect may accept some (LENIENT).

-- @setup
CREATE TABLE ins_probe (a INT) USING parquet;

SELECT [1, 2];

SELECT {'a': 1};

SELECT MAP {'k': 1};

SELECT '1'::INT;

SELECT transform(array(1), lambda x: x + 1);

SELECT list_transform([1], x -> x);

SELECT strftime(DATE '2024-01-01', '%Y');

SELECT COLUMNS(*) FROM VALUES (1, 2) AS t(a, b);

SELECT * EXCLUDE (a) FROM VALUES (1, 2) AS t(a, b);

SELECT * EXCEPT (a) FROM VALUES (1, 2) AS t(a, b);

SELECT a FROM VALUES (1), (2) AS t(a) QUALIFY row_number() OVER (ORDER BY a) = 1;

SELECT #1 FROM VALUES (1) AS t(a);

SELECT * FROM VALUES (1) AS x(a) ASOF JOIN VALUES (1) AS y(b) ON a >= b;

SELECT * FROM VALUES (1) AS x(a) POSITIONAL JOIN VALUES (2) AS y(b);

SELECT CAST(1 AS HUGEINT);

SELECT CAST('a5cf6c42-0c85-418f-af6c-3e4e5b1328f2' AS UUID);

SELECT TRY(1);

PRAGMA version;

SUMMARIZE SELECT 1 AS a;

CREATE MACRO m(x) AS x + 1;

CREATE TABLE ctas_probe AS SELECT 1 AS a;

SELECT add(1, 2);

SELECT cast_value(1, CAST(NULL AS STRING));

SELECT * REPLACE (a + 1 AS a) FROM VALUES (1) AS t(a);

SELECT [1, 2, 3][1:2];

SELECT 'abc'.upper();

SELECT 7 // 2;

SELECT 2 ** 3;

SELECT 'abc' ^@ 'a';

SELECT 'abc' ~ 'b';

SELECT 'abc' GLOB 'a*';

SELECT 'abc' SIMILAR TO 'a.c';

SELECT '{"a":1}'::JSON ->> 'a';

SELECT E'a\nb';

SELECT $$dollar$$;

SELECT CAST(1 AS INT[]);

SELECT CAST(NULL AS STRUCT(a INT));

SELECT CAST(NULL AS MAP(VARCHAR, INT));

SELECT CAST(1 AS UBIGINT);

SELECT CAST('{}' AS JSON);

SELECT CAST(TIME '10:00:00' AS TIME);

SELECT CAST(TIMESTAMP '2024-01-01' AS TIMESTAMPTZ);

SELECT list_value(1, 2);

SELECT regexp_full_match('a', 'a');

SELECT strptime('2024', '%Y');

SELECT epoch_ms(0);

SELECT arg_max(a, a) FROM VALUES (1) AS t(a);

SELECT string_agg(a, ',') FROM VALUES ('x') AS t(a);

SELECT list(a) FROM VALUES (1) AS t(a);

SELECT count_star();

SELECT unnest([1, 2]);

SELECT * FROM generate_series(1, 3);

SELECT * FROM read_csv('x.csv');

SELECT * FROM VALUES (1) AS t(a) USING SAMPLE 10%;

SELECT * FROM VALUES (1) AS t(a) LIMIT 10%;

PIVOT (SELECT 1 AS a, 'x' AS b) ON b USING sum(a);

UNPIVOT (SELECT 1 AS a, 2 AS b) ON a, b INTO NAME k VALUE v;

INSTALL json;

LOAD json;

ATTACH ':memory:' AS other;

COPY (SELECT 1) TO 'out.csv';

EXPORT DATABASE 'dir';

CHECKPOINT;

VACUUM;

BEGIN TRANSACTION;

CALL pragma_version();

PREPARE p AS SELECT 1;

CREATE SEQUENCE s;

CREATE TYPE mood AS ENUM ('a', 'b');

INSERT OR REPLACE INTO ins_probe VALUES (1);

INSERT INTO ins_probe VALUES (1) RETURNING a;

SELECT concat_ws_impl(',', 'a');

SELECT subscript(array(1), 0);
