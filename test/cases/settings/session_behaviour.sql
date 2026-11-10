-- Session behaviour of settings (plan B2), run in order: a setting changed between two executions of one statement,
-- a view created under one value and queried under another, a cached table, and the dialect switch.

-- @setup
RESET;

SELECT size(CAST(NULL AS ARRAY<INT>)) AS n;

-- @setup
SET spark.sql.legacy.sizeOfNull=false;

SELECT size(CAST(NULL AS ARRAY<INT>)) AS n;

-- @setup
CREATE OR REPLACE TEMPORARY VIEW v_size AS SELECT size(CAST(NULL AS ARRAY<INT>)) AS n;

-- @setup
SET spark.sql.legacy.sizeOfNull=true;

SELECT n FROM v_size;

SELECT size(CAST(NULL AS ARRAY<INT>)) AS n;

-- @setup
CREATE OR REPLACE TEMPORARY VIEW v_json AS SELECT to_json(named_struct('a', 1, 'b', CAST(NULL AS INT))) AS j;

-- @setup
SET spark.sql.jsonGenerator.ignoreNullFields=false;

SELECT j FROM v_json;

SELECT to_json(named_struct('a', 1, 'b', CAST(NULL AS INT))) AS j;

-- @setup
RESET spark.sql.jsonGenerator.ignoreNullFields;

SELECT to_json(named_struct('a', 1, 'b', CAST(NULL AS INT))) AS j;

-- @setup
CREATE TABLE cached_t USING parquet AS SELECT map(1, 'a') AS m;

-- @setup
CACHE TABLE cached_t;

-- @setup
SET spark.sql.mapKeyDedupPolicy=LAST_WIN;

SELECT m FROM cached_t;

SELECT map(1, 'a', 1, 'b') AS m;

-- @setup
RESET spark.sql.mapKeyDedupPolicy;

SELECT map_concat(map(1, 'a'), CAST(NULL AS MAP<INT, STRING>)) AS m;

-- @setup
SET spark.sql.legacy.sizeOfNull=false;

SELECT size(CAST(NULL AS ARRAY<INT>)) AS n, size(array(1)) AS k;

-- @setup
RESET;

SELECT size(CAST(NULL AS ARRAY<INT>)) AS n;
