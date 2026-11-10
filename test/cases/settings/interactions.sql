-- Interactions between settings whose documentation refers to each other (plan B4).
-- @matrix spark.sql.ansi.enabled=false,true
-- @matrix spark.sql.legacy.sizeOfNull=true,false
-- @matrix spark.sql.mapKeyDedupPolicy=EXCEPTION,LAST_WIN
-- @matrix spark.sql.groupByOrdinal=true,false
-- @matrix spark.sql.groupByAliases=true,false
-- @matrix spark.sql.orderByOrdinal=true,false

SELECT size(CAST(NULL AS ARRAY<INT>));

SELECT size(a), cardinality(a) FROM VALUES (array(1, 2)), (CAST(NULL AS ARRAY<INT>)) AS t(a);

SELECT map(1, 'a', 1, 'b');

SELECT map_from_arrays(array(1, 1), array('a', 'b'));

SELECT transform_keys(map(1, 'a', 2, 'b'), (k, v) -> 1);

SELECT CAST('abc' AS INT);

SELECT 2147483647 + 1;

SELECT element_at(array(1, 2), 5);

SELECT array(1, 2)[5];

SELECT x, count(*) FROM VALUES (1), (1), (2) AS t(x) GROUP BY 1 ORDER BY 1;

SELECT x AS y, count(*) FROM VALUES (1), (1), (2) AS t(x) GROUP BY y ORDER BY 1;

SELECT x, y FROM VALUES (2, 'a'), (1, 'b') AS t(x, y) ORDER BY 1;

SELECT x, count(*) FROM VALUES (1), (2) AS t(x) GROUP BY 1 ORDER BY 2 DESC, 1;

SELECT size(map(1, 'a', 1, 'b'));
