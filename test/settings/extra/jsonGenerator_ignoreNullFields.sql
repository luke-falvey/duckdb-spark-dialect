-- to_json keeps or drops NULL struct fields; literals, columns, nested structs, arrays of structs, maps, options

SELECT to_json(named_struct('a', 1, 'b', CAST(NULL AS INT)));

SELECT to_json(named_struct('a', CAST(NULL AS STRING), 'b', CAST(NULL AS INT)));

SELECT to_json(struct(1, CAST(NULL AS STRING)));

SELECT to_json(s) FROM VALUES (named_struct('a', 1, 'b', 'x')), (named_struct('a', CAST(NULL AS INT), 'b', 'y')), (named_struct('a', 3, 'b', CAST(NULL AS STRING))) AS t(s);

SELECT to_json(named_struct('a', named_struct('x', CAST(NULL AS INT), 'y', 2), 'b', 1));

SELECT to_json(array(named_struct('a', CAST(NULL AS INT), 'b', 1)));

SELECT to_json(map('k', named_struct('a', CAST(NULL AS INT))));

SELECT to_json(array(1, NULL, 3));

SELECT to_json(map('k', CAST(NULL AS INT)));

SELECT to_json(named_struct('d', DATE '2020-01-02', 'n', CAST(NULL AS DATE)), map('dateFormat', 'dd/MM/yyyy'));

SELECT to_json(CAST(NULL AS STRUCT<a: INT>));

SELECT to_json(named_struct('a', 1, 'b', 2.5, 'c', true));
