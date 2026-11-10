SELECT map(1, 'a', 2, 'b');

SELECT map(1, 'a', 1, 'b');

SELECT map(1, 'a', 2, 'b', 1, 'c');

SELECT map('x', 1, 'x', 2, 'y', 3);

SELECT map(1, 'a', CAST(NULL AS INT), 'b');

SELECT map_from_arrays(array(1, 2), array('a', 'b'));

SELECT map_from_arrays(array(1, 1), array('a', 'b'));

SELECT map_from_arrays(array(1, 2, 1), array('a', 'b', 'c'));

SELECT map_from_arrays(array(1, NULL), array('a', 'b'));

SELECT map_from_entries(array(struct(1, 'a'), struct(2, 'b')));

SELECT map_from_entries(array(struct(1, 'a'), struct(1, 'b')));

SELECT str_to_map('a:1,b:2');

SELECT str_to_map('a:1,a:2');

SELECT str_to_map('a:1,b:2,a:3');

SELECT map_concat(map(1, 'a'), map(2, 'b'));

SELECT map_concat(map(1, 'a'), map(1, 'b'));

SELECT map_concat(map(1, 'a', 2, 'x'), map(1, 'b'), map(1, 'c'));

SELECT transform_keys(map(1, 'a', 2, 'b'), (k, v) -> k + 1);

SELECT transform_keys(map(1, 'a', 2, 'b'), (k, v) -> 1);

SELECT transform_keys(map('a', 1, 'b', 2), (k, v) -> upper(k));

SELECT map_zip_with(map(1, 'a'), map(1, 'b'), (k, v1, v2) -> concat(v1, v2));

SELECT CAST(map(1, 'a') AS MAP<STRING, STRING>);

SELECT CAST(map(1, 'a', 2, 'b') AS MAP<INT, STRING>);

SELECT from_json('{"a": 1, "a": 2}', 'MAP<STRING, INT>');

SELECT from_json('{"a": 1, "b": 2}', 'MAP<STRING, INT>');

SELECT k, v FROM (SELECT explode(map(1, 'a', 1, 'b')));

SELECT m[1] FROM (SELECT map(1, 'a', 1, 'b') AS m);

SELECT size(map(1, 'a', 1, 'b')), map_keys(map(1, 'a', 1, 'b')), map_values(map(1, 'a', 1, 'b'));

SELECT map(a, b) FROM VALUES (1, 'x'), (2, 'y'), (1, 'z') AS t(a, b);

SELECT map_from_arrays(a, b) FROM VALUES (array(1, 2), array('x', 'y')), (array(1, 1), array('p', 'q')) AS t(a, b);

SELECT map_concat(m1, m2) FROM VALUES (map(1, 'a'), map(1, 'b')), (map(2, 'a'), map(3, 'b')), (CAST(NULL AS MAP<INT, STRING>), map(1, 'b')) AS t(m1, m2);

-- negative control: nothing here has duplicate keys
SELECT map_keys(map(1, 'a', 2, 'b')), map_values(map('a', 1)), element_at(map(1, 'a'), 1), map_entries(map(1, 'a'));
