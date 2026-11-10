-- array_insert with a negative index; literals, columns, NULLs, out of range, nesting, positive control

SELECT array_insert(array(1, 2, 3), -1, 4);

SELECT array_insert(array(1, 2, 3), -2, 4);

SELECT array_insert(array(1, 2, 3), -3, 4);

SELECT array_insert(array(1, 2, 3), -4, 4);

SELECT array_insert(array(1, 2, 3), -6, 4);

SELECT array_insert(array(5, 3, 2, 1), -3, 4);

SELECT array_insert(array(1, 2, 3), 2, 4);

SELECT array_insert(array(1, 2, 3), 5, 4);

SELECT array_insert(array(1, NULL, 3), -1, NULL);

SELECT array_insert(CAST(NULL AS ARRAY<INT>), -1, 4);

SELECT array_insert(array(1, 2), CAST(NULL AS INT), 4);

SELECT array_insert(a, i, 9) FROM VALUES (array(1, 2, 3), -1), (array(4), -2), (array(), -1), (array(1, 2), 1) AS t(a, i);

SELECT array_insert(array_insert(array(1, 2, 3), -1, 4), -1, 5);

SELECT array_insert(array(array(1), array(2)), -1, array(3));
