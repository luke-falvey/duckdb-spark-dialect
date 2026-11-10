-- elt over binary and string candidates; literals, columns, NULL, out of range, mixed types

SELECT typeof(elt(1, X'01'));

SELECT elt(1, X'4142', X'43');

SELECT elt(2, X'4142', X'43');

SELECT typeof(elt(1, 'a', 'b'));

SELECT elt(1, 'a', X'41');

SELECT elt(3, 'a', 'b');

SELECT elt(0, 'a', 'b');

SELECT elt(CAST(NULL AS INT), 'a', 'b');

SELECT elt(n, 'a', 'b') FROM VALUES (1), (2), (3), (CAST(NULL AS INT)) AS t(n);

SELECT elt(n, b1, b2) FROM VALUES (1, X'41', X'42'), (2, X'43', CAST(NULL AS BINARY)) AS t(n, b1, b2);

SELECT typeof(elt(1, 1, 2));

SELECT elt(1, 'a', CAST(NULL AS STRING))
