-- base64 line chunking at 76 characters; literals, columns, NULL, empty, boundaries, nesting

SELECT base64(repeat('a', 100));

SELECT base64(repeat('a', 57));

SELECT base64(repeat('a', 58));

SELECT base64(repeat('a', 114));

SELECT base64(repeat('a', 115));

SELECT base64('');

SELECT base64(CAST(NULL AS STRING));

SELECT base64(s) FROM VALUES ('abc'), (repeat('xy', 60)), (CAST(NULL AS STRING)) AS t(s);

SELECT length(base64(repeat('a', 100)));

SELECT unbase64(base64(repeat('a', 100))) = CAST(repeat('a', 100) AS BINARY);

SELECT base64(X'0102FF')
