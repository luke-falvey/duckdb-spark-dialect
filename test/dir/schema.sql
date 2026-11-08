CREATE TABLE ctas_probe(a INTEGER);
CREATE TABLE ins_probe(a INTEGER);
CREATE MACRO m (x) AS ("system".spark."add"(x, 1));

