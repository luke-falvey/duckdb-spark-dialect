-- Generated column names that a later statement refers to (plan 9.5, gating): CTAS and views over unaliased expressions.

-- @setup
CREATE TABLE gn1 USING parquet AS SELECT 1 + 1;

SELECT `(1 + 1)` FROM gn1;

-- @setup
CREATE TABLE gn2 USING parquet AS SELECT count(1) FROM VALUES (1), (2) AS t(x);

SELECT `count(1)` FROM gn2;

-- @setup
CREATE OR REPLACE TEMPORARY VIEW gv3 AS SELECT CAST(x AS INT) FROM VALUES (1), (2) AS t(x);

SELECT `CAST(x AS INT)` FROM gv3;

-- @setup
CREATE TABLE gn4 USING parquet AS SELECT upper('a'), lower('B');

SELECT `upper(a)`, `lower(B)` FROM gn4;

-- @setup
CREATE OR REPLACE TEMPORARY VIEW gv5 AS SELECT x * 2 FROM VALUES (1), (2) AS t(x);

SELECT `(x * 2)` FROM gv5;

-- @setup
CREATE TABLE gn6 USING parquet AS SELECT sum(x) FROM VALUES (1), (2) AS t(x);

SELECT `sum(x)` FROM gn6;

-- @setup
CREATE OR REPLACE TEMPORARY VIEW gv7 AS SELECT x + y FROM VALUES (1, 2) AS t(x, y);

SELECT `(x + y)` FROM gv7;

-- @setup
CREATE TABLE gn8 USING parquet AS SELECT concat('a', 'b');

SELECT `concat(a, b)` FROM gn8;

-- @setup
CREATE OR REPLACE TEMPORARY VIEW gv9 AS SELECT -x FROM VALUES (1) AS t(x);

SELECT `(- x)` FROM gv9;

-- @setup
CREATE TABLE gn10 USING parquet AS SELECT x > 1 FROM VALUES (1), (2) AS t(x);

SELECT `(x > 1)` FROM gn10;

-- @setup
CREATE OR REPLACE TEMPORARY VIEW gv11 AS SELECT array(1, 2);

SELECT `array(1, 2)` FROM gv11;

-- @setup
CREATE TABLE gn12 USING parquet AS SELECT avg(x), max(x), min(x) FROM VALUES (1), (2) AS t(x);

SELECT `avg(x)`, `max(x)`, `min(x)` FROM gn12;

-- @setup
CREATE OR REPLACE TEMPORARY VIEW gv13 AS SELECT length('abc');

SELECT `length(abc)` FROM gv13;

-- @setup
CREATE TABLE gn14 USING parquet AS SELECT abs(-1);

SELECT `abs(-1)` FROM gn14;

-- @setup
CREATE OR REPLACE TEMPORARY VIEW gv15 AS SELECT x IS NULL FROM VALUES (1) AS t(x);

SELECT `(x IS NULL)` FROM gv15;

-- @setup
CREATE TABLE gn16 USING parquet AS SELECT coalesce(NULL, 1);

SELECT `coalesce(NULL, 1)` FROM gn16;

-- @setup
CREATE OR REPLACE TEMPORARY VIEW gv17 AS SELECT x FROM VALUES (1) AS t(x);

SELECT x FROM gv17;

-- @setup
CREATE TABLE gn18 USING parquet AS SELECT round(1.5);

SELECT `round(1.5, 0)` FROM gn18;

-- @setup
CREATE OR REPLACE TEMPORARY VIEW gv19 AS SELECT 'a' || 'b';

SELECT `concat(a, b)` FROM gv19;

-- @setup
CREATE TABLE gn20 USING parquet AS SELECT x BETWEEN 1 AND 2 FROM VALUES (1) AS t(x);

SELECT `((x >= 1) AND (x <= 2))` FROM gn20;
