-- DDL / DML statements in Spark syntax. Statements whose output is engine metadata
-- (SHOW, DESCRIBE, EXPLAIN) only need to succeed; data statements are checked by reading back.

-- @setup
CREATE TABLE t1 (id INT, name STRING, score DOUBLE) USING parquet;

INSERT INTO t1 VALUES (1, 'a', 1.5), (2, 'b', NULL);

SELECT * FROM t1;

INSERT INTO TABLE t1 VALUES (3, 'c', 3.0);

INSERT INTO t1 (id, name) VALUES (4, 'd');

INSERT INTO t1 SELECT id + 10, upper(name), score FROM t1 WHERE id < 3;

SELECT * FROM t1;

INSERT OVERWRITE TABLE t1 SELECT * FROM VALUES (11, 'A', 1.5D), (12, 'B', CAST(NULL AS DOUBLE)) AS v(id, name, score);

SELECT * FROM t1;

INSERT OVERWRITE t1 VALUES (100, 'z', 0.5);

SELECT * FROM t1;

CREATE TABLE t2 USING parquet AS SELECT id, name FROM t1;

SELECT * FROM t2;

CREATE TABLE IF NOT EXISTS t2 (x INT) USING parquet;

CREATE TABLE t3 (a INT, b STRING) USING json;

CREATE TABLE t4 (a INT, b STRING, p STRING) USING parquet PARTITIONED BY (p);

INSERT INTO t4 VALUES (1, 'x', 'p1'), (2, 'y', 'p2');

SELECT * FROM t4;

CREATE TABLE t5 (a INT COMMENT 'the a', b STRING) USING parquet COMMENT 'table comment' TBLPROPERTIES ('k' = 'v');

CREATE TABLE t6 (a INT, b ARRAY<STRING>, c MAP<STRING, INT>, d STRUCT<x: INT, y: STRING>) USING parquet;

INSERT INTO t6 VALUES (1, ARRAY('p', 'q'), MAP('k', 1), STRUCT(5, 'w'));

SELECT a, b[1], c['k'], d.x, d.y FROM t6;

CREATE TABLE t7 USING parquet AS SELECT 1 AS a;

SELECT * FROM t7;

CREATE TABLE t8 (a BIGINT, b DECIMAL(10, 2), c DATE, d TIMESTAMP, e BOOLEAN, f BINARY, g TINYINT, h SMALLINT, i FLOAT, j VARCHAR(10), k CHAR(3)) USING parquet;

INSERT INTO t8 VALUES (1, 2.5, DATE '2024-01-01', TIMESTAMP '2024-01-01 10:00:00', true, X'01', 1, 2, 1.5, 'abc', 'x');

SELECT a, b, c, d, e, f, g, h, i, j, k FROM t8;

CREATE OR REPLACE TEMPORARY VIEW v1 AS SELECT id * 2 AS dbl FROM t1;

SELECT * FROM v1;

CREATE TEMP VIEW v2 AS SELECT 1 AS one;

SELECT * FROM v2;

CREATE OR REPLACE VIEW v3 AS SELECT name FROM t2;

SELECT * FROM v3;

CREATE TEMPORARY VIEW v4 (x, y) AS SELECT 1, 2;

SELECT * FROM v4;

-- @nondeterministic
SHOW TABLES;

-- @nondeterministic
DESCRIBE t1;

-- @nondeterministic
DESCRIBE TABLE EXTENDED t1;

-- @nondeterministic
EXPLAIN SELECT * FROM t1;

CACHE TABLE t1;

UNCACHE TABLE t1;

CACHE LAZY TABLE t1;

UNCACHE TABLE IF EXISTS t1;

REFRESH TABLE t1;

ALTER TABLE t1 ADD COLUMNS (extra STRING);

SELECT * FROM t1;

ALTER TABLE t1 RENAME TO t1_renamed;

SELECT * FROM t1_renamed;

TRUNCATE TABLE t2;

SELECT count(*) FROM t2;

DROP VIEW v1;

DROP VIEW IF EXISTS v1;

DROP TABLE t3;

DROP TABLE IF EXISTS t3;

CREATE DATABASE IF NOT EXISTS db1;

CREATE TABLE db1.t9 (a INT) USING parquet;

INSERT INTO db1.t9 VALUES (9);

SELECT * FROM db1.t9;

USE db1;

SELECT * FROM t9;

USE default;

DROP TABLE db1.t9;

DROP DATABASE db1;

SET spark.sql.shuffle.partitions = 4;

-- @nondeterministic
SET spark.sql.shuffle.partitions;

SELECT 'still works';
