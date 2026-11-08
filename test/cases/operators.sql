-- Every Spark 3.5 operator: arithmetic, bitwise, comparison, logical, string, predicates, access.
-- Each is exercised with normal values, NULLs and edge cases, against literals and table columns.

-- @setup
CREATE TEMPORARY VIEW nums AS SELECT * FROM VALUES
  (1, 2, 1.5D, 2.25D, CAST(10.50 AS DECIMAL(10,2)), 'abc', true),
  (-7, 3, -0.5D, 4.0D, CAST(-3.25 AS DECIMAL(10,2)), 'Hello', false),
  (0, 0, 0.0D, 0.0D, CAST(0 AS DECIMAL(10,2)), '', NULL),
  (NULL, 5, NULL, 1.0D, NULL, NULL, true),
  (2147483647, 1, 1e308D, 1e-308D, CAST(99999999.99 AS DECIMAL(10,2)), 'x%y', false)
  AS nums(a, b, d, e, m, s, flag);

-- ===== + =====
SELECT 1 + 2, 1 + 2.5, 1.5 + 2.5, 1 + NULL, NULL + NULL;

SELECT a + b, d + e, m + 1, a + d FROM nums;

SELECT 2147483647 + 1;

SELECT 9223372036854775807L + 1L;

SELECT CAST(1 AS TINYINT) + CAST(127 AS TINYINT), CAST(1 AS SMALLINT) + CAST(32767 AS SMALLINT);

SELECT +5, +(-5), +a FROM nums;

SELECT DATE '2024-01-31' + 1, 1 + DATE '2024-02-28';

SELECT '1' + 2, '1.5' + 1, 'abc' + 1;

-- ===== - =====
SELECT 5 - 3, 3 - 5, 2.5 - 1, 1 - NULL;

SELECT a - b, d - e, m - b, -a, -d, -m FROM nums;

SELECT -2147483648 - 1;

SELECT -(-2147483648);

SELECT DATE '2024-03-01' - 1, DATE '2024-03-01' - DATE '2024-01-01';

SELECT TIMESTAMP '2024-03-01 10:00:00' - TIMESTAMP '2024-03-01 08:30:00';

SELECT '10' - 3;

-- ===== * =====
SELECT 6 * 7, 2.5 * 4, -3 * 0.5, 2 * NULL;

SELECT a * b, d * e, m * 2, m * m FROM nums;

SELECT 2147483647 * 2;

SELECT 3037000500L * 3037000500L;

SELECT CAST(1.5 AS DECIMAL(3,1)) * CAST(2.25 AS DECIMAL(4,2));

-- ===== / =====
SELECT 7 / 2, 6 / 3, -7 / 2, 1 / 3, 7.0 / 2;

SELECT 1 / 0, 1.0 / 0, 0 / 0, 1.5D / 0, -1.5D / 0.0D, 0.0D / 0.0D, NULL / 1;

SELECT a / b, d / e, m / b, a / 0 FROM nums;

SELECT CAST(1 AS DECIMAL(10,2)) / CAST(3 AS DECIMAL(10,2));

SELECT 10 / '4';

-- ===== % and mod =====
SELECT 7 % 3, -7 % 3, 7 % -3, -7 % -3, 7.5 % 2, 7 % 0, NULL % 2;

SELECT a % b, d % e, m % 3, mod(a, b) FROM nums;

SELECT 5.5D % 0, 5.5D % 2.0D, -5.5D % 2.0D;

-- ===== div =====
SELECT 7 div 2, -7 div 2, 7 div -2, 7 div 0, NULL div 2;

SELECT a div b FROM nums;

SELECT 7.9 div 2, -7.9 div 2.5;

SELECT 9223372036854775807L div -1L;

-- ===== & | ^ ~ =====
SELECT 12 & 10, 12 | 10, 12 ^ 10, ~12, ~0, ~-1;

SELECT a & b, a | b, a ^ b, ~a FROM nums;

SELECT 5 & NULL, NULL | 1, 3 ^ NULL, ~CAST(NULL AS INT);

SELECT CAST(-1 AS TINYINT) & CAST(15 AS TINYINT), 9223372036854775807L ^ -1L;

SELECT 1 | 2 & 3, 1 ^ 3 & 2, 2 * 3 & 7;

SELECT 2 ^ 3 + 1, 1 + 2 ^ 3;

-- ===== = == != <> =====
SELECT 1 = 1, 1 == 1, 1 = 2, 1 != 2, 1 <> 1, 1 = NULL, NULL = NULL, NULL != 1;

SELECT a = b, a == b, a != b, a <> b FROM nums;

SELECT 'abc' = 'abc', 'abc' = 'ABC', 'a' != 'b', 1 = '1', 1 = 1.0, '1.0' = 1;

SELECT DATE '2024-01-01' = TIMESTAMP '2024-01-01 00:00:00', DATE '2024-01-01' = '2024-01-01';

SELECT ARRAY(1, 2) = ARRAY(1, 2), STRUCT(1, 'a') = STRUCT(1, 'a'), ARRAY(1, NULL) = ARRAY(1, NULL);

SELECT 0.0D = -0.0D, CAST('NaN' AS DOUBLE) = CAST('NaN' AS DOUBLE);

-- ===== < <= > >= =====
SELECT 1 < 2, 2 <= 2, 3 > 2, 2 >= 3, 1 < NULL, NULL >= NULL;

SELECT a < b, a <= b, a > b, a >= b, d < e, m > 0 FROM nums;

SELECT 'a' < 'b', 'B' < 'a', 'abc' < 'abd', '' < 'a', '10' < '9', 10 < '9';

SELECT DATE '2024-01-01' < DATE '2024-01-02', TIMESTAMP '2024-01-01 10:00:00' > DATE '2024-01-01';

SELECT CAST('NaN' AS DOUBLE) > 1e308D, CAST('NaN' AS DOUBLE) < CAST('Infinity' AS DOUBLE);

SELECT true > false, ARRAY(1, 2) < ARRAY(1, 3), ARRAY(1) < ARRAY(1, 0);

-- ===== <=> =====
SELECT 1 <=> 1, 1 <=> 2, 1 <=> NULL, NULL <=> NULL, NULL <=> 1;

SELECT a <=> b, s <=> NULL, flag <=> true FROM nums;

SELECT NOT (NULL <=> NULL), (1 <=> NULL) = false;

-- ===== AND OR NOT ! =====
SELECT true AND true, true AND false, true AND NULL, false AND NULL, NULL AND NULL;

SELECT true OR false, false OR false, true OR NULL, false OR NULL, NULL OR NULL;

SELECT NOT true, NOT false, NOT NULL, !true, !false, !NULL;

SELECT flag, NOT flag, !flag, flag AND a > 0, flag OR a > 0 FROM nums;

SELECT !(1 = 2), ! (1 = 1) OR true, NOT 1 = 2 AND true;

SELECT true OR false AND false, (true OR false) AND false, NOT false AND false;

-- ===== || =====
SELECT 'a' || 'b', 'a' || NULL, NULL || 'b', 'a' || 1, 1 || 2, 'x' || 1.5;

SELECT s || '!', s || s, s || a FROM nums;

SELECT ARRAY(1, 2) || ARRAY(3), ARRAY('a') || ARRAY(NULL, 'b');

SELECT 'a' || 'b' || 'c', 1 + 2 || 3, 'v' || 1 + 2;

-- ===== BETWEEN =====
SELECT 2 BETWEEN 1 AND 3, 5 BETWEEN 1 AND 3, 2 NOT BETWEEN 1 AND 3, NULL BETWEEN 1 AND 2, 2 BETWEEN NULL AND 3;

SELECT a, a BETWEEN 0 AND 5, a NOT BETWEEN -10 AND 0 FROM nums;

SELECT 'b' BETWEEN 'a' AND 'c', DATE '2024-02-01' BETWEEN DATE '2024-01-01' AND DATE '2024-03-01', 3 BETWEEN 3 AND 1;

-- ===== IN =====
SELECT 1 IN (1, 2), 3 IN (1, 2), 3 NOT IN (1, 2), 1 IN (NULL, 1), 3 IN (NULL, 1), 3 NOT IN (NULL, 1), NULL IN (1);

SELECT a IN (1, 0, NULL), s IN ('abc', ''), a NOT IN (1, 2) FROM nums;

SELECT a FROM nums WHERE a IN (SELECT b FROM nums);

SELECT a FROM nums WHERE a NOT IN (SELECT b FROM nums WHERE b IS NOT NULL);

SELECT 1 IN ('1', 2), '1' IN (1, 2);

-- ===== LIKE / ILIKE / RLIKE / REGEXP =====
SELECT 'abc' LIKE 'a%', 'abc' LIKE 'a_c', 'abc' LIKE 'A%', 'abc' NOT LIKE 'a%', NULL LIKE 'a', 'a' LIKE NULL;

SELECT 'a_c' LIKE 'a\\_c', 'abc' LIKE 'a\\_c', '100%' LIKE '100\\%', 'a\\b' LIKE 'a\\\\b';

SELECT 'a_c' LIKE 'a/_c' ESCAPE '/', 'abc' LIKE 'a/_c' ESCAPE '/';

SELECT s, s LIKE '%l%', s ILIKE 'h%', s NOT ILIKE '%L%', s LIKE '%\\%%' FROM nums;

SELECT 'ABC' ILIKE 'a%', 'abc' ILIKE 'A_C', 'abc' NOT ILIKE 'x%';

SELECT 'abc' RLIKE 'b', 'abc' RLIKE '^b', 'abc' REGEXP 'a.c', 'abc' NOT RLIKE 'z', 'ABC' RLIKE '(?i)abc', NULL RLIKE 'a';

SELECT s, s RLIKE '^[A-Z]', s REGEXP 'l{2}' FROM nums;

SELECT 'a\nb' RLIKE 'a.b', 'foo123' RLIKE '\\d+', 'line' RLIKE '^l.*e$';

SELECT 'abc' LIKE ANY ('a%', 'z%'), 'abc' LIKE ALL ('a%', '%c'), 'abc' NOT LIKE ANY ('a%', 'z%'), 'abc' LIKE SOME ('x%', '%b%');

SELECT 'ABC' ILIKE ANY ('a%', 'z%'), 'abc' ILIKE ALL ('A%', '%C');

-- ===== IS ... =====
SELECT NULL IS NULL, 1 IS NULL, 1 IS NOT NULL, NULL IS NOT NULL;

SELECT a IS NULL, s IS NOT NULL, isnull(a), isnotnull(s) FROM nums;

SELECT true IS TRUE, NULL IS TRUE, false IS NOT TRUE, NULL IS NOT FALSE, false IS FALSE, NULL IS UNKNOWN, true IS NOT UNKNOWN;

SELECT 1 IS DISTINCT FROM 2, NULL IS DISTINCT FROM NULL, 1 IS NOT DISTINCT FROM 1, NULL IS NOT DISTINCT FROM 1;

SELECT flag IS TRUE, flag IS NOT FALSE, flag IS NULL FROM nums;

-- ===== CASE =====
SELECT CASE WHEN 1 > 0 THEN 'pos' WHEN 1 < 0 THEN 'neg' ELSE 'zero' END;

SELECT a, CASE WHEN a > 0 THEN 'pos' WHEN a < 0 THEN 'neg' WHEN a = 0 THEN 'zero' END FROM nums;

SELECT a, CASE a WHEN 1 THEN 'one' WHEN 0 THEN 'zero' ELSE 'other' END FROM nums;

SELECT CASE NULL WHEN NULL THEN 'null matched' ELSE 'no match' END, CASE WHEN CAST(NULL AS BOOLEAN) THEN 1 ELSE 2 END;

SELECT CASE WHEN true THEN 1 ELSE 'a' END, CASE WHEN false THEN 1 ELSE 2.5 END;

-- ===== [] subscript and . field access =====
SELECT ARRAY(10, 20, 30)[0], ARRAY(10, 20, 30)[2], ARRAY(10, 20, 30)[3], ARRAY(10, 20, 30)[-1];

SELECT MAP('a', 1, 'b', 2)['a'], MAP('a', 1)['z'], MAP(1, 'x', 2, 'y')[2];

SELECT STRUCT(1 AS x, 'q' AS y).x, STRUCT(1 AS x, 'q' AS y).y, named_struct('k', ARRAY(1, 2)).k[1];

SELECT ARRAY(ARRAY(1, 2), ARRAY(3))[1][0], ARRAY(MAP('a', 1))[0]['a'], MAP('k', ARRAY(5, 6))['k'][1];

SELECT ARRAY(1, 2)[CAST(NULL AS INT)], CAST(NULL AS ARRAY<INT>)[0], ARRAY(1, 2)[1 + 0];

SELECT ARRAY(STRUCT(1 AS f), STRUCT(2 AS f))[1].f;

-- ===== operator precedence =====
SELECT 1 + 2 * 3, (1 + 2) * 3, 10 - 4 - 3, 2 * 3 % 4, -2 * 3, 10 / 4 * 2, 7 div 2 * 2;

SELECT 1 + 2 = 3, 1 < 2 = true, NOT 1 + 1 = 3, 1 = 1 AND 2 = 2 OR false;

SELECT -1 & 3, ~1 + 1, - ~1, 1 | 2 = 3;

SELECT 1 + 1 BETWEEN 1 AND 2, 'a' || 'b' LIKE 'ab', 1 + 1 IN (2, 3);

-- ===== lambda -> =====
SELECT transform(ARRAY(1, 2, 3), x -> x * 10), filter(ARRAY(1, 2, 3, 4), x -> x % 2 = 0);

SELECT transform(ARRAY(1, 2, 3), (x, i) -> x + i), exists(ARRAY(1, 2), x -> x > 1), forall(ARRAY(1, 2), x -> x > 1);

SELECT aggregate(ARRAY(1, 2, 3), 0, (acc, x) -> acc + x), aggregate(ARRAY(1, 2, 3), 0, (acc, x) -> acc + x, acc -> acc * 10);

SELECT transform(ARRAY(ARRAY(1, 2), ARRAY(3)), a -> transform(a, x -> x + 1));

SELECT a, transform(ARRAY(1, 2), x -> x + a) FROM nums;
