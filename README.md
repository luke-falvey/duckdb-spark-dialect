# spark_dialect: Spark SQL for DuckDB v2

A DuckDB 2.0 extension that adds a selectable **Spark SQL (3.5)** dialect, built on DuckDB v2's
extensible PEG parser (`DialectExtension`).

```sql
LOAD 'spark_dialect.duckdb_extension';
SET current_dialect = 'spark';      -- also sets dialect_compatibility_mode = 'spark'

SELECT id, skill FROM emp LATERAL VIEW OUTER explode(skills) t AS skill SORT BY id;
SELECT 2147483647 + 1, 7 div 2, 1 / 0, ARRAY(10, 20)[0], 1 <=> NULL, 12 ^ 10, "a string";
-- -2147483648, 3, NULL, 10, false, 6, 'a string'
RESET current_dialect;              -- back to DuckDB SQL; nothing else is affected
```

## How it works

| Layer | File | What it does |
|---|---|---|
| Tokenizer | `src/spark_tokenizer.cpp` | Spark lexical rules (`"strings"`, `\` escapes, `r'raw'`, adjacent literals, `10L`/`1.5BD`, `!`, `X 'ff'`), then token rewrites for pure syntax sugar: `SORT/CLUSTER/DISTRIBUTE BY`, `LEFT SEMI/ANTI`, `MINUS`, `WITH ROLLUP/CUBE`, `LATERAL VIEW`, `INSERT OVERWRITE`, table options (`USING`, `TBLPROPERTIES`, …), `CACHE`/`REFRESH`, Spark `SET`, `ALTER TABLE ADD COLUMNS`, interval literals, `FROM VALUES` column names, generators in `FROM`, the `window` grouping column |
| Grammar | `src/spark_grammar.cpp` | Replaces DuckDB's `Expression` rule with Spark's precedence ladder (from `SqlBaseParser.g4`); Spark types (`ARRAY<T>`, `MAP<K,V>`, `STRUCT<a: T>`, `STRING`, `LONG`, …); constructors; lambdas; hooks on DuckDB's function, cast, CASE and ORDER BY rules (Spark NULL ordering, type coercion, function routing, parse-time rewrites of variadic and higher-order functions) |
| Functions | `src/spark_*.cpp` | ~400 Spark functions in a dedicated `system.spark` schema (not on the search path, so DuckDB SQL is unaffected): SQL macros (`spark_macros.cpp`) plus C++ for type-dependent dispatch (Spark arithmetic/casts/overflow, 0-based subscripts), Java datetime patterns, hashes (Murmur3/XXH64 bit-exact), AES, count-min sketch, XPath, JSON/CSV schemas, … |

Operators compile to calls such as `system.spark.add(a, b)` whose bind step picks a kernel for the
argument types, so `int + int` wraps on overflow like Spark (non-ANSI), `/` by zero is `NULL`,
decimals follow Spark's precision rules, and `CAST` has Spark's non-ANSI semantics.

## Testing: differential against Apache Spark 3.5

```
test/cases/*.sql          the queries (operators, literals/types, query syntax, DDL/DML,
                          + 868 examples taken from Spark's own DESCRIBE FUNCTION docs)
test/make_golden.py       runs every case in Spark 3.5.9 and stores the results (test/golden/)
test/run_duckdb.py        runs the same SQL in DuckDB with the dialect and compares
scripts/coverage_report.py writes COVERAGE.md
```

Values are compared after normalizing engine-specific Python types (numbers numerically with
1e-9 relative tolerance; unordered results as multisets). Two harness rules: DDL/DML statements
only need to succeed (Spark returns no rows, DuckDB a row count; the data is checked by later
queries), and cases Spark marks as random (`rand()`, `uuid()`, random-IV `aes_encrypt`, …) only
need to succeed.

**Current result: all 1233 cases identical to Spark (0 failing, 0 lenient).** That covers all 106
operator cases and every documented example of all 421 Spark built-ins that have one; the other 17
(cast functions and operators) are covered by the operator and literal/type cases.
`test/cases/regressions.sql` re-checks the later fixes with different data. The harness also fails
any case where DuckDB raises an internal error, even if Spark errors too. See
[COVERAGE.md](COVERAGE.md).

### Limitations (behaviour the tests do not exercise)

- ANSI mode (`spark.sql.ansi.enabled = true`) is not emulated; semantics are Spark 3.5's default.
- `sum`/`avg` carry an extra `any_value(x)` aggregate so that their result type can follow Spark's
  rules (`sum(decimal(p,s))` is `decimal(p+10,s)`); this costs a little performance.
- `window(...)` / `session_window(...)` are supported as `GROUP BY` keys. The query's `FROM` source
  becomes a subquery, so references to the source's table alias (e.g. `tab.a`) in that query are
  not available.
- `CHAR(n)` column padding is supported for `n` up to 255.
- Unnamed `STRUCT(...)` values match named struct columns by position in `INSERT ... VALUES`
  and in `CAST`, not in other implicit casts.
- `array_sort` with a comparator sorts by rank (O(n²)); it expects a consistent comparator.
- Statements are rewritten at the token level in places (see the table above), so error messages
  for invalid Spark SQL can point at rewritten text.

## Build

The extension builds against DuckDB **v2.0.0** sources:

```bash
git clone https://github.com/duckdb/duckdb && cd duckdb && git checkout v2.0.0
GEN=ninja EXTENSION_CONFIGS=/path/to/duckdb-spark/extension_config.cmake make release
# -> build/release/extension/spark_dialect/spark_dialect.duckdb_extension
```

To load it into the `duckdb==2.0.0` Python package from a source checkout without tags (version
`v2.0.0-dev1`), open the connection with `allow_unsigned_extensions` and
`allow_extensions_metadata_mismatch`.

Run the tests (needs `pyspark==3.5.*`, Java 17+ and `duckdb==2.0.0`):

```bash
python test/make_golden.py      # only when cases change
python test/run_duckdb.py -v
python scripts/coverage_report.py
```
