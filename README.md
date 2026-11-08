# spark_dialect: Spark SQL for DuckDB v2

[![CI](https://github.com/luke-falvey/duckdb-spark-dialect/actions/workflows/ci.yml/badge.svg)](https://github.com/luke-falvey/duckdb-spark-dialect/actions/workflows/ci.yml)

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

## Two dialects: `spark` and `spark_strict`

| | `spark` (lenient) | `spark_strict` |
|---|---|---|
| Valid Spark 3.5 SQL | Spark's results | Spark's results |
| DuckDB-only SQL | still works (the dialect is built on DuckDB's grammar and catalog) | raises an error |

`SET current_dialect = 'spark_strict'` raises an error for SQL that Spark 3.5 rejects. It is for checking
that a query is portable to Spark. `spark` stays the default recommendation: it also lets you reach for
a DuckDB feature when you want one. They are separate dialects (not a setting) because DuckDB compiles a
dialect's grammar once.

`spark_strict` rejects:

- **Functions** that are not Spark 3.5 built-ins (`[UNRESOLVED_ROUTINE]`): `list_transform`, `strftime`,
  `unnest`, `string_agg`, `count_star`, `generate_series`, `read_csv`, ... The extension's internal helpers
  (`add`, `cast_value`, `subscript`, ...) cannot be called by name in either dialect. Table functions in
  `FROM` are limited to `range`, `explode`, `explode_outer`, `inline`, `inline_outer`, `posexplode`,
  `posexplode_outer`, `stack`, `json_tuple` and `sql_keywords`.
- **Expression syntax**: `[1, 2]`, `{'a': 1}`, `MAP {...}`, `x::T`, `#1`, `alias: expr`, `lambda x: ...`,
  `TRY()`, `COLUMNS()`, `UNPACK()`, `ROW()`, `* EXCLUDE / EXCEPT / REPLACE / RENAME`, list comprehensions,
  DuckDB operators (`//`, `**`, `^@`, `~`, `GLOB`, `SIMILAR TO`, `->>`), `E'...'` and `$$...$$` strings.
- **Query clauses**: `QUALIFY`, `ASOF` / `POSITIONAL` / `NEAREST` / `JOIN BY`, `USING SAMPLE`,
  `TABLESAMPLE` methods and `<n>%`, `LIMIT <n>%`, statement-level `PIVOT ... ON` / `UNPIVOT ... ON`.
- **Statements**: `PRAGMA`, `CALL`, `COPY`, `EXPORT` / `IMPORT`, `ATTACH` / `DETACH`, `INSTALL` / `LOAD`,
  `CHECKPOINT`, `VACUUM`, `BEGIN` / `COMMIT`, `PREPARE` / `EXECUTE`, `SUMMARIZE`, `CREATE MACRO / SEQUENCE /
  TYPE / INDEX / SECRET / TRIGGER`, `INSERT OR REPLACE`, `ON CONFLICT`, `RETURNING`.
- **Types** other than Spark's: `HUGEINT`, `UBIGINT`, `UUID`, `JSON`, `BLOB`, `TIME`, `TIMESTAMPTZ`, `UNION`,
  `ENUM`, `T[]`, DuckDB's `STRUCT(...)` / `MAP(...)`.
- **Spark restrictions** that the lenient dialect relaxes: `CREATE TABLE` (also `... AS SELECT`) needs a
  `USING` clause, like Spark without Hive support (`[NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT]`), and
  `SET key=value` only stores the pair like Spark and never changes a DuckDB setting.

Limits of strict mode: error messages do not use Spark's error classes everywhere; `CREATE TABLE tbl (dummy
...)` without `USING` is let through because DuckDB itself parses type strings that way; and it only
rejects what the corpus in `test/cases/non_spark.sql` and the rules above cover, not every possible
DuckDB extension (a loaded DuckDB extension's own functions are rejected by name, but its syntax is not
audited).

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

**Current result: all 1308 differential cases are identical to Spark under both dialects (0 failing,
0 lenient)**: the original 1233, plus 53 reproducers of bugs found by the fuzzers
(`test/cases/fuzz_regressions.sql`) and the 22 TPC-H queries of the benchmark over generated data
(`test/cases/tpch_small.sql`). Another 73 cases in `test/cases/non_spark.sql` are SQL that Spark 3.5 rejects: `spark_strict`
rejects every one (the lenient `spark` dialect accepts 56 of them, reported as `LENIENT`, which is not a failure),
and `test/cases/strict_valid.sql` holds 65 tricky *valid* queries that must keep working in both. Run
`python test/run_duckdb.py --dialect spark_strict`, which fails on any `LENIENT` result. CI runs both dialects. The 1233 cases cover all 106
operator cases and every documented example of all 421 Spark built-ins that have one; the other 17
(cast functions and operators) are covered by the operator and literal/type cases.
`test/cases/regressions.sql` re-checks the later fixes with different data. The harness also fails
any case where DuckDB raises an internal error, even if Spark errors too. See
[COVERAGE.md](COVERAGE.md).

### Fuzzing

`test/fuzz/` holds fuzzers that look for what the case files miss (details in
[test/fuzz/README.md](test/fuzz/README.md)):

- `mutate.py`: mutated and structurally extreme SQL in every dialect, also through an AddressSanitizer / UBSan
  build; any crash, hang or `INTERNAL` error is a finding
- `args_sweep.py`: every documented Spark function example with edge-case arguments (as constants and as
  columns), compared with Spark
- `differential.py`: random typed queries (joins, grouping sets, windows, `LATERAL VIEW`, `PIVOT`, subqueries,
  ...) over tables of edge-case values, compared with Spark in both dialects, with automatic reduction
- `isolation.py`: DuckDB's own sqllogictest statements must behave the same with the extension loaded
  and a Spark dialect selected and reset
- `spark_corpus.py`: Spark's own `sql-tests` inputs, as a source of findings

Fixed bugs keep a reproducer in `test/cases/fuzz_regressions.sql` (compared with Spark) or
`test/fuzz/crash_regressions.sql` (must not crash). Differences that are deliberately not fixed are listed with
reasons in `test/fuzz/known_differences.toml`. CI runs a fixed-seed smoke of each fuzzer on every push; a nightly
workflow runs them longer with random seeds and the sanitizer build.

### Performance

`bench/` measures the cost of the dialect against equivalent native DuckDB SQL: TPC-H SF1 runs at 1.12x native
(geometric mean of the 22 queries), most functions and operators at 1.0x to 1.3x, and parsing at about 3x.
See [docs/PERFORMANCE.md](docs/PERFORMANCE.md) for the method and analysis and
[docs/PERFORMANCE_RESULTS.md](docs/PERFORMANCE_RESULTS.md) for the numbers.

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
- Expressions nested more deeply than a third of the thread's stack allows (about 160 levels on an 8 MB stack)
  fail with "Expression is nested too deeply" instead of DuckDB's limit of 1000 levels.
- More differences found by the fuzzers that are not fixed: see `test/fuzz/known_differences.toml`.
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

CI (`.github/workflows/ci.yml`) builds the extension against DuckDB v2.0.0, re-records the expected
results with Apache Spark 3.5, runs the comparison and publishes `COVERAGE.md` in the job summary.
The built extension is uploaded as a workflow artifact.

Run the tests locally (needs `pyspark==3.5.*`, Java 17+ and `duckdb==2.0.0`):

```bash
python test/make_golden.py      # only when cases change
python test/run_duckdb.py -v
python scripts/coverage_report.py
```
