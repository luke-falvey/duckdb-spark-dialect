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
- **Type errors Spark's analyzer raises** (`[DATATYPE_MISMATCH]`): casts Spark refuses (`CAST(true AS BINARY)`,
  `CAST(1 AS DATE)`; Spark's `Cast.canCast`, and the stricter `canAnsiCast` for `TRY_CAST` and under ANSI),
  argument types a function rejects (`date_add(true, 1)`, `md5(1)`: a table generated from Spark's answers by
  `scripts/gen_arg_type_rules.py`), inline tables and `CASE` branches without a common type (`VALUES (1), (true)`),
  and DuckDB's own type errors inside the extension's functions carry the class too.
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
test/cases/*.sql            the queries (operators, literals/types, query syntax, DDL/DML,
                            + 868 examples taken from Spark's own DESCRIBE FUNCTION docs)
test/cases/settings/        one file per Spark setting that changes results (66 files, run once per legal value),
                            SET / RESET, interactions between settings, session behaviour
test/cases/types/           the type conversion matrices C1-C14 (generated by scripts/gen_type_cases.py and
                            test/fuzz/args_sweep.py --types): about 90,000 statements
test/cases/spark_corpus/    54 inputs of Apache Spark's own SQL tests (typeCoercion, ansi, timestampNTZ, cast, ...)
test/settings/inventory.toml  all 478 Spark settings, classified (check_inventory.py) with the tier of their emulation
test/make_golden.py         runs every case in Spark 3.5.9 and stores the results (test/golden/)
test/run_duckdb.py          runs the same SQL in DuckDB with the dialect and compares
test/xfail.toml             expected failures, by id, case numbers and configuration
scripts/coverage_report.py  writes COVERAGE.md
```

Per case the harness compares the **rows**, the **column types** (through `duck_type_to_spark`), the number of
columns and the names the user chose, and for statements that fail in Spark the **phase** of the error
(analysis versus run time; a different phase is `PHASE`, a failure in `spark_strict`). In `spark_strict` the error class
of the type and settings files is compared too. Files marked `-- @exact` compare numbers exactly (no tolerance, `-0.0`
and `NaN` apart). Settings apply per file (`-- @config k=v`), per case (`-- @set`), as a product (`-- @matrix`) or
for a whole run (`--config k=v`); every setting is set in Spark and, with the same `SET`, in the dialect. A case that
fails in a known way is an expected failure (`XFAIL`, an entry of `test/xfail.toml`); a case that starts to pass is an
`XPASS` and fails the run until its entry is removed, as does an entry that matches nothing.

**Settings.** `SET` / `RESET` / `SET TIME ZONE` work like Spark's: validated, case-sensitive keys, one stored value per
connection. A semantic setting is *emulated* (`spark.sql.ansi.enabled` partly, `session.timeZone`,
`mapKeyDedupPolicy`, `legacy.sizeOfNull`, `jsonGenerator.ignoreNullFields`, `function.concatBinaryAsString`,
`function.eltOutputAsString`, `chunkBase64String.enabled`, `decimalOperations.allowPrecisionLoss`,
`legacy.statisticalAggregate`, `legacy.negativeIndexInArrayInsert`, `legacy.followThreeValuedLogicInArrayExists`) or
**rejected with an error** (`UNSUPPORTED_FEATURE.SET_CONF`): a `SET` never silently has no effect. The rest of the
semantic settings (for example `caseSensitive`, `storeAssignmentPolicy`, `timestampType`, `groupByOrdinal`) are listed
with the reason in `test/settings/inventory.toml`. DuckDB's own settings do not leak into the Spark dialects
(`scripts/check_duckdb_leaks.py` runs the base files with 17 DuckDB settings at non-default values).

**Current result.** `python test/run_duckdb.py [--dialect spark_strict]` over `test/cases/` ends with 0 failures in
both dialects (CI runs both, in shards, and the files again under `spark.sql.ansi.enabled=true`; the nightly workflow
adds the time zone and decimal precision configurations). The original 1,721 differential cases (documented
examples, operators, literals, query syntax, DDL/DML, TPC-H, fuzz and limitation reproducers, ANSI runtime) all pass in
`spark_strict`; under `spark` the 64 `LENIENT` ones are queries that Spark rejects and the lenient dialect accepts.
The new matrices find what the dialect does not do yet: **the cases that still differ are listed as expected failures,
grouped by id, with a reproducing example, in
[docs/plans/spark-parity/known_differences.md](docs/plans/spark-parity/known_differences.md)**, and counted in
[COVERAGE.md](COVERAGE.md). Most belong to a few causes: DuckDB has one `INTERVAL` and one `TIMESTAMP` type
(`types-interval`, `types-ntz`), ANSI mode's implicit-cast rules for function arguments and comparisons
(`ansi-implicit-cast`), settings
the dialect rejects (`conf-rejected`), and the open work the matrices found (`types-open-c1` ... `types-open-c14`:
implicit conversions of function arguments, common types of `CASE` / `UNION`, key coercion of JOIN `USING` /
`NATURAL` and `IN (subquery)`, and others). `strict` rejects less than Spark does: the strict dialect
still accepts many implicit casts that Spark's analyzer refuses, and those leniencies are the bulk of the expected
failures of `spark_strict`. Run `python scripts/xfail_summary.py` for the table.

### Fuzzing

`test/fuzz/` holds fuzzers that look for what the case files miss (details in
[test/fuzz/README.md](test/fuzz/README.md)):

- `mutate.py`: mutated and structurally extreme SQL in every dialect, also through an AddressSanitizer / UBSan
  build; any crash, hang or `INTERNAL` error is a finding
- `args_sweep.py`: every documented Spark function example with edge-case arguments (as constants and as
  columns), compared with Spark; `--types` replaces each argument slot with a value of every type (the matrix C6)
- `differential.py`: random typed queries (joins, grouping sets, windows, `LATERAL VIEW`, `PIVOT`, subqueries,
  ...) over tables of edge-case values (including `TIMESTAMP_NTZ`, intervals, `CHAR`, `DECIMAL(38,0)`) and mixed
  kinds, compared with Spark in both dialects, with automatic reduction; `--conf-fuzz` also draws a random
  assignment of the emulated settings per batch
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

- Settings: only the emulated settings listed above change behaviour; every other semantic setting is rejected by `SET`
  (inventory notes say why: parser and binder decisions such as `groupByOrdinal`, `caseSensitive`,
  `storeAssignmentPolicy` and `timestampType` cannot be switched from an extension). A view or cached table uses the
  settings in force when it is queried, not when it was created (Spark captures them).
- `TIMESTAMP` and `TIMESTAMP_NTZ` are one DuckDB type, and all `INTERVAL` ranges are one type: `typeof` and the column
  types report Spark's `timestamp` / `interval` where the type is not known, and the cases that depend on the
  difference are expected failures (`types-ntz`, `types-interval`). No `TIMESTAMP` to `TIMESTAMPTZ` remap was evaluated.
- Column names Spark generates for unaliased expressions (`(1 + 1)`, `count(1)`) are not reproduced, so a later
  statement cannot refer to them (`generated_names.sql`).

- ANSI mode (`SET spark.sql.ansi.enabled = true`, which sets the DuckDB setting `spark_ansi_enabled`; `RESET` and
  `SET ... = false` turn it off) covers runtime errors: integer overflow of `+ - * -x abs` and of `sum`, decimal
  overflow of `+ - *` and `sum`, division by zero (`/ % div mod`), invalid and out-of-range `CAST` (strings must be
  integers: `'1.5'` is not), array index out of bounds (`[]`, `element_at`) and `size(NULL)` returning NULL; the
  `try_*` functions stay lenient. ANSI's implicit casts are emulated for arithmetic and common types (a string next
  to an integer is a BIGINT, next to a fraction a DOUBLE, and fails with `CAST_INVALID_INPUT` when it does not
  convert; an integer with FLOAT widens to DOUBLE). Not emulated: the rest of `AnsiTypeCoercion` (function
  arguments, comparisons), map key lookups of missing keys, overflow in `avg`, `pmod`, `date`/`timestamp` arithmetic and
  datetime parsing errors. A prepared statement is bound again when the setting changes.
- `window(...)` / `session_window(...)` are supported as `GROUP BY` keys. For `session_window` over a join, the
  joined tables' aliases are not available in the query (a single table or subquery keeps its alias).
- Struct values are matched to struct columns by position (like Spark) in `INSERT ... VALUES` and `INSERT ... SELECT`
  when the value is written with `struct(...)` or `named_struct(...)`, and in `CAST`. Spark also matches by position in
  `UNION` and in `IN (...)` lists; there DuckDB's by-name rules apply (replacing them would change the default
  dialect too). Everywhere else Spark rejects structs with different field names, which the lenient dialect accepts.
- `array_sort` with a comparator: comparators of the form `CASE WHEN f(l) < f(r) THEN -1 WHEN f(l) > f(r) THEN 1
  ELSE 0 END` (also reversed, for a descending sort) over numbers, strings, booleans, dates and timestamps without
  NULL keys use a stable sort (O(n log n)). Other comparators rank each element (O(n²)); an inconsistent comparator
  can order differently from Spark's TimSort.
- Expressions nested deeper than about 60 levels are transformed on a helper thread with a 64 MB stack (Linux and
  macOS), so DuckDB's limit of 1000 levels applies; on other platforms the deeper statement fails with
  "Expression is nested too deeply".
- More differences found by the fuzzers that are not fixed: see `test/fuzz/known_differences.toml` (7 entries). Spark's
  own SQL test inputs (`python test/fuzz/spark_corpus.py run`) still show, out of 3,734 statements, 512 DuckDB errors
  where Spark succeeds (mostly Spark commands such as `DESC FUNCTION`, interval and datetime-format corner cases),
  156 wrong results, 147 leniencies of `spark_strict` and 3 strict rejects of valid queries (it was 520, 157, 147
  and 17 before the limitations work).
- `CHAR(n)` columns: the padding casts for `n` > 255 are added when such a type is first used in a session; a
  database file that was created with one and is opened in a new session pads again only after the type is used.
- Statements are rewritten at the token level in places (see the table above). Error positions point at the
  original text. Of the 1,019 errors that Spark's own SQL test inputs produce here, 2 name an internal function
  (`to_json_impl` and `mask_impl`, for arguments of a type they do not accept); `python test/fuzz/spark_corpus.py
  names` measures this.

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
