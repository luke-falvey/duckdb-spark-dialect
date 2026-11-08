# Plan: a strict Spark dialect (`spark_strict`)

This plan is written to be handed to a coding agent. It is self-contained: read it top to bottom
before changing code. File and function names refer to this repository at commit `78306f7` (`main`)
plus the CI workflow on branch `ci-workflow`.

## 1. Goal

Today `SET current_dialect = 'spark'` runs every valid Spark 3.5 query with Spark's results (1233/1233
differential test cases), but it also **accepts a lot of SQL that Spark 3.5 rejects**. The dialect is
built as changes on top of DuckDB's grammar and function catalog, so DuckDB-only syntax, types,
statements and functions still work. A probe of 29 DuckDB-only queries found 25 that the dialect accepts
and Spark rejects (section 6.1).

Add a second dialect, **`spark_strict`**, that behaves like `spark` for valid Spark SQL and **raises an
error for SQL that Spark 3.5 rejects**. `spark` stays as it is (lenient), and stays the default
recommendation.

Why a second dialect and not a setting: a `DialectExtension` compiles its grammar once and caches it
(`DialectExtension::GetCompiledGrammar` in DuckDB's `src/parser/peg/dialect_extension.cpp`), so a
runtime setting cannot switch grammar rules. Register two `SparkDialect` instances that differ by a
`strict` flag.

## 2. Acceptance criteria

1. `SET current_dialect = 'spark_strict'` works; `spark` behaves exactly as before (no regressions).
2. All existing differential cases (`test/cases/*.sql`, 1233 today) pass under **both** dialects.
3. A new case file `test/cases/non_spark.sql` (section 6) holds at least 60 queries that Spark 3.5
   rejects. Under `spark_strict` every one of them raises an error in DuckDB (outcome `PASS`, "both
   error"). Under `spark` they are reported as `LENIENT` and do not fail the run.
4. `test/run_duckdb.py --dialect spark_strict` treats any `LENIENT` outcome as a failure.
5. CI runs the suite under both dialects (a matrix over `dialect: [spark, spark_strict]`).
6. No DuckDB `INTERNAL` errors in any case (the harness already fails these).
7. `README.md` documents `spark_strict` and what it rejects; `COVERAGE.md` is regenerated.
8. Valid queries are not noticeably slower under `spark_strict` (spot-check a few large queries).

Error messages do not need to match Spark's error classes, but should be clear, for example
`Spark SQL does not support DuckDB's '::' cast syntax` or `[UNRESOLVED_ROUTINE] Cannot resolve function
'list_transform'`. Use `ParserException` for syntax and `BinderException` for names.

## 3. Environment setup (fresh cloud session)

The session needs: a C++ toolchain, `cmake`, `ninja`, Python 3.11+, Java 17+ (for Spark).

```bash
# DuckDB v2.0.0 sources (the extension builds against them)
git clone --depth 1 --branch v2.0.0 https://github.com/duckdb/duckdb.git ../duckdb

# configure: only the loadable extension and the DuckDB core it needs (same recipe as CI)
cmake -G Ninja -S ../duckdb -B ../duckdb-build -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SHELL=OFF -DBUILD_UNITTESTS=OFF -DENABLE_SANITIZER=FALSE \
  -DOVERRIDE_GIT_DESCRIBE=v2.0.0 \
  -DDUCKDB_EXTENSION_CONFIGS="$PWD/extension_config.cmake"

# first build compiles ~340 objects (20-40 minutes on 4 cores); later builds only recompile src/
ninja -C ../duckdb-build spark_dialect_loadable_extension
export SPARK_EXTENSION=$PWD/../duckdb-build/extension/spark_dialect/spark_dialect.duckdb_extension

python3 -m venv ../venv
../venv/bin/pip install "duckdb==2.0.0" "pyspark==3.5.*" pandas pytz
```

Run the build in the background (it is long) and work on the plan meanwhile. Rebuild after every C++
change with the `ninja` command above (about 30-60 seconds).

Tests:

```bash
../venv/bin/python test/make_golden.py cases/non_spark.sql   # expected results from Spark; only for new/changed case files
../venv/bin/python test/run_duckdb.py -v                      # compare (add --dialect spark_strict after step 5.1)
../venv/bin/python scripts/triage.py test/results/failures.txt  # group failures by cause
```

Quick manual checks: `SPARK_DIALECT_DEBUG_TOKENS=1` prints the token stream before and after the
Spark rewrites (stderr). With the Python package:

```python
import duckdb
con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute(f"LOAD '{path}'")
con.execute("SET current_dialect = 'spark_strict'")
con.sql("SELECT [1, 2]")   # should raise
```

Note: a whole SQL string is tokenized with the dialect active at the time it is parsed, so
`LOAD ...; SET current_dialect = ...; SELECT ...` in one string does not work; run them separately.

## 4. How the extension works (what you need to know)

| Layer | File | Key functions |
|---|---|---|
| Entry point | `src/spark_dialect_extension.cpp` | `LoadInternal` registers the function library and `DialectExtension::Register(config, make_shared_ptr<SparkDialect>())` |
| Dialect class | `src/include/spark_dialect.hpp` | `SparkDialect` (overrides `ApplyGrammarChanges`, `CreateTokenizer`), `SparkTokenizer` |
| Tokenizer and token rewrites | `src/spark_tokenizer.cpp` | `SparkTokenizer::TokenizeInput`, `SparkRewriteTokens` → `RewriteLiterals`, `RewriteStatement`, `RewriteQueryClauses`, `RewritePivotAggregates`, `RewriteWindowColumn`, `RenameWindowCalls`; helpers `IsWord`, `WordAt`, `OpAt`, `Lex`, `Replace`, `ClauseEnd`, `SkipParens` |
| Grammar | `src/spark_grammar.cpp` | `SparkDialect::ApplyGrammarChanges` (rule table `rules`), expression ladder `Sp*` rules, hooks `TransformFunctionHook` / `RewriteFunction` / `FinalizeFunctionHookResult`, `TransformCastHook`, `TransformCaseHook`, `FinalizeOrderByHook` |
| Function routing | `src/spark_functions.cpp` | `IsSparkOverride` / `AddSparkOverride` (names routed to schema `system.spark`), dispatchers registered in `RegisterSparkFunctions` |
| Function library | `src/spark_macros.cpp` (SQL macros), `spark_library.cpp`, `spark_datetime.cpp`, `spark_formats.cpp`, `spark_extras.cpp` | C++ kernels and dispatchers |
| Tests | `test/harness.py`, `test/make_golden.py`, `test/run_duckdb.py`, `test/cases/*.sql`, `test/golden/*.pkl` | case-file directives are documented at the top of `test/harness.py` |

How a query flows:
1. `SparkTokenizer` applies Spark's lexical rules, then `SparkRewriteTokens` rewrites the token stream
   (pure syntax sugar such as `SORT BY` becoming `ORDER BY`, and `LATERAL VIEW`, `INSERT OVERWRITE`, …).
2. DuckDB's PEG grammar, modified by `ApplyGrammarChanges`, parses the tokens. `Expression` is replaced
   by Spark's precedence ladder (`SpExpression` …). Atoms still come from DuckDB's `SingleExpression`,
   which is where most DuckDB-only syntax leaks in.
3. Hooks rewrite function calls (`RewriteFunction`): names in `IsSparkOverride` go to `system.spark.<name>`
   and everything else resolves against DuckDB's catalog. **That fallback is the function leak.**
4. Dispatchers in the `spark` schema bind to Spark semantics once argument types are known.

## 5. Implementation steps

Work in this order; build and run the full suite after each step.

### 5.1 Register `spark_strict` and plumb a `strict` flag

- `SparkDialect(bool strict)`: name `"spark"` or `"spark_strict"`, both with
  `DialectCompatibilityMode::SPARK`. Keep `strict` as a member.
- In `LoadInternal`, register both instances.
- Pass `strict` to the tokenizer (`SparkTokenizer(keyword_helper, strict)`) and use it in
  `ApplyGrammarChanges`.
- Transform functions are plain function pointers wrapped by `ExprProcess<FN>` templates. Where a
  transform needs the flag, register a lambda instead (`grammar_transform_process_function_t` is a
  `std::function`), or register a separate strict transform (for example
  `ExprProcess<TransformFunctionHookStrict>`). Do not use globals: both dialects can be active in
  different connections of the same process.
- Harness: add `--dialect` to `test/run_duckdb.py` (default `spark`), passed to
  `SET current_dialect`. With `spark_strict`, count `LENIENT` as `FAIL`. Write `results/summary.txt`
  per dialect (for example `summary_spark_strict.txt`) and let `scripts/coverage_report.py` report both.
- Checkpoint: the whole suite passes under both dialects (nothing is strict yet).

### 5.2 Restrict functions to Spark's built-ins

- Embed Spark 3.5's function list (`test/spark35_functions.txt`, 438 names, generated from Spark) as a
  `static const char *SPARK_FUNCTIONS[]` table (a small generator script in `scripts/` is preferable to
  hand copying; commit the generated header like `src/include/spark_keywords.hpp`).
- In strict mode, in `RewriteFunction` (and the window path in `FinalizeFunctionHookResult`): an
  unqualified function name that is not in the Spark list raises
  `BinderException("[UNRESOLVED_ROUTINE] Cannot resolve function `%s`")`.
  - Careful: some Spark functions are implemented by parse-time rewrites before the override check
    (`named_struct`, `struct`, `stack`, `json_tuple`, `inline`, `explode*`, `posexplode*`, `transform`,
    `filter`, `zip_with`, `map_filter`, `transform_keys`, `transform_values`, `map_zip_with`,
    `arrays_zip`, `elt`, `decode`, `concat_ws`, `format_string`, `printf`, `reflect`, `java_method`,
    `if`, `sum`, `avg`, `mean`, `array_sort`). Do the check **before** those rewrites, on the name
    the user wrote.
  - Names that grammar rules turn into calls (`SpKeywordFunction`, `SpCastFunction`, `SpSubstrFrom`,
    `SpPositionIn`, `SpArrayCtor`, `SpMapCtor`, `SpStructCtor`, `SpExistsFunction`) are Spark names
    already, but check `TransformKeywordFunction` routes through the same validation.
  - DuckDB's special syntax rules (`SpecialFunctionExpression`: `TRIM`, `SUBSTRING`, `EXTRACT`,
    `POSITION`, `OVERLAY`, `COALESCE`, `NULLIF`, also `TRY(...)`, `COLUMNS(...)`, `UNPACK(...)`,
    `LAMBDA`, `ROW(...)`) reach `TransformFunctionHook` via the hook on `SpecialFunctionExpression`.
    `TRY`, `COLUMNS`, `UNPACK`, `LAMBDA` and `ROW` are not Spark: remove those alternatives from
    `DuckSpecialFunctionExpression` in strict mode (step 5.3).
  - Internal calls created by the extension itself are schema-qualified (`SparkCall(...)` →
    `system.spark.*`, macro bodies use `system.spark.`), so they are not affected by the check, which
    only looks at unqualified names. Keep it that way.
- **Also fix in both dialects:** internal helpers such as `add`, `cast_value`, `as_string`,
  `subscript`, `concat_ws_impl` and `*_impl` are registered through `AddSparkOverride` and are callable
  by users. Make routing by name (`IsSparkOverride`) apply only to names that are Spark functions, so
  helpers are only reachable through qualified internal calls. In `spark` (lenient) mode a non-Spark
  name then falls back to DuckDB's catalog as before. (`add` happens to also exist in DuckDB, which is
  fine for lenient mode.)
- Table functions in `FROM` (`FROM range(3)`, `FROM read_csv(...)`, `FROM generate_series(...)`):
  Spark 3.5 table-valued functions are `range`, `explode`, `explode_outer`, `inline`, `inline_outer`,
  `posexplode`, `posexplode_outer`, `stack`, `json_tuple` and `sql_keywords`. In strict mode, reject
  other table functions. Hook DuckDB's `TableFunction` rule (see `select.gram`: `InnerTableRef <-
  ValuesRef / TableFunction / ...`) the same way as `FunctionExpression`: copy the rule definition into
  a `Duck...` rule with the original transform, and replace it with a hook that validates the name.

### 5.3 Remove DuckDB-only grammar in strict mode

In `ApplyGrammarChanges`, when `strict`, use `ParsedGrammar::RemoveChoice(rule, cursor)` where the
cursor matches the alternative to drop:

```cpp
auto ref = [](const char *name) {
    return [name](const PEGExpression &e) {
        return e.type == PEGExpression::Type::REFERENCE && e.text.GetString() == name;
    };
};
g.RemoveChoice("SingleExpression", ref("ListExpression"));
```

(`RemoveChoice` throws if the rule is not a choice or the cursor finds nothing; see DuckDB's
`test/api/test_grammar_extension.cpp` "Grammar choices can be removed".) The rule texts are in DuckDB's
`src/parser/peg/grammar/statements/*.gram` at tag v2.0.0. Read each rule before removing anything.

| DuckDB-only construct | Where it lives (v2.0.0 grammar) | Strict-mode change |
|---|---|---|
| `[1, 2]` list literals | `SingleExpression` → `ListExpression` | remove (keep `ARRAY(...)`, our `SpArrayCtor`) |
| `{'a': 1}` struct literals | `SingleExpression` → `StructExpression` | remove |
| `MAP {...}` | `SingleExpression` → `MapExpression` | remove (keep `MAP(...)`, our `SpMapCtor`) |
| `#1` positional | `SingleExpression` → `PositionalExpression` | remove |
| `x::type` | our `SpIndirection` → `SpCast` | do not add `SpCast` in strict mode |
| `x[1:2]` slices | our `SpSubscript` is `[expr]` only; check that DuckDB's `SliceExpression` is unreachable | test it |
| `'abc'.upper()` method calls | our `SpField` is `'.' ColLabel`; check that `'abc'.upper()` fails | test it |
| `lambda x: ...` | `SpecialFunctionExpression` → `LambdaExpression` | remove from `DuckSpecialFunctionExpression` |
| `TRY(...)`, `COLUMNS(...)`, `UNPACK(...)`, `ROW(...)` | `SpecialFunctionExpression` | remove (careful: the tokenizer turns `STRUCT(` into `ROW(` inside `INSERT ... VALUES`; in strict mode emit `struct_pack` or a Spark-legal form instead, or skip that rewrite and implement the positional insert another way) |
| `* EXCLUDE`, `* EXCEPT`, `* REPLACE`, `* RENAME` | `StarExpression <- StarQualifierList? '*' ExcludeList? ReplaceList? RenameList?` | `ReplaceRule("StarExpression <- StarQualifierList? '*'", ...)` with the original transform, **or** keep the rule and reject in a hook if any list is present (safer: the generated transform expects the child layout) |
| `QUALIFY` | `SimpleSelect <- ... QualifyClause? ...` | reject in a hook on `QualifyClause` (raise `ParserException`) rather than editing `SimpleSelect`, whose generated transform depends on child positions |
| `ASOF`, `POSITIONAL`, `NEAREST`, `JOIN BY` joins | `RegularJoinClause <- Asof? JoinType? 'JOIN' ...`, `JoinPrefix`, `JoinClause` | remove `PositionalJoinPrefix` from `JoinPrefix`, `NearestJoinClause` and `JoinByClause` from `JoinClause`; for `ASOF` hook the `Asof` rule and raise |
| `USING SAMPLE`, `TABLESAMPLE reservoir(...)` | `SelectSampleClause`, `SampleClause` | reject DuckDB sample methods; keep Spark's `TABLESAMPLE (n PERCENT / n ROWS / BUCKET x OUT OF y)` |
| `LIMIT 10%` | `LimitClause` | reject percentages |
| DuckDB-only statements | `Statement <- ...` (list below) | `RemoveChoice` each of `ExternalResourceStatement`, `ExtensionRepositoryStatement`, `PragmaStatement`, `CallStatement`, `CopyStatement`, `PrepareStatement`, `ExecuteStatement`, `TransactionStatement`, `AttachStatement`, `DetachStatement`, `CheckpointStatement`, `VacuumStatement`, `ExportStatement`, `ImportStatement`, `DeallocateStatement`, `LoadStatement`, `InstallStatement`, `UpdateExtensionsStatement`, `ConnectStatement`, `DisconnectStatement`. Keep `SELECT`, `INSERT`, `CREATE`, `DROP`, `ALTER`, `DESCRIBE`, `SHOW`, `EXPLAIN`, `USE`, `SET`, `RESET`, `TRUNCATE`, `DELETE`, `UPDATE`, `MERGE`, `COMMENT`, `ANALYZE`. Our token rewrites already map Spark's `CACHE`, `REFRESH` and so on |
| `SUMMARIZE`, DuckDB `PIVOT ... ON ... USING` and `UNPIVOT ... ON` statements, `FROM` before `SELECT` when not Spark-legal | `SelectStatementType` (`PivotStatement`, `UnpivotStatement`, `DescribeStatement`) | note: `FROM t SELECT a` **is** valid Spark (Hive syntax) and must keep working; Spark-style `PIVOT (...)` after a table reference must keep working. Remove only DuckDB's statement-level `PIVOT t ON ...` / `UNPIVOT t ON ...` |
| `CREATE MACRO`, `CREATE SEQUENCE`, `CREATE TYPE`, `CREATE SECRET`, `CREATE INDEX` (Spark has none), `CREATE TRIGGER` | `CreateStatementVariation` | remove those alternatives |
| `INSERT OR REPLACE`, `ON CONFLICT`, `RETURNING`, `INSERT BY NAME` | `InsertStatement` | reject in a hook (the generated transform depends on positions; do not edit the rule text) |
| DuckDB operators: `//`, `**`, `^@`, `~~`, `!~~`, `~`/`~*` regex, `GLOB`, `SIMILAR TO`, `->>` / `->` JSON | DuckDB `LikeVariations`, and operator tokens our ladder may pass through as `OperatorLiteral` | our ladder handles only Spark operators; test each and reject leftovers in the tokenizer when `strict` (operator tokens not in Spark's set) |
| `E'...'` escape strings, `$$...$$` dollar quoting | DuckDB tokenizer | Spark has neither: in strict mode treat `$` quoting as an error in `PreScan`/`SparkTokenizer::TokenizeInput`; `E'x'` is an identifier followed by a string in Spark (an error in most positions) |

General rules learned while building the extension (they apply to every step):
- Prefer **hooks that validate and raise** over editing DuckDB rule texts. Many DuckDB rules have
  generated transforms that read children by position (`list_pr.GetChild(3)`), so changing a rule's
  shape breaks them in ways that only show up at run time. `RemoveChoice` on a pure choice rule is
  safe; editing a sequence is not.
- The hook pattern used in `spark_grammar.cpp` (see `FunctionExpression`, `CastExpression`,
  `CaseExpression`, `OrderByExpression`, `SpecialFunctionExpression`): `g.GetRule(name)`, then
  `g.AddRule("Duck<Name> <- <exact original rule text>", rule->transform_process)`, then
  `g.ReplaceRule("<Name> <- Duck<Name>", hook)`. The original text must match the v2.0.0 `.gram`
  file exactly.

### 5.4 Restrict types

Spark 3.5 type names: `BOOLEAN`, `TINYINT`/`BYTE`, `SMALLINT`/`SHORT`, `INT`/`INTEGER`, `BIGINT`/`LONG`,
`FLOAT`/`REAL`, `DOUBLE`, `DECIMAL`/`DEC`/`NUMERIC` (with optional `(p, s)`), `STRING`,
`VARCHAR(n)`, `CHAR(n)`, `BINARY`, `DATE`, `TIMESTAMP`, `TIMESTAMP_NTZ`, `TIMESTAMP_LTZ`, `INTERVAL`
(with qualifiers), `ARRAY<>`, `MAP<,>`, `STRUCT<>`, `VOID`. In strict mode, hook `SimpleType` (or
`QualifiedTypeName`) and reject other names (`HUGEINT`, `UHUGEINT`, `UBIGINT`, `UINTEGER`, `UUID`,
`JSON`, `BLOB`, `BIT`, `VARINT`, `TIME`, `TIMETZ`, `TIMESTAMPTZ`, `UNION`, `ENUM`, `LIST`, DuckDB
`STRUCT(...)`/`MAP(...)`/`T[]` forms, …).

Exception: the extension's own `system.spark.spark_char_N` types (generated by the tokenizer for
`CHAR(n)` columns in `CREATE TABLE`) are qualified; allow qualified `system.spark.*` type names.
`ArrayBounds` (`INT[]`, `INT ARRAY`) are DuckDB-only: reject in strict mode.

### 5.5 Restrict statements relaxed by our rewrites

- `CREATE TABLE t AS SELECT ...` without `USING` / `STORED AS`: Spark 3.5 without Hive support raises
  `[NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT]`. In strict mode, raise the same in
  `RewriteStatement` (`spark_tokenizer.cpp`) when a `CREATE TABLE ... AS` has no `USING` clause.
  Check the golden file: `test/cases/ddl_dml.sql` creates tables with `USING parquet`; plain
  `CREATE TABLE t (cols)` without `USING` is also Hive-only in Spark 3.5. Verify against Spark before
  rejecting it.
- `SET` of DuckDB settings (anything not starting with `spark.` / `hive.` and not `TIME ZONE`): Spark
  accepts any `key=value` string for `SET` (it just stores it), so do **not** pass it to DuckDB's `SET`
  in strict mode. Return the `key`/`value` row like the other Spark settings. Lenient mode keeps
  passing DuckDB settings through (useful).

### 5.6 Tests and CI

1. Create `test/cases/non_spark.sql` with the queries from section 6. Generate its golden file with
   `test/make_golden.py cases/non_spark.sql` and check that **Spark rejects every case**
   (`make_golden.py` prints "N error in Spark"; all must be errors). Drop or fix any case Spark accepts.
   The probe found that `GROUP BY ALL`, `ORDER BY ALL`, `FROM t SELECT ...` and `LIMIT ALL` are
   valid in Spark 3.5. Keep such cases out of this file.
2. Add a small `test/cases/strict_valid.sql` with tricky **valid** Spark queries that a too-aggressive
   strict mode might break: `FROM t SELECT a`, `GROUP BY ALL`, `TABLESAMPLE (10 PERCENT)`,
   `PIVOT (...)` clause, `ARRAY(1,2)[0]`, `MAP('a',1)['a']`, `STRUCT(1 AS x).x`, `x -> x + 1`,
   `(x, i) -> x + i`, `TRY_CAST`, `CAST(x AS STRUCT<a: INT>)`, `IF(...)`, `` `quoted` `` identifiers,
   `LATERAL VIEW`, `SORT BY`, `CLUSTER BY`, `DISTRIBUTE BY`, `INSERT OVERWRITE`, `CACHE TABLE`,
   `SET spark.sql.shuffle.partitions=4`, `x <=> y`, `a DIV b`, `!true`, `1L`, `1.5BD`, `X'1F'`,
   `"double quoted string"`, `r'raw\n'`, `INTERVAL '1' DAY`, `INTERVAL 1 DAY 2 HOURS`,
   `window(ts, '5 minutes')`, `session_window`.
3. `test/run_duckdb.py --dialect spark_strict`: `LENIENT` counts as `FAIL`.
4. CI (`.github/workflows/ci.yml`): add `strategy.matrix.dialect: [spark, spark_strict]` to the test
   steps. Build the extension once: either split into a `build` job that uploads the extension and a
   `test` job (matrix) that downloads it, or run both dialects sequentially in one job. Record the
   golden files once per run, not per matrix entry, if you split jobs.
5. Update `README.md` (usage, what strict rejects, limitations) and regenerate `COVERAGE.md`.

## 6. The `non_spark.sql` corpus

### 6.1 Already probed

These were run in Spark 3.5.9 (all rejected) and in the `spark` dialect (all accepted):

```sql
SELECT [1, 2];
SELECT {'a': 1};
SELECT MAP {'k': 1};
SELECT '1'::INT;
SELECT transform(array(1), lambda x: x + 1);
SELECT list_transform([1], x -> x);
SELECT strftime(DATE '2024-01-01', '%Y');
SELECT COLUMNS(*) FROM VALUES (1, 2) AS t(a, b);
SELECT * EXCLUDE (a) FROM VALUES (1, 2) AS t(a, b);
SELECT * EXCEPT (a) FROM VALUES (1, 2) AS t(a, b);
SELECT a FROM VALUES (1), (2) AS t(a) QUALIFY row_number() OVER (ORDER BY a) = 1;
SELECT #1 FROM VALUES (1) AS t(a);
SELECT * FROM VALUES (1) AS x(a) ASOF JOIN VALUES (1) AS y(b) ON a >= b;
SELECT * FROM VALUES (1) AS x(a) POSITIONAL JOIN VALUES (2) AS y(b);
SELECT CAST(1 AS HUGEINT);
SELECT CAST('a5cf6c42-0c85-418f-af6c-3e4e5b1328f2' AS UUID);
SELECT TRY(1);
PRAGMA version;
SUMMARIZE SELECT 1 AS a;
CREATE MACRO m(x) AS x + 1;
CREATE TABLE ctas_probe AS SELECT 1 AS a;
SELECT add(1, 2);
SELECT cast_value(1, CAST(NULL AS STRING));
```

### 6.2 To add (verify each one fails in Spark with `make_golden.py`)

```sql
SELECT * REPLACE (a + 1 AS a) FROM VALUES (1) AS t(a);
SELECT [1, 2, 3][1:2];
SELECT 'abc'.upper();
SELECT 7 // 2;
SELECT 2 ** 3;
SELECT 'abc' ^@ 'a';
SELECT 'abc' ~ 'b';
SELECT 'abc' GLOB 'a*';
SELECT 'abc' SIMILAR TO 'a.c';
SELECT '{"a":1}'::JSON ->> 'a';
SELECT E'a\nb';
SELECT $$dollar$$;
SELECT CAST(1 AS INT[]);
SELECT CAST(NULL AS STRUCT(a INT));
SELECT CAST(NULL AS MAP(VARCHAR, INT));
SELECT CAST(1 AS UBIGINT);
SELECT CAST('{}' AS JSON);
SELECT CAST(TIME '10:00:00' AS TIME);
SELECT CAST(TIMESTAMP '2024-01-01' AS TIMESTAMPTZ);
SELECT list_value(1, 2);
SELECT regexp_full_match('a', 'a');
SELECT strptime('2024', '%Y');
SELECT epoch_ms(0);
SELECT arg_max(a, a) FROM VALUES (1) AS t(a);
SELECT string_agg(a, ',') FROM VALUES ('x') AS t(a);
SELECT list(a) FROM VALUES (1) AS t(a);
SELECT count_star();
SELECT unnest([1, 2]);
SELECT * FROM generate_series(1, 3);
SELECT * FROM read_csv('x.csv');
SELECT * FROM VALUES (1) AS t(a) USING SAMPLE 10%;
SELECT * FROM VALUES (1) AS t(a) LIMIT 10%;
PIVOT (SELECT 1 AS a, 'x' AS b) ON b USING sum(a);
UNPIVOT (SELECT 1 AS a, 2 AS b) ON a, b INTO NAME k VALUE v;
INSTALL json;
LOAD json;
ATTACH ':memory:' AS other;
COPY (SELECT 1) TO 'out.csv';
EXPORT DATABASE 'dir';
CHECKPOINT;
VACUUM;
BEGIN TRANSACTION;
CALL pragma_version();
PREPARE p AS SELECT 1;
CREATE SEQUENCE s;
CREATE TYPE mood AS ENUM ('a', 'b');
CREATE TABLE ins_probe (a INT) USING parquet;
INSERT OR REPLACE INTO ins_probe VALUES (1);
INSERT INTO ins_probe VALUES (1) RETURNING a;
INSERT INTO ins_probe BY NAME SELECT 1 AS a;
SELECT concat_ws_impl(',', 'a');
SELECT subscript(array(1), 0);
SET threads = 2;
```

(`CREATE TABLE ins_probe` is valid; mark it `-- @setup` so it runs before the cases that use the table.)

Some of these may already fail in DuckDB too (for example `read_csv` of a missing file). That still
counts as "both error", but such a case does not prove strictness. Prefer cases that succeed under
`spark`: run the file under `--dialect spark` and keep the cases reported as `LENIENT`.

## 7. Pitfalls (learned the hard way on this codebase)

- **Parse result shapes** (`spark_grammar.cpp`): a named rule always yields a `ListParseResult`; a
  rule whose body is a choice yields a list with one `ChoiceParseResult` child (use `Alt(pr)`); `X*`
  is an `OptionalParseResult` wrapping a `RepeatParseResult` (use `RepeatItems`); `List(D)` items via
  `ListItems`. Every rule passed to `t.Transform<T>(...)` must have a transform process.
- **New grammar words** must be added to `UnreservedKeyword` (as `ApplyGrammarChanges` does) or they
  become reserved and break identifiers.
- **PEG ordered choice**: the first matching alternative wins; put longer forms first.
- **Tokenizer**: unquoted words are `KEYWORD` **or** `IDENTIFIER` tokens (use `IsWord`); a lone `.` is
  a `NUMBER_LITERAL` token (`IsOp` handles it); synthesize tokens with `Lex(...)` (upper-case words
  become keywords, others identifiers; two-character operators are recognized).
- **Dispatchers**: `Arg(input, i)` moves the child out of `input.children`; read types
  (`ArgType`, `DecimalOperandType`) before calling `Arg`.
- **Functions that can raise errors at run time must be registered with `RegisterFallible`**
  (`spark_dialect.hpp`); otherwise DuckDB turns the error into an `INTERNAL` error.
- **Macros** (`spark_macros.cpp`): unqualified names in bodies resolve to DuckDB built-ins; Spark
  functions are `" S "name(...)`. A macro must not call its own other-arity overloads (DuckDB sees
  recursion). Parameter names must not be keywords (`asc` failed). DuckDB lambda index parameters are
  0-based like Spark's. `CASE` in a macro cannot mix result types, even under a constant condition:
  use a C++ dispatcher.
- **Do not register global cast overrides** for nested types (a `STRUCT → STRUCT` override broke
  `explode`, struct access and more inside DuckDB).
- The Python client's `con.sql()` runs one statement; some rewrites produce two (`INSERT OVERWRITE`
  becomes `DELETE` + `INSERT`). The harness falls back to `execute()`.
- Never edit `test/golden/*.pkl` by hand; regenerate with `make_golden.py` (needs Spark).

## 8. Deliverables

- Code changes in `src/` (and `src/include/`), any generated header, a generator script under
  `scripts/`.
- `test/cases/non_spark.sql`, `test/cases/strict_valid.sql` with golden files, harness changes.
- CI matrix; README and COVERAGE updates.
- A short summary in the PR description: what strict mode rejects, results under both dialects
  (`TOTAL PASS/LENIENT/FAIL`), and anything left lenient on purpose, with reasons.
