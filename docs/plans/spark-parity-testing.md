# Plan: test coverage for full parity with Spark 3.5 (settings and type coercion)

This plan is a self-contained handoff. Read it fully before starting. It assumes no memory of earlier
sessions. Read `README.md` and `test/fuzz/README.md` first; they describe the code layers and the
existing differential tests.

## Implementation status (kept up to date for the agents that continue this plan)

The plan below is unchanged. `[x]` = done and verified (CI green on the branch, zero FAIL / XPASS in both dialects under the
default, ANSI, time zone and decimal configurations); `[ ]` = not done or only partly done, with the reason.
Regenerate the tables: `python scripts/xfail_summary.py` (per xfail id: `docs/plans/spark-parity/known_differences.md`),
`python scripts/coverage_report.py` (COVERAGE.md). Spark JVM runs need `pip install pyspark==3.5.*` and Java 17.

**Workstream A: harness**
- [x] 5.1 column types compared (`duck_type_to_spark`), 5.2 error phase, 5.3 `-- @exact`
- [x] 5.4 settings per file / per case / `@matrix` / `--config`
- [x] 5.5 expected failures (`test/xfail.toml`, per configuration, XPASS and stale entries fail the run; `--no-xfail`, `--results`)
- [x] 5.6 other items (unit tests `test/test_harness.py`, names gating by SQL text, error class gated in `spark_strict` for types / settings files)

**Workstream B: settings**
- [x] B1 inventory of all 478 keys, `check_inventory.py`, `scripts/gen_inventory.py`
- [x] B2 `SET` / `RESET` / `SET TIME ZONE` (`test/cases/settings/set_command.sql`, `time_zone_command.sql`), `session_behaviour.sql`
- [x] B3 one case file per semantic setting (66 files, witnesses verified in Spark)
- [x] B4 `interactions.sql`, `interactions_time.sql`
- [x] B5 DuckDB settings do not leak (`scripts/check_duckdb_leaks.py`, `test/settings/duckdb_inventory.toml`)
- [x] Emulated: `session.timeZone` (partly: zone names and `window()` are xfails), `mapKeyDedupPolicy`, `legacy.sizeOfNull`,
  `jsonGenerator.ignoreNullFields`, `function.concatBinaryAsString`, `function.eltOutputAsString`, `chunkBase64String.enabled`,
  `decimalOperations.allowPrecisionLoss`, `legacy.statisticalAggregate`, `legacy.negativeIndexInArrayInsert`,
  `legacy.followThreeValuedLogicInArrayExists`; `ansi.enabled` runtime errors only
- [ ] Tier 1 not emulated, moved to tier 3 with a note in the inventory: `caseSensitive`, `storeAssignmentPolicy`, `legacy.timeParserPolicy`
- [ ] Tier 2 not emulated (parser / binder / planner decisions), tier 3 with notes: `groupByOrdinal`, `orderByOrdinal`, `groupByAliases`,
  `crossJoin.enabled`, `legacy.setopsPrecedence.enabled`, `parser.escapedStringLiterals`, `parser.quotedRegexColumnNames`,
  `ansi.doubleQuotedIdentifiers`, `ansi.enforceReservedKeywords`, `ansi.relationPrecedence`, `variable.substitute`,
  `lateralColumnAlias.enableImplicitResolution`, `legacy.ctePrecedencePolicy`, `legacy.createHiveTableByDefault`, `charAsVarchar`,
  `legacy.nullInEmptyListBehavior`, `legacy.allowParameterlessCount`, `legacy.castComplexTypesToString.enabled`
- [ ] `timestampType` / `legacy.typeCoercion.datetimeToString.enabled` stay rejected, so their rows of workstream D do not run
- [ ] 9.5 generated column names are not implemented: the 15 cases of `test/cases/generated_names.sql` are xfails (`names-generated`)
- [ ] Views and cached tables capture no settings (`view-captures-settings` xfail)

**Workstream C: type matrices** (`scripts/gen_type_cases.py`, `test/fuzz/args_sweep.py --types`)
- [x] C1-C5, C7-C14 generated, recorded from Spark, run in both dialects
- [x] C6 generated (21,995 statements, one example per function, up to 4 slots, 22 types; numbers compared with tolerance),
  `test/golden/signatures.json.gz`, `scripts/check_int_parameters.py` (report only: its output is noisy)
- [ ] Burn-down of the `types-open-c1` ... `types-open-c14` ids (see known_differences.md for counts). Done so far: float with decimal is
  DOUBLE, bool to timestamp, `-9223372036854775808`, `COALESCE` / `array` / `CASE` / nested common types, `IN` lists and `BETWEEN`
  through Spark's implicit casts, string parameters of string / regexp / date functions, INT parameters truncate fractions.
  Second pass (branch `claude/charming-gates-4xdtsm`): exact decimal division (HALF_UP to Spark's result scale, checked
  against Python's `Decimal` on 21,000 random operand pairs and against live Spark on 300), decimal to integral casts wrap
  like `Decimal.toLong`, `CAST(<integral> AS BINARY)` is the big-endian bytes, `median` / `percentile` are DOUBLE,
  `round` / `bround` of NULL and strings are DOUBLE, `skewness` / `kurtosis` NULL for zero variance, boolean equality with
  FLOAT / DOUBLE / DECIMAL, `IN` over structs compares by position when the field types agree and needs the same field
  names when a cast is needed, mixed kinds raise `DATA_DIFF_TYPES` (they crashed the process with SIGSEGV or raised
  INTERNAL errors before), `array_insert` past Spark's array size limit raises `COLLECTION_SIZE_LIMIT_EXCEEDED` (a huge
  position exhausted memory before: constant folding built the padding), string arguments of the INT parameters of
  `regexp_extract` / `regexp_extract_all` / `regexp_instr` / `sha2`, DOUBLE parameters of the math functions (`sqrt`,
  `atan2`, `pow`, ...) and of `skewness` / `kurtosis` take numbers and strings only (booleans, dates, arrays, ... are
  Spark's `UNEXPECTED_INPUT_TYPE`; the math functions used to accept them). Against the expected failures this branch
  started from, no case fails and these pass now: default 468 (`spark`) / 1,232 (`spark_strict`), ANSI 270 / 648, time
  zone 123 / 259, `allowPrecisionLoss=false` 17 / 17; their entries left `test/xfail.toml`.
  `scripts/xfail_from_failures.py --write` replaced no entries before (it compared ids with file names); fixed.
  Open: JOIN `USING` / `NATURAL` key coercion (DuckDB's binder casts the keys strictly: an unconvertible string is an error,
  Spark's cast gives NULL), `IN (subquery)` with a string and a number (same cause), mixed-kind errors of the other
  operators, and the strict-dialect leniencies (most of the `spark_strict` expected failures: Spark rejects,
  `spark_strict` accepts)
- [ ] 9.3 / 9.4: `types-ntz` and `types-interval` stay (one DuckDB type each); no TIMESTAMP to TIMESTAMPTZ remap was evaluated

**Workstream D: configurations**
- [x] ANSI (all of types except C6, plus operators, literals_types, functions_examples, queries, ddl_dml), time zone
  `America/Los_Angeles` (C1, C2, C4, C10, C12, functions_examples), `allowPrecisionLoss=false` (C3, C8): recorded, run, xfail entries
- [ ] `signatures.json.gz` recorded a second time under ANSI; `caseSensitive`, `timestampType`, `datetimeToString` rows (settings rejected)
- [ ] ANSI implicit-cast rules are not implemented: shared id `ansi-implicit-cast`

**Workstream E: Spark corpus and fuzzers**
- [x] 54 Spark inputs promoted to `test/cases/spark_corpus/` (`scripts/promote_spark_corpus.py`, NOTICE), goldens from live Spark
- [x] `gen.py` new column types and mixed kinds, `differential.py --conf-fuzz` / `--conf`, `compare.py` compares types and phase
  (report only: `REPORT_ONLY` kinds `type_mismatch`, `phase_mismatch`; promote them to failing once the xfail ids are burned down)
- [ ] `spark_corpus.py run` still has its own `SKIP` list (the promoted files are gated through `test/cases/` instead)
- [ ] `differential.py --conf-fuzz --budget 20000` was not run to completion; it excludes `ansi.enabled`

**CI and documentation**
- [x] CI sharded by case group (`scripts/case_group.py`), ANSI configuration on push, unit job (tests, inventory, generated files up to date),
  nightly workflow with the other configurations, `--conf-fuzz`, signatures re-recording
- [x] `scripts/coverage_report.py` settings table and per-matrix tables, `COVERAGE.md`, README testing section and limitations
- [ ] COVERAGE.md is generated locally (CI runs files by group, which writes no `summary.txt`)
- [ ] TPC-H benchmark not re-run (the `tpch` extension is downloaded from `extensions.duckdb.org`, which the cloud
  sessions' network policy blocks; CI has network access); other smoke benchmarks pass `bench/baseline.json`

**Definition of done (section 10)**
1. [x] `check_inventory.py` passes
2. [x] every matrix generated, recorded and run in both dialects under the default configuration and each configuration that runs
3. [x] rows, column types and error phase compared in every case file
4. [ ] zero FAIL / XPASS holds, but the remaining XFAIL ids are not only `conf-rejected`, `types-ntz`, `types-interval`: open ids remain (`types-open-c*`, `corpus-open`, `ansi-implicit-cast`, `names-generated`, `view-captures-settings`, `phase-*`, ...)
5. [x] `SET` of a semantic setting is emulated or rejected, never silently ignored
6. [ ] `args_sweep.py --types` is recorded; `differential.py --conf-fuzz` ran only with small budgets (known differences cover the fixed smoke seeds)
7. [x] README and COVERAGE.md describe the state
8. [x] benchmarks within `bench/baseline.json` (TPC-H not re-run offline)

## 1. Goal

Every observable behaviour of Spark SQL 3.5 that this extension claims to reproduce is checked against a
live Spark 3.5, for **every Spark SQL setting that can change a result** and for **every implicit and
explicit type conversion**. "Observable" means, for a statement: whether it is accepted, the rows, the
**column types**, and whether an error is raised at analysis time or at run time.

The work is test-first. Each workstream adds tests that are compared with Spark, records what fails
today, and then the failures are fixed or explicitly listed. Nothing is hidden: a difference is either
fixed, or tracked as an expected failure with an identifier, or listed in
`test/fuzz/known_differences.toml` with a reason.

### What exists today (measured in the repository)

- 1,721 differential cases in `test/cases/`, all passing in both dialects. Fuzzers in `test/fuzz/`.
- One setting is tested: `spark.sql.ansi.enabled` (one file, 112 cases, runtime errors only).
- The harness compares **rows only**. `make_golden.py` stores Spark's schema and `engines.py` returns
  DuckDB's column types, but `run_duckdb.py` never compares them. Integers, decimals and floats are all
  normalized to "number" and compared with 1e-9 tolerance, so `INT` vs `BIGINT` vs `DOUBLE` results are
  indistinguishable unless a case calls `typeof()` (58 lines in the case files do).
- When Spark raises an error, any DuckDB error is a PASS ("both error"), whatever its cause.
- `test/fuzz/spark_corpus.py` skips Spark's own `typeCoercion/`, `ansi/`, `nonansi/` and `timestampNTZ/`
  test inputs (the `SKIP` tuple).

### Gaps that reading the code predicts (not yet run: confirm them in phase 0)

The extension could not be built in the planning session, so these come from the source, not from a run.

1. `SET <spark key> = <value>` is accepted for every key and ignored for all but
   `spark.sql.ansi.enabled` and `spark.sql.session.timeZone` (`src/spark_tokenizer.cpp`, "SET / RESET").
   A user who sets `spark.sql.mapKeyDedupPolicy=LAST_WIN` silently gets default behaviour.
2. The value is not stored: `SET k=v` then `SET k` returns `<undefined>` where Spark returns `v`.
   `RESET <other key>` does nothing.
3. Keys are matched case-insensitively (`CIEquals`); Spark's keys are case-sensitive.
4. `TIMESTAMP_NTZ` and `TIMESTAMP_LTZ` both map to DuckDB `TIMESTAMP`, and `typeof` returns `timestamp`
   for both and `interval` for every interval type (`SparkTypeName` in `src/spark_functions.cpp`).
5. ANSI mode changes implicit typing in Spark (`'1' + 1` is `bigint`, not `double`; `coalesce('1', 2)` is
   `bigint`, not `string`). The README says this is not emulated.

## 2. Inputs prepared for this plan

These files were produced with Spark 3.5.9 and are committed with the plan.

| file | content |
|---|---|
| `test/spark35_sqlconf.json` | all 478 SQL configuration entries of Spark 3.5.9 (209 public, 269 internal): key, default, doc, version. Written by `scripts/gen_spark_confs.py` |
| `docs/plans/spark-parity/conf_witnesses.json` | 92 witnesses for 87 settings. A witness is a setting, a value and statements whose Spark result was recorded with the default and with the changed value. 68 settings have a witness whose result differs (`differs: true`) |
| `scripts/probe_conf_witnesses.py` | re-runs the witnesses in Spark and refreshes the recorded results; use it when adding witnesses |

Appendix A lists the witnesses. Appendix B explains how the 478 settings fall into classes.

## 3. Rules for the implementing session

1. Spark 3.5 is the only reference. Never write an expected value by hand and never edit a golden file.
   Expected results come from `test/make_golden.py` or from the fuzzers' live Spark session.
2. Never make a test pass by weakening it: no `@skip`, no removed case, no looser comparison, no
   generator change that avoids a failing input. A failure you cannot fix now gets `-- @xfail <id>`
   (section 5.5).
3. Run both dialects after every change: `python test/run_duckdb.py` and
   `python test/run_duckdb.py --dialect spark_strict`. The existing 1,721 cases must stay green.
4. The default dialect must not change: `python test/fuzz/isolation.py` stays green.
5. Performance must not regress: after any change to a bind function in `src/spark_functions.cpp` run
   the smoke benchmarks that CI runs (see `.github/workflows/ci.yml`) and compare with
   `bench/baseline.json`. A setting must be read at bind time, not per row.
6. Commit per step (one generator plus its cases, or one fix plus its regression case), in the style of
   the existing history (`fix: ...; golden cases from Spark`). Push to the working branch.
7. Section 9 holds the decisions that were open. They have defaults: follow them without asking. Stop and
   ask the user only when something not covered there would change user-visible behaviour of the
   default DuckDB dialect or remove an existing feature.

### Environment

- Python: the system `pip` cannot build the `pyspark` wheel on this image. Use a virtual environment:
  `python3 -m venv .venv && .venv/bin/pip install "pyspark==3.5.*" duckdb==2.0.0 pandas pytz`. Java 17+
  is needed.
- Build: see "Build" in `README.md` and the `build` job in `.github/workflows/ci.yml` (DuckDB v2.0.0,
  `-DOVERRIDE_GIT_DESCRIBE=v2.0.0`, target `spark_dialect_loadable_extension`). A first build takes
  about an hour on 4 cores; use `ccache`. Point the tests at the result with `SPARK_EXTENSION=<path>`
  or copy it to `build/spark_dialect.duckdb_extension`.
- Throughput: Spark answers about 10 to 25 small queries per second. Size every generator with that in
  mind (section 8 has the budgets).

## 4. Order of work

| phase | content | exit check |
|---|---|---|
| 0 | Build, run the existing suite, measure the baseline (5.1 to 5.3 switched on in report-only mode; the witnesses of Appendix A run in DuckDB) | `docs/plans/spark-parity/baseline.md` with the counts |
| 1 | Workstream A: harness (types, error phase, exact numbers, per-case settings, xfail) | existing cases pass or carry `@xfail`; unit tests of the harness pass |
| 2 | Workstream B, steps B1 to B3: settings inventory, `SET` / `RESET` command, one file per setting | every key classified; every semantic key has cases |
| 3 | Workstream C: type conversion matrices, non-ANSI | all matrices generated, golden recorded, failures triaged |
| 4 | Workstream D: the same matrices and the existing corpus under ANSI and under the other high-impact settings | as phase 3 for each configuration |
| 5 | Workstream E: Spark's own test inputs, fuzzers with random settings and random types | corpus files promoted; fuzz runs clean or known |
| 6 | Burn down the `@xfail` list; update `README.md`, `COVERAGE.md`, CI | section 10 |

Phases 2 and 3 are independent after phase 1. Fix bugs as they are found when the fix is local
(a kernel, a bind function, a cast). Leave larger features (a new emulated setting, ANSI implicit casts,
a distinct `TIMESTAMP_NTZ`) as `@xfail` until phase 6, then do them in the order of section 9.2.

## 5. Workstream A: harness

All changes are in `test/harness.py`, `test/engines.py`, `test/make_golden.py`, `test/run_duckdb.py`.
Add `test/test_harness.py` (plain `pytest`, no engines) for the new pure functions.

### 5.1 Compare column types

- Spark side: `df.schema.simpleString()` is already stored as `schema`. Store the list of column types
  as well (`[f.dataType.simpleString() for f in df.schema.fields]`), which avoids parsing names that
  contain `:` or `,`.
- DuckDB side: `rel.types` is already returned in `Result.extra["types"]`. Add
  `harness.duck_type_to_spark(type)` that maps a DuckDB type to Spark's `simpleString` form: `INTEGER` to
  `int`, `VARCHAR` to `string`, `BLOB` to `binary`, `DECIMAL(p,s)` to `decimal(p,s)`, `T[]` to
  `array<t>`, `MAP(K, V)` to `map<k,v>`, `STRUCT(a T, ...)` to `struct<a:t,...>`, `HUGEINT` to `bigint`
  only where the extension documents that (sums), and so on. Mirror `SparkTypeName` in
  `src/spark_functions.cpp`, but map from the Python type object (`dtype.id`, `dtype.children`), not from
  strings.
- A case fails when the types differ. The comparison is on by default for every file.
- Representation limits must be visible, not silently equal. Do not map DuckDB `TIMESTAMP` to "either
  timestamp or timestamp_ntz" and do not map `INTERVAL` to "any interval type". Let those cases fail and
  mark them `@xfail types-ntz` / `@xfail types-interval` until sections 9.3 and 9.4 are carried out.
- Spark `void` (untyped NULL), `char(n)` and `varchar(n)` (Spark reports `string` in query results) need
  explicit rules; write them down in the function's docstring.

### 5.2 Compare the error phase, not only "both error"

Today a parser error in DuckDB passes any case where Spark raises any error. In the type matrices many
cells are Spark errors, so this would hide missing features.

- Record on the Spark side: `phase` (already there) and the error class, the text in the leading
  `[...]` of the message (`[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES]`), when there is one.
- Record on the DuckDB side the exception type (`ParserException`, `BinderException`,
  `CatalogException`, `ConversionException`, `OutOfRangeException`, `InvalidInputException`, ...) and
  whether the error came from `con.sql()` (bind) or from `fetchall()` (execution).
- New rule: Spark analysis error must meet a DuckDB parse / bind / catalog error; Spark runtime error must
  meet a DuckDB execution error. A mismatch is a FAIL in `spark_strict` and is reported as `PHASE` in
  `spark` (counted, not failing, like `LENIENT`).
- Error class: the share of cases where the extension's message starts with the same error class as
  Spark's is printed in `COVERAGE.md`. It is gating only where section 9.6 says so.
- Spark folds constants, so an error can move from run time to analysis time between a literal and a
  column argument. That is why matrices use both forms (section 7.1); do not special-case it in the
  harness.

### 5.3 Exact numbers where exactness is the point

- Add a file-level directive `-- @exact`: floats compare bit for bit (`-0.0` differs from `0.0`, `NaN`
  equals `NaN`), and a `float` column is not equal to a `double` column (already covered by 5.1).
- Generated type-matrix files use `@exact`. Existing files keep the tolerance.
- In `@exact` files compare `DECIMAL` values including scale (`1.50` vs `1.5` differ in Spark's type, and
  5.1 already checks the type; keep value comparison numeric).

### 5.4 Settings per file, per case, and as a matrix

- Keep `-- @config key=value` (file level).
- Add a block directive `-- @set key=value` (repeatable): set before the case in both engines, restored
  after it. In Spark use `spark.conf.set` / `unset`; in DuckDB run `SET key = value` and afterwards
  `RESET key`. A case with `@set` also tests that the dialect's `RESET` works.
- Add a file directive `-- @matrix key=v1,v2[,v3]` (repeatable; the product of all `@matrix` lines). The
  file is recorded and compared once per combination. Golden files are keyed by the combination:
  `golden[config_tuple][index]`. `run_duckdb.py` reports each combination as its own line
  (`operators.sql[ansi=true]`).
- Add `--config key=value` to `make_golden.py` and `run_duckdb.py` to run any existing file under an extra
  setting without editing it (used by workstream D). Golden output goes to
  `test/golden/<config-slug>/<file>.pkl`.
- Static Spark settings cannot be changed in a session. `make_golden.py` must fail loudly if
  `spark.conf.set` raises, not skip.
- The golden staleness check (`expected["sql"] != case.sql`) must also cover the directives.

### 5.5 Expected failures

- Add `-- @xfail <id> <reason>`: the case runs; FAIL is reported as `XFAIL` and does not fail the run;
  PASS is reported as `XPASS` and **does** fail the run, so the marker is removed when the bug is fixed.
- `<id>` groups cases with one cause (`conf-mapKeyDedupPolicy`, `ansi-implicit-cast`, `types-ntz`).
  `scripts/coverage_report.py` prints a table of ids with counts into `COVERAGE.md`.
- For generated files the marker cannot live in the file. Add `test/xfail.toml`: entries of
  `file` glob, optional `config`, `sql` regular expression or case id list, `id`, `reason`. The runner
  applies it the same way. An entry that matches no failing case fails the run.
- `@skip` stays only for cases that cannot run at all; do not add new ones.

### 5.6 Other harness items

- Column names: add a comparison of Spark's output column names with DuckDB's, gating for aliases and
  plain column references and report-only for generated names (a share in `COVERAGE.md`). Section 9.5 says which names are gating.
- Time zones: `engines.py` forces `TZ=UTC` in both processes and `SET TimeZone = 'UTC'`. Keep that as the
  process default; a time-zone test changes only the session setting. PySpark returns timestamps as naive
  local datetimes; with the process in UTC the existing normalization holds. In time-zone files also
  select `CAST(ts AS STRING)` next to each timestamp so the rendering is compared.
- Golden size: goldens are pickles committed to the repository and re-recorded in CI. For generated
  matrices commit the goldens compressed (`.pkl.gz`), and shard the CI `golden` job (section 8).

## 6. Workstream B: settings

### B1. Inventory: classify all 478 keys

Create `test/settings/inventory.toml` with one entry per key of `test/spark35_sqlconf.json`:

```toml
[["spark.sql.mapKeyDedupPolicy"]]
class = "semantic"            # see the classes below
values = ["EXCEPTION", "LAST_WIN"]
status = "ignored"            # emulated | partial | ignored | rejected
cases = "test/cases/settings/mapKeyDedupPolicy.sql"
note = ""
```

Classes:

| class | meaning | test obligation |
|---|---|---|
| `semantic` | changes the rows, types, acceptance or errors of some SQL statement | a witness with `differs: true`, and a case file (B3) |
| `command` | changes only the output of a command (`SHOW`, `DESCRIBE`, `EXPLAIN`, error text) | a witness; cases only if the command is supported by the dialect |
| `storage` | only matters when reading or writing files or Hive tables | none, unless the dialect supports that source; note which |
| `execution` | planning, codegen, memory, shuffle, AQE, statistics, streaming, UI: no result change | none; the key must still be accepted by `SET` |
| `client` | affects only the Scala / Python API (`datetime.java8API`, `repl.*`, `pyspark.*`) | none |
| `static` | cannot be set in a session; `SET` raises in Spark | one case in B2 |
| `unobservable` | documented as semantic but no SQL witness could be built | the note says what was tried |

Method:

1. Start from Appendix B, which assigns whole prefixes to `execution`, `storage` and `client`, and from
   Appendix A for the `semantic` keys.
2. For every remaining key read its `doc` in the JSON. If the doc says results, parsing, analysis, types
   or errors change, write a witness in `conf_witnesses.json`, run `scripts/probe_conf_witnesses.py`, and
   classify by the outcome. The 19 witnesses with `differs: false` are a to-do list: improve the
   statements or classify the key as `unobservable` / `command` / `client`.
3. Do not trust the prefix alone for `spark.sql.optimizer.*`, `spark.sql.codegen.*`,
   `spark.sql.execution.*`: Spark's optimizer can remove an expression that would fail (the witness for
   `spark.sql.optimizer.excludedRules` shows `size(map(NULL, 1))` succeeding only with constant folding).
   Those stay `execution`, but record such cases in `known_differences.toml` if the dialect differs.
4. Add `test/settings/check_inventory.py` (run in CI): fails if a key of the JSON is missing from the
   inventory, if a `semantic` key has no differing witness, or if `cases` names a missing file.

### B2. The `SET` / `RESET` command itself

New file `test/cases/settings/set_command.sql`. Each line is a behaviour to cover; record Spark's result.

- `SET` with no arguments (compare only that it succeeds: `@nondeterministic`), `SET -v`.
- `SET key` for: a key set earlier in the session, a known key never set (Spark shows the default), an
  unknown key (`<undefined>`).
- `SET key=value`: output columns `key`, `value`; the value is stored and visible to a later `SET key`.
- Value forms: with and without spaces around `=`, quoted with backticks, a value with spaces, an empty
  value, a value containing `=` or `;`, trailing whitespace.
- Validation: a boolean key with `maybe`, an enum key with an unknown name (and enum names in lower
  case), an integer key with text. Spark raises `INVALID_CONF_VALUE` or an `IllegalArgumentException`.
- Key case: `SET SPARK.SQL.ANSI.ENABLED=true` must not switch ANSI on (Spark keys are case-sensitive).
- A static key (`spark.sql.warehouse.dir`) and a Spark core key (`spark.task.cpus`) raise
  `CANNOT_MODIFY_CONFIG`; the latter depends on `spark.sql.legacy.setCommandRejectsSparkCoreConfs`.
- `RESET key` for a set key, an unset key, an unknown key; `RESET` alone restores every key. After each,
  a witness query shows the default behaviour again.
- `SET TIME ZONE 'America/Los_Angeles'`, `SET TIME ZONE LOCAL`, `SET TIME ZONE INTERVAL 8 HOUR`,
  `SET TIME ZONE INTERVAL '15:40:32' HOUR TO SECOND`, an invalid zone, and their effect on
  `SET spark.sql.session.timeZone`.
- `spark.sql.variable.substitute`: `${key}` in a later statement, `${hivevar:x}`, `${system:...}`,
  nested and unknown references, and the same with the setting off.
- Session behaviour, as cases in one file run in order: a setting changed between two executions of the
  same statement; a temporary view created under one value and queried under another (Spark captures the
  settings of a view at creation, see `spark.sql.legacy.useCurrentConfigsForView`); a `CACHE TABLE`
  created before a change; the setting surviving `SET current_dialect = 'spark_strict'` and back.
- In `spark` (lenient) a key without a dot goes to DuckDB. Add cases that pin down which keys go where.

### B3. One case file per semantic setting

For every `semantic` key create `test/cases/settings/<short key>.sql` with
`-- @matrix <key>=<every legal value>` (add `-- @config` lines for settings it requires, such as
`spark.sql.ansi.enabled=true` for the three `spark.sql.ansi.*` sub-settings).

Content of each file, in this order:

1. The witness from `conf_witnesses.json`.
2. Every expression or statement the setting touches. Find them in Spark's source for branch-3.5: search
   for the accessor of the key in `SQLConf.scala` (for example `mapKeyDedupPolicy`), then for uses of
   that accessor. Each use is a function or rule to cover. For `mapKeyDedupPolicy` that is `map`,
   `map_from_arrays`, `map_from_entries`, `str_to_map`, `map_concat`, `transform_keys`, and maps built
   by `from_json`, `CAST`, and `map_zip_with`.
3. Each of those with literals and with columns (`FROM VALUES ... AS t(x)`), with NULLs, and nested
   inside another expression.
4. A negative control: a statement the setting must not change.

Sizes: expect 10 to 60 cases per setting, about 2,500 cases in total.

For a setting that section 9.2 puts in tier 3 (rejected), the file holds only the witness under the
non-default value, matched by the `conf-rejected` entry of `test/xfail.toml`, plus the same statements
under the default value. Write items 2 to 4 only for tiers 1 and 2.

Settings that need more than a short file:

| setting | extra coverage |
|---|---|
| `spark.sql.ansi.enabled` | workstream D |
| `spark.sql.session.timeZone` | values `UTC`, `America/Los_Angeles`, `Asia/Kolkata`, `Australia/Lord_Howe` (30-minute DST), `+05:30`, `-08:00`; every datetime function of `test/spark35_functions.txt`; DST gap and overlap instants; `CAST` between `STRING`, `DATE`, `TIMESTAMP`, `TIMESTAMP_NTZ`; `date_trunc`, `from_utc_timestamp`, `to_utc_timestamp`, `unix_timestamp`, `date_format` with zone letters, `current_timezone()`, `window()` grouping |
| `spark.sql.timestampType` | `TIMESTAMP` literals, `CAST(... AS TIMESTAMP)`, DDL column types, `to_timestamp`, combined with two time zones |
| `spark.sql.storeAssignmentPolicy` | matrix C7 |
| `spark.sql.legacy.timeParserPolicy` | `EXCEPTION`, `CORRECTED`, `LEGACY` over every pattern letter, for `to_date`, `to_timestamp`, `unix_timestamp`, `to_unix_timestamp`, `date_format`, `from_unixtime`, `from_json` / `from_csv` with `dateFormat` / `timestampFormat` options |
| `spark.sql.decimalOperations.allowPrecisionLoss`, `spark.sql.legacy.literal.pickMinimumPrecision`, `spark.sql.legacy.allowNegativeScaleOfDecimal` | matrix C8 |
| `spark.sql.caseSensitive` | column, alias, struct field, map key, CTE, table and function names; `GROUP BY` / `ORDER BY` references; `USING` joins; duplicate columns that differ in case; struct `CAST` by name |
| `spark.sql.charAsVarchar`, `spark.sql.readSideCharPadding`, `spark.sql.legacy.charVarcharAsString`, `spark.sql.legacy.noCharPaddingInPredicate` | the product of the four, over `CREATE TABLE`, `INSERT`, comparison, `length`, `concat`, joins on `CHAR` columns, over-length values |
| `spark.sql.parser.escapedStringLiterals` | every escape of `literals_types.sql`, `LIKE`, `RLIKE`, `regexp_*`, `split`, raw strings |
| `spark.sql.legacy.interval.enabled`, `spark.sql.legacy.fromDayTimeString.enabled` | matrix C10 |

### B4. Interactions

One file `test/cases/settings/interactions.sql` with `@matrix` products for settings whose docs refer to
each other: ANSI with `legacy.sizeOfNull`, `storeAssignmentPolicy`, `legacy.timeParserPolicy`,
`legacy.typeCoercion.datetimeToString.enabled`, `mapKeyDedupPolicy`; `session.timeZone` with
`timestampType`; `groupByOrdinal` with `groupByAliases` and `orderByOrdinal`.

### B5. DuckDB settings must not leak into the Spark dialects

The lenient dialect passes `SET <key without dot>` to DuckDB. Some DuckDB settings change semantics:
`default_null_order`, `default_order`, `integer_division`, `ieee_floating_point_ops`,
`old_implicit_casting`, `default_collation`, `order_by_non_integer_literal`,
`scalar_subquery_error_on_multiple_rows`, `lambda_syntax`, `preserve_insertion_order`, `Calendar`,
`threads`.

- List DuckDB's settings from `duckdb_settings()` and classify them the same way in
  `test/settings/duckdb_inventory.toml`.
- For each semantic one, run `operators.sql`, `queries.sql` and `literals_types.sql` with the setting at a
  non-default value (`--duck-set key=value`, a new option of `run_duckdb.py` that touches DuckDB only).
  The Spark golden is unchanged, so every case must still pass. A failure means the dialect depends on a
  DuckDB default; fix it in the extension or pin the setting when a Spark dialect is selected.

## 7. Workstream C: type conversion matrices

### 7.1 Common design

Write one generator module, `scripts/gen_type_cases.py`, with one function per matrix. It writes
`test/cases/types/<matrix>.sql` (committed, like `functions_examples.sql`) and is deterministic. Every
generated file starts with `-- @exact`.

**Type universe.** One table `TYPES` in the generator. Each type has a Spark type string, a set of
literal values, and a column form.

| group | types |
|---|---|
| null | untyped `NULL` (`void`) |
| boolean | `BOOLEAN` |
| integral | `TINYINT`, `SMALLINT`, `INT`, `BIGINT` |
| fractional | `FLOAT`, `DOUBLE` |
| decimal | `DECIMAL(5,2)`, `DECIMAL(10,0)`, `DECIMAL(20,5)`, `DECIMAL(38,18)`, `DECIMAL(38,0)` |
| text | `STRING`, `CHAR(5)`, `VARCHAR(5)`, `BINARY` |
| datetime | `DATE`, `TIMESTAMP`, `TIMESTAMP_NTZ` |
| interval | `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND`, and one single-field type of each (`INTERVAL MONTH`, `INTERVAL HOUR`) |
| nested | `ARRAY<INT>`, `ARRAY<STRING>`, `ARRAY<ARRAY<INT>>`, `MAP<STRING,INT>`, `MAP<INT,STRING>`, `STRUCT<a:INT,b:STRING>`, `STRUCT<a:BIGINT,b:STRING>`, `STRUCT<x:INT,y:STRING>` |

**Values per type.** Typical, zero / empty, minimum, maximum, NULL, and the special values of the type
(`NaN`, `Infinity`, `-0.0`; `0001-01-01`, `9999-12-31`, `1582-10-04`; a DST instant; multi-byte text).
`STRING` additionally carries the strings that parse as each other type and the near misses: `'12'`,
`' 12 '`, `'+12'`, `'1.5'`, `'1.'`, `'.5'`, `'1e3'`, `'0x1A'`, `'12abc'`, `''`, `'NaN'`, `'Infinity'`,
`'-inf'`, `'true'`, `'T'`, `'yes'`, `'1'`, `'2147483648'`, `'9223372036854775808'`,
`'2024-02-29'`, `'2024-2-9'`, `'2024'`, `'2024-02-30'`, `'2024-01-01 10:00:00'`,
`'2024-01-01T10:00:00Z'`, `'2024-01-01 10:00:00+05:30'`, `'10:00:00'`, `'1 day'`, `'INTERVAL 1 DAY'`.
Reuse `EDGES` in `test/fuzz/args_sweep.py` and `VALUES` in `test/fuzz/gen.py` where they fit.

**Two forms per cell.** Spark folds constants and has rules that apply only to literals (integer literals
take the decimal precision of their digits; a foldable string compared with a date). DuckDB folds too.
Every cell is generated as

- a literal form: `SELECT typeof(<expr over literals>), <expr over literals>`, and
- a column form: `SELECT typeof(<expr>), <expr> FROM VALUES (<v1>), (<v2>), ... AS t(a, b) ORDER BY ...`
  or over the typed tables below.

Create the shared tables once per file in `@setup` blocks: `tv_<type>` with one column `v` of that type
and the value set as rows, plus an `id` column for a total order.

**One cell per statement** when the cell can raise an error (one failing expression must not hide the
others). Cells that cannot fail (pure `typeof` of decimal arithmetic) are packed, about 40 per statement.

**Result type.** Covered twice: the harness compares the column type (5.1), and `typeof()` is selected,
which tests the dialect's own `typeof`.

### 7.2 The matrices

Sizes are estimates of generated statements, before the configurations of workstream D.

| id | matrix | what varies | Spark rule covered | size |
|---|---|---|---|---|
| C1 | explicit casts | `CAST(v AS T)`, `TRY_CAST(v AS T)`, and the function forms `int(v)`, `string(v)`, ... for every source type, target type and value | `Cast.canCast`, cast semantics | 6,000 |
| C2 | rendering as string | `CAST(v AS STRING)` for every type and value including nested and nested NULLs; the same through `concat`, `\|\|`, `concat_ws`, `format_string('%s')`, `to_json`, `array_join` | `Cast` to string, `castComplexTypesToString` | 600 |
| C3 | arithmetic | `+ - * / % div`, `pmod`, unary `-`, `+`, `& \| ^ ~`, `shiftleft`, for every left type and right type | `ImplicitTypeCasts`, `PromoteStrings`, `DecimalPrecision`, `Division`, `IntegralDivision`, `DateTimeOperations` | 7,000 |
| C4 | comparison | `= <=> != < <= > >=`, `BETWEEN`, `IN (list)`, `IN (subquery)`, for every pair of types, with values chosen so that the direction of the cast changes the answer | `findCommonTypeForBinaryComparison`, `BooleanEquality`, `InConversion` | 5,000 |
| C5 | common type | every pair of types in each context of 7.3 | `findWiderTypeForTwo`, `WidenSetOperationTypes`, `CaseWhenCoercion`, `IfCoercion`, `FunctionArgumentConversion` | 8,000 |
| C6 | function arguments | every documented function, each argument position, a value of every type | `ImplicitTypeCasts` over `ExpectsInputTypes` | 45,000 (see 7.4) |
| C7 | store assignment | `INSERT INTO t VALUES`, `INSERT ... SELECT`, `INSERT OVERWRITE`, by name (`INSERT INTO t (cols)`), CTAS, for every value type into every column type, under `storeAssignmentPolicy` ANSI, LEGACY, STRICT | `TableOutputResolver`, `canANSIStoreAssign`, char / varchar length checks | 4,500 |
| C8 | decimal precision | result type of `+ - * / % div`, `sum`, `avg`, `round`, `bround`, `floor`, `ceil` (one and two arguments), `abs`, `-`, comparison and `UNION`, over a grid of (p, s); values at the overflow boundary; operands mixed with each integral type, with integer literals of 1 to 19 digits, with `FLOAT` / `DOUBLE` | `DecimalPrecision`, `adjustPrecisionScale`, `DecimalType.forType` | 2,500 (packed) |
| C9 | literal typing | integer literals around every width boundary, decimal literals of 1 to 39 digits, exponents, suffixes `Y S L BD D F`, signs, `.5`, `5.`, hex, typed literals (`DATE`, `TIMESTAMP`, `TIMESTAMP_NTZ`, `TIMESTAMP_LTZ`, `INTERVAL` in both syntaxes, `X''`), and invalid forms | `AstBuilder` literal rules | 500 |
| C10 | datetime and interval arithmetic | `date ± int`, `date ± interval`, `timestamp ± interval`, `date - date`, `timestamp - timestamp`, `date - timestamp`, `interval ± interval`, `interval * /` number, string on either side, `date_add` / `date_sub` / `add_months` with string and fractional arguments | `DateTimeOperations`, `StringLiteralCoercion`, ANSI interval types | 1,500 |
| C11 | boolean contexts | a value of every type as the condition of `WHERE`, `HAVING`, `JOIN ... ON`, `CASE WHEN`, `IF`, `AND`, `OR`, `NOT`, `filter(arr, x -> ...)`, `FILTER (WHERE ...)` | condition type checks | 400 |
| C12 | aggregates and windows | every aggregate function over a column of every type: result type and value for normal rows, an empty input, all NULLs and overflow; `lag` / `lead` default values of another type; `RANGE` frame bounds of every type against order keys of every type | aggregate `inputTypes`, `WindowFrameCoercion` | 3,000 |
| C13 | nested types | casts between nested types (element widening and narrowing, struct by position, field count mismatch, map key casts), and the C5 contexts with nested operands whose element types differ | `Cast` for complex types, nested `findWiderTypeForTwo` | 1,500 |
| C14 | equality and ordering by type | join keys of different types (`ON`, `USING`, `NATURAL`: result type and matches), `GROUP BY` / `DISTINCT` / set operations over `-0.0`, `0.0`, `NaN`, values equal after coercion; `ORDER BY` of every type with NULLs, `NaN`, multi-byte strings, binary, arrays, structs | join key coercion, `NormalizeFloatingNumbers`, ordering | 1,200 |

For the decimal grid of C8 use p in {1, 5, 9, 10, 18, 19, 20, 28, 37, 38} and, for each p, s in
{0, 1, 2, 6, p/2, p-1, p}.

### 7.3 Contexts of the common-type matrix (C5)

Run the full type × type matrix in the five contexts that have their own Spark rule: `CASE WHEN`,
`coalesce`, `UNION ALL`, `array(a, b)`, `IN (a, b)`.

Run a reduced set of about 60 pairs (one per pair of type groups, plus every pair involving `STRING`,
`DECIMAL` and untyped NULL) in the others: `IF`, `nvl`, `nvl2`, `ifnull`, `nullif`, `greatest`, `least`,
`nanvl`, `UNION`, `INTERSECT`, `EXCEPT`, rows of a `VALUES` list, `map` keys and values,
`map_from_arrays`, `array_contains`, `array_position`, `array_remove`, `array_append`, `array_prepend`,
`array_insert`, `array_union`, `array_intersect`, `array_except`, `arrays_overlap`, `concat` of arrays,
`map_concat`, `element_at` with a key of another type, `sequence`, `PIVOT ... IN` values, the default of
`lag` / `lead`, `struct` fields across `UNION`.

Set operations with three or more branches and `CASE` with three or more branches need a few cases each:
Spark widens pairwise from the left, which is not associative for `DECIMAL` and `STRING`.

### 7.4 Function arguments (C6)

Extend `test/fuzz/args_sweep.py` with a `--types` mode instead of writing a second tool. Today it replaces
a literal with edge values of the same kind. In `--types` mode it replaces each argument slot of each
documented example with one typical value of every type of 7.1, as a literal and as a column.

- The result per (function, slot, type) is: accepted or rejected by Spark, the result type, the value.
- Spark's results for this sweep are recorded once into `test/golden/signatures.json.gz` (committed) by
  `args_sweep.py --types --record`. CI compares both dialects against the file, which needs no Spark and
  takes minutes. The nightly workflow re-records and fails if Spark's answers changed.
- `spark_strict` must reject what Spark rejects; `spark` may accept (reported as lenient).
- The table `INT_PARAMETERS` in `src/spark_grammar.cpp` is hand-collected. After the sweep, derive the
  complete list of `INT` parameters from `signatures.json.gz` (slots where Spark accepts `BIGINT` and
  returns the result of the wrapped value) and compare it with the table.

## 8. Workstreams D and E, and CI

### D. Everything again under the settings that change typing

| configuration | files |
|---|---|
| `spark.sql.ansi.enabled=true` | all of `test/cases/types/`, `operators.sql`, `literals_types.sql`, `functions_examples.sql`, `queries.sql`, `ddl_dml.sql`; `signatures.json.gz` recorded a second time |
| `spark.sql.session.timeZone=America/Los_Angeles` | C1, C2, C4, C10, C12, and the datetime functions of `functions_examples.sql` |
| `spark.sql.timestampType=TIMESTAMP_NTZ` | C1, C4, C5, C9, C10 |
| `spark.sql.decimalOperations.allowPrecisionLoss=false` | C3, C8 |
| `spark.sql.legacy.typeCoercion.datetimeToString.enabled=true` | C4 |
| `spark.sql.storeAssignmentPolicy` = `LEGACY`, `STRICT` | C7 (already a matrix inside the file) |
| `spark.sql.caseSensitive=true` | `queries.sql`, `ddl_dml.sql`, C13 |

Use `--config` (5.4); do not copy files. Run a configuration only while its setting is emulated: the
`timestampType` row applies only if the remap of section 9.3 is kept, and the
`legacy.typeCoercion.datetimeToString.enabled` row only if that setting is moved out of tier 3. ANSI is the large one: Spark's `AnsiTypeCoercion` replaces the
rules behind C3, C4, C5 and C6 (strings are no longer promoted to `DOUBLE`, string with integer gives
`BIGINT`, fewer implicit casts are legal, casts raise instead of returning NULL). Expect most
ANSI failures to share one `@xfail` id until ANSI implicit casts are implemented.

### E. Spark's own tests and the fuzzers

1. In `test/fuzz/spark_corpus.py` remove `typeCoercion/`, `ansi/`, `nonansi/` and `timestampNTZ/` from
   `SKIP`. Check the directory listing of branch-3.5 for the exact files; `typeCoercion/native/` holds
   `binaryComparison`, `booleanEquality`, `caseWhenCoercion`, `concat`, `dateTimeOperations`,
   `decimalPrecision`, `division`, `elt`, `ifCoercion`, `implicitTypeCasts`, `inConversion`,
   `mapZipWith`, `mapconcat`, `promoteStrings`, `stringCastAndExpressions`, `widenSetOperationTypes`,
   `windowFrameCoercion`, `arrayJoin`. Honour the `--SET` and `--CONFIG_DIM` comment lines of those
   files, which name the settings each file runs under.
2. Promote these inputs, plus `cast.sql`, `try_cast.sql`, `decimalArithmeticOperations.sql`,
   `literals.sql`, `interval.sql`, `datetime-*.sql`, `timezone.sql`, `charvarchar.sql`, `set.sql`, to
   gating case files under `test/cases/spark_corpus/` (Apache-2.0; keep the licence header and add a
   `NOTICE` line). Record goldens from live Spark, not from Spark's `.out` files. Failing statements get
   `test/xfail.toml` entries.
3. `test/fuzz/gen.py`: add columns of the missing types (`TIMESTAMP_NTZ`, both interval types,
   `DECIMAL(38,0)`, `CHAR`, `VARCHAR`, nested arrays) and let the expression generator deliberately mix
   kinds (today it generates an expression for one kind, so cross-type coercion is rarely produced).
4. `test/fuzz/differential.py`: add `--conf-fuzz`. Each batch draws a random assignment of the `semantic`
   settings whose `status` is `emulated` from `inventory.toml`, sets it in Spark and in both dialects,
   and records it in each finding so that `reduce.py` and replays reproduce it.
5. `test/fuzz/compare.py`: use the type and error-phase comparison of workstream A.

### CI

- `golden` job: shard by file group (existing files, `settings/`, `types/` in 4 shards, each extra
  configuration of workstream D). Budget: each shard under 30 minutes.
- Total budget for case files on every push: about 45,000 statements per configuration for the types
  directory is too much for every configuration. On push run `types/` under the default configuration and
  ANSI; run the other configurations of D in `nightly.yml`.
- Add jobs: `check_inventory.py`, the harness unit tests, the `signatures.json.gz` comparison.
- `scripts/coverage_report.py`: add to `COVERAGE.md` a settings table (key, class, status, cases, pass /
  xfail), a table per type matrix (cells, pass, xfail by id), and the report-only metrics (error class
  agreement, column names).

## 9. Decisions (defaults chosen)

These are decided. Follow them without asking. The user may override any of them; an override replaces
the text here. Ask only if a measurement contradicts the premise stated with a decision.

### 9.1 Settings the dialect does not emulate: reject, never ignore

- `SET <key> = <value>` for a `semantic` setting that is not emulated raises
  `[UNSUPPORTED_FEATURE.SET_CONF] spark_dialect does not emulate <key> = <value>` in both dialects when
  the value differs from Spark's default. Setting the default value succeeds and is stored.
- A setting emulated for some values only (`status = "partial"`) rejects the other values the same way.
- Settings of class `execution`, `storage`, `client` and `command` are accepted, stored and returned by
  `SET <key>`, with no other effect. Unknown keys are stored like Spark does.
- `static` keys and Spark core keys raise `CANNOT_MODIFY_CONFIG` like Spark.
- Every rejected setting is listed in the README and has one case in its settings file that expects the
  rejection, marked in `test/xfail.toml` with id `conf-rejected` (Spark accepts the statement, so the
  case differs from Spark by design). This is the only id allowed to remain for settings at the end.

Reason: a rejected `SET` is visible and cannot produce a wrong result; an ignored one can.

### 9.2 Which settings to implement

Implement tiers 1 and 2. Reject tier 3 (9.1).

| tier | settings | notes |
|---|---|---|
| 1 | `ansi.enabled` including ANSI implicit casts, `session.timeZone`, `storeAssignmentPolicy`, `mapKeyDedupPolicy`, `caseSensitive`, `legacy.timeParserPolicy` | set in real workloads; each is large; do them in this order |
| 2 | `legacy.sizeOfNull`, `groupByOrdinal`, `orderByOrdinal`, `groupByAliases`, `jsonGenerator.ignoreNullFields`, `function.concatBinaryAsString`, `function.eltOutputAsString`, `legacy.statisticalAggregate`, `legacy.negativeIndexInArrayInsert`, `legacy.nullInEmptyListBehavior`, `chunkBase64String.enabled`, `decimalOperations.allowPrecisionLoss`, `legacy.setopsPrecedence.enabled`, `crossJoin.enabled`, `parser.escapedStringLiterals`, `parser.quotedRegexColumnNames`, `ansi.doubleQuotedIdentifiers`, `ansi.enforceReservedKeywords`, `ansi.relationPrecedence`, `variable.substitute`, `lateralColumnAlias.enableImplicitResolution`, `legacy.castComplexTypesToString.enabled`, `legacy.followThreeValuedLogicInArrayExists`, `legacy.allowParameterlessCount`, `legacy.createHiveTableByDefault`, `legacy.ctePrecedencePolicy`, `charAsVarchar` | a switch read once at parse or bind time |
| 3 | every other `semantic` setting, including `timestampType` (see 9.3), `legacy.interval.enabled` (see 9.4), `legacy.allowNegativeScaleOfDecimal`, `legacy.exponentLiteralAsDecimal.enabled`, `sources.partitionOverwriteMode`, `defaultColumn.*`, the view and partition-spec legacy keys | rejected |

Rules for moving a setting between tiers without asking:

- A tier 2 setting whose implementation turns out to need more than about 150 lines, or a per-row check,
  moves to tier 3. Say so in the commit message and in `inventory.toml`.
- A tier 3 setting that turns out to be a one-line switch at bind time may be implemented.
- A tier 1 setting is never dropped. If one cannot be finished, it stays `partial` with its failing cases
  under one `@xfail` id and the unsupported values rejected.

### 9.3 `TIMESTAMP` and `TIMESTAMP_NTZ`: one type unless a measured remap is cheap

Spark `TIMESTAMP` is an instant and `TIMESTAMP_NTZ` a wall clock time; the dialect maps both to DuckDB
`TIMESTAMP`. Under UTC the values agree and only `typeof` and the schema differ; under another session
time zone the results differ.

1. First, with the single type: make `session.timeZone` correct for Spark `TIMESTAMP` (tier 1), and make
   `typeof` and the harness type mapping report `timestamp_ntz` wherever the kind is known from the
   expression (a `TIMESTAMP_NTZ` literal, `CAST(... AS TIMESTAMP_NTZ)`, `to_timestamp_ntz`,
   `make_timestamp_ntz`, `localtimestamp`). Cases that still differ carry `@xfail types-ntz`.
2. Then try the remap on a separate branch of work: Spark `TIMESTAMP` to DuckDB
   `TIMESTAMP WITH TIME ZONE`, `TIMESTAMP_NTZ` to `TIMESTAMP`. Keep it only if all three hold: the
   existing suite and the new matrices pass in both dialects; no smoke benchmark exceeds its threshold in
   `bench/baseline.json`; and the TPC-H and TPC-DS geometric means move by less than 5%.
3. If the remap is kept, implement `timestampType` (it moves to tier 1) and remove the `types-ntz` id.
   If not, revert it, keep step 1, reject `timestampType=TIMESTAMP_NTZ` (9.1), and add one entry to
   `known_differences.toml`: `TIMESTAMP_NTZ` values behave like `TIMESTAMP` under a non-UTC session time
   zone. Record the measured numbers in `docs/perf-log.md` either way.

### 9.4 Interval types: one DuckDB type, Spark's name where it is known

Keep DuckDB's single `INTERVAL`. `typeof` and the harness type mapping report Spark's name
(`interval year to month`, `interval day`, `interval hour to second`, ...) where the kind and fields are
known from the expression: interval literals, `make_ym_interval`, `make_dt_interval`, casts to an
interval type, `date - date`, `timestamp - timestamp`, and arithmetic over these. Interval values read
from a table column keep the year-month or day-time distinction the extension already derives from the
value (`IsYearMonth` in `src/spark_extras.cpp`) but not the field range; those cases carry
`@xfail types-interval` and one entry in `known_differences.toml`. `legacy.interval.enabled=true` and
`legacy.fromDayTimeString.enabled=true` are rejected (9.1).

### 9.5 Column names: gate the names a user chose, measure the generated ones

- Gating in both dialects, for every case file: the number of columns, and the name of every column that
  is an explicit alias or a plain column reference.
- Report-only: names Spark generates from an expression (`(1 + 1)`, `CAST(a AS INT)`, `count(1)`).
  `COVERAGE.md` prints the share that match. Do not implement Spark's name generation in this work.
- Exception, gating: a generated name that a later statement in the same case file refers to (CTAS or a
  view over an unaliased expression, then `SELECT` of that column by name). Add 20 such cases to
  `test/cases/settings/` or `queries.sql`; fix what they show.

### 9.6 Errors: phase everywhere, error class in `spark_strict` for the type and settings cases

- The error phase (5.2) is gating in `spark_strict` for every file and reported in `spark`.
- The error class is gating in `spark_strict` for the files under `test/cases/types/` and
  `test/cases/settings/`, for these classes and their sub-classes: `DATATYPE_MISMATCH`,
  `CAST_INVALID_INPUT`, `CAST_OVERFLOW`, `CANNOT_CAST_DATATYPE`, `ARITHMETIC_OVERFLOW`,
  `DIVIDE_BY_ZERO`, `INTERVAL_DIVIDED_BY_ZERO`, `NUMERIC_VALUE_OUT_OF_RANGE`,
  `INVALID_ARRAY_INDEX`, `INVALID_ARRAY_INDEX_IN_ELEMENT_AT`, `DUPLICATED_MAP_KEY`, `NULL_MAP_KEY`,
  `INCOMPATIBLE_DATA_FOR_TABLE`, `INVALID_CONF_VALUE`, `CANNOT_MODIFY_CONFIG`, `WRONG_NUM_ARGS`,
  `UNRESOLVED_COLUMN`, `UNRESOLVED_ROUTINE`. The comparison is on the class up to the first `.` unless the
  extension already emits the sub-class.
- Everywhere else, and in `spark`, the class is report-only. Message text, SQLSTATE and query context are
  never compared.

## 10. Definition of done

1. `check_inventory.py` passes: all 478 keys are classified; every `semantic` key has a Spark-verified
   differing witness and a case file covering every legal value.
2. Every matrix of section 7.2 is generated, recorded from Spark, and run in both dialects under the
   default configuration and each configuration of workstream D.
3. The harness compares rows, column types and error phase for every case file, old and new.
4. Zero `FAIL` and zero `XPASS` in CI in both dialects. Every remaining `XFAIL` id is one of: `conf-rejected`,
   `types-ntz` or `types-interval` as section 9 allows them, or an entry of `known_differences.toml` with a reason and a reproducing
   example. The count per id is printed in `COVERAGE.md`.
5. `SET` of a `semantic` setting never silently has no effect (emulated, or rejected per section 9.1).
6. Fuzzers: `args_sweep.py --types` and `differential.py --conf-fuzz --budget 20000` end with every
   finding fixed or known. `isolation.py` and `replay.py` pass.
7. `README.md` ("Testing", "Limitations") and `COVERAGE.md` describe the new state; the README no longer
   claims more than the tests show.
8. Benchmarks: no smoke ratio above its threshold in `bench/baseline.json`.

## Appendix A: verified setting witnesses

From `docs/plans/spark-parity/conf_witnesses.json` (Spark 3.5.9). "Result" is Spark's result of the last
statement with the default value, then with the changed value.

Keys are shown without the `spark.sql.` prefix.

| setting = value | requires | last statement (see the JSON for setup statements) | Spark default | Spark changed |
|---|---|---|---|---|
| `allowNamedFunctionArguments` = `false` |  | `SELECT mask('AbCD123-@$#', lowerChar => 'q')` | [['XqXXnnn-@$#']] | error (analysis) |
| `ansi.doubleQuotedIdentifiers` = `true` | ansi.enabled=true | `SELECT "a" FROM VALUES (1) t(a)` | [['a']] | [[1]] |
| `ansi.enabled` = `true` |  | `SELECT CAST('a' AS INT), 2147483647 + 1` | [[None, -2147483648]] | error (runtime, CAST_INVALID_INPUT) |
| `ansi.enabled` = `true` |  | `SELECT '1' + 1, typeof('1' + 1), typeof('2' > 1), typeof(coalesce('1', 2))` | [[2.0, 'double', 'boolean', 'string']] | [[2, 'bigint', 'boolean', 'bigint']] |
| `ansi.enforceReservedKeywords` = `true` | ansi.enabled=true | `SELECT 1 AS order` | [[1]] | error (analysis) |
| `ansi.relationPrecedence` = `true` | ansi.enabled=true | `SELECT * FROM VALUES (1) a(x), VALUES (2) b(y) JOIN VALUES (3) c(z) ON a.x < c.z` | [[1, 2, 3]] | error (analysis, UNRESOLVED_COLUMN.WITH_SUGGESTION) |
| `caseSensitive` = `true` |  | `SELECT A FROM VALUES (1) t(a)` | [[1]] | error (analysis, UNRESOLVED_COLUMN.WITH_SUGGESTION) |
| `charAsVarchar` = `true` |  | (2 setup) `SELECT length(c), typeof(c) FROM w_c` | [[5, 'string']] | [[2, 'string']] |
| `chunkBase64String.enabled` = `false` |  | `SELECT base64(repeat('a', 100))` | [['YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWF… | [['YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWF… |
| `columnNameOfCorruptRecord` = `_bad` |  | `SELECT from_json('{bad', 'a INT, _bad STRING')` | [[[None, None]]] | [[[None, ('str', '{bad'])]] |
| `crossJoin.enabled` = `false` |  | `SELECT * FROM VALUES (1) a(x) JOIN VALUES (2) b(y)` | [[1, 2]] | error (runtime) |
| `decimalOperations.allowPrecisionLoss` = `false` |  | `SELECT typeof(CAST(1 AS DECIMAL(38,18)) * CAST(1 AS DECIMAL(38,18))), CAST(1 AS DECIMAL(38,18)) * CAST(1 AS DE…` | [['decimal(38,6)', 1, 'decimal(38,6)']] | [['decimal(38,36)', 1, 'decimal(38,18)']] |
| `defaultColumn.enabled` = `false` |  | (2 setup) `SELECT * FROM w_d2` | [[1, 7]] | error (analysis) |
| `defaultColumn.useNullsForMissingDefaultValues` = `false` |  | (2 setup) `SELECT * FROM w_d` | [[1, None]] | error (analysis, INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA) |
| `function.concatBinaryAsString` = `true` |  | `SELECT typeof(concat(X'01', X'02'))` | [['binary']] | [['string']] |
| `function.eltOutputAsString` = `true` |  | `SELECT typeof(elt(1, X'01'))` | [['binary']] | [['string']] |
| `groupByAliases` = `false` |  | `SELECT x AS k, count(*) FROM VALUES (1) t(x) GROUP BY k` | [[1, 1]] | error (analysis, UNRESOLVED_COLUMN.WITH_SUGGESTION) |
| `groupByOrdinal` = `false` |  | `SELECT x, count(*) FROM VALUES (1),(2) t(x) GROUP BY 1` | [[1, 1], [2, 1]] | error (analysis, MISSING_AGGREGATION) |
| `json.enablePartialResults` = `false` |  | `SELECT from_json('{"a":{"x":1,"y":"q"}, "b": 2}', 'a struct<x:int,y:int>, b int')` | [[[('struct', [1, None], 2])]] | [[[None, None]]] |
| `jsonGenerator.ignoreNullFields` = `false` |  | `SELECT to_json(named_struct('a', 1, 'b', CAST(NULL AS INT)))` | [['{"a":1}']] | [['{"a":1,"b":null}']] |
| `lateralColumnAlias.enableImplicitResolution` = `false` |  | `SELECT 1 AS a, a + 1` | [[1, 2]] | error (analysis, UNRESOLVED_COLUMN.WITHOUT_SUGGESTION) |
| `legacy.allowAutoGeneratedAliasForView` = `true` |  | (1 setup) `SELECT * FROM w_v` | error (analysis, CREATE_PERMANENT_VIEW_WITHOUT_ALIAS) | [[2]] |
| `legacy.allowHashOnMapType` = `true` |  | `SELECT hash(map(1, 2))` | error (analysis, DATATYPE_MISMATCH.HASH_MAP_TYPE) | [[-222940379]] |
| `legacy.allowNegativeScaleOfDecimal` = `true` |  | `SELECT typeof(1E10BD)` | [['decimal(11,0)']] | [['decimal(1,-10)']] |
| `legacy.allowNullComparisonResultInArraySort` = `true` |  | `SELECT array_sort(array(2, 1), (l, r) -> CAST(NULL AS INT))` | error (runtime) | [[[2, 1]]] |
| `legacy.allowParameterlessCount` = `true` |  | `SELECT count() FROM VALUES (1) t(x)` | error (analysis, WRONG_NUM_ARGS.WITH_SUGGESTION) | [[0]] |
| `legacy.allowStarWithSingleTableIdentifierInCount` = `true` |  | `SELECT count(t.*) FROM VALUES (1, NULL) t(x, y)` | error (analysis) | [[0]] |
| `legacy.allowZeroIndexInFormatString` = `true` |  | `SELECT format_string('%0$s', 'a')` | error (analysis, INVALID_PARAMETER_VALUE.ZERO_INDEX) | error (runtime) |
| `legacy.castComplexTypesToString.enabled` = `true` |  | `SELECT CAST(map(1, NULL) AS STRING), CAST(struct(1, NULL) AS STRING), CAST(array(1, NULL) AS STRING)` | [['{1 -> null}', '{1, null}', '[1, null]']] | [['[1 ->]', '[1,]', '[1,]']] |
| `legacy.charVarcharAsString` = `true` |  | (2 setup) `SELECT length(c), v FROM w_cv` | error (analysis, EXCEED_LIMIT_LENGTH) | [[2, 'abcdef']] |
| `legacy.createEmptyCollectionUsingStringType` = `true` |  | `SELECT typeof(array()), typeof(map())` | [['array<void>', 'map<void,void>']] | [['array<string>', 'map<string,string>']] |
| `legacy.createHiveTableByDefault` = `false` |  | (2 setup) `SELECT * FROM w_h` | error (analysis, NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT) | [[1]] |
| `legacy.ctePrecedencePolicy` = `CORRECTED` |  | `WITH t AS (SELECT 1 AS x), t2 AS (WITH t AS (SELECT 2 AS x) SELECT * FROM t) SELECT * FROM t2` | error (analysis, AMBIGUOUS_ALIAS_IN_NESTED_CTE) | [[2]] |
| `legacy.ctePrecedencePolicy` = `LEGACY` |  | `WITH t AS (SELECT 1 AS x), t2 AS (WITH t AS (SELECT 2 AS x) SELECT * FROM t) SELECT * FROM t2` | error (analysis, AMBIGUOUS_ALIAS_IN_NESTED_CTE) | [[1]] |
| `legacy.exponentLiteralAsDecimal.enabled` = `true` |  | `SELECT typeof(1E2), 1E2` | [['double', 100.0]] | [['decimal(3,0)', 1E+2]] |
| `legacy.followThreeValuedLogicInArrayExists` = `false` |  | `SELECT exists(array(1, NULL), x -> x > 5)` | [[None]] | [[False]] |
| `legacy.fromDayTimeString.enabled` = `true` |  | `SELECT INTERVAL '20 15:40:32.99899999' DAY TO HOUR` | error (analysis) | [[datetime.timedelta(days=20, seconds=54000)]] |
| `legacy.histogramNumericPropagateInputType` = `false` |  | `SELECT typeof(histogram_numeric(x, 2)) FROM VALUES (1), (2) t(x)` | [['array<struct<x:int,y:double>>']] | [['array<struct<x:double,y:double>>']] |
| `legacy.integerGroupingId` = `true` |  | `SELECT typeof(grouping_id()) FROM VALUES (1) t(a) GROUP BY ROLLUP(a)` | [['bigint'], ['bigint']] | [['int'], ['int']] |
| `legacy.interval.enabled` = `true` |  | `SELECT typeof(INTERVAL 1 DAY), typeof(DATE '2024-01-02' - DATE '2024-01-01'), typeof(TIMESTAMP '2024-01-02 00:…` | [['interval day', 'interval day', 'interval day to second']] | [['interval', 'interval', 'interval']] |
| `legacy.keepCommandOutputSchema` = `true` |  | `SHOW DATABASES` | [['default']] | [['default']] |
| `legacy.keepPartitionSpecAsStringLiteral` = `true` |  | (2 setup) `SELECT p FROM w_k` | [['5']] | [['05']] |
| `legacy.literal.pickMinimumPrecision` = `false` |  | `SELECT typeof(CAST(1 AS DECIMAL(10,2)) * 3), typeof(CAST(1 AS DECIMAL(10,2)) + 3)` | [['decimal(12,2)', 'decimal(11,2)']] | [['decimal(21,2)', 'decimal(13,2)']] |
| `legacy.lpadRpadAlwaysReturnString` = `true` |  | `SELECT typeof(lpad(X'01', 3, X'00'))` | [['binary']] | [['string']] |
| `legacy.negativeIndexInArrayInsert` = `true` |  | `SELECT array_insert(array(1, 2, 3), -1, 9)` | [[[1, 2, 3, 9]]] | [[[1, 2, 9, 3]]] |
| `legacy.notReserveProperties` = `true` |  | (1 setup) `SELECT 1` | error (analysis) | [[1]] |
| `legacy.nullInEmptyListBehavior` = `false` |  | `SELECT CAST(NULL AS INT) IN (SELECT x FROM VALUES (1) t(x) WHERE false)` | [[None]] | [[False]] |
| `legacy.parseNullPartitionSpecAsStringLiteral` = `true` |  | (2 setup) `SELECT p, p IS NULL FROM w_k2` | [[None, True]] | [['null', False]] |
| `legacy.parser.havingWithoutGroupByAsWhere` = `true` |  | (1 setup) `SELECT x FROM VALUES (1), (2) t(x) HAVING x > 1` | error (analysis, MISSING_GROUP_BY) | [[2]] |
| `legacy.percentileDiscCalculation` = `true` |  | `SELECT percentile_disc(0.1) WITHIN GROUP (ORDER BY x), percentile_disc(0.3) WITHIN GROUP (ORDER BY x), percent…` | [[0.0, 2.0, 4.0, 6.0, 0.0]] | [[0.0, 1.0, 4.0, 5.0, 0.0]] |
| `legacy.setCommandRejectsSparkCoreConfs` = `false` |  | `SET spark.task.cpus=2` | error (analysis, CANNOT_MODIFY_CONFIG) | [['spark.task.cpus', '2']] |
| `legacy.setopsPrecedence.enabled` = `true` |  | `SELECT 1 UNION SELECT 2 INTERSECT SELECT 3` | [[1]] | [] |
| `legacy.sizeOfNull` = `false` |  | `SELECT size(CAST(NULL AS ARRAY<INT>))` | [[-1]] | [[None]] |
| `legacy.statisticalAggregate` = `true` |  | `SELECT var_samp(x), stddev_samp(x), corr(x, x) FROM VALUES (1) t(x)` | [[None, None, None]] | [[NaN, NaN, NaN]] |
| `legacy.storeAnalyzedPlanForView` = `true` |  | (3 setup) `SELECT * FROM w_tv` | [[2]] | [[1]] |
| `legacy.timeParserPolicy` = `CORRECTED` |  | `SELECT to_date('2020-01-27T20:06:11.847', 'yyyy-MM-dd HH'), to_timestamp('2020-1-1 1', 'yyyy-MM-dd HH'), to_da…` | error (runtime, INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER) | [[None, None, None]] |
| `legacy.timeParserPolicy` = `LEGACY` |  | `SELECT to_date('2020-01-27T20:06:11.847', 'yyyy-MM-dd HH'), to_timestamp('2020-1-1 1', 'yyyy-MM-dd HH'), to_da…` | error (runtime, INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER) | [[None, datetime.datetime(2020, 1, 1, 1, 0), datetime.date(2020, 1, 31… |
| `legacy.typeCoercion.datetimeToString.enabled` = `true` |  | `SELECT DATE '2020-01-10' > '2020-1-9', TIMESTAMP '2020-01-10 00:00:00' = '2020-01-10'` | [[True, True]] | [[False, True]] |
| `mapKeyDedupPolicy` = `LAST_WIN` |  | `SELECT map(1, 'a', 1, 'b'), map_from_arrays(array(1, 1), array(1, 2)), str_to_map('a:1,a:2'), map_concat(map(1…` | error (runtime, DUPLICATED_MAP_KEY) | [[[(1, ('str', 'b')]), [(1, 2]), [(('str', 'a', '2')]), [(1, 2])]] |
| `optimizer.excludedRules` = `org.apache.spark.sql.catalyst.optimizer.ConstantFolding` |  | `SELECT size(map(CAST(NULL AS STRING), 1))` | [[1]] | error (runtime) |
| `orderByOrdinal` = `false` |  | `SELECT x, -x FROM VALUES (2),(1),(3) t(x) ORDER BY 2` | [[3, -3], [2, -2], [1, -1]] | [[2, -2], [1, -1], [3, -3]] |
| `parser.escapedStringLiterals` = `true` |  | `SELECT 'a\nb', length('\n'), 'a' RLIKE '\\w'` | [['a\nb', 1, True]] | [['a\\nb', 2, False]] |
| `parser.quotedRegexColumnNames` = `true` |  | `SELECT `(a)?+.+` FROM VALUES (1, 2, 3) t(a, b, c)` | error (analysis, UNRESOLVED_COLUMN.WITH_SUGGESTION) | [[2, 3]] |
| `session.timeZone` = `+05:30` |  | `SELECT CAST(timestamp_seconds(0) AS STRING), current_timezone()` | [['1970-01-01 00:00:00', 'UTC']] | [['1970-01-01 05:30:00', '+05:30']] |
| `session.timeZone` = `America/Los_Angeles` |  | `SELECT CAST(TIMESTAMP '2024-07-01 00:00:00 UTC' AS STRING), hour(from_unixtime(0)), CAST(CAST('2024-03-10 02:3…` | [['2024-07-01 00:00:00', 0, '2024-03-10 02:30:00', 1704067200, '1970-0… | [['2024-06-30 17:00:00', 16, '2024-03-10 03:30:00', 1704096000, '1969-… |
| `sources.partitionOverwriteMode` = `DYNAMIC` |  | (3 setup) `SELECT * FROM w_p ORDER BY a` | [[3, 1]] | [[2, 2], [3, 1]] |
| `stableDerivedColumnAlias.enabled` = `true` |  | `SELECT 1  +  1, 'a'  \|\|  'b'` | [[2, 'ab']] | [[2, 'ab']] |
| `storeAssignmentPolicy` = `LEGACY` |  | (4 setup) `SELECT * FROM w_s ORDER BY a` | error (analysis, INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST) | [[None, datetime.date(2020, 1, 1)], [-1294967296, None], [1, None]] |
| `storeAssignmentPolicy` = `STRICT` |  | (2 setup) `SELECT * FROM w_s3` | [[1]] | error (analysis, INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST) |
| `timestampType` = `TIMESTAMP_NTZ` |  | `SELECT typeof(TIMESTAMP '2024-01-01 00:00:00'), typeof(CAST('2024-01-01' AS TIMESTAMP)), typeof(to_timestamp('…` | [['timestamp', 'timestamp', 'timestamp', 'timestamp']] | [['timestamp_ntz', 'timestamp_ntz', 'timestamp_ntz', 'timestamp']] |
| `variable.substitute` = `false` |  | (1 setup) `SELECT '${w.var}'` | [['5']] | [['${w.var}']] |
| `view.maxNestedViewDepth` = `1` |  | (3 setup) `SELECT * FROM w_v3` | [[1]] | error (analysis) |
| `warehouse.dir` = `/tmp/x` |  | `SELECT 1` | [[1]] | cannot be set: static |

Witnesses that did not differ (improve the statements or reclassify the key, see B1):

`spark.sql.codegen.wholeStage`, `spark.sql.defaultCatalog`, `spark.sql.error.messageFormat`, `spark.sql.legacy.allowTempViewCreationWithMultipleNameparts`, `spark.sql.legacy.doLooseUpcast`, `spark.sql.legacy.groupingIdWithAppendedUserGroupBy`, `spark.sql.legacy.inSubqueryNullability`, `spark.sql.legacy.json.allowEmptyString.enabled`, `spark.sql.legacy.noCharPaddingInPredicate`, `spark.sql.legacy.respectNullabilityInTextDatasetConversion`, `spark.sql.legacy.useCurrentConfigsForView`, `spark.sql.legacy.useV1Command`, `spark.sql.legacy.v1IdentifierNoCatalog`, `spark.sql.maxMetadataStringLength`, `spark.sql.pivotMaxValues`, `spark.sql.readSideCharPadding`, `spark.sql.retainGroupColumns`, `spark.sql.shuffle.partitions`, `spark.sql.sources.default`.

## Appendix B: how the 478 settings split

Counts by prefix from `test/spark35_sqlconf.json`. The class is the starting assumption for B1; each key
is still checked against its `doc`.

| prefix | keys | starting class |
|---|---|---|
| `spark.sql.legacy.*` | 65 | `semantic` for most (35 have a differing witness; the others need one or a different class); `storage` / `client` for the Avro, Parquet, pandas, Dataset and path-option keys |
| `spark.sql.streaming.*` | 56 | `execution` (no streaming in the dialect) |
| `spark.sql.optimizer.*` | 41 | `execution` (see the caveat in B1, step 3) |
| `spark.sql.parquet.*`, `spark.sql.orc.*`, `spark.sql.avro.*`, `spark.sql.hive.*`, `spark.sql.files.*`, `spark.sql.sources.*`, `spark.sql.csv.*`, `spark.sql.json.*` | 103 | `storage`, except `sources.default`, `sources.partitionOverwriteMode`, `json.enablePartialResults`, and the datetime rebase keys if the dialect reads Parquet |
| `spark.sql.execution.*`, `spark.sql.adaptive.*`, `spark.sql.codegen.*`, `spark.sql.cbo.*`, `spark.sql.statistics.*`, `spark.sql.inMemoryColumnarStorage.*`, `spark.sql.shuffle.*`, join / sort / exchange / broadcast keys | about 110 | `execution` |
| `spark.sql.ui.*`, `spark.sql.thriftserver.*`, `spark.sql.thriftServer.*`, `spark.sql.repl.*`, `spark.sql.pyspark.*`, `spark.sql.cli.*`, `spark.sql.debug.*`, `spark.sql.planChangeLog.*`, `spark.sql.redaction.*` | about 25 | `client` |
| `spark.sql.ansi.*` | 4 | `semantic` |
| the remaining single keys (`caseSensitive`, `session.timeZone`, `timestampType`, `mapKeyDedupPolicy`, `storeAssignmentPolicy`, `groupByOrdinal`, ...) | about 75 | check each: most are `semantic` |
| static keys (`warehouse.dir`, `catalogImplementation`, `globalTempDatabase`, `extensions`, ...) | part of the above | `static` |
