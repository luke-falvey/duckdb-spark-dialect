# Plan: closing every gap in Spark 3.5 parity coverage

This plan is a self-contained handoff for one or more long sessions. It assumes no memory of earlier work.
Read, in this order: `README.md` (sections "Testing" and "Limitations"), the status section at the top of
`docs/plans/spark-parity-testing.md`, `docs/plans/spark-parity/known_differences.md`, and
`docs/plans/user-feedback-fixes.md` sections 3 and 6.

Keep a status section at the top of this file up to date as you work (the same `[x]` / `[ ]` form as
`spark-parity-testing.md`), so that a later session can continue.

## 1. Where `main` stands

Measured on `main` at `75572d4`.

**Tests.** 71,598 statements in 161 case files, compared with Spark 3.5.9 for rows, column types, chosen
column names and error phase, in both dialects, under four configurations. Zero `FAIL`, zero `XPASS`.

**Expected failures.** The default configuration alone has 5,764 `XFAIL` cases in `spark` and 19,339 in
`spark_strict` (`COVERAGE.md`). Over all configurations and both dialects, `known_differences.md` lists 73
ids. The large ones:

| id | cases | cause |
|---|---:|---|
| `types-interval` | 9,611 | one DuckDB `INTERVAL` for Spark's 13 ANSI interval types and the legacy one |
| `conf-rejected` | 7,174 | 54 result-changing settings are rejected instead of emulated |
| `ansi-implicit-cast` | 6,024 | ANSI coercion of function arguments and comparisons |
| `types-open-c1` ... `c16` | about 9,600 | differences the type matrices found, not yet traced to causes |
| `types-ntz` | 2,155 | `TIMESTAMP_NTZ` and `TIMESTAMP` are one type |
| `corpus-open` | 377 | Spark's own test inputs |
| 60 smaller ids | about 1,500 | see `known_differences.md` |

**What the tests never touch.** `scripts/spark_coverage.py` (added with this plan) runs every test
statement through Spark's own parser and analyzer. Result in
`docs/plans/spark-parity/coverage_baseline.json`:

| universe (from Spark 3.5.9 itself) | size | reached by a test | not reached |
|---|---:|---:|---:|
| grammar alternatives of `SqlBaseParser` | 335 | 222 | 113 |
| statement kinds (alternatives of the `statement` rule) | 72 | 35 | 37 |
| keywords and tokens | 377 | 262 | 115 |
| Catalyst expression classes, in analyzed plans | 559 | 411 | 148 |
| logical plan classes, in analyzed plans | 175 | 38 | 137 |
| named error classes (`error-classes.json`) | 544 | 101 | 443 |

Examples of what is not reached: `MERGE`, `UPDATE`, `DELETE`, multi-insert, `REPLACE TABLE`,
`SHOW FUNCTIONS / COLUMNS / PARTITIONS / VIEWS / NAMESPACES / TBLPROPERTIES`, `DESCRIBE QUERY`,
`ANALYZE`, `LOAD DATA`, `CREATE FUNCTION`, `TRANSFORM`, `LATERAL` subqueries, parameter markers (`?`,
`:name`), named arguments of table functions, `TABLESAMPLE (n BYTES)`, `BUCKETS`, `UNPIVOT` with
several value columns, `NOT LIKE ALL`, `extract` from intervals, `OVERLAPS`, `IGNORE / RESPECT NULLS`
keywords, `FETCH`, time travel (`VERSION AS OF`).

The error class row counts the classes in the recorded Spark results, which include run-time errors; the
JSON file reports 70, the classes Spark's analyzer raises under the default configuration.

Caveats of these numbers, to fix in workstream L: an item counts as reached even when every case that
reaches it is an expected failure; plan classes are undercounted because Spark replaces many nodes
during analysis and executes commands eagerly; some classes cannot be reached from SQL at all.

**Dimensions with no systematic test at all** (found by reading the tests, not measured):

- how kernels behave on DuckDB's vector shapes (constant, dictionary, selection, more than 2,048 rows):
  almost every case runs on literals or on tables of at most 12 rows
- data-dependent semantics at volume (join multiplicity, grouping, sort and top-N ties, wide decimals in
  `sum`, frames over large partitions): only TPC-H on a small data set
- the mini-languages inside string arguments: datetime patterns, number formats, `format_string`,
  regular expressions, JSON paths, option maps, schema strings: covered only as far as Spark's
  documented examples use them
- session surfaces: prepared statements and parameters, several connections, database files reopened,
  Arrow / pandas export of the dialect's own types
- the extension's own code: no line coverage is measured, so untested branches are unknown; the
  sanitizer build runs only the mutation fuzzer, not the case files

## 2. What "no gaps" means

Two conditions, both checked by a script in CI.

**Condition 1: the ledger is closed.** `test/coverage/ledger.toml` lists every universe of section 6.
Every item of every universe is in exactly one state:

- `covered`: at least one gating case reaches it, and that case is not an expected failure in
  `spark_strict`; or
- `excluded`: it carries a reason from the fixed list in section 10 (for example "not reachable from
  SQL", "Python UDF").

No item is unclassified, and no item is `covered` only by expected failures.

**Condition 2: the expected-failure list is closed.** `test/xfail.toml` contains only ids named in the
`[permanent]` table of the ledger. That table has at most 12 ids, each with a reason why Spark's behaviour
cannot be reproduced and with the text that appears in the README. Every other id is gone because its
cases pass.

End state in numbers: 0 unclassified ledger items; 0 `FAIL`, 0 `XPASS`; at most 12 xfail ids;
error class agreement in `spark_strict` 100% of cases outside the permanent ids (today 9,286 of 26,090);
line coverage of `src/` 100% of lines not marked as excluded; fuzzers saturated (section 8).

## 3. Rules

1. Spark 3.5 is the only reference. Expected results come from `test/make_golden.py` or a live Spark
   session. Never edit a golden file or write an expected value by hand.
2. Never make a case pass by weakening the harness, deleting the case, or steering a generator around it.
3. Tests first. For each universe: write the generator, record from Spark, run, mark what fails, commit.
   Then fix.
4. Fix causes, not cases. Before touching code for an id with more than 50 cases, cluster its failures
   (section 7.1) and write the list of causes into the status section. A fix is done when its cause's
   cluster is empty, not when a count goes down.
5. After every fix: run all eight configuration-by-dialect runs (`scripts/dev/run_all_configs.sh`) with the
   existing `test/xfail.toml` first. Any `FAIL` is a regression: fix it before regenerating entries
   (`scripts/dev/regen_xfail.sh`). Never regenerate to hide a regression.
6. Each fixed bug that a generator did not already cover gets a case in `test/cases/regressions.sql`.
7. The default DuckDB dialect must not change (`test/fuzz/isolation.py`). Performance must stay within
   `bench/baseline.json`; a setting is read at bind time, never per row.
8. Commit per step, in the style of the history (`fix: ...; golden cases from Spark`). Push to the
   working branch. Open a pull request only when the user asks.
9. Decisions are in section 10. Follow them without asking. Ask the user only for something that would
   change the default DuckDB dialect, remove a feature, or add a thirteenth permanent id.

## 4. Environment

As in `docs/plans/user-feedback-fixes.md` section 6. In short:

- Build against DuckDB v2.0.0 as in `.github/workflows/ci.yml` (first build about 35 minutes on 4 cores,
  then about 1 minute). Copy to `build/spark_dialect.duckdb_extension`.
- PySpark: use a virtual environment (`python3 -m venv`, then `pip install "pyspark==3.5.*"
  duckdb==2.0.0`); the system `pip` cannot build the wheel. A JDK (for `javac`) is needed by
  `scripts/spark_coverage.py`.
- A full default run takes about 25 minutes per dialect. Do not run Spark recording and the comparison
  runs at the same time (one memory limit).
- `extensions.duckdb.org` is blocked in cloud sessions; CI can reach it.

## 5. Workstream L: the ledger

Build this first. Everything else reports into it.

1. `test/coverage/ledger.toml`: one table per universe with `source` (how the items are enumerated from
   Spark), `measure` (how a case is mapped to items), and the `excluded` items with reasons. Plus the
   `[permanent]` table of condition 2.
2. Extend `test/coverage/SparkCoverage.java` and `scripts/spark_coverage.py`:
   - write, per case, the items it reaches (not only totals), so that outcome can be joined in;
   - apply each file's `@config` / `@matrix` / `@set` settings before analysing its statements;
   - take plan classes from the **parsed** plan (`sessionState.sqlParser.parsePlan`) as well as the
     analyzed one, so that `Pivot`, `Unpivot`, `GroupingSets`, `LateralJoin`, `MergeIntoTable` and the
     command nodes are seen before Spark rewrites or executes them;
   - take expression classes from parsed, analyzed and optimized plans (three separate columns);
   - take error classes from the golden files (they include runtime errors), not from analysis alone.
3. `test/coverage/check_ledger.py`, run in CI after the comparison jobs: joins the per-case items with
   the results of `run_duckdb.py --results` for `spark_strict`, and fails when an item is unclassified,
   when a `covered` item is reached only by `XFAIL` cases, or when an `excluded` item has no reason from
   section 10.
4. `scripts/coverage_report.py`: add a "Parity ledger" table to `COVERAGE.md` (universe, size, covered,
   excluded by reason, open).
5. The ledger starts with the open counts of section 1. It only ever shrinks: CI fails if an item moves
   from `covered` back to open.

## 6. Workstream U: universes and the cases that close them

Each universe below names its source, what a case must do to cover an item, and the generator to write.
New case files go under `test/cases/<area>/`; generators under `scripts/gen_*_cases.py`, deterministic,
with their output committed and checked for freshness in the CI unit job like the existing ones.

Every generated statement follows the existing matrix conventions (`scripts/gen_type_cases.py`): literal
form and column form, `typeof` next to the value, one statement per cell that can fail, `-- @exact`.

### U1 Grammar alternatives and keywords (335 and 377 items)

- Source: `SparkCoverage universe`. Measure: Spark's parse tree of the case.
- Write `test/cases/grammar/<rule>.sql` by hand for the 113 unreached alternatives and 115 keywords, one
  group per parent rule. For each alternative: the minimal statement, one realistic statement, and one
  with every optional part of the alternative present. Then two negative cases: a near-miss that Spark
  rejects, and the DuckDB spelling of the same feature if DuckDB has one (must fail in `spark_strict`).
- Non-reserved keywords: every keyword Spark allows as an identifier is used as a column name, a table
  alias, a function-call name where legal, and a struct field. Generate from Spark's own lists
  (`SqlBaseParser` rules `ansiNonReserved`, `nonReserved`, `strictNonReserved`): 3 statements per keyword.
- Precedence and associativity: generate every pair of adjacent binary and unary operators without
  parentheses (`a OP1 b OP2 c`) over values that make the two parse trees give different results, and
  compare. About 40 operators give 1,600 pairs.
- Lexical forms: comments (`--`, `/* */`, nested, hints `/*+ ... */`), every numeric literal form,
  every string prefix, Unicode escapes, backtick identifiers with doubled backticks, identifiers that
  start with digits, statements ending in `;`, whitespace forms.

### U2 Statements and commands (72 kinds)

- Source: alternatives of `statement`, plus `dmlStatementNoWith`, `insertInto`, the `ALTER` forms.
- Create `test/coverage/commands.toml`, the analogue of the settings inventory: one entry per statement
  kind with `status` = `supported` | `accepted` (parsed, no effect, documented) | `rejected` (clear error
  in both dialects) | `excluded` (section 10), and its case file.
- For every `supported` kind write `test/cases/commands/<kind>.sql`: every clause of the grammar
  alternative, the result set of the command (column names and types are compared: `SHOW`, `DESCRIBE`,
  `SET`, `EXPLAIN` only for success), and the state afterwards (a following query).
- `rejected` means the same class of error as Spark where Spark also rejects it (for example Spark 3.5
  rejects `UPDATE` / `DELETE` / `MERGE` on a parquet table; record what Spark does and match it), and
  `[UNSUPPORTED_FEATURE]` where Spark supports it and the dialect does not. The second kind counts
  against condition 2: each needs a decision of section 10 or an implementation.
- Priority for implementation, because users meet them: `SHOW TABLES / VIEWS / COLUMNS / FUNCTIONS /
  NAMESPACES / TBLPROPERTIES / CREATE TABLE`, `DESCRIBE TABLE / QUERY / FUNCTION / NAMESPACE`,
  `ALTER TABLE` rename / drop / change column, `REPLACE TABLE`, `CREATE TABLE LIKE`, `TRUNCATE`,
  `ANALYZE` (accepted), `USE`, `SET CATALOG`, `CACHE` / `UNCACHE` / `CLEAR CACHE`, multi-insert
  (`FROM t INSERT ... INSERT ...`), `INSERT ... REPLACE WHERE`, `CREATE TEMPORARY VIEW ... USING`.

### U3 and U4 Catalyst expressions and plan nodes (559 and 175 classes)

- Source: concrete subclasses of `Expression` and `LogicalPlan` in the catalyst jar. Measure: parsed,
  analyzed and optimized plans (workstream L).
- First classify the unreached classes: reachable from SQL or not. Decide by searching branch-3.5 of
  Spark for where the class is constructed (`AstBuilder`, `FunctionRegistry`, an analyzer rule, an
  optimizer rule). Not reachable: `excluded`, reason "not reachable from SQL" with the constructing site.
- For each reachable class write cases in `test/cases/catalyst/`. The baseline already shows these are
  reachable and untested: `LateralSubquery` / `LateralJoin`, `NotLikeAll`, `ExtractIntervalYears` ...
  `ExtractIntervalSeconds`, `ExtractANSIIntervalHours`, `MultiplyInterval`, `DivideInterval`,
  `DateAddInterval`, `NamedArgumentExpression`, `PosParameter`, `NamedParameter`, `HiveHash`, `Product`,
  `WindowTime`, `YearOfWeek`, `InSet` (an `IN` list longer than `spark.sql.optimizer.inSetConversionThreshold`),
  `ReplicateRows` (`EXCEPT ALL`, `INTERSECT ALL`), `WindowGroupLimit`, `Tail`, `Deduplicate`,
  `RebalancePartitions` and the other hints.
- Optimizer-only classes matter because Spark's optimized expression can behave differently from the
  unoptimized one (the `InSet` path treats NULL and NaN on its own code path). For each optimizer rule in
  `spark.sql.optimizer.excludedRules`' legal values that rewrites expressions, add one case that the rule
  fires on, and record Spark with the rule on. This is the only place the ledger looks at optimized plans.

### U5 Functions: every signature, not one example

- Source: `FunctionRegistry.builtin` (438 names) with, per function, the arities Spark accepts. Get the
  arities by probing Spark with 0 to 8 arguments of type `NULL` and recording
  `WRONG_NUM_ARGS` (script `scripts/probe_function_arities.py`, output committed as
  `test/spark35_function_arities.json`).
- Items: (function, arity), (function, slot, type), (function, slot, NULL), (function, modifier).
- Extend `test/fuzz/args_sweep.py --types` (matrix C6): today one example per function and at most 4
  slots. Cover every accepted arity and every slot, with 22 types, literal and column form. Expect about
  60,000 statements; record once into `signatures.json.gz` as today and again under ANSI.
- Modifier matrix for aggregates and window functions, generated from the function list: `DISTINCT`,
  `FILTER (WHERE ...)`, `OVER ()`, `OVER (PARTITION BY ... ORDER BY ...)` with each frame kind (`ROWS`,
  `RANGE` with numeric / date / timestamp / interval bounds, unbounded, current row, exclusion not in
  Spark), `IGNORE NULLS` / `RESPECT NULLS`, `WITHIN GROUP`, inside `GROUPING SETS` / `ROLLUP` / `CUBE`,
  as an argument of another aggregate (rejected), in `HAVING`, in `ORDER BY`. Record Spark's verdict for
  each (function, modifier). This closes `macro-aggregate` as a tested matrix instead of a note.
- Generators (`explode`, `posexplode`, `inline`, `stack`, `json_tuple`, `*_outer`) in each position:
  select list, `LATERAL VIEW`, `LATERAL VIEW OUTER`, `FROM`, nested in an expression (rejected), two in
  one select list (rejected), with and without aliases.
- Higher-order functions: lambdas with 1, 2 and 3 parameters where legal, capturing outer columns,
  nested lambdas, a lambda parameter shadowing a column, NULL arrays and NULL elements.
- Named arguments (`mask(str => ...)`, table functions) for every function Spark gives named parameters.
- Nondeterministic functions (`rand`, `uuid`, `shuffle`, `current_timestamp`, ...): where Spark is
  seeded (`rand(42)`), decide per function whether the sequence is reproduced; otherwise test the
  properties (type, range, distinctness, same value within a statement for `current_timestamp`).

### U6 Error classes (544 named)

- Source: `error/error-classes.json` in `spark-common-utils`. Ignore `_LEGACY_ERROR_TEMP_*` as items, but
  see the last point.
- Classify each class: raised by SQL the dialect handles, or `excluded` (streaming, connect, Python,
  Hive, data source internals, protobuf, ...). Use the class's message text and a search of branch-3.5
  for its raise sites.
- For each in-scope class write a witness in `test/cases/errors/<CLASS>.sql`: one statement per distinct
  raise site you can reach, literal and column form. Spark's own tests name the class next to a query
  (`checkError(... errorClass = "X")` in `sql/core/src/test` and the `.sql.out` files): search there
  first.
- Gate: in `spark_strict` the dialect raises the same class, in the same phase. Extend the gating of
  error classes from the type and settings files to **every** file. Expect a large number of new
  expected failures at first (17,000 cases disagree today); they belong to the burn-down of section 7,
  cause "error class".
- Message parameters: compare the placeholders Spark fills (`<sqlExpr>`, `<inputType>`, ...) only for
  the classes users match on in practice: `CAST_INVALID_INPUT`, `DIVIDE_BY_ZERO`, `UNRESOLVED_COLUMN`,
  `UNRESOLVED_ROUTINE`, `TABLE_OR_VIEW_NOT_FOUND`. Everything else: class and phase only.
- 1,601 recorded Spark errors carry no class (legacy messages). Group them by message template and treat
  each template as an item with phase-only comparison.

### U7 Settings

The inventory is complete (478 keys). What remains is in section 7 (emulating the rejected ones) and
section 8 (fuzzing with ANSI included).

Add one check: `test/settings/check_inventory.py` fails if a setting with `status = "emulated"` has
fewer than 20 cases per legal value, or has no case in `interactions.sql`.

### U8 Types

The matrices C1 to C18 exist. Extend the type table of `scripts/gen_type_cases.py`:

- all 13 ANSI interval types and the legacy mixed interval (today two plus two);
- decimals at every boundary the rules branch on: precision 9 / 10, 18 / 19, 38, scale 0 and scale equal
  to precision, negative values;
- `CHAR(n)` and `VARCHAR(n)` as operands in **every** matrix (today mainly C1, C7, C17);
- nesting to depth 3, empty and NULL at every level, `void` inside arrays, maps and structs;
- every legal map key type; struct fields with names that need quoting, duplicate names, and names that
  differ only in case.

Then regenerate and record. Size grows by roughly half; keep C5's reduced pair set for the secondary
contexts.

### U9 Mini-languages inside arguments

One generator per language, `scripts/gen_format_cases.py`, into `test/cases/formats/`. The item list of
each language comes from Spark's source (named below); every item is exercised alone, repeated to each
legal count, combined pairwise, and with one invalid neighbour.

| language | functions | items from |
|---|---|---|
| datetime patterns | `date_format`, `to_date`, `to_timestamp*`, `unix_timestamp`, `to_unix_timestamp`, `from_unixtime`, `try_to_timestamp`, `to_char` of datetimes, `dateFormat` / `timestampFormat` options | `DateTimeFormatterHelper`: every pattern letter, counts 1 to 10, formatting and parsing separately, the letters legal only for formatting, quoted text, optional sections `[...]`, fractions `S` to `SSSSSSSSS`, zone letters `z O X x Z V`, era and week letters |
| number formats | `to_char`, `to_varchar`, `to_number`, `try_to_number`, `to_binary` formats | `ToNumberParser`: `0 9 . D , G $ S MI PR`, positions, widths against the value's precision; closes `types-to-number`, `types-to-char` |
| `format_string` / `printf` | both | Java `Formatter`: every conversion with every flag, width and precision, argument indexes, for every argument type |
| regular expressions | `rlike`, `regexp`, `regexp_like`, `regexp_extract`, `regexp_extract_all`, `regexp_replace`, `regexp_instr`, `regexp_count`, `regexp_substr`, `split`, `str_to_map`, `sentences` | Java `Pattern` syntax table: every construct, including those RE2 lacks (backreferences, lookaround, possessive and atomic groups, Unicode classes, inline flags); replacement syntax `$1`, `${name}`, `\$` |
| `LIKE` family | `LIKE`, `ILIKE`, `LIKE ALL / ANY / SOME`, `NOT` forms, `ESCAPE` | every wildcard and escape combination, multi-byte, patterns ending in the escape |
| JSON | `get_json_object`, `json_tuple`, `from_json`, `to_json`, `schema_of_json`, `json_array_length`, `json_object_keys` | path syntax; every key of `JSONOptions`; every value type; malformed input under each `mode` |
| CSV | `from_csv`, `to_csv`, `schema_of_csv` | every key of `CSVOptions` |
| XPath / XML | `xpath*` | node, attribute, predicate, function forms |
| schema and type strings | `from_json`, `from_csv`, `CAST` type names, `typeof` output, `schema_of_*` output | DDL schema grammar: every type, nesting, `NOT NULL`, comments, quoted names |
| string-to-value casts | `CAST(str AS T)` for every T | `UTF8String` / `DateTimeUtils` / `IntervalUtils` parsers: every accepted spelling of dates, timestamps (with zones), intervals (all three families), booleans, numbers |
| time zone ids | `session.timeZone`, `from_utc_timestamp`, `to_utc_timestamp`, `convert_timezone`, `make_timestamp`, zone suffixes in strings | every id of `ZoneId.getAvailableZoneIds()`, offset forms, short ids, DST gap and overlap instants per zone family; closes `tz-zone-names` |
| character sets and Unicode | `encode`, `decode`, `upper`, `lower`, `initcap`, `length`, `substring`, `reverse`, `levenshtein`, `soundex`, ordering | every charset Spark accepts; special-casing characters (`ß`, `İ`, final sigma), surrogate pairs, combining marks, invalid UTF-8 in `BINARY` to `STRING` |
| hashes | `hash`, `xxhash64`, `md5`, `sha*`, `crc32` | every type including nested, decimals of each physical width, `-0.0`, `NaN`, NULL positions |

### U10 Execution shapes

Purpose: a kernel that is right on literals can be wrong on a dictionary vector or past row 2,048.

- Build `test/shapes/run_shapes.py`. It takes the argument tuples of every C6 / U5 cell whose Spark
  result is recorded, and evaluates the same call in the dialect over a table that holds those tuples in
  each shape:
  constant vector (one row cross-joined); flat; dictionary (through a join); selection vector (under a
  filter that keeps every third row); all NULL; a single non-NULL row among NULLs; 2,047, 2,048, 2,049
  and 130,000 rows (past a vector and past a row group); `threads` 1 and 4; inside `CASE` branches
  (kernels must not raise for rows the branch does not select).
- The expected value per row is Spark's recorded result for that tuple. No new Spark run is needed.
- A mismatch is a failing case; add the reduced reproducer to `test/cases/regressions.sql` in a form
  Spark can also run (a `VALUES` table and a filter).
- Run the case files once under the AddressSanitizer / UBSan build in the nightly workflow (today only
  the mutation fuzzer uses it).

### U11 Data-dependent semantics at volume

- `test/fuzz/differential.py --scale N`: the fuzz tables `t` and `u` with N rows of seeded random data
  drawn from the edge-value sets plus random values. Spark and the dialect load the same data from
  Parquet files written once per seed. Run query shapes that depend on volume: joins of every type with
  duplicate and NULL keys, `GROUP BY` with many groups, `DISTINCT`, set operations with `ALL`, top-N with
  ties, windows with every frame over partitions of thousands of rows, `sum` / `avg` of wide decimals,
  floats (compare with tolerance, state why), `percentile*`, `collect_list` after `ORDER BY`.
- Order: results are compared as multisets unless the query has a total order. Add a check that flags a
  generated query whose `ORDER BY` is not total, instead of comparing it loosely.
- TPC-DS: the 99 queries of `bench/tpcds/` at scale 0.01 as gating cases against Spark
  (`test/cases/tpcds_small.sql`), like `tpch_small.sql`. Generate the data in CI with DuckDB's `tpcds`
  extension, export to Parquet, load in both engines. In a cloud session, where the extension cannot be
  downloaded, generate the data from the committed Parquet files of a CI artifact or skip locally and let
  CI record.

### U12 Session and API surfaces

`test/api/test_session.py` (pytest, with a Spark session where a Spark answer exists):

- Parameters: Spark's `spark.sql(sql, args)` with named (`:x`) and positional (`?`) markers against
  DuckDB prepared statements in the dialect, for every type of U8; markers in every clause; a statement
  prepared before and executed after a `SET`.
- Scripts: several statements in one string; a failing statement in the middle; comments between.
- Connections: two connections with different dialects and settings on one database; a setting changed
  on one must not affect the other; `SET current_dialect` back and forth inside a transaction.
- Persistence: a database file written in the dialect (tables with every type of U8, views, `CHAR`
  columns) and reopened, in the dialect and in the default dialect; views defined in one dialect and
  read in the other.
- Export: `fetchall`, `fetchnumpy`, Arrow and pandas for every type of U8, including the dialect's own
  types (`spark_char`, and the interval and `TIMESTAMP_NTZ` types of section 7).
- `EXPLAIN` and `EXPLAIN ANALYZE` succeed for every statement kind; no internal function name leaks
  into error messages (`spark_corpus.py names` reports 2: bring to 0 and gate it).

### U13 The extension's own code

- Coverage build: `-DCMAKE_CXX_FLAGS=--coverage` (or clang's source-based coverage) for the extension's
  sources only. Script `scripts/dev/coverage_build.sh`; report with `gcovr` filtered to `src/`.
- Run the default configuration in both dialects plus the ANSI configuration, then report uncovered
  lines and branches per file. For each uncovered region: write the Spark-compared case that reaches it,
  or delete the code if nothing can reach it, or mark it `// LCOV_EXCL_LINE` with a reason (internal
  assertions, out-of-memory paths). The ledger universe is "lines of `src/`"; the gate is zero uncovered
  unmarked lines. Add the measurement to the nightly workflow.
- Mutation sample: `scripts/dev/mutate_src.py` applies 300 seeded single-token mutations to `src/`
  (comparison operators, `+ 1` / `- 1`, swapped branches, a changed constant), rebuilds, and runs the
  base case group. A surviving mutant means a behaviour no test pins down: add the case. Target: at most
  2% survivors that are not equivalent mutants. Run once near the end, not in CI.

### U14 Spark's own tests

- Promote **every** input under Spark branch-3.5 `sql/core/src/test/resources/sql-tests/inputs/` to
  `test/cases/spark_corpus/` (today 54 files), honouring the `--SET`, `--CONFIG_DIM` and `--IMPORT` lines.
  Inputs that need what section 10 excludes (UDFs, Hive, `TRANSFORM` scripts) are `excluded` items in the
  ledger, by file, with the reason. Remove the separate `SKIP` list of `test/fuzz/spark_corpus.py` in
  favour of the ledger.
- Mine Spark's Scala test suites for SQL text: every string passed to `sql("...")` in
  `sql/core/src/test/scala/org/apache/spark/sql/` (`SQLQuerySuite`, `DataFrameFunctionsSuite`,
  `*FunctionsSuite`, `CastSuite`-style SQL, `errors/Query*ErrorsSuite`). Extract with a script, keep
  statements that Spark runs standalone (after their suite's setup statements where those are also
  `sql(...)` calls), de-duplicate against existing cases by Spark parse tree, and add the rest to
  `test/cases/spark_suites/`. Expect several thousand statements; they are the cheapest source of cases
  written by people who knew the corner cases.

## 7. Workstream X: closing the expected failures

### 7.1 Triage before fixing

For each id above 50 cases: run `scripts/show_failures.py` / `scripts/triage.py`, cluster by (matrix,
context, type pair, kind of difference: value, type, phase, error class, lenient). Write the clusters and
their causes into the status section. Replace the catch-all ids (`types-open-c*`, `corpus-open`,
`settings-*`) by cause ids in `test/xfail.toml`. One cause, one id.

### 7.2 Large causes, with the intended design

Do them in this order; each removes the most cases per unit of work and unblocks later ones.

1. **Interval types (`types-interval`, `types-open-c10`, `try-aggregate-interval`, part of U3).**
   Register the 13 ANSI interval types and the legacy interval as extension types, following the pattern
   of `spark_char` in `src/spark_extras.cpp` (`RegisterType` with its casts). Storage stays DuckDB
   `INTERVAL`, with the invariant that a year-month value has only months and a day-time value only
   microseconds. Required behaviour, each already specified by cases in C1, C3, C4, C5, C10, C12:
   literal typing (both syntaxes); `typeof` and the harness mapping; result types of `+ - * /` (the
   widest field range of the operands); field range checks and overflow errors; casts between interval
   types and to and from strings and numbers; comparison and ordering within a family, rejection across
   families; `date ± year-month`, `timestamp ± day-time`, `date - date`, `timestamp - timestamp`;
   `sum`, `avg`, `min`, `max`, `try_sum`, `try_avg`; `extract` and the `make_*_interval` functions;
   rendering; storing in tables; `legacy.interval.enabled` then becomes implementable.
   If registered types cannot carry these through DuckDB's binder (for example implicit casts to plain
   `INTERVAL` erase them in `UNION`), record exactly where, and fall back to tracking the type in the
   extension's bind functions for those sites. `types-interval` may become permanent only for the sites
   proven impossible, listed one by one.
2. **`TIMESTAMP_NTZ` (`types-ntz`).** Register `spark_timestamp_ntz` over DuckDB `TIMESTAMP` the same
   way. Spark `TIMESTAMP` keeps its current representation and session-zone handling. Then implement
   `spark.sql.timestampType`, the casts between the two (they depend on the session zone), the functions
   that return NTZ (`to_timestamp_ntz`, `make_timestamp_ntz`, `localtimestamp`, `convert_timezone`), and
   the comparison rules. This replaces section 9.3 of the earlier plan: no remap to `TIMESTAMPTZ`.
3. **ANSI coercion of function arguments and comparisons (`ansi-implicit-cast`).** Record
   `signatures.json.gz` under ANSI. Generate the ANSI argument rules from it with
   `scripts/gen_arg_type_rules.py`, as was done for the non-ANSI rules (`src/spark_arg_types.inc`).
   Port `AnsiTypeCoercion.findWiderTypeForTwo` and `implicitCast` for comparisons. Then the remaining
   ANSI runtime errors the README lists: missing map keys, `avg` overflow, `pmod`, datetime arithmetic and
   parsing.
4. **Statement-level coercion DuckDB's binder owns** (causes named in the status of
   `spark-parity-testing.md`: JOIN `USING` / `NATURAL` keys, `IN (subquery)`, set operations without a
   common type or with string literals, `VALUES` typing, `INSERT` assignment casts and
   `storeAssignmentPolicy`, aggregates over types Spark rejects). These all need the types of a bound
   subtree before the statement is bound. Spike first (one day at most): find, in DuckDB v2.0.0, a
   supported way for the extension to see bound types at statement level. Candidates, in order: an
   `OptimizerExtension` / operator rewrite that inserts the casts after binding; binding the subquery
   once with `Planner` from inside a bind function to learn its types and then rewriting; a
   table-function wrapper around the operands. Choose one mechanism and use it for all of them.
   `INSERT` already matches structs by position, so there is a hook for the target types.
5. **Settings decided at parse time (`conf-rejected`, 54 settings).** The tokenizer and grammar have no
   client context today, which is why `groupByOrdinal`, `orderByOrdinal`, `groupByAliases`,
   `parser.escapedStringLiterals`, `parser.quotedRegexColumnNames`, `ansi.doubleQuotedIdentifiers`,
   `ansi.enforceReservedKeywords`, `ansi.relationPrecedence`, `legacy.setopsPrecedence.enabled`,
   `variable.substitute`, `legacy.exponentLiteralAsDecimal.enabled` and others are rejected. Spike:
   (a) check whether DuckDB's dialect hooks can reach the `ClientContext` or a per-connection state at
   tokenize time; (b) if not, have `SET` of such a key switch the connection to a variant of the
   dialect whose tokenizer reads a per-connection flag block. Then emulate, in order of use: the
   ordinal and alias switches, the string-literal and identifier switches, set-operation precedence,
   `variable.substitute`, `legacy.timeParserPolicy=CORRECTED` (no legacy parser needed: where the new
   parser fails the result is NULL), `storeAssignmentPolicy` (with item 4), `charAsVarchar`,
   `legacy.charVarcharAsString`, `readSideCharPadding`, `legacy.castComplexTypesToString.enabled`,
   `legacy.nullInEmptyListBehavior`, `legacy.allowParameterlessCount`, the remaining bind-time legacy
   flags. Settings that remain rejected at the end each need a line in the `[permanent]` table; they
   share the single id `conf-rejected`.
6. **Generated column names (`names-generated`).** Implement Spark's naming of unaliased select items
   (`toPrettySQL`) in the grammar transform, starting from the existing `COALESCE` naming. Specify it by
   test first: generate, for every expression class of U3 in a select list, the Spark column name
   (`test/cases/names/`), then make name comparison gating for all columns, not only chosen names.
7. **Aggregates that are macros (`macro-aggregate`).** Reimplement `stddev*`, `var_samp`, `kurtosis`,
   `skewness`, `median`, `try_sum`, `try_avg` and the rest as real aggregate functions (as was done for
   `var_pop`, `regr_*`), so that `OVER`, `DISTINCT` and `FILTER` apply. U5's modifier matrix is the test.
8. **Views capture settings (`view-captures-settings`)**: store the emulated settings in force with the
   view and apply them when it is expanded.
9. **Error classes** (U6): after the mechanisms above exist, map every error the extension raises to
   Spark's class at the raise site, and classify DuckDB's own binder and conversion errors in one place.

### 7.3 Small ids

Each of the remaining ids in `known_differences.md` gets one of: fix (default), or a `[permanent]` entry.
Fix by default: `missing-function`, `types-to-number`, `types-to-char`, `types-ceil-scale`,
`types-percentile-disc`, `types-pivot`, `types-lambda-index`, `types-struct-field-names`, `types-void`,
`tz-window`, `tz-zone-names`, `foldable-arguments`, `subquery-argument`, `datasource-types`,
`lenient-generator-window`, `strict-array-function-types`, every `settings-*` id, every `phase-*` id
(make the bind step defer the error to run time, or raise it early, as Spark does).

Candidates for `[permanent]`, to be confirmed by trying: approximate aggregates with different
algorithms (`approx_percentile`, `approx_count_distinct`, `histogram_numeric`); `array_sort` with an
inconsistent comparator; errors that depend on what Spark's optimizer removes; `spark.sql.caseSensitive`
if the spikes of 7.2 items 4 and 5 give no way to resolve identifiers case-sensitively; date ranges beyond DuckDB's.

## 8. Workstream F: fuzzers to saturation

1. `test/fuzz/compare.py`: make `type_mismatch` and `phase_mismatch` failing kinds (they are report-only
   today) once sections 7.2 items 1 to 3 are done.
2. `test/fuzz/gen.py`: generate from the ledger. Every grammar alternative, expression class and
   function of U1, U3 and U5 gets a production; a run reports which items its queries reached (through
   `SparkCoverage parse`), and the generator weights unreached items higher.
3. `differential.py --conf-fuzz`: include `spark.sql.ansi.enabled` and every setting emulated by then.
4. Saturation rule, replacing fixed budgets: a fuzzer is saturated when three consecutive runs with
   different seeds, of 50,000 queries each (`mutate.py`: 500,000 inputs each), produce no finding
   signature that is not already fixed or permanent. Record the three seeds and summaries in the status
   section. Run to saturation: `mutate.py` (also under the sanitizer build), `args_sweep.py`,
   `differential.py`, `differential.py --conf-fuzz`, `differential.py --scale 100000`, `isolation.py`.
5. `test/fuzz/known_differences.toml` ends as the same list as the ledger's `[permanent]` table.

## 9. Workstream H: harness

- Error class gating in `spark_strict` for every file (U6).
- Column name gating for every column (after 7.2 item 6).
- Tolerance: list the functions whose results depend on the platform's math library (the reason C6
  compares with tolerance), keep the tolerance for exactly those, and compare everything else exactly in
  all files. Remove the blanket 1e-9 tolerance from the other files.
- Row order: fail a case whose SQL has a top-level `ORDER BY` that is not total over the result when the
  two engines differ only in the order of tied rows, with a message that tells the author to add a
  tie-breaker, instead of today's `@unordered` escape.
- `PHASE` and `LENIENT` in the lenient `spark` dialect stay report-only by design. Print the counts in
  `COVERAGE.md` so that a growth is visible.
- `check_ledger.py`, the coverage build and the shape runner get unit tests in `test/test_harness.py`
  style.

## 10. Scope boundary (decided)

Items may be `excluded` from the ledger only for these reasons. Anything else is in scope.

| reason | what it covers |
|---|---|
| not reachable from SQL | classes built only by the DataFrame / Dataset API, encoders, object expressions, physical planning |
| user code | Scala / Java / Python / Hive UDFs, UDAFs, UDTFs, `TRANSFORM` with scripts, `ADD JAR` / `ADD FILE`, `reflect` / `java_method` |
| streaming | everything under structured streaming, watermarks |
| Hive | Hive SerDe tables, `STORED AS`, `ROW FORMAT`, Hive-only commands, the Hive metastore |
| cluster behaviour | partition ids, `input_file_name`, shuffle, repartition hints as execution behaviour (the hints must still parse and be ignored) |
| external catalogs and sources | catalogs other than `spark_catalog`, JDBC, Avro, ORC, Delta, time travel, v2 sources |
| security | `GRANT`, `REVOKE`, roles |
| Spark internals | error classes of connect, protobuf, Python workers, codegen, file-system failures |

Decided in scope, though not obvious: file-based tables (`USING parquet / csv / json`, `LOCATION`,
`PARTITIONED BY`, `` SELECT * FROM parquet.`path` ``) at the level of "data written by Spark is read with the
same values and types, and data written by the dialect is read by Spark", tested as a round trip for
every type of U8; `EXPLAIN` only as "succeeds"; `spark-sql` CLI output formatting is out (the dialect
returns values, not text).

Other decisions:

- Lenient `spark` accepting more than Spark is by design; only `spark_strict` is gated on rejection.
- A statement the dialect cannot support must raise `[UNSUPPORTED_FEATURE]` in both dialects and be
  listed in the README. It may not return a different result.
- The earlier plan's decisions 9.1 (reject, never ignore) and 9.6 stay. 9.3 and 9.4 are replaced by
  section 7.2 items 1 and 2. 9.5 is replaced by 7.2 item 6.

## 11. Order of work and checkpoints

| phase | content | checkpoint (write the numbers into the status section) |
|---|---|---|
| 0 | Build; run the eight runs; confirm zero `FAIL`; run `scripts/spark_coverage.py` | baseline matches section 1 |
| 1 | Workstream L | `check_ledger.py` runs in CI and reports the open counts |
| 2 | Triage (7.1) of every id above 50 cases | cause list; catch-all ids replaced |
| 3 | U1, U2, U3 / U4, U6 cases (tests only, failures marked) | ledger open counts for grammar, statements, classes, error classes are zero or xfail-only |
| 4 | 7.2 items 1 and 2 (types), then U8 regenerate | `types-interval`, `types-ntz` gone or reduced to proven sites |
| 5 | 7.2 items 3, 4, 5 (ANSI, statement coercion, parse-time settings) | `ansi-implicit-cast` gone; rejected settings counted |
| 6 | U5, U9, U10 | signature matrix complete; shape runner clean |
| 7 | 7.2 items 6 to 9 and 7.3 | xfail ids at most 12 |
| 8 | U11, U12, U13, U14 | TPC-DS gating; coverage build at zero uncovered unmarked lines |
| 9 | Workstream F to saturation; harness items of section 9 | three clean runs per fuzzer |
| 10 | Audit | section 12 |

Phases 3 and 4 can run in either order; phase 3 is cheap and makes later fixes visible in the ledger.
After every phase: all eight runs green, CI green, `COVERAGE.md` and `known_differences.md` regenerated,
status section updated, pushed.

CI budget: keep each shard under 30 minutes. New case groups go into `scripts/case_group.py`. On push run
the default and ANSI configurations; everything else nightly. The signature matrix and the shape runner
compare against committed Spark results and need no Spark in CI.

## 12. Definition of done

1. `check_ledger.py` passes with zero open and zero unclassified items in every universe of section 6;
   every exclusion carries a reason from section 10.
2. `test/xfail.toml` holds at most 12 ids, all in the ledger's `[permanent]` table, each with a
   reproducing example and README text. `test/fuzz/known_differences.toml` lists the same differences.
3. Zero `FAIL` and `XPASS` in both dialects in every configuration; error classes and all column names
   are gating in `spark_strict`.
4. Every setting of class `semantic` is emulated for every legal value, or is one of the settings named
   under the single permanent id `conf-rejected`.
5. Coverage build: zero uncovered lines in `src/` that are not marked with a reason. Mutation sample: at
   most 2% non-equivalent survivors.
6. Every fuzzer is saturated by the rule of section 8, including the sanitizer build.
7. Audit by a fresh session that has not done the work: it picks 50 `excluded` ledger items and 5
   permanent ids at random and tries to refute each (reach the item from SQL, or reproduce Spark's
   behaviour). Any success reopens the item.
8. `README.md`, `COVERAGE.md` and `known_differences.md` describe the final state, and the README's
   "Limitations" section contains exactly the permanent differences and the `[UNSUPPORTED_FEATURE]` list.
9. Benchmarks within `bench/baseline.json`; TPC-H and TPC-DS geometric means within 5% of the values
   before this plan.
