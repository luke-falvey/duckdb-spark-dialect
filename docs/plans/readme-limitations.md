# Plan: addressing the README's limitations

A self-contained handoff for removing (or deliberately keeping, with reasons) the items in the README's
"Limitations" section and in `test/fuzz/known_differences.toml`. Read it fully before starting. It assumes no
memory of earlier sessions.

## 1. Context

The repository is a DuckDB v2.0.0 extension with two SQL dialects, `spark` (lenient) and `spark_strict` (rejects
what Spark 3.5 rejects). Spark 3.5 is the reference: `test/make_golden.py` runs the case files in PySpark 3.5,
`test/run_duckdb.py` compares DuckDB's results with them. `test/fuzz/` holds fuzzers (see its README), `bench/`
the benchmarks (see `docs/PERFORMANCE.md`). Read the top-level README first, then `test/fuzz/README.md`.

Code map for this work:

| area | file | notes |
|---|---|---|
| token rewrites | `src/spark_tokenizer.cpp` | `SparkRewriteTokens`, `RewriteGroupingWindow`, `SET` handling, `CHAR(n)` |
| grammar and parse-time rewrites | `src/spark_grammar.cpp` | `RewriteFunction`, `FinalizeFunctionHookResult` (window aggregates), `HookDepthGuard`, strict checks (`CheckFoldableArguments`) |
| bind-time dispatchers and kernels | `src/spark_functions.cpp` | `Builder`, `SparkCast`, `SparkWiderType`, `BindAddSubMul`, `sum_typed` / `avg_typed`, `BindSparkKernel` |
| macros | `src/spark_macros.cpp` | SQL macros in `system.spark`; a macro repeats its arguments (nesting cost) |
| library kernels | `src/spark_library.cpp`, `src/spark_datetime.cpp`, `src/spark_formats.cpp`, `src/spark_extras.cpp` | `CHAR(n)` types and casts are in `RegisterSparkCasts` (spark_extras.cpp) |

The limitations, as the README states them today:

| # | limitation | section |
|---|---|---|
| L1 | ANSI mode (`spark.sql.ansi.enabled = true`) is not emulated | 6 |
| L2 | `sum` / `avg` carry an extra `any_value(x)` aggregate to get Spark's result type | 7 |
| L3 | `window` / `session_window` grouping turns `FROM` into a subquery: the source's table alias is not available | 8 |
| L4 | `CHAR(n)` padding only for `n` ≤ 255 | 9 |
| L5 | unnamed `STRUCT(...)` matches named structs by position only in `INSERT ... VALUES` and `CAST` | 10 |
| L6 | `array_sort` with a comparator is O(n²) and assumes a consistent comparator | 11 |
| L7 | nesting limit ~160 levels (a third of the thread stack) instead of DuckDB's 1000 | 12 |
| L8 | token-level rewrites can make error messages show rewritten text | 13 |
| L9 | the fuzzers' known differences (`test/fuzz/known_differences.toml`, 10 entries) and the untriaged tail | 14 |

## 2. Goals and acceptance criteria

For each limitation the outcome is one of: **removed** (the README bullet goes away), **reduced** (the bullet
is rewritten to the smaller remaining limitation), or **kept** (with a short reason in the README or in
`known_differences.toml`). Sections 6 to 14 propose the outcome and the acceptance test for each.

Overall, the work is done when:

1. every item in the table above has its outcome, and the README's Limitations section and
   `known_differences.toml` reflect it exactly (no stale bullets, no entry whose example no longer differs:
   `python test/fuzz/replay.py --known` must pass, see 5.3);
2. every behaviour change has cases in `test/cases/` with Spark golden output (new file
   `test/cases/limitations.sql`, plus `test/cases/ansi/*.sql` for L1), passing in both dialects;
3. the full suite passes in both dialects, `test/fuzz/replay.py` passes, the CI fuzz smoke stays green (the
   fixed-seed differential run must not gain findings), and `bench/report.py --check bench/baseline.json` passes;
4. TPC-H SF1 stays within 1.15x native (geometric mean) and no function in `bench/func_pairs.toml` gets more
   than 10% slower without an explanation in `docs/PERFORMANCE.md`;
5. `docs/PERFORMANCE.md` is updated where an item changes performance (L2, L6, L7).

## 3. Ground rules

- **Spark is the oracle.** Record expected results with `make_golden.py`; never edit golden files by hand.
  When unsure what Spark does, run it in PySpark first (`test/engines.py` `SparkOracle`) and write the case.
- **One limitation per branch and pull request**, in the order of section 15, each with its tests, README
  change and, where relevant, `known_differences.toml` change. Small reviewable steps beat one large change.
- **Keep the default DuckDB dialect untouched.** Nothing may change behaviour outside `spark` / `spark_strict`.
  The isolation check (`test/fuzz/isolation.py`) must stay clean.
- **Don't trade correctness for a limitation.** If removing a limitation would make other results differ from
  Spark, the item is "kept" or "reduced", not removed.
- Commit only after the full suite passes in both dialects; chain test and commit commands with `&&`, not `;`
  (a failing run must stop the commit).

## 4. Environment setup

Use a fresh container per the steps in `docs/plans/fuzz-and-performance.md` section 4 (DuckDB v2.0.0 sources in
`/home/user/duckdb`, a `RelWithDebInfo` build in `/home/user/build-rel`, `pip install duckdb==2.0.0
"pyspark==3.5.*" pandas pytz`, Java 17+). Additionally:

- Point the default extension path at the build: `mkdir -p build && ln -sf
  /home/user/build-rel/extension/spark_dialect/spark_dialect.duckdb_extension build/` (`build/` is gitignored).
- For TPC-H, build DuckDB's tpch extension from the same tree (the sandbox usually cannot download extensions):
  `cmake -S /home/user/duckdb -B /home/user/build-rel -DBUILD_EXTENSIONS=tpch` then
  `cmake --build /home/user/build-rel --target tpch_loadable_extension`, and pass
  `TPCH_EXTENSION=/home/user/build-rel/extension/tpch/tpch.duckdb_extension` to `bench/tpch_bench.py`.
- **Before any run longer than a few minutes, copy the extension** to a stable path and point
  `SPARK_EXTENSION` at the copy. Rebuilding replaces the file in place, and a worker that restarts during a long
  run then loads a half-written file.
- Disk is a fixed allowance. Debug-info builds are large (~11 GB release, ~14 GB sanitizer); delete object files
  of builds you no longer recompile. The test tools now remove their scratch directories at exit.

## 5. Shared groundwork (do first)

### 5.1 `test/cases/limitations.sql`

A new case file for everything below, sectioned per limitation (`-- ===== L3: ... =====`). Each section
starts with cases that fail today (to show the fix) and keeps working cases around them (to show nothing else
changed). Record with `python test/make_golden.py test/cases/limitations.sql`.

### 5.2 Spark configuration per case file

L1 needs Spark golden results with `spark.sql.ansi.enabled=true`. Extend the case-file format with a file-level
directive `-- @config spark.sql.ansi.enabled=true` (first lines of the file): `make_golden.py` sets those Spark
confs for that file (and resets them afterwards); `run_duckdb.py` runs the matching `SET` statements in the
dialect before the file's cases. Document it in `test/harness.py`'s docstring.

### 5.3 `replay.py --known`

`known_differences.toml` says each example "still reproduces"; nothing checks it yet. Add `--known` to
`test/fuzz/replay.py`: run every entry's `example` in Spark and in both dialects, and fail if it now matches
Spark (the entry is stale). Run it in the CI fuzz smoke job.

## 6. L1: ANSI mode

**Today.** `SET spark.sql.ansi.enabled = true` is accepted and only echoed (`spark_tokenizer.cpp`, Spark `SET`
returns a key/value row); nothing changes. Spark 3.5 with ANSI on: integer overflow, invalid casts, division by
zero, out-of-range array indexes and invalid dates raise errors instead of returning NULL / wrapping; implicit
casts follow the ANSI "store assignment" rules (e.g. string to number is not implicit); some functions change
(`element_at` errors on out-of-range, `size(NULL)` is NULL not -1, `try_*` functions remain lenient).

**Proposed outcome: reduced** (the common runtime-error semantics), with type-coercion differences kept.

**Approach.**
1. Register an extension setting `spark_ansi_enabled` (BOOLEAN, default false) with
   `DBConfig::AddExtensionOption` in `LoadInternal`. Rewrite Spark's `SET spark.sql.ansi.enabled = <bool>` (and
   `RESET`) in the tokenizer to set it, in both dialects (it is a Spark setting, so `spark_strict` allows it).
2. Read it at bind time in the dispatchers (`ClientContext` setting lookup), and pick the ANSI kernel variant:
   - `add` / `subtract` / `multiply` / `negative` / `abs` on integers: the checked kernels that already exist for
     the `try_*` functions, but raising `[ARITHMETIC_OVERFLOW]` instead of returning NULL;
   - `divide` / `div` / `remainder` / `pmod`: `[DIVIDE_BY_ZERO]`;
   - decimal results that overflow (`SparkDecimalResult`, `SparkWideDecimalMultiply`): `[NUMERIC_VALUE_OUT_OF_RANGE]`;
   - `cast_value` (`SparkCast`): invalid string to number / date / boolean, numeric overflow: `[CAST_INVALID_INPUT]`
     / `[CAST_OVERFLOW]`; `try_cast` keeps returning NULL;
   - `subscript` / `element_at`: `[INVALID_ARRAY_INDEX]`; `size(NULL)` returns NULL.
   Every kernel that can now throw must be registered fallible (`RegisterFallible` / `SetFallible`), otherwise
   DuckDB reports an internal error.
3. Do **not** emulate ANSI type coercion in this step; keep it as a known difference.
4. Bound expressions are cached in prepared statements: test that changing the setting between `PREPARE` and
   `EXECUTE` re-binds, or document that it applies at prepare time.

**Tests.** `test/cases/ansi/*.sql` with `-- @config spark.sql.ansi.enabled=true` (5.2): overflow, casts,
division, indexes, dates, `try_*`, mixed with `SET` toggling off again. Spark's own `sql-tests/inputs/ansi/`
directory (fetched by `test/fuzz/spark_corpus.py fetch`) is a finding source: add an `--ansi` option to
`spark_corpus.py` that runs it with the conf set, and triage.

**Acceptance.** All `test/cases/ansi/` cases match Spark in both dialects. The README bullet becomes "ANSI mode
covers runtime errors (overflow, casts, division, indexes); ANSI implicit-cast rules are not emulated".

## 7. L2: the extra `any_value` in `sum` / `avg`

**Today.** `sum(x)` is rewritten at parse time to `system.spark.sum_typed(sum(x), any_value(x))`
(`RewriteFunction` in `spark_grammar.cpp`; window aggregates in `FinalizeFunctionHookResult`). The dispatcher
uses `any_value(x)` only to learn the input type. Cost measured at 1.0x to 1.3x (`sum_bigint` 1.32x).

**Proposed outcome: removed.**

**Approach.** Replace the pair with real aggregate functions `system.spark.spark_sum` and `system.spark.spark_avg`
whose `bind` callback sees the argument type and selects the implementation:
- integer input: a BIGINT state with two's-complement wrap-around (Spark non-ANSI; with L1, checked);
- decimal input: a `hugeint_t` state, finalized to `DECIMAL(min(38, p + 10), s)` for `sum` and
  `DECIMAL(min(38, p + 4), min(38, s + 4))` with HALF_UP rounding for `avg`, NULL on overflow;
- floating input: DuckDB's `sum` / `avg` semantics with Spark's result type (DOUBLE);
- intervals: as `avg_typed` / `sum_typed` do today (year-month vs day-time).
Start from DuckDB's `SumFun` / `AvgFun` templates (`extension/core_functions/aggregate/`) rather than writing
states from scratch. The aggregates must support DISTINCT, FILTER, ORDER BY, window frames (provide `combine`,
and `window` or segment-tree support like DuckDB's sum), and `PIVOT` (the tokenizer's `RewritePivotAggregates`
currently maps pivot sums to `system.main.sum`; route them to the new aggregates).

The aggregate's state type depends on the input type, so register one function per input category with
`AggregateFunctionSet`, or use a bind that swaps in the right `AggregateFunction` (DuckDB's sum does the latter).

**Tests.** All existing aggregate cases, plus in `limitations.sql`: overflow of BIGINT sums (grouped, ungrouped,
windowed with frames), decimal sums near 38 digits, `avg` of decimals with rounding, DISTINCT / FILTER / ORDER BY,
`sum` over empty groups and all-NULL groups, `PIVOT ... sum`, `GROUPING SETS`.

**Acceptance.** No `any_value` in `EXPLAIN` output for `sum` / `avg`; results unchanged; the `sum_*` / `avg_*` /
`group_*` pairs in `bench/func_pairs.toml` at ≤ 1.05x; README bullet removed.

## 8. L3: `window` / `session_window` and the source alias

**Today.** `RewriteGroupingWindow` (`spark_tokenizer.cpp`) turns
`... FROM <source> WHERE <cond> GROUP BY ..., window(t, '5 minutes')` into
`FROM (SELECT * FROM (SELECT *, <window struct> AS "window" FROM <source> <cond>) WHERE "window".start IS NOT
NULL ...) AS __spark_windowed`, so `tab.col` references to the original alias no longer resolve.

**Proposed outcome: removed** for tumbling and sliding windows; **reduced** for `session_window`.

**Approach.**
- *Tumbling windows* (2 or 3 arguments, one window per row): no subquery. Replace the grouping key with the
  window expression itself (`system.spark.__spark_window(t, ...)`, which already resolves anywhere) and every
  `window` column reference in the block (`window.start`, `window`) with the same expression; add the Spark
  filter (`window IS NOT NULL`) to `WHERE`, or to `HAVING` if the block already has aggregates on a NULL window.
  `RewriteWindowColumn` already finds `window` references; make it substitute expressions.
- *Sliding windows* (several windows per row): keep the row multiplication but join it laterally so the source
  keeps its alias: `FROM <source> <alias>, LATERAL (SELECT unnest(system.spark.sliding_windows(<alias>.t, ...))
  AS "window") AS __spark_w` (check that DuckDB v2's LATERAL accepts this; if the source is a join, apply it after
  the join).
- *Session windows* need window functions over the whole input (sessionization), so a subquery stays. Reduce the
  limitation: when the source is a single table or subquery with an alias, give the outer subquery that same
  alias (`AS tab` instead of `AS __spark_windowed`), so `tab.col` resolves; only joins as sources keep the
  limitation.

**Tests.** In `limitations.sql`: `tab.a` references in SELECT, WHERE, HAVING and ORDER BY for each window kind;
sources that are joins; `window` in a subquery and in a CTE; `window_time(window)`; NULL time values; the cases
in `queries.sql` and `fuzz_regressions.sql` keep passing.

**Acceptance.** README bullet reduced to "`session_window` grouping over a join: the joined tables' aliases are
not available". The `group_by_window_rewrite_10M` case in `bench/scaling_bench.py` is not slower.

## 9. L4: `CHAR(n)` for any `n`

**Today.** `RegisterSparkCasts` (`spark_extras.cpp`) registers 255 alias types `spark_char_1` .. `spark_char_255`
with padding casts, and the tokenizer rewrites `CHAR(n)` to `system.spark.spark_char_n`.

**Proposed outcome: removed.**

**Approach.** DuckDB v2 types can take modifiers: `CreateTypeInfo` carries a `TypeConstructorSet`
(`duckdb/function/type_constructor.hpp`; DECIMAL in `src/catalog/default/default_types.cpp` is the model).
1. Register one type `spark_char` in `system.spark` with a constructor taking one integer modifier, which builds a
   VARCHAR alias carrying `n` (e.g. in the alias's `ExtraTypeInfo`, or an alias name `spark_char(n)` parsed back).
   Validate `n` the way Spark does (check Spark's `CharType` for its maximum and its error for `CHAR(0)`).
2. Replace the 255 registered casts with one cast bind function (`CastFunctionSet` has a list of registered bind
   functions, `registered_bind_functions` in `cast_function_set.hpp`) that recognizes the `spark_char` alias on
   either side and pads / validates with the `n` from the type.
3. Rewrite `CHAR(n)` (in `CREATE TABLE`, `CAST`, `ALTER TABLE ADD COLUMNS`) to `system.spark.spark_char(n)`.
4. Check `DESCRIBE` / `typeof` still show Spark's `char(n)` and that tables created before the change still load:
   persisted databases store the alias name, so keep reading `spark_char_<n>` aliases (register them lazily or
   map the old names in the cast bind function).

**Tests.** `CHAR(1)`, `CHAR(255)`, `CHAR(256)`, `CHAR(1000)`, `CHAR(65535)` in CREATE TABLE + INSERT + SELECT,
comparisons between `CHAR(n)` values with trailing spaces (Spark's read-side padding semantics), `CAST(x AS
CHAR(n))`, `length()`, string functions on char columns, a database file created with the old types.

**Acceptance.** README bullet removed.

## 10. L5: unnamed structs in implicit casts

**Today.** Spark matches struct fields by position when it coerces structs implicitly; the dialect does so in
`INSERT ... VALUES` (the tokenizer rewrites `STRUCT(` to `ROW(`) and in `CAST` (`SparkCast` builds the target
struct field by field), but DuckDB's binder matches by name (or fails) elsewhere.

**Proposed outcome: reduced** (covering the coercion sites the dialect controls).

**Approach.** First find, in PySpark, where Spark coerces structs with different field names: CASE / IF /
COALESCE branches, `array(...)` elements, `map(...)` values, set operations (UNION / INTERSECT / EXCEPT),
`INSERT ... SELECT`, `IN (...)` lists, comparisons. For each:
- the dispatchers that compute a common type (`case_when`, `coalesce`, `array`, `map`, comparisons): extend
  `SparkWiderType` with STRUCT (same field count: field names of the first operand, field types wider per
  position) and cast every operand with `SparkCast` (which is positional);
- set operations and `INSERT ... SELECT` are bound by DuckDB: check whether DuckDB v2 offers by-position struct
  coercion there; if not, keep them in the README bullet;
- `IN` lists are bound by DuckDB today (not by the comparison dispatcher); route them through a dispatcher as part
  of this item, or list them as remaining.

**Tests.** In `limitations.sql`, each site with structs whose names differ, nested structs, structs in arrays,
NULL structs, and mismatched field counts (must still fail like Spark).

**Acceptance.** README bullet lists only the remaining sites (expected: set operations and `INSERT ... SELECT`, if
DuckDB cannot be steered there).

## 11. L6: `array_sort` with a comparator

**Today.** `RewriteFunction` (`spark_grammar.cpp`, `array_sort` with 2 arguments) ranks every element by counting
the elements the comparator puts before it: O(n²), and the order for an inconsistent comparator differs from
Spark's (Java's TimSort). 10,000 elements take ~1.3 s; the benchmark ratio is 4.4x.

**Proposed outcome: reduced.**

**Approach.**
1. *Recognize common comparator shapes* at parse time and lower them to DuckDB's `list_sort`: the lambda bodies
   `CASE WHEN l < r THEN -1 WHEN l > r THEN 1 ELSE 0 END` (and with the branches reordered or `>` / `<` swapped,
   giving descending), `l - r` / `r - l` on numbers, `CASE WHEN f(l) < f(r) ...` with the same key expression `f`
   on both sides (sort by `list_transform` to `struct(key, value)`), and the documented NULL-handling forms
   (`WHEN l IS NULL AND r IS NULL THEN 0 WHEN l IS NULL THEN -1 ...` → `NULLS FIRST` / `LAST`). Ties must keep
   input order like TimSort (stable): `list_sort` is stable for equal keys? Verify; if not, add the element index
   as a tie-breaker.
2. Keep the O(n²) path for other comparators, but make a comparator that returns NULL raise Spark's runtime error
   (`The comparator returned NULL`) instead of silently ordering.
3. Inconsistent comparators keep differing (documented).

**Tests.** Each recognized shape ascending / descending, with NULLs and duplicates (stability), on numbers,
strings, structs; a comparator that is not recognized (still correct); a NULL-returning comparator (error, as in
Spark).

**Acceptance.** `array_sort_comparator` in `bench/func_pairs.toml` ≤ 1.2x; `array_sort_comparator_10000` in
`bench/scaling_bench.py` under 50 ms; README bullet reduced to "comparators other than <shapes> are O(n²); an
inconsistent comparator can order differently from Spark's TimSort".

## 12. L7: nesting depth

**Today.** The grammar hooks transform sub-expressions recursively (`Expr(t, ...)` inside `FinalizeExpr`), about
17 hook frames (~17 KB of stack) per level of nesting. `HookDepthGuard` stops at a third of the thread's stack,
about 160 levels on an 8 MB stack; DuckDB's own parser allows 1000.

**Proposed outcome: removed** (DuckDB's 1000-level limit applies), with a guard kept for small stacks.

**Approach**, in order of preference:
1. *Retry on a large stack.* In the outermost hook (depth 0), catch the "nested too deeply" exception and redo the
   transform of that statement on a helper thread with a large stack (e.g. 256 MB, `pthread_attr_setstacksize`;
   on Windows `_beginthreadex` with a stack size), joining before returning. Normal statements never pay for it.
   Check that the transformer state (`PEGTransformer`, arena allocators) is usable from another thread while the
   calling thread waits; the binder and DuckDB's own expression-depth check (1000) then apply.
2. *Fewer frames per level.* Measure how many of the 17 hook frames per level remain after the collapsible rules
   (`SetTransformProcess(..., collapsible = true)` in `ApplyGrammarChanges`); the precedence ladder's
   pass-through levels should not need a hook frame at all. Reducing frames raises the limit proportionally.
3. *Non-recursive hooks* (largest change): rewrite the hooks as resumable `TransformProcess` steps like DuckDB's
   own transformer, so the transform is iterative.

**Tests.** `crash_regressions.sql` (deep nesting cases must still not crash); new cases nesting 500 and 999 levels
of `abs(...)`, parentheses, `CASE`, `NOT`, arrays, lambdas: results like DuckDB's default dialect (works below
1000, DuckDB's depth error at 1000); the ASan build (`mutate.py --shell`) with the structural mutators.

**Acceptance.** Nesting up to DuckDB's limit works in both dialects; README bullet removed (or reduced to platforms
without the large-stack retry). Parse benchmark unchanged.

## 13. L8: error messages

**Today.** Errors for invalid Spark SQL can mention rewritten text: internal names (`system.spark.cast_value`,
`spark_kernel`, `__spark_window`, `system.spark.add`), or a caret position inside tokens the tokenizer inserted.

**Proposed outcome: reduced.**

**Approach.**
1. Measure first: collect error messages from the existing corpora (`test/cases/non_spark.sql`, the Spark
   `sql-tests` corpus via `spark_corpus.py`, a mutation fuzz run) and count those containing `system.spark`,
   `spark_kernel`, `__spark_`, `cast_value`, `_impl`, `concat_ws_array`. Add the count to the fuzz summary
   (`triage.py`) so it is tracked.
2. Fix the sources: dispatchers report their errors with Spark names (`CheckArgs` gets the user-facing name; the
   `[DATATYPE_MISMATCH...]` style the strict checks already use); for errors DuckDB raises about our functions
   (`No function matches system.spark.x(...)`), give the internal functions descriptive public names or catch and
   re-raise in the dispatcher with the Spark function name.
3. Caret positions: generated tokens copy the offset of the token they replace (`Lex(..., offset)`), so carets point
   at the original statement; verify on a sample and fix rewrites that use offset 0.

**Acceptance.** No internal names in error messages over the corpora above, or a short list of remaining cases in
the README bullet.

## 14. L9: the fuzzers' known differences and the untriaged tail

`test/fuzz/known_differences.toml` has 10 entries. Proposed outcome per entry:

| entry | proposal | approach |
|---|---|---|
| BIGINT outside INT range passed to INT parameters wraps in Spark | **fix** | Spark's `ImplicitCastInputTypes` narrowing is a non-ANSI `Cast` (wraps). Derive the list of (function, INT parameter) from the sweep's findings and Spark's expression definitions; wrap those arguments in `system.spark.as_int` (Java narrowing) in the macros / rewrites. With L1, ANSI makes it an error |
| `-0.0` and `0.0` distinct in array set functions / map keys | **fix** | compare doubles by Spark's rule (NaN equal to NaN, -0.0 distinct from 0.0) in `array_distinct` / `array_union` / `array_except` / `array_intersect` / `map_from_arrays`: a kernel, or a key transform that distinguishes the sign of zero |
| VALUES rows unify literal types with DuckDB's rules | **fix (literals)** | in the grammar, give integer literals inside VALUES rows an explicit INT type (no literal adaptation), and compute Spark's wider decimal type per column when the column is all literals; non-literal columns keep DuckDB's rule |
| approx_percentile / percentile_approx / approx_count_distinct algorithms | keep | exact ports of Spark's GK sketch and HLL++ are large; the documented examples already match |
| histogram_numeric bins | keep (or port) | port Spark's `NumericHistogram` if time allows; small and self-contained |
| format_string / printf with NULL arguments ("null") | **fix** | own `java.util.Formatter` subset kernel (`%s %d %f %e %x %c %b %%` with flags, width, precision; NULL → "null") instead of DuckDB's printf |
| array_sort inconsistent comparator | keep | see L6 |
| strict mode accepts untyped NULL arguments | **reduce** | run each Spark function with NULL in each position in PySpark, record which reject `NULL_TYPE`, and in strict mode wrap those arguments in a bind-time check (like `CheckFoldableArguments`, but at bind time where the type is known) |
| date arithmetic beyond DuckDB's date range | investigate | DuckDB's and Spark's date ranges are the same (int days); check what Spark actually does at the boundary before deciding |
| NULL map key in a map the optimizer removes | keep | a Spark optimizer artifact |

**The untriaged tail.** The run over Spark's own `sql-tests` inputs (`python test/fuzz/spark_corpus.py fetch &&
python test/fuzz/spark_corpus.py run`) found, out of 3,734 statements: 520 DuckDB errors where Spark succeeds, 157
wrong results, 147 strict-mode leniencies, 17 strict rejects. Triage these by signature (`triage.py`), largest first
(`extract.sql`, `join-lateral.sql`, `interval.sql`, `cast.sql`, `date.sql` had the most), fix or add entries to
`known_differences.toml`, and add the fixed cases to `fuzz_regressions.sql`. Treat this as ongoing work: set a time
box, and record the remaining counts in the README bullet.

**Acceptance.** The entries proposed as "fix" are gone from `known_differences.toml`; each remaining entry has a
current reason and passes `replay.py --known`; the corpus counts are recorded and lower than above.

## 15. Order of work

Small, independent items first; each is its own pull request:

1. Groundwork (5.1 to 5.3).
2. L2 (aggregates; also a performance improvement, self-contained).
3. L9 fixes that are small: format_string / printf, -0.0 in set functions, BIGINT → INT narrowing.
4. L3 (window aliases).
5. L7 (nesting depth, option 1).
6. L6 (array_sort shapes).
7. L4 (CHAR(n) with type modifiers).
8. L5 (struct coercion).
9. L8 (error messages).
10. L1 (ANSI mode; largest).
11. L9 VALUES typing, strict untyped NULLs, the corpus tail (time-boxed).

After each, update the README's Limitations section in the same pull request.

## 16. Pitfalls (learned in earlier work)

- **`SET` in the Spark dialects is a Spark `SET`.** It does not change a DuckDB setting (in `spark_strict` never;
  in `spark` only keys without a dot). To change DuckDB settings in tests, leave the dialect first
  (`RESET current_dialect`), set, and select the dialect again.
- **Macros repeat their arguments.** A macro that uses a parameter twice makes nested calls grow exponentially
  (`concat` nested 100 deep used 8 GB before it was fixed). Prefer dispatchers or kernels for anything that may be
  nested.
- **Ad-hoc kernels must go through `BindSparkKernel` (`Builder::Kernel`).** Optimizers that copy plans serialize
  them, and a kernel that is not in the registry fails with "Scalar Function ... does not exist".
- **Any kernel that can throw must be fallible** (`RegisterFallible`, `SetFallible`); otherwise DuckDB raises an
  internal error. ANSI mode (L1) makes many kernels throwing.
- **DuckDB's `TRY_CAST` is slow for 38-digit decimals when the scale goes down** (~300 ms per million rows); use
  the dialect's `SparkDecimalResult`.
- **`StringUtil::Format("%016llx")` sign-extends**; format hex by hand.
- **`fnmatch` treats `[...]` as a character class** in `known_differences.toml` signatures: escape `[` as `[[]`.
- **Spark's runtime vs analysis errors** matter for strict mode: `compare.py` classifies by Spark's error class;
  add new classes to `RUNTIME_MARKERS` if ANSI tests show new runtime error classes.
- **Ties under `LIMIT`** make results nondeterministic; add a total order in test queries.
