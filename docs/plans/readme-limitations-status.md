# Status of docs/plans/readme-limitations.md

What happened to each limitation (see the README's Limitations section for the user-facing text).

| # | limitation | outcome | where |
|---|---|---|---|
| L1 | ANSI mode not emulated | **reduced**: runtime errors for overflow, division by zero, casts, array indexes, `size(NULL)`, sums; implicit-cast rules not emulated | `spark_ansi_enabled` setting, `test/cases/ansi/` |
| L2 | extra `any_value(x)` in `sum`/`avg` | **removed**: `spark_sum` / `spark_avg` aggregates | `src/spark_aggregates.cpp` |
| L3 | `window`/`session_window` source alias | **removed** for tumbling and sliding windows (lateral join), **reduced** for sessions (single source keeps its alias) | `RewriteGroupingWindow` |
| L4 | `CHAR(n)` only up to 255 | **removed**: `spark_char(n)` type | `src/spark_extras.cpp` |
| L5 | unnamed `STRUCT` matched by position only in some sites | **reduced**: `named_struct` in INSERT; `UNION` and `IN` stay by name (would change the default dialect) | tokenizer, README |
| L6 | `array_sort` comparator O(n²) | **reduced**: common shapes use `list_sort`; NULL-returning comparators raise | `MatchSortComparator` |
| L7 | nesting limit ~160 levels | **removed** on Linux/macOS (large-stack thread) | `FinalizeExpr`, `RunOnLargeStack` |
| L8 | rewritten text in error messages | **reduced**: measured (2 of 1,019 corpus errors name an internal function) and tracked | `spark_corpus.py names`, `triage.py` |
| L9 | known differences and the corpus tail | `format_string`/`printf`, `-0.0` in array set functions, BIGINT to INT arguments, VALUES typing, strict untyped NULL literals: **fixed**; date range entry: kept with a reproducing example; approx_*, histogram_numeric, NULL map key, inconsistent `array_sort`: **kept**. Corpus: `count(a, b)`, `percentile_cont/disc` in strict mode, `avg`/`sum` of timestamps fixed; 512 / 156 / 147 / 3 remain | `test/fuzz/known_differences.toml` |

Groundwork: `-- @config` file directive (`make_golden.py`, `run_duckdb.py`), `replay.py --known`
(run in CI), `spark_corpus.py run --ansi` and `names`, type-unification rules in `src/spark_types.cpp`.

Not done from the plan: `map_from_arrays` / map keys with `-0.0`; `histogram_numeric` port; ANSI implicit-cast rules;
UNION/IN positional struct coercion; the `Int` narrowing list is a table of the functions found by the sweep and
Spark's definitions (see `INT_PARAMETERS` in `src/spark_grammar.cpp`), not derived from a complete sweep.
