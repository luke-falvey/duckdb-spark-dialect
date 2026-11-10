# Baseline (phase 0)

Measured on the commit before any parity work (extension built from `main` after PR #4, `RelWithDebInfo`,
DuckDB v2.0.0; Spark 3.5.9).

## Existing suite

| dialect | pass | lenient | fail | skip |
|---|---:|---:|---:|---:|
| `spark` | 1731 | 64 | 0 | 0 |
| `spark_strict` | 1795 | 0 | 0 | 0 |

The harness compares rows only (types and error phase are not compared; see plan section 1).

## Gaps predicted by reading the code: all five confirmed

1. `SET spark.sql.mapKeyDedupPolicy=LAST_WIN` is accepted and has no effect.
2. `SET k=v` then `SET k` returns `<undefined>`; `RESET k` does nothing.
3. Keys are case-insensitive: `SET SPARK.SQL.ANSI.ENABLED=true` switches ANSI on (Spark: unknown key, stored only).
4. `typeof(TIMESTAMP_NTZ '...')` and `typeof(TIMESTAMP '...')` are both `timestamp`; `typeof(INTERVAL 1 DAY)` and
   `typeof(INTERVAL 1 MONTH)` are both `interval`.
5. Under ANSI `typeof('1' + 1)` is `double` (Spark: `bigint`) and `typeof(coalesce('1', 2))` is `int` (Spark: `bigint`).

## Setting witnesses (`scripts/run_witnesses_duckdb.py`, 92 witnesses, 87 settings)

Each witness is run in the dialect with the default value and after `SET key = value`, and compared with the result
Spark recorded.

| default run | flipped run | witnesses |
|---|---|---:|
| agrees | silently ignored (same result as the default, Spark differs) | 41 |
| agrees | agrees (setting is not observable in the dialect, or already matches) | 18 |
| agrees | rejected (`session.timeZone=+05:30`: offset zones not supported) | 1 |
| agrees | differs (`session.timeZone=America/Los_Angeles`) | 1 |
| agrees | static key (Spark refuses too) | 1 |
| differs | ignored | 11 + 17 |
| differs | differs | 1 + 1 |

66 settings are silently ignored: a user who sets them gets default behaviour with no message. That is the
behaviour section 9.1 replaces with "emulate, or reject".

"default run differs" (30 witnesses) means the witness statement already differs from Spark under the default
configuration. Those are bugs or missing features independent of any setting (for example `legacy.*` witnesses that
use syntax the dialect lacks, `variable.substitute`, `defaultColumn.*`); they are tracked as cases in the settings
files with `@xfail` ids.
