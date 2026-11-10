# Fuzzing the Spark dialects

The case files in `test/cases/` check behaviour someone thought of. The tools here look for what nobody wrote
down: crashes, hangs and internal errors on odd input, wrong results on generated queries, SQL that
`spark_strict` should reject, and side effects on DuckDB's own dialect. Spark 3.5 is the reference throughout.

| tool | finds | needs Spark |
|---|---|---|
| `mutate.py` | crashes, hangs, `INTERNAL` errors, sanitizer reports | no |
| `args_sweep.py` | wrong results and errors for edge-case arguments of every Spark function | yes |
| `differential.py` | wrong results for generated queries (`gen.py`); strict-mode accept/reject differences | yes |
| `isolation.py` | changes to DuckDB's default dialect caused by loading the extension | no |
| `spark_corpus.py` | differences on Spark's own SQL test inputs (a finding source, not a gate) | yes |
| `replay.py` | re-runs `crash_regressions.sql` (and the crash findings of a run) | no |

Setup is as for the tests (see the top-level README): `duckdb==2.0.0`, `pyspark==3.5.*`, Java 17+, and the
extension at `build/spark_dialect.duckdb_extension` or `$SPARK_EXTENSION`.

## Running

```bash
python test/fuzz/replay.py                                     # crash regressions, every dialect
python test/fuzz/mutate.py --seed 1 --budget 20000 --jobs 3    # mutation fuzzing
python test/fuzz/args_sweep.py --seed 1                        # ~14,000 queries, ~10 minutes
python test/fuzz/differential.py --seed 1 --budget 2000        # ~10 queries/s (Spark is the bottleneck)
python test/fuzz/differential.py --seed 1 --budget 2000 --conf-fuzz   # each batch of 100 queries under a random assignment
                                                               # of the emulated semantic settings (recorded in the finding)
python test/fuzz/differential.py --conf "spark.sql.legacy.sizeOfNull=false,spark.sql.mapKeyDedupPolicy=LAST_WIN" --sql-file q.sql
                                                               # replay under fixed settings
python test/fuzz/args_sweep.py --types --emit                  # write test/cases/types/c6_function_args.sql (plan C6):
python test/make_golden.py test/cases/types/c6_function_args.sql   # every slot of every documented example x every type,
python test/fuzz/args_sweep.py --types --record                # summarized in test/golden/signatures.json.gz
python scripts/check_int_parameters.py                         # compare the INT_PARAMETERS table with Spark's answers
python test/fuzz/isolation.py --duckdb-src /path/to/duckdb --budget 300
python test/fuzz/spark_corpus.py fetch && python test/fuzz/spark_corpus.py run
```

With an AddressSanitizer / UBSan build (`-DENABLE_SANITIZER=TRUE -DENABLE_UBSAN=TRUE -DFORCE_ASSERT=1`, shell and
loadable extension built from the same tree), mutation fuzzing also runs batches through the instrumented shell:

```bash
python test/fuzz/mutate.py --budget 0 --shell build-asan/duckdb \
  --shell-extension build-asan/extension/spark_dialect/spark_dialect.duckdb_extension --shell-budget 20000
```

Every run writes `test/fuzz/out/<run>/findings*.jsonl` and `summary.md` (gitignored). A finding records the SQL,
the dialect, the seed, both engines' results, and a `signature` that groups findings with the same cause
(`triage.py <run dir>` re-summarizes a run). The differential runner shrinks a few findings per signature with
`reduce.py` (`reduced_sql`); `reduce.py "<sql>"` shrinks one query by hand.

Everything is reproducible from the seed: the same seed and budget generate the same inputs.

## Outcomes

`compare.py` classifies a query's results in Spark, `spark` and `spark_strict`:

- `mismatch`: both succeed with different rows; `duck_error`: DuckDB errors where Spark succeeds
- `strict_reject`: Spark and `spark` succeed, `spark_strict` errors
- `strict_lenient`: Spark rejects the query at parse / analysis time, `spark_strict` accepts it
- `lenient` / `lenient_runtime`: `spark` accepts what Spark rejects (allowed for the lenient dialect)
- `crash`, `timeout`, `internal`: never acceptable

## When a fuzzer finds something

1. Reduce it (`reduce.py`, or by hand) and fix the extension. Don't change the generator to avoid it.
2. Add the reduced query to `test/cases/fuzz_regressions.sql` and record its Spark result:
   `python test/make_golden.py test/cases/fuzz_regressions.sql`. Crashes, hangs and internal errors go in
   `crash_regressions.sql` instead (no Spark result needed; `-- @upstream <reason>` for DuckDB's own bugs).
3. Run the whole suite in both dialects, then commit the fix with its regression case.

## Known differences

`known_differences.toml` lists the differences that are deliberately not fixed, each with a reason and an example.
An entry matches findings by a `signature` glob and an optional `sql` regular expression; matched findings are
reported as `known` and do not fail a run. Add an entry only when a fix would need a disproportionate amount of
new code, and keep the list short.
