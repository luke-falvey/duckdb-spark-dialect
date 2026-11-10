"""Run every case in Apache Spark 3.5 and store the results as the expected output.

usage: python test/make_golden.py [--config key=value] [case files...]   (default: test/cases/**/*.sql)
writes test/golden/<case file>.pkl  (types/, settings/ and spark_corpus/ files: .pkl.gz)
       test/golden/<config slug>/<case file>.pkl  for matrix combinations and --config runs

A file with `-- @matrix` lines is recorded once per combination; `--config` records every file under extra
Spark settings (workstream D). Settings are applied with `spark.conf.set`, which raises for static settings: the
script fails loudly instead of skipping.
"""
import argparse
import os
import sys

from engines import SparkOracle, scratch_dir, use_utc  # noqa: E402
from harness import (ROOT, case_file_name, config_slug, file_directives, load_cases, matrix_combinations,  # noqa: E402
                     save_golden)


def set_conf(spark, key, value):
    try:
        spark.spark.conf.set(key, value)
    except Exception as e:  # noqa: BLE001
        raise SystemExit(f"cannot set {key}={value} in Spark (static setting?): {str(e).splitlines()[0]}")


def run_cases(spark, file, base_conf):
    """Run the cases of a file in Spark; `-- @set` settings apply to one case and are restored after it."""
    golden = {}
    for case in load_cases([file]):
        previous = {}
        for key, value in case.sets:
            previous[key] = base_conf.get(key)
            set_conf(spark, key, value)
        r = spark.run(case.sql)
        for key, _ in case.sets:
            if previous[key] is None:
                spark.spark.conf.unset(key)
            else:
                spark.spark.conf.set(key, previous[key])
        entry = {"sql": case.sql, "ok": r.ok, "sets": case.sets}
        if r.ok:
            entry["schema"] = r.schema
            entry["rows"] = r.rows
            entry["types"] = r.extra.get("types")
            entry["names"] = r.extra.get("names")
        else:
            entry["error"] = r.error
            entry["phase"] = r.phase
            entry["error_class"] = r.extra.get("error_class")
        golden[case.index] = entry
    return golden


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("files", nargs="*")
    args = parser.parse_args()
    use_utc()
    files = [os.path.abspath(f) for f in args.files] or sorted(str(p) for p in (ROOT / "cases").rglob("*.sql"))
    cli_extra = tuple((k.strip(), v.strip()) for k, _, v in (c.partition("=") for c in args.config))
    # statements that write files (COPY, EXPORT DATABASE, ...) must not write into the repository
    os.chdir(scratch_dir("spark-golden-"))
    spark = SparkOracle()
    for file in files:
        directives = file_directives(file)
        for combo in matrix_combinations(directives["matrix"]):
            extra = combo + cli_extra
            conf = [*directives["config"], *extra]
            for key, value in conf:
                set_conf(spark, key, value)
            golden = run_cases(spark, file, dict(conf))
            save_golden(file, extra, golden)
            failed = sum(1 for e in golden.values() if not e["ok"])
            label = case_file_name(file) + (f"[{config_slug(extra)}]" if extra else "")
            print(f"{label}: {len(golden)} cases, {failed} error in Spark", flush=True)
            # each file starts from a clean catalog and the default confs
            spark.reset()
            for key, _ in conf:
                spark.spark.conf.unset(key)


if __name__ == "__main__":
    main()
