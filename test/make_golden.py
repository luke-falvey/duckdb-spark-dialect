"""Run every case in Apache Spark 3.5 and store the results as the expected output.

usage: python test/make_golden.py [case files...]   (default: test/cases/*.sql)
writes test/golden/<case file>.pkl
"""
import os
import pickle
import sys

from engines import SparkOracle, scratch_dir, use_utc  # noqa: E402
from harness import ROOT, file_config, load_cases  # noqa: E402


def main():
    use_utc()
    files = [os.path.abspath(f) for f in sys.argv[1:]] or sorted(str(p) for p in (ROOT / "cases").glob("*.sql"))
    # statements that write files (COPY, EXPORT DATABASE, ...) must not write into the repository
    os.chdir(scratch_dir("spark-golden-"))
    spark = SparkOracle()
    for file in files:
        golden = {}
        conf = file_config(file)
        for key, value in conf:
            spark.spark.conf.set(key, value)
        for case in load_cases([file]):
            r = spark.run(case.sql)
            entry = {"sql": case.sql, "ok": r.ok}
            if r.ok:
                entry["schema"] = r.schema
                entry["rows"] = r.rows
            else:
                entry["error"] = r.error
                entry["phase"] = r.phase
            golden[case.index] = entry
        out = ROOT / "golden" / (os.path.basename(file) + ".pkl")
        with open(out, "wb") as f:
            pickle.dump(golden, f)
        failed = sum(1 for e in golden.values() if not e["ok"])
        print(f"{file}: {len(golden)} cases, {failed} error in Spark")
        # each file starts from a clean catalog and the default confs
        spark.reset()
        for key, _ in conf:
            spark.spark.conf.unset(key)


if __name__ == "__main__":
    main()
