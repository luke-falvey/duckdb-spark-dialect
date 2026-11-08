"""Run every case in Apache Spark 3.5 and store the results as the expected output.

usage: python test/make_golden.py [case files...]   (default: test/cases/*.sql)
writes test/golden/<case file>.pkl
"""
import os
import pickle
import sys
import time

os.environ["TZ"] = "UTC"
time.tzset()

from pyspark.sql import SparkSession  # noqa: E402

from harness import ROOT, load_cases, normalize  # noqa: E402


def spark_session():
    spark = (SparkSession.builder.master("local[2]")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.warehouse.dir", str(ROOT / "golden" / "_warehouse"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    assert spark.version.startswith("3.5"), spark.version
    return spark


def main():
    files = sys.argv[1:] or sorted(str(p) for p in (ROOT / "cases").glob("*.sql"))
    spark = spark_session()
    for file in files:
        cases = load_cases([file])
        golden = {}
        for case in cases:
            entry = {"sql": case.sql}
            try:
                df = spark.sql(case.sql)
                rows = df.collect()
                entry["ok"] = True
                entry["schema"] = df.schema.simpleString()
                entry["rows"] = [[normalize(v) for v in row] for row in rows]
            except Exception as e:  # noqa: BLE001
                entry["ok"] = False
                entry["error"] = str(e).split("\n")[0][:300]
            golden[case.index] = entry
        out = ROOT / "golden" / (os.path.basename(file) + ".pkl")
        with open(out, "wb") as f:
            pickle.dump(golden, f)
        failed = sum(1 for e in golden.values() if not e["ok"])
        print(f"{file}: {len(golden)} cases, {failed} error in Spark")
        # each file starts from a clean catalog
        for t in spark.catalog.listTables():
            if t.isTemporary:
                spark.catalog.dropTempView(t.name)
            elif t.tableType == "VIEW":
                spark.sql(f"DROP VIEW IF EXISTS {t.name}")
            else:
                spark.sql(f"DROP TABLE IF EXISTS {t.name}")


if __name__ == "__main__":
    main()
