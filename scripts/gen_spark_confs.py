"""Dump every SQL configuration entry registered in Apache Spark 3.5 (public and internal) to
test/spark35_sqlconf.json: key, default, doc, public, version.

usage: python scripts/gen_spark_confs.py        (needs pyspark==3.5.* and Java 17+)

`SET -v` lists only the public entries; the internal ones (spark.sql.legacy.*, ...) are read from
SQLConf's registry by reflection. The file is the input of the settings inventory
(docs/plans/spark-parity-testing.md, workstream B).
"""
import json
import os
import time
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "test" / "spark35_sqlconf.json"


def main():
    os.environ["TZ"] = "UTC"
    time.tzset()
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    assert spark.version.startswith("3.5"), spark.version
    spark.sql("SET -v").collect()  # makes sure every entry is registered
    jvm = spark._jvm
    module = jvm.java.lang.Class.forName("org.apache.spark.sql.internal.SQLConf$").getField("MODULE$").get(None)
    field = next(f for f in module.getClass().getDeclaredFields() if "sqlConfEntries" in f.getName())
    field.setAccessible(True)
    entries = []
    for e in list(field.get(module).values()):
        entries.append({"key": e.key(), "default": e.defaultValueString(), "doc": e.doc(),
                        "public": bool(e.isPublic()), "version": e.version()})
    entries.sort(key=lambda d: d["key"])
    OUT.write_text(json.dumps(entries, indent=1) + "\n")
    print(f"{OUT}: {len(entries)} entries, {sum(d['public'] for d in entries)} public (Spark {spark.version})")
    spark.stop()


if __name__ == "__main__":
    main()
