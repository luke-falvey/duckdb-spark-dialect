"""Re-run the setting witnesses in Apache Spark 3.5 and refresh their recorded results.

usage: python scripts/probe_conf_witnesses.py [file]   (default: docs/plans/spark-parity/conf_witnesses.json)

A witness is a setting (`key` = `value`, plus the settings in `requires`) and a list of statements whose
last result differs between Spark's default and the changed setting. For each entry the script runs the
statements twice in a clean catalog, with and without the setting, and stores both results
(`spark_default`, `spark_flipped`) and `differs`. Add an entry with only key / value / requires /
statements and run the script to fill in the rest. Error messages are compared without expression ids
(`a#12`), which change from run to run.
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "test"))
from engines import SparkOracle  # noqa: E402

DEFAULT = ROOT / "docs" / "plans" / "spark-parity" / "conf_witnesses.json"


def run(oracle, statements):
    result = None
    for sql in statements:
        result = oracle.run(sql)
        if not result.ok:
            break
    if result.ok:
        return {"ok": True, "schema": result.schema, "rows": repr(result.rows)[:400]}
    return {"ok": False, "error": re.sub(r"#\d+", "#N", result.error)[:200], "phase": result.phase}


def main():
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT
    entries = json.loads(path.read_text())
    oracle = SparkOracle()
    conf = oracle.spark.conf
    for entry in entries:
        requires = entry.get("requires") or {}
        oracle.reset()
        for key, value in requires.items():
            conf.set(key, value)
        entry["spark_default"] = run(oracle, entry["statements"])
        oracle.reset()
        entry["set_error"] = None
        try:
            conf.set(entry["key"], entry["value"])
        except Exception as e:  # noqa: BLE001  (static and core settings cannot be changed in a session)
            entry["set_error"] = str(e).split("\n")[0][:200]
        entry["spark_flipped"] = None if entry["set_error"] else run(oracle, entry["statements"])
        for key in [entry["key"], *requires]:
            try:
                conf.unset(key)
            except Exception:  # noqa: BLE001
                pass
        entry["differs"] = entry["spark_default"] != entry["spark_flipped"]
        print(("DIFF " if entry["differs"] else "SAME ") + f"{entry['key']}={entry['value']}", flush=True)
    path.write_text(json.dumps(entries, indent=1) + "\n")
    differing = {e["key"] for e in entries if e["differs"]}
    print(f"{path}: {len(entries)} witnesses, {len(differing)} settings with a differing witness")


if __name__ == "__main__":
    main()
