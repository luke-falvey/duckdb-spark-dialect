"""Plan B5: every semantic DuckDB setting at a non-default value must leave the Spark dialects' results unchanged.

usage: python scripts/check_duckdb_leaks.py [--dialect spark|spark_strict]
Runs operators.sql, queries.sql and literals_types.sql with `--duck-set key=value` for every `semantic` entry of
test/settings/duckdb_inventory.toml and fails when a case fails.
"""
import argparse
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dialect", default="spark")
    args = ap.parse_args()
    inv = tomllib.loads((ROOT / "test" / "settings" / "duckdb_inventory.toml").read_text())["settings"]
    files = [str(ROOT / "test" / "cases" / f) for f in ("operators.sql", "queries.sql", "literals_types.sql")]
    bad = 0
    for key, e in inv.items():
        if e["class"] != "semantic":
            continue
        out = subprocess.run([sys.executable, str(ROOT / "test" / "run_duckdb.py"), "--dialect", args.dialect,
                              "--duck-set", f"{key}={e['value']}", *files], capture_output=True, text=True).stdout
        total = [l for l in out.splitlines() if l.startswith("TOTAL")]
        failed = (not total) or "FAIL=0" not in total[0] or "XPASS=0" not in total[0]
        print(("LEAK " if failed else "ok   ") + f"{key}={e['value']}  " + (total[0] if total else out[-200:]))
        bad += failed
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
