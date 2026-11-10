"""Run the setting witnesses (docs/plans/spark-parity/conf_witnesses.json) in the Spark dialect and compare with
the results Spark recorded for them.

usage: python scripts/run_witnesses_duckdb.py [--dialect spark|spark_strict] [--json out.json] [file]

For every witness two runs are made in a clean DuckDB session: with the default value, and after
`SET key = value` (after the `requires` settings). Each is compared with `spark_default` / `spark_flipped`:
  ok      both succeed with the same result (or both fail),
  rows    both succeed, the rows or the result type differ,
  status  one engine succeeds and the other fails,
  reject  the SET itself is rejected (the dialect declines to emulate the setting: allowed by plan section 9.1).
The summary per setting says whether the dialect honours the value (`emulated`: flipped run agrees with Spark and
the witness differs), rejects it, or silently ignores it (`ignored`: the flipped result equals the default result
although Spark's differs).
"""
import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "test"))
from engines import DuckRunner, extension_path  # noqa: E402

DEFAULT = ROOT / "docs" / "plans" / "spark-parity" / "conf_witnesses.json"


def run(duck, statements):
    result = None
    for sql in statements:
        result = duck.run(sql)
        if not result.ok:
            break
    if result.ok:
        return {"ok": True, "rows": repr(result.rows)[:400], "types": result.extra.get("types")}
    return {"ok": False, "error": re.sub(r"#\d+", "#N", result.error or "")[:200]}


def same(spark, duck):
    if spark is None:
        return None
    if spark["ok"] != duck["ok"]:
        return "status"
    if not spark["ok"]:
        return "ok"
    return "ok" if spark["rows"] == duck["rows"] else "rows"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dialect", default="spark")
    ap.add_argument("--json")
    ap.add_argument("file", nargs="?", default=str(DEFAULT))
    args = ap.parse_args()
    entries = json.loads(Path(args.file).read_text())
    out = []
    counts = Counter()
    for entry in entries:
        key, value, requires = entry["key"], entry["value"], entry.get("requires") or {}
        row = {"key": key, "value": value, "differs": entry["differs"]}
        for phase in ("default", "flipped"):
            duck = DuckRunner(dialect=args.dialect, timeout=30, extension=extension_path())
            try:
                for rk, rv in requires.items():
                    duck.run(f"SET {rk} = {rv}")
                rejected = None
                if phase == "flipped":
                    if entry.get("set_error"):
                        row["flipped"] = "static"
                        continue
                    r = duck.run(f"SET {key} = {value}")
                    if not r.ok:
                        rejected = (r.error or "")[:160]
                result = run(duck, entry["statements"]) if rejected is None else {"ok": False, "error": rejected}
                row[phase + "_result"] = result
                if phase == "default":
                    row["default"] = same(entry["spark_default"], result)
                elif rejected is not None:
                    row["flipped"] = "reject"
                else:
                    row["flipped"] = same(entry["spark_flipped"], result)
                    if entry["differs"] and row["default_result"] == result:
                        row["flipped"] = "ignored"
            finally:
                duck.close()
        counts[(row["default"], row.get("flipped"))] += 1
        out.append(row)
        print(f"{key}={value}: default {row['default']}, flipped {row.get('flipped')}", flush=True)
    print()
    print("(default, flipped): count")
    for k, v in sorted(counts.items(), key=lambda kv: str(kv[0])):
        print(f"  {k}: {v}")
    ignored = sorted({r['key'] for r in out if r.get('flipped') == 'ignored'})
    print(f"settings silently ignored (witness differs in Spark, not in the dialect): {len(ignored)}")
    if args.json:
        Path(args.json).write_text(json.dumps(out, indent=1) + "\n")


if __name__ == "__main__":
    main()
