"""A long-lived worker for bench/ab.py: one DuckDB process with one extension build loaded.

Protocol: JSON lines on stdin, one JSON line per request on stdout.
  {"sql": "...", "dialect": "spark"|"spark_strict"|null, "threads": N}  ->  {"seconds": s, "digest": "...", "error": null}
"""
import argparse
import hashlib
import json
import sys
import time

import duckdb


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ext", required=True)
    ap.add_argument("--db", default=":memory:")
    args = ap.parse_args()
    con = duckdb.connect(args.db, read_only=args.db != ":memory:",
                         config={"allow_unsigned_extensions": "true", "allow_extensions_metadata_mismatch": "true"})
    con.execute(f"LOAD '{args.ext}'")
    con.execute("SET TimeZone = 'UTC'")
    dialect = "?"
    threads = None
    print(json.dumps({"ready": True}), flush=True)
    for line in sys.stdin:
        req = json.loads(line)
        out = {"seconds": None, "digest": None, "error": None}
        try:
            if req.get("threads") != threads:
                threads = req.get("threads")
                if threads:
                    con.execute("RESET current_dialect")
                    con.execute(f"SET threads = {threads}")
                    dialect = "?"
            want = req.get("dialect")
            if want != dialect:
                if want in (None, "duckdb"):
                    con.execute("RESET current_dialect")
                else:
                    con.execute(f"SET current_dialect = '{want}'")
                if req.get("ansi") is not None:
                    pass
                dialect = want
            for statement in req.get("prepare", []):
                con.execute(statement)
            start = time.perf_counter()
            rows = con.execute(req["sql"]).fetchall()
            out["seconds"] = time.perf_counter() - start
            if req.get("digest", True):
                out["digest"] = hashlib.md5(repr(sorted(map(repr, rows))).encode()).hexdigest()[:12]
        except Exception as e:  # noqa: BLE001
            out["error"] = str(e).split("\n")[0][:200]
        print(json.dumps(out), flush=True)


if __name__ == "__main__":
    main()
