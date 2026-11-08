"""Summarize bench/out/*.json as Markdown (printed; --write puts it in docs/PERFORMANCE_RESULTS.md).

usage: python bench/report.py [--write] [--check bench/baseline.json]
  --check   fail (exit 1) on the catastrophic thresholds: TPC-H geometric mean > 1.5x or any query > 4x,
            a function ratio > 3x its baseline, or parse time for 10,000 items > 20x that of 1,000.
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402

RESULTS_MD = common.PROJECT / "docs" / "PERFORMANCE_RESULTS.md"


def load(name):
    p = common.OUT_DIR / f"{name}.json"
    return json.loads(p.read_text()) if p.exists() else None


def first(*names):
    for n in names:
        r = load(n)
        if r:
            return n, r
    return None, None


def ms(t):
    return f"{t * 1000:.1f}"


def func_section(data):
    rows = data["results"]
    out = ["## Functions and operators (spark dialect vs native DuckDB, 1 thread)", "",
           f"{rows[0]['rows'] if rows else 0:,} rows per table; ratio = spark time / native time "
           "(median of 5 runs).", "",
           "| pair | kind | spark ms | native ms | ratio |", "|---|---|---:|---:|---:|"]
    for r in sorted(rows, key=lambda r: -(r.get("ratio") or 0)):
        sp = ms(r["spark"]["median"]) if "spark" in r else r.get("spark_error", "error")[:40]
        nat = ms(r["native"]["median"]) if "native" in r else "-"
        ratio = f"{r['ratio']:.2f}x" if "ratio" in r else "-"
        out.append(f"| {r['name']} | {r['kind']} | {sp} | {nat} | {ratio} |")
    ratios = [r["ratio"] for r in rows if "ratio" in r]
    g = common.geomean(ratios)
    out += ["", f"Geometric mean ratio over {len(ratios)} pairs: **{g:.2f}x**" if g else "", ""]
    return out


def tpch_section(name, data):
    rows = data["results"]
    dialects = [k[:-6] for k in rows[0] if k.endswith("_ratio")] if rows else []
    head = "| query | native ms | " + " | ".join(f"{d} ms | {d} ratio" for d in dialects) + " |"
    out = [f"## TPC-H ({name.replace('tpch_', '')}, all threads)", "", head,
           "|---|---:|" + "---:|---:|" * len(dialects)]
    for r in rows:
        cells = [f"Q{r['query']:02d}", ms(r["native"]["median"])]
        for d in dialects:
            if d in r:
                flag = "" if r.get(f"{d}_same_result") else " (result differs)"
                cells += [ms(r[d]["median"]), f"{r[d + '_ratio']:.2f}x{flag}"]
            else:
                cells += [r.get(f"{d}_error", "error")[:40], "-"]
        out.append("| " + " | ".join(cells) + " |")
    out.append("")
    for d in dialects:
        g = common.geomean([r.get(f"{d}_ratio") for r in rows])
        if g:
            out.append(f"Geometric mean ratio, {d}: **{g:.3f}x**")
    out.append("")
    return out


def parse_section(data):
    res = data["results"]
    out = ["## Parsing", "", "| dialect | µs per statement | ratio |", "|---|---:|---:|"]
    for d in ("duckdb", "spark", "spark_strict"):
        r = res["corpus"].get(d)
        if r:
            out.append(f"| {d} | {r['per_statement_us']:.1f} | {r['ratio']:.2f}x |")
    out += ["", f"Over the {res['corpus']['duckdb']['statements']} case statements that parse in all three dialects.", "",
            "Parse time (ms) by statement size:", "", "| shape | dialect | " +
            " | ".join(str(n) for n in next(iter(res["scaling"].values()))["duckdb"]) + " |"]
    sizes = list(next(iter(res["scaling"].values()))["duckdb"])
    out.append("|---|---|" + "---:|" * len(sizes))
    for shape, by_d in res["scaling"].items():
        for d, row in by_d.items():
            cells = [f"{row[n] * 1000:.2f}" if isinstance(row.get(n), float) else str(row.get(n, "-"))[:20] for n in sizes]
            out.append(f"| {shape} | {d} | " + " | ".join(cells) + " |")
    out.append("")
    return out


def scaling_section(data):
    out = ["## Large inputs (spark dialect, memory_limit 1GB)", "", "| case | outcome | seconds | detail |",
           "|---|---|---:|---|"]
    for r in data["results"]:
        detail = (r.get("result") or r.get("error") or "")[:70].replace("|", "\\|")
        out.append(f"| {r['name']} | {r['status']} | {r['seconds']:.2f} | {detail} |")
    out.append("")
    return out


def check(baseline_path):
    problems = []
    for name in ("tpch_sf0.1", "tpch_sf1"):
        data = load(name)
        if not data:
            continue
        for d in ("spark", "spark_strict"):
            ratios = [r.get(f"{d}_ratio") for r in data["results"] if r.get(f"{d}_ratio")]
            g = common.geomean(ratios)
            if g and g > 1.5:
                problems.append(f"{name} {d}: geometric mean ratio {g:.2f} > 1.5")
            for r in data["results"]:
                # 4x, not 3x: Q1's Spark decimal types are 128-bit where DuckDB's are 64-bit (~2.5-3.3x, see
                # docs/PERFORMANCE.md)
                if (r.get(f"{d}_ratio") or 0) > 4:
                    problems.append(f"{name} Q{r['query']} {d}: ratio {r[d + '_ratio']:.2f} > 4")
    _, func = first("func_smoke", "func")
    if func and baseline_path and Path(baseline_path).exists():
        base = {r["name"]: r.get("ratio") for r in json.loads(Path(baseline_path).read_text())["func"]}
        for r in func["results"]:
            b = base.get(r["name"])
            if b and r.get("ratio") and r["ratio"] > 3 * max(b, 1.0):
                problems.append(f"function {r['name']}: ratio {r['ratio']:.2f} > 3x baseline {b:.2f}")
    _, parse = first("parse_smoke", "parse")
    if parse:
        for shape, by_d in parse["results"]["scaling"].items():
            for d, row in by_d.items():
                small, big = row.get("1000"), row.get("10000")
                if isinstance(small, float) and isinstance(big, float) and big > 20 * small:
                    problems.append(f"parse {shape} {d}: 10000 items {big / small:.0f}x slower than 1000")
    return problems


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--check", default=None)
    args = ap.parse_args()
    lines = ["# Performance results", "", "Generated by `python bench/report.py --write` from `bench/out/*.json`. "
             "See [PERFORMANCE.md](PERFORMANCE.md) for the method and the analysis.", ""]
    machine = None
    for name in ("parse", "parse_smoke"):
        d = load(name)
        if d:
            lines += parse_section(d)
            machine = d["machine"]
            break
    name, d = first("func", "func_smoke")
    if d:
        lines += func_section(d)
        machine = machine or d["machine"]
    for name in ("tpch_sf1", "tpch_sf0.1"):
        d = load(name)
        if d:
            lines += tpch_section(name, d)
            break
    name, d = first("scaling", "scaling_smoke")
    if d:
        lines += scaling_section(d)
    if machine:
        lines += ["## Machine", "", "```", json.dumps(machine, indent=1), "```", ""]
    text = "\n".join(lines) + "\n"
    if args.write:
        RESULTS_MD.write_text(text)
        print(f"wrote {RESULTS_MD}")
    else:
        print(text)
    if args.check is not None:
        problems = check(args.check)
        for p in problems:
            print("THRESHOLD:", p)
        sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
