"""Signatures (stable keys to group findings) and run summaries.

usage: python test/fuzz/triage.py test/fuzz/out/<run>/      (re-summarize a run's findings)
"""
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

FUNC_RE = re.compile(r"\b([a-z_][a-z_0-9]*)\s*\(", re.I)
SQL_WORDS = {"select", "from", "where", "as", "and", "or", "not", "in", "values", "case", "when", "then", "else",
             "end", "over", "on", "using", "exists", "cast", "try_cast", "join", "lateral", "view", "group", "by"}


def mask_message(msg):
    """Mask the parts of an error message that vary between otherwise identical bugs."""
    if not msg:
        return ""
    msg = msg.split("\n")[0]
    msg = re.sub(r"system\.spark\.|__spark_\w+", "<fn>.", msg)
    msg = re.sub(r"'(?:[^'\\]|\\.)*'", "'?'", msg)
    msg = re.sub(r'"(?:[^"\\]|\\.)*"', '"?"', msg)
    msg = re.sub(r"\b\d+(\.\d+)?\b", "N", msg)
    msg = re.sub(r"\bat or near .*", "at or near ?", msg)
    msg = re.sub(r"(LINE N:).*", r"\1", msg)
    return msg[:160]


def functions_in(sql):
    names = {m.group(1).lower() for m in FUNC_RE.finditer(sql or "")}
    return sorted(names - SQL_WORDS)


def first_keyword(sql):
    m = re.match(r"\s*\(*\s*([A-Za-z_]+)", sql or "")
    return m.group(1).upper() if m else "?"


def signature(rec):
    kind = rec["kind"]
    duck = rec.get("duck") or {}
    err = duck.get("error") or ""
    if kind in ("crash", "sanitizer") and ("#0" in err or "#1" in err):
        frames = re.findall(r"#\d+ 0x[0-9a-f]+ in (\S+)", err)
        frames = [f for f in frames if not f.startswith(("__", "std::", "abort", "raise"))]
        return f"{kind}|" + "|".join(frames[:3])
    if kind in ("crash", "timeout"):
        muts = "+".join(sorted(set(rec.get("mutators") or []))) or rec.get("generator", "?")
        return f"{kind}|{rec.get('dialect')}|{first_keyword(rec['sql'])}|{muts}"
    if kind in ("internal", "duck_error", "strict_reject"):
        return f"{kind}|{mask_message(err)}"
    if kind in ("mismatch", "strict_mismatch"):
        sql = rec.get("reduced_sql") or rec["sql"]
        return f"{kind}|{','.join(functions_in(sql)[:6])}|{rec.get('difference', '?')}"
    if kind in ("lenient", "strict_lenient", "lenient_runtime"):
        spark = rec.get("spark") or {}
        return f"{kind}|{mask_message(spark.get('error'))[:80]}"
    return f"{kind}|?"


# names of the dialect's internal functions: they should not show up in error messages for invalid Spark SQL
INTERNAL_NAMES = re.compile(
    r'system\.spark|"system"|spark_kernel|__spark_|\bcast_value|_impl\b|concat_ws_array|\bspark_char|comparator_result|'
    r'zero_key|narrow_int|case_when|inline_rows|sum_result|try_avg_result|\bcmp_(eq|ne|lt|gt|le|ge)\b|op_bit|'
    r'\bas_(int|long|double|string|date|timestamp|boolean|binary|number)\b|from_number|\bsubscript\b|get_field')


def internal_name_errors(findings):
    """Findings whose DuckDB error message mentions an internal function name."""
    return [f for f in findings if INTERNAL_NAMES.search(str((f.get("duck") or {}).get("error") or ""))]


def summarize(findings, counts=None, title="fuzz run", examples=1):
    by_sig = defaultdict(list)
    for f in findings:
        by_sig[f.get("signature") or signature(f)].append(f)
    kinds = Counter(f["kind"] for f in findings)
    lines = [f"# {title}", ""]
    if counts:
        lines += ["## Inputs", "", "| outcome | count |", "|---|---|"]
        lines += [f"| {k} | {v} |" for k, v in sorted(counts.items())]
        lines.append("")
    leaking = internal_name_errors(findings)
    lines += ["## Error messages", "", f"{len(leaking)} of {len(findings)} findings have an internal function name in "
              "the DuckDB error message", ""]
    lines += ["## Findings", "", f"{len(findings)} findings, {len(by_sig)} signatures: "
              + ", ".join(f"{k}={v}" for k, v in sorted(kinds.items())), ""]
    for sig, items in sorted(by_sig.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        lines.append(f"### `{sig}` ({len(items)})")
        for f in items[:examples]:
            sql = f.get("reduced_sql") or f["sql"]
            shown = sql if len(sql) < 600 else sql[:600] + f" ... [{len(sql)} chars]"
            lines += ["", f"dialect `{f.get('dialect')}`, seed {f.get('seed')}", "", "```sql", shown, "```"]
            err = (f.get("duck") or {}).get("error")
            if err:
                lines += ["", f"> {str(err)[:400]}"]
            if f.get("spark") and f["kind"] not in ("crash", "timeout", "internal"):
                lines += ["", f"> spark: {str(f['spark'])[:300]}"]
        lines.append("")
    return "\n".join(lines) + "\n"


def load_findings(run_dir):
    out = []
    for p in sorted(Path(run_dir).glob("findings*.jsonl")):
        out += [json.loads(ln) for ln in p.read_text().splitlines() if ln.strip()]
    return out


if __name__ == "__main__":
    run = Path(sys.argv[1])
    print(summarize(load_findings(run), title=str(run)))
