"""Function-argument sweep: call every Spark function with edge-case arguments, compare with Spark.

Seeds are the documented examples in test/spark35_function_docs.json (plus test/fuzz/manual_seeds.sql).
Each literal argument is replaced by edge values of the same kind, one slot at a time (and some pairs),
both as a constant and as a column (`FROM VALUES ... AS t(x)`, so DuckDB sees a real vector with NULLs
and repeats instead of a constant-folded value).

usage: python test/fuzz/args_sweep.py [--seed N] [--per-function N] [--functions f1,f2] [--no-strict]
"""
import argparse
import json
import random
import re
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import corpus  # noqa: E402
from corpus import lex  # noqa: E402
from engines import DuckRunner, SparkOracle, scratch_dir, use_utc  # noqa: E402
import compare  # noqa: E402
import triage  # noqa: E402

GENERATOR = "args_sweep@1"
DOCS = corpus.TEST_DIR / "spark35_function_docs.json"
MANUAL = Path(__file__).resolve().parent / "manual_seeds.sql"

NONDETERMINISTIC = {
    "rand", "randn", "random", "uuid", "shuffle", "current_date", "current_timestamp", "now", "curdate",
    "localtimestamp", "unix_timestamp", "monotonically_increasing_id", "spark_partition_id", "input_file_name",
    "input_file_block_start", "input_file_block_length", "version", "current_user", "user", "session_user",
    "current_timezone", "reflect", "java_method", "try_reflect", "uniform", "randstr", "aes_encrypt",
    "current_catalog", "current_database", "current_schema", "assert_true", "raise_error",
    # results that depend on hash order or ties: Spark documents them as nondeterministic
    "collect_set", "mode",
}
# arguments that are sizes: huge values are legitimately slow / huge in both engines
SIZE_FUNCTIONS = {"repeat", "space", "array_repeat", "sequence", "lpad", "rpad", "range", "explode",
                  "posexplode", "stack", "format_string", "printf", "randstr", "array_insert", "array_resize"}

EDGES = {
    "int": ["0", "1", "-1", "2147483647", "-2147483648", "NULL", "127Y", "-128Y", "32767S",
            "9223372036854775807L", "-9223372036854775808L", "2", "3", "100"],
    "decimal": ["0.0", "-0.0BD", "99999999999999999999.99BD", "CAST(1 AS DECIMAL(38,0))", "0.1", "NULL",
                "-1.5", "1.005", "123456789.123456789"],
    "double": ["0.0D", "-0.0D", "CAST('NaN' AS DOUBLE)", "CAST('Infinity' AS DOUBLE)",
               "CAST('-Infinity' AS DOUBLE)", "1e308D", "4.9e-324D", "0.1D", "NULL", "1.0E7D", "9999999.0D",
               "0.001D", "0.0001D", "-2.5D", "1.5D"],
    "string": ["''", "' '", "'  a  '", "'NULL'", "'é'", "'日本語'", "'😀'", "'a\\\\b'", "'%_'", "'\\t\\n'",
               "repeat('ab', 5000)", "NULL", "'abc'", "'ABC'", "'a,b,,c,'", "'1'", "'-1.5'", "'1e3'",
               "'true'", "'2024-02-29'", "'{\"a\":1}'"],
    "date_string": ["'1970-01-01'", "'0001-01-01'", "'9999-12-31'", "'2024-02-29'", "'2023-02-29'",
                    "'2024-1-5'", "'2024-01-01 12:34:56'", "'2024'", "''", "'not a date'", "NULL"],
    "pattern": ["''", "'yyyy-MM-dd HH:mm:ss.SSSSSS'", "'G'", "'E'", "'EEEE'", "'''quoted'''", "'yyyy'",
                "'MMM d, yyyy'", "'h:mm a'", "'Q'", "NULL", "'x'", "'zzz'"],
    "json": ["'{}'", "'[]'", "'{\"a\":null}'", "'{\"a\":[1,2,{\"b\":\"c\"}]}'", "'not json'", "''", "NULL",
             "'{\"a\":1e400}'", "'{\"a\":\"\\\\u00e9\"}'", "'[1,'"],
    "regex": ["''", "'('", "'\\\\'", "'.*'", "'(a|b)*'", "'[0-9]+'", "'^$'", "'\\\\d'", "NULL", "'a{2,}'"],
    "date": ["DATE '1970-01-01'", "DATE '0001-01-01'", "DATE '9999-12-31'", "DATE '1582-10-15'",
             "DATE '2024-02-29'", "CAST(NULL AS DATE)", "DATE '1969-12-31'"],
    "timestamp": ["TIMESTAMP '1970-01-01 00:00:00'", "TIMESTAMP '1969-12-31 23:59:59.999999'",
                  "TIMESTAMP '2024-02-29 12:00:00.123456'", "TIMESTAMP '9999-12-31 23:59:59.999999'",
                  "CAST(NULL AS TIMESTAMP)", "TIMESTAMP '0001-01-01 00:00:00'"],
    "bool": ["true", "false", "NULL"],
    "binary": ["X''", "X'00'", "X'FF'", "X'00FF00FF'", "CAST(NULL AS BINARY)"],
    "array": ["array()", "array(NULL)", "array(1, NULL, 1)", "CAST(NULL AS ARRAY<INT>)", "sequence(1, 1000)",
              "array(3, 1, 2)", "array(-0.0D, 0.0D, CAST('NaN' AS DOUBLE))", "array('b', 'a', NULL)"],
    "map": ["map()", "map(1, NULL)", "CAST(NULL AS MAP<INT,INT>)", "map('a', 1, 'b', 2)"],
}


def classify_literal(tok, prev_word, fn):
    kind, text = tok
    if kind == "number":
        t = text.upper()
        if t.endswith(("D", "F")) or "E" in t:
            return "double"
        if "." in t or t.endswith("BD"):
            return "decimal"
        return "int"
    if kind == "string":
        body = text[1:-1]
        if prev_word in ("DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "INTERVAL", "X"):
            return None  # handled as a typed literal
        if re.fullmatch(r"\d{4}-\d{1,2}-\d{1,2}([ T][\d:.]+)?", body):
            return "date_string"
        if re.search(r"(yyyy|yy|MM|dd|HH|mm|ss|EEE|MMM)", body) and "date" in fn or fn in (
                "to_char", "date_format", "from_unixtime", "to_timestamp", "to_date", "unix_timestamp",
                "to_unix_timestamp", "try_to_timestamp"):
            return "pattern"
        if body.startswith(("{", "[")):
            return "json"
        if fn in ("regexp", "rlike", "regexp_like", "regexp_extract", "regexp_extract_all", "regexp_replace",
                  "regexp_count", "regexp_instr", "regexp_substr", "split") and re.search(r"[\\\[\](){}+*?^$|]", body):
            return "regex"
        return "string"
    if kind == "word" and text.lower() in ("true", "false"):
        return "bool"
    return None


def slots(sql, fn):
    """Positions of replaceable literal arguments: list of (start, end, kind) token ranges."""
    toks = lex(sql)
    out = []
    i = 0
    while i < len(toks):
        kind, text = toks[i]
        up = text.upper() if kind == "word" else None
        nxt = toks[i + 1] if i + 1 < len(toks) else None
        if up in ("DATE", "TIMESTAMP") and nxt and nxt[0] == "string":
            out.append((i, i + 2, "date" if up == "DATE" else "timestamp"))
            i += 2
            continue
        if up == "X" and nxt and nxt[0] == "string":
            out.append((i, i + 2, "binary"))
            i += 2
            continue
        if kind == "word" and text.lower() in ("array", "map") and nxt and nxt[1] == "(" and i > 0 and toks[i - 1][1] in ("(", ","):
            # a whole array(...) / map(...) argument
            depth, j = 0, i + 1
            while j < len(toks):
                depth += {"(": 1, ")": -1}.get(toks[j][1], 0)
                if depth == 0:
                    break
                j += 1
            out.append((i, j + 1, text.lower()))
            i = j + 1
            continue
        prev = toks[i - 1][1].upper() if i > 0 and toks[i - 1][0] == "word" else ""
        k = classify_literal(toks[i], prev, fn)
        if k:
            # a leading unary minus belongs to the number
            start = i - 1 if i > 0 and toks[i - 1][1] == "-" and (i < 2 or toks[i - 2][1] in ("(", ",")) else i
            out.append((start, i + 1, k))
        i += 1
    return toks, out


def substitute(toks, replacements):
    """replacements: {(start, end): text}"""
    out, i = [], 0
    starts = {s: (e, t) for (s, e), t in replacements.items()}
    while i < len(toks):
        if i in starts:
            e, t = starts[i]
            out.append(t)
            i = e
        else:
            out.append(toks[i][1])
            i += 1
    return " ".join(out)


SIMPLE_SELECT = re.compile(r"^\s*SELECT\s+(?!.*\bFROM\b)(?!.*\bLATERAL\b)(?!.*\bOVER\b)(.+)$", re.I | re.S)
AGGREGATE_HINT = re.compile(r"\bGROUP\s+BY\b|\bFROM\b", re.I)


def column_form(toks, slot, edge, original):
    """Same call with the slot read from a column of a 4-row VALUES table."""
    sql = substitute(toks, {(slot[0], slot[1]): "x"})
    m = SIMPLE_SELECT.match(sql)
    if not m:
        return None
    exprs = m.group(1)
    return f"SELECT {exprs} FROM VALUES ({edge}), ({original}), (NULL), ({edge}) AS t(x)"


def variants(fn, example, rng, per_example):
    toks, sl = slots(example, fn)
    out = []
    for s in sl:
        original = " ".join(t for _, t in toks[s[0]:s[1]])
        for edge in EDGES[s[2]]:
            if fn in SIZE_FUNCTIONS and s[2] in ("int", "decimal", "double") and edge not in (
                    "0", "1", "-1", "2", "3", "NULL", "0.0", "0.1", "1.5D", "-2.5D", "0.0D", "-0.0D"):
                continue  # huge sizes make both engines allocate gigabytes
            if edge == original:
                continue
            out.append(("const", substitute(toks, {(s[0], s[1]): edge})))
            col = column_form(toks, s, edge, original)
            if col:
                out.append(("column", col))
    # some pairs of slots
    if len(sl) >= 2:
        for _ in range(min(6, per_example // 4)):
            a, b = rng.sample(sl, 2)
            out.append(("pair", substitute(toks, {(a[0], a[1]): rng.choice(EDGES[a[2]]),
                                                 (b[0], b[1]): rng.choice(EDGES[b[2]])})))
    rng.shuffle(out)
    seen, uniq = set(), []
    for v in out:
        if v[1] not in seen and v[1] != example:
            seen.add(v[1])
            uniq.append(v)
    return uniq[:per_example]


def load_seeds(functions=None):
    docs = json.loads(DOCS.read_text())
    seeds = []
    for fn, info in sorted(docs.items()):
        if fn in NONDETERMINISTIC or (functions and fn not in functions):
            continue
        for ex in info.get("examples", []):
            seeds.append((fn, ex))
    if MANUAL.exists():
        from harness import load_cases
        for case in load_cases([MANUAL]):
            fn = case.directives.get("function", "?")
            if not functions or fn in functions:
                seeds.append((fn, case.sql))
    return seeds


# --types: one typical value of every type of the universe in plan 7.1, per argument slot
TYPE_VALUES = {
    "void": "NULL", "boolean": "true", "tinyint": "CAST(1 AS TINYINT)", "smallint": "CAST(1 AS SMALLINT)", "int": "1",
    "bigint": "CAST(1 AS BIGINT)", "bigint_wrap": "4294967297L", "float": "CAST(1.5 AS FLOAT)", "double": "1.5D",
    "decimal": "CAST(1.5 AS DECIMAL(5,2))", "string": "'a'", "string_num": "'1'", "string_date": "'2024-01-01'",
    "binary": "X'41'", "date": "DATE'2024-01-01'", "timestamp": "TIMESTAMP'2024-01-01 10:00:00'",
    "timestamp_ntz": "TIMESTAMP_NTZ'2024-01-01 10:00:00'", "interval_dt": "INTERVAL '1' DAY",
    "interval_ym": "INTERVAL '1' MONTH", "array_int": "array(1, 2)", "array_str": "array('a')", "map": "map('a', 1)",
    "struct": "named_struct('a', 1)",
}
COLUMN_TYPES = ["int", "string", "double", "decimal", "date", "array_int"]
TYPES_CASE_FILE = corpus.TEST_DIR / "cases" / "types" / "c6_function_args.sql"


def emit_types(per_function_examples=1, max_slots=4):
    """Write the C6 case file: every documented function, each argument slot, a value of every type (literal form
    for all types, column form for COLUMN_TYPES). Deterministic. Spark's answers are recorded by make_golden.py."""
    lines = ["-- C6 function arguments: each slot of the documented examples replaced by a value of every type.",
             "-- Generated by test/fuzz/args_sweep.py --types --emit; do not edit. Expectations come from Spark.", ""]
    n = 0
    taken = Counter()
    for fn, example in load_seeds():
        if taken[fn] >= per_function_examples:
            continue
        toks, sl = slots(example, fn)
        if not sl:
            continue
        taken[fn] += 1
        for idx, slot in enumerate(sl[:max_slots]):
            original = " ".join(t for _, t in toks[slot[0]:slot[1]])
            for tname, value in TYPE_VALUES.items():
                if value == original:
                    continue
                lines.append(f"-- @sweep {fn} slot={idx} type={tname}")
                lines.append(substitute(toks, {(slot[0], slot[1]): value}).rstrip().rstrip(";") + ";")
                lines.append("")
                n += 1
                if tname in COLUMN_TYPES:
                    col = column_form(toks, slot, value, original)
                    if col:
                        lines.append(f"-- @sweep {fn} slot={idx} type={tname} form=column")
                        lines.append(col.rstrip().rstrip(";") + ";")
                        lines.append("")
                        n += 1
    TYPES_CASE_FILE.parent.mkdir(parents=True, exist_ok=True)
    TYPES_CASE_FILE.write_text("\n".join(lines))
    print(f"wrote {n} cases to {TYPES_CASE_FILE}")


def record_signatures():
    """--types --record: summarize the recorded Spark answers of the C6 file into test/golden/signatures.json.gz
    ({function: {slot: {type: [accepted, result type]}}}) and derive the INT parameters."""
    import gzip
    from harness import load_cases, load_golden
    golden = load_golden(str(TYPES_CASE_FILE), ())
    sig = {}
    for case in load_cases([str(TYPES_CASE_FILE)]):
        d = case.directives.get("sweep")
        if not d:
            continue
        parts = d.split()
        fn, slot, tname = parts[0], parts[1].split("=")[1], parts[2].split("=")[1]
        column = len(parts) > 3
        e = golden[case.index]
        entry = [bool(e["ok"]), (e.get("types") or [None])[0] if e["ok"] else None,
                 repr(e["rows"])[:80] if e["ok"] else None]
        sig.setdefault(fn, {}).setdefault(slot, {})[tname + (":column" if column else "")] = entry
    out = corpus.TEST_DIR / "golden" / "signatures.json.gz"
    with gzip.open(out, "wt") as f:
        json.dump(sig, f, sort_keys=True)
    # INT parameters: the 4294967297L argument gives the result of 1 (it wraps to INT in Spark)
    derived = {}
    for fn, slots_ in sig.items():
        for slot, types in slots_.items():
            wrap, one = types.get("bigint_wrap"), types.get("int")
            if wrap and one and wrap[0] and one[0] and wrap[2] == one[2]:
                derived.setdefault(fn, []).append(int(slot))
    (corpus.TEST_DIR / "golden" / "int_parameters_derived.json").write_text(json.dumps(derived, sort_keys=True, indent=1))
    print(f"wrote {out} ({len(sig)} functions), {len(derived)} functions with INT parameters")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--types", action="store_true", help="type sweep (plan C6): with --emit write the case file, with --record summarize the goldens")
    ap.add_argument("--emit", action="store_true")
    ap.add_argument("--record", action="store_true")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--per-function", type=int, default=40, help="variants per function (spread over its examples)")
    ap.add_argument("--functions", default=None)
    ap.add_argument("--no-strict", action="store_true")
    ap.add_argument("--timeout", type=float, default=20.0)
    ap.add_argument("--out", default=None)
    ap.add_argument("--replay", help="re-run the failing findings of an earlier run (findings.jsonl)")
    ap.add_argument("--per-signature", type=int, default=2, help="with --replay: examples per signature")
    args = ap.parse_args()
    if args.types:
        if args.emit:
            emit_types()
        if args.record:
            record_signatures()
        return
    use_utc()
    import os
    out_arg = Path(args.out).resolve() if args.out else None
    replay = Path(args.replay).resolve() if args.replay else None
    os.chdir(scratch_dir("sweep-"))
    rng = random.Random(args.seed)
    functions = set(args.functions.split(",")) if args.functions else None
    seeds = load_seeds(functions)
    per_fn = Counter(fn for fn, _ in seeds)

    run_id = time.strftime("%Y%m%d-%H%M%S") + f"-sweep-{args.seed}"
    out_dir = out_arg or (corpus.FUZZ_DIR / "out" / run_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    queries = []
    if replay:
        taken = Counter()
        for line in replay.read_text().splitlines():
            f = json.loads(line)
            if f["kind"] not in compare.FAILING or f.get("known"):
                continue
            if any(m in (f["spark"].get("error") or "") for m in SparkOracle.DEAD_MARKERS):
                continue  # recorded while the Spark JVM was dead
            if taken[f["signature"]] < args.per_signature:
                taken[f["signature"]] += 1
                queries.append((f["function"], f["form"], f["sql"]))
    else:
        for fn, ex in seeds:
            n = max(4, args.per_function // per_fn[fn])
            queries += [(fn, form, sql) for form, sql in variants(fn, ex, rng, n)]
    print(f"args_sweep: {len(seeds)} seed examples, {len(queries)} variants, out={out_dir}", flush=True)

    spark = SparkOracle()
    lenient = DuckRunner(dialect="spark", timeout=args.timeout)
    strict = None if args.no_strict else DuckRunner(dialect="spark_strict", timeout=args.timeout)
    known = compare.load_known()
    counts = Counter()
    findings = []
    start = time.time()
    for i, (fn, form, sql) in enumerate(queries):
        s = spark.run(sql)
        d = lenient.run(sql)
        t = strict.run(sql) if strict else None
        found = compare.classify(s, d, t, ordered=False)
        counts["queries"] += 1
        counts["spark_ok" if s.ok else "spark_error"] += 1
        if not found:
            counts["pass"] += 1
        for kind, dialect, diff in found:
            duck = d if dialect == "spark" else t
            rec = {"kind": kind, "dialect": dialect, "sql": sql, "function": fn, "form": form, "seed": args.seed,
                   "generator": GENERATOR, "difference": diff,
                   "spark": {"ok": s.ok, "rows": repr(s.rows)[:500], "error": s.error, "phase": s.phase},
                   "duck": {"status": duck.status, "rows": repr(duck.rows)[:500], "error": duck.error}}
            rec["signature"] = f"{kind}|{fn}|{diff or triage.mask_message(duck.error or s.error)[:60]}"
            k = compare.is_known(rec["signature"], known, rec["sql"])
            rec["known"] = bool(k)
            counts[("known:" if k else "") + kind] += 1
            findings.append(rec)
            with open(out_dir / "findings.jsonl", "a") as f:
                f.write(json.dumps(rec) + "\n")
        if i % 500 == 499:
            print(f"  {i + 1}/{len(queries)} ({(i + 1) / (time.time() - start):.0f}/s) "
                  f"findings={sum(1 for f in findings if not f['known'])}", flush=True)
    lenient.close()
    if strict:
        strict.close()
    summary = triage.summarize(findings, counts, title=f"args_sweep seed={args.seed}")
    (out_dir / "summary.md").write_text(summary)
    print("\n".join(summary.splitlines()[:40]))
    failing = [f for f in findings if f["kind"] in compare.FAILING and not f["known"]]
    print(f"failing findings: {len(failing)} in {len({f['signature'] for f in failing})} signatures")
    sys.exit(1 if failing else 0)


if __name__ == "__main__":
    main()
