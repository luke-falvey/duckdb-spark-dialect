"""Shrink a failing query while it keeps failing the same way.

reduce(sql, still_fails) greedily tries smaller variants of the statement and keeps the first one for
which still_fails(variant) is true, until nothing smaller fails or the step budget is spent:
  1. drop a top-level SELECT item, a WHERE / HAVING / ORDER BY clause, or a JOIN condition term
  2. replace a parenthesized expression or function call by one of its arguments, or by NULL / 0 / ''
  3. delete single tokens (a final ddmin-style pass)

usage (CLI): python test/fuzz/reduce.py "<sql>" --dialect spark   (reduces a Spark-vs-DuckDB difference)
"""
import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from corpus import lex, unlex  # noqa: E402

LEAVES = ["NULL", "0", "1", "''", "'a'"]


def _matching(toks, i):
    depth = 0
    for j in range(i, len(toks)):
        t = toks[j][1]
        if t in ("(", "["):
            depth += 1
        elif t in (")", "]"):
            depth -= 1
            if depth == 0:
                return j
    return None


def _split_args(toks, start, end):
    """Top-level comma-separated ranges inside toks[start:end]."""
    out, depth, cur = [], 0, start
    for j in range(start, end):
        t = toks[j][1]
        if t in ("(", "["):
            depth += 1
        elif t in (")", "]"):
            depth -= 1
        elif t == "," and depth == 0:
            out.append((cur, j))
            cur = j + 1
    out.append((cur, end))
    return [(a, b) for a, b in out if b > a]


def _top_level_clause_spans(toks):
    """Spans of top-level clauses that can be dropped: WHERE ..., HAVING ..., ORDER BY ... (not with LIMIT)."""
    spans = []
    depth = 0
    marks = []
    for j, (k, t) in enumerate(toks):
        if t in ("(", "["):
            depth += 1
        elif t in (")", "]"):
            depth -= 1
        elif depth == 0 and k == "word":
            marks.append((j, t.upper()))
    stops = {"WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "UNION", "INTERSECT", "EXCEPT", "MINUS", "SORT",
             "DISTRIBUTE", "CLUSTER", "WINDOW", "LATERAL", "OFFSET"}
    has_limit = any(w == "LIMIT" for _, w in marks)
    for idx, (j, w) in enumerate(marks):
        if w in ("WHERE", "HAVING") or (w == "ORDER" and not has_limit):
            end = next((m for m, w2 in marks[idx + 1:] if w2 in stops), len(toks))
            spans.append((j, end))
    return spans


def _select_items(toks):
    """Top-level SELECT item ranges (between the first SELECT and its FROM)."""
    depth = 0
    start = end = None
    for j, (k, t) in enumerate(toks):
        if t in ("(", "["):
            depth += 1
        elif t in (")", "]"):
            depth -= 1
        elif depth == 0 and k == "word":
            u = t.upper()
            if u == "SELECT" and start is None:
                start = j + 1
                if j + 1 < len(toks) and toks[j + 1][1].upper() == "DISTINCT":
                    start += 1
            elif u == "FROM" and start is not None:
                end = j
                break
    if start is None:
        return []
    return _split_args(toks, start, end if end is not None else len(toks))


def candidates(sql):
    toks = lex(sql)
    # 1. drop select items (keep at least one) and clauses
    items = _select_items(toks)
    if len(items) > 1:
        for a, b in reversed(items):
            # also drop the separating comma
            if b < len(toks) and toks[b][1] == ",":
                yield unlex(toks[:a] + toks[b + 1:])
            elif a > 0 and toks[a - 1][1] == ",":
                yield unlex(toks[:a - 1] + toks[b:])
    for a, b in _top_level_clause_spans(toks):
        yield unlex(toks[:a] + toks[b:])
    # 2. replace calls / parenthesized expressions by an argument or a leaf
    for i, (k, t) in enumerate(toks):
        if t != "(":
            continue
        j = _matching(toks, i)
        if j is None:
            continue
        start = i - 1 if i > 0 and toks[i - 1][0] == "word" and toks[i - 1][1].upper() not in (
            "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "ON", "BY", "HAVING", "AS", "OVER", "EXISTS") else i
        args = _split_args(toks, i + 1, j)
        for a, b in args:
            inner = toks[a:b]
            # a lambda argument `x -> body` cannot stand alone
            if any(tt == "->" for _, tt in inner):
                continue
            yield unlex(toks[:start] + inner + toks[j + 1:])
        if start != i or len(args) > 0:
            for leaf in LEAVES:
                yield unlex(toks[:start] + [("literal", leaf)] + toks[j + 1:])
    # 2b. replace literal arguments by simpler ones
    for i, (k, t) in enumerate(toks):
        if k in ("string", "number") and t not in ("0", "1", "''"):
            for leaf in ("0", "1", "''", "NULL"):
                yield unlex(toks[:i] + [("literal", leaf)] + toks[i + 1:])
    # 3. delete single tokens
    for i in range(len(toks) - 1, -1, -1):
        yield unlex(toks[:i] + toks[i + 1:])


def reduce(sql, still_fails, max_steps=300):
    """Returns (reduced sql, steps used)."""
    steps = 0
    current = unlex(lex(sql))
    progress = True
    seen = {current}
    while progress and steps < max_steps:
        progress = False
        for cand in candidates(current):
            if cand in seen or len(cand) >= len(current):
                continue
            seen.add(cand)
            steps += 1
            if still_fails(cand):
                current = cand
                progress = True
                break
            if steps >= max_steps:
                break
    return current, steps


def main():
    """Reduce a Spark-vs-DuckDB difference (rows differ, or DuckDB errors where Spark succeeds)."""
    import compare
    import gen
    from engines import DuckRunner, SparkOracle, use_utc
    ap = argparse.ArgumentParser()
    ap.add_argument("sql")
    ap.add_argument("--dialect", default="spark")
    ap.add_argument("--no-fuzz-schema", action="store_true", help="do not create the fuzz tables t and u")
    ap.add_argument("--steps", type=int, default=300)
    args = ap.parse_args()
    use_utc()
    spark = SparkOracle()
    duck = DuckRunner(dialect=args.dialect)
    if not args.no_fuzz_schema:
        spark.setup(gen.setup_statements())
        duck.setup(gen.setup_statements())
    s, d = spark.run(args.sql), duck.run(args.sql)
    kind = compare.classify_one(s, d, ordered=False)[0]
    print(f"original: {kind}")
    if kind == "pass":
        return

    def still(c):
        return compare.classify_one(spark.run(c), duck.run(c), ordered=False)[0] == kind
    out, steps = reduce(args.sql, still, args.steps)
    print(f"reduced in {steps} steps:\n{out}")
    print("spark:", spark.run(out))
    print("duck: ", duck.run(out))
    duck.close()


if __name__ == "__main__":
    main()
