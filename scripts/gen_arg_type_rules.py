"""Write src/spark_arg_types.inc: the argument types Spark's analyzer rejects, per function and argument position,
taken from Spark's recorded answers to the C6 matrix (test/cases/types/c6_function_args.sql, test/golden/).

spark_strict checks a function's arguments against this table at bind time (`check_arg`), so a call Spark rejects
with DATATYPE_MISMATCH is rejected too. Only direct arguments of the named function are used (a swept value nested
in another call is skipped), only analysis-time DATATYPE_MISMATCH errors count as a rejection, and kinds the dialect
cannot tell apart (TIMESTAMP and TIMESTAMP_NTZ, the interval kinds, the string values, the array element types) are
rejected only when Spark rejects every one of them.

usage: python scripts/gen_arg_type_rules.py [--check]
"""
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "test"))
sys.path.insert(0, str(ROOT / "test" / "fuzz"))
from harness import load_cases, load_golden  # noqa: E402
from args_sweep import TYPE_VALUES, TYPES_CASE_FILE  # noqa: E402

OUT = ROOT / "src" / "spark_arg_types.inc"

# the kinds check_arg distinguishes, and the swept types that make one up
KINDS = {
    "NULL": ["void"], "BOOLEAN": ["boolean"], "TINYINT": ["tinyint"], "SMALLINT": ["smallint"], "INTEGER": ["int"],
    "BIGINT": ["bigint"], "FLOAT": ["float"], "DOUBLE": ["double"], "DECIMAL": ["decimal"],
    "STRING": ["string", "string_num", "string_date"], "BINARY": ["binary"], "DATE": ["date"],
    "TIMESTAMP": ["timestamp", "timestamp_ntz"], "INTERVAL": ["interval_dt", "interval_ym"],
    "LIST": ["array_int", "array_str"], "MAP": ["map"], "STRUCT": ["struct"],
}
# calls the grammar rewrites by the shape of their arguments (lambdas, variadic forms): left alone
SKIP = {"transform", "filter", "exists", "forall", "aggregate", "reduce", "array_sort", "zip_with", "map_filter",
        "transform_keys", "transform_values", "map_zip_with", "stack", "json_tuple", "inline", "inline_outer",
        "explode", "explode_outer", "posexplode", "posexplode_outer", "reflect", "java_method", "try_reflect",
        "named_struct", "struct", "window", "session_window", "if", "iff", "case", "coalesce", "nvl", "nvl2",
        "ifnull", "nullif", "cast", "try_cast"}
# functions whose arguments must share a type: a rejection can come from the other arguments, not the type itself
SAME_TYPE = {"concat", "greatest", "least", "array", "map", "array_union", "array_intersect", "array_except",
             "arrays_overlap", "arrays_zip", "map_concat", "elt", "equal_null", "nanvl"}
NESTED = ("LIST", "MAP", "STRUCT")


def top_level_args(sql, fn):
    """The arguments of the first call of `fn` in sql, as text, or None."""
    m = re.search(r"(?<![\w.])" + re.escape(fn) + r" \(", sql)
    if not m:
        return None
    i = m.end()
    depth, quote, start, args = 0, None, i, []
    while i < len(sql):
        c = sql[i]
        if quote:
            if c == "\\":
                i += 1
            elif c == quote:
                quote = None
        elif c in "'\"`":
            quote = c
        elif c == "(":
            depth += 1
        elif c == ")":
            if depth == 0:
                args.append(sql[start:i].strip())
                return args
            depth -= 1
        elif c == "," and depth == 0:
            args.append(sql[start:i].strip())
            start = i + 1
        i += 1
    return None


def collect():
    golden = load_golden(str(TYPES_CASE_FILE), ())
    by_slot = defaultdict(list)  # (fn, slot) -> [(type, args, verdict)]
    for case in load_cases([str(TYPES_CASE_FILE)]):
        d = case.directives.get("sweep")
        if not d or "form=column" in d:
            continue
        parts = d.split()
        fn, slot, tname = parts[0], int(parts[1].split("=")[1]), parts[2].split("=")[1]
        if fn in SKIP or not re.fullmatch(r"[a-z_0-9]+", fn):
            continue
        e = golden.get(case.index)
        args = top_level_args(case.sql, fn)
        if e is None or args is None:
            continue
        if e["ok"]:
            verdict = "allow"
        elif e.get("phase") == "analysis" and "DATATYPE_MISMATCH" in (e.get("error_class") or e.get("error") or ""):
            verdict = "deny"
        else:
            verdict = "other"
        by_slot[(fn, slot)].append((tname, args, verdict))
    # functions with an array / map / struct argument in their example (by position): whether a value is accepted
    # next to it depends on its element types, which the matrix does not vary
    nested_positions = defaultdict(set)
    swept_values = set(TYPE_VALUES.values())
    for (fn, slot), cases in by_slot.items():
        for _, args, _ in cases:
            for i, a in enumerate(args):
                if a not in swept_values and re.match(r"(array|map|named_struct|struct) ?\(", a):
                    nested_positions[fn].add(i)
    rules = defaultdict(dict)  # fn -> {position: set(kinds)}
    for (fn, slot), cases in by_slot.items():
        # the argument position the slot is: the one holding the swept value in every case
        positions = None
        for tname, args, _ in cases:
            here = {i for i, a in enumerate(args) if a == TYPE_VALUES[tname]}
            positions = here if positions is None else positions & here
        if not positions or len(positions) != 1:
            continue
        pos = positions.pop()
        verdicts = {tname: verdict for tname, _, verdict in cases}
        denied = set()
        for kind, members in KINDS.items():
            seen = [verdicts[m] for m in members if m in verdicts]
            if seen and all(v == "deny" for v in seen):
                denied.add(kind)
        if fn in SAME_TYPE or (nested_positions[fn] and pos not in nested_positions[fn]):
            denied = set()  # the rejection may come from the other arguments
        elif nested_positions[fn]:
            denied -= set(NESTED)  # the position takes a nested value: which element types fit is not known here
        if denied and len(denied) < len(KINDS):
            rules[fn][pos] = denied
    return rules


def render(rules):
    lines = ["// Generated by scripts/gen_arg_type_rules.py from Spark's answers to the C6 matrix; do not edit.",
             "// {function, argument position (0-based), argument kinds Spark's analyzer rejects}"]
    for fn in sorted(rules):
        for pos in sorted(rules[fn]):
            kinds = " | ".join(f"ArgKind::{k}" for k in sorted(rules[fn][pos]))
            lines.append(f'{{"{fn}", {pos}, {kinds}}},')
    return "\n".join(lines) + "\n"


def main():
    text = render(collect())
    if "--check" in sys.argv:
        if not OUT.exists() or OUT.read_text() != text:
            raise SystemExit(f"{OUT} is out of date: run python scripts/gen_arg_type_rules.py")
        return
    OUT.write_text(text)
    print(f"wrote {OUT} ({text.count(chr(10)) - 2} rules)")


if __name__ == "__main__":
    main()
