"""Generate test/cases/types/<matrix>.sql, the type conversion matrices C1-C14 of docs/plans/spark-parity-testing.md.

usage: python scripts/gen_type_cases.py [matrix ...]        (default: all)

Deterministic. Every file starts with `-- @exact`; cells that can raise an error are one statement each, cells that
cannot (typeof of decimal arithmetic) are packed. The expectations come from Spark (test/make_golden.py), never from
this script.
"""
import itertools
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "test" / "cases" / "types"


class T:
    """A type of the universe: Spark name, typed literal expressions (typical, edge, NULL), a group."""

    def __init__(self, name, group, values, short=None, literal_ok=True):
        self.name, self.group, self.values = name, group, values
        self.short = short or name.lower().replace("(", "").replace(")", "").replace(",", "_").replace(" ", "_").replace("<", "_").replace(">", "")
        self.literal_ok = literal_ok

    @property
    def null(self):
        return f"CAST(NULL AS {self.name})"


def cast(v, t):
    return f"CAST({v} AS {t})"


def intlit(t, lo, hi):
    return [cast(1, t), cast(lo, t), cast(hi, t), cast("NULL", t)]


STRINGS = ["'12'", "' 12 '", "'+12'", "'1.5'", "'1.'", "'.5'", "'1e3'", "'0x1A'", "'12abc'", "''", "'NaN'", "'Infinity'",
           "'-inf'", "'true'", "'T'", "'yes'", "'1'", "'2147483648'", "'9223372036854775808'", "'2024-02-29'", "'2024-2-9'",
           "'2024'", "'2024-02-30'", "'2024-01-01 10:00:00'", "'2024-01-01T10:00:00Z'", "'2024-01-01 10:00:00+05:30'",
           "'10:00:00'", "'1 day'", "'INTERVAL 1 DAY'", "'日本語'", "NULL"]

TYPES = [
    T("BOOLEAN", "boolean", ["true", "false", cast("NULL", "BOOLEAN")]),
    T("TINYINT", "integral", intlit("TINYINT", -128, 127)),
    T("SMALLINT", "integral", intlit("SMALLINT", -32768, 32767)),
    T("INT", "integral", intlit("INT", -2147483648, 2147483647)),
    T("BIGINT", "integral", intlit("BIGINT", "-9223372036854775808", "9223372036854775807")),
    T("FLOAT", "fractional", [cast("1.5", "FLOAT"), cast("'NaN'", "FLOAT"), cast("-0.0", "FLOAT"), cast("NULL", "FLOAT")]),
    T("DOUBLE", "fractional", ["1.5D", cast("'NaN'", "DOUBLE"), cast("'Infinity'", "DOUBLE"), cast("-0.0", "DOUBLE"), cast("NULL", "DOUBLE")]),
    T("DECIMAL(5,2)", "decimal", [cast("123.45", "DECIMAL(5,2)"), cast("-999.99", "DECIMAL(5,2)"), cast("NULL", "DECIMAL(5,2)")]),
    T("DECIMAL(10,0)", "decimal", [cast("1234567890", "DECIMAL(10,0)"), cast("-9999999999", "DECIMAL(10,0)"), cast("NULL", "DECIMAL(10,0)")]),
    T("DECIMAL(20,5)", "decimal", [cast("123456789012345.12345", "DECIMAL(20,5)"), cast("0.00001", "DECIMAL(20,5)"), cast("NULL", "DECIMAL(20,5)")]),
    T("DECIMAL(38,18)", "decimal", [cast("1.5", "DECIMAL(38,18)"), cast("12345678901234567890.123456789012345678", "DECIMAL(38,18)"), cast("NULL", "DECIMAL(38,18)")]),
    T("DECIMAL(38,0)", "decimal", [cast("1", "DECIMAL(38,0)"), cast("99999999999999999999999999999999999999", "DECIMAL(38,0)"), cast("NULL", "DECIMAL(38,0)")]),
    T("STRING", "text", ["'abc'", "''", "'日本語'", cast("NULL", "STRING")]),
    T("CHAR(5)", "text", [cast("'ab'", "CHAR(5)"), cast("NULL", "CHAR(5)")], literal_ok=False),
    T("VARCHAR(5)", "text", [cast("'ab'", "VARCHAR(5)"), cast("NULL", "VARCHAR(5)")], literal_ok=False),
    T("BINARY", "text", ["X'4142'", "X''", cast("NULL", "BINARY")]),
    T("DATE", "datetime", ["DATE'2024-02-29'", "DATE'0001-01-01'", "DATE'9999-12-31'", cast("NULL", "DATE")]),
    T("TIMESTAMP", "datetime", ["TIMESTAMP'2024-02-29 10:30:00.123456'", "TIMESTAMP'0001-01-01 00:00:00'", "TIMESTAMP'2024-03-10 02:30:00'", cast("NULL", "TIMESTAMP")]),
    T("TIMESTAMP_NTZ", "datetime", ["TIMESTAMP_NTZ'2024-02-29 10:30:00.123456'", "TIMESTAMP_NTZ'0001-01-01 00:00:00'", cast("NULL", "TIMESTAMP_NTZ")]),
    T("INTERVAL YEAR TO MONTH", "interval", ["INTERVAL '1-2' YEAR TO MONTH", "INTERVAL '-0-1' YEAR TO MONTH", cast("NULL", "INTERVAL YEAR TO MONTH")]),
    T("INTERVAL DAY TO SECOND", "interval", ["INTERVAL '1 02:03:04.5' DAY TO SECOND", "INTERVAL '-0 00:00:01' DAY TO SECOND", cast("NULL", "INTERVAL DAY TO SECOND")]),
    T("INTERVAL MONTH", "interval", ["INTERVAL '5' MONTH", cast("NULL", "INTERVAL MONTH")]),
    T("INTERVAL HOUR", "interval", ["INTERVAL '5' HOUR", cast("NULL", "INTERVAL HOUR")]),
    T("ARRAY<INT>", "nested", ["array(1, 2)", "array()", "array(1, NULL)", cast("NULL", "ARRAY<INT>")]),
    T("ARRAY<STRING>", "nested", ["array('a', 'b')", "array('1', NULL)", cast("NULL", "ARRAY<STRING>")]),
    T("ARRAY<ARRAY<INT>>", "nested", ["array(array(1), array(2, 3))", "array(array(1), NULL)", cast("NULL", "ARRAY<ARRAY<INT>>")]),
    T("MAP<STRING,INT>", "nested", ["map('a', 1)", "map('a', NULL)", cast("NULL", "MAP<STRING,INT>")]),
    T("MAP<INT,STRING>", "nested", ["map(1, 'a')", "map(1, NULL)", cast("NULL", "MAP<INT,STRING>")]),
    T("STRUCT<a:INT,b:STRING>", "nested", ["named_struct('a', 1, 'b', 'x')", "named_struct('a', NULL, 'b', NULL)", cast("NULL", "STRUCT<a:INT,b:STRING>")]),
    T("STRUCT<a:BIGINT,b:STRING>", "nested", [cast("named_struct('a', 1, 'b', 'x')", "STRUCT<a:BIGINT,b:STRING>")]),
    T("STRUCT<x:INT,y:STRING>", "nested", ["named_struct('x', 1, 'y', 'x')"]),
]
BY_GROUP = {}
for _t in TYPES:
    BY_GROUP.setdefault(_t.group, []).append(_t)
SCALAR = [t for t in TYPES if t.group != "nested"]
NUMERIC = [t for t in TYPES if t.group in ("integral", "fractional", "decimal")]
STRING_T = next(t for t in TYPES if t.name == "STRING")
VOID = "NULL"


def vals(t, n=2):
    return [v for v in t.values if not v.startswith("CAST(NULL")][:n]


def stmt(sql):
    return sql.rstrip().rstrip(";") + ";\n"


def write(name, header, cases, setup=()):
    OUT.mkdir(parents=True, exist_ok=True)
    lines = ["-- @exact", f"-- {header}", "-- Generated by scripts/gen_type_cases.py; do not edit. Expectations come from Spark.", ""]
    for s in setup:
        lines += ["-- @setup", stmt(s)]
    for c in cases:
        lines.append(stmt(c))
    (OUT / f"{name}.sql").write_text("\n".join(lines))
    print(f"{name}: {len(cases)} cases")


def values_rows(exprs):
    return ", ".join(f"({e})" for e in exprs)


# ---------------------------------------------------------------------------
def c1_casts():
    cases = []
    sources = [(t.name, t.values[:3] if t.group != "text" or t is not STRING_T else STRINGS) for t in TYPES if t.literal_ok]
    targets = [t.name for t in TYPES]
    for sname, svals in sources:
        for v in svals:
            for tgt in targets:
                cases.append(f"SELECT typeof(CAST({v} AS {tgt})), CAST({v} AS {tgt})")
                cases.append(f"SELECT typeof(TRY_CAST({v} AS {tgt})), TRY_CAST({v} AS {tgt})")
        # column form: one statement per target
        for tgt in targets:
            cases.append(f"SELECT typeof(CAST(v AS {tgt})), CAST(v AS {tgt}) FROM VALUES {values_rows(svals[:4])} AS t(v)")
    for v in ["NULL", "CAST(NULL AS INT)"]:
        for tgt in targets:
            cases.append(f"SELECT typeof(CAST({v} AS {tgt})), CAST({v} AS {tgt})")
    for fn in ["tinyint", "smallint", "int", "bigint", "float", "double", "decimal", "string", "binary", "boolean", "date", "timestamp"]:
        for t in SCALAR:
            if not t.literal_ok:
                continue
            for v in vals(t, 2):
                cases.append(f"SELECT typeof({fn}({v})), {fn}({v})")
    for v in STRINGS:
        for fn in ["int", "bigint", "double", "boolean", "date", "timestamp", "decimal"]:
            cases.append(f"SELECT typeof({fn}({v})), {fn}({v})")
    write("c1_casts", "C1 explicit casts: CAST, TRY_CAST and the function forms, literal and column forms", cases)


def c2_string_rendering():
    cases = []
    for t in TYPES:
        for v in t.values[:4]:
            cases.append(f"SELECT typeof(CAST({v} AS STRING)), CAST({v} AS STRING)")
            cases.append(f"SELECT typeof(concat({v}, 'x')), concat({v}, 'x')")
            cases.append(f"SELECT typeof(concat_ws('-', {v}, 'x')), concat_ws('-', {v}, 'x')")
            cases.append(f"SELECT typeof({v} || 'x'), {v} || 'x'")
            cases.append(f"SELECT format_string('%s', {v})")
            cases.append(f"SELECT to_json(named_struct('v', {v}))")
    for e in ["array(1, NULL)", "map('a', NULL)", "array(array(1, NULL), NULL)", "named_struct('a', NULL, 'b', array(NULL))", "map(1, array(NULL))",
              "array(named_struct('a', 1))", "array(DATE'2024-01-01', NULL)", "array(TIMESTAMP'2024-01-01 10:00:00', NULL)",
              "map('k', CAST(1.50 AS DECIMAL(5,2)))", "array(1.5D, CAST('NaN' AS DOUBLE))", "array(X'4142', NULL)", "array(true, NULL)"]:
        cases.append(f"SELECT CAST({e} AS STRING)")
        cases.append(f"SELECT array_join(array(CAST({e} AS STRING)), ',')")
    write("c2_string_rendering", "C2 rendering as string: cast, concat, ||, concat_ws, format_string, to_json, nested NULLs", cases)


def c3_arithmetic():
    cases = []
    ops = ["+", "-", "*", "/", "%", "div"]
    types = [t for t in TYPES if t.literal_ok and t.group != "nested"]
    for a, b in itertools.product(types, types):
        va, vb = vals(a, 1)[0] if vals(a, 1) else a.null, vals(b, 1)[0] if vals(b, 1) else b.null
        for op in ops:
            cases.append(f"SELECT typeof({va} {op} {vb}), {va} {op} {vb}")
    for a in types:
        va = vals(a, 1)[0]
        for fn in ["-", "+", "~"]:
            cases.append(f"SELECT typeof({fn}({va})), {fn}({va})")
        for fn in ["abs", "sign", "negative", "positive"]:
            cases.append(f"SELECT typeof({fn}({va})), {fn}({va})")
        for b in types:
            if a.group in ("integral",) and b.group in ("integral",):
                vb = vals(b, 1)[0]
                for op in ["&", "|", "^"]:
                    cases.append(f"SELECT typeof({va} {op} {vb}), {va} {op} {vb}")
                cases.append(f"SELECT typeof(shiftleft({va}, {vb})), shiftleft({va}, {vb})")
            vb = vals(b, 1)[0]
            cases.append(f"SELECT typeof(pmod({va}, {vb})), pmod({va}, {vb})")
    # column form for a diagonal of mixed types
    for a, b in itertools.product(NUMERIC, NUMERIC):
        cases.append(f"SELECT typeof(a + b), a + b, a * b, a / b FROM VALUES ({vals(a, 1)[0]}, {vals(b, 1)[0]}), ({a.null}, {vals(b, 1)[0]}) AS t(a, b)")
    write("c3_arithmetic", "C3 arithmetic and bitwise operators over every pair of scalar types, literal and column forms", cases)


def c4_comparison():
    cases = []
    types = [t for t in TYPES if t.literal_ok and t.group != "nested"]
    for a, b in itertools.product(types, types):
        va, vb = vals(a, 1)[0], vals(b, 1)[0]
        for op in ["=", "<=>", "<", ">="]:
            cases.append(f"SELECT typeof({va} {op} {vb}), {va} {op} {vb}")
        cases.append(f"SELECT {va} BETWEEN {vb} AND {vb}")
        cases.append(f"SELECT {va} IN ({vb}, {vb})")
        cases.append(f"SELECT {va} IN (SELECT x FROM VALUES ({vb}) AS t(x))")
        cases.append(f"SELECT a = b, a < b FROM VALUES ({va}, {vb}) AS t(a, b)")
    # values chosen so that the direction of the cast changes the answer
    for a, b in [("'1'", "1"), ("'1.0'", "1"), ("'01'", "1"), ("'abc'", "1"), ("1", "1.0D"), ("CAST(1 AS DECIMAL(3,1))", "'1.0'"),
                 ("DATE'2024-01-01'", "'2024-01-01'"), ("DATE'2024-01-01'", "'2024-1-1'"), ("TIMESTAMP'2024-01-01 00:00:00'", "'2024-01-01'"),
                 ("'2024-01-01'", "DATE'2024-01-01'"), ("true", "'true'"), ("true", "1"), ("true", "'1'"), ("CAST(0.1 AS FLOAT)", "0.1D"),
                 ("CAST('NaN' AS DOUBLE)", "CAST('NaN' AS DOUBLE)"), ("-0.0D", "0.0D"), ("9223372036854775807", "9223372036854775807.0D"),
                 ("'9223372036854775807'", "9223372036854775807"), ("'2'", "10"), ("'10'", "9")]:
        for op in ["=", "<", ">", "<=>"]:
            cases.append(f"SELECT {a} {op} {b}, {b} {op} {a}")
    write("c4_comparison", "C4 comparison, BETWEEN and IN over every pair of scalar types", cases)


CONTEXTS = [
    ("CASE WHEN", "CASE WHEN true THEN {a} ELSE {b} END"),
    ("CASE WHEN else", "CASE WHEN false THEN {a} ELSE {b} END"),
    ("coalesce", "coalesce({a}, {b})"),
    ("array", "array({a}, {b})"),
    ("IN", "{a} IN ({b}, {a})"),
]
EXTRA_CONTEXTS = [
    "if(true, {a}, {b})", "nvl({a}, {b})", "nvl2({a}, {a}, {b})", "ifnull({a}, {b})", "nullif({a}, {b})", "greatest({a}, {b})",
    "least({a}, {b})", "nanvl({a}, {b})", "array_contains(array({a}), {b})", "array_position(array({a}), {b})", "array_remove(array({a}), {b})",
    "array_append(array({a}), {b})", "array_union(array({a}), array({b}))", "array_intersect(array({a}), array({b}))",
    "array_except(array({a}), array({b}))", "arrays_overlap(array({a}), array({b}))", "concat(array({a}), array({b}))",
    "map({a}, {b})", "map_from_arrays(array({a}), array({b}))", "element_at(map({a}, 1), {b})", "struct({a}, {b})",
]


def c5_common_type():
    cases = []
    types = [t for t in TYPES if t.literal_ok]
    for a, b in itertools.product(types, types):
        va, vb = vals(a, 1)[0], vals(b, 1)[0]
        for _, ctx in CONTEXTS:
            e = ctx.format(a=va, b=vb)
            cases.append(f"SELECT typeof({e}), {e}")
        cases.append(f"SELECT typeof(v), v FROM (SELECT {va} AS v UNION ALL SELECT {vb} AS v) ORDER BY v")
        cases.append(f"SELECT typeof(v), v FROM (SELECT {va} AS v UNION SELECT {vb} AS v) ORDER BY v")
    # reduced set in the other contexts: one pair per pair of groups, plus STRING / DECIMAL / void
    reps = {g: ts[0] for g, ts in BY_GROUP.items()}
    reps["decimal2"] = next(t for t in TYPES if t.name == "DECIMAL(20,5)")
    pairs = list(itertools.combinations_with_replacement(reps.values(), 2))
    for t in (STRING_T, next(t for t in TYPES if t.name == "DECIMAL(5,2)")):
        pairs += [(t, o) for o in types]
    seen = set()
    for a, b in pairs:
        if (a.name, b.name) in seen or not (a.literal_ok and b.literal_ok):
            continue
        seen.add((a.name, b.name))
        va, vb = vals(a, 1)[0], vals(b, 1)[0]
        for ctx in EXTRA_CONTEXTS:
            e = ctx.format(a=va, b=vb)
            cases.append(f"SELECT typeof({e}), {e}")
    for a in types:
        cases.append(f"SELECT typeof(coalesce(NULL, {vals(a, 1)[0]})), coalesce(NULL, {vals(a, 1)[0]})")
        cases.append(f"SELECT typeof(array(NULL, {vals(a, 1)[0]})), array(NULL, {vals(a, 1)[0]})")
    # three or more branches: pairwise widening from the left is not associative
    trio = ["CAST(1 AS DECIMAL(5,2))", "'1'", "1", "1.5D", "DATE'2024-01-01'", "CAST(1 AS BIGINT)", "CAST(1 AS DECIMAL(20,5))", "true", "TIMESTAMP'2024-01-01 00:00:00'"]
    for x, y, z in itertools.permutations(trio, 3):
        cases.append(f"SELECT typeof(CASE WHEN true THEN {x} WHEN false THEN {y} ELSE {z} END)")
        cases.append(f"SELECT typeof(coalesce({x}, {y}, {z}))")
        cases.append(f"SELECT typeof(v) FROM (SELECT {x} AS v UNION ALL SELECT {y} UNION ALL SELECT {z}) LIMIT 1")
    write("c5_common_type", "C5 common type of every pair of types in CASE WHEN, coalesce, UNION [ALL], array, IN and the reduced contexts", cases)


GRID_P = [1, 5, 9, 10, 18, 19, 20, 28, 37, 38]


def grid():
    out = []
    for p in GRID_P:
        for s in sorted({0, 1, 2, 6, p // 2, p - 1, p}):
            if 0 <= s <= p:
                out.append((p, s))
    return out


def c8_decimal_precision():
    cases = []
    g = grid()
    small = [(1, 0), (5, 2), (10, 0), (10, 5), (18, 9), (20, 5), (28, 10), (38, 0), (38, 18), (38, 38), (37, 2), (19, 19)]
    ops = ["+", "-", "*", "/", "%", "div"]
    cells = []
    for (p1, s1) in g:
        for (p2, s2) in small:
            for op in ops:
                a, b = f"CAST(1 AS DECIMAL({p1},{s1}))", f"CAST(1 AS DECIMAL({p2},{s2}))"
                cells.append(f"typeof({a} {op} {b})")
    for i in range(0, len(cells), 40):
        cases.append("SELECT " + ", ".join(cells[i:i + 40]))
    ints = ["CAST(1 AS TINYINT)", "CAST(1 AS SMALLINT)", "CAST(1 AS INT)", "CAST(1 AS BIGINT)", "1.5D", "CAST(1.5 AS FLOAT)"] + [str(10 ** n) for n in range(0, 19)]
    cells = []
    for (p, s) in g:
        for i in ints:
            for op in ["+", "*", "/", "%"]:
                cells.append(f"typeof(CAST(1 AS DECIMAL({p},{s})) {op} {i})")
                cells.append(f"typeof({i} {op} CAST(1 AS DECIMAL({p},{s})))")
    for i in range(0, len(cells), 40):
        cases.append("SELECT " + ", ".join(cells[i:i + 40]))
    cells = []
    for (p, s) in g:
        d = f"CAST(1 AS DECIMAL({p},{s}))"
        for e in ["sum({d})", "avg({d})", "round({d})", "round({d}, 1)", "round({d}, -1)", "bround({d}, 1)", "floor({d})", "ceil({d})", "floor({d}, 1)",
                  "ceil({d}, 1)", "abs({d})", "-({d})", "{d} = 1", "array({d}, CAST(1 AS DECIMAL(10,2)))", "coalesce({d}, CAST(1 AS DECIMAL(3,3)))"]:
            cells.append("typeof(" + e.format(d=d) + ")")
    for i in range(0, len(cells), 40):
        cases.append("SELECT " + ", ".join(cells[i:i + 40]))
    # values at the overflow boundary
    for (p, s) in [(38, 0), (38, 18), (20, 5), (10, 2), (5, 2)]:
        mx = "9" * (p - s) + ("." + "9" * s if s else "")
        mn = "0." + "0" * (s - 1) + "1" if s else "1"
        for op in ["+", "-", "*", "/", "%", "div"]:
            cases.append(f"SELECT CAST({mx} AS DECIMAL({p},{s})) {op} CAST({mx} AS DECIMAL({p},{s}))")
            cases.append(f"SELECT CAST({mx} AS DECIMAL({p},{s})) {op} CAST({mn} AS DECIMAL({p},{s}))")
            cases.append(f"SELECT CAST({mn} AS DECIMAL({p},{s})) {op} CAST(3 AS DECIMAL(2,0))")
        cases.append(f"SELECT sum(x), avg(x) FROM VALUES (CAST({mx} AS DECIMAL({p},{s}))), (CAST({mx} AS DECIMAL({p},{s}))) AS t(x)")
    write("c8_decimal_precision", "C8 decimal result types and boundary values over the (p, s) grid", cases)


LITERALS = (["0", "1", "-1", "127", "128", "-128", "-129", "32767", "32768", "2147483647", "2147483648", "-2147483648", "-2147483649",
             "9223372036854775807", "9223372036854775808", "-9223372036854775808", "-9223372036854775809", "18446744073709551616",
             "99999999999999999999999999999999999999", "100000000000000000000000000000000000000",
             "1Y", "127Y", "128Y", "1S", "32767S", "32768S", "1L", "9223372036854775807L", "9223372036854775808L", "1BD", "1.5BD", "1D", "1.5D", "1F", "1.5F", "1e3", "1E-3", "1.5e3", "1e3D", "1e3F",
             ".5", "5.", "0.5", "00.5", "1.", "-.5", "+1", "--1", "- 1", "1.0", "1.00", "0.0", "0.00", "123456789.123456789", "1.2345678901234567890123456789012345678",
             "1.23456789012345678901234567890123456789", "12345678901234567890123456789012345678.9", "0x1F", "0xFF", "X'1F'", "X''", "X'GG'",
             "DATE'2024-02-29'", "DATE'2024-2-9'", "DATE'2024'", "DATE'2024-02-30'", "DATE'abc'", "TIMESTAMP'2024-02-29 10:30:00'",
             "TIMESTAMP'2024-02-29'", "TIMESTAMP'2024-02-29T10:30:00'", "TIMESTAMP'2024-02-29 10:30:00+05:30'", "TIMESTAMP'2024-02-29 10:30:00 UTC'", "TIMESTAMP'abc'",
             "TIMESTAMP_NTZ'2024-02-29 10:30:00'", "TIMESTAMP_LTZ'2024-02-29 10:30:00'", "TIMESTAMP_NTZ'2024-02-29 10:30:00+05:30'",
             "INTERVAL '1' DAY", "INTERVAL 1 DAY", "INTERVAL '1-2' YEAR TO MONTH", "INTERVAL '1 02:03:04' DAY TO SECOND", "INTERVAL 1 YEAR 2 MONTH",
             "INTERVAL '1' YEAR", "INTERVAL '5' HOUR", "INTERVAL '1 day'", "INTERVAL '1' SECOND", "INTERVAL '1.5' SECOND", "INTERVAL 1 HOUR 30 MINUTES",
             "INTERVAL -1 DAY", "INTERVAL '-1' DAY", "INTERVAL '1' WEEK", "INTERVAL 1 MILLISECOND", "interval '1 2:3' day to minute", "INTERVAL ''",
             "'abc'", "\"abc\"", "'a''b'", "'a\\'b'", "'\\n'", "'\\u0041'", "'\\101'", "''", "'%'", "true", "false", "TRUE", "NULL", "null", "CAST(NULL AS INT)",
             "1_000", "1e", "1e+3", "0b101", "1L2", "1BD1", "-0", "-0.0", "-0.0D", "0.0F", "1e400", "1e-400", "1e38F", "1e39F",
             "9223372036854775807 + 1", "-9223372036854775808 - 1", "2147483647 + 1", "2147483648 * 2", "1/0", "1.0/0", "-1 % 0"])


def c9_literals():
    cases = [f"SELECT typeof({lit}), {lit}" for lit in LITERALS]
    write("c9_literals", "C9 literal typing: integer / decimal / exponent / suffix / typed / interval / invalid literals", cases)


def c10_datetime():
    cases = []
    d, ts, ntz = "DATE'2024-03-10'", "TIMESTAMP'2024-03-10 10:30:00'", "TIMESTAMP_NTZ'2024-03-10 10:30:00'"
    ym, ds, m, h = "INTERVAL '1-2' YEAR TO MONTH", "INTERVAL '1 02:03:04' DAY TO SECOND", "INTERVAL '5' MONTH", "INTERVAL '5' HOUR"
    operands = [d, ts, ntz, ym, ds, m, h, "1", "CAST(1 AS BIGINT)", "1.5D", "CAST(1.5 AS DECIMAL(5,2))", "'1'", "'2024-03-10'", "'2024-03-10 10:30:00'", "'1 day'", "NULL", "CAST(NULL AS DATE)"]
    for a, b in itertools.product(operands, operands):
        for op in ["+", "-"]:
            cases.append(f"SELECT typeof({a} {op} {b}), {a} {op} {b}")
        for op in ["*", "/"]:
            if a in (ym, ds, m, h) or b in (ym, ds, m, h):
                cases.append(f"SELECT typeof({a} {op} {b}), {a} {op} {b}")
    for fn in ["date_add", "date_sub", "add_months", "dateadd", "date_diff", "datediff", "months_between", "last_day", "next_day"]:
        for a in [d, ts, "'2024-03-10'", "'2024-03-10 10:30:00'", "NULL"]:
            for b in ["1", "1.5D", "'1'", "'x'", "CAST(1 AS BIGINT)", "-1", "CAST(NULL AS INT)", "d2"] if fn not in ("last_day",) else [""]:
                if b == "d2":
                    b = d
                if fn == "last_day":
                    expr = f"{fn}({a})"
                elif fn == "next_day":
                    expr = f"{fn}({a}, 'Monday')"
                else:
                    expr = f"{fn}({a}, {b})"
                cases.append(f"SELECT typeof({expr}), {expr}")
    write("c10_datetime_arithmetic", "C10 datetime and interval arithmetic, string operands, date functions with string / fractional arguments", cases)


def c11_boolean_contexts():
    cases = []
    for t in TYPES:
        if not t.literal_ok:
            continue
        for v in t.values[:2]:
            cases.append(f"SELECT 1 AS r FROM VALUES (1) AS t(x) WHERE {v}")
            cases.append(f"SELECT CASE WHEN {v} THEN 'y' ELSE 'n' END")
            cases.append(f"SELECT if({v}, 'y', 'n')")
            cases.append(f"SELECT {v} AND true, {v} OR false")
            cases.append(f"SELECT NOT {v}")
            cases.append(f"SELECT x FROM VALUES (1) AS t(x) GROUP BY x HAVING {v}")
            cases.append(f"SELECT a.x FROM VALUES (1) AS a(x) JOIN VALUES (1) AS b(y) ON {v}")
            cases.append(f"SELECT filter(array(1, 2), x -> {v})")
            cases.append(f"SELECT count(*) FILTER (WHERE {v}) FROM VALUES (1) AS t(x)")
    write("c11_boolean_contexts", "C11 a value of every type as a condition: WHERE, HAVING, ON, CASE WHEN, IF, AND, OR, NOT, filter, FILTER", cases)


AGGS = ["sum({c})", "avg({c})", "min({c})", "max({c})", "count({c})", "first({c})", "last({c})", "collect_list({c})", "collect_set({c})", "approx_count_distinct({c})",
        "stddev({c})", "var_pop({c})", "count(DISTINCT {c})", "any_value({c})", "mode({c})", "median({c})", "percentile_approx({c}, 0.5)", "sum(DISTINCT {c})",
        "bit_and({c})", "bool_and({c})", "max_by({c}, {c})", "skewness({c})", "kurtosis({c})", "try_sum({c})", "try_avg({c})"]


def c12_aggregates():
    cases = []
    for t in TYPES:
        if not t.literal_ok:
            continue
        rows = [v for v in t.values[:3]]
        rowsets = {"rows": rows, "one": rows[:1], "nulls": [t.null, t.null]}
        for label, rs in rowsets.items():
            for agg in AGGS:
                e = agg.format(c="v")
                cases.append(f"SELECT typeof({e}), {e} FROM VALUES {values_rows(rs)} AS t(v)")
        for agg in AGGS[:8]:
            e = agg.format(c="v")
            cases.append(f"SELECT typeof({e}), {e} FROM (SELECT {t.null} AS v) WHERE false")
    for a, b in itertools.product(["INT", "STRING", "DECIMAL(5,2)", "DOUBLE", "DATE", "BIGINT"], repeat=2):
        cases.append(f"SELECT typeof(lag(v, 1, CAST(NULL AS {b})) OVER (ORDER BY o)), lag(v, 1, CAST(NULL AS {b})) OVER (ORDER BY o) FROM VALUES (CAST(NULL AS {a}), 1), (CAST(NULL AS {a}), 2) AS t(v, o)")
    ordkeys = [("INT", "1, 2, 3"), ("BIGINT", "1L, 2L, 3L"), ("DOUBLE", "1D, 2D, 3D"), ("DECIMAL(5,2)", "1.5BD, 2.5BD, 3.5BD"), ("DATE", "DATE'2024-01-01', DATE'2024-01-02', DATE'2024-01-03'"),
                ("TIMESTAMP", "TIMESTAMP'2024-01-01 00:00:00', TIMESTAMP'2024-01-02 00:00:00', TIMESTAMP'2024-01-03 00:00:00'"), ("STRING", "'a', 'b', 'c'")]
    for _, ks in ordkeys:
        rows = values_rows(ks.split(", "))
        for bound in ["1", "1L", "1.5D", "INTERVAL '1' DAY", "'1'", "CAST(1 AS DECIMAL(3,1))"]:
            cases.append(f"SELECT sum(1) OVER (ORDER BY k RANGE BETWEEN {bound} PRECEDING AND CURRENT ROW) FROM VALUES {rows} AS t(k)")
    write("c12_aggregates_windows", "C12 aggregates over every type (normal rows, empty input, all NULL), lag / lead defaults, RANGE frame bounds", cases)


def c13_nested():
    cases = []
    nested = [t for t in TYPES if t.group == "nested"]
    for a, b in itertools.product(nested, nested):
        va = vals(a, 1)[0]
        cases.append(f"SELECT typeof(CAST({va} AS {b.name})), CAST({va} AS {b.name})")
        cases.append(f"SELECT typeof(TRY_CAST({va} AS {b.name})), TRY_CAST({va} AS {b.name})")
        vb = vals(b, 1)[0]
        for _, ctx in CONTEXTS:
            e = ctx.format(a=va, b=vb)
            cases.append(f"SELECT typeof({e}), {e}")
    for src, tgt in [("array(1, 2)", "ARRAY<DOUBLE>"), ("array(1.5D, 2.5D)", "ARRAY<INT>"), ("array('1', 'x')", "ARRAY<INT>"), ("array(1, 2)", "ARRAY<STRING>"),
                     ("array(300, 2)", "ARRAY<TINYINT>"), ("map(1, 2)", "MAP<STRING,STRING>"), ("map(1.5D, 2)", "MAP<INT,INT>"), ("map('a', 1)", "MAP<INT,INT>"),
                     ("named_struct('a', 1, 'b', 2)", "STRUCT<x:STRING,y:STRING>"), ("named_struct('a', 1, 'b', 2)", "STRUCT<a:INT>"), ("named_struct('a', 1)", "STRUCT<a:INT,b:INT>"),
                     ("named_struct('a', 'x')", "STRUCT<a:INT>"), ("array(named_struct('a', 1))", "ARRAY<STRUCT<a:STRING>>"), ("array(array(1))", "ARRAY<ARRAY<STRING>>"),
                     ("map(1, array(1))", "MAP<STRING,ARRAY<DOUBLE>>"), ("array(1, NULL)", "ARRAY<INT>"), ("array()", "ARRAY<INT>"), ("map()", "MAP<INT,INT>"),
                     ("array(1, 2)", "ARRAY<ARRAY<INT>>"), ("array(array(1))", "ARRAY<INT>"), ("array(1)", "MAP<INT,INT>"), ("map(1, 2)", "ARRAY<INT>"), ("1", "ARRAY<INT>"), ("array(1)", "INT")]:
        cases.append(f"SELECT typeof(CAST({src} AS {tgt})), CAST({src} AS {tgt})")
        cases.append(f"SELECT typeof(TRY_CAST({src} AS {tgt})), TRY_CAST({src} AS {tgt})")
        cases.append(f"SELECT typeof(CAST(v AS {tgt})), CAST(v AS {tgt}) FROM VALUES ({src}), (NULL) AS t(v)")
    write("c13_nested", "C13 casts between nested types and the common-type contexts with nested operands", cases)


def c14_equality_ordering():
    cases = []
    keys = ["CAST(1 AS INT)", "CAST(1 AS BIGINT)", "1.0D", "CAST(1 AS DECIMAL(5,2))", "'1'", "'01'", "'1.0'", "true", "DATE'2024-01-01'", "'2024-01-01'", "TIMESTAMP'2024-01-01 00:00:00'",
            "CAST(1 AS TINYINT)", "CAST(1.0 AS FLOAT)", "X'31'"]
    for a, b in itertools.product(keys, keys):
        cases.append(f"SELECT typeof(a), a, b FROM VALUES ({a}) AS l(a) JOIN VALUES ({b}) AS r(b) ON a = b")
        cases.append(f"SELECT typeof(k), k FROM VALUES ({a}) AS l(k) JOIN VALUES ({b}) AS r(k) USING (k)")
        cases.append(f"SELECT * FROM VALUES ({a}) AS l(k) NATURAL JOIN VALUES ({b}) AS r(k)")
    floats = "(0.0D), (-0.0D), (CAST('NaN' AS DOUBLE)), (CAST('NaN' AS DOUBLE)), (CAST('Infinity' AS DOUBLE)), (NULL), (1.0D)"
    for e in ["SELECT DISTINCT v FROM VALUES {f} AS t(v) ORDER BY 1", "SELECT v, count(*) FROM VALUES {f} AS t(v) GROUP BY v ORDER BY 1",
              "SELECT v FROM VALUES {f} AS t(v) INTERSECT SELECT v FROM VALUES {f} AS t(v)", "SELECT v FROM VALUES {f} AS t(v) EXCEPT SELECT v FROM VALUES (0.0D) AS t(v)",
              "SELECT v FROM VALUES {f} AS t(v) UNION SELECT v FROM VALUES (CAST('NaN' AS DOUBLE)) AS t(v)", "SELECT v FROM VALUES {f} AS t(v) ORDER BY v DESC",
              "SELECT v FROM VALUES {f} AS t(v) ORDER BY v NULLS LAST", "SELECT a.v FROM VALUES {f} AS a(v) JOIN VALUES {f} AS b(v) ON a.v = b.v",
              "SELECT sort_array(array(0.0D, -0.0D, CAST('NaN' AS DOUBLE), NULL, 1.0D))", "SELECT array_distinct(array(0.0D, -0.0D, CAST('NaN' AS DOUBLE), CAST('NaN' AS DOUBLE)))",
              "SELECT max(v), min(v) FROM VALUES {f} AS t(v)", "SELECT v, row_number() OVER (ORDER BY v) FROM VALUES {f} AS t(v)"]:
        cases.append(e.format(f=floats))
    for t in TYPES:
        if not t.literal_ok:
            continue
        rows = values_rows(t.values[:4] + [t.null])
        cases.append(f"SELECT v FROM VALUES {rows} AS t(v) ORDER BY v")
        cases.append(f"SELECT v FROM VALUES {rows} AS t(v) ORDER BY v DESC NULLS FIRST")
        cases.append(f"SELECT v, count(*) FROM VALUES {rows}, {rows} AS t(v) GROUP BY v ORDER BY 1")
        cases.append(f"SELECT DISTINCT v FROM VALUES {rows} AS t(v) ORDER BY 1")
    for s in ["('b'), ('a'), ('B'), ('é'), ('日'), ('A'), (''), ('ab'), ('a b')", "(X'01'), (X'00'), (X'FF'), (X''), (X'0100')"]:
        cases.append(f"SELECT v FROM VALUES {s} AS t(v) ORDER BY v")
    write("c14_equality_ordering", "C14 join key coercion, -0.0 / NaN grouping and set operations, ordering of every type", cases)


def c7_store_assignment():
    cases, setup = [], []
    targets = [t for t in TYPES if t.group != "nested" or t.name in ("ARRAY<INT>", "MAP<STRING,INT>", "STRUCT<a:INT,b:STRING>")]
    sources = [t for t in TYPES if t.literal_ok]
    for i, tgt in enumerate(targets):
        setup.append(f"CREATE TABLE IF NOT EXISTS sa{i} (v {tgt.name}) USING parquet")
    for i, tgt in enumerate(targets):
        for src in sources:
            v = vals(src, 1)[0]
            cases.append(f"INSERT INTO sa{i} VALUES ({v})")
            cases.append(f"INSERT INTO sa{i} SELECT {v}")
        cases.append(f"SELECT typeof(v), v FROM sa{i} ORDER BY 2")
        cases.append(f"INSERT OVERWRITE sa{i} SELECT {vals(STRING_T, 1)[0]}")
        cases.append(f"CREATE TABLE sa_ctas{i} USING parquet AS SELECT {vals(tgt, 1)[0] if vals(tgt, 1) else 'NULL'} AS v")
        cases.append(f"INSERT INTO sa{i} (v) VALUES ({vals(tgt, 1)[0] if vals(tgt, 1) else 'NULL'})")
    header = "-- @matrix spark.sql.storeAssignmentPolicy=ANSI,LEGACY,STRICT"
    OUT.mkdir(parents=True, exist_ok=True)
    lines = ["-- @exact", header, "-- C7 store assignment: every value type into every column type under every policy.",
             "-- Generated by scripts/gen_type_cases.py; do not edit. Expectations come from Spark.", ""]
    for s in setup:
        lines += ["-- @setup", stmt(s)]
    for c in cases:
        lines.append(stmt(c))
    (OUT / "c7_store_assignment.sql").write_text("\n".join(lines))
    print(f"c7_store_assignment: {len(cases)} cases")


MATRICES = {
    "c1": c1_casts, "c2": c2_string_rendering, "c3": c3_arithmetic, "c4": c4_comparison, "c5": c5_common_type,
    "c7": c7_store_assignment, "c8": c8_decimal_precision, "c9": c9_literals, "c10": c10_datetime, "c11": c11_boolean_contexts,
    "c12": c12_aggregates, "c13": c13_nested, "c14": c14_equality_ordering,
}


def main():
    wanted = sys.argv[1:] or list(MATRICES)
    for k in wanted:
        MATRICES[k]()


if __name__ == "__main__":
    main()
