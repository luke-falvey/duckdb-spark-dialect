"""Shared helpers for the Spark-vs-DuckDB differential tests.

Case files (test/cases/*.sql) hold one statement per block; blocks are separated by blank lines.
Comment lines starting with `-- @` are directives for the block that follows:

  -- @setup          run in both engines before the checks, results not compared
  -- @nondeterministic  only check that both engines succeed (rand(), now(), ...)
  -- @ordered        compare rows in order (default: in order iff the query has a top-level ORDER BY)
  -- @unordered      compare rows as a multiset even with ORDER BY (ties in sort keys)
  -- @skip <reason>  do not run in DuckDB (documented gap)

File-level directive, in the leading comment lines of a case file (before the first statement):

  -- @config spark.sql.ansi.enabled=true   Spark conf for the whole file: make_golden.py sets it in Spark
                                           (and unsets it afterwards), run_duckdb.py runs `SET key = value`
                                           in the dialect before the file's cases. Repeat for several confs.
"""
import datetime
import decimal
import math
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent


class Case:
    def __init__(self, file, index, sql, directives):
        self.file = file
        self.index = index
        self.sql = sql
        self.directives = directives

    @property
    def id(self):
        return f"{self.file}:{self.index}"

    def has(self, name):
        return name in self.directives

    @property
    def ordered(self):
        if self.has("unordered"):
            return False
        if self.has("ordered"):
            return True
        # crude top-level ORDER BY detection: last ORDER BY outside parentheses
        depth = 0
        lowered = re.sub(r"'[^']*'", "''", self.sql.lower())
        found = False
        i = 0
        while i < len(lowered):
            c = lowered[i]
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            elif depth == 0 and lowered.startswith("order by", i):
                found = True
            i += 1
        return found


def file_config(path):
    """The `-- @config key=value` settings in the leading comment lines of a case file, in order."""
    conf = []
    for line in Path(path).read_text().splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if not stripped.startswith("--"):
            break
        m = re.match(r"--\s*@config\s+([\w.]+)\s*=\s*(.*)", stripped)
        if m:
            conf.append((m.group(1), m.group(2).strip()))
    return conf


def load_cases(paths):
    cases = []
    for path in paths:
        path = Path(path)
        text = path.read_text()
        blocks = re.split(r"\n\s*\n", text)
        index = 0
        for block in blocks:
            directives = {}
            sql_lines = []
            for line in block.strip().splitlines():
                stripped = line.strip()
                m = re.match(r"--\s*@(\w+)\s*(.*)", stripped)
                if m:
                    directives[m.group(1)] = m.group(2).strip()
                elif stripped.startswith("--") and not sql_lines:
                    continue
                else:
                    sql_lines.append(line)
            sql = "\n".join(sql_lines).strip().rstrip(";").strip()
            if not sql:
                continue
            index += 1
            cases.append(Case(path.name, index, sql, directives))
    return cases


FLOAT_REL = 1e-9
FLOAT_ABS = 1e-12


def normalize(value):
    """Map engine-specific Python values onto a common, comparable representation."""
    if value is None:
        return None
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, int):
        return ("num", decimal.Decimal(value))
    if isinstance(value, decimal.Decimal):
        if value.is_nan():
            return ("nan",)
        return ("num", value.normalize() if value != 0 else decimal.Decimal(0))
    if isinstance(value, float):
        if math.isnan(value):
            return ("nan",)
        if math.isinf(value):
            return ("inf", value > 0)
        return ("float", value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return ("bytes", bytes(value))
    if isinstance(value, str):
        return ("str", value)
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return ("ts", value)
    if isinstance(value, datetime.date):
        return ("date", value)
    if isinstance(value, datetime.time):
        return ("time", value)
    if isinstance(value, datetime.timedelta):
        return ("interval", value)
    if isinstance(value, dict):
        return ("map", sorted(((normalize(k), normalize(v)) for k, v in value.items()), key=repr))
    if hasattr(value, "asDict"):  # pyspark Row (struct)
        return ("struct", [normalize(v) for v in value])
    if isinstance(value, tuple):
        return ("struct", [normalize(v) for v in value])
    if isinstance(value, list):
        return ("list", [normalize(v) for v in value])
    return ("other", repr(value))


def normalize_typed(value, dtype):
    """Normalize a DuckDB value using its column type (DuckDB returns both STRUCT and MAP as dict)."""
    if value is None or dtype is None:
        return normalize(value)
    kind = getattr(dtype, "id", None)
    if kind not in ("struct", "map", "list", "array"):
        return normalize(value)
    children = dtype.children
    if kind == "struct" and isinstance(value, dict):
        return ("struct", [normalize_typed(value.get(name), t) for name, t in children])
    if kind == "map" and isinstance(value, dict):
        kt, vt = children[0][1], children[1][1]
        return ("map", sorted(((normalize_typed(k, kt), normalize_typed(v, vt)) for k, v in value.items()), key=repr))
    if kind in ("list", "array") and isinstance(value, (list, tuple)):
        ct = children[0][1] if children else None
        return ("list", [normalize_typed(v, ct) for v in value])
    return normalize(value)


def values_equal(a, b):
    if a == b:
        return True
    if a is None or b is None:
        return False
    # numbers: int / decimal / float compare numerically
    numeric = ("num", "float")
    # PySpark returns year-month intervals as a number of months; DuckDB's client as 30-day months
    if a[0] == "num" and b[0] == "interval":
        return b[1] == datetime.timedelta(days=30 * int(a[1])) and a[1] == int(a[1])
    if b[0] == "num" and a[0] == "interval":
        return values_equal(b, a)
    if a[0] in numeric and b[0] in numeric:
        fa, fb = float(a[1]), float(b[1])
        if fa == fb:
            return True
        # relative tolerance, plus an absolute one for results that should be zero but carry rounding noise
        # from a different summation order (e.g. covar_pop of values that cancel: 0.0 vs 1.1e-16)
        return abs(fa - fb) <= FLOAT_REL * max(abs(fa), abs(fb)) or abs(fa - fb) <= FLOAT_ABS
    if a[0] != b[0]:
        return False
    if a[0] in ("list", "struct"):
        return len(a[1]) == len(b[1]) and all(values_equal(x, y) for x, y in zip(a[1], b[1]))
    if a[0] == "map":
        return len(a[1]) == len(b[1]) and all(
            values_equal(ka, kb) and values_equal(va, vb) for (ka, va), (kb, vb) in zip(a[1], b[1]))
    return False


def rows_equal(ra, rb):
    return len(ra) == len(rb) and all(values_equal(x, y) for x, y in zip(ra, rb))


def sort_key(row):
    return repr([("~", repr(v)) if v is not None else ("", "") for v in row])


def results_equal(expected, actual, ordered):
    if len(expected) != len(actual):
        return False
    if not ordered:
        expected = sorted(expected, key=sort_key)
        actual = sorted(actual, key=sort_key)
        if all(rows_equal(a, b) for a, b in zip(expected, actual)):
            return True
        # float rounding can change the sort order; fall back to greedy matching
        remaining = list(actual)
        for row in expected:
            for i, cand in enumerate(remaining):
                if rows_equal(row, cand):
                    del remaining[i]
                    break
            else:
                return False
        return True
    return all(rows_equal(a, b) for a, b in zip(expected, actual))
