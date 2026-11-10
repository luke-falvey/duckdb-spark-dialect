"""Shared helpers for the Spark-vs-DuckDB differential tests.

Case files (test/cases/*.sql) hold one statement per block; blocks are separated by blank lines.
Comment lines starting with `-- @` are directives for the block that follows:

  -- @setup          run in both engines before the checks, results not compared
  -- @nondeterministic  only check that both engines succeed (rand(), now(), ...)
  -- @ordered        compare rows in order (default: in order iff the query has a top-level ORDER BY)
  -- @unordered      compare rows as a multiset even with ORDER BY (ties in sort keys)
  -- @skip <reason>  do not run in DuckDB (documented gap)
  -- @set key=value  (repeatable) set before the case in both engines and restore afterwards; in DuckDB this is
                     `SET key = value` / `RESET key`, so the case also tests the dialect's RESET
  -- @xfail <id> <reason>  the case is expected to fail (XFAIL); if it passes it is XPASS, which fails the run

File-level directive, in the leading comment lines of a case file (before the first statement):

  -- @config spark.sql.ansi.enabled=true   Spark conf for the whole file: make_golden.py sets it in Spark
                                           (and unsets it afterwards), run_duckdb.py runs `SET key = value`
                                           in the dialect before the file's cases. Repeat for several confs.
  -- @matrix key=v1,v2[,v3]                the file is recorded and compared once per combination of the values
                                           of all `@matrix` lines (repeatable; the product), on top of `@config`
  -- @exact                                floats compare bit for bit (-0.0 differs from 0.0, NaN equals NaN) and
                                           decimals compare exactly; used by the generated type matrices

Besides the rows, a case compares the column types (Spark's `simpleString` against DuckDB's type mapped by
`duck_type_to_spark`), the number of columns and the names of aliased / plain columns, and for errors the phase
(analysis = DuckDB parse / bind, runtime = DuckDB execution).
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
    def sets(self):
        return list(getattr(self, "set_list", []))

    @property
    def xfail(self):
        """(id, reason) from `-- @xfail <id> <reason>`, or None."""
        if "xfail" not in self.directives:
            return None
        parts = self.directives["xfail"].split(None, 1)
        return (parts[0] if parts else "xfail", parts[1] if len(parts) > 1 else "")

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


def file_directives(path):
    """`@config` (list of (key, value)), `@matrix` (list of (key, [values])) and `@exact` (bool) from the leading
    comment lines of a case file."""
    conf, matrix, exact = [], [], False
    for line in Path(path).read_text().splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if not stripped.startswith("--"):
            break
        m = re.match(r"--\s*@matrix\s+([\w.]+)\s*=\s*(.*)", stripped)
        if m:
            matrix.append((m.group(1), [v.strip() for v in m.group(2).split(",") if v.strip() != ""]))
        elif re.match(r"--\s*@exact\b", stripped):
            exact = True
    return {"config": file_config(path), "matrix": matrix, "exact": exact}


def matrix_combinations(matrix):
    """The product of all `@matrix` lines: a list of tuples of (key, value); [()] when there is no matrix."""
    combos = [()]
    for key, values in matrix:
        combos = [c + ((key, v),) for c in combos for v in values]
    return combos


def config_slug(pairs):
    """A short file-name-safe name of a configuration (a sequence of (key, value))."""
    if not pairs:
        return ""
    return "_".join(re.sub(r"[^A-Za-z0-9_.=+-]", "-", f"{k.replace('spark.sql.', '')}={v}") for k, v in pairs)


def case_file_name(path):
    """Name of a case file under test/cases, with directories folded in (`types/c1.sql` -> `types__c1.sql`)."""
    path = Path(path).resolve()
    try:
        rel = path.relative_to(ROOT / "cases")
    except ValueError:
        return path.name
    return "__".join(rel.parts)


def load_cases(paths):
    cases = []
    for path in paths:
        path = Path(path)
        text = path.read_text()
        blocks = re.split(r"\n\s*\n", text)
        index = 0
        for block in blocks:
            directives = {}
            set_list = []
            sql_lines = []
            for line in block.strip().splitlines():
                stripped = line.strip()
                m = re.match(r"--\s*@(\w+)\s*(.*)", stripped)
                if m:
                    if m.group(1) == "set":
                        kv = re.match(r"([\w.]+)\s*=\s*(.*)", m.group(2).strip())
                        if kv:
                            set_list.append((kv.group(1), kv.group(2).strip()))
                    else:
                        directives[m.group(1)] = m.group(2).strip()
                elif stripped.startswith("--") and not sql_lines:
                    continue
                else:
                    sql_lines.append(line)
            sql = "\n".join(sql_lines).strip().rstrip(";").strip()
            if not sql:
                continue
            index += 1
            case = Case(case_file_name(path), index, sql, directives)
            case.set_list = set_list
            cases.append(case)
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


EXACT = False  # set by the runners for `@exact` files; see exact_mode()


class exact_mode:
    """Context manager: compare floats bit for bit and decimals exactly inside the block."""

    def __init__(self, on=True):
        self.on = on

    def __enter__(self):
        global EXACT
        self.previous, EXACT = EXACT, self.on

    def __exit__(self, *exc):
        global EXACT
        EXACT = self.previous


def _exact_equal(a, b):
    """`@exact` comparison of two scalar tags: floats by bit pattern (NaN equals NaN), numbers by exact value."""
    if a[0] == "float" or b[0] == "float":
        if a[0] != b[0]:
            return False
        return a[1] == b[1] and math.copysign(1.0, a[1]) == math.copysign(1.0, b[1])
    if a[0] == "num" and b[0] == "num":
        return a[1] == b[1]
    return None


def values_equal(a, b):
    if EXACT and a is not None and b is not None and a[0] in ("num", "float") and b[0] in ("num", "float"):
        return bool(_exact_equal(a, b))
    if a == b:
        return True
    if a is None or b is None:
        return False
    # numbers: int / decimal / float compare numerically
    numeric = ("num", "float")
    if EXACT and a[0] == "float" and b[0] == "float":
        return _exact_equal(a, b)
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


# ----------------------------------------------------------------------------------------------
# Column types

def duck_type_to_spark(dtype):
    """Spark's `DataType.simpleString()` for a DuckDB type object (`duckdb.DuckDBPyType`).

    The mapping is the one the extension documents (see `SparkTypeName` in src/spark_functions.cpp). Rules:
      * `CHAR(n)` / `VARCHAR(n)` columns are `VARCHAR` in DuckDB and `string` in Spark query results.
      * An untyped NULL is `void`.
      * DuckDB `TIMESTAMP` is `timestamp`, `TIMESTAMP WITH TIME ZONE` is `timestamp` too, `INTERVAL` is `interval`:
        the representation cannot tell `timestamp_ntz` or the interval ranges apart. These are NOT widened to match
        either: a case whose Spark type is `timestamp_ntz` or `interval day to second` fails on its type and carries an
        `@xfail types-ntz` / `@xfail types-interval` entry until plan sections 9.3 and 9.4 are done.
      * `HUGEINT` stays `hugeint` (no Spark type); the extension returns `BIGINT` for `sum` of integers.
      * `UNION` and other DuckDB-only types keep their DuckDB name in lower case.
    """
    kind = str(getattr(dtype, "id", dtype)).lower()
    simple = {"boolean": "boolean", "tinyint": "tinyint", "smallint": "smallint", "integer": "int",
              "bigint": "bigint", "float": "float", "double": "double", "varchar": "string", "blob": "binary",
              "date": "date", "timestamp": "timestamp", "timestamp with time zone": "timestamp",
              "interval": "interval", "null": "void", "hugeint": "hugeint", "uhugeint": "uhugeint",
              "utinyint": "utinyint", "usmallint": "usmallint", "uinteger": "uinteger", "ubigint": "ubigint",
              "time": "time", "uuid": "uuid"}
    if kind in simple:
        return simple[kind]
    children = []
    try:
        children = list(dtype.children)
    except Exception:  # noqa: BLE001
        pass
    if kind == "decimal":
        params = dict(children)
        return f"decimal({params.get('precision')},{params.get('scale')})"
    if kind in ("list", "array"):
        return f"array<{duck_type_to_spark(children[0][1])}>"
    if kind == "map":
        return f"map<{duck_type_to_spark(children[0][1])},{duck_type_to_spark(children[1][1])}>"
    if kind == "struct":
        return "struct<" + ",".join(f"{name}:{duck_type_to_spark(t)}" for name, t in children) + ">"
    return kind


def normalize_spark_type(text):
    """A Spark type string with `char(n)` / `varchar(n)` folded to `string` (Spark reports `string` in results)."""
    return re.sub(r"\b(?:var)?char\(\d+\)", "string", text)


def types_equal(spark_types, duck_types):
    """Column types of Spark (simpleString list) and DuckDB (already mapped by duck_type_to_spark)."""
    if spark_types is None or duck_types is None:
        return True
    return [normalize_spark_type(t) for t in spark_types] == [normalize_spark_type(t) for t in duck_types]


# ----------------------------------------------------------------------------------------------
# Column names

PLAIN_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def gating_names(spark_names, sql=""):
    """Indexes of the columns whose name is compared strictly: explicit aliases (`AS name`) and plain column
    references (`SELECT a, b FROM ...`). A name that Spark generates from an expression (`(1 + 1)`, `count(1)`, `1`, and
    `x` for `CAST(x AS INT)`) is report-only. Detected from the statement text, which is approximate on purpose: a
    name counts only if it is a plain identifier that follows `AS` or stands alone between `SELECT` / `,` and `,` /
    `FROM` / the end."""
    flat = re.sub(r"'(?:[^']|'')*'", "''", sql)
    gating = []
    for i, name in enumerate(spark_names):
        if not PLAIN_NAME.match(name) or spark_names.count(name) > 1:
            # duplicates cannot all be aliases or references (Spark names `CAST(ts AS STRING)` as `ts`)
            continue
        quoted = re.escape(name)
        alias = re.search(rf"\bAS\s+`?{quoted}`?(?![A-Za-z0-9_])", flat, re.I)
        bare = re.search(rf"(?:\bSELECT\s+(?:DISTINCT\s+)?|,\s*)(?:\w+\.)?`?{quoted}`?\s*(?:,|\bFROM\b|$)", flat, re.I)
        if alias or bare:
            gating.append(i)
    return gating


def names_status(spark_names, duck_names, sql=""):
    """('ok', None) / ('count', detail) / ('gating', detail) / ('report', detail)."""
    if spark_names is None or duck_names is None:
        return ("ok", None)
    if len(spark_names) != len(duck_names):
        return ("count", f"{len(spark_names)} columns in Spark, {len(duck_names)} in DuckDB")
    gating = set(gating_names(spark_names, sql))
    bad_gating = [(i, spark_names[i], duck_names[i]) for i in gating if spark_names[i] != duck_names[i]]
    if bad_gating:
        return ("gating", f"column names differ: " + ", ".join(f"{s!r} vs {d!r}" for _, s, d in bad_gating))
    bad = [(spark_names[i], duck_names[i]) for i in range(len(spark_names)) if spark_names[i] != duck_names[i]]
    if bad:
        return ("report", "generated names differ: " + ", ".join(f"{s!r} vs {d!r}" for s, d in bad[:3]))
    return ("ok", None)


# ----------------------------------------------------------------------------------------------
# Judging one case

GATED_ERROR_CLASSES = (
    "DATATYPE_MISMATCH", "CAST_INVALID_INPUT", "CAST_OVERFLOW", "CANNOT_CAST_DATATYPE", "ARITHMETIC_OVERFLOW",
    "DIVIDE_BY_ZERO", "INTERVAL_DIVIDED_BY_ZERO", "NUMERIC_VALUE_OUT_OF_RANGE", "INVALID_ARRAY_INDEX",
    "INVALID_ARRAY_INDEX_IN_ELEMENT_AT", "DUPLICATED_MAP_KEY", "NULL_MAP_KEY", "INCOMPATIBLE_DATA_FOR_TABLE",
    "INVALID_CONF_VALUE", "CANNOT_MODIFY_CONFIG", "WRONG_NUM_ARGS", "UNRESOLVED_COLUMN", "UNRESOLVED_ROUTINE")

COMMANDS = ("INSERT", "CREATE", "DROP", "ALTER", "TRUNCATE", "USE", "CACHE", "UNCACHE", "REFRESH", "DELETE",
            "UPDATE", "MERGE", "CLEAR", "ADD", "MSCK", "ANALYZE", "SET", "RESET")


SCHEMA_EXEMPT = ("SHOW", "DESCRIBE", "DESC", "EXPLAIN")


def is_schema_exempt(sql):
    """Statements whose output schema is an engine detail (the catalog commands): rows are compared, the column
    types and names are not."""
    words = sql.lstrip("( ").split(None, 1)
    return bool(words) and words[0].upper() in SCHEMA_EXEMPT


def is_command(sql):
    words = sql.lstrip("( ").split(None, 1)
    return bool(words) and words[0].upper() in COMMANDS


def error_class_matches(spark_class, duck_class):
    """Spark's error class against the extension's: equal, or equal up to the first `.` when the extension does
    not emit the sub-class."""
    if not spark_class or not duck_class:
        return False
    if spark_class == duck_class:
        return True
    if "." in duck_class:
        return False
    return spark_class.split(".")[0] == duck_class


def class_is_gated(spark_class, relative_file):
    """Plan 9.6: gating in spark_strict for these classes in files under types/ and settings/."""
    if not spark_class or not (relative_file.startswith("types__") or relative_file.startswith("settings__")):
        return False
    return spark_class.split(".")[0] in GATED_ERROR_CLASSES


class Outcome:
    """kind: PASS, LENIENT, PHASE, FAIL (never XFAIL / XPASS: those are applied by the runner).
    notes: report-only observations (`names`, `class`)."""

    def __init__(self, kind, detail="", notes=None):
        self.kind = kind
        self.detail = detail
        self.notes = notes or {}

    def __iter__(self):
        yield self.kind
        yield self.detail

    def __repr__(self):
        return f"Outcome({self.kind!r}, {self.detail!r})"


def zone_from_spark(zone_id):
    """A tzinfo for a Spark session time zone id (region ids, `Z`, `+h`, `+hh:mm`, `UTC+05:30`, short ids), or None for
    UTC / an id this helper does not know."""
    import datetime as dt
    import re as _re
    import zoneinfo
    if not zone_id or zone_id in ("UTC", "Z", "GMT", "UT"):
        return None
    short = {"EST": "-05:00", "MST": "-07:00", "HST": "-10:00", "PST": "America/Los_Angeles", "IST": "Asia/Kolkata",
             "JST": "Asia/Tokyo", "CST": "America/Chicago", "AET": "Australia/Sydney"}
    zone_id = short.get(zone_id, zone_id)
    offset = _re.sub(r"^(UTC|GMT|UT)(?=[+-])", "", zone_id)
    m = _re.fullmatch(r"([+-])(\d{1,2})(?::?(\d{2}))?(?::?(\d{2}))?", offset)
    if m:
        seconds = int(m.group(2)) * 3600 + int(m.group(3) or 0) * 60 + int(m.group(4) or 0)
        return dt.timezone(dt.timedelta(seconds=-seconds if m.group(1) == "-" else seconds))
    try:
        return zoneinfo.ZoneInfo(zone_id)
    except Exception:  # noqa: BLE001
        return None


def instants(rows, spark_types, zone):
    """DuckDB returns a Spark TIMESTAMP as the wall clock time in the session zone (one naive TIMESTAMP type);
    PySpark returns the instant as a naive datetime in the process time zone (UTC in the tests). For the columns whose
    Spark type is `timestamp` convert the wall clock time to the instant, so that equal instants compare equal."""
    if zone is None or not spark_types:
        return rows
    import datetime as dt
    columns = [i for i, t in enumerate(spark_types) if t == "timestamp"]
    if not columns:
        return rows
    out = []
    for row in rows:
        row = list(row)
        for i in columns:
            v = row[i]
            if v is not None and v[0] == "ts":
                wall = v[1].replace(tzinfo=zone)
                try:
                    row[i] = ("ts", wall.astimezone(dt.timezone.utc).replace(tzinfo=None))
                except OverflowError:
                    pass  # an instant outside Python's range (year 1 / 9999 in a zone with an offset): left as the wall clock
        out.append(row)
    return out


def judge(sql, ordered, directives, expected, result, dialect="spark", relative_file="", exact=False, session_zone=None):
    """Compare Spark's recorded result with one DuckDB result.

    expected: golden entry (ok, rows, types, names, error, phase, error_class).
    result:   engines.Result of the dialect run (status, rows, error, extra{types? spark_types, names, stage,
              error_class}).
    Outcomes: FAIL for crashes / internal errors, an error where Spark succeeds, different rows, column types,
    column count or aliased column names; for a Spark error: PASS ("both error") when the phase matches, PHASE
    (FAIL in spark_strict) when it does not, LENIENT (FAIL in spark_strict) when DuckDB succeeds.
    The error class is a note, and FAIL only for the gated classes in spark_strict (class_is_gated)."""
    strict = dialect == "spark_strict"
    notes = {}
    duck_error = None if result.status == "ok" else result.error
    if result.status in ("crash", "timeout") or (duck_error and "INTERNAL" in duck_error):
        return Outcome("FAIL", f"duckdb internal error: ({result.status}) {duck_error}")
    extra = result.extra or {}
    if not expected["ok"]:
        if duck_error is None:
            if strict:
                return Outcome("FAIL", f"LENIENT in strict mode; spark error: {expected['error'][:120]}")
            return Outcome("LENIENT", f"spark error: {expected['error'][:120]}")
        spark_phase = expected.get("phase")
        duck_phase = {"bind": "analysis", "exec": "runtime"}.get(extra.get("stage"))
        spark_class = expected.get("error_class")
        duck_class = extra.get("error_class")
        notes["class"] = error_class_matches(spark_class, duck_class)
        if spark_phase and duck_phase and spark_phase != duck_phase:
            detail = f"error phase: spark {spark_phase}, duckdb {duck_phase} ({extra.get('exc')}); {duck_error[:100]}"
            return Outcome("FAIL" if strict else "PHASE", detail, notes)
        if strict and not notes["class"] and class_is_gated(spark_class, relative_file):
            return Outcome("FAIL", f"error class: spark {spark_class}, duckdb {duck_class}; {duck_error[:100]}", notes)
        return Outcome("PASS", "both error", notes)
    if duck_error is not None:
        return Outcome("FAIL", f"duckdb error: {duck_error}")
    command = is_command(sql)
    if command and expected["rows"] == []:
        # DDL / DML: Spark returns no rows, DuckDB a row count; the data is checked by later queries
        return Outcome("PASS", "")
    if "nondeterministic" not in directives:
        actual_rows = instants(result.rows, expected.get("types"), zone_from_spark(session_zone))
        with exact_mode(exact):
            same = results_equal(expected["rows"], actual_rows, ordered)
        if not same:
            return Outcome("FAIL", f"rows differ\n      spark:  {expected['rows'][:5]}\n      duckdb: {result.rows[:5]}")
    if not command and not is_schema_exempt(sql):
        duck_types = extra.get("spark_types")
        if not types_equal(expected.get("types"), duck_types):
            return Outcome("FAIL", f"column types differ\n      spark:  {expected.get('types')}\n"
                                   f"      duckdb: {duck_types}")
        status, detail = names_status(expected.get("names"), extra.get("names"), sql)
        notes["names"] = status
        if status in ("count", "gating"):
            return Outcome("FAIL", detail, notes)
    return Outcome("PASS", "", notes)


# ----------------------------------------------------------------------------------------------
# Golden files

import gzip  # noqa: E402
import pickle  # noqa: E402


def golden_dir(extra=()):
    d = ROOT / "golden"
    slug = config_slug(extra)
    return d / slug if slug else d


def golden_candidates(path, extra=()):
    name = case_file_name(path)
    d = golden_dir(extra)
    return [d / (name + ".pkl"), d / (name + ".pkl.gz")]


def golden_exists(path, extra=()):
    return any(p.exists() for p in golden_candidates(path, extra))


def load_golden(path, extra=()):
    for p in golden_candidates(path, extra):
        if p.exists():
            data = p.read_bytes()
            return pickle.loads(gzip.decompress(data) if p.suffix == ".gz" else data)
    raise FileNotFoundError(f"no golden file for {case_file_name(path)} {config_slug(extra)}: run make_golden.py")


def uses_gz(path):
    """Generated files (types/, settings/, spark_corpus/) are stored compressed."""
    name = case_file_name(path)
    return name.startswith(("types__", "settings__", "spark_corpus__"))


def save_golden(path, extra, golden):
    name = case_file_name(path)
    d = golden_dir(extra)
    d.mkdir(parents=True, exist_ok=True)
    for p in golden_candidates(path, extra):
        p.unlink(missing_ok=True)
    if uses_gz(path):
        (d / (name + ".pkl.gz")).write_bytes(gzip.compress(pickle.dumps(golden), 6, mtime=0))
    else:
        (d / (name + ".pkl")).write_bytes(pickle.dumps(golden))


# ----------------------------------------------------------------------------------------------
# Expected failures (test/xfail.toml)

import fnmatch  # noqa: E402
import tomllib  # noqa: E402

XFAIL_FILE = ROOT / "xfail.toml"


RUN_COMBOS = set()  # (case file name, configuration slug) pairs that run in this process, for the stale-entry check


class XfailEntry:
    """An entry of test/xfail.toml: `id`, `reason`, `file` (glob on the case file name, `types/*.sql` style paths
    are written with `__`), optional `config` (glob on the configuration slug; "" = default configuration),
    `dialects` (default both), and which cases: `sql` (regular expression), `cases` (indexes or "a-b" ranges)."""

    def __init__(self, raw):
        self.id = raw["id"]
        self.reason = raw.get("reason", "")
        self.file = raw["file"].replace("/", "__")
        self.config = raw.get("config")
        self.dialects = raw.get("dialects", ["spark", "spark_strict"])
        self.sql = re.compile(raw["sql"], re.S) if raw.get("sql") else None
        self.cases = None
        if raw.get("cases") is not None:
            self.cases = set()
            for c in raw["cases"]:
                if isinstance(c, int):
                    self.cases.add(c)
                else:
                    lo, _, hi = str(c).partition("-")
                    self.cases.update(range(int(lo), int(hi or lo) + 1))
        self.hits = 0

    def applies_to_file(self, name, slug):
        if not fnmatch.fnmatchcase(name, self.file):
            return False
        return self.config is None or fnmatch.fnmatchcase(slug, self.config)

    def matches(self, case, slug, dialect):
        if dialect not in self.dialects or not self.applies_to_file(case.file, slug):
            return False
        if self.cases is not None and case.index not in self.cases:
            return False
        return self.sql is None or bool(self.sql.search(case.sql))


def load_xfail(path=XFAIL_FILE):
    if not Path(path).exists():
        return []
    return [XfailEntry(e) for e in tomllib.loads(Path(path).read_text()).get("xfail", [])]


def apply_xfail(case, slug, dialect, outcome, entries):
    """XFAIL / XPASS for a case marked with `-- @xfail` or matched by an entry; the outcome otherwise.
    Only FAIL counts as failing: a marked case with another outcome is XPASS (the marker is stale)."""
    marker = case.xfail
    ident = marker[0] if marker else None
    reason = marker[1] if marker else ""
    for entry in entries:
        if entry.matches(case, slug, dialect):
            if ident is None:
                ident, reason = entry.id, entry.reason
            entry.hits += 1
            if outcome.kind == "FAIL":
                entry.fails = getattr(entry, "fails", 0) + 1
            break
    if ident is None:
        return outcome
    if outcome.kind == "FAIL":
        return Outcome("XFAIL", f"[{ident}] {reason}; {outcome.detail}", dict(outcome.notes, xfail=ident))
    return Outcome("XPASS", f"[{ident}] expected to fail but {outcome.kind}", dict(outcome.notes, xfail=ident))
