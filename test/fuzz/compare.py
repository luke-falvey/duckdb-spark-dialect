"""Classify one query's results in Spark, `spark` and `spark_strict` (shared by the differential fuzzers).

Outcomes (finding kinds):
  pass             same rows, or both engines raised an error
  mismatch         both succeed, rows differ
  duck_error       Spark succeeds, DuckDB raises an ordinary error
  lenient          Spark rejects at parse / analysis time, `spark` succeeds (recorded, not a failure)
  lenient_runtime  Spark fails at run time, DuckDB returns rows (review: often a missing runtime check)
  strict_reject    Spark succeeds, `spark` succeeds, `spark_strict` raises an error
  strict_lenient   Spark rejects at parse / analysis time, `spark_strict` succeeds
  strict_mismatch  `spark` and `spark_strict` both succeed with different rows
  crash, timeout, internal
"""
import fnmatch
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import corpus  # noqa: E402,F401
from harness import results_equal, values_equal  # noqa: E402

FAILING = {"mismatch", "duck_error", "strict_reject", "strict_lenient", "strict_mismatch", "crash", "timeout",
           "internal"}
KNOWN_FILE = Path(__file__).resolve().parent / "known_differences.toml"

# Spark error messages that are run-time failures even though they surface from spark.sql()
# (constant folding evaluates expressions during analysis)
RUNTIME_MARKERS = ("Exception: ", "[ARITHMETIC_OVERFLOW]", "[DIVIDE_BY_ZERO]", "[CAST_INVALID_INPUT]",
                   "[INVALID_ARRAY_INDEX", "[INVALID_PARAMETER_VALUE", "[CANNOT_PARSE", "[INVALID_FRACTION",
                   "[DATETIME_", "[USER_RAISED_EXCEPTION]", "[INVALID_URL]", "[MALFORMED_", "[SQL_CONF_NOT_FOUND]",
                   "[CONVERSION_INVALID_INPUT]", "[ILLEGAL_DAY_OF_WEEK]", "[NUMERIC_VALUE_OUT_OF_RANGE]",
                   "[INVALID_FORMAT", "[COLLECTION_SIZE_LIMIT", "[INVALID_ARRAY_INDEX_IN_ELEMENT_AT]",
                   "[INVALID_PATTERN", "[INVALID_REGEX")


def spark_static_error(spark):
    """Spark rejected the query as invalid SQL (parse or analysis), not a run-time failure."""
    if spark.ok:
        return False
    err = spark.error or ""
    if spark.phase == "runtime":
        return False
    if err.startswith("An error occurred while calling") or "java.lang." in err:
        return False
    return not any(m in err for m in RUNTIME_MARKERS)


def describe_difference(expected, actual):
    if len(expected) != len(actual):
        return "row_count"
    for er, ar in zip(expected, actual):
        if len(er) != len(ar):
            return "column_count"
        for e, a in zip(er, ar):
            if values_equal(e, a):
                continue
            if e is None or a is None:
                return "null_vs_value"
            if e[0] != a[0]:
                return f"type:{e[0]}/{a[0]}"
            return f"value:{e[0]}"
    return "order"


def classify_one(spark, duck, ordered):
    """Outcome for one DuckDB dialect against Spark; returns (kind, difference)."""
    if duck.status in ("crash", "timeout", "internal"):
        return duck.status, None
    if spark.ok:
        if not duck.ok:
            return "duck_error", None
        if results_equal(spark.rows, duck.rows, ordered):
            return "pass", None
        return "mismatch", describe_difference(spark.rows, duck.rows)
    if not duck.ok:
        return "pass", None
    return ("lenient", None) if spark_static_error(spark) else ("lenient_runtime", None)


def classify(spark, lenient, strict, ordered):
    """Findings (kind, dialect, difference) for one query. `strict` may be None."""
    out = []
    k, diff = classify_one(spark, lenient, ordered)
    if k != "pass":
        out.append((k, "spark", diff))
    if strict is None:
        return out
    ks, diffs = classify_one(spark, strict, ordered)
    if ks in ("crash", "timeout", "internal"):
        out.append((ks, "spark_strict", None))
    elif ks == "duck_error":
        # only a strict-mode finding if the lenient dialect handled it; otherwise it is the same bug
        if k != "duck_error":
            out.append(("strict_reject", "spark_strict", None))
    elif ks == "lenient":
        out.append(("strict_lenient", "spark_strict", None))
    elif ks == "mismatch" and k != "mismatch":
        out.append(("mismatch", "spark_strict", diffs))
    if lenient.ok and strict.ok and not results_equal(lenient.rows, strict.rows, ordered) and k != "mismatch":
        out.append(("strict_mismatch", "spark_strict", describe_difference(lenient.rows, strict.rows)))
    return out


# ---- known differences ---------------------------------------------------------------------------

def load_known():
    if not KNOWN_FILE.exists():
        return []
    import tomllib
    return tomllib.loads(KNOWN_FILE.read_text()).get("difference", [])


def is_known(signature, known, sql=None):
    """The known difference that covers a finding: its `signature` glob matches, and its optional `sql`
    regular expression is found in the query."""
    import re
    for k in known:
        if not fnmatch.fnmatchcase(signature, k["signature"]):
            continue
        if k.get("sql") and not (sql and re.search(k["sql"], sql)):
            continue
        return k
    return None
