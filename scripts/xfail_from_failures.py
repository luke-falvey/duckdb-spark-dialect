"""Write test/xfail.toml entries for the cases that fail today, grouped by cause.

usage: python scripts/xfail_from_failures.py [--write] [--files GLOB ...]

Reads test/results/failures.txt and failures_spark_strict.txt (written by `run_duckdb.py` for the default and the
strict dialect), classifies every FAIL by the rules in RULES (first match wins: a regular expression over the SQL and
over the failure detail), and prints the entries (or, with --write, replaces the entries of the files in
test/xfail.toml that this run covered). A failure no rule matches gets the id `unclassified`: add a rule, or fix the
bug. Run the two dialects first:

    python test/run_duckdb.py [files]; python test/run_duckdb.py --dialect spark_strict [files]

The entries list case numbers per file (and configuration), so they stay valid while the generated file does not
change; regenerate a generated file and rerun this script.
"""
import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "test" / "results"
XFAIL = ROOT / "test" / "xfail.toml"

# (id, reason, regex over the SQL, regex over the detail); first match wins
RULES = [
    ("tz-window", "window() buckets by the UTC epoch in Spark (displayed in the session zone); the extension buckets the session wall clock",
     r"window\s*\(", r"rows differ"),
    ("tz-zone-names", "zone abbreviations (z) and long names (zzzz) of region zones need the time zone database; the extension prints the zone id",
     r"date_format\(|from_unixtime\(", r"rows differ"),
    ("phase-element-at-zero", "Spark raises the index-zero error at run time; the extension folds the constant at bind time",
     r"element_at\(", r"error phase"),
    ("phase-div-literal", "Spark raises division by a literal zero at analysis time; the extension at run time",
     r"\bdiv\s+0", r"error phase"),
    ("missing-function", "function missing in the dialect (the case only passed because both engines fail)",
     r"make_interval|raise_error", r"error phase"),
    ("phase-regex-literal", "Spark compiles regular expressions at run time; DuckDB at bind time for literal patterns",
     r"regexp|rlike", r"error phase"),
    ("phase-array-repeat", "Spark rejects the huge count at analysis time; the extension at run time",
     r"array_repeat", r"error phase"),
    ("phase-like-escape", "Spark rejects an invalid escape in a literal pattern at analysis time; the extension at run time",
     r"LIKE", r"error phase"),
    ("types-ceil-scale", "ceil / floor with a scale argument return DOUBLE; Spark returns a DECIMAL",
     r"\b(ceil|ceiling|floor)\s*\([^()]*,", r"column types differ"),
    ("types-to-number", "to_number / try_to_number return DECIMAL(38,10); Spark derives the type from the format",
     r"to_number", r"column types differ"),
    ("types-lambda-index", "list_transform passes a BIGINT index; Spark's lambda index is INT",
     r"\((x|a|e), i\) ->", r"column types differ"),
    ("types-struct-field-names", "a struct built in a lambda has no field names in DuckDB (Spark: the argument names)",
     r"zip_with", r"column types differ"),
    ("types-void", "an untyped NULL stays void in Spark (sum(void) is DOUBLE, concat(NULL, NULL) is STRING)",
     r"sum\(NULL\)|try_sum\(col\)|concat\(NULL, NULL\)|sum\(col\) FROM VALUES \(NULL\)", r"column types differ"),
    ("types-pivot", "PIVOT aggregates are HUGEINT in DuckDB; Spark's count / sum are BIGINT",
     r"PIVOT", r"column types differ"),
    ("types-percentile-disc", "percentile_disc keeps the input type in DuckDB; Spark returns DOUBLE",
     r"percentile_disc", r"column types differ"),
    ("types-interval", "DuckDB has one INTERVAL type: Spark's interval ranges are not representable (plan 9.4)", None,
     r"column types differ[\s\S]*interval"),
    ("types-interval", "date_part of an interval field: Spark's field type depends on the interval range (plan 9.4)",
     r"(date_part|datepart|extract).*INTERVAL", r"column types differ"),
    ("types-ntz", "TIMESTAMP_NTZ and TIMESTAMP are one DuckDB type (plan 9.3)", None,
     r"column types differ[\s\S]*timestamp_ntz"),
]


def _tiers():
    """{case file folded name: tier} from the settings inventory (semantic keys)."""
    import tomllib
    inv = tomllib.loads((ROOT / "test" / "settings" / "inventory.toml").read_text())["keys"]
    out = {}
    for key, e in inv.items():
        if e.get("class") == "semantic":
            out[Path(e["cases"]).relative_to("test/cases").as_posix().replace("/", "__")] = e["tier"]
    return out


TIERS = None


def classify(sql, detail, file=""):
    global TIERS
    if "UNSUPPORTED_FEATURE.SET_CONF" in detail:
        if TIERS is None:
            TIERS = _tiers()
        if TIERS.get(file, 3) >= 3:
            return ("conf-rejected", "a setting the dialect does not emulate is rejected by SET (plan 9.1); Spark accepts it")
        return ("conf-todo", "tier 1 / 2 setting not implemented yet (plan 9.2)")
    for ident, reason, sql_re, detail_re in RULES:
        if sql_re and not re.search(sql_re, sql, re.S | re.I):
            continue
        if detail_re and not re.search(detail_re, detail, re.S):
            continue
        return ident, reason
    if file == "settings__ansi_enabled.sql" or (file.startswith("types__") and "ansi" in file):
        return "ansi-implicit-cast", "ANSI mode changes Spark's implicit casts (AnsiTypeCoercion); the dialect keeps the non-ANSI rules"
    if file.startswith("settings__"):
        ident = "settings-" + file[len("settings__"):-len(".sql")]
        return ident, "the statement differs from Spark under this setting value (a missing feature or a bug, see the case)"
    return "unclassified", "no rule matches this failure: classify or fix it"


def parse(path, dialect):
    """{(file, config, index): (sql, detail)} of the FAIL blocks of a failures file."""
    out = {}
    if not path.exists():
        return out
    for block in path.read_text().split("\n\n"):
        m = re.match(r"FAIL (\S+):(\d+) ?(\S*)\n  sql: (.*?)\n  (.*)", block, re.S)
        if not m:
            continue
        file, index, config, sql, detail = m.group(1), int(m.group(2)), m.group(3), m.group(4), m.group(5)
        out[(file, config, index)] = (sql, detail)
    return out


def ranges(numbers):
    numbers = sorted(numbers)
    parts, start, prev = [], None, None
    for n in numbers:
        if start is None:
            start = prev = n
        elif n == prev + 1:
            prev = n
        else:
            parts.append((start, prev))
            start = prev = n
    if start is not None:
        parts.append((start, prev))
    return [a if a == b else f"{a}-{b}" for a, b in parts]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--files", nargs="*", help="only these case files (glob on the folded name)")
    args = ap.parse_args()
    spark = parse(RESULTS / "failures.txt", "spark")
    strict = parse(RESULTS / "failures_spark_strict.txt", "spark_strict") or parse(RESULTS / "failures_strict.txt", "x")
    # (id, reason, file, config, dialects) -> [indexes]
    groups = defaultdict(list)
    import fnmatch
    for key in sorted(set(spark) | set(strict)):
        file, config, index = key
        if args.files and not any(fnmatch.fnmatchcase(file, g.replace("/", "__")) for g in args.files):
            continue
        info = spark.get(key) or strict.get(key)
        ident, reason = classify(info[0], info[1], file)
        dialects = tuple(d for d, found in (("spark", key in spark), ("spark_strict", key in strict)) if found)
        groups[(ident, reason, file, config, dialects)].append(index)
    lines = []
    unclassified = 0
    for (ident, reason, file, config, dialects), indexes in sorted(groups.items()):
        unclassified += len(indexes) if ident == "unclassified" else 0
        lines.append("[[xfail]]")
        lines.append(f'id = "{ident}"')
        lines.append(f'reason = "{reason}"')
        lines.append(f'file = "{file}"')
        if config:
            lines.append(f'config = "{config}"')
        if len(dialects) == 1:
            lines.append(f'dialects = ["{dialects[0]}"]')
        lines.append("cases = [" + ", ".join(f'"{r}"' if isinstance(r, str) else str(r) for r in ranges(indexes)) + "]")
        lines.append("")
    text = "\n".join(lines)
    total = sum(len(v) for v in groups.values())
    print(f"{total} failing cases in {len(groups)} entries, {unclassified} unclassified", file=sys.stderr)
    if not args.write:
        print(text)
        return
    # keep the entries of files this run did not cover
    kept = []
    if XFAIL.exists():
        covered = {k[0] for k in groups} | {f for f in (args.files or []) if "*" not in f}
        block = []
        for line in XFAIL.read_text().splitlines():
            if line.startswith("[[xfail]]") and block:
                kept.append(block)
                block = []
            block.append(line)
        if block:
            kept.append(block)
        kept = [b for b in kept if not any(l.startswith("file = ") and l.split('"')[1] in covered for l in b)
                and any(l.startswith("[[xfail]]") for l in b)]
    header = ("# Expected failures (see docs/plans/spark-parity-testing.md section 5.5). Generated by\n"
              "# scripts/xfail_from_failures.py from the failures of test/run_duckdb.py; an entry that matches a case\n"
              "# which passes makes the run fail (XPASS), an entry that matches no failing case is stale.\n\n")
    XFAIL.write_text(header + "\n".join("\n".join(b).rstrip() + "\n" for b in kept) + ("\n" if kept else "") + text)
    print(f"wrote {XFAIL}", file=sys.stderr)


if __name__ == "__main__":
    main()
