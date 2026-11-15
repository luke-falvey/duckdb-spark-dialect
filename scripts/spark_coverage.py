"""Which parts of Spark SQL 3.5 do the case files exercise? Measured by Spark's own parser and analyzer.

usage: python scripts/spark_coverage.py [--out FILE] [case files...]     (default: every file under test/cases/)

Compiles test/coverage/SparkCoverage.java against the jars of the installed pyspark (needs a JDK), runs it over
every statement of the case files and writes a JSON report (default docs/plans/spark-parity/coverage_baseline.json):
for each universe (grammar alternatives, statement kinds, keywords, Catalyst expression classes, logical plan
classes, named error classes) its size, how many items some statement reaches, and the items none reaches.

It measures the statements, not their outcome in the dialect: an item counts as reached even if every case that
reaches it is an expected failure. test/coverage/check_ledger.py (plan, workstream L) adds that condition.
"""
import argparse
import json
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "test"))
from harness import load_cases  # noqa: E402

JAVA_OPENS = [f"--add-opens=java.base/{p}=ALL-UNNAMED" for p in (
    "java.lang", "java.lang.invoke", "java.nio", "java.net", "java.io", "java.util", "java.util.concurrent",
    "sun.nio.ch", "sun.util.calendar", "sun.security.action")]


def read(path):
    items = {}
    for line in Path(path).read_text().splitlines():
        if line and not line.startswith("#"):
            key, _, value = line.partition("\t")
            items[key] = value
    return items


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(ROOT / "docs" / "plans" / "spark-parity" / "coverage_baseline.json"))
    ap.add_argument("files", nargs="*")
    args = ap.parse_args()
    import pyspark
    jars = Path(pyspark.__file__).parent / "jars"
    classpath = f"{jars}/*"
    files = [Path(f) for f in args.files] or sorted((ROOT / "test" / "cases").rglob("*.sql"))
    work = Path(tempfile.mkdtemp(prefix="spark-coverage-"))
    subprocess.run(["javac", "-proc:none", "-nowarn", "-cp", classpath, "-d", str(work),
                    str(ROOT / "test" / "coverage" / "SparkCoverage.java")], check=True)
    records, count = [], 0
    for f in files:
        records.append("\x02" + f.name)
        for case in load_cases([f]):
            records.append(case.sql)
            count += 1
    (work / "statements").write_text("\x01".join(records))
    java = ["java", "-Xmx3g", *JAVA_OPENS, "-cp", f"{work}:{classpath}", "SparkCoverage"]
    catalyst = next(jars.glob("spark-catalyst_*.jar"))
    subprocess.run([*java, "universe", str(work / "universe"), str(catalyst)], check=True)
    subprocess.run([*java, "parse", str(work / "statements"), str(work / "parse")], check=True)
    subprocess.run([*java, "analyze", str(work / "statements"), str(work / "analyze"), str(work / "warehouse")],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    universe = read(work / "universe")
    seen = {**read(work / "parse"), **read(work / "analyze")}
    with zipfile.ZipFile(next(jars.glob("spark-common-utils_*.jar"))) as z:
        classes = json.loads(z.read("error/error-classes.json"))
    errors = set()
    for name, info in classes.items():
        if not name.startswith("_LEGACY_ERROR_TEMP"):
            errors.update(f"ERR:{name}.{sub}" for sub in info.get("subClass", {})) if info.get("subClass") else \
                errors.add(f"ERR:{name}")

    def part(name, items):
        items = sorted(items)
        missing = [i.split(":", 1)[1] for i in items if i not in seen]
        return {"universe": name, "size": len(items), "reached": len(items) - len(missing), "not_reached": missing}

    report = {
        "spark": pyspark.__version__, "statements": count, "files": len(files),
        "universes": [
            part("grammar alternatives", [k for k in universe if k.startswith("G:")]),
            part("statement kinds", [k for k, parent in universe.items() if parent == "StatementContext"]),
            part("keywords and tokens", [k for k in universe if k.startswith("TOKEN:")]),
            part("Catalyst expression classes (analyzed plans)", [k for k in universe if k.startswith("E:")]),
            part("logical plan classes (analyzed plans)", [k for k in universe if k.startswith("P:")]),
            part("named error classes", errors),
        ]}
    Path(args.out).write_text(json.dumps(report, indent=1) + "\n")
    for u in report["universes"]:
        print(f"{u['universe']:48} {u['reached']:5} of {u['size']:5} reached")
    print(args.out)


if __name__ == "__main__":
    main()
