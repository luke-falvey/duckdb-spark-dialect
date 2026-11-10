"""Print the failures of the last test/run_duckdb.py run compactly: python scripts/show_failures.py [file-substring] [config-substring] [--strict]"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
args = [a for a in sys.argv[1:] if not a.startswith("--")]
strict = "--strict" in sys.argv
path = ROOT / "test" / "results" / ("failures_spark_strict.txt" if strict else "failures.txt")
want_file = args[0] if args else ""
want_config = args[1] if len(args) > 1 else ""
for block in path.read_text().split("\n\n"):
    m = re.match(r"(FAIL|PHASE|XPASS) (\S+):(\d+) ?(\S*)\n  sql: (.*?)\n  (.*)", block, re.S)
    if not m or want_file not in m.group(2) or want_config not in m.group(4):
        continue
    print(f"{m.group(1)} {m.group(2)}:{m.group(3)} [{m.group(4)}]")
    print("   sql:", m.group(5)[:230].replace("\n", " "))
    for line in m.group(6).split("\n"):
        print("   ", line.strip()[:420])
