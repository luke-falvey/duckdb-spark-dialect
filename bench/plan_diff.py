"""Where do the plans of the Spark dialect differ from the native plans?

usage: python bench/plan_diff.py [--suite tpch] [--queries 1,9] [--sf 1]

Prints, per query, the operators (join types, aggregate shapes, scans with filters) that appear in one plan and not
in the other. A different plan shape (a join the optimizer could not reorder, an aggregate that was not pushed down,
a filter that stayed above a scan) is usually worth more than any kernel: it is how a fallible predicate blocking
the optimizer was found in TPC-H Q13.
"""
import argparse
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402


def shape(plan):
    """A multiset of plan features: operator names, join types, aggregate function lists, filters present."""
    items = Counter()
    for m in re.finditer(r"─ ([A-Za-z ]+?) ─", plan):
        items["op:" + m.group(1).strip()] += 1
    for m in re.finditer(r"Join Type: (\w+)", plan):
        items["join:" + m.group(1)] += 1
    for m in re.finditer(r"Aggregates: ([^│]+?)\s*│", plan):
        names = sorted(set(re.findall(r"([a-z_]+)\(", m.group(1))))
        items["agg:" + ",".join(names)] += 1
    items["filters"] += len(re.findall(r"Filters:", plan))
    return items


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", default="1")
    ap.add_argument("--queries")
    ap.add_argument("--dialect", default="spark")
    args = ap.parse_args()
    import tpch_bench
    db = tpch_bench.ensure_data(args.sf)
    con = common.connect(str(db), threads=1, read_only=True)
    nums = [int(q) for q in args.queries.split(",")] if args.queries else range(1, 23)
    for q in nums:
        sql = (tpch_bench.QUERIES / f"q{q:02d}.sql").read_text().strip().rstrip(";")
        common.set_dialect(con, None)
        native = shape(con.execute("EXPLAIN " + sql).fetchall()[-1][1])
        common.set_dialect(con, args.dialect)
        spark = shape(con.execute("EXPLAIN " + sql).fetchall()[-1][1])
        common.set_dialect(con, None)
        only_native = native - spark
        only_spark = spark - native
        # projections are cheap noise: report them only when the counts differ a lot
        keys = [k for k in set(only_native) | set(only_spark) if not (k == "op:Projection" and
                abs(native[k] - spark[k]) <= 3)]
        if not keys:
            print(f"Q{q:02d}: same shape")
            continue
        detail = ", ".join(f"{k} native={native[k]} {args.dialect}={spark[k]}" for k in sorted(keys))
        print(f"Q{q:02d}: {detail}")


if __name__ == "__main__":
    main()
