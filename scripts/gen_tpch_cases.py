"""Generate test/cases/tpch_small.sql: the 22 TPC-H queries of bench/tpch/ over a small synthetic data set.

The tables are temporary views computed from range() with integer arithmetic, so Spark and DuckDB build
exactly the same data without any data files. Values are chosen so that the queries' predicates select rows
(TPC-H vocabulary for names, types, containers, segments; dates spanning 1992-1998).
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def pick(values, expr):
    """element_at(array(...), pmod(expr, n) + 1)"""
    items = ", ".join(f"'{v}'" for v in values)
    return f"element_at(array({items}), CAST(pmod({expr}, {len(values)}) AS INT) + 1)"


NATIONS = ["ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE", "GERMANY", "INDIA",
           "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA", "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA",
           "ROMANIA", "SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES"]
NATION_REGION = [0, 1, 1, 1, 4, 0, 3, 3, 2, 2, 4, 4, 2, 4, 0, 0, 0, 1, 2, 3, 4, 2, 3, 3, 1]
REGIONS = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]
COLORS = ["almond", "forest", "green", "blue", "khaki", "red", "navy", "pink", "lemon", "olive", "plum"]
TYPES1 = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"]
TYPES2 = ["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"]
TYPES3 = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"]
CONTAINERS = ["SM CASE", "SM BOX", "SM PACK", "SM PKG", "MED BAG", "MED BOX", "MED PKG", "MED PACK",
              "LG CASE", "LG BOX", "LG PACK", "LG PKG", "JUMBO BOX", "WRAP CASE"]
SEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"]
PRIORITIES = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
INSTRUCT = ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"]
MODES = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]

N_SUPP, N_PART, N_CUST, N_ORDERS = 100, 200, 300, 1500


def setup():
    regions = ", ".join(f"({i}, '{r}', 'region {i}')" for i, r in enumerate(REGIONS))
    nations = ", ".join(f"({i}, '{n}', {NATION_REGION[i]}, 'nation {i}')" for i, n in enumerate(NATIONS))
    dec = lambda e: f"CAST({e} AS DECIMAL(15,2))"  # noqa: E731
    return [
        f"CREATE OR REPLACE TEMPORARY VIEW region AS SELECT * FROM VALUES {regions} AS t(r_regionkey, r_name, r_comment)",
        f"CREATE OR REPLACE TEMPORARY VIEW nation AS SELECT * FROM VALUES {nations} AS t(n_nationkey, n_name, n_regionkey, n_comment)",
        f"""CREATE OR REPLACE TEMPORARY VIEW supplier AS SELECT
  CAST(id + 1 AS INT) AS s_suppkey, concat('Supplier#', lpad(CAST(id + 1 AS STRING), 9, '0')) AS s_name,
  concat('addr ', CAST(id AS STRING)) AS s_address, CAST(pmod(id * 7, 25) AS INT) AS s_nationkey,
  concat(CAST(10 + pmod(id * 7, 25) AS STRING), '-', lpad(CAST(id AS STRING), 3, '0'), '-555-0100') AS s_phone,
  {dec('pmod(id * 3779, 11000) / 1.0 - 999.99')} AS s_acctbal,
  CASE WHEN pmod(id, 9) = 0 THEN 'careful Customer accounts Complaints' ELSE concat('supplier comment ', CAST(id AS STRING)) END AS s_comment
  FROM range({N_SUPP})""",
        f"""CREATE OR REPLACE TEMPORARY VIEW part AS SELECT
  CAST(id + 1 AS INT) AS p_partkey,
  concat({pick(COLORS, 'id')}, ' ', {pick(COLORS, 'id * 3 + 1')}, ' ', {pick(COLORS, 'id * 7 + 2')}) AS p_name,
  concat('Manufacturer#', CAST(pmod(id, 5) + 1 AS STRING)) AS p_mfgr,
  concat('Brand#', CAST(pmod(id, 5) + 1 AS STRING), CAST(pmod(id * 3, 5) + 1 AS STRING)) AS p_brand,
  concat({pick(TYPES1, 'id')}, ' ', {pick(TYPES2, 'id * 7')}, ' ', {pick(TYPES3, 'id * 3')}) AS p_type,
  CAST(pmod(id * 13, 50) + 1 AS INT) AS p_size, {pick(CONTAINERS, 'id * 5')} AS p_container,
  {dec('900 + pmod(id, 1000) / 10.0')} AS p_retailprice, concat('part comment ', CAST(id AS STRING)) AS p_comment
  FROM range({N_PART})""",
        f"""CREATE OR REPLACE TEMPORARY VIEW partsupp AS SELECT
  CAST(pmod(id, {N_PART}) + 1 AS INT) AS ps_partkey,
  CAST(pmod(pmod(id, {N_PART}) + div(id, {N_PART}) * 25, {N_SUPP}) + 1 AS INT) AS ps_suppkey,
  CAST(pmod(id * 7919, 9999) + 1 AS INT) AS ps_availqty, {dec('1 + pmod(id * 104729, 100000) / 100.0')} AS ps_supplycost,
  concat('partsupp comment ', CAST(id AS STRING)) AS ps_comment
  FROM range({N_PART * 4})""",
        f"""CREATE OR REPLACE TEMPORARY VIEW customer AS SELECT
  CAST(id + 1 AS INT) AS c_custkey, concat('Customer#', lpad(CAST(id + 1 AS STRING), 9, '0')) AS c_name,
  concat('caddr ', CAST(id AS STRING)) AS c_address, CAST(pmod(id * 11, 25) AS INT) AS c_nationkey,
  concat(CAST(10 + pmod(id * 11, 25) AS STRING), '-', lpad(CAST(id AS STRING), 3, '0'), '-555-0199') AS c_phone,
  {dec('pmod(id * 7907, 11000) / 1.0 - 999.99')} AS c_acctbal, {pick(SEGMENTS, 'id')} AS c_mktsegment,
  concat('customer comment ', CAST(id AS STRING)) AS c_comment
  FROM range({N_CUST})""",
        f"""CREATE OR REPLACE TEMPORARY VIEW orders AS SELECT
  CAST(id + 1 AS INT) AS o_orderkey,
  CAST(3 * pmod(id * 7 + div(id, 97), {N_CUST // 3}) + 1 + pmod(div(id, 3), 2) AS INT) AS o_custkey,
  {pick(['F', 'O', 'P'], 'id * 5')} AS o_orderstatus, {dec('1000 + pmod(id * 7919, 400000) / 1.0')} AS o_totalprice,
  date_add(DATE '1992-01-01', CAST(pmod(id * 13, 2400) AS INT)) AS o_orderdate, {pick(PRIORITIES, 'id * 3')} AS o_orderpriority,
  concat('Clerk#', lpad(CAST(pmod(id, 50) AS STRING), 9, '0')) AS o_clerk, 0 AS o_shippriority,
  CASE WHEN pmod(id, 13) = 0 THEN 'quick special packages requests sleep' ELSE concat('order comment ', CAST(id AS STRING)) END AS o_comment
  FROM range({N_ORDERS})""",
        f"""CREATE OR REPLACE TEMPORARY VIEW lineitem AS SELECT
  CAST(div(l.id, 4) + 1 AS INT) AS l_orderkey,
  CAST(pmod(l.id * 7, {N_PART}) + 1 AS INT) AS l_partkey,
  CAST(pmod(pmod(l.id * 7, {N_PART}) + pmod(l.id, 4) * 25, {N_SUPP}) + 1 AS INT) AS l_suppkey,
  CAST(pmod(l.id, 4) + 1 AS INT) AS l_linenumber, {dec('pmod(l.id, 50) + 1')} AS l_quantity,
  {dec('(pmod(l.id, 50) + 1) * (900 + pmod(pmod(l.id * 7, 200), 1000) / 10.0)')} AS l_extendedprice,
  {dec('pmod(l.id, 11) / 100.0')} AS l_discount, {dec('pmod(l.id, 9) / 100.0')} AS l_tax,
  CASE WHEN date_add(o.o_orderdate, CAST(pmod(l.id, 120) AS INT) + 31) <= DATE '1995-06-17' THEN {pick(['R', 'A'], 'l.id')} ELSE 'N' END AS l_returnflag,
  CASE WHEN date_add(o.o_orderdate, CAST(pmod(l.id, 120) AS INT) + 1) > DATE '1995-06-17' THEN 'O' ELSE 'F' END AS l_linestatus,
  date_add(o.o_orderdate, CAST(pmod(l.id, 120) AS INT) + 1) AS l_shipdate,
  date_add(o.o_orderdate, CAST(pmod(l.id * 3, 90) AS INT) + 30) AS l_commitdate,
  date_add(o.o_orderdate, CAST(pmod(l.id, 120) + pmod(l.id * 7, 30) AS INT) + 2) AS l_receiptdate,
  {pick(INSTRUCT, 'l.id * 3')} AS l_shipinstruct, {pick(MODES, 'l.id * 5')} AS l_shipmode,
  concat('lineitem comment ', CAST(l.id AS STRING)) AS l_comment
  FROM range({N_ORDERS * 4}) AS l JOIN orders AS o ON o.o_orderkey = div(l.id, 4) + 1""",
    ]


def main():
    lines = ["-- Generated by scripts/gen_tpch_cases.py: the TPC-H queries of bench/tpch/ (Spark SQL) over a small",
             "-- synthetic data set, so the benchmark's query text is also checked against Spark 3.5.", ""]
    for stmt in setup():
        lines += ["-- @setup", stmt + ";", ""]
    for q in range(1, 23):
        sql = (ROOT / "bench" / "tpch" / f"q{q:02d}.sql").read_text().strip().rstrip(";")
        sql = "\n".join(line for line in sql.splitlines() if line.strip() and not line.strip().startswith("--"))
        if q == 10:
            # revenue ties at the LIMIT boundary on this small data set: add a tie-breaker (test copy only)
            sql = sql.replace("revenue DESC", "revenue DESC, c_custkey")
        lines += [f"-- TPC-H Q{q}", sql + ";", ""]
    (ROOT / "test" / "cases" / "tpch_small.sql").write_text("\n".join(lines))
    print("wrote test/cases/tpch_small.sql")


if __name__ == "__main__":
    main()
