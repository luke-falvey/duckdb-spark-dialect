"""Typed generator of random, mostly valid Spark SQL over a fixed fuzz schema.

The generator is deliberately simple: expressions are generated for a "kind" (int, num, str, bool,
date, ts, arr_i, arr_s, map, bin) from columns, literals, operators and a signature table of Spark
functions. Queries combine them into the shapes in QUERY_SHAPES. Results must be deterministic:
LIMIT always comes with a total ORDER BY, order-sensitive aggregates are sorted, window functions
order by the unique `id` column, and nondeterministic functions are never generated.
"""
import random
import re

GENERATOR = "gen@1"

# ---- schema --------------------------------------------------------------------------------------

COLUMNS = [  # name, kind, Spark type
    ("id", "int", "INT"), ("k", "int", "INT"), ("ti", "int", "TINYINT"), ("si", "int", "SMALLINT"),
    ("i", "int", "INT"), ("bi", "int", "BIGINT"), ("d", "num", "DECIMAL(10,2)"), ("dl", "num", "DECIMAL(38,18)"),
    ("f", "num", "FLOAT"), ("db", "num", "DOUBLE"), ("b", "bool", "BOOLEAN"), ("g", "str", "STRING"),
    ("s", "str", "STRING"), ("s2", "str", "STRING"), ("bin", "bin", "BINARY"), ("dt", "date", "DATE"),
    ("ts", "ts", "TIMESTAMP"), ("ai", "arr_i", "ARRAY<INT>"), ("ast", "arr_s", "ARRAY<STRING>"),
    ("m", "map", "MAP<STRING,INT>"), ("st", "struct", "STRUCT<a: INT, b: STRING>"),
]

VALUES = {
    "k": ["1", "1", "2", "2", "3", "NULL", "4", "1", "2", "5", "NULL", "3"],
    "ti": ["0", "1", "-1", "127", "-128", "NULL", "5", "100", "-7", "3", "2", "1"],
    "si": ["0", "32767", "-32768", "NULL", "12", "-5", "300", "7", "1", "2", "3", "4"],
    "i": ["0", "1", "-1", "2147483647", "-2147483648", "NULL", "42", "7", "-13", "100", "3", "2"],
    "bi": ["0", "9223372036854775807", "-9223372036854775808", "NULL", "1", "-1", "123456789012", "5", "7",
           "-3", "2", "2"],
    "d": ["0.00", "-0.01", "12345678.99", "-12345678.99", "NULL", "1.50", "2.25", "-3.75", "0.10", "99.99",
          "1.00", "1.50"],
    "dl": ["0", "1.000000000000000001", "-12345678901234567890.123456789012345678", "NULL", "0.5", "-0.5",
           "3.141592653589793238", "1", "2", "10", "-7.25", "0.001"],
    "f": ["0.0", "-0.0", "1.5", "-2.25", "NULL", "3.4028235E38", "1.0E-7", "100.0", "0.1", "7.0", "2.5", "1.5"],
    "db": ["0.0", "-0.0", "CAST('NaN' AS DOUBLE)", "CAST('Infinity' AS DOUBLE)", "NULL", "1.5", "-2.5", "0.1",
           "1.0E7", "0.001", "123.456", "2.5"],
    "b": ["true", "false", "NULL", "true", "false", "true", "NULL", "false", "true", "true", "false", "false"],
    "g": ["'x'", "'y'", "'x'", "'z'", "NULL", "'y'", "'x'", "'z'", "'y'", "'x'", "NULL", "'z'"],
    "s": ["''", "' a '", "'abc'", "'ABC'", "NULL", "'é日本😀'", "'a,b,,c'", "'123'", "'-1.5'", "'2024-02-29'",
          "'a%b_c'", "'abc'"],
    "s2": ["'a'", "'b'", "NULL", "''", "'abc'", "','", "'x'", "'%'", "'B'", "'c'", "'a'", "'1'"],
    "bin": ["X''", "X'00'", "X'FF'", "NULL", "X'616263'", "X'00FF'", "X'01'", "X'6869'", "X'FFFF'", "X'7F'",
            "X'80'", "X'61'"],
    "dt": ["DATE '1970-01-01'", "DATE '2024-02-29'", "DATE '0001-01-01'", "DATE '9999-12-31'", "NULL",
           "DATE '1969-12-31'", "DATE '2023-03-15'", "DATE '2000-01-01'", "DATE '1582-10-15'", "DATE '2024-12-31'",
           "DATE '2024-01-01'", "DATE '2023-03-15'"],
    "ts": ["TIMESTAMP '1970-01-01 00:00:00'", "TIMESTAMP '2024-02-29 12:34:56.123456'", "NULL",
           "TIMESTAMP '1969-12-31 23:59:59.999999'", "TIMESTAMP '2000-01-01 00:00:00'",
           "TIMESTAMP '2023-03-15 08:00:00'", "TIMESTAMP '9999-12-31 23:59:59'", "TIMESTAMP '2024-01-01 00:00:01'",
           "TIMESTAMP '2001-09-09 01:46:40'", "TIMESTAMP '2024-06-30 23:00:00'", "TIMESTAMP '2023-03-15 08:00:00'",
           "TIMESTAMP '1999-12-31 23:59:59.5'"],
    "ai": ["array()", "array(1, 2, 3)", "array(NULL)", "NULL", "array(3, 1, 2, 1)", "array(-1, 0, NULL, 5)",
           "array(2147483647, -2147483648)", "array(7)", "array(1, 1)", "array(10, 20, 30, 40)", "array(0)",
           "array(1, 2, 3)"],
    "ast": ["array()", "array('a', 'b')", "array(NULL, 'x')", "NULL", "array('b', 'a', 'b')", "array('')",
            "array('é', 'Z', 'a')", "array('1', '10', '2')", "array('x')", "array('a', NULL)", "array('c')",
            "array('a', 'b')"],
    "m": ["map()", "map('a', 1)", "map('a', NULL, 'b', 2)", "NULL", "map('x', -1, 'y', 0)", "map('', 0)",
          "map('a', 1, 'b', 2, 'c', 3)", "map('k', 2147483647)", "map('z', 5)", "map('a', 2)", "map('b', 1)",
          "map('a', 1)"],
    "st": ["named_struct('a', 1, 'b', 'x')", "named_struct('a', NULL, 'b', NULL)", "NULL",
           "named_struct('a', -1, 'b', '')", "named_struct('a', 2147483647, 'b', 'é')", "named_struct('a', 0, 'b', 'y')",
           "named_struct('a', 3, 'b', 'x')", "named_struct('a', 1, 'b', 'z')", "named_struct('a', 5, 'b', NULL)",
           "named_struct('a', 2, 'b', 'w')", "named_struct('a', 7, 'b', 'q')", "named_struct('a', 1, 'b', 'x')"],
}


def setup_statements():
    """CREATE TEMPORARY VIEW statements for the fuzz tables t (12 rows) and u (8 rows)."""
    stmts = []
    for name, nrows, shift in (("t", 12, 0), ("u", 8, 5)):
        rows = []
        for r in range(nrows):
            vals = [str(r + 1)]
            for col, _, _ in COLUMNS[1:]:
                vs = VALUES[col]
                vals.append(vs[(r + shift) % len(vs)])
            rows.append("(" + ", ".join(vals) + ")")
        raw = ", ".join(f"c{j}" for j in range(len(COLUMNS)))
        casts = ", ".join(f"CAST(c{j} AS {typ}) AS {col}" for j, (col, _, typ) in enumerate(COLUMNS))
        stmts.append(f"CREATE OR REPLACE TEMPORARY VIEW {name} AS SELECT {casts} FROM VALUES "
                     + ", ".join(rows) + f" AS v({raw})")
    return stmts


# ---- signatures ----------------------------------------------------------------------------------
# "name:return:arg,arg,..."   kinds: int num str bool date ts arr_i arr_s map bin; `small` = a small int
# literal, `'...'` = a fixed literal argument, `lam_i`/`lam_s` = a lambda over int / string elements

SIGNATURES = """
length:int:str char_length:int:str octet_length:int:str bit_length:int:str ascii:int:str instr:int:str,str
locate:int:str,str size:int:arr_i size:int:arr_s size:int:map cardinality:int:arr_i array_size:int:arr_s
year:int:date month:int:date day:int:date dayofweek:int:date dayofyear:int:date weekofyear:int:date
quarter:int:date hour:int:ts minute:int:ts second:int:ts datediff:int:date,date shiftleft:int:int,small
shiftright:int:int,small bit_count:int:int abs:int:int pmod:int:int,int greatest:int:int,int least:int:int,int
find_in_set:int:str,str levenshtein:int:str,str array_position:int:arr_i,int element_at:int:arr_i,small
array_max:int:arr_i array_min:int:arr_i hash:int:str hash:int:int xxhash64:int:str crc32:int:bin
unix_date:int:date unix_seconds:int:ts factorial:int:small bit_and:int:int ntile_dummy:int:int
abs:num:num sqrt:num:num exp:num:num ln:num:num log10:num:num log2:num:num power:num:num,num pow:num:num,small
round:num:num,small bround:num:num,small floor:num:num ceil:num:num sin:num:num cos:num:num atan2:num:num,num
cbrt:num:num degrees:num:num radians:num:num hypot:num:num,num signum:num:num mod:num:num,num nanvl:num:num,num
greatest:num:num,num least:num:num,num try_add:num:num,num try_divide:num:num,num try_multiply:num:num,num
try_subtract:num:num,num rint:num:num expm1:num:num log1p:num:num months_between:num:ts,ts positive:num:num
negative:num:num div0_dummy:num:num
upper:str:str lower:str:str initcap:str:str trim:str:str ltrim:str:str rtrim:str:str reverse:str:str
concat:str:str,str concat_ws:str:',',str,str substr:str:str,small,small substring:str:str,small left:str:str,small
right:str:str,small lpad:str:str,small,str rpad:str:str,small,str repeat:str:str,small replace:str:str,str,str
translate:str:str,str,str regexp_replace:str:str,'[a-c]',str regexp_extract:str:str,'([a-z]+)',small
substring_index:str:str,str,small format_string:str:'%s-%d',str,int md5:str:str sha1:str:str sha2:str:str,'256'
base64:str:bin hex:str:int hex:str:str soundex:str:str space:str:small date_format:str:ts,'yyyy-MM-dd HH:mm'
date_format:str:date,'EEE MMM d yyyy' array_join:str:arr_s,',' elt:str:small,str,str nvl:str:str,str ucase:str:str
chr:str:int bin:str:int conv:str:str,'10','16' format_number:str:num,small split_part:str:str,',',small
to_char:str:num,'999.99' string:str:num string:str:date string:str:ts string:str:bool lcase:str:str
coalesce:str:str,str unhex_dummy:str:str
isnull:bool:str isnotnull:bool:int startswith:bool:str,str endswith:bool:str,str contains:bool:str,str
array_contains:bool:arr_i,int isnan:bool:num map_contains_key:bool:map,str equal_null:bool:int,int
exists:bool:arr_i,lam_b forall:bool:arr_i,lam_b like_dummy:bool:str arrays_overlap:bool:arr_i,arr_i
date_add:date:date,small date_sub:date:date,small add_months:date:date,small last_day:date:date
next_day:date:date,'MON' trunc:date:date,'MM' to_date:date:str make_date:date:'2024',small,small
date_from_unix_date:date:int dateadd:date:date,small
to_timestamp:ts:str date_trunc:ts:'HOUR',ts timestamp_seconds:ts:int timestamp_millis:ts:int
array:arr_i:int,int sequence:arr_i:small,small array_distinct:arr_i:arr_i array_sort:arr_i:arr_i
sort_array:arr_i:arr_i,bool array_union:arr_i:arr_i,arr_i array_intersect:arr_i:arr_i,arr_i
array_except:arr_i:arr_i,arr_i slice:arr_i:arr_i,small,small array_remove:arr_i:arr_i,int array_compact:arr_i:arr_i
reverse:arr_i:arr_i transform:arr_i:arr_i,lam_i filter:arr_i:arr_i,lam_b concat:arr_i:arr_i,arr_i
array_repeat:arr_i:int,small map_values:arr_i:map array_append:arr_i:arr_i,int array_prepend:arr_i:arr_i,int
array_insert:arr_i:arr_i,small,int
split:arr_s:str,',' array:arr_s:str,str map_keys:arr_s:map sort_array:arr_s:arr_s array_distinct:arr_s:arr_s
transform:arr_s:arr_s,lam_s array_remove:arr_s:arr_s,str
map:map:str,int map_filter:map:map,lam_kv
"""


def parse_signatures():
    sigs = []
    # spaces inside quoted literal arguments are not separators
    text = re.sub(r"'[^']*'", lambda m: m.group().replace(" ", "\x00"), SIGNATURES)
    for item in text.split():
        name, ret, args = item.replace("\x00", " ").split(":", 2)
        if name.endswith("_dummy"):
            continue
        sigs.append((name, ret, [a for a in args.split(",") if a] if args else []))
    return sigs


SIGS = parse_signatures()
BY_RETURN = {}
for _s in SIGS:
    BY_RETURN.setdefault(_s[1], []).append(_s)

LITERALS = {
    "int": ["0", "1", "-1", "2", "7", "NULL", "2147483647", "-2147483648", "100", "3L", "5S", "-128Y"],
    "num": ["0.0", "1.5", "-2.25", "0.1D", "-0.0D", "1e10D", "NULL", "CAST('NaN' AS DOUBLE)", "3.14159", "100.5BD",
            "1", "-7"],
    "str": ["''", "'a'", "'abc'", "' x '", "NULL", "'é'", "'1'", "'a,b'", "'%'", "'ABC'", "'2024-02-29'"],
    "bool": ["true", "false", "CAST(NULL AS BOOLEAN)"],
    "date": ["DATE '2024-02-29'", "DATE '1970-01-01'", "DATE '2000-01-31'", "CAST(NULL AS DATE)"],
    "ts": ["TIMESTAMP '2024-02-29 12:00:00'", "TIMESTAMP '1970-01-01 00:00:00'", "CAST(NULL AS TIMESTAMP)"],
    "arr_i": ["array(1, 2, 3)", "array()", "array(NULL, 1)", "CAST(NULL AS ARRAY<INT>)"],
    "arr_s": ["array('a', 'b')", "array()", "array(NULL)"],
    "map": ["map('a', 1)", "map()"],
    "bin": ["X'00'", "X'616263'", "CAST(NULL AS BINARY)"],
    "small": ["0", "1", "2", "3", "-1", "5"],
}

KINDS = ["int", "num", "str", "bool", "date", "ts", "arr_i", "arr_s", "map", "bin"]
NUMERIC = {"int", "num"}


class Gen:
    def __init__(self, rng, max_depth=3):
        self.rng = rng
        self.max_depth = max_depth

    def pick(self, seq):
        return self.rng.choice(seq)

    # columns visible in the current scope: list of (sql, kind)
    def columns_of(self, alias):
        out = []
        for col, kind, _ in COLUMNS:
            ref = f"{alias}.{col}" if alias else col
            if kind == "struct":
                out += [(f"{ref}.a", "int"), (f"{ref}.b", "str")]
            else:
                out.append((ref, kind))
        return out

    def literal(self, kind):
        return self.pick(LITERALS.get(kind, LITERALS["int"]))

    def expr(self, kind, cols, depth=0):
        rng = self.rng
        if kind == "small":
            return self.literal("small")
        leaf = depth >= self.max_depth or rng.random() < 0.25 + 0.15 * depth
        if leaf:
            matching = [c for c, k in cols if k == kind or (kind == "num" and k == "int")]
            if matching and rng.random() < 0.75:
                return self.pick(matching)
            return self.literal(kind)
        r = rng.random()
        if r < 0.45 and kind in BY_RETURN:
            return self.call(self.pick(BY_RETURN[kind]), cols, depth)
        if r < 0.75:
            op = self.operator(kind, cols, depth)
            if op:
                return op
        if r < 0.85:
            return self.conditional(kind, cols, depth)
        if r < 0.93:
            return self.cast_from_other(kind, cols, depth)
        if kind in ("int", "str") and rng.random() < 0.5:
            return self.subscript(kind, cols, depth)
        return self.expr(kind, cols, depth + 1)

    def call(self, sig, cols, depth):
        name, _, args = sig
        parts = []
        for a in args:
            if a.startswith("'"):
                parts.append(a)
            elif a == "lam_i":
                parts.append("x -> " + self.pick(["x + 1", "x * 2", "-x", "coalesce(x, 0)", "x % 3", "abs(x)"]))
            elif a == "lam_s":
                parts.append("x -> " + self.pick(["upper(x)", "concat(x, '!')", "coalesce(x, '?')", "reverse(x)"]))
            elif a == "lam_b":
                parts.append("x -> " + self.pick(["x > 1", "x IS NULL", "x % 2 = 0", "x = 0", "x < 0"]))
            elif a == "lam_kv":
                parts.append("(k, v) -> " + self.pick(["v > 0", "k = 'a'", "v IS NULL"]))
            else:
                parts.append(self.expr(a, cols, depth + 1))
        return f"{name}({', '.join(parts)})"

    def operator(self, kind, cols, depth):
        rng = self.rng
        e = lambda k: self.expr(k, cols, depth + 1)  # noqa: E731
        if kind == "int":
            op = self.pick(["+", "-", "*", "div", "%", "&", "|", "^"])
            if rng.random() < 0.1:
                return f"(~{e('int')})"
            if rng.random() < 0.1:
                return f"(-{e('int')})"
            return f"({e('int')} {op} {e('int')})"
        if kind == "num":
            op = self.pick(["+", "-", "*", "/", "%"])
            return f"({e(self.pick(['num', 'int']))} {op} {e('num')})"
        if kind == "str":
            return f"({e('str')} || {e(self.pick(['str', 'int']))})"
        if kind == "bool":
            r = rng.random()
            if r < 0.3:
                k = self.pick(["int", "num", "str", "date"])
                op = self.pick(["=", "<>", "<", "<=", ">", ">=", "<=>", "!=", "=="])
                return f"({e(k)} {op} {e(k)})"
            if r < 0.45:
                return f"({e('bool')} {self.pick(['AND', 'OR'])} {e('bool')})"
            if r < 0.55:
                return f"(NOT {e('bool')})"
            if r < 0.65:
                k = self.pick(["int", "str", "date"])
                return f"({e(k)} {self.pick(['IS NULL', 'IS NOT NULL'])})"
            if r < 0.75:
                k = self.pick(["int", "num"])
                return f"({e(k)} {self.pick(['BETWEEN', 'NOT BETWEEN'])} {e(k)} AND {e(k)})"
            if r < 0.85:
                k = self.pick(["int", "str"])
                return f"({e(k)} {self.pick(['IN', 'NOT IN'])} ({', '.join(e(k) for _ in range(rng.randint(1, 4)))}))"
            if r < 0.93:
                pat = self.pick(["'a%'", "'%b%'", "'_'", "''", "'%'", "'a\\\\%'", "'A%'"])
                return f"({e('str')} {self.pick(['LIKE', 'NOT LIKE', 'ILIKE', 'RLIKE'])} {pat})"
            return f"({e(self.pick(['bool', 'int']))} IS {self.pick(['TRUE', 'FALSE', 'NOT TRUE', 'UNKNOWN', 'NOT DISTINCT FROM NULL'])})"
        if kind == "date":
            return f"({e('date')} {self.pick(['+', '-'])} {self.pick(['INTERVAL 1 DAY', 'INTERVAL 2 MONTH', 'INTERVAL 1 YEAR'])})"
        if kind == "ts":
            return f"({e('ts')} {self.pick(['+', '-'])} {self.pick(['INTERVAL 1 HOUR', 'INTERVAL 90 MINUTE', 'INTERVAL 1 DAY', 'INTERVAL 3 SECOND'])})"
        if kind == "arr_i":
            return f"array({e('int')}, {e('int')})"
        return None

    def conditional(self, kind, cols, depth):
        rng = self.rng
        e = lambda k: self.expr(k, cols, depth + 1)  # noqa: E731
        r = rng.random()
        if r < 0.35:
            branches = " ".join(f"WHEN {e('bool')} THEN {e(kind)}" for _ in range(rng.randint(1, 3)))
            other = f" ELSE {e(kind)}" if rng.random() < 0.7 else ""
            return f"CASE {branches}{other} END"
        if r < 0.5:
            k = self.pick(["int", "str"])
            return f"CASE {e(k)} WHEN {e(k)} THEN {e(kind)} ELSE {e(kind)} END"
        if r < 0.7:
            return f"if({e('bool')}, {e(kind)}, {e(kind)})"
        if r < 0.85:
            return f"coalesce({e(kind)}, {e(kind)})"
        if r < 0.92 or kind == "map":
            return f"nvl({e(kind)}, {e(kind)})"
        return f"nullif({e(kind)}, {e(kind)})"

    CASTS = {"int": ("INT", ["num", "str", "bool", "int"]), "num": ("DOUBLE", ["int", "str", "num"]),
             "str": ("STRING", ["int", "num", "date", "ts", "bool", "arr_i", "map", "bin"]),
             "bool": ("BOOLEAN", ["int", "str"]), "date": ("DATE", ["str", "ts"]), "ts": ("TIMESTAMP", ["str", "date"]),
             "arr_s": ("ARRAY<STRING>", ["arr_i"]), "arr_i": ("ARRAY<INT>", ["arr_s"])}
    TARGETS = {"int": ["TINYINT", "SMALLINT", "INT", "BIGINT"], "num": ["DOUBLE", "FLOAT", "DECIMAL(10,2)", "DECIMAL(38,0)", "DECIMAL(5,3)"]}

    def cast_from_other(self, kind, cols, depth):
        if kind not in self.CASTS:
            return self.literal(kind)
        typ, froms = self.CASTS[kind]
        typ = self.pick(self.TARGETS.get(kind, [typ]))
        fn = "CAST" if self.rng.random() < 0.8 else "try_cast"
        return f"{fn}({self.expr(self.pick(froms), cols, depth + 1)} AS {typ})"

    def subscript(self, kind, cols, depth):
        if kind == "int":
            return self.pick([f"{self.expr('arr_i', cols, depth + 1)}[{self.literal('small')}]",
                              f"{self.expr('map', cols, depth + 1)}[{self.pick(['a', 'b', 'x'])!r}]"])
        return f"{self.expr('arr_s', cols, depth + 1)}[{self.literal('small')}]"


# ---- queries -------------------------------------------------------------------------------------

AGGREGATES = [  # template, argument kind, result kind
    ("count(*)", None), ("count({})", "any"), ("count(DISTINCT {})", "any"), ("sum({})", "int"), ("sum({})", "num"),
    ("avg({})", "int"), ("avg({})", "num"), ("min({})", "any_ord"), ("max({})", "any_ord"),
    ("bool_and({})", "bool"), ("bool_or({})", "bool"), ("every({})", "bool"), ("some({})", "bool"),
    ("array_sort(collect_set({}))", "int"), ("sort_array(collect_list({}))", "str"), ("median({})", "num"),
    ("percentile({}, 0.5)", "int"), ("stddev_pop({})", "int"), ("var_samp({})", "int"),
    ("count_if({})", "bool"), ("sum(DISTINCT {})", "int"), ("max_by(id, {})", "int_unique"),
    ("min({}) FILTER (WHERE {})", "int_filter"), ("bit_or({})", "int"), ("bit_xor({})", "int"),
]
WINDOW_FUNCS = ["row_number()", "rank()", "dense_rank()", "percent_rank()", "cume_dist()", "ntile(3)",
                "lag({e})", "lead({e}, 2)", "lag({e}, 1, 0)", "sum({e})", "count({e})", "min({e})", "max({e})",
                "avg({e})", "first_value({e})", "last_value({e})", "nth_value({e}, 2)"]
FRAMES = ["", "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
          "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING", "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
          "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"]


class QueryGen(Gen):
    def __init__(self, rng, max_depth=3):
        super().__init__(rng, max_depth)
        self.shapes = {
            "project": (self.q_project, 5), "aggregate": (self.q_aggregate, 4), "grouping_sets": (self.q_grouping, 2),
            "window": (self.q_window, 3), "join": (self.q_join, 3), "semi_anti": (self.q_semi_anti, 1),
            "setop": (self.q_setop, 2), "lateral": (self.q_lateral, 2), "pivot": (self.q_pivot, 1),
            "subquery": (self.q_subquery, 2), "cte": (self.q_cte, 1), "sort_by": (self.q_sort_by, 1),
            "limit": (self.q_limit, 2), "distinct": (self.q_distinct, 1),
        }

    def query(self, shapes=None):
        names = [s for s in self.shapes if not shapes or s in shapes]
        weights = [self.shapes[s][1] for s in names]
        shape = self.rng.choices(names, weights)[0]
        return shape, self.shapes[shape][0]()

    def kinds(self):
        return self.pick(KINDS[:6] * 3 + KINDS[6:])

    def select_list(self, cols, n=None, orderable=False):
        n = n or self.rng.randint(1, 4)
        kinds = [self.kinds() for _ in range(n)]
        if orderable:
            kinds = [k if k != "map" else "int" for k in kinds]
        return ", ".join(f"{self.expr(k, cols)} AS c{j}" for j, k in enumerate(kinds))

    def where(self, cols):
        return f" WHERE {self.expr('bool', cols)}" if self.rng.random() < 0.5 else ""

    def table(self):
        return self.pick(["t", "u"])

    def q_project(self):
        cols = self.columns_of(None)
        return f"SELECT id, {self.select_list(cols)} FROM {self.table()}{self.where(cols)}"

    def agg(self, cols):
        tmpl, kind = self.pick(AGGREGATES)
        if kind is None:
            return tmpl
        if kind == "any":
            return tmpl.format(self.expr(self.kinds(), cols))
        if kind == "any_ord":
            return tmpl.format(self.expr(self.pick(["int", "num", "str", "date", "ts", "bool"]), cols))
        if kind == "int_unique":
            return "max_by(id, id)"  # max_by with ties is nondeterministic; id is unique
        if kind == "int_filter":
            return tmpl.format(self.expr("int", cols), self.expr("bool", cols))
        return tmpl.format(self.expr(kind, cols))

    def q_aggregate(self):
        cols = self.columns_of(None)
        keys = self.rng.sample(["g", "k", "b", "dt", "ti", f"({self.expr('int', cols, 2)})"], self.rng.randint(0, 2))
        aggs = ", ".join(f"{self.agg(cols)} AS a{j}" for j in range(self.rng.randint(1, 3)))
        sel = ", ".join(keys + [aggs])
        group = f" GROUP BY {', '.join(keys)}" if keys else ""
        having = f" HAVING {self.agg(cols).split(' FILTER')[0]} IS NOT NULL" if keys and self.rng.random() < 0.3 else ""
        return f"SELECT {sel} FROM {self.table()}{self.where(cols)}{group}{having}"

    def q_grouping(self):
        cols = self.columns_of(None)
        agg = self.agg(cols)
        form = self.rng.randrange(4)
        if form == 0:
            group = "GROUP BY g, k WITH ROLLUP"
        elif form == 1:
            group = "GROUP BY CUBE(g, b)"
        elif form == 2:
            group = "GROUP BY GROUPING SETS ((g), (k), ())"
        else:
            group = "GROUP BY ROLLUP(g, k)"
        keys = {0: "g, k", 1: "g, b", 2: "g, k", 3: "g, k"}[form]
        extra = self.pick(["grouping_id()", f"grouping({keys.split(', ')[0]})", "count(*)"])
        return f"SELECT {keys}, {agg} AS a0, {extra} AS gid FROM {self.table()} {group}"

    def q_window(self):
        cols = self.columns_of(None)
        items = []
        for j in range(self.rng.randint(1, 3)):
            fn = self.pick(WINDOW_FUNCS)
            if "{e}" in fn:
                fn = fn.format(e=self.expr(self.pick(["int", "num"]), cols, 2))
            part = self.pick(["", "PARTITION BY g", "PARTITION BY k", "PARTITION BY b, g"])
            order_key = self.pick(["id", "i, id", "s, id", "db, id", "dt DESC, id", "k NULLS LAST, id"])
            ranking = fn.split("(")[0] in ("row_number", "rank", "dense_rank", "percent_rank", "cume_dist", "ntile",
                                           "lag", "lead")
            frame = "" if ranking else self.pick(FRAMES)
            if frame.startswith("RANGE"):
                order_key = "id"
            items.append(f"{fn} OVER ({part} ORDER BY {order_key} {frame}) AS w{j}")
        return f"SELECT id, {', '.join(items)} FROM {self.table()}{self.where(cols)}"

    def q_join(self):
        cols = self.columns_of("a") + self.columns_of("b")
        kind = self.pick(["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN", "CROSS JOIN", "INNER JOIN"])
        on = "" if kind == "CROSS JOIN" else " ON " + self.pick(
            ["a.k = b.k", "a.g = b.g", "a.k = b.k AND a.b = b.b", f"a.k = b.k AND {self.expr('bool', cols, 2)}",
             "a.k <=> b.k", "a.id = b.id + 1"])
        if kind == "CROSS JOIN" and self.rng.random() < 0.5:
            return f"SELECT a.id, b.id AS bid, {self.select_list(cols, 2)} FROM t a CROSS JOIN u b{self.where(cols)}"
        if kind == "JOIN" and self.rng.random() < 0.3:
            return f"SELECT a.id, {self.select_list(cols, 2)} FROM t a JOIN u b USING (k)"
        return f"SELECT a.id, b.id AS bid, {self.select_list(cols, 2)} FROM t a {kind} u b{on}"

    def q_semi_anti(self):
        cols = self.columns_of("a")
        kind = self.pick(["LEFT SEMI JOIN", "LEFT ANTI JOIN", "SEMI JOIN", "ANTI JOIN"])
        on = self.pick(["a.k = b.k", "a.g = b.g", "a.k = b.k AND a.i > b.i"])
        return f"SELECT a.id, {self.select_list(cols, 2)} FROM t a {kind} u b ON {on}"

    def q_setop(self):
        kind = self.kinds()
        c = self.columns_of(None)
        op = self.pick(["UNION", "UNION ALL", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL", "MINUS"])
        if kind in ("map",) and op != "UNION ALL":
            kind = "int"  # maps are not comparable
        return f"SELECT {self.expr(kind, c)} AS c0 FROM t {op} SELECT {self.expr(kind, c)} AS c0 FROM u"

    def q_lateral(self):
        cols = self.columns_of("a") + [("x", "int")]
        outer = self.pick(["", "OUTER "])
        gen = self.pick([("explode(a.ai)", "e AS x", [("x", "int")]),
                         ("posexplode(a.ai)", "e AS p, x", [("x", "int"), ("p", "int")]),
                         ("explode(a.m)", "e AS mk, x", [("x", "int"), ("mk", "str")]),
                         ("explode(sequence(1, a.ti % 4))", "e AS x", [("x", "int")])])
        cols = self.columns_of("a") + gen[2]
        return f"SELECT a.id, {self.select_list(cols, 2)} FROM t a LATERAL VIEW {outer}{gen[0]} {gen[1]}"

    def q_pivot(self):
        agg = self.pick(["sum(i)", "count(i)", "max(s)", "avg(d)", "min(dt)"])
        return (f"SELECT * FROM (SELECT k, g, i, s, d, dt FROM {self.table()}) "
                f"PIVOT ({agg} FOR g IN ('x', 'y', 'z'))")

    def q_subquery(self):
        cols = self.columns_of("a")
        form = self.rng.randrange(4)
        if form == 0:
            pred = "a.k IN (SELECT k FROM u)"
        elif form == 1:
            pred = "EXISTS (SELECT 1 FROM u b WHERE b.k = a.k AND b.i > a.i)"
        elif form == 2:
            pred = "a.k NOT IN (SELECT k FROM u WHERE k IS NOT NULL)"
        else:
            pred = "a.i > (SELECT avg(i) FROM u)"
        extra = f", (SELECT max(b.id) FROM u b WHERE b.g = a.g) AS sq" if self.rng.random() < 0.5 else ""
        return f"SELECT a.id, {self.select_list(cols, 2)}{extra} FROM t a WHERE {pred}"

    def q_cte(self):
        cols = self.columns_of(None)
        inner = f"SELECT id, k, g, {self.expr('int', cols)} AS x, {self.expr('str', cols)} AS y FROM t"
        outer_cols = [("id", "int"), ("k", "int"), ("g", "str"), ("x", "int"), ("y", "str")]
        return (f"WITH c AS ({inner}) SELECT id, {self.expr(self.pick(['int', 'str', 'bool']), outer_cols)} AS r "
                f"FROM c WHERE {self.expr('bool', outer_cols)} OR id > 6")

    def q_sort_by(self):
        cols = self.columns_of(None)
        clause = self.pick(["SORT BY i", "DISTRIBUTE BY g", "CLUSTER BY k", "DISTRIBUTE BY k SORT BY id"])
        return f"SELECT id, {self.select_list(cols, 2)} FROM {self.table()} {clause}"

    def q_limit(self):
        cols = self.columns_of(None)
        sel = self.select_list(cols, self.rng.randint(1, 3), orderable=True)
        n = self.rng.randint(0, 6)
        off = f" OFFSET {self.rng.randint(1, 4)}" if self.rng.random() < 0.3 else ""
        order = self.pick(["id", "id DESC", "c0, id", "c0 DESC NULLS FIRST, id", "c0 NULLS LAST, id"])
        return f"SELECT id, {sel} FROM {self.table()} ORDER BY {order} LIMIT {n}{off}"

    def q_distinct(self):
        cols = self.columns_of(None)
        return f"SELECT DISTINCT {self.select_list(cols, self.rng.randint(1, 2), orderable=True)} FROM {self.table()}"


def generate(seed, n, max_depth=3, shapes=None):
    rng = random.Random(seed)
    g = QueryGen(rng, max_depth)
    for _ in range(n):
        yield g.query(shapes)
