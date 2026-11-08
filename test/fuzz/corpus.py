"""Seed statements, a small SQL lexer and the token vocabulary shared by the fuzzers."""
import re
import sys
from pathlib import Path

FUZZ_DIR = Path(__file__).resolve().parent
TEST_DIR = FUZZ_DIR.parent
PROJECT = TEST_DIR.parent
for p in (str(TEST_DIR), str(FUZZ_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

from harness import load_cases  # noqa: E402

CASE_FILES = sorted((TEST_DIR / "cases").glob("*.sql"))

# statements that only change engine configuration or reach outside the process; they make fuzz
# runs slow or non-reproducible without exercising the parser any differently
UNSAFE = re.compile(r"\b(INSTALL|ATTACH|DETACH|LOAD|FORCE|CHECKPOINT|VACUUM|EXPORT|IMPORT|COPY)\b", re.I)


def seed_statements(files=None):
    """(setup statements, seed statements) from the case files."""
    setups, seeds = [], []
    for case in load_cases(files or CASE_FILES):
        (setups if case.has("setup") else seeds).append(case.sql)
    return setups, seeds


# ---- lexer -------------------------------------------------------------------------------------

TOKEN_RE = re.compile(r"""
    (?P<ws>\s+)
  | (?P<comment>--[^\n]*|/\*.*?\*/)
  | (?P<string>[rR]?'(?:\\.|''|[^'\\])*'|[rR]?"(?:\\.|""|[^"\\])*")
  | (?P<ident>`(?:``|[^`])*`)
  | (?P<number>(?:\d+\.\d*|\.\d+|\d+)(?:[eE][+-]?\d+)?(?:BD|[LSYDF])?\b)
  | (?P<word>[A-Za-z_][A-Za-z_0-9]*)
  | (?P<op><=>|::|->|=>|<=|>=|<>|!=|==|\|\||<<|>>>|>>|[-+*/%=<>!~^&|(),.;:\[\]{}?$#@])
  | (?P<other>.)
""", re.S | re.X)


def lex(sql):
    """List of (kind, text). Whitespace is dropped; join tokens with ' ' to rebuild a statement."""
    out = []
    for m in TOKEN_RE.finditer(sql):
        kind = m.lastgroup
        if kind != "ws":
            out.append((kind, m.group()))
    return out


def unlex(tokens):
    return " ".join(t for _, t in tokens)


# ---- vocabulary ----------------------------------------------------------------------------------

def _spark_keywords():
    text = (PROJECT / "src" / "include" / "spark_keywords.hpp").read_text()
    return re.findall(r'\{"([A-Z_]+)",', text)


def _rewrite_words():
    """Words the tokenizer rewrites and grammar hooks match on: the most likely to reach odd paths."""
    words = set()
    for f in ("spark_tokenizer.cpp", "spark_grammar.cpp"):
        words.update(re.findall(r'"([A-Z_]{2,})"', (PROJECT / "src" / f).read_text()))
    return sorted(words)


def _function_names():
    path = TEST_DIR / "spark35_functions.txt"
    return [ln.strip() for ln in path.read_text().splitlines() if re.fullmatch(r"[a-z_][a-z_0-9]*", ln.strip())]


PUNCT = ["(", ")", "(", ")", "[", "]", ",", ".", ";", "::", "->", "=>", "<=>", "!", "~", "^", "|", "&",
         "*", "+", "-", "/", "%", "=", "<", ">", "||", ":", "<<", ">>", "{", "}", "?", "$"]

EDGE_LITERALS = [
    "0", "1", "-1", "2147483647", "-2147483648", "2147483648", "9223372036854775807L",
    "-9223372036854775808L", "9223372036854775808", "127Y", "-129Y", "32768S", "1.5BD",
    "99999999999999999999999999999999999999", "0.000000000000000000000000000000000000001",
    "1e308D", "1e309", "-0.0D", "4.9e-324D", "NULL", "''", "' '", "'\\\\'", "'\\u0000'", "'%_'",
    "'日本語'", "'😀'", "X'FF'", "X'F'", "r'\\d+'", "DATE '0001-01-01'", "DATE '9999-12-31'",
    "DATE '2024-02-30'", "TIMESTAMP '1970-01-01 00:00:00'", "INTERVAL 1 DAY", "INTERVAL '1-2' YEAR TO MONTH",
    "array()", "map()", "struct()", "array(NULL)", "CAST(NULL AS INT)", "'NaN'", "CAST('NaN' AS DOUBLE)",
]

_VOCAB = None


def vocabulary():
    global _VOCAB
    if _VOCAB is None:
        words = sorted(set(_spark_keywords()) | set(_rewrite_words()))
        _VOCAB = {"words": words, "functions": _function_names(), "punct": PUNCT, "literals": EDGE_LITERALS}
    return _VOCAB
