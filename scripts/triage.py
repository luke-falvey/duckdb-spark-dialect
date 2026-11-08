"""Group failures in test/results/failures.txt by cause."""
import re, collections, sys
text = open(sys.argv[1] if len(sys.argv) > 1 else 'test/results/failures.txt').read()
fails = [f for f in re.split(r'\n(?=FAIL |LENIENT |SKIP )', text) if f.startswith('FAIL')]
missing = collections.Counter(); other = collections.Counter(); differ = []
for f in fails:
    m = re.search(r'Function with name (\w+) does not exist', f)
    if m:
        missing[m.group(1)] += 1; continue
    if 'rows differ' in f:
        differ.append(f); continue
    m = re.search(r'duckdb error: ([^\n]{0,80})', f)
    other[re.sub(r'"[^"]*"|\'[^\']*\'', 'X', m.group(1) if m else '?')[:70]] += 1
print(f"{len(fails)} failures: {sum(missing.values())} missing function, {len(differ)} rows differ, {sum(other.values())} other errors")
print("missing:", ' '.join(sorted(missing)))
print("other errors:")
for k, c in other.most_common(40):
    print(f"  {c:4} {k}")
