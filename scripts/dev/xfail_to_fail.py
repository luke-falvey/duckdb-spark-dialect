"""Turn XFAIL blocks of a normal-mode failures file into FAIL blocks (the expected-failure prefix removed)."""
import sys, re, tomllib
entries = tomllib.load(open(sys.argv[3], 'rb'))['xfail']
prefixes = sorted({f"[{e['id']}] {e['reason']}; " for e in entries}, key=len, reverse=True)
text = open(sys.argv[1]).read()
out = []
for b in re.split(r'\n(?=[A-Z]+ \S+:\d+)', text):
    if b.startswith('XFAIL '):
        b = 'FAIL ' + b[len('XFAIL '):]
        for p in prefixes:
            if '\n  ' + p in b:
                b = b.replace('\n  ' + p, '\n  ', 1); break
        else:
            raise SystemExit('no prefix for: ' + b[:200])
    out.append(b)
open(sys.argv[2], 'w').write('\n'.join(out))
