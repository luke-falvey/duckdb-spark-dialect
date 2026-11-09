# Performance backlog

Plan: [plans/performance-testing.md](plans/performance-testing.md). Log: [perf-log.md](perf-log.md).
Status: `todo`, `in progress`, `done`, `dropped: <reason>`. Ordered by end-to-end impact. Each item gets about two
hours; update this file whenever an item changes state.

| # | item | today | idea to start from | target | status |
|---|---|---|---|---|---|
| 1 | `sum(decimal)` / `avg(decimal)` 128-bit state | 1.6x -> 1.5x | accumulate in `int64_t` while the running sum fits (checked add), switch the state to `hugeint_t` on overflow; or pick the state by input precision at bind time | <= 1.15x; TPC-H geomean <= 1.12x | done (64-bit partial sum: 7% faster; the TPC-H cost was elsewhere, see 11) |
| 2 | Q01 / `mul_decimal`: 128-bit decimal products | 2.57x / 4.0x | `EXPLAIN ANALYZE` for the dominant operator; keep 64-bit physical types where Spark's precision <= 18 | Q01 <= 2.0x | todo |
| 3 | `format_string` / `printf` | 5.3x -> 0.5x | parse the format string once; fast paths for `%s` / `%d` | <= 1.5x | done |
| 4 | `to_json` | 18x | write JSON from the vectors directly instead of `Value`s per row | <= 2x | todo |
| 5 | `date_format` / `to_timestamp` with patterns | 3.5x -> 1.9x / 2.2x -> 2.0x | profile the formatter after the pattern cache; no `std::string` per field | <= 1.5x | done (1.9x; rest is AddString + memcpy) |
| 6 | `split` with a literal separator | 3.4x | build the list child vector directly | <= 1.3x | done: 3.4x -> ~1.7x (206 -> 102 ms) | (literal separator done, see perf-log)
| 7 | `substr` | 2.3x | detect ASCII once per string; use DuckDB's `substring` when the start is positive | <= 1.3x | todo |
| 8 | `array_contains`, `size`, `floor`, `hex`, string to int / date casts, `cast_double_to_string` | 1.8x to 2.3x | small kernels instead of macros wrapping conversions | <= 1.3x | todo |
| 9 | parsing | 3.35x | order `SpAtom` alternatives, first-set check for keyword alternatives, fewer hook frames, merge tokenizer passes | <= 2.0x | todo |
| 10 | findings of phase B | | | | todo |
| 11 | audit of fallible kernels in predicates / joins / aggregate arguments: they block filter pushdown below outer joins and aggregate pushdown (found with Q13: `like_escape`) | Q13 1.5x -> 1.0x for LIKE | check which dispatcher results are fallible although they cannot fail for the bound arguments (constant, valid patterns; casts that cannot overflow); bind non-fallible forms then | no pushdown regression in TPC-H / TPC-DS plans | in progress (LIKE done) |
| 12 | correctness findings from the sweeps (not performance): `x LIKE p ESCAPE NULL` accepted by the lenient dialect where Spark errors; `regexp_like(NULL, <invalid regex>)` raises where Spark returns NULL; `from_unixtime(9223372036854775807L)` raises where Spark wraps | | | | todo |
