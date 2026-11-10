"""Engine runners shared by the differential tests, the fuzzers and the benchmarks.

  SparkOracle   one long-lived local SparkSession (the reference engine)
  DuckRunner    DuckDB with the extension, in a separate worker process, so that a crash or a hang
                is reported as a result instead of killing the caller
  ShellRunner   the `duckdb` shell binary (used with the AddressSanitizer build)

Every run returns a Result. Result.status is one of:
  ok        rows holds the normalized rows
  error     an ordinary error (error holds the first line)
  internal  DuckDB raised an INTERNAL error (the database is invalidated; the worker is restarted)
  crash     the worker process died (error holds the signal)
  timeout   the statement did not finish in time (the worker is restarted)
  sanitizer the ASan / UBSan build reported an error (ShellRunner only)
"""
import multiprocessing as mp
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

TEST_DIR = Path(__file__).resolve().parent
PROJECT = TEST_DIR.parent
if str(TEST_DIR) not in sys.path:
    sys.path.insert(0, str(TEST_DIR))

DEFAULT_EXTENSION = PROJECT / "build" / "spark_dialect.duckdb_extension"


def extension_path():
    return Path(os.environ.get("SPARK_EXTENSION", DEFAULT_EXTENSION)).resolve()


def use_utc():
    os.environ["TZ"] = "UTC"
    time.tzset()


_SCRATCH = []


def _remove_scratch():
    for path in _SCRATCH:
        shutil.rmtree(path, ignore_errors=True)


def scratch_dir(prefix="spark-dialect-"):
    """A fresh working directory, so statements that write files (COPY, EXPORT DATABASE, ...) never
    write into the repository. It is removed when the process that created it exits."""
    import atexit
    if not _SCRATCH:
        atexit.register(_remove_scratch)
    path = tempfile.mkdtemp(prefix=prefix)
    _SCRATCH.append(path)
    return path


@dataclass
class Result:
    status: str
    rows: list = None
    error: str = None
    phase: str = None   # Spark only: "analysis" (spark.sql) or "runtime" (collect)
    schema: str = None
    seconds: float = 0.0
    extra: dict = field(default_factory=dict)

    @property
    def ok(self):
        return self.status == "ok"


def first_line(e, limit=300):
    """The first non-empty line of an exception message (PySpark messages often start with a blank line)."""
    for line in str(e).split("\n"):
        if line.strip():
            return line.strip()[:limit]
    return ""


ERROR_CLASS = re.compile(r"\[([A-Z][A-Z0-9_]*(?:\.[A-Z][A-Z0-9_]*)*)\]")


def error_class_of(text, exc=None):
    """The error class of a Spark (or extension) error: `getErrorClass()` when the exception has one, else the
    first bracketed upper-case token of the message (`[DIVIDE_BY_ZERO]`, `[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES]`)."""
    if exc is not None and hasattr(exc, "getErrorClass"):
        try:
            cls = exc.getErrorClass()
            if cls:
                return cls
        except Exception:  # noqa: BLE001
            pass
    m = ERROR_CLASS.search(text or "")
    return m.group(1) if m else None


# ----------------------------------------------------------------------------------------------
# Spark

class SparkOracle:
    def __init__(self, warehouse=None):
        use_utc()
        from pyspark.sql import SparkSession
        self.warehouse = warehouse or scratch_dir("spark-warehouse-")
        self.spark = (SparkSession.builder.master("local[2]")
                      .config("spark.ui.enabled", "false")
                      .config("spark.sql.session.timeZone", "UTC")
                      .config("spark.sql.warehouse.dir", str(self.warehouse))
                      .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
                      .getOrCreate())
        self.spark.sparkContext.setLogLevel("ERROR")
        assert self.spark.version.startswith("3.5"), self.spark.version
        if not hasattr(self, "replay"):
            self.replay = []
            self.restarts = 0

    DEAD_MARKERS = ("Connection refused", "Py4JNetworkError", "SparkContext was shut down",
                    "Cannot call methods on a stopped SparkContext", "Answer from Java side is empty",
                    "java.lang.OutOfMemoryError")

    def _restart(self):
        try:
            self.spark.stop()
        except Exception:  # noqa: BLE001
            pass
        # forget the dead JVM so that getOrCreate() launches a new one
        from pyspark import SparkContext
        from pyspark.sql import SparkSession
        SparkContext._active_spark_context = None
        SparkContext._gateway = None
        SparkContext._jvm = None
        SparkSession._instantiatedSession = None
        SparkSession._activeSession = None
        self.__init__(self.warehouse)
        for sql in self.replay:
            self.run(sql)
        self.restarts += 1

    def run(self, sql, _retry=True):
        r = self._run(sql)
        if r.extra.pop("fatal", False) or (not r.ok and (r.error or "").startswith("An error occurred while calling")
                                            and not self._alive()):
            self._restart()
            return r
        if not r.ok and any(m in (r.error or "") for m in self.DEAD_MARKERS):
            # the JVM died (usually out of memory on a huge result): restart and try once more, so a dead
            # session is not recorded as "Spark rejects this query" for every following query
            self._restart()
            if _retry:
                r = self.run(sql, _retry=False)
                if not r.ok and any(m in (r.error or "") for m in self.DEAD_MARKERS):
                    self._restart()
                    r.status = "crash"
        return r

    def _alive(self):
        """False when the JVM is gone or going (a raw Py4J error, not a Spark error, can mean it is exiting: an
        out-of-memory error in an executor thread exits the JVM shortly after the statement failed)."""
        time.sleep(0.3)
        try:
            return self.spark.sql("SELECT 1").collect()[0][0] == 1
        except Exception:  # noqa: BLE001
            return False

    def _run(self, sql):
        from harness import normalize
        start = time.perf_counter()
        try:
            df = self.spark.sql(sql)
        except Exception as e:  # noqa: BLE001
            text = first_line(e)
            return Result("error", error=text, phase="analysis", seconds=time.perf_counter() - start,
                          extra={"error_class": error_class_of(str(e), e)})
        try:
            rows = df.collect()
            out = [[normalize(v) for v in row] for row in rows]
            fields = df.schema.fields
            return Result("ok", rows=out, schema=df.schema.simpleString(), seconds=time.perf_counter() - start,
                          extra={"types": [f.dataType.simpleString() for f in fields],
                                 "names": [f.name for f in fields]})
        except Exception as e:  # noqa: BLE001
            # str() of a Py4J error asks the JVM for the Java stack trace: once, and it fails when the JVM is gone
            try:
                full = str(e)
            except Exception as gone:  # noqa: BLE001
                full = f"{type(e).__name__}\nPy4JNetworkError: {gone}"
            text = first_line(full)
            # a fatal JVM error (out of memory on a huge result) can surface as a Py4J wrapper whose first line
            # names no dead marker; the JVM then exits while the next statement runs, which would record that
            # statement as rejected by Spark. Keep this statement's error and start a new JVM for the next one.
            fatal = any(m in full for m in self.DEAD_MARKERS) and not any(m in text for m in self.DEAD_MARKERS)
            try:
                error_class = error_class_of(full, e)
            except Exception:  # noqa: BLE001
                error_class = error_class_of(full)
            return Result("error", error=text, phase="runtime", seconds=time.perf_counter() - start,
                          extra={"error_class": error_class, **({"fatal": True} if fatal else {})})

    def setup(self, statements):
        for sql in statements:
            r = self.run(sql)
            if not r.ok:
                raise RuntimeError(f"spark setup failed: {sql[:200]}: {r.error}")
            self.replay.append(sql)

    def reset(self):
        """Drop every table and view, so the next file / run starts from a clean catalog."""
        self.replay = []
        for sql in ("USE default", "CLEAR CACHE"):
            try:
                self.spark.sql(sql)
            except Exception:  # noqa: BLE001
                pass
        for db in self.spark.catalog.listDatabases():
            if db.name != "default":
                try:
                    self.spark.sql(f"DROP DATABASE IF EXISTS `{db.name}` CASCADE")
                except Exception:  # noqa: BLE001
                    pass
        for t in self.spark.catalog.listTables():
            try:
                if t.isTemporary:
                    self.spark.catalog.dropTempView(t.name)
                elif t.tableType == "VIEW":
                    self.spark.sql(f"DROP VIEW IF EXISTS `{t.name}`")
                else:
                    self.spark.sql(f"DROP TABLE IF EXISTS `{t.name}`")
            except Exception:  # noqa: BLE001
                pass
        for fn in self.spark.catalog.listFunctions():
            if fn.isTemporary and fn.className and not fn.className.startswith("org.apache.spark"):
                try:
                    self.spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS `{fn.name}`")
                except Exception:  # noqa: BLE001
                    pass

    def stop(self):
        self.spark.stop()


# Spark error classes that mean "the query is not valid Spark SQL" (parse or analysis time).
SPARK_ANALYSIS_PREFIXES = ("[PARSE_SYNTAX_ERROR]", "[UNRESOLVED_", "[DATATYPE_MISMATCH", "[WRONG_NUM_ARGS",
                           "[UNSUPPORTED_", "[INVALID_", "[NOT_SUPPORTED_", "[TABLE_OR_VIEW_NOT_FOUND]",
                           "[AMBIGUOUS_", "[MISSING_", "[INCOMPATIBLE_")


def spark_rejects_statically(result):
    """True if Spark rejected the query at parse / analysis time (not a runtime failure)."""
    return (not result.ok) and result.phase == "analysis"


# ----------------------------------------------------------------------------------------------
# DuckDB worker process

def _duck_worker(conn, extension, dialect, load_extension, workdir, memory_mb=None):
    use_utc()
    os.chdir(workdir)
    if memory_mb:
        # a runaway statement fails with an allocation error instead of exhausting the machine's memory
        import resource
        limit = memory_mb * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
    import duckdb
    from harness import duck_type_to_spark, normalize_typed

    try:
        con = duckdb.connect(config={"allow_unsigned_extensions": "true",
                                     "allow_extensions_metadata_mismatch": "true"})
        if load_extension:
            con.execute(f"LOAD '{extension}'")
        con.execute("SET TimeZone = 'UTC'")
        if memory_mb:
            # DuckDB's own limit, below the process limit, so large operators spill or fail cleanly
            con.execute(f"SET memory_limit = '{max(256, memory_mb // 2)}MB'")
        if dialect:
            con.execute(f"SET current_dialect = '{dialect}'")
        conn.send(("ready", None))
    except Exception as e:  # noqa: BLE001
        conn.send(("failed", first_line(e)))
        return

    while True:
        msg = conn.recv()
        if msg is None:
            return
        sql, timeout, want_rows = msg
        timer = threading.Timer(timeout, con.interrupt) if timeout else None
        if timer:
            timer.daemon = True
            timer.start()
        start = time.perf_counter()
        stage = "bind"
        try:
            try:
                rel = con.sql(sql)
                stage = "exec"
                rows = rel.fetchall() if rel is not None else []
                types = rel.types if rel is not None else []
                names = list(rel.columns) if rel is not None else []
            except duckdb.InvalidInputException as e:
                if "Expected exactly one SQL statement" not in str(e):
                    raise
                # token rewrites such as INSERT OVERWRITE produce several statements
                stage = "exec"
                rows = con.execute(sql).fetchall()
                types = []
                names = []
            elapsed = time.perf_counter() - start
            if want_rows:
                out = [[normalize_typed(v, types[i] if i < len(types) else None) for i, v in enumerate(row)]
                       for row in rows]
            else:
                out = len(rows)
            info = {"spark_types": [duck_type_to_spark(t) for t in types], "names": names}
            reply = ("ok", out, None, [str(t) for t in types], elapsed, info)
        except Exception as e:  # noqa: BLE001
            elapsed = time.perf_counter() - start
            text = first_line(e)
            if isinstance(e, duckdb.InterruptException) or "INTERRUPT" in text.upper():
                status = "timeout"
            elif "INTERNAL" in text:
                status = "internal"
            else:
                status = "error"
            reply = (status, None, text, None, elapsed, {"exc": type(e).__name__, "stage": stage})
        finally:
            if timer:
                timer.cancel()
        conn.send(reply)
        if reply[0] == "internal":
            return  # the database is invalidated after an internal error


class DuckRunner:
    """DuckDB (+ extension) in a worker process. dialect=None keeps DuckDB's default dialect."""

    def __init__(self, dialect="spark", load_extension=True, timeout=10.0, extension=None, workdir=None,
                 memory_mb=None):
        self.dialect = dialect
        # virtual memory per worker process; the extension with debug info maps about 1 GB by itself
        self.memory_mb = memory_mb or int(os.environ.get("DUCK_WORKER_MEMORY_MB", "4096"))
        self.load_extension = load_extension
        self.timeout = timeout
        self.extension = str(extension or extension_path())
        self.workdir = workdir or scratch_dir("duck-work-")
        self.replay = []          # statements re-run after a restart (setup state)
        self.restarts = 0
        self._ctx = mp.get_context("spawn")
        self._proc = None
        self._conn = None

    # -- process management
    def _start(self):
        parent, child = self._ctx.Pipe()
        self._proc = self._ctx.Process(target=_duck_worker, daemon=True,
                                       args=(child, self.extension, self.dialect, self.load_extension, self.workdir,
                                             self.memory_mb))
        self._proc.start()
        child.close()
        self._conn = parent
        if not parent.poll(120):
            raise RuntimeError("duckdb worker did not start")
        kind, err = parent.recv()
        if kind != "ready":
            raise RuntimeError(f"duckdb worker failed to start: {err}")
        for sql in self.replay:
            self._send(sql, self.timeout, False)

    def _kill(self):
        if self._proc is not None:
            if self._proc.is_alive():
                self._proc.kill()
            self._proc.join(5)
        self._proc = None
        self._conn = None

    def restart(self):
        self._kill()
        self.restarts += 1
        self._start()

    def close(self):
        if self._conn is not None:
            try:
                self._conn.send(None)
            except (BrokenPipeError, OSError):
                pass
        if self._proc is not None:
            self._proc.join(5)
        self._kill()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- running
    def _send(self, sql, timeout, want_rows):
        self._conn.send((sql, timeout, want_rows))
        # the worker interrupts itself at `timeout`; give it a grace period before killing it
        wait = (timeout or 3600) + 3
        if not self._conn.poll(wait):
            return Result("timeout", error=f"no answer after {wait:.0f}s (killed)", seconds=wait)
        try:
            status, rows, error, types, elapsed, info = self._conn.recv()
        except (EOFError, ConnectionResetError):
            self._proc.join(5)
            code = self._proc.exitcode
            sig = -code if code is not None and code < 0 else None
            name = signal.Signals(sig).name if sig else f"exit {code}"
            return Result("crash", error=f"worker died: {name}")
        extra = {"types": types}
        extra.update(info or {})
        if error:
            extra["error_class"] = error_class_of(error)
        return Result(status, rows=rows, error=error, seconds=elapsed, extra=extra)

    def run(self, sql, timeout=None, want_rows=True):
        if self._proc is None:
            self._start()
        r = self._send(sql, timeout or self.timeout, want_rows)
        if r.status in ("crash", "timeout", "internal"):
            # crash: the process is gone; internal: the database is invalidated;
            # timeout: the interrupt may not have been honoured
            if r.status == "timeout" and self._proc is not None and self._proc.is_alive() and "killed" not in (r.error or ""):
                pass  # interrupted cleanly; the connection is still usable
            else:
                self.restart()
        return r

    def setup(self, statements):
        for sql in statements:
            r = self.run(sql)
            if not r.ok:
                raise RuntimeError(f"duckdb setup failed: {sql[:200]}: {r.error}")
            self.replay.append(sql)


# ----------------------------------------------------------------------------------------------
# DuckDB shell binary (sanitizer builds)

SANITIZER_MARKERS = ("ERROR: AddressSanitizer", "runtime error:", "ERROR: LeakSanitizer",
                     "SUMMARY: UndefinedBehaviorSanitizer", "SUMMARY: AddressSanitizer")


class ShellRunner:
    """Runs statements through a `duckdb` shell binary that has the extension statically linked.

    run_batch() sends many statements in one process, each followed by a marker query, and reports
    the first statement after which the process crashed or the sanitizer complained."""

    def __init__(self, binary, dialect="spark", timeout=60.0, workdir=None, extension=None):
        self.binary = str(binary)
        # the extension built with the same sanitizer flags as the shell (build-asan/extension/...)
        self.extension = str(extension) if extension else None
        self.dialect = dialect
        self.timeout = timeout
        self.workdir = workdir or scratch_dir("duck-shell-")
        self.env = dict(os.environ, TZ="UTC",
                        ASAN_OPTIONS="detect_leaks=0:abort_on_error=1:symbolize=1",
                        UBSAN_OPTIONS="print_stacktrace=1:halt_on_error=1")

    @staticmethod
    def _terminate(sql):
        sql = sql.rstrip()
        # an unterminated comment / string would swallow the marker; the shell needs a ';'
        return sql if sql.endswith(";") else sql + ";"

    def _script(self, statements):
        lines = [".bail off"]
        if self.extension:
            lines.append(f"LOAD '{self.extension}';")
        lines.append("SET TimeZone = 'UTC';")
        if self.dialect:
            lines.append(f"SET current_dialect = '{self.dialect}';")
        for i, sql in enumerate(statements):
            lines.append(self._terminate(sql))
            # reset the dialect-independent part of the input state: close any open quote / comment
            lines.append(f"SELECT 'MARK_{i}';")
        return "\n".join(lines) + "\n"

    def run_batch(self, statements, timeout=None):
        """Returns (status, index, detail): status ok | crash | sanitizer | timeout | internal."""
        script = self._script(statements)
        try:
            # -no-agent: the shell switches to an agent output format when AI_AGENT is set
            p = subprocess.run([self.binary, "-unsigned", "-no-agent", "-batch", "-noheader", "-csv", ":memory:"],
                               input=script.encode("utf-8", "surrogateescape"),
                               capture_output=True, timeout=timeout or self.timeout, cwd=self.workdir, env=self.env)
        except subprocess.TimeoutExpired as e:
            out = (e.stdout or b"").decode("utf-8", "replace")
            return "timeout", self._last_mark(out) + 1, "batch timed out"
        out = p.stdout.decode("utf-8", "replace")
        err = p.stderr.decode("utf-8", "replace")
        last = self._last_mark(out)
        if any(m in err for m in SANITIZER_MARKERS):
            return "sanitizer", last + 1, self._sanitizer_summary(err)
        if p.returncode < 0:
            return "crash", last + 1, f"signal {signal.Signals(-p.returncode).name}"
        if "INTERNAL Error" in err or "INTERNAL Error" in out:
            m = re.search(r"INTERNAL Error[^\n]*", err + out)
            return "internal", self._internal_index(out, err, last), m.group(0)[:300]
        return "ok", None, None

    @staticmethod
    def _last_mark(out):
        marks = re.findall(r"MARK_(\d+)", out)
        return int(marks[-1]) if marks else -1

    @staticmethod
    def _internal_index(out, err, last):
        return last + 1

    @staticmethod
    def _sanitizer_summary(err):
        lines = [ln for ln in err.splitlines() if any(m in ln for m in SANITIZER_MARKERS) or re.match(r"\s+#\d+ ", ln)]
        return "\n".join(lines[:12])[:2000]

    def run(self, sql, timeout=None):
        status, _, detail = self.run_batch([sql], timeout)
        return Result(status, error=detail)


def find_shell(build_dir):
    for cand in (Path(build_dir) / "duckdb", Path(build_dir) / "tools" / "shell" / "duckdb"):
        if cand.exists():
            return cand
    found = shutil.which("duckdb")
    return Path(found) if found else None
