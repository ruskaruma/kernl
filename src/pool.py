"""
Kernl Worker Pool — preforked bwrap sandboxes for high-throughput execution.

Eliminates per-request overhead:
  - bwrap namespace creation:  ~16ms → 0ms (created once at pool start)
  - CPython interpreter startup: ~26ms → 0ms (process stays alive)
  - Runtime module import:       ~1ms → 0ms (imported once at startup)

Worker lifecycle:
  - Workers are recycled after max_requests or when RSS exceeds max_rss_mb
  - Dead workers are detected on submit() and replaced automatically
  - State is reset between runs (sys.modules, os.environ, sys.path, /tmp)

Communication: line-delimited JSON over stdin/stdout pipes.

Note on --die-with-parent:
  bwrap's --die-with-parent uses PR_SET_PDEATHSIG, which tracks the THREAD
  that called fork(), not the process. Initial workers are forked from the
  main thread (safe). Replacement workers are forked from submit threads
  (the spawning thread persists in the thread pool, so this is also safe
  as long as the thread pool outlives the workers — which it does since the
  pool object owns both). We keep --die-with-parent for safety: if the host
  process crashes, all workers are killed by the kernel.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections import deque
from queue import Queue, Empty

from src.log import log, categorize_exit


_PYTHON_BIN = sys.executable
_RESOLV_TARGET = os.path.realpath("/etc/resolv.conf")
_SSL_CERT = None
for _p in ["/etc/ssl/certs/ca-certificates.crt",
           "/etc/pki/tls/certs/ca-bundle.crt",
           "/etc/ssl/cert.pem"]:
    if os.path.exists(_p):
        _SSL_CERT = _p
        break

_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
_WORKER_SCRIPT = os.path.join(_SRC_DIR, "worker.py")
_RUNTIME_SCRIPT = os.path.join(_SRC_DIR, "runtime.py")

# Defaults
DEFAULT_MAX_REQUESTS = 1000
DEFAULT_MAX_RSS_MB = 200  # CPython base ~77MB, so ~123MB headroom for agent work

# Active control thresholds
DEFAULT_UNHEALTHY_CONSECUTIVE = 3       # N consecutive high-RSS runs → replace
DEFAULT_TIMEOUT_STREAK = 2              # N timeouts on same worker → replace
DEFAULT_ERROR_RATE_WARN = 0.10          # 10% errors → degraded
DEFAULT_QUEUE_WAIT_WARN_MS = 100.0      # queue wait above this = pressure
_LATENCY_SAMPLE_CAP = 1024              # bounded ring buffer per metric

# Explainability
DEFAULT_TIMELINE_CAP = 20               # per-worker event history
DEFAULT_ANOMALY_WINDOW_S = 30.0         # replacement burst detection window
DEFAULT_ANOMALY_THRESHOLD = 4           # replacements in window -> anomaly


class Worker:
    """A single bwrap-sandboxed Python worker process."""

    __slots__ = ("proc", "id", "_lock")

    def __init__(self, proc: subprocess.Popen, worker_id: int):
        self.proc = proc
        self.id = worker_id
        self._lock = threading.Lock()

    def send(self, cmd: dict) -> dict:
        """Send a JSON command, read a JSON response. Thread-safe."""
        with self._lock:
            line = json.dumps(cmd, separators=(",", ":")) + "\n"
            self.proc.stdin.write(line)
            self.proc.stdin.flush()
            resp = self.proc.stdout.readline()
            if not resp:
                raise RuntimeError(f"worker {self.id}: no response (dead)")
            return json.loads(resp)

    def is_alive(self) -> bool:
        return self.proc.poll() is None

    def kill(self):
        try:
            self.proc.kill()
            self.proc.wait(timeout=2)
        except Exception:
            pass


class WorkerPool:
    """
    Pool of preforked bwrap workers with automatic lifecycle management.

    Isolation guarantees (matching run.py single-shot path):
      - PID + IPC + UTS + user + mount namespace isolation per worker
      - UID/GID 65534 (nobody:nogroup) inside user namespace
      - Network isolated by default (--unshare-net, opt-in via allow_network)
      - Seccomp BPF syscall filter (blocks ptrace, mount, unshare, etc.)
      - Read-only root filesystem
      - Hidden /home, /root, /var, /run
      - Writable /tmp only (tmpfs, cleaned between runs)
      - Clearenv (only whitelisted env vars)

    Additional guarantees from worker state reset:
      - sys.modules restored between runs (no module leakage)
      - os.environ restored between runs (no env var leakage)
      - sys.path restored between runs
      - /tmp cleaned between runs (no file leakage)

    Lifecycle:
      - Workers recycled after max_requests or RSS exceeding max_rss_mb
      - Dead workers replaced automatically on next submit()
    """

    def __init__(
        self,
        size: int,
        api_key: str = "",
        dry_run: bool = False,
        use_sandbox: bool = True,
        allow_network: bool = False,
        timeout: int = 120,
        max_requests: int = DEFAULT_MAX_REQUESTS,
        max_rss_mb: float = DEFAULT_MAX_RSS_MB,
        unhealthy_rss_mb: float = 0,  # 0 = auto (0.75 * max_rss_mb)
        unhealthy_consecutive: int = DEFAULT_UNHEALTHY_CONSECUTIVE,
        timeout_streak: int = DEFAULT_TIMEOUT_STREAK,
        error_rate_warn: float = DEFAULT_ERROR_RATE_WARN,
        queue_wait_warn_ms: float = DEFAULT_QUEUE_WAIT_WARN_MS,
        timeline_cap: int = DEFAULT_TIMELINE_CAP,
        anomaly_window_s: float = DEFAULT_ANOMALY_WINDOW_S,
        anomaly_threshold: int = DEFAULT_ANOMALY_THRESHOLD,
    ):
        self.size = size
        self._api_key = api_key
        self._dry_run = dry_run
        self._use_sandbox = use_sandbox
        self._allow_network = allow_network
        self._timeout = timeout
        self._max_requests = max_requests
        self._max_rss_mb = max_rss_mb
        self._unhealthy_rss_mb = unhealthy_rss_mb or (max_rss_mb * 0.75)
        self._unhealthy_consecutive = unhealthy_consecutive
        self._timeout_streak = timeout_streak
        self._error_rate_warn = error_rate_warn
        self._queue_wait_warn_ms = queue_wait_warn_ms
        self._timeline_cap = timeline_cap
        self._anomaly_window_s = anomaly_window_s
        self._anomaly_threshold = anomaly_threshold
        self._workers: list[Worker] = []
        self._workers_lock = threading.Lock()
        self._available: Queue[Worker] = Queue()
        self._worker_dir: str | None = None
        self._cmd: list[str] | None = None
        self._cmd_env: dict | None = None
        self._next_id = 0
        self._id_lock = threading.Lock()
        self._shutdown = False
        self._start_time: float = 0.0

        # --- Metrics ---
        self._metrics_lock = threading.Lock()
        self._m_requests = 0          # total submit() calls
        self._m_completions = 0       # status == "complete"
        self._m_errors = {            # failures by category
            "timeout": 0,
            "oom": 0,
            "seccomp": 0,
            "runtime": 0,
            "worker_death": 0,
        }
        self._m_recycles = 0          # workers recycled (max_requests/rss)
        self._m_replacements = 0      # dead workers replaced
        self._m_total_exec_ms = 0.0   # sum of worker_ms
        self._m_total_agent_ms = 0.0  # sum of agent elapsed_ms (LLM time)
        self._m_peak_rss_kb = 0       # highest RSS seen across any worker
        self._m_worker_stats: dict[int, dict] = {}  # worker_id -> per-worker stats

        # Latency samples (bounded ring buffers) for percentile computation
        self._m_lat_agent: deque[float] = deque(maxlen=_LATENCY_SAMPLE_CAP)
        self._m_lat_infra: deque[float] = deque(maxlen=_LATENCY_SAMPLE_CAP)
        self._m_lat_wait: deque[float] = deque(maxlen=_LATENCY_SAMPLE_CAP)

        # Per-agent attribution: name -> {requests, completions, failures, total_agent_ms}
        self._m_agent_stats: dict[str, dict] = {}

        # Queue-pressure signals
        self._m_pressure_hits = 0     # submits where queue wait exceeded threshold
        self._m_total_wait_ms = 0.0   # sum of queue wait times

        # Unhealthy-replacement counter (distinct from normal recycle/replace)
        self._m_unhealthy_replacements = 0

        # Explainability: reason breakdowns, anomaly burst tracking, timelines
        self._m_replacement_reasons: dict[str, int] = {}
        self._m_replacement_times: deque[float] = deque(maxlen=256)
        self._m_anomalies = 0
        self._m_timelines: dict[int, deque] = {}   # worker_id -> deque of events

    def _alloc_id(self) -> int:
        with self._id_lock:
            wid = self._next_id
            self._next_id += 1
            return wid

    def _record_event(self, wid: int, **event) -> None:
        """Append a timestamped event to a worker's ring-buffered timeline."""
        tl = self._m_timelines.setdefault(wid, deque(maxlen=self._timeline_cap))
        event["ts"] = round(time.time(), 3)
        tl.append(event)

    def _note_replacement(self, reason: str) -> None:
        """
        Record a replacement for anomaly/reason stats. Emits anomaly_detected
        when replacement rate crosses the configured window threshold.
        """
        now = time.monotonic()
        with self._metrics_lock:
            self._m_replacement_reasons[reason] = self._m_replacement_reasons.get(reason, 0) + 1
            self._m_replacement_times.append(now)
            recent = [t for t in self._m_replacement_times if now - t <= self._anomaly_window_s]
            burst = len(recent)
            if burst >= self._anomaly_threshold:
                self._m_anomalies += 1
                errors = dict(self._m_errors)
                reasons = dict(self._m_replacement_reasons)
                load = self._m_requests
                peak = self._m_peak_rss_kb
            else:
                burst = 0
        if burst:
            log("anomaly_detected", level="warn",
                replacements_in_window=burst, window_s=self._anomaly_window_s,
                reason=reason, errors=errors, replacement_reasons=reasons,
                requests=load, peak_rss_kb=peak)

    def start(self) -> float:
        """
        Start all workers. Returns startup time in ms.

        All Popen calls happen on the main thread (required by bwrap
        --die-with-parent tracking the fork()-calling thread).
        Ready-wait happens in parallel via threads.
        """
        t0 = time.monotonic()
        self._start_time = t0

        # Stage worker scripts to a temp dir (shared by all workers read-only)
        self._worker_dir = tempfile.mkdtemp(prefix="kernl_pool_")
        shutil.copy2(_WORKER_SCRIPT, os.path.join(self._worker_dir, "worker.py"))
        shutil.copy2(_RUNTIME_SCRIPT, os.path.join(self._worker_dir, "runtime.py"))

        # Build command once — identical for all workers
        if self._use_sandbox:
            self._cmd = self._build_bwrap_cmd()
            self._cmd_env = None
        else:
            self._cmd = self._build_process_cmd()
            self._cmd_env = os.environ.copy()
            self._cmd_env["PYTHONDONTWRITEBYTECODE"] = "1"
            self._cmd_env["PYTHONUNBUFFERED"] = "1"
            # KERNL_DRY_RUN is per-request, not per-worker startup

        # Phase 1: Fork all workers from the main thread (sequential, fast)
        # Each worker gets its own seccomp fd via _spawn_proc().
        procs = []
        for _ in range(self.size):
            wid = self._alloc_id()
            proc = self._spawn_proc()
            procs.append((wid, proc))

        # Phase 2: Wait for ready signals in parallel
        errors = []
        err_lock = threading.Lock()

        def _wait_ready(wid: int, proc: subprocess.Popen):
            try:
                ready_line = proc.stdout.readline()
                if not ready_line:
                    stderr = proc.stderr.read(500)
                    proc.kill()
                    with err_lock:
                        errors.append((wid, f"no ready signal: {stderr}"))
                    return
                ready = json.loads(ready_line)
                if ready.get("status") != "ready":
                    proc.kill()
                    with err_lock:
                        errors.append((wid, f"bad ready: {ready}"))
                    return
                w = Worker(proc, wid)
                with self._workers_lock:
                    self._workers.append(w)
                self._available.put(w)
            except Exception as e:
                proc.kill()
                with err_lock:
                    errors.append((wid, str(e)))

        threads = []
        for wid, proc in procs:
            t = threading.Thread(target=_wait_ready, args=(wid, proc))
            t.start()
            threads.append(t)
        for t in threads:
            t.join(timeout=10)

        if errors:
            self.shutdown()
            raise RuntimeError(f"{len(errors)} workers failed: {errors[0][1]}")

        startup_ms = (time.monotonic() - t0) * 1000
        from src.cgroup import has_cgroup
        log("pool_start", size=self.size, startup_ms=round(startup_ms, 1),
            sandbox=self._use_sandbox, network=self._allow_network,
            cgroup=has_cgroup(), timeout=self._timeout)
        return startup_ms

    def _build_bwrap_cmd(self) -> list[str]:
        """
        Build the bwrap command template for worker processes.

        Hardening (matching run.py's _build_bwrap_full_cmd):
          - User namespace: --unshare-user with uid/gid 65534 (nobody:nogroup)
          - Network: --unshare-net by default (opt-in via allow_network)
          - Seccomp: added per-spawn via _spawn_proc() (each worker needs its own fd)
        """
        env = {
            "PATH": "/usr/bin:/bin",
            "HOME": "/tmp",
            "ANTHROPIC_API_KEY": self._api_key,
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONUNBUFFERED": "1",
        }
        # KERNL_DRY_RUN is NOT set here — it's sent per-request in the
        # JSON protocol and set by the worker before each run_agent() call.
        if _SSL_CERT:
            env["SSL_CERT_FILE"] = _SSL_CERT

        cmd = ["bwrap"]

        # --- User namespace isolation ---
        cmd += ["--unshare-user"]
        cmd += ["--uid", "65534"]       # nobody
        cmd += ["--gid", "65534"]       # nogroup

        # --- Namespace isolation ---
        cmd += ["--unshare-pid", "--unshare-ipc", "--unshare-uts"]
        cmd += ["--hostname", "kernl"]
        if not self._allow_network:
            cmd += ["--unshare-net"]    # no network — only loopback interface

        # --- Filesystem ---
        cmd += ["--ro-bind", "/", "/"]
        cmd += ["--tmpfs", "/home"]
        cmd += ["--tmpfs", "/root"]
        cmd += ["--tmpfs", "/var"]
        cmd += ["--tmpfs", "/boot"]     # hide kernel images

        # Hide sensitive system config
        if os.path.isdir("/etc/ssh"):
            cmd += ["--tmpfs", "/etc/ssh"]      # hide SSH host keys
        if os.path.isdir("/sys/firmware"):
            cmd += ["--tmpfs", "/sys/firmware"]  # hide BIOS/ACPI/DMI data

        # /run: hide host runtime state, preserve DNS resolver only if network enabled
        cmd += ["--tmpfs", "/run"]
        if self._allow_network and _RESOLV_TARGET.startswith("/run/") and os.path.exists(_RESOLV_TARGET):
            cmd += ["--dir", os.path.dirname(_RESOLV_TARGET)]
            cmd += ["--ro-bind", _RESOLV_TARGET, _RESOLV_TARGET]

        cmd += [
            "--proc", "/proc",
            "--dev", "/dev",
            "--tmpfs", "/tmp",
            "--ro-bind", self._worker_dir, "/tmp/worker",
            "--die-with-parent",
            "--new-session",
        ]

        # Note: --seccomp is NOT here — it's added per-spawn in _spawn_proc()
        # because each bwrap process needs its own seccomp fd.

        cmd += ["--clearenv"]
        for key, val in env.items():
            cmd += ["--setenv", key, val]

        cmd += ["--", _PYTHON_BIN, "/tmp/worker/worker.py"]
        return cmd

    def _build_process_cmd(self) -> list[str]:
        """Fallback: unsandboxed worker for systems without bwrap."""
        return [_PYTHON_BIN, os.path.join(self._worker_dir, "worker.py")]

    def _spawn_proc(self) -> subprocess.Popen:
        """
        Create a new worker process with seccomp filter and cgroup limits.

        Each bwrap process needs its own seccomp fd (pipe is consumed on read),
        so we create one per spawn rather than baking it into the command template.

        cgroup limits (memory.max, cpu.max, pids.max) are applied via
        systemd-run if available. Falls back to no resource limits otherwise.
        """
        cmd = list(self._cmd)  # copy template
        pass_fds = ()
        seccomp_fd = -1

        if self._use_sandbox:
            from src.seccomp import create_seccomp_fd
            seccomp_fd = create_seccomp_fd()
            if seccomp_fd >= 0:
                # Insert --seccomp <fd> before the -- separator
                sep = cmd.index("--")
                cmd = cmd[:sep] + ["--seccomp", str(seccomp_fd)] + cmd[sep:]
                pass_fds = (seccomp_fd,)

        # cgroup v2 resource limits per worker
        from src.cgroup import cgroup_prefix
        WORKER_MEMORY = 256 * 1024 * 1024  # 256MB per worker
        cg_prefix = cgroup_prefix(memory_bytes=WORKER_MEMORY, cpu_percent=100, max_pids=32)
        if cg_prefix:
            cmd = cg_prefix + cmd

        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=self._cmd_env,
            pass_fds=pass_fds,
        )

        # Close our copy of the seccomp fd — bwrap has read it
        if seccomp_fd >= 0:
            os.close(seccomp_fd)

        return proc

    def _spawn_one(self) -> Worker:
        """
        Spawn a single worker and wait for its ready signal.

        Can be called from any thread. For --die-with-parent safety, the
        worker lives as long as the calling thread. Since this is called
        from ThreadPoolExecutor threads (which persist in the pool), or
        from the main thread during start(), this is safe.
        """
        wid = self._alloc_id()
        proc = self._spawn_proc()
        ready_line = proc.stdout.readline()
        if not ready_line:
            stderr = proc.stderr.read(500)
            proc.kill()
            raise RuntimeError(f"replacement worker {wid} failed: {stderr}")

        ready = json.loads(ready_line)
        if ready.get("status") != "ready":
            proc.kill()
            raise RuntimeError(f"replacement worker {wid} bad ready: {ready}")

        w = Worker(proc, wid)
        with self._workers_lock:
            self._workers.append(w)
        return w

    def _remove_worker(self, worker: Worker):
        """Remove a worker from the tracked list (does NOT kill it)."""
        with self._workers_lock:
            self._workers = [w for w in self._workers if w.id != worker.id]

    def _replace_worker(self, old: Worker, reason: str = "unknown",
                        request_id: str = "", ctx: dict | None = None) -> Worker | None:
        """
        Kill old worker, spawn replacement. Logs carry full context (rss,
        request_count, timeout_count, error_count) and correlation id so the
        chain of events leading to replacement is explainable.
        """
        self._remove_worker(old)
        ctx = ctx or {}

        # Snapshot per-worker counters for explainability
        with self._metrics_lock:
            ws = self._m_worker_stats.get(old.id, {})
            ctx.setdefault("rss_kb", ws.get("peak_rss_kb", 0))
            ctx.setdefault("request_count", ws.get("requests", 0))
            ctx.setdefault("error_count", ws.get("failures", 0))
            ctx.setdefault("timeout_streak", ws.get("timeout_streak", 0))

        # Diagnose cause of death before killing
        returncode = old.proc.poll()
        cause = None
        if returncode is not None and returncode != 0:
            cause = categorize_exit(returncode)
            stderr_snippet = ""
            try:
                stderr_snippet = (old.proc.stderr.read(512) or "").strip()
            except Exception:
                pass
            log("worker_death", level="warn", worker_id=old.id,
                request_id=request_id, returncode=returncode, cause=cause,
                reason=reason, stderr=stderr_snippet[:256], **ctx)
            with self._metrics_lock:
                if cause == "oom":
                    self._m_errors["oom"] += 1
                elif cause == "seccomp":
                    self._m_errors["seccomp"] += 1
            self._record_event(old.id, kind="death", request_id=request_id,
                               cause=cause, returncode=returncode)

        try:
            old.send({"cmd": "shutdown"})
        except Exception:
            pass
        old.kill()

        if self._shutdown:
            return None

        try:
            new = self._spawn_one()
            log("worker_replace", worker_id=new.id, replaced=old.id,
                reason=reason, request_id=request_id, cause=cause, **ctx)
            self._record_event(old.id, kind="replace", reason=reason,
                               request_id=request_id, **ctx)
            self._note_replacement(reason)
            return new
        except Exception:
            # Replacement failed — pool capacity permanently reduced
            self._note_replacement(reason)
            return None

    def submit(self, manifest: dict, input_data: dict, agent_source: str, dry_run: bool | None = None) -> dict:
        """
        Submit an agent execution to the pool.

        Blocks until a worker is available, sends the request, waits for
        the response. Handles dead workers and recycling transparently.
        """
        rid = uuid.uuid4().hex[:12]
        t_submit = time.monotonic()
        worker = self._available.get()
        wait_ms = (time.monotonic() - t_submit) * 1000

        with self._metrics_lock:
            self._m_requests += 1
            self._m_total_wait_ms += wait_ms
            self._m_lat_wait.append(wait_ms)
            if wait_ms > self._queue_wait_warn_ms:
                self._m_pressure_hits += 1
                _emit_pressure = True
            else:
                _emit_pressure = False

        if _emit_pressure:
            log("pool_pressure", level="warn", request_id=rid,
                wait_ms=round(wait_ms, 1),
                available=self._available.qsize(),
                target=self.size)

        # Check if worker is still alive before sending
        if not worker.is_alive():
            with self._metrics_lock:
                self._m_replacements += 1
                self._m_errors["worker_death"] += 1
            new = self._replace_worker(worker, reason="dead_on_checkout",
                                       request_id=rid)
            if new is None:
                return {"status": "error", "output": "worker died, replacement failed",
                        "request_id": rid}
            worker = new

        # Resolve dry_run: explicit param > pool default
        use_dry_run = dry_run if dry_run is not None else self._dry_run

        try:
            result = worker.send({
                "cmd": "run",
                "manifest": manifest,
                "input_data": input_data,
                "agent_source": agent_source,
                "dry_run": use_dry_run,
                "timeout": self._timeout,
            })
        except Exception as e:
            # Worker died mid-request
            with self._metrics_lock:
                self._m_replacements += 1
                self._m_errors["worker_death"] += 1
            new = self._replace_worker(worker, reason="died_mid_request",
                                       request_id=rid)
            if new:
                self._available.put(new)
            return {"status": "error", "output": f"worker {worker.id} died: {e}",
                    "request_id": rid}

        # --- Track metrics from result ---
        status = result.get("status", "unknown")
        worker_ms = result.get("_worker_ms", 0)
        agent_ms = result.get("elapsed_ms", 0)
        rss_kb = result.get("_rss_kb", 0)
        peak_rss_kb = result.get("_peak_rss_kb", 0)
        req_count = result.get("_request_count", 0)

        rss_mb = rss_kb / 1024
        infra_ms_val = max(worker_ms - agent_ms, 0.0) if worker_ms and agent_ms else 0.0
        agent_name = manifest.get("agent", {}).get("name", "?")

        with self._metrics_lock:
            self._m_total_exec_ms += worker_ms
            self._m_total_agent_ms += agent_ms
            self._m_lat_agent.append(agent_ms)
            self._m_lat_infra.append(infra_ms_val)
            if peak_rss_kb > self._m_peak_rss_kb:
                self._m_peak_rss_kb = peak_rss_kb

            if status == "complete":
                self._m_completions += 1
            elif status == "timeout":
                self._m_errors["timeout"] += 1
            elif status == "error":
                self._m_errors["runtime"] += 1

            # Per-worker stats (including consecutive-high-RSS and timeout streak)
            ws = self._m_worker_stats.setdefault(worker.id, {
                "requests": 0, "failures": 0, "peak_rss_kb": 0,
                "consecutive_high_rss": 0, "timeout_streak": 0,
            })
            ws["requests"] += 1
            if status != "complete":
                ws["failures"] += 1
            if peak_rss_kb > ws["peak_rss_kb"]:
                ws["peak_rss_kb"] = peak_rss_kb

            # High-RSS streak (early-warning below max_rss_mb)
            if rss_mb > self._unhealthy_rss_mb:
                ws["consecutive_high_rss"] += 1
            else:
                ws["consecutive_high_rss"] = 0

            # Timeout streak per worker
            if status == "timeout":
                ws["timeout_streak"] += 1
            else:
                ws["timeout_streak"] = 0

            unhealthy_rss = ws["consecutive_high_rss"] >= self._unhealthy_consecutive
            unhealthy_timeout = ws["timeout_streak"] >= self._timeout_streak

            # Per-agent attribution
            ag = self._m_agent_stats.setdefault(agent_name, {
                "requests": 0, "completions": 0, "failures": 0,
                "total_agent_ms": 0.0, "total_infra_ms": 0.0,
            })
            ag["requests"] += 1
            ag["total_agent_ms"] += agent_ms
            ag["total_infra_ms"] += infra_ms_val
            if status == "complete":
                ag["completions"] += 1
            else:
                ag["failures"] += 1

        # Log execution with correlation id and record timeline entry
        log("agent_exec", request_id=rid, worker_id=worker.id, status=status,
            worker_ms=round(worker_ms, 1), agent_ms=round(agent_ms, 1),
            infra_ms=round(infra_ms_val, 1), rss_kb=rss_kb,
            request_count=req_count, agent=agent_name)
        self._record_event(worker.id, kind="exec", request_id=rid,
                           status=status, agent=agent_name,
                           worker_ms=round(worker_ms, 1), rss_kb=rss_kb)

        result["request_id"] = rid

        ctx = {
            "rss_kb": rss_kb,
            "request_count": req_count,
            "error_count": ws["failures"],
            "timeout_streak": ws["timeout_streak"],
        }

        # Decide whether this worker needs to be removed:
        # 1. Standard recycle (request count / hard RSS limit)
        # 2. Unhealthy: consecutive high RSS below hard limit
        # 3. Unhealthy: repeated timeouts (adaptive/aggressive recycle)
        if req_count >= self._max_requests or rss_mb > self._max_rss_mb:
            recycle_reason = "max_requests" if req_count >= self._max_requests else "max_rss"
            log("worker_recycle", request_id=rid, worker_id=worker.id,
                reason=recycle_reason, **ctx)
            with self._metrics_lock:
                self._m_recycles += 1
            new = self._replace_worker(worker, reason=recycle_reason,
                                       request_id=rid, ctx=dict(ctx))
            if new:
                self._available.put(new)
        elif unhealthy_rss:
            log("worker_unhealthy", level="warn", request_id=rid,
                worker_id=worker.id, reason="consecutive_high_rss",
                threshold_mb=round(self._unhealthy_rss_mb, 1),
                streak=ws["consecutive_high_rss"], **ctx)
            with self._metrics_lock:
                self._m_unhealthy_replacements += 1
            new = self._replace_worker(worker, reason="unhealthy_rss",
                                       request_id=rid, ctx=dict(ctx))
            if new:
                self._available.put(new)
        elif unhealthy_timeout:
            log("worker_unhealthy", level="warn", request_id=rid,
                worker_id=worker.id, reason="timeout_streak",
                streak=ws["timeout_streak"], **ctx)
            with self._metrics_lock:
                self._m_unhealthy_replacements += 1
            new = self._replace_worker(worker, reason="timeout_streak",
                                       request_id=rid, ctx=dict(ctx))
            if new:
                self._available.put(new)
        else:
            self._available.put(worker)

        return result

    def ping_all(self) -> tuple[int, int]:
        """Ping all workers. Returns (alive, dead) counts."""
        alive = dead = 0
        with self._workers_lock:
            workers = list(self._workers)
        for w in workers:
            try:
                r = w.send({"cmd": "ping"})
                if r.get("status") == "ok":
                    alive += 1
                else:
                    dead += 1
            except Exception:
                dead += 1
        return alive, dead

    def stats(self) -> dict:
        """Return pool statistics (backward-compatible)."""
        with self._metrics_lock:
            return {
                "target_size": self.size,
                "active_workers": len(self._workers),
                "available": self._available.qsize(),
                "recycled": self._m_recycles,
                "replaced": self._m_replacements,
                "unhealthy_replaced": self._m_unhealthy_replacements,
                "pressure_hits": self._m_pressure_hits,
            }

    @staticmethod
    def _percentiles(samples) -> dict:
        """
        Compute p50/p95/p99 from a bounded sample buffer.

        Returns zeros for empty input. Uses simple index-based nearest-rank;
        for a few thousand samples this is plenty accurate without numpy.
        """
        if not samples:
            return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "count": 0}
        s = sorted(samples)
        n = len(s)

        def _at(p: float) -> float:
            idx = min(n - 1, max(0, int(round(p * (n - 1)))))
            return round(s[idx], 2)

        return {"p50": _at(0.50), "p95": _at(0.95), "p99": _at(0.99), "count": n}

    def _compute_health_score(self, total_req: int, total_errors: int,
                              timeouts: int, alive: int) -> int:
        """
        Lightweight 0-100 health score.

        Signals (each clamped, summed, subtracted from 100):
          - error rate:    up to -40
          - timeout rate:  up to -30
          - churn rate:    up to -20 (recycles + replacements per request)
          - availability:  up to -20 (per missing worker vs target)
        """
        score = 100
        if total_req > 0:
            error_rate = total_errors / total_req
            timeout_rate = timeouts / total_req
            churn_rate = (self._m_recycles + self._m_replacements
                          + self._m_unhealthy_replacements) / total_req
            score -= min(40, int(error_rate * 200))
            score -= min(30, int(timeout_rate * 300))
            score -= min(20, int(churn_rate * 100))
        if self.size > 0 and alive < self.size:
            score -= int(20 * (self.size - alive) / self.size)
        return max(0, min(100, score))

    def health(self) -> dict:
        """
        Return structured system state for monitoring.

        Includes: pool status, worker counts, all metrics, error breakdown,
        per-worker stats, isolation config, and uptime.
        """
        with self._workers_lock:
            workers = list(self._workers)
        alive = sum(1 for w in workers if w.is_alive())

        with self._metrics_lock:
            total_req = self._m_requests
            avg_exec = round(self._m_total_exec_ms / total_req, 1) if total_req else 0
            avg_agent = round(self._m_total_agent_ms / total_req, 1) if total_req else 0
            avg_wait = round(self._m_total_wait_ms / total_req, 1) if total_req else 0
            total_errors = sum(self._m_errors.values())

            error_rate = total_errors / total_req if total_req else 0
            timeout_rate = self._m_errors["timeout"] / total_req if total_req else 0

            metrics = {
                "requests": total_req,
                "completions": self._m_completions,
                "errors": dict(self._m_errors),
                "error_total": total_errors,
                "error_rate": round(error_rate, 4),
                "timeout_rate": round(timeout_rate, 4),
                "recycles": self._m_recycles,
                "replacements": self._m_replacements,
                "unhealthy_replacements": self._m_unhealthy_replacements,
                "avg_exec_ms": avg_exec,
                "avg_agent_ms": avg_agent,
                "avg_infra_ms": round(avg_exec - avg_agent, 1) if avg_exec else 0,
                "avg_wait_ms": avg_wait,
                "peak_rss_kb": self._m_peak_rss_kb,
                "pressure_hits": self._m_pressure_hits,
            }
            percentiles = {
                "agent_ms": self._percentiles(self._m_lat_agent),
                "infra_ms": self._percentiles(self._m_lat_infra),
                "queue_wait_ms": self._percentiles(self._m_lat_wait),
            }
            worker_stats = {
                str(wid): dict(ws)
                for wid, ws in self._m_worker_stats.items()
            }
            agent_stats: dict[str, dict] = {}
            for name, ag in self._m_agent_stats.items():
                n = ag["requests"]
                agent_stats[name] = {
                    "requests": n,
                    "completions": ag["completions"],
                    "failures": ag["failures"],
                    "success_rate": round(ag["completions"] / n, 4) if n else 0,
                    "avg_agent_ms": round(ag["total_agent_ms"] / n, 1) if n else 0,
                    "avg_infra_ms": round(ag["total_infra_ms"] / n, 1) if n else 0,
                }
            score = self._compute_health_score(
                total_req, total_errors, self._m_errors["timeout"], alive
            )

            # Explainability: replacement-reason breakdown + top causes + timelines
            replacement_reasons = dict(self._m_replacement_reasons)
            anomalies = self._m_anomalies
            timelines = {
                str(wid): list(tl) for wid, tl in self._m_timelines.items()
            }
            nonzero_errors = {k: v for k, v in self._m_errors.items() if v}
            top_error = max(nonzero_errors.items(), key=lambda kv: kv[1])[0] if nonzero_errors else None
            top_replace = max(replacement_reasons.items(), key=lambda kv: kv[1])[0] if replacement_reasons else None
            top_causes = {
                "top_error_category": top_error,
                "top_replacement_reason": top_replace,
                "summary": (
                    f"most replacements due to {top_replace}" if top_replace
                    else "no replacements recorded"
                ),
            }

        # Status: blend availability and error rate.
        if alive == 0:
            status = "down"
        elif alive < self.size:
            status = "degraded"
        elif total_req > 0 and error_rate >= self._error_rate_warn:
            status = "degraded"
        else:
            status = "healthy"

        uptime = time.monotonic() - self._start_time if self._start_time else 0

        from src.cgroup import has_cgroup
        return {
            "status": status,
            "score": score,
            "uptime_s": round(uptime, 1),
            "workers": {
                "target": self.size,
                "alive": alive,
                "dead": len(workers) - alive,
                "available": self._available.qsize(),
            },
            "metrics": metrics,
            "percentiles": percentiles,
            "per_worker": worker_stats,
            "per_agent": agent_stats,
            "timelines": timelines,
            "replacement_reasons": replacement_reasons,
            "anomalies": anomalies,
            "top_causes": top_causes,
            "thresholds": {
                "unhealthy_rss_mb": round(self._unhealthy_rss_mb, 1),
                "unhealthy_consecutive": self._unhealthy_consecutive,
                "timeout_streak": self._timeout_streak,
                "error_rate_warn": self._error_rate_warn,
                "queue_wait_warn_ms": self._queue_wait_warn_ms,
            },
            "isolation": {
                "sandbox": self._use_sandbox,
                "network": self._allow_network,
                "seccomp": self._use_sandbox,
                "cgroup": has_cgroup(),
                "timeout_s": self._timeout,
            },
        }

    def shutdown(self):
        """Gracefully shut down all workers."""
        self._shutdown = True
        with self._workers_lock:
            workers = list(self._workers)
            self._workers.clear()
        for w in workers:
            try:
                w.send({"cmd": "shutdown"})
            except Exception:
                pass
            w.kill()
        # Drain the queue
        while not self._available.empty():
            try:
                self._available.get_nowait()
            except Empty:
                break
        if self._worker_dir:
            shutil.rmtree(self._worker_dir, ignore_errors=True)
            self._worker_dir = None

        uptime = time.monotonic() - self._start_time if self._start_time else 0
        with self._metrics_lock:
            log("pool_shutdown", uptime_s=round(uptime, 1),
                requests=self._m_requests, completions=self._m_completions,
                errors=sum(self._m_errors.values()))

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *a):
        self.shutdown()

    def __del__(self):
        if not self._shutdown:
            self.shutdown()
