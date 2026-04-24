# Kernl Technical Report

## System Definition

Kernl is a 3,000-line Python runtime for executing AI agents inside Linux user-namespace sandboxes. It provides three execution modes with different isolation/performance tradeoffs, a preforked worker pool for high-throughput batch execution, and a minimal agent loop that runs entirely on the Python standard library with zero external dependencies inside the sandbox.

The core value proposition: bwrap namespace isolation at 70ms overhead per agent (1.1% of real API latency), or 1ms with preforked workers (0.0%).

---

## Part 1: Architecture and Current State

### End-to-End Execution Pipeline

```
agent.py                    (user-authored Python class with @agent/@tool decorators)
    |
    v
manifest.py:parse_agent_file()   (AST analysis — no execution, extracts tools/state/config)
    |
    v
build.py:build()                  (packages manifest.json + agent.py + runtime.py → .kb tar.gz)
    |
    v
run.py:run()                      (extracts bundle, selects isolation, spawns subprocess)
    |                              or
pool.py:WorkerPool.submit()       (sends JSON command to preforked bwrap worker)
    |
    v
runtime.py:run_agent()            (agent loop: LLM call → tool dispatch → repeat)
    |
    v
Anthropic API (v1/messages)       (or mock LLM in dry-run mode)
```

**Key design constraint:** `runtime.py` runs INSIDE the sandbox. It cannot import anything outside the standard library. The entire agent framework — LLM client, tool compiler, agent loop — is ~370 lines of stdlib Python.

### Bundle Format (.kb)

A `.kb` file is a gzipped tar containing exactly three files:

| File | Purpose |
|------|---------|
| `manifest.json` | Agent config, tool schemas, tool source, content hash, memory budget |
| `agent.py` | Original agent source (copied verbatim) |
| `runtime.py` | Injected executor (identical across all agents) |

Content hash = SHA256(manifest_json + agent_source)[:16]. Bundle cache key = filename + size + mtime.

### Execution Modes

**Mode 1: Process (no sandbox)**
- Direct subprocess: `python3 runtime.py manifest.json '<input>'`
- Environment: `os.environ.copy()` with whitelisted vars
- Isolation: none (resource limits via `setrlimit` for memory/CPU/fds)
- Overhead: ~45ms (CPython startup + runtime import)
- Use case: development, trusted agents

**Mode 2: bwrap (sandboxed)**
- Full user-namespace isolation via bubblewrap
- PID namespace: agent is PID 1, cannot see host processes
- IPC namespace: isolated System V IPC
- UTS namespace: hostname = "kernl"
- Mount namespace: read-only `/`, hidden `/home /root /var /run`, writable `/tmp` only
- Network: shared by default (opt-in `--unshare-net` via `disable_network=True`)
- Clearenv: only PATH, HOME, ANTHROPIC_API_KEY, PYTHONDONTWRITEBYTECODE, PYTHONUNBUFFERED, SSL_CERT_FILE, KERNL_DRY_RUN
- `--die-with-parent`: kernel kills worker if host dies (via PR_SET_PDEATHSIG)
- `--new-session`: detached from controlling terminal
- Overhead: ~75ms (bwrap namespace creation + CPython startup)
- Use case: untrusted agents, production single-shot execution

**Mode 3: Pool (preforked workers)**
- Long-lived Python processes inside bwrap sandboxes
- Runtime imported once at startup (CPython cost paid once)
- Communication: line-delimited JSON over stdin/stdout pipes
- State reset between runs: sys.modules, os.environ, sys.path, /tmp cleaned
- Worker lifecycle: recycled after max_requests (default 1000) or RSS > max_rss_mb (default 200MB)
- Dead workers replaced automatically on next submit()
- Overhead: ~1ms per request (JSON serialize + pipe write + pipe read)
- Pool startup: ~100-260ms for 32 workers (one-time cost)
- Use case: batch execution, high-throughput multi-tenant

### Performance Characteristics

**Dry-run benchmark (mock LLM, n=100, 32 threads):**

| Method | p50 | Throughput | Success |
|--------|-----|-----------|---------|
| process (no sandbox) | 45ms | 546/s | 100% |
| bwrap (sandboxed) | 75ms | 383/s | 100% |
| pool (steady state) | 0.6ms | 7,436/s | 100% |
| pool (amortized) | 1.9ms | 668/s | 100% |

**Real API benchmark (claude-sonnet, 2-step tool loop, 5 runs each):**

| Method | Avg Total | Avg LLM | Avg Infra | Infra % |
|--------|-----------|---------|-----------|---------|
| process (no sandbox) | 7,307ms | 7,250ms | 58ms | 0.8% |
| bwrap (sandboxed) | 6,596ms | 6,526ms | 70ms | 1.1% |
| pool (preforked) | 7,646ms | 7,645ms | 1ms | 0.0% |

**Spawn overhead (trivial workload, n=100):**

| Method | p50 | vs Docker |
|--------|-----|-----------|
| subprocess (bare) | 29ms | — |
| bwrap (full ns) | 67ms | 32x faster |
| Docker | 2,139ms | baseline |

### Isolation Guarantees

**What IS isolated (bwrap and pool modes):**
- Filesystem: read-only root, agent can only write to /tmp
- Process visibility: agent cannot see host PIDs
- IPC: isolated System V IPC namespace
- Hostname: fixed to "kernl"
- Environment: clearenv, only whitelisted variables
- /home, /root, /var, /run: hidden (tmpfs overlays)

**What is NOT isolated:**
- Network: shared by default (agent can make arbitrary outbound connections)
- UID/GID: runs as calling user inside namespace (no user namespace mapping)
- Syscalls: no seccomp filter (agent can call any syscall the kernel allows)
- cgroups: no memory/CPU limits enforced at container level (only setrlimit for process mode)
- /proc: mounted but readable (agent can read /proc/self/status, /proc/meminfo, etc.)

**Pool-specific state isolation:**
- sys.modules restored (agent-imported modules removed between runs)
- os.environ restored from startup snapshot
- sys.path restored from startup snapshot
- /tmp cleaned (agent-written files removed, /tmp/worker bind mount preserved)
- Tool executors: fresh namespace per run (build_tool_executor creates new dict)

**Known gaps:**
- C extensions with global state survive module removal (removed from sys.modules but C state persists in process memory)
- signal handlers set by agents persist (not reset between runs)
- threading.Thread objects spawned by agents persist (no thread cleanup)
- /dev/shm is shared (no tmpfs overlay)
- atexit handlers registered by agents persist

### Worker Pool Design

```
WorkerPool                          Worker (inside bwrap)
┌─────────────────┐                ┌──────────────────────┐
│ Queue[Worker]    │  stdin JSON   │ runtime.py (imported) │
│ ┌───┐ ┌───┐     │ ──────────>   │ _INITIAL_MODULES      │
│ │ W0│ │ W1│ ... │               │ _INITIAL_ENVIRON      │
│ └───┘ └───┘     │  stdout JSON  │ _INITIAL_PATH         │
│ _workers_lock   │ <──────────   │                        │
│ _available queue│               │ handle_run():          │
│ stats counters  │               │   set dry_run env      │
└─────────────────┘               │   run_agent()          │
                                  │   _reset_state()       │
                                  │   return result + meta │
                                  └──────────────────────┘
```

**Lifecycle:**

1. `start()`: fork all workers from main thread (sequential Popen), wait for ready signals in parallel threads. Main thread requirement: bwrap --die-with-parent uses PR_SET_PDEATHSIG which tracks the fork()-calling thread.

2. `submit()`: pull worker from Queue (blocking), check alive, send JSON command, wait for JSON response, check recycling thresholds, return worker to queue (or recycle).

3. Recycling: after each submit, check `_request_count >= max_requests` or `_rss_kb/1024 > max_rss_mb`. If triggered: kill old worker, spawn replacement, return replacement to queue. Old worker's result is still returned.

4. Dead worker detection: `is_alive()` check before sending. If dead: `_replace_worker()`, increment `_replaced` counter. If replacement fails: pool capacity permanently reduced.

5. `shutdown()`: send shutdown command to all workers, kill all, drain queue, clean temp dir.

### Test Suite

14 tests covering:
- Basic execution: no-sandbox, bwrap, pool execution, multiple requests, ping
- State isolation: env vars, request counting, per-request dry_run switching
- Worker recycling: count-based recycling, capacity preservation
- Dead worker replacement: kill/detect/replace, stats tracking
- Bundle cache: manifest correctness, idempotency

Run: `python3 tests/test_kernl.py`

### Codebase

| Component | File | Lines | Purpose |
|-----------|------|-------|---------|
| CLI | `kernl` | 203 | build, run, inspect, exec commands |
| Runner | `src/run.py` | 557 | isolation detection, bwrap command building, bundle cache, execution |
| Pool | `src/pool.py` | 442 | preforked worker management, lifecycle, recycling |
| Worker | `src/worker.py` | 187 | sandbox-side executor, state reset, JSON protocol |
| Runtime | `src/runtime.py` | 370 | agent loop, LLM client, tool compiler |
| Manifest | `src/manifest.py` | 146 | AST-based agent parsing |
| Build | `src/build.py` | 114 | .kb bundle packaging |
| Tests | `tests/test_kernl.py` | 316 | correctness test suite |
| Benchmark | `bench/run_benchmark.py` | 510 | dry-run performance |
| Real API bench | `bench/real_api_benchmark.py` | 190 | real API overhead measurement |
| **Total** | | **3,035** | |

---

## Part 2: Production Evolution Design

### 2.1 Isolation Hardening

#### 2.1.1 Seccomp Syscall Filtering

**Current state:** No syscall filtering. Agent code can call any syscall the kernel allows inside the namespace.

**Change:** Add `--seccomp` flag to bwrap with a BPF filter whitelist.

**Implementation:**
```
Allowed syscalls (whitelist):
  read, write, open, openat, close, stat, fstat, lstat, poll
  mmap, mprotect, munmap, brk, mremap
  ioctl (TIOCGWINSZ only), access, pipe, pipe2
  dup, dup2, dup3, fcntl
  socket, connect, sendto, recvfrom, sendmsg, recvmsg
  bind, listen, accept (for tool HTTP clients)
  setsockopt, getsockopt, getpeername, getsockname
  clone (CLONE_VM|CLONE_FS|CLONE_FILES|CLONE_SIGHAND — threads only)
  execve (blocked — prevent shell escape)
  wait4, exit, exit_group
  uname, getpid, gettid, getuid, getgid
  futex, nanosleep, clock_gettime, clock_nanosleep
  rt_sigaction, rt_sigprocmask, rt_sigreturn
  getcwd, chdir, readlink, readlinkat
  getdents64, mkdir, rmdir, unlink, rename
  getrandom
  
Blocked (EPERM):
  execve, execveat          — no shell escape
  ptrace                    — no debugging/injection
  mount, umount2            — no filesystem manipulation
  reboot, syslog            — no system control
  keyctl, request_key       — no kernel keyring
  pivot_root, chroot        — no namespace escape
  personality               — no execution domain change
  kexec_load, init_module   — no kernel module loading
```

bwrap supports `--seccomp <fd>` where the fd points to a compiled BPF program. The filter is compiled once at pool startup using the `seccomp` module (or raw BPF bytecode) and passed to each worker.

**Why it matters:** Without seccomp, a malicious agent tool can call `execve("/bin/sh", ...)` to escape the Python sandbox, or use `ptrace` to inject into the worker process. The namespace isolates the filesystem view but not the syscall surface.

**Integration:** Add to `_build_bwrap_full_cmd()` in `run.py` and `_build_bwrap_cmd()` in `pool.py`. Compile the BPF filter once at module load. Pass via `--seccomp` with a pipe fd.

#### 2.1.2 Cgroups (Memory, CPU Limits)

**Current state:** Process mode uses `setrlimit()` for memory/CPU. Bwrap and pool modes have no resource limits.

**Change:** Create a cgroup per worker (or per pool) with hard memory and CPU limits.

**Implementation:**
```
For pool workers (cgroup v2):
  /sys/fs/cgroup/kernl/pool_{pool_id}/
    memory.max = max_rss_mb * 1024 * 1024
    memory.swap.max = 0                    # no swap
    cpu.max = "100000 100000"              # 100% of one core (adjustable)
    pids.max = 32                          # limit fork bombs

For single-shot workers:
  /sys/fs/cgroup/kernl/run_{hash}/
    memory.max = memory_budget.total_bytes
    cpu.max = "100000 100000"
    pids.max = 16
```

Requires: cgroup v2 (unified hierarchy), write access to `/sys/fs/cgroup/`. On systems without cgroup write access, fall back to `setrlimit()`.

**Why it matters:** `setrlimit` is per-process and trivially bypassable by forking. A cgroup limit is enforced by the kernel across all processes in the group — the OOM killer terminates the agent if it exceeds memory.max. This prevents a single agent from consuming all host memory.

**Integration:** Add `_setup_cgroup()` and `_cleanup_cgroup()` to `run.py`. Pool creates one cgroup at startup; workers are added via `echo $PID > cgroup.procs`. Single-shot creates/destroys per run.

#### 2.1.3 Network Isolation by Default

**Current state:** Network is shared. Agents can make arbitrary outbound connections. `disable_network=False` is the default.

**Change:** Default to `--unshare-net` (no network). Provide explicit opt-in for agents that need it.

**Implementation:**
```python
# In manifest.py — add network permission to @agent decorator
@agent(name="researcher", model="...", network=True)

# In manifest.json
"agent": { ..., "network": true }

# In run.py — default to network=False
disable_network = not manifest["agent"].get("network", False)
```

For agents that declare `network=True`, bind-mount `/etc/resolv.conf` and keep the network namespace shared. For all others, `--unshare-net` creates an isolated network namespace with only a loopback interface.

**Why it matters:** The API key is passed as an environment variable. An agent with network access could exfiltrate it to an arbitrary endpoint. Network isolation by default limits the blast radius of compromised tool code.

**Integration:** Add `network` field to `AgentManifest`. Parse in `manifest.py`. Pass through `build.py` to manifest.json. Read in `run.py` and `pool.py` to set `--unshare-net` or not.

#### 2.1.4 UID/GID Namespace Isolation

**Current state:** Agent runs as the calling user inside the namespace. No UID/GID remapping.

**Change:** Add `--unshare-user` with UID/GID mapping to create a dedicated user namespace.

**Implementation:**
```
bwrap --unshare-user --uid 65534 --gid 65534  # nobody:nogroup
```

This makes the agent run as `nobody` inside the namespace, mapped to the calling user outside. Files owned by the host user appear owned by nobody inside the sandbox. The agent cannot write to any host-owned files (because the filesystem is read-only anyway), but this adds defense-in-depth.

**Why it matters:** If a future bug allows write access to the host filesystem, running as nobody prevents modification of files owned by the host user. It also prevents the agent from reading files that are group-restricted to the host user's groups.

**Integration:** Add `--unshare-user --uid 65534 --gid 65534` to bwrap commands. Verify compatibility with `--die-with-parent` (PR_SET_PDEATHSIG requires the same UID or CAP_KILL — this works because the user namespace maps the calling user's UID).

### 2.2 Execution Model

#### 2.2.1 Worker Pool: Timeouts

**Current state:** `worker.send()` blocks indefinitely waiting for a response. If a worker hangs (infinite loop in agent tool), the calling thread blocks forever.

**Change:** Add per-request timeout to the worker protocol.

**Implementation:**
```python
# In Worker.send():
def send(self, cmd: dict, timeout: float = 120) -> dict:
    with self._lock:
        line = json.dumps(cmd, separators=(",", ":")) + "\n"
        self.proc.stdin.write(line)
        self.proc.stdin.flush()
        
        # Use select() for timeout on stdout
        import select
        ready, _, _ = select.select([self.proc.stdout], [], [], timeout)
        if not ready:
            # Worker hung — kill and raise
            self.proc.kill()
            raise TimeoutError(f"worker {self.id}: no response in {timeout}s")
        
        resp = self.proc.stdout.readline()
        if not resp:
            raise RuntimeError(f"worker {self.id}: no response (dead)")
        return json.loads(resp)
```

In `submit()`, catch `TimeoutError`, kill the worker, spawn replacement, return timeout error result.

**Why it matters:** A single hung worker permanently blocks one thread and one pool slot. With timeouts, hung workers are killed and replaced, preserving pool capacity.

#### 2.2.2 Worker Pool: Backpressure

**Current state:** `submit()` blocks on `self._available.get()` with no timeout. If all workers are busy, callers block indefinitely.

**Change:** Add queue timeout and rejection.

**Implementation:**
```python
def submit(self, ..., queue_timeout: float = 30) -> dict:
    try:
        worker = self._available.get(timeout=queue_timeout)
    except Empty:
        return {"status": "error", "output": "pool overloaded: no worker available",
                "_queue_depth": self.size - self._available.qsize()}
```

**Why it matters:** Without backpressure, a burst of requests fills the queue and all subsequent callers block. With backpressure, callers get an immediate rejection they can handle (retry, shed load, alert).

#### 2.2.3 Worker Pool: Autoscaling

**Current state:** Pool size is fixed at construction. Cannot grow or shrink.

**Change:** Add min/max bounds and load-based scaling.

**Implementation:**
```python
class WorkerPool:
    def __init__(self, min_size=2, max_size=32, scale_threshold=0.8):
        # If available/total < (1 - scale_threshold), spawn more workers
        # If available/total > scale_threshold for 60s, kill idle workers
```

Scale-up: when `_available.qsize() / len(_workers) < 0.2` for 5 consecutive submits, spawn workers up to `max_size`. Scale-down: background thread checks every 60s, kills idle workers down to `min_size`.

**Why it matters:** Fixed pool wastes memory during low load and bottlenecks during high load. Autoscaling matches capacity to demand.

#### 2.2.4 Failure Recovery and Monitoring

**Current state:** Dead workers detected reactively on submit(). Replacement failure permanently reduces pool capacity. No alerting.

**Change:** Add a background health-check thread and capacity tracking.

**Implementation:**
```python
def _health_loop(self):
    """Background thread: periodic health check every 10s."""
    while not self._shutdown:
        time.sleep(10)
        with self._workers_lock:
            for w in self._workers:
                if not w.is_alive():
                    self._replace_worker(w)
        # Check capacity
        if len(self._workers) < self.size:
            deficit = self.size - len(self._workers)
            self._emit("pool.capacity_deficit", deficit)
```

Add `_emit()` method for structured event emission (see observability section below).

**Why it matters:** Reactive detection means a dead worker wastes a pool slot until the next submit(). Proactive detection keeps the pool at target capacity. Capacity deficit alerting prevents silent degradation.

#### 2.2.5 Safe State Reset Guarantees

**Current state:** `_reset_state()` covers sys.modules, os.environ, sys.path, /tmp. Does not cover signal handlers, threading, atexit, C extension globals, /dev/shm.

**Change:** Add signal handler reset and thread cleanup.

**Implementation:**
```python
def _reset_state():
    # ... existing resets ...
    
    # 5. Reset signal handlers to defaults
    import signal
    for sig in (signal.SIGALRM, signal.SIGUSR1, signal.SIGUSR2):
        try:
            signal.signal(sig, signal.SIG_DFL)
        except (OSError, ValueError):
            pass
    
    # 6. Kill lingering threads (best-effort)
    import threading
    main_thread = threading.main_thread()
    for t in threading.enumerate():
        if t is not main_thread and t.daemon:
            # Can't kill non-daemon threads safely, but daemon threads
            # will die when the process exits
            pass
    
    # 7. Clear atexit handlers
    import atexit
    atexit._clear()
```

**Why it matters:** Signal handlers and atexit registrations persist across runs. A malicious agent could register a SIGALRM handler that fires during the next agent's execution, or an atexit handler that runs during worker shutdown.

### 2.3 Agent Model

#### 2.3.1 Stronger Tool Schema

**Current state:** Tool parameters extracted from Python type hints (str, int, float, bool). No validation of inputs from the LLM. The LLM can pass any JSON to any tool.

**Change:** Generate full JSON Schema with type constraints and validate inputs before execution.

**Implementation:**
```python
# In manifest.py — enhance parameter extraction
def _python_type_to_schema(annotation) -> dict:
    mapping = {
        "str": {"type": "string"},
        "int": {"type": "integer"},
        "float": {"type": "number"},
        "bool": {"type": "boolean"},
        "list": {"type": "array"},
        "dict": {"type": "object"},
    }
    # Also handle: list[str], Optional[int], Literal["a","b"], etc.

# In runtime.py — validate before execution
def execute_tool(executors, tool_name, tool_input, tool_schema) -> str:
    # Validate tool_input against tool_schema before calling
    errors = _validate_json_schema(tool_input, tool_schema)
    if errors:
        return f"VALIDATION ERROR: {errors}"
    return str(executors[tool_name](**tool_input))
```

**Why it matters:** The LLM sometimes passes wrong types (string instead of int, missing required fields). Without validation, the tool crashes with a Python TypeError. With validation, the error message is structured and the LLM can self-correct.

#### 2.3.2 Input Validation

**Current state:** Agent state fields (input) are passed as a flat dict. No validation that required fields are present or have correct types.

**Change:** Validate input_data against agent state field declarations before execution.

**Implementation:**
```python
# In runtime.py:run_agent()
state_fields = manifest["agent"].get("state_fields", [])
for field in state_fields:
    if field["name"] not in input_data:
        return {"status": "error", "output": f"missing input field: {field['name']}"}
```

#### 2.3.3 Execution Constraints

**Current state:** max_steps limits LLM calls. No limit on tool execution time, tool output size, or total token usage.

**Change:** Add configurable constraints.

**Implementation:**
```python
@agent(
    name="researcher",
    model="claude-sonnet-4-20250514",
    max_steps=10,
    max_tool_time_seconds=30,      # per-tool execution timeout
    max_tool_output_bytes=100_000,  # truncate tool output
    max_total_tokens=50_000,        # stop if token budget exceeded
)
```

Enforce in runtime.py by wrapping `execute_tool()` with `signal.alarm()` (or threading.Timer) and checking token usage from API responses.

### 2.4 Observability

#### 2.4.1 Structured Logging

**Current state:** No logging. Errors are returned as result dicts. Debugging requires reading process stderr.

**Change:** Add structured JSON logging to stderr (inside sandbox) and to a configurable handler (host side).

**Implementation:**
```python
# In runtime.py (sandbox side) — write to stderr
import sys, json, time

def _log(level: str, event: str, **data):
    entry = {"ts": time.time(), "level": level, "event": event, **data}
    sys.stderr.write(json.dumps(entry) + "\n")

# Events:
_log("info", "agent.start", agent=name, model=model, max_steps=max_steps)
_log("info", "llm.call", step=step, tokens_in=usage["input_tokens"])
_log("info", "tool.exec", tool=name, elapsed_ms=elapsed)
_log("info", "agent.complete", steps=steps, total_ms=elapsed)
_log("error", "tool.error", tool=name, error=str(e))
```

Host side: worker.py captures stderr and forwards to pool, which writes to a configurable handler (stderr, file, or callback).

#### 2.4.2 Metrics

**Current state:** Pool tracks `recycled` and `replaced` counters. Per-request timing available in result dicts.

**Change:** Add a metrics interface.

**Implementation:**
```python
class PoolMetrics:
    def __init__(self):
        self.requests_total = 0
        self.requests_ok = 0
        self.requests_error = 0
        self.requests_timeout = 0
        self.workers_recycled = 0
        self.workers_replaced = 0
        self.workers_failed = 0
        self.latency_ms = []           # ring buffer, last 1000
        self.queue_wait_ms = []        # ring buffer, last 1000
    
    def snapshot(self) -> dict:
        """Return current metrics as a dict (for health endpoint or logging)."""
```

#### 2.4.3 Health Checks

**Current state:** `ping_all()` exists but must be called manually.

**Change:** Add a health check method that returns a structured health report.

**Implementation:**
```python
def health(self) -> dict:
    alive, dead = self.ping_all()
    metrics = self._metrics.snapshot()
    return {
        "healthy": dead == 0 and alive >= self.size,
        "workers": {"alive": alive, "dead": dead, "target": self.size},
        "metrics": metrics,
        "uptime_seconds": time.monotonic() - self._start_time,
    }
```

---

## Part 3: Competitive Analysis

### Kernl vs Docker

| Dimension | Docker | Kernl (bwrap) |
|-----------|--------|---------------|
| **Isolation** | Full: namespaces + cgroups + seccomp + AppArmor/SELinux | Partial: namespaces only (no cgroups, no seccomp, no MAC) |
| **Startup** | 2,139ms p50 (benchmark) | 67ms p50 (32x faster) |
| **Image size** | python:3.12-slim = 125MB | No image. Bind-mounts host `/`. ~5KB bundle |
| **Resource control** | cgroups v2 with full knobs | setrlimit only (bypassable by forking) |
| **Filesystem** | Copy-on-write layers (overlay2) | Read-only bind mount + tmpfs |
| **Network** | Bridge + veth + iptables | Shared (or --unshare-net) |
| **Root required** | dockerd runs as root (rootless mode available) | No. User namespaces only |
| **Orchestration** | Docker Compose, Swarm, Kubernetes | None (single-host pool only) |

**Where Kernl is stronger:** Startup latency (32x), zero image overhead, no root/daemon requirement, simpler operational model for single-host batch execution.

**Where Kernl is weaker:** No cgroups (no hard memory/CPU limits), no seccomp (full syscall surface exposed), no MAC policy, no image layer caching, no multi-host orchestration, no standardized OCI interface.

**Borrowable ideas:** Docker's seccomp default profile (300+ syscalls blocked), cgroup integration pattern, health check protocol (HEALTHCHECK instruction).

### Kernl vs Firecracker

| Dimension | Firecracker | Kernl (bwrap) |
|-----------|-------------|---------------|
| **Isolation** | Hardware: KVM hypervisor, separate kernel | Software: user namespaces (shared kernel) |
| **Startup** | ~125ms (microVM boot) | ~67ms (bwrap namespace) |
| **Overhead** | ~5MB per VM (minimal kernel) | ~0 (shared host kernel, shared Python) |
| **Escape difficulty** | Requires KVM/hypervisor exploit | Requires kernel namespace escape (more common) |
| **Syscall surface** | Guest kernel → KVM → host kernel (2 layers) | Direct syscalls to shared host kernel |
| **Network** | TAP device, full isolation | Shared by default |
| **Root required** | Yes (KVM access) | No |
| **Device passthrough** | Limited to virtio | Full /dev bind-mount |

**Where Kernl is stronger:** Lower overhead (no VM memory), no root/KVM requirement, faster iteration during development, pool model eliminates per-request startup entirely.

**Where Kernl is weaker:** Fundamentally weaker isolation. Namespace escapes are a known attack surface (CVE-2022-0185, CVE-2021-31440). Firecracker's KVM boundary is a much harder target. No independent kernel means a kernel exploit affects all sandboxes.

**Borrowable ideas:** Firecracker's jailer (a minimal C program that sets up cgroups + seccomp before launching the VM). Kernl could use a similar "pre-exec" binary that sets up cgroups and seccomp before exec'ing into the bwrap namespace.

### Kernl vs gVisor

| Dimension | gVisor (runsc) | Kernl (bwrap) |
|-----------|----------------|---------------|
| **Isolation** | Application kernel: intercepts ALL syscalls, reimplements Linux API in Go | Namespace isolation: syscalls go directly to host kernel |
| **Startup** | ~200-300ms (Sentry process) | ~67ms |
| **Overhead** | 10-30% CPU for syscall interception | ~0% (native syscalls) |
| **Compatibility** | Most Linux programs work but some break (no GPU, limited /proc) | Full compatibility (it IS Linux) |
| **Syscall filtering** | Every syscall reimplemented or rejected | No filtering (full kernel surface) |
| **File I/O** | gofer process for all file access (isolated) | Direct filesystem access (bind-mounted) |

**Where Kernl is stronger:** Startup (3-4x faster), zero CPU overhead for syscalls, full Linux compatibility (no surprises with /proc, /dev, etc.), much simpler operation.

**Where Kernl is weaker:** gVisor's application kernel is fundamentally more secure — even if the guest breaks out of gVisor, it's still in a user-space process, not talking to the real kernel. Kernl has no equivalent defense layer.

**Borrowable ideas:** gVisor's Sentry architecture (a single process that mediates all syscalls) is too heavyweight for Kernl's use case, but the principle of "default deny" for syscalls is exactly what seccomp filtering would provide. gVisor's gofer (separate process for filesystem I/O) could inspire a more restrictive /tmp implementation.

### Kernl vs Serverless Runtimes (Lambda, Vercel)

| Dimension | AWS Lambda | Vercel Functions | Kernl Pool |
|-----------|-----------|------------------|------------|
| **Isolation** | Firecracker microVM per function | V8 isolate or Node.js process | bwrap namespace per worker |
| **Cold start** | 200-500ms (Python) | ~50ms (edge), ~200ms (serverless) | 1ms (warm pool), 260ms (cold pool) |
| **Warm invocation** | ~1ms (reused container) | ~1ms (reused isolate) | ~1ms (reused worker) |
| **Concurrency model** | 1 request per instance (default) | Many requests per instance | 1 request per worker (serialized) |
| **Scaling** | 0 → 1000 instances in seconds | Automatic | Fixed pool (manual) |
| **Max duration** | 900s | 300s | No limit (configurable timeout) |
| **Resource control** | Memory: 128MB-10GB, CPU proportional | CPU: proportional to memory | None (no cgroups) |
| **Network** | VPC, security groups | Edge network | Shared host network |
| **State** | Ephemeral (wiped between invocations) | Ephemeral | Reset between invocations (best-effort) |
| **Cost model** | Per-invocation + GB-seconds | Per-invocation + active CPU | Self-hosted (fixed cost) |

**Where Kernl is stronger:** Self-hosted (no cloud dependency, no egress costs), zero cold start for warm pool workers, no invocation limit, full Python stdlib available, no deployment pipeline required, agent source stays local.

**Where Kernl is weaker:** No auto-scaling, no multi-region, no managed monitoring, no managed networking, weaker isolation than Lambda (Firecracker) or Vercel (V8 isolates + Fluid Compute), no request-level resource limits. Lambda's concurrency model scales to thousands of parallel invocations; Kernl's pool is fixed-size on a single host.

**Borrowable ideas:**
- Lambda's **provisioned concurrency** = Kernl's pool (they converged on the same design for eliminating cold starts)
- Lambda's **reserved concurrency** (cap per function) = Kernl could add per-agent pool partitioning
- Vercel's **Fluid Compute** (reusing instances across concurrent requests) = Kernl's pool already does this
- Lambda's **execution environment reuse** with state reset = Kernl's `_reset_state()` is the same pattern, but Lambda also resets the filesystem via Firecracker snapshotting (stronger guarantee)

---

## Compressed System Definition

**Kernl** is a single-host Python runtime for executing AI agents inside Linux user-namespace sandboxes. It compiles agent source into reproducible bundles (.kb), executes them inside bwrap namespaces with per-request state isolation, and eliminates spawn overhead via preforked worker pools. The entire in-sandbox runtime is 370 lines of stdlib Python with zero external dependencies.

**Architecture:** agent.py → AST parse → .kb bundle → bwrap namespace → runtime.py agent loop → Anthropic API

**Three execution modes:**
1. **Process** — no isolation, 45ms overhead, for development
2. **bwrap** — namespace isolation, 75ms overhead (1.1% of real API time), for production single-shot
3. **Pool** — preforked workers, 1ms overhead (0.0% of real API time), for batch execution

**Isolation guarantees (current):** PID/IPC/UTS/mount namespaces, read-only root, clearenv, writable /tmp only. **Not yet hardened:** no seccomp, no cgroups, no network isolation by default, no UID remapping.

**State isolation between pool runs:** sys.modules, os.environ, sys.path restored; /tmp cleaned; tool executors fresh per run. **Known gaps:** C extension globals, signal handlers, threads, atexit handlers persist.

**Production gaps (priority order):**
1. Seccomp syscall filtering (blocks shell escape via execve)
2. cgroup memory/CPU limits (prevents resource exhaustion)
3. Network isolation by default (prevents API key exfiltration)
4. Worker timeouts and backpressure (prevents pool starvation)
5. Structured logging and metrics (enables debugging and monitoring)
6. Input/tool schema validation (prevents type errors in agent loop)
7. UID namespace isolation (defense-in-depth)
8. Autoscaling (matches capacity to demand)

**Competitive position:** 32x faster than Docker, comparable warm-invocation latency to Lambda/Vercel, weaker isolation than all three. Best suited for single-host batch execution of semi-trusted agents where startup latency matters and cloud dependency is unacceptable.
