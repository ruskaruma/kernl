#!/usr/bin/env python3
"""
Kernl Real API Benchmark — measures actual LLM latency vs infrastructure overhead.

Usage:
  ANTHROPIC_API_KEY=sk-ant-... python3 bench/real_api_benchmark.py

What this measures:
  - Total wall-clock time for each agent execution
  - LLM API call time (reported by runtime.run_agent via elapsed_ms)
  - Kernl infrastructure overhead = total - agent_elapsed
  - Infrastructure as % of total time

Methods tested:
  1. process (no sandbox)  — bare Python, no isolation
  2. bwrap (sandboxed)     — full bwrap namespace isolation
  3. kernl pool            — preforked workers, no spawn per request

Each method runs the SAME agent (bench.kb) with REAL Anthropic API calls.
The mock LLM is NOT used — every run makes actual HTTP requests.
"""
import json
import os
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

KB_PATH = os.path.join(PROJECT_ROOT, "bench.kb")
RUNS_PER_METHOD = 5


def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print("ERROR: Set ANTHROPIC_API_KEY environment variable.")
        print("  ANTHROPIC_API_KEY=sk-ant-... python3 bench/real_api_benchmark.py")
        sys.exit(1)

    print("=" * 76)
    print("KERNL REAL API BENCHMARK")
    print("=" * 76)
    print(f"  API key:    {api_key[:12]}...{api_key[-4:]}")
    print(f"  Runs/method: {RUNS_PER_METHOD}")
    print(f"  Agent:      bench.kb (2-step tool loop)")
    print()
    print("  Each run makes REAL Anthropic API calls. No mock/dry-run.")
    print("  'agent_ms' = time inside runtime.run_agent (includes all LLM calls)")
    print("  'infra_ms' = total - agent_ms (spawn, bwrap, tar extract, etc.)")
    print()

    from src.run import run, get_cached_bundle
    from src.pool import WorkerPool

    # =====================================================================
    # Method 1: process (no sandbox)
    # =====================================================================
    print("-" * 76)
    print("METHOD 1: process (no sandbox)")
    print("-" * 76)
    results_process = []
    for i in range(RUNS_PER_METHOD):
        t0 = time.monotonic()
        r = run(KB_PATH, {"input_data": "benchmark"}, use_sandbox=False, dry_run=False,
                allow_network=True)
        total_ms = (time.monotonic() - t0) * 1000
        agent_ms = r.get("elapsed_ms", 0)
        infra_ms = total_ms - agent_ms
        pct = (infra_ms / total_ms * 100) if total_ms > 0 else 0
        status = r.get("status", "?")
        steps = r.get("steps", 0)
        tools = len(r.get("tool_calls", []))
        results_process.append({
            "total": total_ms, "agent": agent_ms, "infra": infra_ms,
            "pct": pct, "status": status,
        })
        print(f"  run {i+1}: total={total_ms:>7.0f}ms  agent={agent_ms:>7.0f}ms  "
              f"infra={infra_ms:>5.0f}ms ({pct:>4.1f}%)  "
              f"steps={steps} tools={tools}  [{status}]")
    print()

    # =====================================================================
    # Method 2: bwrap (sandboxed)
    # =====================================================================
    print("-" * 76)
    print("METHOD 2: bwrap (sandboxed)")
    print("-" * 76)
    results_bwrap = []
    for i in range(RUNS_PER_METHOD):
        t0 = time.monotonic()
        r = run(KB_PATH, {"input_data": "benchmark"}, use_sandbox=True, dry_run=False,
                allow_network=True)
        total_ms = (time.monotonic() - t0) * 1000
        agent_ms = r.get("elapsed_ms", 0)
        infra_ms = total_ms - agent_ms
        pct = (infra_ms / total_ms * 100) if total_ms > 0 else 0
        status = r.get("status", "?")
        steps = r.get("steps", 0)
        tools = len(r.get("tool_calls", []))
        results_bwrap.append({
            "total": total_ms, "agent": agent_ms, "infra": infra_ms,
            "pct": pct, "status": status,
        })
        print(f"  run {i+1}: total={total_ms:>7.0f}ms  agent={agent_ms:>7.0f}ms  "
              f"infra={infra_ms:>5.0f}ms ({pct:>4.1f}%)  "
              f"steps={steps} tools={tools}  [{status}]")
    print()

    # =====================================================================
    # Method 3: kernl pool (preforked)
    # =====================================================================
    print("-" * 76)
    print("METHOD 3: kernl pool (preforked, 4 workers)")
    print("-" * 76)
    bundle_dir, manifest = get_cached_bundle(KB_PATH)
    with open(os.path.join(bundle_dir, "agent.py")) as f:
        source = f.read()

    pool = WorkerPool(size=4, api_key=api_key, dry_run=False, allow_network=True)
    startup_ms = pool.start()
    print(f"  pool startup: {startup_ms:.0f}ms")

    results_pool = []
    for i in range(RUNS_PER_METHOD):
        t0 = time.monotonic()
        r = pool.submit(manifest, {"input_data": "benchmark"}, source, dry_run=False)
        total_ms = (time.monotonic() - t0) * 1000
        agent_ms = r.get("elapsed_ms", 0)
        worker_ms = r.get("_worker_ms", 0)
        infra_ms = total_ms - agent_ms
        pct = (infra_ms / total_ms * 100) if total_ms > 0 else 0
        status = r.get("status", "?")
        steps = r.get("steps", 0)
        tools = len(r.get("tool_calls", []))
        results_pool.append({
            "total": total_ms, "agent": agent_ms, "infra": infra_ms,
            "pct": pct, "status": status, "worker": worker_ms,
        })
        print(f"  run {i+1}: total={total_ms:>7.0f}ms  agent={agent_ms:>7.0f}ms  "
              f"infra={infra_ms:>5.0f}ms ({pct:>4.1f}%)  "
              f"steps={steps} tools={tools}  [{status}]")
    pool.shutdown()
    print()

    # =====================================================================
    # Summary
    # =====================================================================
    print("=" * 76)
    print("SUMMARY — Average over successful runs")
    print("=" * 76)
    print()

    def _summarize(name, results, startup=0):
        ok = [r for r in results if r["status"] == "complete"]
        if not ok:
            print(f"  {name}: all runs failed")
            return
        avg_total = sum(r["total"] for r in ok) / len(ok)
        avg_agent = sum(r["agent"] for r in ok) / len(ok)
        avg_infra = sum(r["infra"] for r in ok) / len(ok)
        avg_pct = sum(r["pct"] for r in ok) / len(ok)
        print(f"  {name:<32s}  total={avg_total:>7.0f}ms  "
              f"llm={avg_agent:>7.0f}ms  infra={avg_infra:>5.0f}ms ({avg_pct:.1f}%)")
        if startup:
            print(f"  {'(pool startup amortized)':<32s}  +{startup:.0f}ms one-time cost")

    _summarize("process (no sandbox)", results_process)
    _summarize("bwrap (sandboxed)", results_bwrap)
    _summarize("kernl pool (preforked)", results_pool, startup=startup_ms)

    print()
    print("  INTERPRETATION:")
    print("  - 'llm' is the time spent in LLM API calls (the irreducible floor)")
    print("  - 'infra' is everything Kernl adds: spawn, bwrap, state reset, IPC")
    print("  - Pool infra should be <5ms; bwrap infra includes CPython startup")
    print("  - If infra% is <5%, Kernl overhead is negligible for your use case")
    print()

    # Show failed runs if any
    all_results = [("process", results_process), ("bwrap", results_bwrap),
                   ("pool", results_pool)]
    failures = [(name, r) for name, results in all_results
                for r in results if r["status"] != "complete"]
    if failures:
        print("  FAILURES:")
        for name, r in failures:
            print(f"    {name}: status={r['status']}")


if __name__ == "__main__":
    main()
