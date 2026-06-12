# Phase 2: push single-shard toward 10⁴ jetton TPS

Context: read benchmark/DESIGN.md + benchmark/RESULTS.md first. Phase-1 found single-shard
ceilings of 132–145 jTPS (208GB state) / 236–241 (RAM state, byte-limit) with the collator
bottlenecked on serial celldb reads, an effective ~190ms collation budget, and ~0.54ms CPU
per transfer. Phase-2 goal: 10⁴ jetton TPS single shard (= 30k tx/s = ~12k txs per 400ms
block), per explicit direction. Constraints: (1) NO correctness compromises; (2) avoid
public-ABI changes — block format & collated data are fair game if minimal, full-node-visible
ABI is not; (3) prefer eliminating the phase-1 prefetcher (no lingering threads) once proper
I/O multiplexing exists.

## Machine-sharing protocol (MANDATORY for every agent)

- Any activity that launches validator-engine / runs nets / benchmarks MUST hold the lock:
  `flock /mnt/bench/work/bench.lock <cmd>` (wait with `flock -w 7200` if needed).
- Heavy processes: prefix `choom -n 1000 --`.
- NEVER rebuild binaries in the MAIN build dir (`src/build`) while not holding the lock —
  benchmarks run from it. Agents working in worktrees use their own build dir
  (cmake with `-DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache`,
  same flags as src/build/CMakeCache.txt: clang21, RelWithDebInfo, Ninja).
- Scratch space: /mnt/bench/work/<agent-name>/.

## Workstreams

- **P0 profile**: where do 400ms of wc0 collation real time go when trx_* ≈ 60ms?
  (consensus explorer over session-logs + collator stats + perf). Output drives W1/W2 design.
- **C0 config parity**: enable full collated data (validation must not read celldb), sync
  shard block limits + global version with current mainnet values in the benchmark config.
- **W1/W2 collator multiplexing & parallelism** (after P0): first saturate ONE collation
  thread by interleaving I/O waits with execution (stackful coroutines or async dict/account
  loads); then parallelize execution across independent accountchains. Replaces the
  account-prefetch thread pool.
- **W3 simplex-aware ext pool**: continue the user's WIP (branch `mempool` + a stash on top;
  reconstruct intent, reimplement cleanly on HEAD). Core idea: candidates remove externals
  from the mempool as today, but when a final certificate collapses histories, walk the
  REJECTED blocks and re-add their externals to the mempool. (Writing rejected externals
  into collated data: out of scope.) Fixes the dead-candidate-loses-externals collapse
  (RESULTS.md §4b) properly.
- **W4 liteserver tip desync**: unify/fix the multiple independently-tracked mc tips so
  "block not found (possibly out of sync)" stops happening (ltdb vs shard client vs ls).
- **W5 celldb layout** (after P0): (a) bundle ~5 levels of the accounts dict per IO op
  (one ~4KB read serves a whole descent segment; relates to celldb-compress-depth);
  (b) [explore only] single materialized state + Merkle updates for the rest.

## Status log (append-only; agents update their line on completion)

- P0: pending
- C0: pending
- W1/W2: blocked on P0
- W3: pending
- W4: pending
- W5: blocked on P0
