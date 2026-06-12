# Phase 2: push single-shard toward 10⁴ jetton TPS

Context: read benchmark/DESIGN.md + benchmark/RESULTS.md first. Phase-1 found single-shard
ceilings of 132–145 jTPS (208GB state) / 236–241 (RAM state, byte-limit) with the collator
bottlenecked on serial celldb reads, an effective ~190ms collation budget, and ~0.54ms CPU
per transfer. Phase-2 goal: 10⁴ jetton TPS single shard (= 30k tx/s = ~12k txs per 400ms
block), per explicit direction. Constraints: (1) NO correctness compromises; (2) ABI: the
BLOCK format is public ABI — minimal changes only if unavoidable; COLLATED DATA is exchanged
between validators only — fine to modify arbitrarily; (3) prefer eliminating the phase-1
prefetcher (no lingering threads) once proper I/O multiplexing exists; (4) the primary regime
is the 208GB state that does NOT fit in RAM (disk-bound); RAM-resident numbers are secondary.
Ordering note: C0 lands BEFORE P0 — config parity (full collated data) changes what profiling
measures.

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

## C0 findings (mainnet config parity, 2026-06-12)

Mechanism: "full collated data" is carried by capability bit `capFullCollatedData = 512`
in **ConfigParam 8** (`GlobalVersion.capabilities`, ton/ton-types.h). The collator turns it
on from config (`validator/impl/collator.cpp:947`) and emits Merkle proofs of every
accessed prev-state/out-queue cell into the collated data; ValidateQuery does not read
the config bit at all — it detects the proof roots while unpacking collated data
(`validate-query.cpp:681/711`), sets `full_collated_data_`, and then skips
`load_prev_states()` entirely (line 393): prev state, neighbor out-queues and account
storage dicts are reconstructed from `virt_roots_`, so wc0 validation performs no celldb
state reads. Upstream's separate CollatorConfig param (41) is EMPTY on mainnet — the
param-8 capability bit is the live mechanism.

Current mainnet values (toncenter `getConfigParam`, fetched 2026-06-12):

- **Param 8**: version=14, capabilities=0x3EE=1006
  (2|4|8|32|64|128|256|512 = createstats, bouncemsgbody, reportversion, shortdequeue,
  storeoutmsgqueuesize, msgmetadata, **defermessages**, **fullcollateddata**;
  no capIhr, no capSplitMergeTransactions).
- **Param 22** (mc block limits, underload/soft/hard): bytes 131072/524288/1048576,
  gas 200000/1000000/2500000, lt_delta 1000/5000/10000.
- **Param 23** (basechain block limits): bytes 262144/1048576/2097152,
  gas 2000000/10000000/20000000, lt_delta 1000/5000/10000.
- **Param 41** (upstream CollatorConfig): empty.

tontester (`zerostate.py`): param 8 now mainnet 0x3EE with
`NetworkConfig.full_collated_data` (default True) gating bit 512; params 22/23 use the
mainnet baselines above, `block_limit_mul` scaling the bytes+lt soft/hard limits and
`gas_limit_mul` the gas soft/hard limits. NOTE vs phase-1 configs at mul=1: basechain
gas soft limit dropped 100M→10M and lt_delta soft 500k→5k, so phase-1 numbers are not
directly comparable; basechain bytes soft limit rose 512K→1M (mainnet).

Verification (208GB state, rate 300, 60s, mul=1, `/mnt/bench/results/c0-parity-r300`):
included=13276, tps_included=586, jetton_tps=195. wc0 ValidateQuery perf logs contain NO
`wait_block_state #i` action (the prev-state celldb load) anywhere in the run, and wc0
validation cpu time ≈ real time (e.g. 60.2ms cpu / 60.4ms real at block 199) — validation
is pure CPU over collated-data proofs. Timings: validation n=387 mean 28.5ms max 118ms;
collation n=387 mean 175ms max 491ms (collation still I/O-bound; that is P0/W1 territory).

## Status log (append-only; agents update their line on completion)

- P0: pending
- C0: done — full collated data on by default (param-8 cap 0x3EE, mainnet parity) +
  mainnet 22/23 block limits as mul baselines; verified wc0 validation reads no celldb
  state (r300 run: 586 tps included, validation pure-CPU ~28ms mean).
- W1/W2: blocked on P0
- W3: pending
- W4: pending
- W5: blocked on P0
