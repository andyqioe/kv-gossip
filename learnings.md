# 01 — Mini Distributed KV Store with Gossip — Learnings

Goal of the project: eventual consistency across nodes with **no central coordinator**.
Every write eventually reaches every node, tolerating temporarily unreachable peers.

These notes capture the *reasoning*, not just the answers — the "why" is the reusable part.

---

## 1. Identity vs. address (the decision that keeps recurring)

- **Identity** = *who* a node is (a stable `uuid`). **Address** = *where* to reach it (`host:port`).
- You **cannot derive identity from address** — a uuid is opaque; only the remote node knows its own id. Bootstrap therefore must *ask*, not compute.
- Address can change (restart on new port, container gets new IP) without identity changing. That's the whole point of the split.
- **When collapsing them is OK:** a static single-DC toy where nodes have fixed addresses → keying peers by `host:port` is fine, and a separate uuid becomes dead weight. This project ended up doing exactly that (`peers: dict[str, PeerState]` keyed by `"host:port"`).
- **When it breaks:** restart/move tolerance. A node restarting on a different port becomes a *new* identity → ghost entry until eviction.
- **The one place uuid still earns its keep:** as the deterministic **conflict tiebreaker** (see §3), not as the peer key.

## 2. Discovery / bootstrap

- Seeds are a **static, hardcoded hint list** of `(host, port)` — NOT a live membership registry. Their only job is first contact.
- A seed is a *hint, not an authority*: if unreachable, proceed anyway. If seeds were mandatory, rebooting the cluster while the seed is down would deadlock bootstrapping.
- After first contact, membership spreads through gossip: **every gossip message carries the sender's peer table**, so a new node learns the whole cluster transitively.
- The seed list is essentially immutable at runtime; it changes only via ops actions (seed decommissioned, cluster re-addressed, more seeds added for reliability). It is *not* mutated as members join/leave — that's the peer table's job.

## 3. Conflict resolution — the core distributed-systems lesson

**Newness is a property of a *key*, not a *node*.**

- A single node-level version counter cannot express "who wrote *this key* most recently." If A writes `x` and B writes `y`, both bump their global counter to the same value → merging by global version silently loses data. → Use **per-key versions**: `key_version: dict[str, (seq, origin_id)]`.
- **Logical clock, not wall clock.** Wall clocks can't be trusted across machines (NTP jumps, skew). Use a monotonically increasing **Lamport-style counter** (`self.seq`) stamped on each write. "Bigger integer = written later on the node that produced it."
- **Deterministic tiebreak with origin id.** Two nodes can mint the *same* version for the same key concurrently. Comparing `version` alone leaves `5 > 5 == False` → they never converge. Fix: compare the tuple `(version, origin_id)`. Every node computes the same comparison → everyone picks the same winner. This is still last-writer-wins, just made *total* under concurrency.
  - Store the **incoming** origin when relaying someone else's write (`_merge`), your **own** id when authoring (`put`). A value's `(version, origin)` is immutable and global — misattributing it on relay causes divergence.
  - `origin_id` must be a **string** (`str(uuid)`), because `uuid.UUID` is not JSON-serializable and a str-vs-UUID comparison raises `TypeError`.
- **Advance the clock on merge:** `self.seq = max(self.seq, incoming_version)`. Without this, a node's own *later* write can be stamped with a counter lower than data it already holds → its legitimate write loses everywhere. (The reference solution omits this — our version is strictly *more* correct.)

## 4. Clocks

- `time.monotonic()` — for **durations/timeouts** (peer liveness). Never jumps backward; immune to NTP. **Local-only** — the value is meaningless on another machine, so never send it over the wire.
- Bug we hit: shipping `last_seen` (a monotonic value) inside gossiped peer state, then comparing it against the *receiver's* monotonic clock → garbage. Fix: reconstruct discovered peers with host/port only and let `last_seen` default to the *local* clock.
- `time.time()` (wall clock) — only for calendar/logging, never for cross-node ordering. This is exactly *why* §3 uses logical versions.

## 5. Networking model — connection-per-round vs. persistent

- **RTT math (single DC):** intra-DC RTT ~0.1–1 ms; TCP handshake ~1 RTT; gossip interval ~1 s. So the handshake tax is **~0.1% of the interval** — negligible.
- **Persistent connections trade a tiny latency win for statefulness**, and statefulness is a liability you pay for continuously. Concrete failure modes they force you to own:
  1. Half-open connections (peer dies without FIN; socket looks alive for minutes) — TCP keepalive defaults to 2h, useless → need app heartbeats.
  2. Reconnect storms / thundering herd after a rack/switch recovers → need backoff + jitter.
  3. FD exhaustion at scale (O(N) sockets/node → O(N²) cluster-wide) → `EMFILE`.
  4. State desync after silent reconnect (missed messages if you built deltas).
  5. Duplicate connections when both sides dial simultaneously → need a tiebreak.
  6. Backpressure stalls from a slow consumer.
- **Gossip already does liveness at the app layer** (heartbeat + TTL eviction), so persistent connections would duplicate that machinery. → **Chose connection-per-round.** Failure becomes synchronous and local: `connect()` works now or throws now; nothing dangles.
- **When to flip to persistent:** cross-region/WAN (tens of ms RTT) or very high message frequency. Neither applies here.

## 6. Retry policy — don't (here)

- The **periodic gossip loop *is* the retry**, and a distributed/redundant one: a failed push this round is fixed next round, or reaches the peer transitively via someone else.
- Transient failure → next round fixes it for free. Sustained failure → eviction handles it; retrying just wastes effort on a corpse.
- Retry belongs in request/response systems *without* built-in redundancy. Adding it here would only introduce thundering-herd risk. `MAX_ATTEMPTS` was a reflex from RPC-shaped thinking.
- Idiomatic retry, *when you do need it*: **bounded** loop + **exponential backoff** + **jitter** (jitter is the critical part — prevents synchronized retry storms) + **catch specific exceptions** (bare `except` hides real bugs).

## 7. Serialization

- **Objects don't survive JSON — only primitives do.** `json.dumps` can't serialize a custom class (`PeerState`, `Gossip`) or a `uuid.UUID`. Build a plain dict/list of primitives.
- `@dataclass` + `dataclasses.asdict()` gives you a typed struct *and* recursive conversion to primitive dicts (it recurses into nested dataclasses, including those inside dict values). `TypedDict` is the lighter alternative when the message is pure data with no behavior.
- **Parse at the boundary** (`_handle_conn`), so core logic (`_merge`) works on structured data and stays unit-testable without sockets. Rebuild objects on the way in (`Gossip(**raw)`, `PeerState(**peer_dict)`).
- **Framing:** TCP is a byte stream, not messages. Newline-delimited JSON + `reader.readline()` gives free framing — but the payload MUST end with `\n` or `readline()` blocks forever. `readline()` buffers partial packets for you; the only "partial" case to guard is EOF without a trailing `\n` (`not data.endswith(b"\n")`). Watch `LimitOverrunError` if one message exceeds the 64 KiB buffer.
- `writer.write(bytes)` only *queues*; `await writer.drain()` flushes with backpressure. There is no `.send`/`.flush` on `StreamWriter`. Convert str→bytes with `.encode()`, not char-casting.

## 8. asyncio mechanics

- `asyncio.start_server(cb, host, port)` — first arg is the **per-connection callback** `async def cb(reader, writer)`; it already binds + listens + accepts. `serve_forever()` just keeps the coroutine alive so the server isn't torn down.
- The callback fires **once per new TCP connection** (not per message). Connection-per-request → looks like per-message; persistent → loop `readline()` inside for many messages.
- `reader` = bytes *from* peer; `writer` = channel *to* peer (call methods on it, don't assign data).
- Outbound: `reader, writer = await asyncio.open_connection(host, port)` — the client-side mirror of the server callback. This is where outbound failure surfaces (`ConnectionRefusedError`/`OSError`).
- `await asyncio.gather(*coros, return_exceptions=True)` = concurrent fan-out + wait-for-all; `return_exceptions=True` so one dead peer doesn't sink the whole round. `gather` takes coroutines as separate args (`*[...]`), not a list; and it must be awaited. `async with server:` guarantees cleanup — but `StreamWriter` is NOT an async context manager; close it with `writer.close(); await writer.wait_closed()`.
- Single event loop = no true parallelism → no locks needed *as long as* no `await` splits a read-modify-write on shared state.
- `random.sample` needs a **sequence** (`list(...)`, not a dict view or iterator); cap with `min(fanout, len(peers))` or it raises when peers < fanout.

## 9a. The gossip loop & idle parking — don't hold a copy of a fact you already have

This was the session's recurring bug. **One principle in three costumes:** every bug below was some
version of *deriving/duplicating state you already hold instead of reading the source of truth.*

- **`random.sample(peers, min(fanout, len(peers) - 1))`** → the `- 1` assumed the node's *own* entry
  sits in `self.peers`. It doesn't. Effects: a node with exactly 1 peer sampled **0** targets (never
  gossiped → 2-node clusters never converged); an empty node computed `min(fanout, -1) = -1` →
  `ValueError: Sample larger than population or is negative` → crashed the loop. Fix: `min(fanout, len(peers))`.
- **Idle without busy-polling.** An isolated node must wait for a peer without burning CPU.
  - `while True: continue` (guard *before* the sleep) = **starvation** — no `await`, so the single-threaded
    event loop never yields and `_listen` can't accept the very connection you're waiting for. asyncio is
    cooperative: you only yield *at a suspending `await`*, never by preemption.
  - `await asyncio.sleep(0); continue` = fixes starvation (it yields) but is a **busy-poll** — `sleep(0)`
    doesn't sleep, it reschedules immediately → pegs a core spinning while idle.
  - **Right answer: block until signalled.** `asyncio.Event` — the isolated node does
    `await self._has_peers.wait()` (zero CPU, parks until set); `_merge` calls `.set()` the instant it
    learns a peer → sub-ms wake, no polling. "Don't poll, signal."
- **Event = doorbell, `self.peers` = state.** The deadlock: guarding the loop with `self._has_peers.is_set()`
  made the Event a *second copy* of "do I have peers." `_start` added seed peers to the dict but forgot to
  `.set()` → a seeded node had peers yet parked forever. Two mutation sites set it (`_merge`), one didn't
  (`_start`) → drift → deadlock. **Fix: guard on the real state — `if not self.peers:` — and demote the Event
  to a pure wake-up signal.** Idling is identical (still `await wait()`); a missed `.set()` can now only cost
  a late wake-up, never a deadlock. `is_set()` bought nothing but a drift bug.
- **Invariant maintenance lives at the *mutation*, not the *caller*.** `.set()` belongs where a peer is
  added, `.clear()` (guarded by `if not self.peers`) where peers are removed (eviction) — never re-checked
  by whoever *calls* eviction, or a second call site silently breaks it.
- **`asyncio.Event()` — the `()` matters.** `self._has_peers = asyncio.Event` stored the *class*; `.set()`
  then failed with `TypeError: set() missing 1 required positional argument: 'self'`. And `.wait()` is a
  coroutine — bare `self._has_peers.wait()` without `await` is a dropped-on-the-floor no-op.

## 9b. Failure detection runs on its own clock, not the data path

- Eviction-per-`_merge` (my first cut) vs. eviction-per-gossip-loop (reference). Decisive difference:
  `_merge` only fires when a peer *sends* — i.e. when it's *alive*. So merge-driven eviction **stalls exactly
  when peers are dying** (silent → no merges → no pruning), the one moment it's needed. It also does O(P)
  dict-rebuild work ~`fanout`× per interval instead of once.
- Periodic eviction runs on an independent tick regardless of traffic → self-heals, and composes with the
  idle-park design (it's what drains `self.peers` to empty and fires `.clear()` so the node can park again).
  This is why real systems (SWIM) run failure detection on a dedicated protocol tick.
- Keep the `last_seen` *update* in `_merge` (a receive genuinely *is* evidence of liveness); move the
  *eviction decision* to the periodic loop. Split: merge records liveness, the loop acts on staleness.

## 9. Data-center topology (context)

- A DC is thousands of independent machines: server → rack (top-of-rack switch) → row → fabric. Never a single entity; the "single system" feel is the abstraction you're *building*.
- A **cluster** is a *logical* grouping of nodes (processes) over physical machines; one DC hosts many clusters.
- Latency isn't flat even intra-DC (same rack ~10–50 µs vs. cross-rack sub-ms). Real gossip is often rack-aware to avoid **correlated failure** (a rack/switch dying takes out many peers at once). For this toy we treat the DC as flat but note eviction/quorum must tolerate losing a *chunk* of peers simultaneously.

---

## Status / open follow-ups

- **Done & verified.** `test_harness.py` (two nodes on 127.0.0.1:9001/9002) converges **both directions**:
  A→B and B→A, with idle nodes parked at zero CPU via the Event doorbell. `ALL GOOD ✅`.
- Core logic complete and correct: discovery, per-key LWW with origin tiebreak + clock advance, push gossip,
  Event-based idle parking, periodic eviction.
- Config still hardcoded (`host`/`port`) — the harness monkeypatches around it; real multi-node needs them as
  constructor args. Migrate to env (12-factor: config in env, read+validate once at startup, fail fast).
- Minor non-blocking cleanups noted in review: `port` typed `str` (convention is `int`); `_start(seed_list)`
  param duplicates the unused `self.seed_list`; `readline()` carries a 64 KiB frame ceiling.
- Possible optimization: **write-triggered gossip** (push on `put`) layered on top of the periodic loop for lower write-propagation latency — but the periodic loop stays mandatory (it's the reliability mechanism).
