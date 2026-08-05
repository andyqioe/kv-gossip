# kv-gossip

A mini distributed key-value store with **gossip-based eventual consistency** — nodes
discover each other and replicate writes with no central coordinator. Every write
eventually reaches every node, tolerating temporarily unreachable peers.

Built as a systems-design learning exercise. See [`learnings.md`](learnings.md) for the
full reasoning behind each design decision.

## Design highlights

- **Per-key last-writer-wins** using a logical (Lamport-style) clock plus an origin-id
  tiebreak, so concurrent writes converge deterministically without trusting wall clocks.
- **Push gossip** over newline-framed JSON on TCP; membership spreads transitively since
  every message carries the sender's peer table.
- **TTL peer eviction** on an independent periodic tick (failure detection off the data path).
- **Zero-CPU idle parking** via an `asyncio.Event` doorbell — an isolated node blocks until
  a peer appears instead of busy-polling.

## Run the convergence check

```bash
python3 test_harness.py
```

Spins up two nodes on `127.0.0.1:9001/9002`, writes on each, and asserts the value
propagates to the other via gossip.

## Files

- `main.py` — the implementation
- `test_harness.py` — two-node convergence sanity check
- `learnings.md` — design reasoning and the bugs worked through
- `_solution.py` — a reference implementation, for comparison
