"""
Two-node convergence harness for the gossip KV store.

Not a unit test — a runnable sanity check. Spins up two KVNodes on distinct
local ports, writes a key on A, and asserts it propagates to B via gossip.

Run:  python test_harness.py
"""
import asyncio
from main import KVNode


def make_node(port: int, seeds, interval=0.2):
    node = KVNode(seed_list=seeds)
    # main.py hardcodes host/port in __init__; override for local multi-node.
    node.host = "127.0.0.1"
    node.port = port
    node.interval = interval      # speed up gossip for the test
    node.ttl = 60                 # don't evict during the run
    return node


async def wait_until(cond, timeout=10.0, step=0.2):
    waited = 0.0
    while waited < timeout:
        if cond():
            return True
        await asyncio.sleep(step)
        waited += step
    return False


async def main():
    # A has no seeds; B seeds to A. B contacts A first -> A learns B (via sender),
    # then A gossips back -> B learns A's data. Bidirectional discovery bootstraps.
    A = make_node(9001, seeds=[])
    B = make_node(9002, seeds=[("127.0.0.1", 9001)])

    a_task = asyncio.create_task(A._start([]))
    b_task = asyncio.create_task(B._start([("127.0.0.1", 9001)]))

    await asyncio.sleep(0.5)  # let both servers bind

    print("--- write x=hello on A ---")
    A.put("x", "hello")

    ok = await wait_until(lambda: B.get("x") == "hello")
    print(f"A.get('x') = {A.get('x')!r}")
    print(f"B.get('x') = {B.get('x')!r}")
    print(f"A.peers    = {list(A.peers.keys())}")
    print(f"B.peers    = {list(B.peers.keys())}")
    assert ok, "FAILED: B did not converge on x=hello"
    print("CONVERGED (A -> B) ✅")

    # Second write, opposite direction, tests LWW + reverse propagation.
    print("--- write y=world on B ---")
    B.put("y", "world")
    ok2 = await wait_until(lambda: A.get("y") == "world")
    print(f"A.get('y') = {A.get('y')!r}")
    assert ok2, "FAILED: A did not converge on y=world"
    print("CONVERGED (B -> A) ✅")

    a_task.cancel()
    b_task.cancel()
    print("ALL GOOD ✅")


if __name__ == "__main__":
    asyncio.run(main())
