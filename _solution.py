"""Reference implementation — for answer checking only."""
import asyncio
import json
import random
import time
import uuid


class PeerState:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.last_seen: float = time.monotonic()


class KVNode:
    def __init__(self, host: str, port: int, fanout: int = 2):
        self.id = str(uuid.uuid4())
        self.host = host
        self.port = port
        self.fanout = fanout
        self.store: dict = {}
        self.peers: dict = {}       # peer_id -> PeerState
        self._version: dict = {}    # key -> logical timestamp (lamport-ish: insertion order)
        self._seq = 0

    # --- public API ---

    def put(self, key: str, value: str) -> None:
        self._seq += 1
        self.store[key] = value
        self._version[key] = self._seq

    def get(self, key: str):
        return self.store.get(key)

    # --- networking ---

    async def start(self, seed_peers: list = ()) -> None:
        for host, port in seed_peers:
            peer_id = f"{host}:{port}"
            self.peers[peer_id] = PeerState(host, port)
        await asyncio.gather(self._listen(), self._gossip_loop())

    async def _listen(self) -> None:
        server = await asyncio.start_server(self._handle_conn, self.host, self.port)
        async with server:
            await server.serve_forever()

    async def _handle_conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            data = await reader.read(65536)
            msg = json.loads(data)
            self._merge(msg)
        finally:
            writer.close()

    async def _gossip_loop(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            if not self.peers:
                continue
            targets = random.sample(list(self.peers.values()), min(self.fanout, len(self.peers)))
            await asyncio.gather(*[self._push(p) for p in targets], return_exceptions=True)
            self._evict_stale()

    async def _push(self, peer: PeerState) -> None:
        payload = json.dumps({
            "sender_id": self.id,
            "sender_host": self.host,
            "sender_port": self.port,
            "store": self.store,
            "versions": self._version,
            "peers": [{"id": pid, "host": p.host, "port": p.port}
                      for pid, p in self.peers.items()],
        }).encode()
        try:
            reader, writer = await asyncio.open_connection(peer.host, peer.port)
            writer.write(payload)
            await writer.drain()
            writer.close()
            peer.last_seen = time.monotonic()
        except (ConnectionRefusedError, OSError):
            pass  # transient; eviction handles sustained failures

    def _merge(self, msg: dict) -> None:
        # Merge store: keep value with higher version (last-writer-wins by seq)
        remote_versions = msg.get("versions", {})
        for key, value in msg.get("store", {}).items():
            remote_v = remote_versions.get(key, 0)
            local_v = self._version.get(key, 0)
            if remote_v > local_v:
                self.store[key] = value
                self._version[key] = remote_v

        # Merge peer list
        sender_id = msg.get("sender_id")
        if sender_id and sender_id != self.id:
            if sender_id not in self.peers:
                self.peers[sender_id] = PeerState(msg["sender_host"], msg["sender_port"])
            self.peers[sender_id].last_seen = time.monotonic()

        for peer_info in msg.get("peers", []):
            pid = peer_info["id"]
            if pid != self.id and pid not in self.peers:
                self.peers[pid] = PeerState(peer_info["host"], peer_info["port"])

    def _evict_stale(self, timeout: float = 10.0) -> None:
        now = time.monotonic()
        self.peers = {
            pid: p for pid, p in self.peers.items()
            if now - p.last_seen < timeout
        }


# Key design decisions captured here for reference:
# 1. last-writer-wins by logical sequence number (not wall clock — avoids clock skew)
# 2. fanout=2 is typical; convergence time is O(log N / log fanout) rounds
# 3. push-based gossip; pull (anti-entropy) can be added for full sync
# 4. stale eviction after 10s of silence; SWIM uses suspicion + confirmation
# 5. TCP for reliability; UDP is fine too since gossip tolerates loss
