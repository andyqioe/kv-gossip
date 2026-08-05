"""
Mini Distributed KV Store with Gossip
======================================
Nodes find each other and share updates without a central coordinator.
Goal: eventual consistency — every write eventually reaches every node,
even if some nodes are temporarily unreachable.

Questions to answer before writing code:
- How does a new node discover at least one peer to start with?
- What exactly is in a gossip message?
- How do you merge two conflicting values for the same key?
- How do you decide a peer is dead and remove it?
- What is the tradeoff between fanout size and bandwidth?
"""
import time
import uuid
import asyncio
import json
import random
from dataclasses import dataclass, asdict, field

# Step 1: Peer state — hint: what's the minimum a node needs to know about a peer?

@dataclass
class PeerState:
    host: str
    port: str
    last_seen: float=field(default_factory=time.monotonic)

    def update_last_seen(self):
        self.last_seen = time.monotonic()

@dataclass
class Gossip: 
    host: str
    port: str
    key_version: dict[str, int]
    peers: dict[str, PeerState]
    cache: dict[str, str]

# Step 2: Node state — hint: what does a node own? (identity, peers, kv cache, fanout)
class KVNode:
    id: uuid.UUID
    addr: str
    port: str
    seed_list: list[tuple[str, str]]
    peers: dict[str, PeerState] # map of kvnode_id to its peerstate
    cache: dict[str, str] # type configurable
    key_version: dict[str, tuple[int, str]]  # uuid
    fanout: int 
    ttl: time

    def __init__(self, seed_list, fanout=2, ttl=123):
        # config - migrate later to env
        self.host="127.0.0.1"
        self.port="8888" # should be from a global env
        self.ttl=ttl
        self.interval=3
        self.seq=0
        

        # node specific data
        self.id=uuid.uuid4() # what types of uuid to use? explore later the tradeoffs
        self.peers={}
        self.seed_list=seed_list
        self.fanout=fanout # depends on the number of nodes in the system, figure out how the fanout correlates to bandwidth and convergence time
        self.cache={} # this should be in sync with the other nodes in the system
        self.key_version={}

        # loop semantics
        self._has_peers=asyncio.Event()

    def put(self, key, value):
        self.cache[key]=value
        self.seq+=1
        self.key_version[key]=self.seq, str(self.id)

    def get(self, key):
        if key in self.cache:
            return self.cache[key] 
        else:
            return None

# Step 3: Bootstrap — hint: accept a seed list of (host, port); if a seed is unreachable, proceed anyway

    # Input: list(host,port)
    # Output:
    # - Assign peers (key? value?)
    # - Start the server & listen for incoming connections

    # When does a new incoming connection happen?
    # - Case1: New node wants to join the cluster
    # - Case2: Existing node dies and is restarted

    # Where does the connection handling happen?
    # - Implementation is abstracted away by asyncio.start_server

    async def _start(self, seed_list: list[tuple[str, str]]):
        for host, port in seed_list:
            if self.host == host and self.port == port:
                continue
            key=f"{host}:{port}"
            self.peers[key]=PeerState(host, port)
            self._has_peers.set()

        await asyncio.gather(
            self._listen(),
            self._gossip_loop(),
        )

    # What does the callback function do?
    # - The callback function is called on new connection

    # What is the gossip model? What should it be? What are the tradeoffs of each approach?
    # - Connection tier
    #   - One-connection-per-gossip - trade-off connection overhead via tcp, latency lower-bounded by travel-time as a function of geographical distance between host machines
    #   - In a data-center, this tradeoff is almost neglible (0.1ms), but could be substantial between geographical calls.
    # In real production systems, database systems are almost always a cluster of machines in one data-center location. 

    # Choice: 1:1 connection-to-gossip
    # Reason: Neglible RTT, persistent connection management creates significant operational complexity

    async def _listen(self):
        server=await asyncio.start_server(self._handle_conn, self.host, self.port)
        async with server:
            await server.serve_forever()

# Step 4: Listen loop — hint: accept incoming TCP connections, read a gossip message, call merge
    # think for a second here:
    # what does merge need to do? how can we do this efficiently?
    #   merge needs to update the peer_list with the updated peer states.
    #   merge needs to update the KV cache with the updated peer states.

    async def _handle_conn(self, reader, writer):
        # to-do retry?
        try:
            data=await reader.readline() 
            if not data:
                return 
            if not data.endswith(b"\n"):
                return
            
            raw=json.loads(data) 
            gossip_data=Gossip(**raw)
            self._merge(gossip_data)
        finally:
            writer.close()
            await writer.wait_closed()

    # Step 5: Gossip loop — hint: every N seconds, pick `fanout` random peers and push your state to each
    async def _gossip_loop(self):
        while True:
            if not self._has_peers.is_set():
                await self._has_peers.wait()
            selected=random.sample(list(self.peers.values()), min(self.fanout, len(self.peers)))
            await asyncio.gather(
                *[self._push(state) for state in selected],
                return_exceptions=True
            )
            self._evict_stale_peers()
            await asyncio.sleep(self.interval)

# Step 6: Push to peer — hint: serialize your cache and peer list; handle connection failures gracefully
    async def _push(self, state: PeerState):
        try:
            curr_ver, curr_cache=self.key_version, self.cache
            curr_peers=self.peers
            gossip=Gossip(
                self.host,
                self.port,
                curr_ver,
                curr_peers,
                curr_cache,
            )
            serialized_gossip=json.dumps(asdict(gossip)) 
            _, writer = await asyncio.open_connection(state.host, state.port)
            writer.write((serialized_gossip + "\n").encode())
            await writer.drain()
            writer.close()
        except (ConnectionError, OSError):
            # log
            return

# Step 7: Merge — hint: for each incoming key, keep the value that "wins" — how do you decide without a timestamp?
# one key: check if key version is > current cache's key. If >, update, if not, skip over.
    def _merge(self, d_gossip):
        # Check key version
        for key, val in d_gossip.cache.items():
            if key in d_gossip.key_version:
                if key in self.key_version:
                    current_version, c_origin_id=self.key_version[key]
                else:
                    current_version,c_origin_id=-1,""

                gossip_version, g_origin_id=d_gossip.key_version[key]
                if (gossip_version, g_origin_id) > (current_version, c_origin_id):
                    self.key_version[key]=gossip_version, g_origin_id
                    self.cache[key]=val
                    self.seq=max(gossip_version, self.seq)
                

        # update peer list
        for gpk, gossip_peer in d_gossip.peers.items():
            # Case 1: gossip_peer in d_gossip, not in self.peers - add gossip_peer to self.peers
            # Case 2: gossip_peer in d_gossip, in self.peers - do nothing (unupdated)
            # Case 3: gossip_peer not in d_gossip, in self.peers - keep self.peers
            # Case 4: gossip_peer not in d_gossip, not in self.peers - unreachable
            if gpk not in self.peers and gpk != f"{self.host}:{self.port}":
                self.peers[gpk] = PeerState(gossip_peer["host"], gossip_peer["port"])
                self._has_peers.set()

        # update last seen of peer we are merging
        peer_key=f"{d_gossip.host}:{d_gossip.port}"
        if peer_key not in self.peers:
            self.peers[peer_key] = PeerState(d_gossip.host, d_gossip.port)
            self._has_peers.set()
        else:
            self.peers[peer_key].update_last_seen()

        


# Step 8: Evict stale peers — hint: filter peers whose last_seen exceeds a timeout threshold
    def _evict_stale_peers(self):
        self.peers={peer_key : peer for peer_key, peer in self.peers.items() if time.monotonic() - peer.last_seen < self.ttl}
        # Question: do we need to propogate? 
        # no need, the system periodically refreshes every 3 seconds. At smaller scales this is fine, but to optimize we should 
        # propogate on update
        if not self.peers:
            self._has_peers.clear()


    
