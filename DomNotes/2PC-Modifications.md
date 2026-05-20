# MiniSpanner 2PC — Modifications Notes

Working notes on the sharded-replicated + Two-Phase Commit (CS 410) implementation on top of Apache ResilientDB. Captures *what was broken after Owen's merge*, *what was changed to fix it*, and *evidence the pipeline works end-to-end*. Intended as raw material for the LaTeX writeup.

---

## Architecture summary

- **4 shards × 4 replicas = 16 `kv_service` processes**, all on `127.0.0.1`, ports `180{shard}{replica}` (e.g. shard 1: 18001–18004; shard 2: 18011–18014; etc.).
- Each shard runs internal **PBFT** among its 4 replicas. The first node of each shard (ids 1, 5, 9, 13) is the **primary / shard leader**.
- The four shard leaders also act as **2PC peers** of one another. Every client transaction is broadcast as 2PC PREPARE to the other three leaders before being committed.
- The client (`kv_service_tools`) reads `client.shard_leaders.config`, which contains the four leaders and `multiShardClientRoundRobin: true`. Within a single client session it rotates among the four leaders; whichever leader receives a request becomes the **coordinator** for that transaction.

---

## End-to-end transaction path

```
kv_service_tools (round-robin)
    └─► shard leader receives TYPE_CLIENT_REQUEST
            └─► ResponseManager::NewUserRequest()  ── enqueues in batch_queue_
                    └─► BatchProposeMsg() drains, forwards to local primary as TYPE_NEW_TXNS
                            └─► Commitment::ProcessNewRequest()
                                    ├─► [2PC] PREPARE  ── direct NetChannel → 3 cross-shard leaders
                                    │                       └─► Process2PCPrepare → VOTE YES
                                    ├─► [2PC] VOTE collected (3/3)
                                    ├─► [2PC] GLOBAL_COMMIT broadcast → 3 cross-shard leaders
                                    │                       └─► Process2PCCommit → starts local PBFT on participant shard
                                    └─► PBFT on coordinator shard
                                            └─► PRE_PREPARE → PREPARE → COMMIT → execute → reply to client
```

---

## Problems found after merging Owen's sharded branch

### 1. Client requests sat in `batch_queue_` forever (zero proposals)
- **Symptom:** `client set ret = 0` returned in ~1.3 ms; shard-1 leader log showed `propose:0 prepare:0 commit:0 execute:0` indefinitely; subsequent `get` hung.
- **Cause:** `ResponseManager` only starts its `BatchProposeMsg` drain thread when this node's certificate type is `CertificateKeyInfo::CLIENT` or `IsTestMode()` is true. Shard leaders are `REPLICA`-cert, and there is no separate proxy node in the sharded deployment, so the drain thread was never started. The queue grew, but nothing ever forwarded `TYPE_NEW_TXNS` to `Commitment::ProcessNewRequest`.

### 2. `SendSingleMessage` short-conn path threw away the target
- **Symptom:** Even if a cross-shard send compiled, it never arrived at the peer.
- **Cause:** `ReplicaCommunicator::SendSingleMessage` had `return SendMessageInternal(message, replicas_);` in the `is_use_long_conn_ == false` branch — i.e. it ignored the `replica_info` argument and broadcast to its own shard's replicas instead of the explicit peer.

### 3. Cross-shard delivery via the long-conn pool didn't work
- **Symptom:** `replica_communicator_->SendMessage(prep, peer)` returned 0 quickly but no peer leader registered any inbound 2PC traffic; `server call:0` on shards 2/3/4.
- **Cause:** `ConsensusManager::GetReplicaClient(..., is_use_long_conn=true)` constructs the broadcast client in long-conn mode. The long-conn pool routes to **`port + 10000`** (e.g. 28011 instead of 18011) and signs with the local shard's verifier. Cross-shard peers do not exchange public keys via heartbeats, so even if a connection landed, signature verification would fail. The hb logs confirm `from region:N sender:M` only ever within the same region.

---

## Changes

### A. `platform/consensus/ordering/pbft/response_manager.cpp` (~line 62)

Widen the gate so the queue-drain thread also starts on nodes that have cross-shard peers configured (i.e. shard leaders in the sharded deployment).

```cpp
if (config_.GetPublicKeyCertificateInfo()
            .public_key()
            .public_key_info()
            .type() == CertificateKeyInfo::CLIENT ||
    config_.IsTestMode() ||
    !config_.GetCrossShardPeers().empty()) {   // <-- added
  user_req_thread_ = std::thread(&ResponseManager::BatchProposeMsg, this);
}
```

Why it's safe: non-leader replicas never receive `TYPE_CLIENT_REQUEST` (the client config only lists leaders), so their queue stays empty and the thread idles. On leaders, the drained `TYPE_NEW_TXNS` is forwarded to `GetPrimary()` which is the leader itself, so it loops back into the local dispatcher and reaches `Commitment::ProcessNewRequest`.

### B. `platform/networkstrate/replica_communicator.cpp` (~line 192)

Make `SendSingleMessage` actually target its parameter when running in short-conn mode.

```cpp
} else {
  return SendMessageInternal(message, {replica_info});  // was: replicas_
}
```

This bug is dormant for in-cluster PBFT (which uses the long-conn pool) but would have broken any caller routing to an explicit peer over short-conn. Fixing for correctness.

### C. `platform/consensus/ordering/pbft/commitment.cpp` — cross-shard sends use direct `NetChannel`

The long-conn pool isn't a valid carrier for cross-shard traffic (port-shift + signing reasons in (3) above). Replaced three cross-shard sends with direct short-conn writes via `NetChannel(ip, port).SendRawMessage(...)`. These go to the receiver's base listener port and skip the local verifier; the receiver's normal dispatch (`InternalConsensusCommit` → `case TYPE_2PC_PREPARE / TYPE_2PC_VOTE / TYPE_2PC_COMMIT`) picks them up.

1. **Coordinator PREPARE → peers** (in `ProcessNewRequest`):
   ```cpp
   for (const auto& peer : cross_peers) {
     Request prep;
     prep.CopyFrom(*user_request);
     prep.set_type(Request::TYPE_2PC_PREPARE);
     *prep.mutable_client_info() = coord_contact;
     NetChannel channel(peer.ip(), peer.port());
     channel.SendRawMessage(prep);
   }
   ```
2. **Participant VOTE → coordinator** (in `Process2PCPrepare`):
   ```cpp
   if (request->has_client_info() && !request->client_info().ip().empty()) {
     const auto& dest = request->client_info();
     NetChannel channel(dest.ip(), dest.port());
     channel.SendRawMessage(vote);
   }
   ```
3. **Coordinator GLOBAL_COMMIT → peers** (in `Process2PCVote`):
   ```cpp
   for (const auto& peer : cross_peers) {
     NetChannel channel(peer.ip(), peer.port());
     channel.SendRawMessage(commit_msg);
   }
   ```

Added `#include "interface/rdbc/net_channel.h"` at the top of `commitment.cpp`.

### D. (already merged from Owen) Supporting pieces

These came in with the sharded branch; listed for completeness so the reader of the report can navigate.

- `platform/proto/resdb.proto` — `TYPE_2PC_PREPARE = 22`, `TYPE_2PC_VOTE = 23`, `TYPE_2PC_COMMIT = 24`.
- `platform/proto/replica_info.proto` — `repeated ReplicaInfo cross_shard_peer = 26;` in `ResConfigData`, plus `multi_shard_client_round_robin`.
- `platform/config/resdb_config.{h,cpp}` — `GetCrossShardPeers()`, `MultiShardClientRoundRobin()` accessors.
- `platform/consensus/ordering/pbft/commitment.h` — declarations `Process2PCPrepare`, `Process2PCVote`, `Process2PCCommit`, plus participant state (`participant_twopc_by_hash_`).
- `platform/consensus/ordering/pbft/consensus_manager_pbft.cpp` — three new `case` arms in `InternalConsensusCommit` dispatching to the 2PC handlers.
- `interface/rdbc/transaction_constructor.cpp` — `PickDestReplica()` round-robins across the leaders listed in the client config when `MultiShardClientRoundRobin()` is true.
- `scripts/deploy/config/sharded/server/shard{1..4}.server.config` — each shard config carries the three other shard leaders as `crossShardPeer`.
- `scripts/deploy/config/sharded/client.shard_leaders.config` — four leaders + `multiShardClientRoundRobin: true`.

---

## Verification (recorded on the merged `DomTest` branch, inside `dom-sharded` container)

Single transaction trace (truncated to the salient lines from `kv_1.log`):
```
[2PC] Coordinator starting 2PC for seq: 1
[2PC] sending PREPARE seq=1 to peer id=5  ip=127.0.0.1 port=18011
[2PC] sending PREPARE seq=1 to peer id=9  ip=127.0.0.1 port=18021
[2PC] sending PREPARE seq=1 to peer id=13 ip=127.0.0.1 port=18031
[2PC] Coordinator received VOTE from replica 5  for seq: 1
[2PC] Coordinator received VOTE from replica 9  for seq: 1
[2PC] Coordinator received VOTE from replica 13 for seq: 1
[2PC] Vote count for seq 1: 3/3
[2PC] All votes received for seq: 1. Broadcasting GLOBAL COMMIT.
[2PC] GLOBAL_COMMIT -> peer id=5 / 9 / 13
[2PC] 2PC complete for seq: 1. Starting PBFT consensus.
```

Throughput driver (500 sequential SETs via `kv_service_tools`, each a fresh process):
```
Transactions:        500
Elapsed (sec):       0.7395
Throughput (txn/s):  676.1
Avg latency (ms):    1.479
```

Server-side: 8 distinct `2PC complete` rounds on the coordinator (shard 1) — i.e. ResponseManager batched ~62 client ops per round. Each round drove a full cross-shard 2PC + intra-shard PBFT.

Durability check — 12 sampled keys across the range:
```
k1 -> v1 OK,  k50 -> v50 OK,  k100 -> v100 OK,
k150 -> v150 OK, k200 -> v200 OK, k250 -> v250 OK,
k300 -> v300 OK, k350 -> v350 OK, k400 -> v400 OK,
k450 -> v450 OK, k499 -> v499 OK, k500 -> v500 OK
hits: 12 / 12
```

Round-robin caveat: only shard 1 acted as coordinator during the shell loop because each fresh `kv_service_tools` process resets the round-robin pointer in `TransactionConstructor` to index 0. Inside one long-lived client session the four leaders alternate as designed. A future C++ benchmark driver should keep one client alive across many SETs to exercise that path.

---

## Files modified vs Owen's branch (this work)

| Path | Lines | What |
|---|---|---|
| `platform/consensus/ordering/pbft/response_manager.cpp` | +1 | Add `!config_.GetCrossShardPeers().empty()` to the BatchProposeMsg gate. |
| `platform/networkstrate/replica_communicator.cpp` | ±1 | Fix `SendSingleMessage` short-conn target. |
| `platform/consensus/ordering/pbft/commitment.cpp` | +1 include, ±~20 | Direct `NetChannel` sends for cross-shard PREPARE / VOTE / GLOBAL_COMMIT, plus diagnostic LOG lines. |
| `Docker/Dockerfile_mac`, `DomNotes/Startup-sharded.md` | new content | Apple-Silicon-native sharded image (`dom-sharded`) + run notes. |

## Open follow-ups

- Replace the shell-loop benchmark with a single-process C++ driver to actually exercise client-side round-robin and get a steady-state throughput number across all four coordinators.
- The diagnostic `LOG(ERROR) << "[2PC] ..."` lines are useful while iterating but should be downgraded to `VLOG(1)` or removed before submission.
- The direct `NetChannel` cross-shard send is unsigned. For an academic submission this matches the "no concurrency control / no aborts" simplification in the spec; for a real deployment, cross-shard keys would need to be exchanged (e.g. extend the heartbeat to publish each leader's key to its cross-shard peers).
