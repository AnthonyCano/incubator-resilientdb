/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "interface/rdbc/transaction_constructor.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <sys/mman.h>
#include <unistd.h>

#include <chrono>
#include <cerrno>
#include <cstring>
#include <mutex>

namespace resdb {

namespace {

// kv_service_tools runs one process per SET in the shell bench; each process
// only called PickDestReplica once, so a per-process counter made
// (n / clientBatchNum) % num_shards depend on (pid^time) and could systematically
// starve a shard. A tiny shared counter in a mmap'd file gives one global
// sequence across all short-lived clients on this host (Linux/WSL).
constexpr char kShardedClientSeqPath[] = "/tmp/resdb_sharded_client_send_seq";

std::atomic<uint64_t>* MmappedGlobalSendSeq() {
  static std::atomic<uint64_t>* ptr = nullptr;
  static std::mutex init_mu;
  std::lock_guard<std::mutex> lk(init_mu);
  if (ptr != nullptr) {
    return ptr;
  }
  int fd = open(kShardedClientSeqPath, O_RDWR | O_CREAT, 0666);
  if (fd < 0) {
    LOG(WARNING) << "open " << kShardedClientSeqPath << " failed: "
                 << strerror(errno);
    return nullptr;
  }
  if (ftruncate(fd, sizeof(uint64_t)) != 0) {
    LOG(WARNING) << "ftruncate " << kShardedClientSeqPath << " failed: "
                 << strerror(errno);
    close(fd);
    return nullptr;
  }
  void* mem =
      mmap(nullptr, sizeof(uint64_t), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  close(fd);
  if (mem == MAP_FAILED) {
    LOG(WARNING) << "mmap " << kShardedClientSeqPath << " failed: "
                 << strerror(errno);
    return nullptr;
  }
  ptr = reinterpret_cast<std::atomic<uint64_t>*>(mem);
  return ptr;
}

}  // namespace

TransactionConstructor::TransactionConstructor(const ResDBConfig& config)
    : NetChannel("", 0),
      config_(config),
      timeout_ms_(
          config.GetClientTimeoutMs()) {  // default 2s for process timeout
  socket_->SetRecvTimeout(timeout_ms_);
  // Seed proxy_send_round_ for non-sharded or mmap-fallback routing.
  const uint64_t pid_bits = static_cast<uint64_t>(getpid());
  const uint64_t time_bits = static_cast<uint64_t>(
      std::chrono::steady_clock::now().time_since_epoch().count());
  proxy_send_round_.store(pid_bits ^ time_bits, std::memory_order_relaxed);
}

void TransactionConstructor::PickDestReplica() {
  const std::vector<ReplicaInfo>& replicas = config_.GetReplicaInfos();
  if (replicas.empty()) {
    return;
  }
  if (config_.MultiShardClientRoundRobin() && replicas.size() > 1) {
    // Assignment-style routing: consecutive batches of clientBatchNum() sends
    // go to shard leaders 0,1,2,3,... in order. Uses a host-wide counter so
    // many short-lived kv_service_tools processes still advance the same
    // sequence (see MmappedGlobalSendSeq).
    uint32_t batch_size = config_.ClientBatchNum();
    if (batch_size < 1u) {
      batch_size = 1u;
    }
    uint64_t n = 0;
    if (std::atomic<uint64_t>* g = MmappedGlobalSendSeq()) {
      n = g->fetch_add(1, std::memory_order_relaxed);
    } else {
      n = proxy_send_round_.fetch_add(1, std::memory_order_relaxed);
    }
    const size_t idx = static_cast<size_t>(
        (n / static_cast<uint64_t>(batch_size)) % replicas.size());
    LOG(INFO) << "[proxy] batch_rr idx=" << idx
              << " leader_replica_id=" << replicas[idx].id()
              << " ip=" << replicas[idx].ip() << " port=" << replicas[idx].port()
              << " send_seq=" << n << " clientBatchNum=" << batch_size;
    NetChannel::SetDestReplicaInfo(replicas[idx]);
  } else {
    NetChannel::SetDestReplicaInfo(replicas[0]);
  }
}

absl::StatusOr<std::string> TransactionConstructor::GetResponseData(
    const Response& response) {
  std::string hash_;
  std::set<int64_t> hash_counter;
  std::string resp_str;
  for (const auto& each_resp : response.resp()) {
    // Check signature
    std::string hash = SignatureVerifier::CalculateHash(each_resp.data());

    if (!hash_.empty() && hash != hash_) {
      LOG(ERROR) << "hash not the same";
      return absl::InvalidArgumentError("hash not match");
    }
    if (hash_.empty()) {
      hash_ = hash;
      resp_str = each_resp.data();
    }
    hash_counter.insert(each_resp.signature().node_id());
  }
  // LOG(INFO) << "recv hash:" << hash_counter.size()
  //         << " need:" << config_.GetMinClientReceiveNum()
  //        << " data len:" << resp_str.size();
  if (hash_counter.size() >=
      static_cast<size_t>(config_.GetMinClientReceiveNum())) {
    return resp_str;
  }
  return absl::InvalidArgumentError("data not enough");
}

int TransactionConstructor::SendRequest(
    const google::protobuf::Message& message, Request::Type type) {
  PickDestReplica();
  return NetChannel::SendRequest(message, type, false);
}

int TransactionConstructor::SendRequest(
    const google::protobuf::Message& message,
    google::protobuf::Message* response, Request::Type type) {
  PickDestReplica();
  int ret = NetChannel::SendRequest(message, type, true);
  if (ret == 0) {
    std::string resp_str;
    int ret = NetChannel::RecvRawMessageData(&resp_str);
    if (ret >= 0) {
      if (!response->ParseFromString(resp_str)) {
        LOG(ERROR) << "parse response fail:" << resp_str.size();
        return -2;
      }
      return 0;
    }
  }
  return -1;
}

}  // namespace resdb
