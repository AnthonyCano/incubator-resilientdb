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

#include "platform/consensus/ordering/pbft/commitment.h"

#include <cstdlib>
#include <glog/logging.h>
#include <thread>
#include <unistd.h>

#include "common/utils/utils.h"
#include "interface/rdbc/net_channel.h"
#include "platform/consensus/ordering/pbft/transaction_utils.h"

namespace resdb {

namespace {
bool UseLegacyPbftAfter2PC() {
  const char* env = std::getenv("RESDB_USE_LEGACY_PBFT_AFTER_2PC");
  return env != nullptr && std::string(env) == "1";
}
}  // namespace

Commitment::Commitment(const ResDBConfig& config,
                       MessageManager* message_manager,
                       ReplicaCommunicator* replica_communicator,
                       SignatureVerifier* verifier)
    : config_(config),
      message_manager_(message_manager),
      stop_(false),
      replica_communicator_(replica_communicator),
      verifier_(verifier) {
  executed_thread_ = std::thread(&Commitment::PostProcessExecutedMsg, this);
  global_stats_ = Stats::GetGlobalStats();
  duplicate_manager_ = std::make_unique<DuplicateManager>(config);
  message_manager_->SetDuplicateManager(duplicate_manager_.get());

  global_stats_->SetProps(
      config_.GetSelfInfo().id(), config_.GetSelfInfo().ip(),
      config_.GetSelfInfo().port(), config_.GetConfigData().enable_resview(),
      config_.GetConfigData().enable_faulty_switch());
  global_stats_->SetPrimaryId(message_manager_->GetCurrentPrimary());

  twopc_watchdog_ = std::thread(&Commitment::TwoPCWatchdog, this);
}

Commitment::~Commitment() {
  stop_ = true;
  if (twopc_watchdog_.joinable()) {
    twopc_watchdog_.join();
  }
  if (executed_thread_.joinable()) {
    executed_thread_.join();
  }
}

void Commitment::SetPreVerifyFunc(
    std::function<bool(const Request& request)> func) {
  pre_verify_func_ = func;
}

void Commitment::SetNeedCommitQC(bool need_qc) { need_qc_ = need_qc; }

// Handle the user request and send a pre-prepare message to others.
// TODO if not a primary, redicet to the primary replica.
int Commitment::ProcessNewRequest(std::unique_ptr<Context> context,
                                  std::unique_ptr<Request> user_request) {
  if (context == nullptr || context->signature.signature().empty()) {
    LOG(ERROR) << "user request doesn't contain signature, reject";
    return -2;
  }

  if (uint64_t seq =
          duplicate_manager_->CheckIfExecuted(user_request->hash())) {
    LOG(ERROR) << "This request is already executed with seq: " << seq;
    user_request->set_seq(seq);
    message_manager_->SendResponse(std::move(user_request));
    return -2;
  }

  if (config_.GetSelfInfo().id() != message_manager_->GetCurrentPrimary()) {
    // LOG(ERROR) << "current node is not primary. primary:"
    //            << message_manager_->GetCurrentPrimary()
    //            << " seq:" << user_request->seq()
    //            << " hash:" << user_request->hash();
    LOG(INFO) << "NOT PRIMARY, Primary is "
              << message_manager_->GetCurrentPrimary();
    replica_communicator_->SendMessage(*user_request,
                                       message_manager_->GetCurrentPrimary());
    {
      std::lock_guard<std::mutex> lk(rc_mutex_);
      request_complained_.push(
          std::make_pair(std::move(context), std::move(user_request)));
    }

    return -3;
  }

  // check signatures
  bool valid = verifier_->VerifyMessage(user_request->data(),
                                        user_request->data_signature());
  if (!valid) {
    LOG(ERROR) << "request is not valid:"
               << user_request->data_signature().DebugString();
    LOG(ERROR) << " msg:" << user_request->data().size();
    return -2;
  }

  if (pre_verify_func_ && !pre_verify_func_(*user_request)) {
    LOG(ERROR) << " check by the user func fail";
    return -2;
  }

  global_stats_->IncClientRequest();
  if (duplicate_manager_->CheckAndAddProposed(user_request->hash())) {
    return -2;
  }
  auto seq = message_manager_->AssignNextSeq();

  // Artificially make the primary stop proposing new trasactions.

  if (!seq.ok()) {
    LOG(ERROR) << " seq fail";
    duplicate_manager_->EraseProposed(user_request->hash());
    global_stats_->SeqFail();
    Request request;
    request.set_type(Request::TYPE_RESPONSE);
    request.set_sender_id(config_.GetSelfInfo().id());
    request.set_proxy_id(user_request->proxy_id());
    request.set_ret(-2);
    request.set_hash(user_request->hash());

    replica_communicator_->SendMessage(request, request.proxy_id());
    return -2;
  }

  global_stats_->RecordStateTime("request");

  user_request->set_current_view(message_manager_->GetCurrentView());
  user_request->set_seq(*seq);
  user_request->set_sender_id(config_.GetSelfInfo().id());
  user_request->set_primary_id(config_.GetSelfInfo().id());

  // === 2PC Phase 1: PREPARE to participants ===
  LOG(ERROR) << "[2PC] Coordinator starting 2PC for seq: " << *seq;
  const std::vector<ReplicaInfo> cross_peers = config_.GetCrossShardPeers();
  ReplicaInfo coord_contact;
  coord_contact.set_id(config_.GetSelfInfo().id());
  coord_contact.set_ip(config_.GetSelfInfo().ip());
  coord_contact.set_port(config_.GetSelfInfo().port());

  if (!cross_peers.empty()) {
    // Cross-shard: other shard leaders vote; coordinator contact in client_info.
    // The long-conn pool (replica_communicator_) routes to port+10000 and
    // signs with the local shard's verifier, so it can't carry cross-shard
    // traffic. Use a direct short-conn NetChannel to the peer's listener.
    for (const auto& peer : cross_peers) {
      Request prep;
      prep.CopyFrom(*user_request);
      prep.set_type(Request::TYPE_2PC_PREPARE);
      *prep.mutable_client_info() = coord_contact;
      LOG(ERROR) << "[2PC] sending PREPARE seq=" << *seq
                 << " to peer id=" << peer.id() << " ip=" << peer.ip()
                 << " port=" << peer.port();
      NetChannel channel(peer.ip(), peer.port());
      const int psend = channel.SendRawMessage(prep);
      if (psend < 0) {
        LOG(ERROR) << "[2PC] PREPARE send FAILED peer id=" << peer.id()
                   << " ip=" << peer.ip() << " port=" << peer.port()
                   << " seq=" << *seq << " (votes may never arrive; 2PC watchdog will timeout)";
      }
    }
  } else {
    // Legacy intra-shard 2PC among local replicas.
    user_request->set_type(Request::TYPE_2PC_PREPARE);
    replica_communicator_->BroadCast(*user_request);
  }

  // Store request while waiting for votes
  {
    std::lock_guard<std::mutex> lk(twopc_mutex_);
    user_request->set_type(Request::TYPE_PRE_PREPARE);  // restore for later PBFT use
    pending_2pc_requests_[*seq] = std::move(user_request);
    twopc_started_at_[*seq] = std::chrono::steady_clock::now();
    twopc_voters_[*seq].clear();
  }

  const int expect_votes =
      cross_peers.empty() ? static_cast<int>(config_.GetReplicaNum()) - 1
                          : static_cast<int>(cross_peers.size());
  LOG(ERROR) << "[2PC] coordinator_replica_id=" << config_.GetSelfInfo().id()
             << " expect_votes=" << expect_votes << " seq=" << *seq
             << " cross_shard=" << (cross_peers.empty() ? 0 : 1);

  return 0;
}

// Receive the pre-prepare message from the primary.
// TODO check whether the sender is the primary.
int Commitment::ProcessProposeMsg(std::unique_ptr<Context> context,
                                  std::unique_ptr<Request> request) {
  if (global_stats_->IsFaulty() || context == nullptr ||
      context->signature.signature().empty()) {
    LOG(ERROR) << "user request doesn't contain signature, reject";
    return -2;
  }
  if (request->is_recovery()) {
    if (message_manager_->GetNextSeq() == 0 ||
        request->seq() == message_manager_->GetNextSeq()) {
      message_manager_->SetNextSeq(request->seq() + 1);
      while (!pending_recovery_.empty()) {
        LOG(ERROR) << " pending size:" << pending_recovery_.size()
                   << " first:" << pending_recovery_.begin()->first
                   << " next:" << message_manager_->GetNextSeq();
        if (pending_recovery_.begin()->first <=
            message_manager_->GetNextSeq()) {
          if (pending_recovery_.begin()->first ==
              message_manager_->GetNextSeq()) {
            message_manager_->SetNextSeq(pending_recovery_.begin()->first + 1);
            message_manager_->AddConsensusMsg(
                pending_recovery_.begin()->second.first->signature,
                std::move(pending_recovery_.begin()->second.second));
          }
          pending_recovery_.erase(pending_recovery_.begin());
        } else {
          break;
        }
      }

    } else if (request->seq() > message_manager_->GetNextSeq()) {
      uint64_t seq = request->seq();
      pending_recovery_[seq] =
          std::make_pair(std::move(context), std::move(request));
      return 0;
    } else if (!request->force_recovery()) {
      LOG(ERROR) << " recovery request not valid:"
                 << " current seq:" << message_manager_->GetNextSeq()
                 << " data seq:" << request->seq();
      return 0;
    }
    request->set_force_recovery(false);
    return message_manager_->AddConsensusMsg(context->signature,
                                             std::move(request));
  }

  if (request->sender_id() != message_manager_->GetCurrentPrimary()) {
    LOG(ERROR) << "the request is not from primary. sender:"
               << request->sender_id() << " seq:" << request->seq();
    return -2;
  }

  if (request->sender_id() != config_.GetSelfInfo().id()) {
    if (pre_verify_func_ && !pre_verify_func_(*request)) {
      LOG(ERROR) << " check by the user func fail";
      return -2;
    }
    // global_stats_->GetTransactionDetails(std::move(request));
    BatchUserRequest batch_request;
    batch_request.ParseFromString(request->data());
    batch_request.clear_createtime();
    std::string data;
    batch_request.SerializeToString(&data);
    // check signatures
    bool valid =
        verifier_->VerifyMessage(request->data(), request->data_signature());
    if (!valid) {
      LOG(ERROR) << "request is not valid:"
                 << request->data_signature().DebugString();
      LOG(ERROR) << " msg:" << request->data().size();
      return -2;
    }
    if (duplicate_manager_->CheckAndAddProposed(request->hash())) {
      LOG(INFO) << "The request is already proposed, reject";
      return -2;
    }
  }

  global_stats_->IncPropose();
  global_stats_->RecordStateTime("pre-prepare");
  std::unique_ptr<Request> prepare_request = resdb::NewRequest(
      Request::TYPE_PREPARE, *request, config_.GetSelfInfo().id());
  prepare_request->clear_data();

  // Add request to message_manager.
  // If it has received enough same requests(2f+1), broadcast the prepare
  // message.
  CollectorResultCode ret =
      message_manager_->AddConsensusMsg(context->signature, std::move(request));
  if (ret == CollectorResultCode::STATE_CHANGED) {
    replica_communicator_->BroadCast(*prepare_request);
  }
  return ret == CollectorResultCode::INVALID ? -2 : 0;
}

// If receive 2f+1 prepare message, broadcast a commit message.
int Commitment::ProcessPrepareMsg(std::unique_ptr<Context> context,
                                  std::unique_ptr<Request> request) {
  if (context == nullptr || context->signature.signature().empty()) {
    LOG(ERROR) << "user request doesn't contain signature, reject";
    return -2;
  }
  if (request->is_recovery()) {
    uint64_t seq = request->seq();
    CollectorResultCode ret = message_manager_->AddConsensusMsg(
        context->signature, std::move(request));
    if (ret == CollectorResultCode::STATE_CHANGED) {
      if (message_manager_->GetHighestPreparedSeq() < seq) {
        message_manager_->SetHighestPreparedSeq(seq);
      }
    }
    return ret;
  }
  // global_stats_->IncPrepare();
  std::unique_ptr<Request> commit_request = resdb::NewRequest(
      Request::TYPE_COMMIT, *request, config_.GetSelfInfo().id());
  commit_request->mutable_data_signature()->Clear();
  // Add request to message_manager.
  // If it has received enough same requests(2f+1), broadcast the commit
  // message.
  uint64_t seq = request->seq();
  CollectorResultCode ret =
      message_manager_->AddConsensusMsg(context->signature, std::move(request));
  if (ret == CollectorResultCode::STATE_CHANGED) {
    if (message_manager_->GetHighestPreparedSeq() < seq) {
      message_manager_->SetHighestPreparedSeq(seq);
    }
    // If need qc, sign the data
    if (need_qc_ && verifier_) {
      auto signature_or = verifier_->SignMessage(commit_request->hash());
      if (!signature_or.ok()) {
        LOG(ERROR) << "Sign message fail";
        return -2;
      }
      *commit_request->mutable_data_signature() = *signature_or;
      // LOG(ERROR) << "sign hash"
      //           << commit_request->data_signature().DebugString();
    }
    global_stats_->RecordStateTime("prepare");
    replica_communicator_->BroadCast(*commit_request);
  }
  return ret == CollectorResultCode::INVALID ? -2 : 0;
}

// If receive 2f+1 commit message, commit the request.
int Commitment::ProcessCommitMsg(std::unique_ptr<Context> context,
                                 std::unique_ptr<Request> request) {
  if (context == nullptr || context->signature.signature().empty()) {
    LOG(ERROR) << "user request doesn't contain signature, reject"
               << " context:" << (context == nullptr);
    return -2;
  }
  uint64_t seq = request->seq();
  if (request->is_recovery()) {
    return message_manager_->AddConsensusMsg(context->signature,
                                             std::move(request));
  }
  // Duplicate COMMIT after seq is committed: AddConsensusMsg returns
  // STATE_CHANGED (IsCommitted short-circuit), not INVALID; tests and
  // callers expect -2 for invalid/duplicate commits.
  if (message_manager_->IsSeqCommitted(seq)) {
    LOG(ERROR) << " duplicate COMMIT for already-committed seq:" << seq;
    return -2;
  }
  // global_stats_->IncCommit();
  // Add request to message_manager.
  // If it has received enough same requests(2f+1), message manager will
  // commit the request.
  CollectorResultCode ret =
      message_manager_->AddConsensusMsg(context->signature, std::move(request));
  if (ret == CollectorResultCode::STATE_CHANGED) {
    // LOG(ERROR)<<request->data().size();
    // global_stats_->GetTransactionDetails(request->data());
    global_stats_->RecordStateTime("commit");
  }
  return ret == CollectorResultCode::INVALID ? -2 : 0;
}

// =========== private threads ===========================
// If the transaction is executed, send back to the proxy.
int Commitment::PostProcessExecutedMsg() {
  while (!stop_) {
    auto batch_resp = message_manager_->GetResponseMsg();
    if (batch_resp == nullptr) {
      continue;
    }
    global_stats_->SendSummary();
    Request request;
    request.set_hash(batch_resp->hash());
    request.set_seq(batch_resp->seq());
    request.set_type(Request::TYPE_RESPONSE);
    request.set_sender_id(config_.GetSelfInfo().id());
    request.set_current_view(batch_resp->current_view());
    request.set_proxy_id(batch_resp->proxy_id());
    request.set_primary_id(batch_resp->primary_id());
    if (batch_resp->proxy_id() <= 0) {
      continue;
    }
    LOG(ERROR) << "send back to proxy:" << batch_resp->proxy_id();
    batch_resp->SerializeToString(request.mutable_data());
    replica_communicator_->SendMessage(request, request.proxy_id());
  }
  return 0;
}

DuplicateManager* Commitment::GetDuplicateManager() {
  return duplicate_manager_.get();
}

// =========== 2PC Handlers ===========================

// Participant: receive PREPARE from coordinator, vote YES
int Commitment::Process2PCPrepare(std::unique_ptr<Context> context,
                                  std::unique_ptr<Request> request) {
  uint64_t seq = request->seq();
  int coordinator_id = request->sender_id();
  LOG(ERROR) << "[2PC] Participant " << config_.GetSelfInfo().id()
             << " received PREPARE for seq: " << seq
             << " from coordinator: " << coordinator_id;

  // Cross-shard coordinator tags PREPARE with client_info; stash by hash until
  // GLOBAL COMMIT drives local PBFT on this shard leader.
  if (request->has_client_info() && !request->client_info().ip().empty()) {
    std::lock_guard<std::mutex> lk(participant_twopc_mutex_);
    participant_twopc_by_hash_[request->hash()] =
        std::make_unique<Request>(*request);
  }

  // Always vote YES (no aborts per assignment spec)
  Request vote;
  vote.set_type(Request::TYPE_2PC_VOTE);
  vote.set_seq(seq);
  vote.set_sender_id(config_.GetSelfInfo().id());
  vote.set_current_view(message_manager_->GetCurrentView());
  vote.set_hash(request->hash());
  vote.set_proxy_id(request->proxy_id());

  LOG(ERROR) << "[2PC] Participant " << config_.GetSelfInfo().id()
             << " voting YES for seq: " << seq;
  if (request->has_client_info() && !request->client_info().ip().empty()) {
    const auto& dest = request->client_info();
    LOG(ERROR) << "[2PC] sending VOTE seq=" << seq
               << " to coordinator id=" << dest.id() << " ip=" << dest.ip()
               << " port=" << dest.port();
    NetChannel channel(dest.ip(), dest.port());
    const int vsend = channel.SendRawMessage(vote);
    if (vsend < 0) {
      LOG(ERROR) << "[2PC] VOTE send FAILED to coordinator id=" << dest.id()
                 << " ip=" << dest.ip() << " port=" << dest.port()
                 << " seq=" << seq;
    }
  } else {
    replica_communicator_->SendMessage(vote, coordinator_id);
  }
  return 0;
}

// Coordinator: collect votes, when all received send GLOBAL_COMMIT then start PBFT
int Commitment::Process2PCVote(std::unique_ptr<Context> context,
                               std::unique_ptr<Request> request) {
  uint64_t seq = request->seq();
  int voter_id = request->sender_id();
  LOG(ERROR) << "[2PC] Coordinator received VOTE from replica " << voter_id
             << " for seq: " << seq;

  std::unique_ptr<Request> stored_request;
  bool all_votes_received = false;

  {
    std::lock_guard<std::mutex> lk(twopc_mutex_);
    if (pending_2pc_requests_.find(seq) == pending_2pc_requests_.end()) {
      LOG(WARNING) << "[2PC] VOTE for unknown or already-finished seq " << seq
                   << " from " << voter_id;
      return 0;
    }
    auto& voters = twopc_voters_[seq];
    if (!voters.insert(voter_id).second) {
      LOG(WARNING) << "[2PC] duplicate VOTE ignored seq=" << seq
                   << " voter=" << voter_id;
      return 0;
    }
    const int num_participants =
        config_.GetCrossShardPeers().empty()
            ? static_cast<int>(config_.GetReplicaNum()) - 1
            : static_cast<int>(config_.GetCrossShardPeers().size());
    LOG(ERROR) << "[2PC] Vote count for seq " << seq << ": "
               << static_cast<int>(voters.size()) << "/" << num_participants;

    if (static_cast<int>(voters.size()) >= num_participants) {
      all_votes_received = true;
      stored_request = std::move(pending_2pc_requests_[seq]);
      pending_2pc_requests_.erase(seq);
      twopc_voters_.erase(seq);
      twopc_started_at_.erase(seq);
    }
  }

  if (all_votes_received && stored_request) {
    // === 2PC Phase 2: Send GLOBAL COMMIT ===
    LOG(ERROR) << "[2PC] All votes received for seq: " << seq
               << ". Broadcasting GLOBAL COMMIT.";
    Request commit_msg;
    commit_msg.set_type(Request::TYPE_2PC_COMMIT);
    commit_msg.set_seq(seq);
    commit_msg.set_sender_id(config_.GetSelfInfo().id());
    commit_msg.set_current_view(message_manager_->GetCurrentView());
    commit_msg.set_hash(stored_request->hash());
    const std::vector<ReplicaInfo> cross_peers = config_.GetCrossShardPeers();
    if (!cross_peers.empty()) {
      // Cross-shard GLOBAL_COMMIT: direct NetChannel for the same reason as
      // PREPARE/VOTE.
      for (const auto& peer : cross_peers) {
        LOG(ERROR) << "[2PC] sending GLOBAL_COMMIT seq=" << seq
                   << " to peer id=" << peer.id() << " ip=" << peer.ip()
                   << " port=" << peer.port();
        NetChannel channel(peer.ip(), peer.port());
        const int gsend = channel.SendRawMessage(commit_msg);
        if (gsend < 0) {
          LOG(ERROR) << "[2PC] GLOBAL_COMMIT send FAILED peer id=" << peer.id()
                     << " ip=" << peer.ip() << " port=" << peer.port();
        }
      }
    }

    if (UseLegacyPbftAfter2PC()) {
      stored_request->set_type(Request::TYPE_PRE_PREPARE);
      replica_communicator_->BroadCast(*stored_request);
    } else {
      StartShardPaxos(std::move(stored_request));
    }
  }

  return 0;
}

// Participant: receive GLOBAL COMMIT from coordinator
int Commitment::Process2PCCommit(std::unique_ptr<Context> context,
                                 std::unique_ptr<Request> request) {
  uint64_t seq = request->seq();
  LOG(ERROR) << "[2PC] Participant " << config_.GetSelfInfo().id()
             << " received GLOBAL COMMIT for seq: " << seq;

  std::unique_ptr<Request> stashed;
  const std::string commit_hash = request->hash();
  {
    std::lock_guard<std::mutex> lk(participant_twopc_mutex_);
    auto it = participant_twopc_by_hash_.find(commit_hash);
    if (it != participant_twopc_by_hash_.end()) {
      stashed = std::move(it->second);
      participant_twopc_by_hash_.erase(it);
    }
  }
  if (!stashed) {
    // Local replicas receive commit then PrePrepare from coordinator; no stash.
    VLOG(1) << "[2PC] COMMIT without stashed prepare (expected on local backups).";
    return 0;
  }

  auto local_seq = message_manager_->AssignNextSeq();
  if (!local_seq.ok()) {
    LOG(ERROR) << "[2PC] AssignNextSeq failed on participant shard";
    {
      std::lock_guard<std::mutex> lk(participant_twopc_mutex_);
      participant_twopc_by_hash_[commit_hash] = std::move(stashed);
    }
    return -2;
  }
  LOG(ERROR) << "[2PC] Cross-shard participant " << config_.GetSelfInfo().id()
             << " starting local PBFT (coordinator seq was " << seq
             << ", local seq " << *local_seq << ")";
  stashed->set_seq(*local_seq);
  stashed->set_current_view(message_manager_->GetCurrentView());
  stashed->set_sender_id(config_.GetSelfInfo().id());
  stashed->set_primary_id(message_manager_->GetCurrentPrimary());
  // Cross-shard participants must not reply to client proxy; only the global
  // 2PC coordinator shard should emit client responses.
  stashed->set_proxy_id(0);
  stashed->set_need_response(false);
  if (verifier_) {
    auto sig_or = verifier_->SignMessage(stashed->data());
    if (!sig_or.ok()) {
      LOG(ERROR) << "[2PC] Re-sign of cross-shard batch failed for hash "
                 << commit_hash << "; skipping local Paxos";
      {
        std::lock_guard<std::mutex> lk(participant_twopc_mutex_);
        participant_twopc_by_hash_[commit_hash] = std::move(stashed);
      }
      return -2;
    }
    *stashed->mutable_data_signature() = *sig_or;
  }
  if (UseLegacyPbftAfter2PC()) {
    stashed->set_type(Request::TYPE_PRE_PREPARE);
    replica_communicator_->BroadCast(*stashed);
    return 0;
  }
  return StartShardPaxos(std::move(stashed));
}

void Commitment::TwoPCWatchdog() {
  constexpr std::chrono::seconds kTimeout(30);
  while (!stop_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    if (stop_) {
      break;
    }
    std::vector<uint64_t> timed_out;
    {
      std::lock_guard<std::mutex> lk(twopc_mutex_);
      const auto now = std::chrono::steady_clock::now();
      for (const auto& pr : pending_2pc_requests_) {
        const uint64_t seq = pr.first;
        auto ts_it = twopc_started_at_.find(seq);
        if (ts_it != twopc_started_at_.end() &&
            now - ts_it->second > kTimeout) {
          timed_out.push_back(seq);
        }
      }
    }
    for (uint64_t seq : timed_out) {
      Timeout2PC(seq);
    }
  }
}

void Commitment::Timeout2PC(uint64_t seq) {
  std::unique_ptr<Request> req;
  std::string hash;
  int proxy_id = 0;
  {
    std::lock_guard<std::mutex> lk(twopc_mutex_);
    auto it = pending_2pc_requests_.find(seq);
    if (it == pending_2pc_requests_.end()) {
      return;
    }
    req = std::move(it->second);
    pending_2pc_requests_.erase(it);
    twopc_voters_.erase(seq);
    twopc_started_at_.erase(seq);
    hash = req->hash();
    proxy_id = req->proxy_id();
  }
  duplicate_manager_->EraseProposed(hash);
  global_stats_->SeqFail();
  Request err;
  err.set_type(Request::TYPE_RESPONSE);
  err.set_sender_id(config_.GetSelfInfo().id());
  err.set_proxy_id(proxy_id);
  err.set_ret(-2);
  err.set_hash(hash);
  err.set_seq(seq);
  LOG(ERROR) << "[2PC] TIMEOUT waiting for votes seq=" << seq
             << " proxy_id=" << proxy_id
             << " — sending error response so client unblocks";
  replica_communicator_->SendMessage(err, err.proxy_id());
}

// ------------------------ Intra-shard Paxos (classic broadcast flow) ------------------------

std::unique_ptr<Context> Commitment::ContextFromDataSignature(
    const Request& request) {
  auto ctx = std::make_unique<Context>();
  if (request.has_data_signature() &&
      !request.data_signature().signature().empty()) {
    ctx->signature = request.data_signature();
  } else {
    ctx->signature.set_signature("paxos");
  }
  return ctx;
}

void Commitment::PaxosMaybeSendAccept(uint64_t seq) {
  std::unique_ptr<Request> accept_msg;
  uint64_t proposal = 0;
  {
    std::lock_guard<std::mutex> lk(paxos_mu_);
    auto it = paxos_leader_by_seq_.find(seq);
    if (it == paxos_leader_by_seq_.end() || !it->second->pending_txn) {
      return;
    }
    PaxosLeaderState& st = *it->second;
    if (st.accept_sent) {
      return;
    }
    if (static_cast<int>(st.promised_replicas.size()) <
        config_.GetMinDataReceiveNum()) {
      return;
    }
    st.accept_sent = true;
    proposal = st.proposal_num;

    accept_msg = std::make_unique<Request>(*st.pending_txn);
    if (st.best_prior_n > 0 && !st.best_prior_value.empty()) {
      accept_msg->set_data(st.best_prior_value);
      if (verifier_) {
        auto sig_or = verifier_->SignMessage(accept_msg->data());
        if (sig_or.ok()) {
          *accept_msg->mutable_data_signature() = *sig_or;
        }
      }
      st.pending_txn->set_data(st.best_prior_value);
      if (verifier_) {
        auto sig2 = verifier_->SignMessage(st.pending_txn->data());
        if (sig2.ok()) {
          *st.pending_txn->mutable_data_signature() = *sig2;
        }
      }
    }
  }

  accept_msg->set_type(Request::TYPE_PAXOS_ACCEPT);
  accept_msg->set_paxos_proposal_num(proposal);
  accept_msg->set_sender_id(message_manager_->GetCurrentPrimary());
  accept_msg->set_primary_id(message_manager_->GetCurrentPrimary());
  accept_msg->set_current_view(message_manager_->GetCurrentView());

  replica_communicator_->BroadCast(*accept_msg);

  auto ctx = ContextFromDataSignature(*accept_msg);
  auto local = std::make_unique<Request>(*accept_msg);
  ProcessPaxosAccept(std::move(ctx), std::move(local));
}

void Commitment::PaxosMaybeBroadcastLearn(uint64_t seq) {
  std::unique_ptr<Request> learn;
  {
    std::lock_guard<std::mutex> lk(paxos_mu_);
    auto it = paxos_leader_by_seq_.find(seq);
    if (it == paxos_leader_by_seq_.end() || !it->second->pending_txn) {
      return;
    }
    PaxosLeaderState& st = *it->second;
    if (st.learn_sent) {
      return;
    }
    if (static_cast<int>(st.accepted_replicas.size()) <
        config_.GetMinDataReceiveNum()) {
      return;
    }
    st.learn_sent = true;
    learn = std::make_unique<Request>(*st.pending_txn);
    const auto acc_it = paxos_acceptor_by_seq_.find(seq);
    if (acc_it != paxos_acceptor_by_seq_.end() &&
        !acc_it->second.accepted_value.empty()) {
      learn->set_data(acc_it->second.accepted_value);
    }
    if (verifier_) {
      auto sig_or = verifier_->SignMessage(learn->data());
      if (sig_or.ok()) {
        *learn->mutable_data_signature() = *sig_or;
      }
    }
  }

  learn->set_type(Request::TYPE_PAXOS_LEARN);
  learn->set_sender_id(message_manager_->GetCurrentPrimary());
  learn->set_primary_id(message_manager_->GetCurrentPrimary());
  learn->set_current_view(message_manager_->GetCurrentView());

  replica_communicator_->BroadCast(*learn);

  {
    std::lock_guard<std::mutex> lk(paxos_mu_);
    paxos_leader_by_seq_.erase(seq);
  }
}

int Commitment::StartShardPaxos(std::unique_ptr<Request> txn_request) {
  if (txn_request == nullptr) {
    return -2;
  }
  if (config_.GetSelfInfo().id() != message_manager_->GetCurrentPrimary()) {
    LOG(ERROR) << "[Paxos] only shard leader may propose";
    return -2;
  }
  const uint64_t seq = txn_request->seq();
  const std::string hash = txn_request->hash();
  const int64_t proxy_id = txn_request->proxy_id();
  uint64_t n = 0;
  {
    std::lock_guard<std::mutex> lk(paxos_mu_);
    n = ++paxos_proposal_counter_;
    auto st = std::make_unique<PaxosLeaderState>();
    st->proposal_num = n;
    st->pending_txn = std::move(txn_request);
    paxos_leader_by_seq_[seq] = std::move(st);
  }

  Request prepare;
  prepare.set_type(Request::TYPE_PAXOS_PREPARE);
  prepare.set_seq(seq);
  prepare.set_hash(hash);
  prepare.set_proxy_id(proxy_id);
  prepare.set_current_view(message_manager_->GetCurrentView());
  prepare.set_sender_id(message_manager_->GetCurrentPrimary());
  prepare.set_primary_id(message_manager_->GetCurrentPrimary());
  prepare.set_paxos_proposal_num(n);

  replica_communicator_->BroadCast(prepare);
  return 0;
}

int Commitment::ProcessPaxosPrepare(std::unique_ptr<Context> context,
                                    std::unique_ptr<Request> request) {
  if (context == nullptr || context->signature.signature().empty()) {
    return -2;
  }
  if (request->sender_id() != message_manager_->GetCurrentPrimary()) {
    LOG(ERROR) << "[Paxos] Prepare not from primary";
    return -2;
  }
  const uint64_t seq = request->seq();
  const uint64_t n = request->paxos_proposal_num();
  uint64_t reply_high = 0;
  std::string reply_val;
  {
    std::lock_guard<std::mutex> lk(paxos_mu_);
    auto& slot = paxos_acceptor_by_seq_[seq];
    if (n <= slot.promised && slot.promised != 0) {
      LOG(ERROR) << "[Paxos] acceptor rejects Prepare n=" << n << " seq=" << seq;
      return -2;
    }
    slot.promised = n;
    reply_high = slot.accepted_n;
    reply_val = slot.accepted_value;
  }

  Request promise;
  promise.set_type(Request::TYPE_PAXOS_PROMISE);
  promise.set_seq(seq);
  promise.set_hash(request->hash());
  promise.set_proxy_id(request->proxy_id());
  promise.set_current_view(message_manager_->GetCurrentView());
  promise.set_sender_id(config_.GetSelfInfo().id());
  promise.set_primary_id(message_manager_->GetCurrentPrimary());
  promise.set_paxos_proposal_num(n);
  promise.set_paxos_promise_highest_accept_num(reply_high);
  promise.set_paxos_promise_highest_accept_value(reply_val);

  replica_communicator_->SendMessage(promise, message_manager_->GetCurrentPrimary());
  return 0;
}

int Commitment::ProcessPaxosPromise(std::unique_ptr<Context> context,
                                    std::unique_ptr<Request> request) {
  if (context == nullptr || context->signature.signature().empty()) {
    return -2;
  }
  if (config_.GetSelfInfo().id() != message_manager_->GetCurrentPrimary()) {
    return 0;
  }
  const uint64_t seq = request->seq();
  const uint64_t n = request->paxos_proposal_num();
  bool ready = false;
  {
    std::lock_guard<std::mutex> lk(paxos_mu_);
    auto it = paxos_leader_by_seq_.find(seq);
    if (it == paxos_leader_by_seq_.end() || !it->second->pending_txn) {
      return 0;
    }
    PaxosLeaderState& st = *it->second;
    if (st.proposal_num != n) {
      return 0;
    }
    if (!st.promised_replicas.insert(request->sender_id()).second) {
      return 0;
    }
    if (request->paxos_promise_highest_accept_num() > st.best_prior_n) {
      st.best_prior_n = request->paxos_promise_highest_accept_num();
      st.best_prior_value = request->paxos_promise_highest_accept_value();
    }
    ready = static_cast<int>(st.promised_replicas.size()) >=
            config_.GetMinDataReceiveNum();
  }
  if (ready) {
    PaxosMaybeSendAccept(seq);
  }
  return 0;
}

int Commitment::ProcessPaxosAccept(std::unique_ptr<Context> context,
                                   std::unique_ptr<Request> request) {
  if (context == nullptr || context->signature.signature().empty()) {
    return -2;
  }
  if (request->sender_id() != message_manager_->GetCurrentPrimary()) {
    LOG(ERROR) << "[Paxos] Accept not from primary";
    return -2;
  }
  const uint64_t seq = request->seq();
  const uint64_t n = request->paxos_proposal_num();

  if (config_.GetSelfInfo().id() != message_manager_->GetCurrentPrimary()) {
    if (pre_verify_func_ && !pre_verify_func_(*request)) {
      return -2;
    }
    bool valid =
        verifier_->VerifyMessage(request->data(), request->data_signature());
    if (!valid) {
      LOG(ERROR) << "[Paxos] Accept value failed verify seq=" << seq;
      return -2;
    }
  }

  {
    std::lock_guard<std::mutex> lk(paxos_mu_);
    auto& slot = paxos_acceptor_by_seq_[seq];
    if (slot.promised > n) {
      LOG(ERROR) << "[Paxos] acceptor rejects Accept n=" << n << " promised="
                 << slot.promised << " seq=" << seq;
      return -2;
    }
    if (slot.accepted_n == n && slot.accepted_value == request->data()) {
      // duplicate — still notify leader once is enough; resend accepted
    } else {
      slot.accepted_n = n;
      slot.accepted_value = request->data();
    }
  }


  Request acc;
  acc.set_type(Request::TYPE_PAXOS_ACCEPTED);
  acc.set_seq(seq);
  acc.set_hash(request->hash());
  acc.set_proxy_id(request->proxy_id());
  acc.set_current_view(message_manager_->GetCurrentView());
  acc.set_sender_id(config_.GetSelfInfo().id());
  acc.set_primary_id(message_manager_->GetCurrentPrimary());
  acc.set_paxos_proposal_num(n);
  acc.clear_data();
  replica_communicator_->SendMessage(acc, message_manager_->GetCurrentPrimary());
  return 0;
}

int Commitment::ProcessPaxosAccepted(std::unique_ptr<Context> context,
                                     std::unique_ptr<Request> request) {
  if (context == nullptr || context->signature.signature().empty()) {
    return -2;
  }
  if (config_.GetSelfInfo().id() != message_manager_->GetCurrentPrimary()) {
    return 0;
  }
  const uint64_t seq = request->seq();
  const uint64_t n = request->paxos_proposal_num();
  const int64_t acc_id = request->sender_id();

  bool majority = false;
  {
    std::lock_guard<std::mutex> lk(paxos_mu_);
    auto it = paxos_leader_by_seq_.find(seq);
    if (it == paxos_leader_by_seq_.end() || !it->second->pending_txn) {
      return 0;
    }
    if (it->second->proposal_num != n) {
      return 0;
    }
    if (!it->second->accepted_replicas.insert(acc_id).second) {
      return 0;
    }
    majority = static_cast<int>(it->second->accepted_replicas.size()) >=
               config_.GetMinDataReceiveNum();
  }
  if (majority) {
    PaxosMaybeBroadcastLearn(seq);
  }
  return 0;
}

int Commitment::ProcessPaxosLearn(std::unique_ptr<Context> context,
                                  std::unique_ptr<Request> request) {
  if (context == nullptr || context->signature.signature().empty()) {
    return -2;
  }
  const uint64_t seq = request->seq();
  if (message_manager_->IsSeqCommitted(seq)) {
    return 0;
  }

  const int primary = message_manager_->GetCurrentPrimary();
  const int self_id = config_.GetSelfInfo().id();

  if (!request->data().empty() && request->sender_id() == primary) {
    if (self_id != primary) {
      if (pre_verify_func_ && !pre_verify_func_(*request)) {
        return -2;
      }
      if (!verifier_->VerifyMessage(request->data(), request->data_signature())) {
        LOG(ERROR) << "[Paxos] Learn verify failed seq=" << seq;
        return -2;
      }
    }
    const std::string learn_hash = request->hash();
    const int64_t learn_proxy = request->proxy_id();
    CollectorResultCode ret =
        message_manager_->AddConsensusMsg(context->signature, std::move(request));
    // Every replica (including primary) must broadcast an empty LEARN ack so
    // MessageManager can reach MinDataReceiveNum on TYPE_PAXOS_LEARN acks
    // (primary was previously silent, leaving backups with only 2 peers).
    Request ack;
    ack.set_type(Request::TYPE_PAXOS_LEARN);
    ack.set_seq(seq);
    ack.set_hash(learn_hash);
    ack.set_proxy_id(learn_proxy);
    ack.set_current_view(message_manager_->GetCurrentView());
    ack.set_sender_id(self_id);
    ack.set_primary_id(primary);
    ack.clear_data();
    auto actx = std::make_unique<Context>();
    if (verifier_) {
      auto sig_or = verifier_->SignMessage(ack.hash());
      if (sig_or.ok()) {
        actx->signature = *sig_or;
      } else {
        actx->signature.set_signature("paxos-learn-ack");
      }
    } else {
      actx->signature.set_signature("paxos-learn-ack");
    }
    replica_communicator_->BroadCast(ack);
    return ret == CollectorResultCode::INVALID ? -2 : 0;
  }

  if (request->data().empty()) {
    CollectorResultCode ret =
        message_manager_->AddConsensusMsg(context->signature, std::move(request));
    return ret == CollectorResultCode::INVALID ? -2 : 0;
  }

  return -2;
}

}  // namespace resdb
