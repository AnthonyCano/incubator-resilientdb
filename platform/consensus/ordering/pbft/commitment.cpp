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

#include <glog/logging.h>
#include <unistd.h>

#include "common/utils/utils.h"
#include "interface/rdbc/net_channel.h"
#include "platform/consensus/ordering/pbft/transaction_utils.h"

namespace resdb {

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
}

Commitment::~Commitment() {
  stop_ = true;
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
  VLOG(1) << "[2PC] Coordinator starting 2PC for seq: " << *seq;
  const std::vector<ReplicaInfo> cross_peers = config_.GetCrossShardPeers();
  ReplicaInfo coord_contact;
  coord_contact.set_id(config_.GetSelfInfo().id());
  coord_contact.set_ip(config_.GetSelfInfo().ip());
  coord_contact.set_port(config_.GetSelfInfo().port());

  if (!cross_peers.empty()) {
    // Cross-shard: other shard leaders vote; coordinator contact in client_info.
    // Use a direct short-conn NetChannel (bypasses the same-shard long-conn pool)
    // so the PREPARE reaches a cross-shard peer's base listener port. Signature
    // verification is skipped here because cross-shard nodes haven't exchanged
    // public keys via heartbeats.
    for (const auto& peer : cross_peers) {
      Request prep;
      prep.CopyFrom(*user_request);
      prep.set_type(Request::TYPE_2PC_PREPARE);
      *prep.mutable_client_info() = coord_contact;
      VLOG(1) << "[2PC] sending PREPARE seq=" << *seq
                 << " to peer id=" << peer.id() << " ip=" << peer.ip()
                 << " port=" << peer.port();
      NetChannel channel(peer.ip(), peer.port());
      int sret = channel.SendRawMessage(prep);
      VLOG(1) << "[2PC] direct send ret=" << sret;
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
    twopc_voters_[*seq].clear();
  }

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
      // Cross-shard PRE_PREPARE: the data was signed by another shard's
      // leader, whose key this verifier doesn't know. Accept it iff the
      // signer is not a member of our local shard. Per the spec's
      // "no concurrency control / no aborts" simplification, cross-shard
      // pre-prepares are trusted.
      int64_t signer_id = request->data_signature().node_id();
      bool signer_is_local = false;
      for (const auto& replica : config_.GetReplicaInfos()) {
        if (replica.id() == signer_id) {
          signer_is_local = true;
          break;
        }
      }
      if (signer_is_local) {
        LOG(ERROR) << "request is not valid:"
                   << request->data_signature().DebugString();
        LOG(ERROR) << " msg:" << request->data().size();
        return -2;
      }
      VLOG(1) << "[2PC] accepting cross-shard PRE_PREPARE signed by node "
                 << signer_id << " (foreign shard)";
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
  VLOG(1) << "[2PC] Participant " << config_.GetSelfInfo().id()
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

  VLOG(1) << "[2PC] Participant " << config_.GetSelfInfo().id()
             << " voting YES for seq: " << seq;
  if (request->has_client_info() && !request->client_info().ip().empty()) {
    const auto& dest = request->client_info();
    VLOG(1) << "[2PC] vote -> coord ip=" << dest.ip()
               << " port=" << dest.port();
    NetChannel channel(dest.ip(), dest.port());
    channel.SendRawMessage(vote);
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
  VLOG(1) << "[2PC] Coordinator received VOTE from replica " << voter_id
             << " for seq: " << seq;

  std::unique_ptr<Request> stored_request;
  bool all_votes_received = false;

  {
    std::lock_guard<std::mutex> lk(twopc_mutex_);
    // Ignore votes for sequences we no longer track (already committed/timed out).
    if (pending_2pc_requests_.find(seq) == pending_2pc_requests_.end()) {
      return 0;
    }
    // Dedup: a duplicate vote from the same replica must not advance quorum.
    twopc_voters_[seq].insert(voter_id);
    const int num_participants =
        config_.GetCrossShardPeers().empty()
            ? static_cast<int>(config_.GetReplicaNum()) - 1
            : static_cast<int>(config_.GetCrossShardPeers().size());
    VLOG(1) << "[2PC] Vote count for seq " << seq << ": "
               << twopc_voters_[seq].size() << "/" << num_participants;

    if (static_cast<int>(twopc_voters_[seq].size()) >= num_participants) {
      all_votes_received = true;
      stored_request = std::move(pending_2pc_requests_[seq]);
      pending_2pc_requests_.erase(seq);
      twopc_voters_.erase(seq);
    }
  }

  if (all_votes_received && stored_request) {
    // === 2PC Phase 2: Send GLOBAL COMMIT ===
    VLOG(1) << "[2PC] All votes received for seq: " << seq
               << ". Broadcasting GLOBAL COMMIT.";
    Request commit_msg;
    commit_msg.set_type(Request::TYPE_2PC_COMMIT);
    commit_msg.set_seq(seq);
    commit_msg.set_sender_id(config_.GetSelfInfo().id());
    commit_msg.set_current_view(message_manager_->GetCurrentView());
    commit_msg.set_hash(stored_request->hash());
    const std::vector<ReplicaInfo> cross_peers = config_.GetCrossShardPeers();
    if (!cross_peers.empty()) {
      for (const auto& peer : cross_peers) {
        VLOG(1) << "[2PC] GLOBAL_COMMIT -> peer id=" << peer.id()
                   << " ip=" << peer.ip() << " port=" << peer.port();
        NetChannel channel(peer.ip(), peer.port());
        channel.SendRawMessage(commit_msg);
      }
    }
    replica_communicator_->BroadCast(commit_msg);

    // === Now proceed with PBFT: broadcast PrePrepare ===
    VLOG(1) << "[2PC] 2PC complete for seq: " << seq
               << ". Starting PBFT consensus.";
    stored_request->set_type(Request::TYPE_PRE_PREPARE);
    replica_communicator_->BroadCast(*stored_request);
  }

  return 0;
}

// Participant: receive GLOBAL COMMIT from coordinator
int Commitment::Process2PCCommit(std::unique_ptr<Context> context,
                                 std::unique_ptr<Request> request) {
  uint64_t seq = request->seq();
  VLOG(1) << "[2PC] Participant " << config_.GetSelfInfo().id()
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
    VLOG(1) << "[2PC] AssignNextSeq failed on participant shard";
    {
      std::lock_guard<std::mutex> lk(participant_twopc_mutex_);
      participant_twopc_by_hash_[commit_hash] = std::move(stashed);
    }
    return -2;
  }
  VLOG(1) << "[2PC] Cross-shard participant " << config_.GetSelfInfo().id()
             << " starting local PBFT (coordinator seq was " << seq
             << ", local seq " << *local_seq << ")";
  stashed->set_seq(*local_seq);
  stashed->set_current_view(message_manager_->GetCurrentView());
  stashed->set_sender_id(config_.GetSelfInfo().id());
  stashed->set_primary_id(message_manager_->GetCurrentPrimary());
  stashed->set_type(Request::TYPE_PRE_PREPARE);
  replica_communicator_->BroadCast(*stashed);
  return 0;
}

}  // namespace resdb
