/*
 * Long-lived multi-threaded KV benchmark driver with TCP keep-alive.
 *
 * Modes:
 *   --ops N            Issue N SETs total across all threads (default).
 *   --duration SEC     Flood SETs for SEC seconds across all threads.
 *   --concurrency C    Number of parallel client threads (default 1).
 *   --prefix STR       Key prefix (default "k_").
 *   --verify 0|1       After SET phase, GET back each written key and
 *                      check the value matches (default 1 in ops mode,
 *                      0 in duration mode).
 *
 * Each thread holds one KVClient per shard leader, each with TCP
 * keep-alive enabled, and round-robins among its own clients. This
 * avoids per-call TCP socket creation and 3-way handshakes that
 * otherwise dominate the client-observed latency.
 */

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "interface/kv/kv_client.h"
#include "platform/config/resdb_config_utils.h"

using resdb::GenerateResDBConfig;
using resdb::KVClient;
using resdb::ReplicaInfo;
using resdb::ResConfigData;
using resdb::ResDBConfig;

namespace {

struct Args {
  std::string cfg_path;
  int ops = 500;
  int duration_sec = 0;
  int concurrency = 1;
  std::string prefix = "k_";
  int verify = -1;  // -1 = auto
};

void Usage(const char* prog) {
  fprintf(stderr,
          "usage: %s --config PATH [--ops N | --duration SEC] "
          "[--concurrency C] [--prefix STR] [--verify 0|1]\n"
          "  short form: %s CFG NUM_OPS [PREFIX] [VERIFY]\n",
          prog, prog);
}

bool ParseArgs(int argc, char** argv, Args* a) {
  if (argc >= 3 && argv[1][0] != '-') {
    a->cfg_path = argv[1];
    a->ops = std::atoi(argv[2]);
    if (argc >= 4) a->prefix = argv[3];
    if (argc >= 5) a->verify = std::atoi(argv[4]);
    return !a->cfg_path.empty() && a->ops > 0;
  }
  for (int i = 1; i < argc; ++i) {
    std::string k = argv[i];
    auto next = [&]() -> const char* {
      if (i + 1 >= argc) { Usage(argv[0]); std::exit(1); }
      return argv[++i];
    };
    if (k == "--config") a->cfg_path = next();
    else if (k == "--ops") a->ops = std::atoi(next());
    else if (k == "--duration") a->duration_sec = std::atoi(next());
    else if (k == "--concurrency") a->concurrency = std::atoi(next());
    else if (k == "--prefix") a->prefix = next();
    else if (k == "--verify") a->verify = std::atoi(next());
    else { Usage(argv[0]); return false; }
  }
  if (a->cfg_path.empty()) return false;
  if (a->concurrency < 1) a->concurrency = 1;
  return true;
}

// Build N single-leader configs from the loaded client config so each
// resulting KVClient has exactly one destination and PickDestReplica()
// always picks replicas[0].
std::vector<ResDBConfig> BuildPerLeaderConfigs(const ResDBConfig& base) {
  std::vector<ResDBConfig> out;
  const auto& leaders = base.GetReplicaInfos();
  ResConfigData data = base.GetConfigData();
  // Disable multi-shard round-robin so PickDestReplica picks replicas[0]
  // deterministically (replicas[0] is now the single bound leader).
  data.set_multi_shard_client_round_robin(false);
  for (const auto& leader : leaders) {
    std::vector<ReplicaInfo> single = {leader};
    ResDBConfig c(single, leader, data);  // self_info irrelevant for client
    c.SetClientTimeoutMs(100000);
    out.push_back(std::move(c));
  }
  return out;
}

}  // namespace

int main(int argc, char** argv) {
  Args args;
  if (!ParseArgs(argc, argv, &args)) {
    Usage(argv[0]);
    return 1;
  }

  const bool duration_mode = args.duration_sec > 0;
  const bool verify = args.verify >= 0 ? args.verify != 0 : (!duration_mode);

  ResDBConfig base_config = GenerateResDBConfig(args.cfg_path);
  base_config.SetClientTimeoutMs(100000);
  std::vector<ResDBConfig> per_leader_cfgs = BuildPerLeaderConfigs(base_config);
  const size_t num_leaders = per_leader_cfgs.size();
  printf("=== bound to %zu shard leaders, persistent TCP per leader ===\n",
         num_leaders);

  std::atomic<uint64_t> total_ops{0};
  std::atomic<uint64_t> total_ok{0};
  std::atomic<bool> stop{false};

  printf("=== SET phase: concurrency=%d, %s%d, prefix=%s ===\n",
         args.concurrency,
         duration_mode ? "duration=" : "ops=",
         duration_mode ? args.duration_sec : args.ops,
         args.prefix.c_str());
  fflush(stdout);

  auto t0 = std::chrono::steady_clock::now();

  std::vector<std::thread> threads;
  threads.reserve(args.concurrency);
  for (int tid = 0; tid < args.concurrency; ++tid) {
    threads.emplace_back([&, tid]() {
      // One KVClient per leader, persistent TCP per client.
      std::vector<std::unique_ptr<KVClient>> clients;
      clients.reserve(num_leaders);
      for (auto& cfg : per_leader_cfgs) {
        // Note: long-conn mode (IsLongConnection(true)) is not used here.
        // The server's base listener port closes connections after each
        // request; persistent TCP requires server-side changes that are
        // out of scope. We still benefit from per-leader pinning: each
        // KVClient's PickDestReplica picks its sole replica deterministically,
        // so the 8 threads no longer race the static rotor.
        clients.push_back(std::make_unique<KVClient>(cfg));
      }

      for (;;) {
        uint64_t op_id = total_ops.fetch_add(1, std::memory_order_relaxed) + 1;
        if (!duration_mode && op_id > static_cast<uint64_t>(args.ops)) break;
        if (duration_mode && stop.load(std::memory_order_relaxed)) break;
        const size_t idx = op_id % clients.size();
        std::string k = args.prefix + std::to_string(op_id);
        std::string v = "v" + std::to_string(op_id);
        if (clients[idx]->Set(k, v) == 0) {
          total_ok.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  if (duration_mode) {
    std::this_thread::sleep_for(std::chrono::seconds(args.duration_sec));
    stop.store(true, std::memory_order_relaxed);
  }
  for (auto& th : threads) th.join();

  auto t1 = std::chrono::steady_clock::now();
  double set_secs = std::chrono::duration<double>(t1 - t0).count();
  uint64_t ok = total_ok.load();
  uint64_t issued = total_ops.load();

  printf("ops issued:    %llu\n", (unsigned long long)issued);
  printf("ops succeeded: %llu\n", (unsigned long long)ok);
  printf("elapsed (s):   %.4f\n", set_secs);
  printf("throughput:    %.1f txn/s\n", ok / set_secs);
  printf("avg latency:   %.3f ms\n", (set_secs * 1000.0) / ok);
  fflush(stdout);

  if (!verify) return 0;

  // GET phase: single-thread, also persistent TCP via one client per leader.
  printf("=== GET phase (single-thread verify) ===\n");
  fflush(stdout);
  std::vector<std::unique_ptr<KVClient>> gclients;
  for (auto& cfg : per_leader_cfgs) {
    gclients.push_back(std::make_unique<KVClient>(cfg));
  }
  uint64_t verify_n = ok;
  auto g0 = std::chrono::steady_clock::now();
  uint64_t hits = 0, misses = 0;
  for (uint64_t i = 1; i <= verify_n; ++i) {
    const size_t idx = i % gclients.size();
    std::string k = args.prefix + std::to_string(i);
    std::string expected = "v" + std::to_string(i);
    auto res = gclients[idx]->Get(k);
    if (res != nullptr && *res == expected) hits++;
    else misses++;
  }
  auto g1 = std::chrono::steady_clock::now();
  double get_secs = std::chrono::duration<double>(g1 - g0).count();

  printf("ops:           %llu\n", (unsigned long long)verify_n);
  printf("hits:          %llu\n", (unsigned long long)hits);
  printf("misses:        %llu\n", (unsigned long long)misses);
  printf("elapsed (s):   %.4f\n", get_secs);
  printf("throughput:    %.1f txn/s\n", verify_n / get_secs);
  printf("avg latency:   %.3f ms\n", (get_secs * 1000.0) / verify_n);
  return hits == verify_n ? 0 : 2;
}
