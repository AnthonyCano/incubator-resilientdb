#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Drive kv_service_tools against the 4-shard leader client config.
#
# Modes:
#   correctness (default) — N (SET, GET) pairs, count successes/failures.
#   DIAG=1                — single SET + single GET with stderr visible, plus
#                           a leader-log summary so you can see what is wrong.
#   BENCH=1               — by default: start 16 replicas, readiness wait,
#                           cluster warmup, parallel SET flood for DURATION,
#                           stats wait, per-shard coordinator throughput (four
#                           calculate_result.py runs on primaries 1,5,9,13),
#                           cooldown, killall kv_service (trap cleans up on exit).
#
# Usage:
#   ./sharded_kv_performance.sh [WORKSPACE_ROOT]
#
# From repo root (local, no SSH — same fixed 60s / single client as
# performance_local/run_performance.sh):
#   bash scripts/deploy/performance_local/sharded_performance_local.sh
#
# Fixed benchmark parameters (match run_performance.sh sleep 60 + one client):
#   BENCH=1, DURATION=60, CONCURRENCY=1
#
# Optional overrides (advanced only):
#   DIAG=1                 — one SET + one GET diagnostic, then exit.
#   BENCH=0                — correctness loop (100 SET/GET pairs) instead of bench.
#   AUTO_START_CLUSTER=0   — cluster already running.
#   KV_SERVICE_TOOLS, SKIP_KV_SERVICE_TOOLS_BUILD — client binary path.
#
# Assignment alignment (sharded 2PC + PBFT):
#   • Fixed 4 shards × 4 replicas: server JSON under scripts/deploy/config/sharded/.
#   • Proxy batch round-robin: TransactionConstructor + clientBatchNum in
#     client.shard_leaders.config (multiShardClientRoundRobin=true).
#   • Coordinator = PBFT primary that receives the client batch; 2PC to
#     crossShardPeer leaders; PBFT after GLOBAL_COMMIT — platform/consensus/
#     ordering/pbft/commitment.cpp.
#   • Client replies follow existing ResDB response path (see assignment options
#     in the handout if you need only the coordinator shard to answer).
#
# Outputs (fixed names — every run overwrites the previous one):
#   $CERT_DIR/logs/sharded_perf.txt              — run log
#   $CERT_DIR/logs/sharded_perf_2pc.txt          — [2PC] log snippets
#   BENCH mode also writes:
#   $CERT_DIR/logs/sharded_bench_kv_{1,5,9,13}.slice — coordinator-primary slices
#   $CERT_DIR/logs/sharded_bench_results.log         — combined four-shard report
#   $CERT_DIR/logs/sharded_bench_shard{1..4}_results.log — one file per shard
#
set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE="$(cd "${1:-$(cd "${SCRIPT_DIR}/../../.." && pwd)}" && pwd)"
CERT_ROOT="${WORKSPACE}/scripts/deploy/config_out_sharded"
mkdir -p "${CERT_ROOT}"
CERT_ROOT=$(cd "${CERT_ROOT}" && pwd)
LOG_DIR="${CERT_ROOT}/logs"
mkdir -p "${LOG_DIR}"

# Wipe stale artifacts from prior runs so the logs folder doesn't bloat.
# We only touch this script's own output files (server kv_*.log files are
# left alone — those are owned by start_sharded_kv_cluster.sh).
rm -f \
  "${LOG_DIR}"/sharded_perf_*.txt \
  "${LOG_DIR}"/sharded_bench_*.slice \
  "${LOG_DIR}"/sharded_bench_*_results.log \
  "${LOG_DIR}"/sharded_bench_results.log \
  "${LOG_DIR}"/client.shard_leaders.bench.*.config \
  "${LOG_DIR}"/client.shard_leaders.perf.*.config \
  2>/dev/null || true

STATIC_CFG="${WORKSPACE}/scripts/deploy/config/sharded"
CLIENT_SRC="${STATIC_CFG}/client.shard_leaders.config"
# Fixed defaults aligned with performance_local/run_performance.sh (60s, one client).
PIN_CLIENT=1
DO_GET=1
DIAG="${DIAG:-0}"
BENCH="${BENCH:-1}"
DURATION=60
CONCURRENCY=8
AUTO_START_CLUSTER="${AUTO_START_CLUSTER:-1}"
CLUSTER_READY_WAIT=90
CLUSTER_WARMUP_SEC=20
COORDINATOR_LISTEN_WAIT_SEC=120
POST_BENCH_STATS_WAIT=5
COOLDOWN_SEC=5
OPS=100

auto_started_kv=0
declare -A STARTUP_LOG_OFFSETS

CALC_PY="${WORKSPACE}/scripts/deploy/performance_local/calculate_result.py"

if [[ ! -f "${CLIENT_SRC}" ]]; then
  echo "Missing client config: ${CLIENT_SRC}" >&2
  exit 1
fi

cd "${WORKSPACE}"

BAZEL_BIN_FALLBACK="${WORKSPACE}/bazel-bin"
KV_REL_BIN="service/tools/kv/api_tools/kv_service_tools"

if [[ -n "${KV_SERVICE_TOOLS:-}" ]]; then
  kv_in="${KV_SERVICE_TOOLS}"
  case "${kv_in}" in
    /*) ;;
    *) kv_in="${WORKSPACE}/${kv_in}" ;;
  esac
  if [[ ! -x "${kv_in}" ]]; then
    echo "KV_SERVICE_TOOLS is missing or not executable: ${kv_in}" >&2
    exit 1
  fi
  TOOL="$(cd "$(dirname "${kv_in}")" && pwd)/$(basename "${kv_in}")"
elif [[ "${SKIP_KV_SERVICE_TOOLS_BUILD:-0}" == "1" ]]; then
  BAZEL_BIN="$(bazel info bazel-bin 2>/dev/null || true)"
  if [[ -z "${BAZEL_BIN}" ]]; then
    BAZEL_BIN="${BAZEL_BIN_FALLBACK}"
  fi
  TOOL="${BAZEL_BIN}/${KV_REL_BIN}"
  if [[ ! -x "${TOOL}" ]]; then
    echo "SKIP_KV_SERVICE_TOOLS_BUILD=1 but binary not found: ${TOOL}" >&2
    exit 1
  fi
else
  bazel build //service/tools/kv/api_tools:kv_service_tools
  BAZEL_BIN="$(bazel info bazel-bin 2>/dev/null || true)"
  if [[ -z "${BAZEL_BIN}" ]]; then
    BAZEL_BIN="${BAZEL_BIN_FALLBACK}"
  fi
  TOOL="${BAZEL_BIN}/${KV_REL_BIN}"
  if [[ ! -x "${TOOL}" ]]; then
    echo "Expected kv_service_tools after build: ${TOOL}" >&2
    exit 1
  fi
fi

RUN_LOG="${LOG_DIR}/sharded_perf.txt"
SNIP_LOG="${LOG_DIR}/sharded_perf_2pc.txt"
# Truncate fixed-name outputs so the new run starts from a clean slate.
: >"${RUN_LOG}"
: >"${SNIP_LOG}"
echo "kv_service_tools: ${TOOL}" | tee -a "${RUN_LOG}"

# Pinned client config (single leader) is useful for correctness + diag.
# For BENCH we want all four leaders driven concurrently. With
# multiShardClientRoundRobin=true, TransactionConstructor uses a host-wide mmap
# send counter so one short-lived process per SET still rotates shard leaders;
# every concurrent bench slot uses the same client JSON.
if [[ "${BENCH}" == "1" ]]; then
  CLIENT_USE="${LOG_DIR}/client.shard_leaders.bench.config"
  sed 's/"multiShardClientRoundRobin": false/"multiShardClientRoundRobin": true/' \
    "${CLIENT_SRC}" >"${CLIENT_USE}"
  echo "BENCH: round-robin client config -> ${CLIENT_USE}" | tee -a "${RUN_LOG}"
elif [[ "${PIN_CLIENT}" == "1" ]]; then
  CLIENT_USE="${LOG_DIR}/client.shard_leaders.perf.config"
  sed 's/"multiShardClientRoundRobin": true/"multiShardClientRoundRobin": false/' \
    "${CLIENT_SRC}" >"${CLIENT_USE}"
  echo "Using pinned client config (multiShardClientRoundRobin=false): ${CLIENT_USE}" | tee -a "${RUN_LOG}"
else
  CLIENT_USE="${CLIENT_SRC}"
  echo "Using committed client config (round-robin if enabled in JSON)." | tee -a "${RUN_LOG}"
fi

cleanup_client_config() {
  case "${CLIENT_USE}" in
    "${LOG_DIR}/client.shard_leaders.perf.config" | \
    "${LOG_DIR}/client.shard_leaders.bench.config")
      [[ -f "${CLIENT_USE}" ]] && rm -f "${CLIENT_USE}"
      ;;
  esac
}

stop_kv_if_auto_started() {
  if [[ "${auto_started_kv}" -eq 1 ]]; then
    echo "Stopping kv_service (auto-started cluster)..." >&2
    if command -v killall >/dev/null 2>&1; then
      killall -9 kv_service 2>/dev/null || true
    else
      pkill -9 -f kv_service 2>/dev/null || true
    fi
    auto_started_kv=0
  fi
}

cleanup_on_exit() {
  cleanup_client_config
  stop_kv_if_auto_started
}

trap cleanup_on_exit EXIT

extract_2pc_snippets() {
  local out="$1"
  : >"${out}"
  for id in 1 5 9 13; do
    local f="${LOG_DIR}/kv_${id}.log"
    if [[ ! -f "${f}" ]]; then
      continue
    fi
    {
      echo "======== kv_${id}.log (grep -m 120 -F '[2PC]') ========"
      grep -m 120 -F '[2PC]' "${f}" 2>/dev/null || echo "(no [2PC] lines matched)"
      echo ""
    } >>"${out}"
  done
}

leader_log_size() {
  local id="$1"
  local f="${LOG_DIR}/kv_${id}.log"
  if [[ -f "${f}" ]]; then
    stat -c %s "${f}" 2>/dev/null || wc -c <"${f}"
  else
    echo 0
  fi
}

# PBFT primary listen ports for shards 1–4 (see server/shard*.server.config).
primary_port_open() {
  local port="$1"
  if command -v nc >/dev/null 2>&1; then
    nc -z -w1 127.0.0.1 "${port}" >/dev/null 2>&1
    return $?
  fi
  # Bash built-in TCP (common on Linux/WSL).
  timeout 0.4 bash -c "exec 3<>/dev/tcp/127.0.0.1/${port}" >/dev/null 2>&1
}

wait_coordinator_primary_ports() {
  local max_wait="$1"
  local ports=(18001 18011 18021 18031)
  local deadline=$(($(date +%s) + max_wait))
  while [[ "$(date +%s)" -lt "${deadline}" ]]; do
    local ok=1
    for p in "${ports[@]}"; do
      if ! primary_port_open "${p}"; then
        ok=0
        break
      fi
    done
    if [[ "${ok}" -eq 1 ]]; then
      return 0
    fi
    sleep 0.5
  done
  return 1
}

check_cluster_startup_health() {
  local bad=0
  for id in $(seq 1 16); do
    local f="${LOG_DIR}/kv_${id}.log"
    if [[ ! -f "${f}" ]]; then
      echo "warn: missing startup log ${f}" | tee -a "${RUN_LOG}"
      bad=1
      continue
    fi
    local off="${STARTUP_LOG_OFFSETS[$id]:-0}"
    local scan_file="${f}"
    if [[ "${off}" -gt 0 ]]; then
      scan_file="$(mktemp)"
      tail -c +$((off + 1)) "${f}" >"${scan_file}" 2>/dev/null || true
    fi
    if grep -Eq "double free|corruption \\(out\\)|Segmentation fault|Aborted|terminate called" "${scan_file}" 2>/dev/null; then
      echo "warn: unhealthy startup signal in ${f}" | tee -a "${RUN_LOG}"
      bad=1
    fi
    if [[ "${scan_file}" != "${f}" ]]; then
      rm -f "${scan_file}"
    fi
  done
  return "${bad}"
}

# ------------------------------------------------------------------ DIAG mode
if [[ "${DIAG}" == "1" ]]; then
  echo "OPS=${OPS} DIAG=1 log=${RUN_LOG}" | tee -a "${RUN_LOG}"
  key="diag-$(date +%s)"
  val="vdiag"

  echo "" | tee -a "${RUN_LOG}"
  echo "------- SET key=${key} (stderr visible) -------" | tee -a "${RUN_LOG}"
  "${TOOL}" --config "${CLIENT_USE}" --cmd set --key "${key}" --value "${val}" 2>&1 \
    | tee -a "${RUN_LOG}" || true

  # Give the cluster a moment to drive 2PC + PBFT + execute.
  sleep 2

  echo "" | tee -a "${RUN_LOG}"
  echo "------- GET key=${key} (stderr visible) -------" | tee -a "${RUN_LOG}"
  "${TOOL}" --config "${CLIENT_USE}" --cmd get --key "${key}" 2>&1 \
    | tee -a "${RUN_LOG}" || true

  echo "" | tee -a "${RUN_LOG}"
  echo "------- last 30 [2PC] lines from kv_1.log -------" | tee -a "${RUN_LOG}"
  grep -F '[2PC]' "${LOG_DIR}/kv_1.log" 2>/dev/null | tail -30 \
    | tee -a "${RUN_LOG}" || echo "(no [2PC] lines)" | tee -a "${RUN_LOG}"

  echo "" | tee -a "${RUN_LOG}"
  echo "------- last 5 stats lines from kv_1.log (propose/prepare/commit/execute) -------" | tee -a "${RUN_LOG}"
  grep -F 'execute:' "${LOG_DIR}/kv_1.log" 2>/dev/null | tail -5 \
    | tee -a "${RUN_LOG}" || echo "(no stats lines)" | tee -a "${RUN_LOG}"

  extract_2pc_snippets "${SNIP_LOG}"
  echo "2PC snippet file: ${SNIP_LOG}" | tee -a "${RUN_LOG}"
  exit 0
fi

# ----------------------------------------------------------------- BENCH mode
if [[ "${BENCH}" == "1" ]]; then
  START_SH="${WORKSPACE}/scripts/deploy/script/start_sharded_kv_cluster.sh"
  RESULT_LOG="${LOG_DIR}/sharded_bench_results.log"
  : >"${RESULT_LOG}"
  echo "BENCH duration=${DURATION}s concurrency=${CONCURRENCY} log=${RUN_LOG}" \
    | tee -a "${RUN_LOG}"
  echo "AUTO_START_CLUSTER=${AUTO_START_CLUSTER} CLUSTER_READY_WAIT=${CLUSTER_READY_WAIT} COORDINATOR_LISTEN_WAIT_SEC=${COORDINATOR_LISTEN_WAIT_SEC} CLUSTER_WARMUP_SEC=${CLUSTER_WARMUP_SEC} POST_BENCH_STATS_WAIT=${POST_BENCH_STATS_WAIT} COOLDOWN_SEC=${COOLDOWN_SEC}" \
    | tee -a "${RUN_LOG}"

  if [[ "${AUTO_START_CLUSTER}" == "1" ]]; then
    for id in $(seq 1 16); do
      STARTUP_LOG_OFFSETS[$id]=$(leader_log_size "$id")
    done
    if [[ ! -d "${CERT_ROOT}/cert" ]]; then
      echo "Missing ${CERT_ROOT}/cert. Run:" >&2
      echo "  bash scripts/deploy/script/generate_sharded_configs.sh \"\$(pwd)\" \"\$(pwd)/scripts/deploy/config_out_sharded\" 127.0.0.1" >&2
      exit 1
    fi
    echo "Starting 16 kv_service replicas..." | tee -a "${RUN_LOG}"
    bash "${START_SH}" "${WORKSPACE}" "${CERT_ROOT}"
    auto_started_kv=1

    echo "Waiting up to ${CLUSTER_READY_WAIT}s for 16 kv_service processes..." \
      | tee -a "${RUN_LOG}"
    deadline=$(($(date +%s) + CLUSTER_READY_WAIT))
    while true; do
      n="$(pgrep -f "${WORKSPACE}/bazel-bin/service/kv/kv_service" 2>/dev/null | wc -l | tr -d ' ')"
      if [[ "${n}" -ge 16 ]]; then
        echo "Detected ${n} kv_service process(es)." | tee -a "${RUN_LOG}"
        break
      fi
      if [[ "$(date +%s)" -ge "${deadline}" ]]; then
        echo "warn: gave up waiting for 16 kv_service (seen ${n}). Continuing." \
          | tee -a "${RUN_LOG}"
        break
      fi
      sleep 0.4
    done

    coord_wait="${COORDINATOR_LISTEN_WAIT_SEC}"
    echo "Waiting up to ${coord_wait}s for coordinator listen ports 18001 18011 18021 18031..." \
      | tee -a "${RUN_LOG}"
    if wait_coordinator_primary_ports "${coord_wait}"; then
      echo "All four shard-primary ports accepting TCP." | tee -a "${RUN_LOG}"
    else
      echo "warn: coordinator ports not all open within ${coord_wait}s (see kv_1 kv_5 kv_9 kv_13 logs; OOM/hang during pool init?)." \
        | tee -a "${RUN_LOG}"
    fi

    if [[ "${CLUSTER_WARMUP_SEC}" -gt 0 ]]; then
      echo "Cluster warmup ${CLUSTER_WARMUP_SEC}s..." | tee -a "${RUN_LOG}"
      sleep "${CLUSTER_WARMUP_SEC}"
    fi
    if ! check_cluster_startup_health; then
      echo "BENCH abort: cluster failed startup health checks." | tee -a "${RUN_LOG}"
      exit 1
    fi
  else
    echo "AUTO_START_CLUSTER=0: using already-running kv_service cluster." \
      | tee -a "${RUN_LOG}"
  fi

  # Byte offsets for coordinator primaries only (replica ids 1,5,9,13).
  declare -A SIZE_BEFORE
  for id in 1 5 9 13; do
    SIZE_BEFORE[$id]=$(leader_log_size "$id")
  done

  t0=$(date +%s)
  echo "BENCH client flood start wall=$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
    | tee -a "${RUN_LOG}"
  pids=()
  for j in $(seq 1 "${CONCURRENCY}"); do
    (
      i=0
      while :; do
        i=$((i + 1))
        now=$(date +%s)
        elapsed=$((now - t0))
        if [[ "${elapsed}" -ge "${DURATION}" ]]; then
          break
        fi
        "${TOOL}" --config "${CLIENT_USE}" --cmd set \
          --key "bench-${j}-${i}" --value "v" >/dev/null 2>&1 || true
      done
    ) &
    pids+=("$!")
  done
  for p in "${pids[@]}"; do
    wait "${p}" 2>/dev/null || true
  done
  echo "BENCH client flood end wall=$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
    | tee -a "${RUN_LOG}"
  t1=$(date +%s)
  elapsed=$((t1 - t0))
  if [[ "${elapsed}" -lt 1 ]]; then
    elapsed=1
  fi

  echo "BENCH client loop done in ${elapsed}s. Waiting ${POST_BENCH_STATS_WAIT}s for last stats window..." \
    | tee -a "${RUN_LOG}"
  sleep "${POST_BENCH_STATS_WAIT}"

  # Slice coordinator-primary logs only (one PBFT primary / 2PC coordinator per shard).
  SHARD_SLICES=()
  for id in 1 5 9 13; do
    f="${LOG_DIR}/kv_${id}.log"
    if [[ ! -f "${f}" ]]; then
      echo "warn: missing ${f} (shard coordinator primary id=${id})" | tee -a "${RUN_LOG}"
      continue
    fi
    out="${LOG_DIR}/sharded_bench_kv_${id}.slice"
    off="${SIZE_BEFORE[$id]:-0}"
    size_now=$(leader_log_size "$id")
    if [[ "${size_now}" -le "${off}" ]]; then
      echo "warn: kv_${id}.log did not grow during bench (before=${off} after=${size_now})" \
        | tee -a "${RUN_LOG}"
      continue
    fi
    tail -c +$((off + 1)) "${f}" >"${out}"
    SHARD_SLICES+=("${id}:${out}")
  done

  if [[ "${#SHARD_SLICES[@]}" -eq 0 ]]; then
    echo "BENCH: no coordinator-primary log slices (kv_1/5/9/13). Is the cluster running?" \
      | tee -a "${RUN_LOG}"
    exit 1
  fi

  if [[ ! -f "${CALC_PY}" ]]; then
    echo "Missing calculate_result.py at ${CALC_PY}" | tee -a "${RUN_LOG}"
    exit 1
  fi

  {
    echo ""
    echo "======== per-shard coordinator throughput (calculate_result.py each primary) ========"
    echo "duration_s=${elapsed} concurrency=${CONCURRENCY}"
    echo "Each block is the shard where that replica is PBFT primary / cross-shard 2PC coordinator for client-destined ops."
    echo ""
  } | tee -a "${RUN_LOG}" "${RESULT_LOG}"

  shard_idx=0
  for entry in "${SHARD_SLICES[@]}"; do
    id="${entry%%:*}"
    slice="${entry#*:}"
    case "${id}" in
      1) shard_idx=1 ;;
      5) shard_idx=2 ;;
      9) shard_idx=3 ;;
      13) shard_idx=4 ;;
      *) shard_idx="${id}" ;;
    esac
    shard_log="${LOG_DIR}/sharded_bench_shard${shard_idx}_results.log"
    {
      echo "=== Shard ${shard_idx} (coordinator primary replica_id=${id}) ==="
      echo "slice: ${slice}"
      python3 "${CALC_PY}" "${slice}"
      echo ""
    } | tee "${shard_log}" | tee -a "${RUN_LOG}" | tee -a "${RESULT_LOG}"
  done

  {
    echo "======== bench summary (per-shard files) ========"
    echo "combined: ${RESULT_LOG}"
    for s in 1 2 3 4; do
      f="${LOG_DIR}/sharded_bench_shard${s}_results.log"
      [[ -f "${f}" ]] && echo "shard${s}: ${f}"
    done
  } | tee -a "${RUN_LOG}"

  extract_2pc_snippets "${SNIP_LOG}"
  echo "2PC snippet file: ${SNIP_LOG}" | tee -a "${RUN_LOG}"
  echo "bench results: ${RESULT_LOG}" | tee -a "${RUN_LOG}"

  if [[ "${auto_started_kv}" -eq 1 ]]; then
    if [[ "${COOLDOWN_SEC}" -gt 0 ]]; then
      echo "Cooldown ${COOLDOWN_SEC}s before stopping replicas..." | tee -a "${RUN_LOG}"
      sleep "${COOLDOWN_SEC}"
    fi
    echo "Stopping kv_service (all replicas)..." | tee -a "${RUN_LOG}"
    stop_kv_if_auto_started
  fi

  exit 0
fi

# --------------------------------------------------------- correctness (loop)
ok=0
fail=0
get_ok=0
get_fail=0
t0=$(date +%s)

echo "OPS=${OPS} DO_GET=${DO_GET} log=${RUN_LOG}" | tee -a "${RUN_LOG}"

for i in $(seq 1 "${OPS}"); do
  key="sharded-perf-${i}"
  val="v${i}"
  # stderr captured to the run log so glog ERRORS (e.g. send request fail) show up.
  out_set="$("${TOOL}" --config "${CLIENT_USE}" --cmd set --key "${key}" --value "${val}" 2>>"${RUN_LOG}" || true)"
  echo "${out_set}" >>"${RUN_LOG}"
  if echo "${out_set}" | grep -q 'ret = 0'; then
    ok=$((ok + 1))
  else
    fail=$((fail + 1))
    echo "SET fail i=${i}" | tee -a "${RUN_LOG}"
  fi

  if [[ "${DO_GET}" == "1" ]]; then
    out_get="$("${TOOL}" --config "${CLIENT_USE}" --cmd get --key "${key}" 2>>"${RUN_LOG}" || true)"
    echo "${out_get}" >>"${RUN_LOG}"
    if echo "${out_get}" | grep -q "value = ${val}"; then
      get_ok=$((get_ok + 1))
    else
      get_fail=$((get_fail + 1))
      echo "GET fail i=${i}" | tee -a "${RUN_LOG}"
    fi
  fi

  if [[ $((i % 20)) -eq 0 ]]; then
    echo "... progress ${i}/${OPS} ok=${ok} fail=${fail}" | tee -a "${RUN_LOG}"
  fi
done

t1=$(date +%s)
elapsed=$((t1 - t0))
if [[ "${elapsed}" -lt 1 ]]; then
  elapsed=1
fi

{
  echo ""
  echo "======== summary ========"
  echo "elapsed_s=${elapsed} ops=${OPS}"
  echo "set_ok=${ok} set_fail=${fail}"
  if [[ "${DO_GET}" == "1" ]]; then
    echo "get_ok=${get_ok} get_fail=${get_fail}"
  fi
  # awk for fractional tps even when ops < elapsed.
  awk -v ok="${ok}" -v e="${elapsed}" 'BEGIN { printf "approx_set_tps=%.2f\n", ok / e }'
} | tee -a "${RUN_LOG}"

extract_2pc_snippets "${SNIP_LOG}"
echo "2PC snippet file: ${SNIP_LOG}" | tee -a "${RUN_LOG}"
