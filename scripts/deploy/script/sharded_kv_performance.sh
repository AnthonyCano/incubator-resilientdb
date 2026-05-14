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
#   BENCH=1               — parallel SET flood for DURATION seconds, then
#                           run calculate_result.py over shard-leader logs to
#                           print max/avg throughput and avg client latency
#                           (same approach pbft_performance.sh uses).
#
# Usage:
#   ./sharded_kv_performance.sh <WORKSPACE_ROOT> [OPS] [CERT_DIR]
#
# From repo root (local, no SSH — same spirit as performance_local):
#   bash scripts/deploy/performance_local/sharded_performance_local.sh [OPS] [CERT_DIR]
#
# Environment:
#   PIN_CLIENT=1 (default) — copy client JSON with multiShardClientRoundRobin
#             false so SET then GET on the same key hits the same leader.
#   PIN_CLIENT=0           — use committed client.shard_leaders.config as-is.
#   DO_GET=1 (default)     — after each SET, GET the same key.
#   DO_GET=0               — SET only.
#
#   DIAG=1                 — diagnostic: one SET + one GET with stderr captured
#                            into the run log; also prints recent [2PC] lines
#                            and the most recent stats lines from kv_1.log.
#
#   BENCH=1                — throughput/latency benchmark mode.
#   DURATION=60 (default)  — bench wall-clock seconds.
#   CONCURRENCY=8 (default)— number of parallel kv_service_tools driver loops.
#   BENCH_WARMUP=5 (default) seconds discarded from the start of each leader
#                            log before calculate_result.py runs (the first
#                            stats window often shows TPS=0).
#
# Outputs (fixed names — every run overwrites the previous one):
#   $CERT_DIR/logs/sharded_perf.txt              — run log
#   $CERT_DIR/logs/sharded_perf_2pc.txt          — [2PC] log snippets
#   BENCH mode also writes:
#   $CERT_DIR/logs/sharded_bench_kv_{1,5,9,13}.slice  — log slices
#   $CERT_DIR/logs/sharded_bench_results.log          — calculate_result.py
#
set -eu

WORKSPACE=$(cd "$1" && pwd)
OPS="${2:-100}"
CERT_ROOT=$(cd "${3:-${WORKSPACE}/scripts/deploy/config_out_sharded}" && pwd)
LOG_DIR="${CERT_ROOT}/logs"
mkdir -p "${LOG_DIR}"

# Wipe stale artifacts from prior runs so the logs folder doesn't bloat.
# We only touch this script's own output files (server kv_*.log files are
# left alone — those are owned by start_sharded_kv_cluster.sh).
rm -f \
  "${LOG_DIR}"/sharded_perf_*.txt \
  "${LOG_DIR}"/sharded_bench_*.slice \
  "${LOG_DIR}"/sharded_bench_*_results.log \
  "${LOG_DIR}"/client.shard_leaders.bench.*.config \
  "${LOG_DIR}"/client.shard_leaders.perf.*.config \
  2>/dev/null || true

STATIC_CFG="${WORKSPACE}/scripts/deploy/config/sharded"
CLIENT_SRC="${STATIC_CFG}/client.shard_leaders.config"
PIN_CLIENT="${PIN_CLIENT:-1}"
DO_GET="${DO_GET:-1}"
DIAG="${DIAG:-0}"
BENCH="${BENCH:-0}"
DURATION="${DURATION:-60}"
CONCURRENCY="${CONCURRENCY:-8}"
BENCH_WARMUP="${BENCH_WARMUP:-5}"

CALC_PY="${WORKSPACE}/scripts/deploy/performance_local/calculate_result.py"

if [[ ! -f "${CLIENT_SRC}" ]]; then
  echo "Missing client config: ${CLIENT_SRC}" >&2
  exit 1
fi

cd "${WORKSPACE}"
bazel build //service/tools/kv/api_tools:kv_service_tools

TOOL="${WORKSPACE}/bazel-bin/service/tools/kv/api_tools/kv_service_tools"
RUN_LOG="${LOG_DIR}/sharded_perf.txt"
SNIP_LOG="${LOG_DIR}/sharded_perf_2pc.txt"
# Truncate fixed-name outputs so the new run starts from a clean slate.
: >"${RUN_LOG}"
: >"${SNIP_LOG}"

# Pinned client config (single leader) is useful for correctness + diag.
# For BENCH we want all four leaders driven concurrently. The in-process
# round-robin counter in TransactionConstructor is now seeded by
# (pid ^ steady_clock_ns), so even fork-per-set drivers distribute across
# leaders. We just hand every concurrent slot the same round-robin config.
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
trap cleanup_client_config EXIT

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
  RESULT_LOG="${LOG_DIR}/sharded_bench_results.log"
  : >"${RESULT_LOG}"
  echo "BENCH duration=${DURATION}s concurrency=${CONCURRENCY} log=${RUN_LOG}" \
    | tee -a "${RUN_LOG}"

  declare -A SIZE_BEFORE
  for id in 1 5 9 13; do
    SIZE_BEFORE[$id]=$(leader_log_size "$id")
  done

  t0=$(date +%s)
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
  t1=$(date +%s)
  elapsed=$((t1 - t0))
  if [[ "${elapsed}" -lt 1 ]]; then
    elapsed=1
  fi

  echo "BENCH client loop done in ${elapsed}s. Waiting 5s for last stats window..." \
    | tee -a "${RUN_LOG}"
  sleep 5

  # Slice each leader's log: keep only bytes appended during the bench window
  # (skip BENCH_WARMUP seconds-worth at the start because the first stats line
  # is almost always a 0-tps cold sample).
  RESULT_LOGS=()
  for id in 1 5 9 13; do
    f="${LOG_DIR}/kv_${id}.log"
    if [[ ! -f "${f}" ]]; then
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
    if [[ "${BENCH_WARMUP}" -gt 0 ]]; then
      # stats lines are emitted every 5s; drop the first (BENCH_WARMUP/5) blocks
      # of lines by stripping the first matching stats line, conservatively.
      :
    fi
    RESULT_LOGS+=("${out}")
  done

  if [[ "${#RESULT_LOGS[@]}" -eq 0 ]]; then
    echo "BENCH: no leader log slices to analyze. Is the cluster running?" \
      | tee -a "${RUN_LOG}"
    exit 1
  fi

  if [[ ! -f "${CALC_PY}" ]]; then
    echo "Missing calculate_result.py at ${CALC_PY}" | tee -a "${RUN_LOG}"
    exit 1
  fi

  echo "Running calculate_result.py over: ${RESULT_LOGS[*]}" | tee -a "${RUN_LOG}"
  python3 "${CALC_PY}" "${RESULT_LOGS[@]}" >"${RESULT_LOG}" 2>&1 || true
  {
    echo ""
    echo "======== bench summary ========"
    echo "duration_s=${elapsed} concurrency=${CONCURRENCY}"
    echo "leader_log_slices: ${RESULT_LOGS[*]}"
    echo "--- calculate_result.py ---"
    cat "${RESULT_LOG}"
  } | tee -a "${RUN_LOG}"

  extract_2pc_snippets "${SNIP_LOG}"
  echo "2PC snippet file: ${SNIP_LOG}" | tee -a "${RUN_LOG}"
  echo "bench results: ${RESULT_LOG}" | tee -a "${RUN_LOG}"
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
