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
# Local sharded KV benchmark (no SSH, no deploy_local): same idea as
# pbft_performance.sh + run_performance.sh, but for the 4x4 shard layout
# started by scripts/deploy/script/start_sharded_kv_cluster.sh.
#
# Prerequisite for BENCH with AUTO_START_CLUSTER=1 (default): certs under
# CERT_DIR/cert/ (see generate_sharded_configs.sh). The script starts 16
# kv_service, warms up, runs the bench, analyzes logs, cools down, then kills
# replicas (trap also kills on failure). Set AUTO_START_CLUSTER=0 if the
# cluster is already running.
#
# Usage from repository root:
#   bash scripts/deploy/performance_local/sharded_performance_local.sh [OPS] [CERT_DIR]
#
# Defaults:
#   OPS=100
#   CERT_DIR=<repo>/scripts/deploy/config_out_sharded  (must contain logs/ and cert/)
#
# Environment (passed through to sharded_kv_performance.sh):
#   PIN_CLIENT=1|0   DO_GET=1|0
#   DIAG=1                                  — one SET + one GET with stderr
#                                             visible, plus a kv_1.log summary.
#   BENCH=1 [DURATION=60] [CONCURRENCY=8]   — auto-starts 16 replicas (default),
#                                             warmup, bench, four per-shard
#                                             coordinator throughput reports, stop.
#   AUTO_START_CLUSTER=0                    — skip start/stop; use existing cluster.
#   CLUSTER_READY_WAIT CLUSTER_WARMUP_SEC (default 20) COORDINATOR_LISTEN_WAIT_SEC (default 120)
#   POST_BENCH_STATS_WAIT COOLDOWN_SEC
#   KV_SERVICE_TOOLS SKIP_KV_SERVICE_TOOLS_BUILD — see sharded_kv_performance.sh
#   (default: bazel build then binary under `bazel info bazel-bin`).
#   See scripts/deploy/script/sharded_kv_performance.sh for full details.
#
# Example: 60s benchmark with 16 concurrent client loops:
#   BENCH=1 DURATION=60 CONCURRENCY=16 \
#     bash scripts/deploy/performance_local/sharded_performance_local.sh
#
set -eu

PERF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# performance_local -> deploy -> scripts -> repo root
REPO_ROOT="$(cd "${PERF_DIR}/../../.." && pwd)"
SCRIPT_DIR="$(cd "${PERF_DIR}/../script" && pwd)"

OPS="${1:-100}"
CERT_ROOT="${2:-${REPO_ROOT}/scripts/deploy/config_out_sharded}"

exec bash "${SCRIPT_DIR}/sharded_kv_performance.sh" "${REPO_ROOT}" "${OPS}" "${CERT_ROOT}"
