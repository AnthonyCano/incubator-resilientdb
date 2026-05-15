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
# Start 4x4 sharded kv_service processes (MiniSpanner-style layout).
#
# Usage:
#   ./start_sharded_kv_cluster.sh <WORKSPACE_ROOT> [CERT_DIR]
#
# Server/client JSON: scripts/deploy/config/sharded/ (committed).
# CERT_DIR: directory that contains a cert/ subdir (default: .../config_out_sharded).
# Prereq: run generate_sharded_configs.sh <WORKSPACE> <CERT_DIR's parent> once.
# Optional: KV_START_STAGGER_SEC=0.2 (default) — delay between launching each replica
# to reduce simultaneous huge malloc peaks (see sharded_kv_performance.sh).
#
set -eu

WORKSPACE=$(cd "$1" && pwd)
CERT_ROOT=$(cd "${2:-${WORKSPACE}/scripts/deploy/config_out_sharded}" && pwd)
STATIC_CFG="${WORKSPACE}/scripts/deploy/config/sharded"
LOG_DIR="${CERT_ROOT}/logs"
mkdir -p "${LOG_DIR}"

cd "${WORKSPACE}"

if command -v killall >/dev/null 2>&1; then
  killall -9 kv_service 2>/dev/null || true
else
  pkill -9 -f kv_service 2>/dev/null || true
fi

bazel build //service/kv:kv_service

CERT="${CERT_ROOT}/cert"
if [[ ! -d "${CERT}" ]]; then
  echo "Missing ${CERT}. Run: bash scripts/deploy/script/generate_sharded_configs.sh ${WORKSPACE} ${CERT_ROOT}" >&2
  exit 1
fi

KV="${WORKSPACE}/bazel-bin/service/kv/kv_service"

for idx in $(seq 1 16); do
  shard=$(( (idx - 1) / 4 + 1 ))
  CFG="${STATIC_CFG}/server/shard${shard}.server.config"
  PRI="${CERT}/node_${idx}.key.pri"
  CRT="${CERT}/cert_${idx}.cert"
  nohup "${KV}" "${CFG}" "${PRI}" "${CRT}" >"${LOG_DIR}/kv_${idx}.log" 2>&1 &
  echo "Started node ${idx} (shard ${shard}) pid $!"
  # Stagger starts so 16× huge LockFreeCollectorPools do not malloc simultaneously
  # (avoids primaries stuck/OOM before listen — e.g. shard3 kv_9 on 18021).
  sleep "${KV_START_STAGGER_SEC:-0.2}"
done

echo "All 16 kv_service processes launched. Logs: ${LOG_DIR}"
