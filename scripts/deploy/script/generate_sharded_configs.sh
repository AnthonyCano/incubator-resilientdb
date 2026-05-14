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
# Generate keys and certificates for 4 shards x 4 replicas (16 nodes).
# Server/client JSON is committed under scripts/deploy/config/sharded/ — this
# script only materializes OUT/cert/ with node keys signed by deploy admin keys.
#
# Usage:
#   ./generate_sharded_configs.sh <WORKSPACE_ROOT> <OUTPUT_DIR> [IP]
#
set -eu

WORKSPACE=$(cd "$1" && pwd)
OUT=$(mkdir -p "$2" && cd "$2" && pwd)
IP="${3:-127.0.0.1}"

ADMIN_KEY_DIR="${WORKSPACE}/scripts/deploy/data/cert"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd "${WORKSPACE}"

if [[ ! -f "${ADMIN_KEY_DIR}/admin.key.pri" ]]; then
  echo "Missing admin keys at ${ADMIN_KEY_DIR}. Run deploy key setup first." >&2
  exit 1
fi

bazel build //tools:key_generator_tools //tools:certificate_tools

CERT_OUT="${OUT}/cert"
mkdir -p "${CERT_OUT}"

echo "Generating 16 node keypairs into ${CERT_OUT}"
"${SCRIPT_DIR}/generate_key.sh" "${WORKSPACE}" "${CERT_OUT}" 16

CERT_TOOLS="${WORKSPACE}/bazel-bin/tools/certificate_tools"
for idx in $(seq 1 16); do
  shard=$(( (idx - 1) / 4 + 1 ))
  rep=$(( (idx - 1) % 4 + 1 ))
  port=$(( 18001 + (shard - 1) * 10 + (rep - 1) ))
  "${CERT_TOOLS}" "${CERT_OUT}" \
    "${ADMIN_KEY_DIR}/admin.key.pri" \
    "${ADMIN_KEY_DIR}/admin.key.pub" \
    "${CERT_OUT}/node_${idx}.key.pub" \
    "${idx}" "${IP}" "${port}" replica
done

echo "Certificates ready under ${OUT}/cert"
echo "Static JSON (edit in repo): ${WORKSPACE}/scripts/deploy/config/sharded/"
echo "  server/shard{1..4}.server.config"
echo "  client.shard_leaders.config"
