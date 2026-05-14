#!/bin/bash
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
# Optional Docker entrypoint: 4 shards x 4 replicas (16 kv_service), local IP.
#
# Example (PowerShell: one line; default ENTRYPOINT runs this script):
#   docker build -f Docker/Dockerfile -t resdb:shared .
#   docker rm -f resdb-shard; docker run -d --name resdb-shard resdb:shared
#
set -e
cd /app
sed -i 's/\r$//' .bazelversion 2>/dev/null || true
find scripts/deploy/script scripts/deploy/performance_local -name '*.sh' \
  -exec sed -i 's/\r$//' {} + 2>/dev/null || true

bazel build //service/kv:kv_service //tools:key_generator_tools //tools:certificate_tools

OUT=/app/scripts/deploy/config_out_sharded
bash scripts/deploy/script/generate_sharded_configs.sh /app "${OUT}" 127.0.0.1
bash scripts/deploy/script/start_sharded_kv_cluster.sh /app "${OUT}"

echo "Sharded cluster up. Client config: /app/scripts/deploy/config/sharded/client.shard_leaders.config"
echo "Certificates: ${OUT}/cert"
tail -f /dev/null
