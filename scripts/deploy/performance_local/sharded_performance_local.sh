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
# Local sharded KV benchmark (no SSH): same fixed timing as
# performance_local/run_performance.sh (60s run, single client driver).
#
# Prerequisite: certs under scripts/deploy/config_out_sharded/cert/
# (see generate_sharded_configs.sh).
#
# Usage from repository root:
#   bash scripts/deploy/performance_local/sharded_performance_local.sh
#
set -eu

PERF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${PERF_DIR}/../../.." && pwd)"
SCRIPT_DIR="$(cd "${PERF_DIR}/../script" && pwd)"

export BENCH=1

exec bash "${SCRIPT_DIR}/sharded_kv_performance.sh" "${REPO_ROOT}"
