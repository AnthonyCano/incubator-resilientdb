#!/usr/bin/env bash
# Host-side runner: starts dom-sharded if needed, builds kv_benchmark inside
# the container (if missing or --rebuild), and executes a long-lived
# single-process benchmark so PickDestReplica()'s round-robin actually
# rotates across all four shard leaders.
#
# Usage:
#   DomNotes/run_benchmark.sh                # 500 ops, default prefix
#   DomNotes/run_benchmark.sh 2000           # 2000 ops
#   DomNotes/run_benchmark.sh 2000 bench_    # custom key prefix
#   REBUILD=1 DomNotes/run_benchmark.sh      # force bazel rebuild
set -euo pipefail

CONTAINER="${CONTAINER:-dom-sharded}"
N="${1:-500}"
PREFIX="${2:-bench_}"
REBUILD="${REBUILD:-0}"

CFG=/app/scripts/deploy/config/sharded/client.shard_leaders.config
BIN=/app/bazel-bin/service/tools/kv/api_tools/kv_benchmark

# 1. Make sure the container is running.
if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    echo ">> starting existing container $CONTAINER"
    docker start "$CONTAINER" >/dev/null
  else
    echo "ERROR: container '$CONTAINER' does not exist. Build it first:" >&2
    echo "  docker build -t dom-sharded -f Docker/Dockerfile_mac ." >&2
    echo "  docker run -it --name dom-sharded dom-sharded" >&2
    exit 1
  fi
fi

# 2. Build the benchmark binary inside the container if needed.
if [[ "$REBUILD" = "1" ]] || ! docker exec "$CONTAINER" test -x "$BIN"; then
  REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
  echo ">> copying benchmark sources into $CONTAINER"
  docker cp "$REPO_ROOT/service/tools/kv/api_tools/kv_benchmark.cpp" \
    "$CONTAINER":/app/service/tools/kv/api_tools/kv_benchmark.cpp
  docker cp "$REPO_ROOT/service/tools/kv/api_tools/BUILD" \
    "$CONTAINER":/app/service/tools/kv/api_tools/BUILD
  echo ">> building kv_benchmark inside $CONTAINER"
  docker exec "$CONTAINER" bash -lc \
    "cd /app && bazel build //service/tools/kv/api_tools:kv_benchmark"
fi

# 3. Make sure the shard cluster is up (kv_service processes running).
if ! docker exec "$CONTAINER" pgrep -f kv_service >/dev/null; then
  echo ">> sharded cluster not running, starting it"
  docker exec "$CONTAINER" bash -lc \
    "cd /app && bash scripts/deploy/script/start_sharded_kv_cluster.sh /app /app/scripts/deploy/config_out_sharded"
  sleep 5
fi

# 4. Drive the benchmark.
echo ">> running $N ops (prefix=$PREFIX) in a single client process"
docker exec "$CONTAINER" "$BIN" "$CFG" "$N" "$PREFIX" 1

# 5. Print per-leader 2PC coordinator counts so we can see all four were used.
echo
echo ">> per-leader '2PC complete' counts (proves round-robin reached every shard):"
for id in 1 5 9 13; do
  count=$(docker exec "$CONTAINER" bash -lc \
    "grep -c '2PC complete' /app/scripts/deploy/config_out_sharded/logs/kv_${id}.log || true")
  echo "   kv_${id}.log  2PC complete: ${count}"
done
