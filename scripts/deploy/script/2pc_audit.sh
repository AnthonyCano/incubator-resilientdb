#!/bin/bash
# Comprehensive audit across all 16 nodes.
echo "=== leader 2PC counts ==="
for id in 1 5 9 13; do
  f=/app/scripts/deploy/config_out_sharded/logs/kv_${id}.log
  cs=$(grep -c 'Coordinator starting 2PC' $f)
  av=$(grep -c 'All votes received' $f)
  pr=$(grep -c 'received PREPARE for seq' $f)
  gc=$(grep -c 'received GLOBAL COMMIT for seq' $f)
  lp=$(grep -c 'starting local PBFT' $f)
  printf "kv_%-2d: coord_starts=%-4d all_votes=%-4d prepares_recv=%-4d global_commits_recv=%-4d local_pbft_starts=%-4d\n" "$id" "$cs" "$av" "$pr" "$gc" "$lp"
done

echo ""
echo "=== per-node PBFT msg type counts (type 3=PRE_PREPARE, 4=PREPARE, 5=COMMIT) ==="
for id in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16; do
  f=/app/scripts/deploy/config_out_sharded/logs/kv_${id}.log
  t3=$(grep -c "recv impl type:3 " $f)
  t4=$(grep -c "recv impl type:4 " $f)
  t5=$(grep -c "recv impl type:5 " $f)
  printf "kv_%-2d: pre_prepare=%-5d prepare=%-5d commit=%-5d\n" "$id" "$t3" "$t4" "$t5"
done

echo ""
echo "=== last monitor block per leader (key metrics) ==="
for id in 1 5 9 13; do
  f=/app/scripts/deploy/config_out_sharded/logs/kv_${id}.log
  line=$(grep -E 'execute done:[0-9]' $f | tail -1)
  printf "kv_%-2d: %s\n" "$id" "$line"
done

echo ""
echo "=== verification & connection errors ==="
for id in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16; do
  f=/app/scripts/deploy/config_out_sharded/logs/kv_${id}.log
  inv=$(grep -c 'is not valid' $f)
  cnf=$(grep -c 'connect fail' $f)
  printf "kv_%-2d: invalid=%-3d connect_fail=%-3d\n" "$id" "$inv" "$cnf"
done
