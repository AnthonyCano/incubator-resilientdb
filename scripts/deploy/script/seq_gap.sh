#!/bin/bash
for id in 1 5 9 13; do
  f=/app/scripts/deploy/config_out_sharded/logs/kv_${id}.log
  line=$(grep -E 'seq gap:[0-9]+' $f | tail -1)
  printf "kv_%-2d %s\n" "$id" "$line"
done
