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

import argparse

def read_tps(file):
    samples = []
    lat = []
    with open(file) as f:
        for l in f.readlines():
            s = l.split()
            txn = None
            t = None
            for r in s:
                k = r.split(':')[0]
                if k == 'txn':
                    txn = int(r.split(':')[1])
                elif k == 'time':
                    t = int(r.split(':')[1])
            if txn is not None:
                samples.append((txn, t))
            if l.find("client latency") > 0:
                lat.append(float(s[-1].split(':')[-1]))
    return samples, lat

def in_window(sample_time, warmup_sec, duration_sec):
    if sample_time is None:
        return True
    if sample_time <= warmup_sec:
        return False
    if duration_sec > 0 and sample_time > duration_sec:
        return False
    return True

def cal_tps(samples, warmup_sec, duration_sec):
    tps_sum = []
    tps_max = 0
    kept = 0

    for v, sample_time in samples:
        if not in_window(sample_time, warmup_sec, duration_sec):
            continue
        kept += 1
        if v == 0:
            continue
        tps_max = max(tps_max, v)
        tps_sum.append(v) 

    print("max throughput:",tps_max)
    print("windowed samples:", kept)
    if not tps_sum:
        print("average throughput: 0 (no txn samples in selected window)")
    else:
        print("average throughput:", sum(tps_sum) / len(tps_sum))

def cal_lat(lat):
    lat_sum = []
    lat_max = 0
    for v in lat:
        if v == 0:
            continue
        lat_max = max(lat_max, v)
        lat_sum.append(v) 

    print("max latency:",lat_max)
    if not lat_sum:
        print("average latency: 0 (no client latency lines in logs)")
    else:
        print("average latency:", sum(lat_sum) / len(lat_sum))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--warmup-sec', type=int, default=0,
                        help='Exclude samples with time<=warmup-sec')
    parser.add_argument('--duration-sec', type=int, default=0,
                        help='Exclude samples with time>duration-sec (0 keeps all)')
    parser.add_argument('files', nargs='+')
    args = parser.parse_args()
    files = args.files
    print("calculate results, number of nodes:",len(files))


    samples = []
    lat = []
    for f in files:
        t, l = read_tps(f)
        samples += t
        lat += l

    cal_tps(samples, args.warmup_sec, args.duration_sec)
    cal_lat(lat)
