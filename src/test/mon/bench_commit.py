#!/usr/bin/python3

import json
import rados
from statistics import mean, stdev
import time

times = []

conf = {
  'mon_client_target_rank': '0'
}

with rados.Rados(conffile=rados.Rados.DEFAULT_CONF_FILES, conf=conf) as conn:
    for i in range(1000):
        cmd = {
            'prefix': 'config-key set',
            'key': 'bench',
            'val': str(i),
        }
        start = time.monotonic()
        ret, buf, out = conn.mon_command(json.dumps(cmd), b'')
        stop = time.monotonic()
        assert ret == 0
        times.append(stop-start)

print(f"min/max/mean/stddev: {min(times):.6f}/{max(times):.6f}/{mean(times):.6f}/{stdev(times):.6f}")
