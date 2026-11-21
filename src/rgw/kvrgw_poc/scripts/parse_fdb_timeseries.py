#!/usr/bin/env python3
import json, sys

data = open(sys.argv[1]).read()
samples = data.split('--- ')
results = []

for s in samples:
    if not s.strip() or not s.strip()[0].isdigit():
        continue
    lines = s.strip().split('\n', 1)
    ts = int(lines[0].strip().rstrip(' ---'))
    try:
        j = json.loads(lines[1])
    except:
        continue

    cluster = j.get('cluster', {})
    wl = cluster.get('workload', {})
    ops = wl.get('operations', {})
    txns = wl.get('transactions', {})
    qos = cluster.get('qos', {})

    reads_hz = ops.get('reads', {}).get('hz', 0)
    writes_hz = ops.get('writes', {}).get('hz', 0)
    committed_hz = txns.get('committed', {}).get('hz', 0)
    started_hz = txns.get('started', {}).get('hz', 0)
    conflict_hz = txns.get('conflicted', {}).get('hz', 0)

    procs = cluster.get('processes', {})
    ss_cpus = []
    log_cpus = []
    proxy_cpus = []
    ss_disk_busy = []

    for addr, p in procs.items():
        cpu = p.get('cpu', {}).get('usage_cores', 0)
        disk = p.get('disk', {}).get('busy', 0)
        roles = [r.get('role', '') for r in p.get('roles', [])]
        if 'storage' in roles:
            ss_cpus.append(cpu)
            ss_disk_busy.append(disk)
        elif 'log' in roles:
            log_cpus.append(cpu)
        elif 'commit_proxy' in roles or 'grv_proxy' in roles or 'resolver' in roles:
            proxy_cpus.append(cpu)

    dur_lag = qos.get('limiting_durability_lag_storage_server', {}).get('seconds', 0)
    data_lag = qos.get('limiting_data_lag_storage_server', {}).get('seconds', 0)
    queue_bytes = qos.get('limiting_queue_bytes_storage_server', 0)
    perf_limited = qos.get('performance_limited_by', {}).get('name', '')

    results.append({
        'ts': ts,
        'reads_hz': reads_hz,
        'writes_hz': writes_hz,
        'committed_hz': committed_hz,
        'started_hz': started_hz,
        'conflict_hz': conflict_hz,
        'ss_cpu_max': max(ss_cpus) if ss_cpus else 0,
        'ss_cpu_avg': sum(ss_cpus)/len(ss_cpus) if ss_cpus else 0,
        'log_cpu_max': max(log_cpus) if log_cpus else 0,
        'proxy_cpu_max': max(proxy_cpus) if proxy_cpus else 0,
        'ss_disk_max': max(ss_disk_busy) if ss_disk_busy else 0,
        'dur_lag': dur_lag,
        'data_lag': data_lag,
        'queue_MB': queue_bytes / 1e6,
        'limited_by': perf_limited,
    })

hdr = "{:>6} {:>8} {:>8} {:>7} {:>7} {:>6} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>12}".format(
    'T(s)', 'reads', 'writes', 'txn_cm', 'txn_st', 'confl',
    'ss_avg', 'ss_max', 'log_mx', 'prx_mx', 'ss_dsk', 'dur_lg', 'q_MB', 'limited_by')
print(hdr)

t0 = results[0]['ts'] if results else 0
for r in results:
    print("{:>6} {:>8.0f} {:>8.0f} {:>7.0f} {:>7.0f} {:>6.1f} {:>7.2f} {:>7.2f} {:>7.2f} {:>7.2f} {:>7.2f} {:>7.2f} {:>7.1f} {:>12}".format(
        r['ts'] - t0, r['reads_hz'], r['writes_hz'], r['committed_hz'], r['started_hz'],
        r['conflict_hz'], r['ss_cpu_avg'], r['ss_cpu_max'], r['log_cpu_max'],
        r['proxy_cpu_max'], r['ss_disk_max'], r['dur_lag'], r['queue_MB'], r['limited_by']))
