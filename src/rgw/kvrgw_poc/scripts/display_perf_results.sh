#!/bin/bash
#
# Ceph - scalable distributed file system
#
# Author: Gabriel BenHanokh <gbenhano@redhat.com>
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.
#
#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $(basename "$0") <results-dir> <selector>... [--deviation N]

Selectors:
  KVRGW   - KVRGW latency, batch stats, error stats
  SS      - FDB storage server stats
  LOG     - FDB log server stats
  SL      - FDB stateless server stats
  FDB     - All FDB stats (SS + LOG + SL)
  HEALTH  - FDB cluster health summary (default: 2-tick, --long: exponential timeline)

Options:
  --deviation N    Outlier threshold as % deviation from median (default 10)
  --long           Show full detailed output (default: summary table only)
  --compact        With HEALTH --long/--linear: aggregate servers by role
  --linear         HEALTH: show every Nth sample (use with --interval)
  --interval N     With --linear: show every Nth sample (default 5)
  --warmup N       Samples after first nonzero before ACTIVE phase (default 3)

Example:
  $(basename "$0") perf-results/FDB-Default_Perf-Test-Default_20260829/ KVRGW SS --deviation 15
  $(basename "$0") perf-results/FDB-Default_Perf-Test-Default_20260829/ KVRGW --long
  $(basename "$0") perf-results/FDB-Default_Perf-Test-Default_20260829/ HEALTH
  $(basename "$0") perf-results/FDB-Default_Perf-Test-Default_20260829/ HEALTH --long --compact
  $(basename "$0") perf-results/FDB-Default_Perf-Test-Default_20260829/ HEALTH --linear --interval 2
EOF
  exit 1
}

[[ $# -ge 2 ]] || usage

RESULTS_DIR="${1}"; shift
DEVIATION=10
LONG=0
COMPACT=0
LINEAR=0
INTERVAL=5
WARMUP=3
SELECTORS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --deviation) DEVIATION="$2"; shift 2 ;;
    --long) LONG=1; shift ;;
    --compact) COMPACT=1; shift ;;
    --linear) LINEAR=1; shift ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    KVRGW|SS|LOG|SL|FDB|HEALTH) SELECTORS+=("$1"); shift ;;
    *) echo "Unknown: $1" >&2; usage ;;
  esac
done

[[ ${#SELECTORS[@]} -gt 0 ]] || { echo "ERROR: at least one selector required" >&2; usage; }
[[ -d "${RESULTS_DIR}" ]] || { echo "ERROR: ${RESULTS_DIR} not found" >&2; exit 1; }

show_fdb=0
show_ss=0
show_log=0
show_sl=0
show_kvrgw=0
show_health=0

for sel in "${SELECTORS[@]}"; do
  case "$sel" in
    FDB) show_fdb=1 ;;
    SS) show_ss=1 ;;
    LOG) show_log=1 ;;
    SL) show_sl=1 ;;
    KVRGW) show_kvrgw=1 ;;
    HEALTH) show_health=1 ;;
  esac
done

parse_fdb_stats() {
  local fdb_log="$1"
  python3 - "${fdb_log}" "${DEVIATION}" <<'PYEOF'
import sys, re, os
from collections import defaultdict
from datetime import datetime

fdb_log = sys.argv[1]
deviation = float(sys.argv[2])
mode = os.environ.get('FDB_DISPLAY_MODE', 'ss')

samples = []
current = {}
_skip_next = False

with open(fdb_log) as f:
    for line in f:
        line = line.strip()
        if line == '=== SKIP ===':
            _skip_next = True
            continue
        if line == '---':
            if _skip_next:
                samples.append({'skip': True})
                _skip_next = False
            elif current:
                samples.append(current)
            current = {}
            continue
        if line.startswith('ts='):
            current['ts'] = line.split('=',1)[1]
        elif line.startswith('cluster_'):
            for m in re.finditer(r'(\w+)=([\d.]+)', line):
                current[m.group(1)] = float(m.group(2))
        elif line.startswith('qos_'):
            for m in re.finditer(r'(\w+)=([\d.]+)', line):
                current[m.group(1)] = float(m.group(2))
        elif line.startswith('ss '):
            addr = re.search(r'addr=(\S+)', line).group(1)
            entry = {'addr': addr}
            for m in re.finditer(r'(\w+)=([\d.]+)', line):
                if m.group(1) != 'addr':
                    entry[m.group(1)] = float(m.group(2))
            current.setdefault('ss_list', []).append(entry)
        elif line.startswith('log '):
            addr = re.search(r'addr=(\S+)', line).group(1)
            entry = {'addr': addr}
            for m in re.finditer(r'(\w+)=([\d.]+)', line):
                if m.group(1) != 'addr':
                    entry[m.group(1)] = float(m.group(2))
            current.setdefault('log_list', []).append(entry)
        elif line.startswith('sl '):
            addr = re.search(r'addr=(\S+)', line).group(1)
            role = re.search(r'role=(\S+)', line).group(1)
            entry = {'addr': addr, 'role_name': role}
            for m in re.finditer(r'(\w+)=([\d.]+)', line):
                if m.group(1) not in ('addr','role'):
                    entry[m.group(1)] = float(m.group(2))
            current.setdefault('sl_list', []).append(entry)
        elif line.startswith('disk_free'):
            parts = line.split()[1:]
            for p in parts:
                k,v = p.split('=')
                current[f'disk_free_{k}'] = float(v)

if not samples:
    print("  (no FDB samples found)")
    sys.exit(0)

# Detect drain from SS input_hz: after ACTIVE phase, 2+ consecutive zero-input valid samples
drain_start_idx = -1
_drain_seen_active = False
_drain_consec = 0
for _di, s in enumerate(samples):
    if s.get('skip'): continue
    ss_inp = sum(e.get('input_hz', 0) for e in s.get('ss_list', []))
    if not _drain_seen_active:
        if ss_inp > 0: _drain_seen_active = True
    else:
        if ss_inp == 0:
            _drain_consec += 1
            if _drain_consec >= 2:
                drain_start_idx = _di - 1
                break
        else:
            _drain_consec = 0

def stats_line(label, vals):
    if not vals: return
    vals.sort()
    n = len(vals)
    mn, mx = min(vals), max(vals)
    avg = sum(vals)/n
    med = vals[n//2] if n%2==1 else (vals[n//2-1]+vals[n//2])/2
    def pct(p):
        k=(p/100.0)*(n-1); f=int(k); c=min(f+1,n-1)
        return vals[f]+(vals[c]-vals[f])*(k-f)
    p90,p95,p99 = pct(90),pct(95),pct(99)
    dev_thresh = deviation/100.0
    outliers = sum(1 for v in vals if med>0 and abs(v-med)/med>dev_thresh)
    dev_pct = 100.0*outliers/n if n>0 else 0
    print(f'  {label:<20s} {mn:>10.1f} {mx:>10.1f} {avg:>10.1f} {med:>10.1f} {p90:>10.1f} {p95:>10.1f} {p99:>10.1f} {dev_pct:>6.1f}%')

def print_rate_header():
    print(f'  {"Metric":<20s} {"min":>10s} {"max":>10s} {"avg":>10s} {"median":>10s} {"p90":>10s} {"p95":>10s} {"p99":>10s} {"dev%":>6s}')
    print(f'  {"-"*20} {"-"*10} {"-"*10} {"-"*10} {"-"*10} {"-"*10} {"-"*10} {"-"*10} {"-"*6}')

def fmt_bytes(b):
    if abs(b) >= 1e9: return f'{b/1e9:.1f} GB'
    if abs(b) >= 1e6: return f'{b/1e6:.1f} MB'
    if abs(b) >= 1e3: return f'{b/1e3:.1f} KB'
    return f'{b:.0f} B'

def collect_per_server(samples, key, field):
    addrs = []
    for s in samples:
        for e in s.get(key, []):
            if e['addr'] not in addrs:
                addrs.append(e['addr'])
    result = {}
    for addr in addrs:
        result[addr] = []
        for s in samples:
            found = [e for e in s.get(key,[]) if e['addr']==addr]
            result[addr].append(found[0] if found else {})
    return result

drain_line = drain_start_idx if drain_start_idx >= 0 else len(samples)

def get_vals(entries, field, phase='all'):
    """Extract values for a field, filtering by phase and skipping empty/warmup samples."""
    result = []
    for i, e in enumerate(entries):
        if not e: continue
        v = e.get(field)
        if v is None: continue
        if phase == 'load' and (i < active_start or i >= drain_line): continue
        if phase == 'drain' and i < drain_line: continue
        if phase == 'all' and i < active_start: continue
        result.append(v)
    return result

def get_sample_vals(samples, field, phase='all'):
    result = []
    for i, s in enumerate(samples):
        if s.get('skip'): continue
        v = s.get(field)
        if v is None: continue
        if phase == 'load' and (i < active_start or i >= drain_line): continue
        if phase == 'drain' and i < drain_line: continue
        if phase == 'all' and i < active_start: continue
        result.append(v)
    return result

def phase_summary(label, entries_or_samples, field, is_sample=False):
    """Print LOAD vs DRAIN summary table for an accumulation metric."""
    print(f'  {label:<20s}  {"Phase":<8s} {"min":>10s} {"max":>10s} {"avg":>10s} {"median":>10s}')
    print(f'  {"":<20s}  {"--------":<8s} {"----------":>10s} {"----------":>10s} {"----------":>10s} {"----------":>10s}')
    for phase_name, phase_key in [("LOAD", "load"), ("DRAIN", "drain")]:
        if is_sample:
            vals = get_sample_vals(entries_or_samples, field, phase_key)
        else:
            vals = get_vals(entries_or_samples, field, phase_key)
        vals = [v for v in vals if v > 0]
        if not vals: continue
        vals_s = sorted(vals)
        n = len(vals_s)
        mn, mx = min(vals_s), max(vals_s)
        avg = sum(vals_s)/n
        med = vals_s[n//2] if n%2==1 else (vals_s[n//2-1]+vals_s[n//2])/2
        print(f'  {"":<20s}  {phase_name:<8s} {mn:>10.1f} {mx:>10.1f} {avg:>10.1f} {med:>10.1f}')

long_mode = os.environ.get('FDB_LONG', '0') == '1'
warmup_n = int(os.environ.get('FDB_WARMUP', '3'))

# Compute ACTIVE phase start from SS input_hz (actual KVRGW data flowing)
active_start = 0
_sm_state = 'IDLE'
_sm_run = 0
for _si, s in enumerate(samples):
    if s.get('skip'): continue
    ss_input = sum(e.get('input_hz', 0) for e in s.get('ss_list', []))
    if _sm_state == 'IDLE':
        if ss_input > 0: _sm_state = 'RAMPUP'; _sm_run = 1
    elif _sm_state == 'RAMPUP':
        if ss_input > 0:
            _sm_run += 1
            if _sm_run >= warmup_n:
                _sm_state = 'ACTIVE'; active_start = _si; break
        else:
            _sm_run = 0

def median_val(vals):
    if not vals: return 0
    s = sorted(vals)
    n = len(s)
    return s[n//2] if n%2==1 else (s[n//2-1]+s[n//2])/2

def fmt_rate(v):
    if abs(v) >= 1e6: return f'{v/1e6:.1f}M'
    if abs(v) >= 1e3: return f'{v/1e3:.1f}K'
    return f'{v:.1f}'

def compact_table(label, metrics):
    """Print a compact table: label row then metric rows with min/max/avg/median/dev%."""
    print(f'  {"Metric":<16s} {"min":>10s} {"max":>10s} {"avg":>10s} {"median":>10s} {"dev%":>6s}')
    print(f'  {"-"*16} {"-"*10} {"-"*10} {"-"*10} {"-"*10} {"-"*6}')
    for name, vals in metrics:
        vals = [v for v in vals if v > 0]
        if not vals: continue
        s = sorted(vals)
        n = len(s)
        mn, mx = min(s), max(s)
        avg = sum(s)/n
        med = s[n//2] if n%2==1 else (s[n//2-1]+s[n//2])/2
        dev_thresh = deviation/100.0
        outliers = sum(1 for v in s if med>0 and abs(v-med)/med>dev_thresh)
        dev_pct = 100.0*outliers/n if n>0 else 0
        print(f'  {name:<16s} {mn:>10.1f} {mx:>10.1f} {avg:>10.1f} {med:>10.1f} {dev_pct:>5.1f}%')

def log_accum_line(entries, field, label):
    """One-line log accumulation summary: rate/s, total, drain status."""
    load_vals = get_vals(entries, field, 'load')
    load_vals = [v for v in load_vals if v > 0]
    drain_vals = get_vals(entries, field, 'drain')
    drain_vals = [v for v in drain_vals if v > 0]
    if not load_vals and not drain_vals:
        return
    total = max(load_vals) if load_vals else 0
    first = load_vals[0] if load_vals else 0
    n_load = len(load_vals)
    rate = (load_vals[-1] - first) / (n_load * 2) if n_load > 1 and first > 0 else 0
    drain_end = drain_vals[-1] if drain_vals else total
    drain_start = drain_vals[0] if drain_vals else total
    drained = "YES" if drain_end < drain_start * 0.5 else "NO"
    print(f'  {label}: {fmt_bytes(rate)}/s  total={fmt_bytes(total)}  drain={drained} ({fmt_bytes(drain_start)}→{fmt_bytes(drain_end)})')

if mode == 'ss':
    print('\n  ==========================================')
    print('  Storage Servers')
    print('  ==========================================')

    ss_data = collect_per_server(samples, 'ss_list', 'addr')

    if not long_mode:
        for addr, entries in ss_data.items():
            print(f'\n  --- SS {addr} ---')
            cpu_med = median_val(get_vals(entries, 'cpu', 'load'))
            mem_vals = get_vals(entries, 'rss_bytes', 'load')
            mem_med = median_val(mem_vals)
            inp_med = median_val(get_vals(entries, 'input_hz', 'load'))
            dur_med = median_val(get_vals(entries, 'durable_hz', 'load'))
            lag_med = median_val(get_vals(entries, 'data_lag_s', 'load'))
            dlag_med = median_val(get_vals(entries, 'durability_lag_s', 'load'))
            stored_med = median_val(get_vals(entries, 'stored_bytes', 'load'))
            disk_med = median_val(get_vals(entries, 'disk_busy', 'load'))
            compact_table(addr, [
                ('cpu', get_vals(entries, 'cpu', 'load')),
                ('mem (MB)', [v/1e6 for v in get_vals(entries, 'rss_bytes', 'load') if v > 0]),
                ('input (MB/s)', [v/1e6 for v in get_vals(entries, 'input_hz', 'load') if v > 0]),
                ('durable (MB/s)', [v/1e6 for v in get_vals(entries, 'durable_hz', 'load') if v > 0]),
                ('data_lag_s', get_vals(entries, 'data_lag_s', 'load')),
                ('stored (MB)', [v/1e6 for v in get_vals(entries, 'stored_bytes', 'load') if v > 0]),
                ('disk_busy', get_vals(entries, 'disk_busy', 'load')),
            ])
            log_accum_line(entries, 'stored_bytes', 'stored')
            log_accum_line(entries, 'queue_disk_bytes', 'queue')

        print(f'\n  --- Cluster ---')
        txn = median_val(get_sample_vals(samples, 'cluster_txn_hz', 'load'))
        writes = median_val(get_sample_vals(samples, 'cluster_writes_hz', 'load'))
        conflict = median_val(get_sample_vals(samples, 'cluster_conflict_hz', 'load'))
        print(f'  txn/s={fmt_rate(txn)}  writes/s={fmt_rate(writes)}  conflict/s={fmt_rate(conflict)}')
    else:
        if drain_start_idx >= 0:
            print(f'  Drain marker at sample {drain_start_idx}')
        rate_fields = [('cpu','cpu'),('input_hz','input_hz'),('durable_hz','durable_hz'),
                       ('data_lag_s','data_lag_s'),('durability_lag_s','durability_lag_s'),
                       ('disk_busy','disk_busy'),('disk_reads_hz','disk_reads_hz'),('disk_writes_hz','disk_writes_hz')]
        accum_fields = [('stored_bytes','stored_bytes'),('kvstore_bytes','kvstore_bytes'),
                        ('queue_disk_bytes','queue_disk_bytes'),('mem_bytes','mem_bytes'),('rss_bytes','rss_bytes')]
        for addr, entries in ss_data.items():
            print(f'\n  --- SS {addr} ---')
            print('\n  Rate metrics (LOAD phase):')
            print_rate_header()
            for label,field in rate_fields:
                vals = get_vals(entries, field, 'load')
                vals = [v for v in vals if v > 0]
                if vals: stats_line(label, vals)
            if drain_start_idx >= 0:
                print('\n  Rate metrics (DRAIN phase):')
                print_rate_header()
                for label,field in rate_fields:
                    vals = get_vals(entries, field, 'drain')
                    vals = [v for v in vals if v > 0]
                    if vals: stats_line(label, vals)
            print('\n  Accumulation metrics:')
            for field,label in accum_fields:
                phase_summary(label, entries, field)
        print('\n  --- Cluster Workload ---')
        for phase_name, phase_key in [("LOAD", "load"), ("DRAIN", "drain")]:
            print(f'\n  {phase_name} phase:')
            print_rate_header()
            for field,label in [('cluster_txn_hz','txn/sec'),('cluster_reads_hz','reads/sec'),
                                 ('cluster_writes_hz','writes/sec'),('cluster_conflict_hz','conflict/sec')]:
                vals = get_sample_vals(samples, field, phase_key)
                vals = [v for v in vals if v > 0]
                if vals: stats_line(label, vals)

elif mode == 'log':
    print('\n  ==========================================')
    print('  Log Servers')
    print('  ==========================================')

    log_data = collect_per_server(samples, 'log_list', 'addr')

    if not long_mode:
        for addr, entries in log_data.items():
            print(f'\n  --- LOG {addr} ---')
            compact_table(addr, [
                ('cpu', get_vals(entries, 'cpu', 'load')),
                ('mem (MB)', [v/1e6 for v in get_vals(entries, 'rss_bytes', 'load') if v > 0]),
                ('input (MB/s)', [v/1e6 for v in get_vals(entries, 'input_hz', 'load') if v > 0]),
                ('durable (MB/s)', [v/1e6 for v in get_vals(entries, 'durable_hz', 'load') if v > 0]),
                ('disk_busy', get_vals(entries, 'disk_busy', 'load')),
            ])
            log_accum_line(entries, 'queue_disk_bytes', 'queue_disk')
            log_accum_line(entries, 'queue_mem_bytes', 'queue_mem')
    else:
        if drain_start_idx >= 0:
            print(f'  Drain marker at sample {drain_start_idx}')
        rate_fields = [('cpu','cpu'),('input_hz','input_hz'),('durable_hz','durable_hz'),
                       ('disk_busy','disk_busy'),('disk_reads_hz','disk_reads_hz'),('disk_writes_hz','disk_writes_hz')]
        accum_fields = [('queue_disk_bytes','queue_disk_bytes'),('queue_mem_bytes','queue_mem_bytes'),
                        ('mem_bytes','mem_bytes'),('rss_bytes','rss_bytes')]
        for addr, entries in log_data.items():
            print(f'\n  --- LOG {addr} ---')
            print('\n  Rate metrics (LOAD phase):')
            print_rate_header()
            for label,field in rate_fields:
                vals = get_vals(entries, field, 'load')
                vals = [v for v in vals if v > 0]
                if vals: stats_line(label, vals)
            if drain_start_idx >= 0:
                print('\n  Rate metrics (DRAIN phase):')
                print_rate_header()
                for label,field in rate_fields:
                    vals = get_vals(entries, field, 'drain')
                    vals = [v for v in vals if v > 0]
                    if vals: stats_line(label, vals)
            print('\n  Accumulation metrics:')
            for field,label in accum_fields:
                phase_summary(label, entries, field)

elif mode == 'sl':
    print('\n  ==========================================')
    print('  Stateless Servers')
    print('  ==========================================')

    sl_data = collect_per_server(samples, 'sl_list', 'addr')

    if not long_mode:
        for addr, entries in sl_data.items():
            roles = set(e.get('role_name','?') for e in entries if e)
            print(f'\n  --- SL {addr} ({",".join(roles)}) ---')
            compact_table(addr, [
                ('cpu', get_vals(entries, 'cpu', 'load')),
                ('mem (MB)', [v/1e6 for v in get_vals(entries, 'rss_bytes', 'load') if v > 0]),
            ])
        print(f'\n  --- Cluster QoS (LOAD) ---')
        compact_table('QoS', [
            ('worst_q_ss (MB)', [v/1e6 for v in get_sample_vals(samples, 'qos_worst_queue_ss', 'load') if v > 0]),
            ('worst_q_log (MB)', [v/1e6 for v in get_sample_vals(samples, 'qos_worst_queue_log', 'load') if v > 0]),
            ('limit_q (MB)', [v/1e6 for v in get_sample_vals(samples, 'qos_limiting_queue', 'load') if v > 0]),
            ('dur_lag_s', [v for v in get_sample_vals(samples, 'qos_durability_lag_s', 'load') if v > 0]),
        ])
    else:
        if drain_start_idx >= 0:
            print(f'  Drain marker at sample {drain_start_idx}')
        for addr, entries in sl_data.items():
            roles = set(e.get('role_name','?') for e in entries if e)
            print(f'\n  --- SL {addr} ({",".join(roles)}) ---')
            print('\n  Rate metrics (LOAD phase):')
            print_rate_header()
            vals = get_vals(entries, 'cpu', 'load')
            vals = [v for v in vals if v > 0]
            if vals: stats_line('cpu', vals)
            if drain_start_idx >= 0:
                print('\n  Rate metrics (DRAIN phase):')
                print_rate_header()
                vals = get_vals(entries, 'cpu', 'drain')
                vals = [v for v in vals if v > 0]
                if vals: stats_line('cpu', vals)
            print('\n  Accumulation metrics:')
            for field,label in [('mem_bytes','mem_bytes'),('rss_bytes','rss_bytes')]:
                phase_summary(label, entries, field)
        print('\n  --- Cluster QoS ---')
        for phase_name, phase_key in [("LOAD", "load"), ("DRAIN", "drain")]:
            print(f'\n  {phase_name} phase:')
            print_rate_header()
            for field,label in [('qos_worst_queue_ss','worst_queue_ss'),('qos_worst_queue_log','worst_queue_log'),
                                 ('qos_limiting_queue','limiting_queue'),('qos_durability_lag_s','durability_lag_s')]:
                vals = get_sample_vals(samples, field, phase_key)
                vals = [v for v in vals if v > 0]
                if vals: stats_line(label, vals)

elif mode == 'fdb':
    import subprocess, tempfile

    print('\n  ==========================================')
    print('  FDB Cluster Health Summary')
    print('  ==========================================')

    if drain_start_idx >= 0:
        print(f'  Drain marker at sample {drain_start_idx} (load phase: 0-{drain_start_idx-1}, drain phase: {drain_start_idx}-{len(samples)-1})')
    drain_line = drain_start_idx if drain_start_idx >= 0 else len(samples)

    def make_dat(series):
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.dat', delete=False)
        for i,v in series: f.write(f'{i} {v}\n')
        f.close()
        return f.name

    def gnuplot_graph(title, ylabel, y2label, series1, label1, series2, label2, dual_axis=True):
        f1 = make_dat(series1)
        f2 = make_dat(series2)
        drain_cmd = ""
        if drain_start_idx >= 0:
            drain_cmd = f'set arrow from {drain_start_idx},graph 0 to {drain_start_idx},graph 1 nohead lt 0 lw 2; set label "DRAIN" at {drain_start_idx},graph 0.95 center;'
        axes = "axes x1y2" if dual_axis else ""
        y2_setup = f'set y2label "{y2label}"; set y2tics;' if dual_axis else ""
        try:
            gp = subprocess.run(['gnuplot', '-e', f'''
set terminal dumb 100 25;
set title "{title}";
set xlabel "sample";
set ylabel "{ylabel}";
{y2_setup}
{drain_cmd}
plot "{f1}" using 1:2 with lines title "{label1}" axes x1y1, \
     "{f2}" using 1:2 with lines title "{label2}" {axes};
'''], capture_output=True, text=True, timeout=10)
            print(gp.stdout)
        except Exception as e:
            print(f'  (gnuplot error: {e})')
        os.unlink(f1)
        os.unlink(f2)

    def phase_table(label, series):
        pre = [v for i,v in series if i < drain_line]
        post = [v for i,v in series if i >= drain_line]
        print(f'  {label:<20s}  {"Phase":<8s} {"min":>10s} {"max":>10s} {"avg":>10s} {"median":>10s}')
        print(f'  {"":<20s}  {"--------":<8s} {"----------":>10s} {"----------":>10s} {"----------":>10s} {"----------":>10s}')
        for phase_name, vals in [("LOAD", pre), ("DRAIN", post)]:
            if not vals: continue
            vals_s = sorted(vals)
            n = len(vals_s)
            mn, mx = min(vals_s), max(vals_s)
            avg = sum(vals_s)/n
            med = vals_s[n//2] if n%2==1 else (vals_s[n//2-1]+vals_s[n//2])/2
            print(f'  {"":<20s}  {phase_name:<8s} {mn:>10.1f} {mx:>10.1f} {avg:>10.1f} {med:>10.1f}')

    # Graph 1: Storage Queues & Durability
    all_stored = []
    all_durable = []
    for i,s in enumerate(samples):
        stored_sum = sum(e.get('stored_bytes',0) for e in s.get('ss_list',[]))
        ss_list = s.get('ss_list',[])
        durable_avg = sum(e.get('durable_hz',0) for e in ss_list) / len(ss_list) if ss_list else 0
        all_stored.append((i, stored_sum/1e6))
        all_durable.append((i, durable_avg/1e6))

    print('\n  --- Storage Queues & Durability ---')
    gnuplot_graph("Storage: stored_bytes (MB) vs durable_rate (MB/s)", "MB", "MB/s",
                  all_stored, "stored_MB", all_durable, "durable_MB/s")

    phase_table("stored_bytes (MB)", all_stored)
    phase_table("durable_hz (MB/s)", all_durable)

    zero_durable = sum(1 for i,v in all_durable if v == 0 and i < drain_line)
    nonzero_stored = sum(1 for i,v in all_stored if v > 0 and i < drain_line)
    if zero_durable > 0 and nonzero_stored > 0:
        print(f'  [WARNING] durable_bytes.hz was 0 in {zero_durable} load-phase samples while data was queued')

    if drain_start_idx >= 0:
        last_stored = all_stored[-1][1] if all_stored else 0
        drain_first = all_stored[drain_start_idx][1] if drain_start_idx < len(all_stored) else 0
        if last_stored >= drain_first * 0.9 and drain_first > 1:
            print(f'  [WARNING] stored_bytes did not drain: start={drain_first:.1f} MB, end={last_stored:.1f} MB')

    # Graph 2: TLog Queue Growth
    all_q_mem = []
    all_q_disk = []
    for i,s in enumerate(samples):
        qm = sum(e.get('queue_mem_bytes',0) for e in s.get('log_list',[]))
        qd = sum(e.get('queue_disk_bytes',0) for e in s.get('log_list',[]))
        all_q_mem.append((i, qm/1e6))
        all_q_disk.append((i, qd/1e6))

    print('\n  --- TLog Queue Growth ---')
    gnuplot_graph("TLog Queues: memory (MB) vs disk (MB)", "MB", "MB",
                  all_q_mem, "queue_mem_MB", all_q_disk, "queue_disk_MB", dual_axis=False)

    phase_table("queue_mem (MB)", all_q_mem)
    phase_table("queue_disk (MB)", all_q_disk)

    if len(all_q_disk) >= 4:
        load_vals = [v for i,v in all_q_disk if i < drain_line]
        if len(load_vals) >= 4:
            half = len(load_vals)//2
            first_avg = sum(load_vals[:half])/half
            second_avg = sum(load_vals[half:])/half
            if second_avg > first_avg * 1.5 and second_avg > 1:
                print(f'  [WARNING] TLog queue_disk shows sustained growth during load: first half avg={first_avg:.1f} MB, second half avg={second_avg:.1f} MB')

    if drain_start_idx >= 0:
        last_qd = all_q_disk[-1][1] if all_q_disk else 0
        drain_first_qd = all_q_disk[drain_start_idx][1] if drain_start_idx < len(all_q_disk) else 0
        if last_qd >= drain_first_qd * 0.9 and drain_first_qd > 1:
            print(f'  [WARNING] TLog queue_disk did not drain: start={drain_first_qd:.1f} MB, end={last_qd:.1f} MB')

    # Graph 3: Memory & Disk Headroom
    all_ram = []
    all_disk_free = []
    for i,s in enumerate(samples):
        ram = sum(e.get('rss_bytes', e.get('mem_bytes',0)) for e in s.get('ss_list',[])) + \
              sum(e.get('rss_bytes', e.get('mem_bytes',0)) for e in s.get('log_list',[])) + \
              sum(e.get('rss_bytes', e.get('mem_bytes',0)) for e in s.get('sl_list',[]))
        all_ram.append((i, ram/1e9))
        df_vals = [v for k,v in s.items() if k.startswith('disk_free_')]
        min_df = min(df_vals) if df_vals else 100
        all_disk_free.append((i, min_df))

    print('\n  --- Memory & Disk Headroom ---')
    gnuplot_graph("Host: Total FDB RAM (GB) vs Min Disk Free (%)", "GB", "%",
                  all_ram, "RAM_GB", all_disk_free, "DiskFree_pct")

    phase_table("RAM (GB)", all_ram)
    phase_table("Disk Free (%)", all_disk_free)

    max_ram_gb = max(v for _,v in all_ram) if all_ram else 0
    min_disk_pct = min(v for _,v in all_disk_free) if all_disk_free else 100
    if max_ram_gb > 0:
        n_procs = len(next((s.get('ss_list',[]) for s in samples if s.get('ss_list')),[])) + \
                  len(next((s.get('log_list',[]) for s in samples if s.get('log_list')),[])) + \
                  len(next((s.get('sl_list',[]) for s in samples if s.get('sl_list')),[]))
        limit_gb = n_procs * 2.0
        if max_ram_gb > limit_gb * 0.9:
            print(f'  [WARNING] Total FDB RAM peaked at {max_ram_gb:.1f} GB (90% of {limit_gb:.0f} GB limit)')
    if min_disk_pct < 5:
        print(f'  [WARNING] Disk free space dropped to {min_disk_pct:.1f}% (below 5% threshold)')

elif mode == 'health':
    compact_mode = os.environ.get('FDB_COMPACT', '0') == '1'
    sample_interval_s = 2

    def ss_flag(entry):
        if not entry: return '[NO_DATA]'
        cpu = entry.get('cpu', 0)
        inp = entry.get('input_hz', 0)
        dur = entry.get('durable_hz', 0)
        lag = entry.get('data_lag_s', 0)
        flags = []
        if cpu >= 0.95: flags.append('[SATURATED]')
        if cpu > 0.25:
            if dur > 0 and inp / dur > 2.0: flags.append('[HOTSPOT]')
        if lag > 5.0: flags.append('[FALLING_BEHIND]')
        if inp == 0 and dur == 0 and cpu < 0.05: flags.append('[IDLE]')
        if inp == 0 and dur > 0: flags.append('[DRAINING]')
        return ' '.join(flags) if flags else '[OK]'

    def cluster_flag(sample):
        lq = sample.get('qos_limiting_queue', 0)
        return '[THROTTLING]' if lq > 10e6 else '[OK]'

    def fmt_mb(v):
        if v >= 1e9: return f'{v/1e9:.1f}GB'
        return f'{v/1e6:.1f}MB'

    def print_tick(label, idx, max_idx=None):
        if idx < 0 or idx >= len(samples):
            return
        if max_idx is None:
            max_idx = len(samples)
        s = samples[idx]
        actual_idx = idx
        while not s.get('ss_list') and not s.get('log_list') and actual_idx + 1 < max_idx:
            actual_idx += 1
            if actual_idx >= len(samples): return
            s = samples[actual_idx]
        if not s.get('ss_list') and not s.get('log_list'):
            return
        suffix = f' → sample {actual_idx}' if actual_idx != idx else ''
        print(f'\n  {label}{suffix}')

        ss_list = s.get('ss_list', [])
        log_list = s.get('log_list', [])
        sl_list = s.get('sl_list', [])

        if not compact_mode:
            for e in ss_list:
                addr = e.get('addr', '?')
                cpu = e.get('cpu', 0)
                inp = e.get('input_hz', 0)
                dur = e.get('durable_hz', 0)
                lag = e.get('data_lag_s', 0)
                ratio = f'{inp/dur:.1f}x' if dur > 0 else ('0/0' if inp == 0 else 'INF')
                flag = ss_flag(e)
                print(f'    SS :{addr.split(":")[-1]:<5s} in/dur={ratio:<6s} lag={lag:<6.1f}s cpu={cpu:.2f}  {flag}')
            for e in log_list:
                addr = e.get('addr', '?')
                cpu = e.get('cpu', 0)
                inp = e.get('input_hz', 0)
                qd = e.get('queue_disk_bytes', 0)
                print(f'    LOG:{addr.split(":")[-1]:<5s} input={fmt_mb(inp)}/s  q_disk={fmt_mb(qd)}  cpu={cpu:.2f}')
            for e in sl_list:
                addr = e.get('addr', '?')
                role = e.get('role_name', '?')
                cpu = e.get('cpu', 0)
                print(f'    SL :{addr.split(":")[-1]:<5s} ({role:<13s}) cpu={cpu:.2f}')
        else:
            if ss_list:
                cpus = [e.get('cpu',0) for e in ss_list]
                lags = [e.get('data_lag_s',0) for e in ss_list]
                ratios = []
                for e in ss_list:
                    inp = e.get('input_hz',0); dur = e.get('durable_hz',0)
                    ratios.append(inp/dur if dur > 0 else (0 if inp == 0 else 99))
                n_hot = sum(1 for e in ss_list if 'HOTSPOT' in ss_flag(e))
                n_sat = sum(1 for e in ss_list if 'SATURATED' in ss_flag(e))
                flags_str = ''
                if n_hot: flags_str += f' {n_hot} HOTSPOT'
                if n_sat: flags_str += f' {n_sat} SATURATED'
                print(f'    SS({len(ss_list)}):  in/dur={min(ratios):.1f}-{max(ratios):.1f}x  lag={min(lags):.1f}-{max(lags):.1f}s  cpu={min(cpus):.2f}-{max(cpus):.2f}{flags_str if flags_str else "  [OK]"}')
            if log_list:
                cpus = [e.get('cpu',0) for e in log_list]
                inps = [e.get('input_hz',0) for e in log_list]
                qds = [e.get('queue_disk_bytes',0) for e in log_list]
                print(f'    LOG({len(log_list)}): input={fmt_mb(sum(inps))}/s  q_disk={fmt_mb(sum(qds))}  cpu={max(cpus):.2f}')
            if sl_list:
                cpus = [e.get('cpu',0) for e in sl_list]
                print(f'    SL({len(sl_list)}):  cpu={min(cpus):.2f}-{max(cpus):.2f}')

        lq = s.get('qos_limiting_queue', 0)
        wq = s.get('qos_worst_queue_ss', 0)
        dl = s.get('qos_durability_lag_s', 0)
        cf = cluster_flag(s)
        print(f'    Cluster: lim_q={fmt_mb(lq)}  worst_ss_q={fmt_mb(wq)}  dur_lag={dl:.1f}s  {cf}')

    linear_mode = os.environ.get('FDB_LINEAR', '0') == '1'
    linear_interval = int(os.environ.get('FDB_INTERVAL', '5'))

    def exp_ticks(start, end):
        ticks = []
        t = 1
        while True:
            idx = start + int(t / sample_interval_s)
            if idx >= end - 2: break
            ticks.append(idx)
            t *= 2
        if end - 2 >= start and end - 2 not in ticks:
            ticks.append(end - 2)
        if end - 1 >= start and end - 1 not in ticks:
            ticks.append(end - 1)
        return ticks

    def linear_ticks(start, end, step):
        ticks = []
        for idx in range(start, end, step):
            ticks.append(idx)
        if end - 2 >= start and end - 2 not in ticks:
            ticks.append(end - 2)
        if end - 1 >= start and end - 1 not in ticks:
            ticks.append(end - 1)
        return sorted(set(ticks))

    def parse_ts(s):
        ts = s.get('ts') if s else None
        if not ts:
            return None
        try:
            return datetime.fromisoformat(ts.replace('Z', '+00:00'))
        except Exception:
            return None

    t0 = None
    for s in samples:
        t0 = parse_ts(s)
        if t0:
            break

    def elapsed_s(idx):
        if t0 is None:
            return idx * sample_interval_s
        i = idx
        while i < len(samples):
            t = parse_ts(samples[i])
            if t is not None:
                return int((t - t0).total_seconds())
            i += 1
        return idx * sample_interval_s

    if not long_mode and not linear_mode:
        print('\n  ==========================================')
        print('  FDB Cluster Health')
        print('  ==========================================')

        load_end = drain_line - 1 if drain_line > 0 else len(samples) - 1
        load_prev = max(0, load_end - 1)

        print_tick(f'=== LOAD (sample {load_prev}) ===', load_prev, load_end + 1)
        print_tick(f'=== LOAD (sample {load_end}) ===', load_end, drain_line)

        if drain_start_idx >= 0 and drain_start_idx < len(samples):
            drain_end = len(samples) - 1
            drain_prev = max(drain_start_idx, drain_end - 1)
            print_tick(f'=== DRAIN (sample {drain_prev}) ===', drain_prev, drain_end + 1)
            print_tick(f'=== DRAIN (sample {drain_end}) ===', drain_end, len(samples))
    else:
        if linear_mode:
            label = f'FDB Cluster Health (linear, every {linear_interval} samples)'
        else:
            label = 'FDB Cluster Health (exponential timeline)'
        print(f'\n  ==========================================')
        print(f'  {label}')
        print(f'  ==========================================')

        load_end = drain_line if drain_line < len(samples) else len(samples)
        if linear_mode:
            load_ticks_list = linear_ticks(0, load_end, linear_interval)
        else:
            load_ticks_list = exp_ticks(0, load_end)

        print('\n  === LOAD Phase ===')
        for ti, idx in enumerate(load_ticks_list):
            next_bound = load_ticks_list[ti + 1] if ti + 1 < len(load_ticks_list) else load_end
            elapsed = elapsed_s(idx) if linear_mode else idx * sample_interval_s
            print_tick(f't={elapsed}s (sample {idx})', idx, next_bound)

        if drain_start_idx >= 0 and drain_start_idx < len(samples):
            drain_end = len(samples)
            if linear_mode:
                drain_ticks_list = linear_ticks(drain_start_idx, drain_end, linear_interval)
            else:
                drain_ticks_list = exp_ticks(drain_start_idx, drain_end)

            print('\n  === DRAIN Phase ===')
            for ti, idx in enumerate(drain_ticks_list):
                next_bound = drain_ticks_list[ti + 1] if ti + 1 < len(drain_ticks_list) else drain_end
                if linear_mode:
                    elapsed = elapsed_s(idx)
                else:
                    elapsed = (idx - drain_start_idx) * sample_interval_s
                print_tick(f't={elapsed}s (sample {idx})', idx, next_bound)

PYEOF
}

FDB_LOG_FILE=$(ls "${RESULTS_DIR}"/fdb_stats_*.log 2>/dev/null | head -1 || true)

if [[ $show_fdb -eq 1 || $show_ss -eq 1 || $show_log -eq 1 || $show_sl -eq 1 || $show_health -eq 1 ]]; then
  if [[ -z "${FDB_LOG_FILE}" ]]; then
    echo "WARNING: no fdb_stats log found in ${RESULTS_DIR}"
  else
    if [[ $show_health -eq 1 ]]; then
      FDB_DISPLAY_MODE=health FDB_LONG="${LONG}" FDB_COMPACT="${COMPACT}" FDB_LINEAR="${LINEAR}" FDB_INTERVAL="${INTERVAL}" parse_fdb_stats "${FDB_LOG_FILE}"
    fi
    if [[ $show_fdb -eq 1 ]]; then
      FDB_DISPLAY_MODE=fdb parse_fdb_stats "${FDB_LOG_FILE}"
    fi
    if [[ $show_ss -eq 1 ]]; then
      FDB_DISPLAY_MODE=ss FDB_LONG="${LONG}" FDB_WARMUP="${WARMUP}" parse_fdb_stats "${FDB_LOG_FILE}"
    fi
    if [[ $show_log -eq 1 ]]; then
      FDB_DISPLAY_MODE=log FDB_LONG="${LONG}" FDB_WARMUP="${WARMUP}" parse_fdb_stats "${FDB_LOG_FILE}"
    fi
    if [[ $show_sl -eq 1 ]]; then
      FDB_DISPLAY_MODE=sl FDB_LONG="${LONG}" FDB_WARMUP="${WARMUP}" parse_fdb_stats "${FDB_LOG_FILE}"
    fi
  fi
fi

if [[ $show_kvrgw -eq 1 ]]; then
  echo ""
  echo "=========================================="
  echo "  KVRGW Stats"
  echo "=========================================="

  for stats_file in "${RESULTS_DIR}"/instance-*_*.stats; do
    [[ -f "$stats_file" ]] || continue
    inst_name=$(basename "$stats_file" | sed 's/_[0-9]*.stats$//')

    python3 - "${stats_file}" "${inst_name}" "${DEVIATION}" "${LONG}" "${WARMUP}" <<'KVRGW_PY'
import sys, re

stats_file = sys.argv[1]
inst_name = sys.argv[2]
deviation = float(sys.argv[3])
long_mode = sys.argv[4] == '1'
warmup_n = int(sys.argv[5])

# Parse into per-sample records grouped by --- separator
records = []
cur = {}
with open(stats_file) as f:
    for line in f:
        line = line.strip()
        if line.startswith('--- instance-'):
            if cur: records.append(cur)
            cur = {}
            continue
        if 'entries_hz=' in line:
            m = re.search(r'entries_hz=([\d.]+)', line)
            if m:
                v = float(m.group(1))
                if v > 0:
                    cur['iops'] = v
            m = re.search(r'commits_hz=([\d.]+)', line)
            if m: cur['commits'] = float(m.group(1))
            m = re.search(r'interval_wait_us=([\d.]+)', line)
            if m: cur['wait'] = float(m.group(1))
            m = re.search(r'interval_queue_size=([\d.]+)', line)
            if m: cur['queue'] = float(m.group(1))
        elif line.startswith('GetObject '):
            m = re.search(r'interval_total_us=([\d.]+)', line)
            if m: cur['get_lat'] = float(m.group(1))
            m = re.search(r'interval_hz=([\d.]+)', line)
            if m:
                v = float(m.group(1))
                if v > 0:
                    cur['get_hz'] = v
        elif line.startswith('PutObject '):
            m = re.search(r'interval_total_us=([\d.]+)', line)
            if m: cur['put_lat'] = float(m.group(1))
        elif 'GetObject_hz=' in line:
            m = re.search(r'GetObject_hz=([\d.]+)', line)
            if m:
                v = float(m.group(1))
                if v > 0:
                    cur['get_ops_hz'] = v
        elif 'interval_total_us=' in line:
            m = re.search(r'interval_total_us=([\d.]+)', line)
            if m and 'put_lat' not in cur and 'get_lat' not in cur:
                cur['fallback_lat'] = float(m.group(1))
        elif 'KVRGW_ERR_' in line:
            cur['err_by_type'] = {m.group(1): int(m.group(2))
                                  for m in re.finditer(r'(KVRGW_ERR_\w+)=(\d+)', line)}
        elif line == 'OK (all zero)':
            cur['err_by_type'] = {}
    if cur: records.append(cur)

for r in records:
    entries = r.get('iops')
    get_iops = r.get('get_ops_hz') or r.get('get_hz')
    if not entries and get_iops:
        r['iops'] = get_iops
    if get_iops and not entries:
        if 'get_lat' in r:
            r['latency'] = r['get_lat']
    elif 'put_lat' in r:
        r['latency'] = r['put_lat']
    elif 'fallback_lat' in r:
        r['latency'] = r['fallback_lat']
    elif 'get_lat' in r:
        r['latency'] = r['get_lat']

# Drop records with no data at all
records = [r for r in records if r]

# State machine: IDLE -> RAMPUP -> ACTIVE -> DRAIN (one-way)
state = 'IDLE'
nonzero_run = 0
for r in records:
    iops_val = r.get('iops')
    if state == 'IDLE':
        if iops_val is not None and iops_val > 0:
            state = 'RAMPUP'; nonzero_run = 1
    elif state == 'RAMPUP':
        if iops_val is not None and iops_val > 0:
            nonzero_run += 1
            if nonzero_run >= warmup_n:
                state = 'ACTIVE'
        else:
            nonzero_run = 0
    r['state'] = state

# Filter to ACTIVE records with valid non-zero IOPS (zero mid-test = collection artifact)
active = [r for r in records if r.get('state') == 'ACTIVE' and r.get('iops', 0) > 0]
total = len(records)

iops = [r['iops'] for r in active if r.get('iops') is not None]
latency = [r['latency'] for r in active if r.get('latency') is not None]
wait = [r['wait'] for r in active if r.get('wait') is not None]
queue = [r['queue'] for r in active if r.get('queue') is not None]
commits = [r['commits'] for r in active if r.get('commits') is not None]

# get-error-stats is a running total (never reset). Use max per type.
err_totals = {}
for r in records:
    for k, v in r.get('err_by_type', {}).items():
        err_totals[k] = max(err_totals.get(k, 0), v)
prev = {}
for r in records:
    if r.get('state') == 'ACTIVE' and r.get('iops', 0) > 0:
        break
    for k, v in r.get('err_by_type', {}).items():
        prev[k] = v
err_new = []
for r in active:
    delta = 0
    types = r.get('err_by_type', {})
    for k, v in types.items():
        delta += max(0, v - prev.get(k, 0))
        prev[k] = v
    if 'err_by_type' in r:
        err_new.append(delta)

def stats_row(label, vals):
    if not vals: return
    vals = sorted(vals)
    n = len(vals)
    mn, mx = min(vals), max(vals)
    avg = sum(vals) / n
    med = vals[n//2] if n % 2 == 1 else (vals[n//2-1] + vals[n//2]) / 2
    def pct(p):
        k = (p / 100.0) * (n - 1); f = int(k); c = min(f + 1, n - 1)
        return vals[f] + (vals[c] - vals[f]) * (k - f)
    p90, p95, p99 = pct(90), pct(95), pct(99)
    dev_thresh = deviation / 100.0
    outliers = sum(1 for v in vals if med > 0 and abs(v - med) / med > dev_thresh)
    dev_pct = 100.0 * outliers / n if n > 0 else 0
    print(f'  {label:<14s} {mn:>10.1f} {mx:>10.1f} {avg:>10.1f} {med:>10.1f} {p90:>10.1f} {p95:>10.1f} {p99:>10.1f} {dev_pct:>6.1f}%')

print(f'\n  --- {inst_name} (ACTIVE: {len(active)} of {total} samples, warmup={warmup_n}) ---')
print(f'  {"Metric":<14s} {"min":>10s} {"max":>10s} {"avg":>10s} {"median":>10s} {"p90":>10s} {"p95":>10s} {"p99":>10s} {"dev%":>6s}')
print(f'  {"-"*14} {"-"*10} {"-"*10} {"-"*10} {"-"*10} {"-"*10} {"-"*10} {"-"*10} {"-"*6}')
stats_row('IOPS', iops)
stats_row('latency_us', latency)
stats_row('wait_us', wait)
stats_row('queue_size', queue)
stats_row('err_new', err_new)
print('  Errors (lifetime):')
if err_totals:
    for k, v in sorted(err_totals.items(), key=lambda x: -x[1]):
        print(f'    {v:>8d}  {k}')
else:
    print('    (none)')

if long_mode:
    print()
    def long_row(label, vals):
        if not vals: return
        vals = sorted(vals)
        n = len(vals)
        mn, mx = min(vals), max(vals)
        avg = sum(vals) / n
        med = vals[n//2] if n % 2 == 1 else (vals[n//2-1] + vals[n//2]) / 2
        dev_thresh = deviation / 100.0
        outliers = sum(1 for v in vals if med > 0 and abs(v - med) / med > dev_thresh)
        dev_pct = 100.0 * outliers / n if n > 0 else 0
        print(f'  {label:.<40s} min={mn:>12.2f}  max={mx:>12.2f}  avg={avg:>12.2f}  median={med:>12.2f}  outliers({int(deviation)}%)={dev_pct:.1f}%')
    long_row('entries/sec', list(iops))
    long_row('commits/sec', list(commits))
    long_row('wait_us', list(wait))
    long_row('queue_size', list(queue))
    long_row('new errors/sample', list(err_new))
KVRGW_PY
  done

  if [[ $LONG -eq 1 ]]; then
    for inst_log in "${RESULTS_DIR}"/instance-*_*.log; do
      [[ -f "$inst_log" ]] || continue
      inst_name=$(basename "$inst_log" | sed 's/_[0-9]*.log$//')
      echo ""
      echo "  --- ${inst_name} final results ---"
      grep -E 'IOPS=|batch_commits=|Error breakdown' "$inst_log" 2>/dev/null | sed 's/^/    /' || echo "    (no data)"
    done
  fi
fi

echo ""
echo "Deviation threshold: ${DEVIATION}%"
