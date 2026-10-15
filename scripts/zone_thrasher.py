#!/usr/bin/env python3
"""
zone_thrasher.py -- Stretch EC zone thrasher for vstart clusters.

Repeatedly takes down all OSDs and the zone monitor for a randomly chosen
datacenter zone, waits for the cluster to enter degraded stretch mode, holds
for a configurable duration (to stress the degraded codepath), then revives
everything and waits for the cluster to return to healthy stretch mode.

Zone topology is auto-discovered from the live cluster:
  - Datacenter buckets are read from: ceph osd crush tree -f json
  - Monitor zone assignments are read from: ceph mon dump -f json
  - The tiebreak monitor is identified from: mon dump tiebreaker_mon field
  - OSD process PIDs are found via:  ceph osd find <id> -f json  (for the
    data path) and the running ceph-osd process table (for signal delivery)

Usage:
    python3 scripts/zone_thrasher.py [options]

    Run from the ceph build directory (the one containing ./bin/ceph).

Examples:
    # Auto-discover zones, kill each for 60 s, recover, repeat
    python3 scripts/zone_thrasher.py

    # Longer hold, slower recovery stress
    python3 scripts/zone_thrasher.py --hold-duration 120 --thrash-delay 60

    # Single pass (kill one zone once then exit cleanly)
    python3 scripts/zone_thrasher.py --iterations 1

    # Dry run - print what would be done without doing it
    python3 scripts/zone_thrasher.py --dry-run
"""

import argparse
import json
import os
import random
import signal
import subprocess
import sys
import time


# ---------------------------------------------------------------------------
# Ceph command helpers
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Build directory detection
# ---------------------------------------------------------------------------

def find_ceph_binary():
    """
    Locate the ceph binary. Checks in order:
      1. CEPH_BUILD_DIR environment variable
      2. ./bin/ceph  (running from build dir)
      3. build/bin/ceph  (running from source root)
      4. PATH (installed ceph)
    """
    # Explicit override
    build_dir = os.environ.get('CEPH_BUILD_DIR')
    if build_dir:
        candidate = os.path.join(build_dir, 'bin', 'ceph')
        if os.path.isfile(candidate):
            return candidate
        print(f'Warning: CEPH_BUILD_DIR={build_dir} set but {candidate} not found',
              flush=True)

    # Running from inside the build directory
    candidate = os.path.join(os.getcwd(), 'bin', 'ceph')
    if os.path.isfile(candidate):
        return candidate

    # Running from the source root (build/ is a sibling of src/)
    candidate = os.path.join(os.getcwd(), 'build', 'bin', 'ceph')
    if os.path.isfile(candidate):
        return candidate

    # Fall back to whatever is on PATH
    import shutil
    found = shutil.which('ceph')
    if found:
        return found

    print('Error: cannot find the ceph binary.', flush=True)
    print('  Run from the build directory, set CEPH_BUILD_DIR, or install ceph.',
          flush=True)
    sys.exit(1)


CEPH_BIN = None  # populated in main() before any commands run


def run_ceph(args, debug=False, verbose=False):
    """
    Run a 'ceph ...' command and return stdout as a string.
    args is a list of strings NOT including the 'ceph' binary itself.
    debug=True logs the command being run.
    verbose=True also dumps the full stdout (useful for small responses
    like crush tree / mon dump; avoid for osd dump which is very large).
    Exits on failure.
    """
    cmd = [CEPH_BIN] + args
    if debug:
        print(f'[DEBUG] Running: {" ".join(cmd)}', flush=True)
    try:
        result = subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if verbose and result.stdout.strip():
            print(f'[DEBUG] stdout: {result.stdout.strip()}', flush=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f'Error running: {" ".join(cmd)}', flush=True)
        print(f'  returncode: {e.returncode}', flush=True)
        print(f'  stderr: {e.stderr.strip()}', flush=True)
        sys.exit(1)


def run_ceph_json(args, debug=False, verbose=False):
    """Run a ceph command, parse and return the JSON output."""
    return json.loads(run_ceph(args + ['-f', 'json'], debug=debug, verbose=verbose))


def find_build_dir():
    """Return the ceph build directory regardless of where the script is run from."""
    build_dir = os.environ.get('CEPH_BUILD_DIR')
    if build_dir and os.path.isdir(build_dir):
        return build_dir
    # Running from inside the build directory
    if os.path.isfile(os.path.join(os.getcwd(), 'bin', 'ceph')):
        return os.getcwd()
    # Running from the source root (build/ is a subdirectory)
    candidate = os.path.join(os.getcwd(), 'build')
    if os.path.isfile(os.path.join(candidate, 'bin', 'ceph')):
        return candidate
    return os.getcwd()


def find_vstart_pid_dir():
    """
    Return the directory where vstart writes PID files.
    Defaults to <build_dir>/out.
    """
    out_dir = os.path.join(find_build_dir(), 'out')
    if not os.path.isdir(out_dir):
        print(f"Error: vstart output directory not found at {out_dir}", flush=True)
        print("Run from the build directory or set CEPH_BUILD_DIR.", flush=True)
        sys.exit(1)
    return out_dir


# ---------------------------------------------------------------------------
# Zone topology discovery
# ---------------------------------------------------------------------------

def discover_zones(debug=False):
    """
    Query the live cluster and return a topology dict:

        {
            'zones': [
                {'name': 'zone1', 'osds': [0,1,2,...], 'mon': 'a'},
                {'name': 'zone2', 'osds': [3,4,5,...], 'mon': 'b'},
            ],
            'tiebreak_mon': 'c',
        }

    Raises SystemExit if the cluster is not configured as a 2-zone stretch
    cluster or if topology cannot be determined.
    """
    print('--> Discovering zone topology from cluster...', flush=True)

    # --- Step 1: find datacenter buckets and their member OSDs via crush tree ---
    crush = run_ceph_json(['osd', 'crush', 'tree'], debug=debug, verbose=debug)
    nodes = crush.get('nodes', [])

    # Build id -> node lookup for fast child traversal
    by_id = {n['id']: n for n in nodes}

    # Find all buckets of type 'datacenter'
    # The JSON field is 'type' (string), not 'type_name'
    datacenter_buckets = [n for n in nodes if n.get('type') == 'datacenter']

    if not datacenter_buckets:
        print('Error: No datacenter buckets found in CRUSH tree.', flush=True)
        print('  The cluster must be configured with datacenter-type CRUSH buckets', flush=True)
        print('  for stretch EC zone discovery to work.', flush=True)
        sys.exit(1)

    # Exclude the tiebreak datacenter (it holds no OSDs — only the arbiter mon)
    data_buckets = []
    for bucket in datacenter_buckets:
        osds = _collect_osds(bucket, by_id)
        if osds:
            data_buckets.append((bucket['name'], osds))

    if len(data_buckets) < 2:
        print(f'Error: Found {len(data_buckets)} datacenter bucket(s) with OSDs '
              f'(need at least 2 for stretch mode).', flush=True)
        sys.exit(1)

    if debug:
        for name, osds in data_buckets:
            print(f'[DEBUG] datacenter bucket "{name}": OSDs {sorted(osds)}', flush=True)

    # --- Step 2: get monitor zone assignments and tiebreak mon from mon dump ---
    mon_dump = run_ceph_json(['mon', 'dump'], debug=debug, verbose=debug)
    tiebreak_mon = mon_dump.get('tiebreaker_mon', '')

    # crush_location for each mon: {"datacenter": "zone1", "root": "default"}
    mon_by_datacenter = {}
    for mon_info in mon_dump.get('mons', []):
        mon_name = mon_info['name']
        # crush_location is a list of {"key": ..., "val": ...} pairs
        crush_loc_raw = mon_info.get('crush_location', [])
        if isinstance(crush_loc_raw, list):
            crush_loc = {entry['key']: entry['val'] for entry in crush_loc_raw}
        else:
            crush_loc = crush_loc_raw
        dc = crush_loc.get('datacenter', '')
        if dc and mon_name != tiebreak_mon:
            mon_by_datacenter[dc] = mon_name
        if debug:
            print(f'[DEBUG] mon.{mon_name}: crush_location={crush_loc}', flush=True)

    if not tiebreak_mon:
        print('Error: No tiebreaker_mon found in mon dump.', flush=True)
        print('  Is stretch mode enabled on this cluster?', flush=True)
        sys.exit(1)

    print(f'--> Tiebreak monitor: mon.{tiebreak_mon}', flush=True)

    # --- Step 3: assemble the final zone list ---
    zones = []
    for dc_name, osds in sorted(data_buckets):
        mon = mon_by_datacenter.get(dc_name)
        if not mon:
            print(f'Warning: No monitor found for datacenter "{dc_name}". '
                  f'Zone will be killed without a monitor kill.', flush=True)
        zones.append({
            'name': dc_name,
            'osds': sorted(osds),
            'mon': mon,
        })
        print(f'--> Zone "{dc_name}": OSDs={sorted(osds)}, mon={mon}', flush=True)

    # --- Step 4: warn if CRUSH bucket weights are unbalanced ---
    # An imbalance triggers STRETCH_MODE_BUCKET_WEIGHT_IMBALANCE in the OSDMap
    # health checks.  It does not block degraded/recovering transitions, but it
    # does mean PGs may end up stuck in clean+remapped+peered (not active) after
    # recovery because CRUSH cannot satisfy the 2-bucket peering constraint
    # evenly.  Warn early so the user can equalise weights before thrashing.
    _check_crush_zone_weight_balance(nodes, by_id, data_buckets)

    return {'zones': zones, 'tiebreak_mon': tiebreak_mon}


def _check_crush_zone_weight_balance(nodes, by_id, data_buckets):
    """
    Warn if the two data datacenter buckets have significantly different CRUSH
    weights (> 10% delta).  Unbalanced zones cause
    STRETCH_MODE_BUCKET_WEIGHT_IMBALANCE and can leave PGs stuck in
    clean+remapped+peered after stretch recovery.
    """
    if len(data_buckets) != 2:
        return
    # Look up the datacenter bucket nodes by name to get their weights
    name_to_node = {n['name']: n for n in nodes if n.get('type') == 'datacenter'}
    weights = []
    for dc_name, _ in data_buckets:
        node = name_to_node.get(dc_name)
        if node is None:
            return
        # CRUSH weights in the tree JSON are stored under 'crush_weight' (float)
        # which is the sum of children.  Fall back to 'weight' if absent.
        w = node.get('crush_weight', node.get('weight'))
        if w is None:
            return
        weights.append((dc_name, float(w)))

    (name1, w1), (name2, w2) = weights
    if w1 == 0 or w2 == 0:
        return
    delta_pct = abs(w1 - w2) / min(w1, w2) * 100
    if delta_pct > 10.0:
        print(
            f'\n[WARN] CRUSH weight imbalance detected between zones:\n'
            f'       {name1}: {w1:.4f}  vs  {name2}: {w2:.4f}  '
            f'(delta {delta_pct:.1f}%)\n'
            f'       This will produce a STRETCH_MODE_BUCKET_WEIGHT_IMBALANCE\n'
            f'       health warning and may leave PGs stuck in\n'
            f'       clean+remapped+peered (not active) after recovery.\n'
            f'       Fix with: ceph osd reweight-by-utilization  OR\n'
            f'       manually equalise OSD weights across both datacenters.',
            flush=True,
        )


def _collect_osds(bucket, by_id):
    """
    Recursively collect all OSD IDs (type 'osd', id >= 0) under a bucket node.
    """
    osds = []
    for child_id in bucket.get('children', []):
        child = by_id.get(child_id)
        if child is None:
            continue
        if child.get('type') == 'osd' and child['id'] >= 0:
            osds.append(child['id'])
        else:
            osds.extend(_collect_osds(child, by_id))
    return osds


# ---------------------------------------------------------------------------
# Stretch mode status
# ---------------------------------------------------------------------------

def is_degraded_stretch_mode(debug=False):
    """Return True if the cluster is currently in degraded stretch mode."""
    osd_dump = run_ceph_json(['osd', 'dump'], debug=debug)
    stretch = osd_dump.get('stretch_mode', {})
    return stretch.get('degraded_stretch_mode', 0) == 1


def get_pg_stats(debug=False):
    """
    Return (inactive_pgs, non_clean_pgs, total_pgs, states_dict) from 'ceph status'.
    Parses pgmap.num_pg_by_state or pgmap.pgs_by_state to ensure accurate counts.
    """
    status = run_ceph_json(['status'], debug=debug)
    pgmap = status.get('pgmap', {})
    total_pgs = pgmap.get('num_pgs', 0)

    # In Ceph JSON, state map may appear under 'num_pg_by_state' or 'pgs_by_state'
    states = {}
    if 'num_pg_by_state' in pgmap:
        states = pgmap['num_pg_by_state']
    elif 'pgs_by_state' in pgmap:
        for entry in pgmap['pgs_by_state']:
            state_name = entry.get('state_name')
            count = entry.get('count', 0)
            if state_name:
                states[state_name] = count

    inactive_pgs = 0
    non_clean_pgs = 0
    for state_name, count in states.items():
        state_tokens = state_name.split('+')
        if 'active' not in state_tokens:
            inactive_pgs += count
        if 'clean' not in state_tokens:
            non_clean_pgs += count

    return inactive_pgs, non_clean_pgs, total_pgs, states


def wait_for_degraded_stretch(timeout, debug=False):
    """
    Block until degraded_stretch_mode == 1.
    Raises RuntimeError on timeout.
    """
    print(f'--> Waiting for degraded stretch mode (timeout={timeout}s)...', flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        if is_degraded_stretch_mode(debug=debug):
            print('--> Cluster entered degraded stretch mode.', flush=True)
            return
        time.sleep(5)
    raise RuntimeError(
        f'Timed out after {timeout}s waiting for cluster to enter '
        f'degraded stretch mode. Check OSD/mon logs.'
    )


def wait_for_healthy_stretch(timeout, debug=False):
    """
    Block until degraded_stretch_mode == 0 AND recovering_stretch_mode == 0
    AND all PGs are active+clean (no inactive or unpeered/undersized PGs).
    """
    print(f'--> Waiting for healthy stretch mode and all PGs active+clean '
          f'(timeout={timeout}s)...', flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        osd_dump = run_ceph_json(['osd', 'dump'], debug=debug)
        stretch = osd_dump.get('stretch_mode', {})
        degraded = stretch.get('degraded_stretch_mode', 0)
        recovering = stretch.get('recovering_stretch_mode', 0)

        inactive, non_clean, total, states = get_pg_stats(debug=debug)
        state_summary = ', '.join(f'{k}: {v}' for k, v in sorted(states.items())) or 'none'

        if degraded == 0 and recovering == 0:
            if inactive == 0 and non_clean == 0:
                print(f'--> Cluster healthy: stretch mode restored, all {total} PGs active+clean.',
                      flush=True)
                return
            else:
                print(f'--> Stretch flags clear, but waiting on PGs '
                      f'(inactive={inactive}, non_clean={non_clean}/{total}) [{state_summary}]...',
                      flush=True)
        else:
            print(f'--> Waiting: degraded={degraded} recovering={recovering} '
                  f'(inactive={inactive}, non_clean={non_clean}/{total}) [{state_summary}]',
                  flush=True)

        time.sleep(10)
    raise RuntimeError(
        f'Timed out after {timeout}s waiting for cluster to return to '
        f'healthy stretch mode and all PGs active+clean. Recovery may be stuck.'
    )


# ---------------------------------------------------------------------------
# Monitor quorum
# ---------------------------------------------------------------------------

def get_quorum_names(debug=False):
    """Return the list of monitor names currently in quorum."""
    status = run_ceph_json(['quorum_status'], debug=debug)
    return status.get('quorum_names', [])


def wait_for_full_quorum(expected_count, timeout=300, debug=False):
    """Block until all expected monitors are in quorum."""
    print(f'--> Waiting for full quorum ({expected_count} mons)...', flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        names = get_quorum_names(debug=debug)
        if len(names) == expected_count:
            print(f'--> Full quorum reached: {names}', flush=True)
            return
        time.sleep(3)
    raise RuntimeError(
        f'Timed out after {timeout}s waiting for quorum of size {expected_count}. '
        f'Current quorum: {get_quorum_names()}'
    )


# ---------------------------------------------------------------------------
# OSD daemon control via vstart PID files
# ---------------------------------------------------------------------------

def _osd_pid_file(out_dir, osd_id):
    return os.path.join(out_dir, f'osd.{osd_id}.pid')


def _read_pid(pid_file):
    try:
        with open(pid_file) as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


def kill_osd(osd_id, out_dir, dry_run=False, debug=False):
    """
    Stop an OSD by sending SIGTERM to its process.
    Does not mark it out — simulates a hard failure.
    """
    pid_file = _osd_pid_file(out_dir, osd_id)
    pid = _read_pid(pid_file)
    if pid is None:
        print(f'  [WARN] No PID file for osd.{osd_id} at {pid_file}, '
              f'trying ceph daemon exit', flush=True)
        _daemon_exit('osd', osd_id, dry_run=dry_run, debug=debug)
        return
    print(f'  Stopping osd.{osd_id} (pid {pid})', flush=True)
    if not dry_run:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            print(f'  [WARN] osd.{osd_id} pid {pid} already gone', flush=True)


def revive_osd(osd_id, out_dir, dry_run=False, debug=False):
    """
    Restart an OSD using vstart's per-OSD run script.
    Falls back to ceph-osd -i if the run script is not found.
    """
    build_dir = find_build_dir()
    run_script = os.path.join(build_dir, f'run-osd.{osd_id}.sh')
    if os.path.exists(run_script):
        print(f'  Starting osd.{osd_id} via {run_script}', flush=True)
        if not dry_run:
            subprocess.Popen(
                ['bash', run_script],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        return

    # Fallback: reconstruct the ceph-osd invocation from the cluster config
    ceph_osd_bin = os.path.join(build_dir, 'bin', 'ceph-osd')
    conf = os.path.join(build_dir, 'ceph.conf')
    print(f'  Starting osd.{osd_id} via ceph-osd -i', flush=True)
    cmd = [ceph_osd_bin, '-i', str(osd_id), '--foreground', '-c', conf]
    if debug:
        print(f'  [DEBUG] {" ".join(cmd)}', flush=True)
    if not dry_run:
        subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


# ---------------------------------------------------------------------------
# Monitor daemon control
# ---------------------------------------------------------------------------

def _mon_pid_file(out_dir, mon_id):
    return os.path.join(out_dir, f'mon.{mon_id}.pid')


def _daemon_exit(daemon_type, daemon_id, dry_run=False, debug=False):
    """Ask a daemon to exit cleanly via the admin socket."""
    cmd = ['./bin/ceph', 'daemon', f'{daemon_type}.{daemon_id}', 'exit']
    if debug:
        print(f'  [DEBUG] {" ".join(cmd)}', flush=True)
    if not dry_run:
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def kill_mon(mon_id, out_dir, dry_run=False, debug=False):
    """Stop a monitor by sending SIGTERM to its process."""
    pid_file = _mon_pid_file(out_dir, mon_id)
    pid = _read_pid(pid_file)
    if pid is None:
        print(f'  [WARN] No PID file for mon.{mon_id} at {pid_file}, '
              f'trying ceph daemon exit', flush=True)
        _daemon_exit('mon', mon_id, dry_run=dry_run, debug=debug)
        return
    print(f'  Stopping mon.{mon_id} (pid {pid})', flush=True)
    if not dry_run:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            print(f'  [WARN] mon.{mon_id} pid {pid} already gone', flush=True)


def revive_mon(mon_id, out_dir, dry_run=False, debug=False):
    """Restart a monitor using vstart's per-mon run script or ceph-mon -i."""
    build_dir = find_build_dir()
    run_script = os.path.join(build_dir, f'run-mon.{mon_id}.sh')
    if os.path.exists(run_script):
        print(f'  Starting mon.{mon_id} via {run_script}', flush=True)
        if not dry_run:
            subprocess.Popen(
                ['bash', run_script],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        return

    ceph_mon_bin = os.path.join(build_dir, 'bin', 'ceph-mon')
    conf = os.path.join(build_dir, 'ceph.conf')
    print(f'  Starting mon.{mon_id} via ceph-mon -i', flush=True)
    cmd = [ceph_mon_bin, '-i', mon_id, '--foreground', '-c', conf]
    if debug:
        print(f'  [DEBUG] {" ".join(cmd)}', flush=True)
    if not dry_run:
        subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


# ---------------------------------------------------------------------------
# Zone kill / revive
# ---------------------------------------------------------------------------

def kill_zone(zone, out_dir, dry_run=False, debug=False):
    """Stop all OSDs and the monitor for the given zone."""
    print(f"\n--- Killing zone '{zone['name']}' "
          f"(OSDs: {zone['osds']}, mon: {zone['mon']}) ---", flush=True)
    for osd_id in zone['osds']:
        kill_osd(osd_id, out_dir, dry_run=dry_run, debug=debug)
    if zone['mon']:
        kill_mon(zone['mon'], out_dir, dry_run=dry_run, debug=debug)


def revive_zone(zone, out_dir, dry_run=False, debug=False, quorum_size=None):
    """Restart the monitor then all OSDs for the given zone.

    The monitor is started first and we block until it rejoins quorum.  OSDs
    are only started after the monitor is back, which ensures the mon leader
    sees dead_mon_buckets == 0 when it processes the OSD boot beacons and can
    therefore transition straight into recovering_stretch_mode without needing
    an external nudge.  Starting OSDs while the zone mon is still absent causes
    the OSD-boot path in update_from_paxos to skip go_recovery_stretch_mode()
    (because dead_mon_buckets.size() != 0), and the cluster gets stuck.
    """
    print(f"\n--- Reviving zone '{zone['name']}' ---", flush=True)
    if zone['mon']:
        revive_mon(zone['mon'], out_dir, dry_run=dry_run, debug=debug)
        if not dry_run and quorum_size is not None:
            # Block until the mon rejoins quorum so that dead_mon_buckets is
            # cleared on the leader before OSD boot beacons arrive.
            wait_for_full_quorum(quorum_size, timeout=120, debug=debug)
    for osd_id in zone['osds']:
        revive_osd(osd_id, out_dir, dry_run=dry_run, debug=debug)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def thrash_loop(topology, out_dir, hold_duration, degrade_timeout,
                revive_timeout, thrash_delay, iterations, dry_run, debug,
                rng):
    """
    Main thrash loop. Runs for `iterations` cycles (0 = infinite).
    Returns True if all iterations completed without error.
    """
    zones = topology['zones']
    total_mons = len(run_ceph_json(['mon', 'dump'], debug=debug).get('mons', []))

    iteration = 0
    while iterations == 0 or iteration < iterations:
        iteration += 1
        zone = rng.choice(zones)

        print(f'\n{"="*60}', flush=True)
        print(f'Iteration {iteration}: thrashing zone "{zone["name"]}"', flush=True)
        print(f'{"="*60}', flush=True)

        # 1. Kill the zone
        kill_zone(zone, out_dir, dry_run=dry_run, debug=debug)

        # 2. Wait for degraded stretch mode
        if not dry_run:
            wait_for_degraded_stretch(degrade_timeout, debug=debug)
        else:
            print(f'[DRY RUN] Would wait up to {degrade_timeout}s for degraded stretch mode',
                  flush=True)

        # 3. Hold — IO stress window in degraded state
        print(f"\n--> Holding zone '{zone['name']}' down for {hold_duration}s...",
              flush=True)
        if not dry_run:
            time.sleep(hold_duration)
        else:
            print(f'[DRY RUN] Would sleep {hold_duration}s', flush=True)

        # 4. Revive the zone — pass quorum_size so the mon is confirmed back
        # in quorum before OSDs are started (see revive_zone docstring).
        revive_zone(zone, out_dir, dry_run=dry_run, debug=debug,
                    quorum_size=total_mons)

        # 5. Confirm full quorum (may already be satisfied by revive_zone, but
        # re-check with the longer revive_timeout to catch slow cases).
        if not dry_run:
            wait_for_full_quorum(total_mons, timeout=revive_timeout, debug=debug)
            # Ensure stretch recovery mode is triggered if OSDs booted before quorum formed
            osd_dump = run_ceph_json(['osd', 'dump'], debug=debug)
            stretch = osd_dump.get('stretch_mode', {})
            if stretch.get('degraded_stretch_mode', 0) == 1 and stretch.get('recovering_stretch_mode', 0) == 0:
                num_up = osd_dump.get('num_up_osds', len(osd_dump.get('up_osds', [])))
                num_total = len(osd_dump.get('osds', []))
                if num_total > 0 and (num_up / num_total) > 0.5:
                    print('--> Quorum restored and OSDs up; nudging monitor into recovery stretch mode...', flush=True)
                    try:
                        run_ceph(['osd', 'force_recovery_stretch_mode', '--yes-i-really-mean-it'], debug=debug)
                    except Exception as e:
                        if debug:
                            print(f'[DEBUG] force_recovery_stretch_mode error: {e}', flush=True)
        else:
            print(f'[DRY RUN] Would wait for quorum of {total_mons} mons', flush=True)

        # 6. Wait for healthy stretch mode
        if not dry_run:
            wait_for_healthy_stretch(revive_timeout, debug=debug)
        else:
            print(f'[DRY RUN] Would wait up to {revive_timeout}s for healthy stretch mode',
                  flush=True)

        print(f"\n--> Iteration {iteration} complete: zone '{zone['name']}' recovered.",
              flush=True)

        if iterations == 0 or iteration < iterations:
            print(f'--> Sleeping {thrash_delay}s before next iteration...', flush=True)
            if not dry_run:
                time.sleep(thrash_delay)

    return True


def cleanup(topology, out_dir, debug=False):
    """
    Best-effort cleanup: revive all zones and wait for full quorum.
    Called on KeyboardInterrupt or unhandled exception.
    """
    print('\n--> Cleanup: reviving all zones...', flush=True)
    total_mons = len(topology['zones']) + 1  # zones + tiebreak
    for zone in topology['zones']:
        try:
            revive_zone(zone, out_dir, dry_run=False, debug=debug,
                        quorum_size=total_mons)
        except Exception as e:
            print(f'  [WARN] Failed to revive zone {zone["name"]}: {e}', flush=True)

    try:
        wait_for_full_quorum(total_mons, timeout=120, debug=debug)
    except RuntimeError as e:
        print(f'  [WARN] {e}', flush=True)

    print('--> Cleanup complete.', flush=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            'Stretch EC zone thrasher for vstart clusters.\n\n'
            'Repeatedly kills all daemons in a random zone to trigger degraded\n'
            'stretch mode, holds for --hold-duration seconds, then revives and\n'
            'waits for healthy stretch mode to be restored.\n\n'
            'Run from the ceph build directory (the one containing ./bin/ceph).'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--hold-duration', type=int, default=60, metavar='SECS',
        help='Seconds to keep the zone down (default: 60)',
    )
    parser.add_argument(
        '--degrade-timeout', type=int, default=300, metavar='SECS',
        help='Seconds to wait for degraded stretch mode to engage (default: 300)',
    )
    parser.add_argument(
        '--revive-timeout', type=int, default=600, metavar='SECS',
        help='Seconds to wait for healthy stretch mode after reviving (default: 600)',
    )
    parser.add_argument(
        '--thrash-delay', type=int, default=30, metavar='SECS',
        help='Seconds to sleep between iterations (default: 30)',
    )
    parser.add_argument(
        '--iterations', type=int, default=0, metavar='N',
        help='Number of thrash iterations (default: 0 = infinite)',
    )
    parser.add_argument(
        '--seed', type=int, default=None,
        help='RNG seed for reproducibility (default: random)',
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Print what would be done without killing any daemons',
    )
    parser.add_argument(
        '--debug', action='store_true',
        help='Print raw ceph commands and their output',
    )
    args = parser.parse_args()

    # Resolve the ceph binary once, before any commands are issued
    global CEPH_BIN
    CEPH_BIN = find_ceph_binary()

    seed = args.seed if args.seed is not None else int(time.time())
    rng = random.Random(seed)

    print('=' * 60, flush=True)
    print('Stretch EC Zone Thrasher', flush=True)
    print('=' * 60, flush=True)
    print(f'  hold-duration:   {args.hold_duration}s', flush=True)
    print(f'  degrade-timeout: {args.degrade_timeout}s', flush=True)
    print(f'  revive-timeout:  {args.revive_timeout}s', flush=True)
    print(f'  thrash-delay:    {args.thrash_delay}s', flush=True)
    print(f'  iterations:      {"infinite" if args.iterations == 0 else args.iterations}',
          flush=True)
    print(f'  seed:            {seed}', flush=True)
    if args.dry_run:
        print('  *** DRY RUN — no daemons will be killed ***', flush=True)
    print('=' * 60, flush=True)

    # Discover topology
    topology = discover_zones(debug=args.debug)
    out_dir = find_vstart_pid_dir()

    print(f'\nPress Ctrl+C to stop.\n', flush=True)

    try:
        thrash_loop(
            topology=topology,
            out_dir=out_dir,
            hold_duration=args.hold_duration,
            degrade_timeout=args.degrade_timeout,
            revive_timeout=args.revive_timeout,
            thrash_delay=args.thrash_delay,
            iterations=args.iterations,
            dry_run=args.dry_run,
            debug=args.debug,
            rng=rng,
        )
    except RuntimeError as e:
        print(f'\nFATAL: {e}', flush=True)
        cleanup(topology, out_dir, debug=args.debug)
        sys.exit(1)
    except KeyboardInterrupt:
        print('\n\nInterrupted by user.', flush=True)
        cleanup(topology, out_dir, debug=args.debug)
        sys.exit(0)

    print('\nAll iterations complete.', flush=True)


if __name__ == '__main__':
    main()
