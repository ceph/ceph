#!/usr/bin/env python3
"""
Fault injection for a pool on the vstart cluster in $CEPH_BUILD (source env.sh).
Works on replicated and erasure coded pools, in one zone or over two
datacenters: it reads the pool's type, size and zones and only uses the
actions and clients that apply.  A pool spans zones (datacenter buckets) if
it was created with num_zones > 1, where the monitors support that, or if it
is a replicated pool in global stretch mode ('ceph mon enable_stretch_mode');
zone size is size / num_zones or size / stretch_bucket_count.
Random OSD faults run while verifying workloads:

  rados-plain       ceph_test_rados
  rados-local[-z]   ceph_test_rados with localized reads (one per zone)
  rados-balanced    ceph_test_rados with balanced reads
  ioseq             ceph_test_rados_io_sequence
  rbd               random rbd writes with data in the pool, read back and compared
  probe-*           io_probe.py clients, watched for stuck ops

Actions for every pool, keeping each zone within its failure budget (m for
an EC pool, otherwise size - min_size, or zone size - 1 with zones):
  mark_down, out_in, kill_restart, upmap_items, rm_upmap, repeer, deep_scrub
  reweight          osd reweight of an in OSD to 0.5, 0.75 or 1
  primary_affinity  osd primary-affinity 0, 0.5 or 1
  pg_num            double or halve the pool's pg_num (split/merge), one at a time
Quiesce resets reweight and primary-affinity to 1.

Multi-zone pools also get:
  upmap_zone, upmap_flip  re-place or swap a PG's zone blocks (for a
                          replicated pool a swap moves the primary's zone)
  zone_partial            (EC) kill enough of a zone's OSDs to drop it below k
  zone_failover           (stretch mode) whole zone failures (zone OSDs and
                          zone monitor), run as a state machine
                          across cycles so other actions and workloads continue
                          while a zone is down. If stretch mode is still
                          degraded (not recovering) --nudge-after seconds
                          after the revive, with full quorum, that is
                          recorded as a finding and the monitors are nudged
                          with 'osd force_recovery_stretch_mode'. Variants:
    standard        kill zone OSDs+mon, wait degraded, hold, revive mon then OSDs
    osds_first      as standard, but revive OSDs before the mon
    flap            once the revived zone is recovering, kill the other zone
    surviving_loss  while degraded, also kill a failure budget of OSDs in the
                    surviving zone
    osds_only       kill every OSD in a zone but leave its mon running
    mon_only        kill only the zone mon

Stops (leaving the cluster as it is, but with any monitor it killed started
again) on: daemon crash signature, a daemon dying that we did not kill,
workload failure/miscompare, a workload still running long after everything
is revived at the end, scrub inconsistency, a quiesce deep scrub not
finishing, low disk space, or PGs of any pool (and stretch mode) not returning
to active+clean, and not remapped, after everything is revived.
Non-fatal oddities are appended to <rundir>/findings.log with diagnostics.
"""

import argparse
import json
import os
import random
import signal
import socket
import subprocess
import sys
import time

BUILD = os.environ["CEPH_BUILD"]
OUT = f"{BUILD}/out"
CONF = f"{BUILD}/ceph.conf"
# Daemon/tool binaries: default the main build; CHAOS_BIN_DIR points at a
# snapshot of a fixed build (its lib/ must be first in LD_LIBRARY_PATH).
BIN = os.environ.get("CHAOS_BIN_DIR", f"{BUILD}/bin")
CRUSH_ITEM_NONE = 2147483647


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def ceph(cmd, timeout=90, quiet=False):
    try:
        r = subprocess.run(f"ceph --connect-timeout 30 {cmd}", shell=True,
                           capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        if not quiet:
            log(f"  ceph {cmd}: timed out")
        return None
    if r.returncode != 0:
        if not quiet:
            log(f"  ceph {cmd}: rc={r.returncode} {r.stderr.strip()[:200]}")
        return None
    return r.stdout


def ceph_json(cmd, quiet=False):
    out = ceph(f"{cmd} -f json", quiet=quiet)
    try:
        return json.loads(out) if out else None
    except json.JSONDecodeError:
        return None


def datacenter_osds():
    """{datacenter: [osds]} for the CRUSH datacenter buckets that hold OSDs."""
    nodes = {n["id"]: n for n in ceph_json("osd crush tree")["nodes"]}

    def leaves(i):
        return [i] if i >= 0 else sum((leaves(c) for c in nodes[i].get("children", [])), [])
    zones = {n["name"]: sorted(leaves(i)) for i, n in nodes.items()
             if n["type"] == "datacenter"}
    return {z: osds for z, osds in zones.items() if osds}


def free_gb(path):
    st = os.statvfs(path)
    return st.f_bavail * st.f_frsize / (1 << 30)


def dup_acting(pg):
    osds = [o for o in pg["acting"] if o != CRUSH_ITEM_NONE]
    return len(osds) != len(set(osds))


def daemon_env():
    env = dict(os.environ)
    env.pop("CEPH_KEYRING", None)
    return env


class Daemons:
    """Kill/start vstart OSDs and mons; remembers which ones we stopped."""

    def __init__(self):
        self.stopped = set()      # ("osd", 3) / ("mon", "a")
        self.starting = {}        # key -> (Popen, time)
        self.killed_pid = {}      # key -> pid we signalled

    def pid(self, key):
        typ, i = key
        try:
            pid = int(open(f"{OUT}/{typ}.{i}.pid").read().strip())
            os.kill(pid, 0)
            return pid
        except (OSError, ValueError):
            return None

    def kill(self, key, sig=signal.SIGKILL):
        pid = self.pid(key)
        self.stopped.add(key)
        self.starting.pop(key, None)
        if pid:
            os.kill(pid, sig)
            self.killed_pid[key] = pid
            log(f"  kill -{sig.name} {key[0]}.{key[1]} (pid {pid})")

    def start(self, key):
        typ, i = key
        if key not in self.stopped:
            return
        old = self.killed_pid.pop(key, None) or self.pid(key)
        if old:
            # SIGTERM may still be shutting down (pid file can go before sockets)
            for _ in range(120):
                try:
                    os.kill(old, 0)
                except OSError:
                    break
                time.sleep(1)
        if typ == "mon":
            # A client reconnecting to a dead mon can self-connect on its port
            # (vstart ports are in the ephemeral range), leaving a TIME_WAIT
            # that blocks the bind for ~60s: retry until the mon starts.
            for attempt in range(12):
                r = subprocess.run(f"{BIN}/ceph-mon -i {i} -c {CONF}",
                                   shell=True, cwd=BUILD, env=daemon_env(),
                                   capture_output=True)
                if r.returncode == 0:
                    log(f"  started mon.{i}")
                    break
                log(f"  mon.{i} start rc={r.returncode}, retrying ({attempt})")
                time.sleep(10)
            else:
                log(f"!!! mon.{i} did not start")
            self.stopped.discard(key)
            return
        p = subprocess.Popen(f"{BIN}/ceph-{typ} -i {i} -c {CONF}",
                             shell=True, cwd=BUILD, env=daemon_env(),
                             stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
        self.stopped.discard(key)
        self.starting[key] = (p, time.time())
        log(f"  starting {typ}.{i}")

    def initializing(self):
        return [k for k, (p, t) in self.starting.items() if p.poll() is None]

    def is_dead(self, key):
        if key in self.stopped:
            return False
        if key in self.starting:
            p, t = self.starting[key]
            if p.poll() is None and time.time() - t < 300:
                return False
            del self.starting[key]
            if p.returncode:
                log(f"!!! ceph-{key[0]} -i {key[1]} exited rc={p.returncode}")
                return True
        return self.pid(key) is None


class Cluster:
    def __init__(self, pool):
        self.pool = pool
        pools = ceph_json("osd pool ls detail")
        p = next(p for p in pools if p["pool_name"] == pool)
        self.pool_id = p["pool_id"]
        self.size = p["size"]
        self.min_size = p["min_size"]
        # an error where the monitors have no num_zones, as on main
        nz = ceph_json(f"osd pool get {pool} num_zones", quiet=True)
        self.num_zones = nz["num_zones"] if nz else 1
        mon_dump = ceph_json("mon dump")
        # monitors with num_zones also report stretch_mode for its pools
        self.global_stretch = mon_dump.get("stretch_mode", False) and \
            self.num_zones == 1 and p.get("peering_crush_bucket_count", 0) > 0
        if self.global_stretch:
            self.num_zones = ceph_json("osd dump")["stretch_mode"]["stretch_bucket_count"]
        self.multi_zone = self.num_zones > 1
        self.zone_size = self.size // self.num_zones
        self.erasure = bool(p.get("erasure_code_profile"))
        flags = p.get("flags_names", "").split(",")
        self.ec_optimized = "ec_optimizations" in flags
        self.omap = not self.erasure or "supports_omap" in flags
        if self.erasure:
            prof = ceph_json(f"osd erasure-code-profile get {p['erasure_code_profile']}")
            self.k, self.m = int(prof["k"]), int(prof["m"])
            self.budget = self.m
        elif self.multi_zone:
            self.budget = self.zone_size - 1
        else:
            self.budget = self.size - self.min_size
        self.all_mons = sorted(m["name"] for m in mon_dump["mons"])
        self.zone_mon = {}
        if self.multi_zone:
            self.zones = datacenter_osds()
            if len(self.zones) != self.num_zones:
                sys.exit(f"pool {pool} spans {self.num_zones} zones but the "
                         f"CRUSH map has datacenters {self.zones}")
            for m in mon_dump["mons"]:
                loc = {e["key"]: e["val"] for e in m.get("crush_location", [])}
                if loc.get("datacenter") in self.zones:
                    self.zone_mon[loc["datacenter"]] = m["name"]
        else:
            self.zones = {"all": sorted(int(o) for o in ceph_json("osd ls"))}
        self.osd_zone = {o: z for z, osds in self.zones.items() for o in osds}
        self.all_osds = sorted(self.osd_zone)
        kind = f"erasure k={self.k} m={self.m}" if self.erasure else \
            f"replicated size={self.size} min_size={self.min_size}"
        stretch = " (global stretch mode)" if self.global_stretch else ""
        log(f"pool {pool} id={self.pool_id} {kind} num_zones={self.num_zones}"
            f"{stretch} budget/zone={self.budget} zones={self.zones} "
            f"zone_mons={self.zone_mon} mons={self.all_mons}")

    def pgs(self):
        d = ceph_json(f"pg ls-by-pool {self.pool}", quiet=True)
        return d["pg_stats"] if d else []

    def all_pgs(self):
        d = ceph_json("pg ls", quiet=True)
        return d["pg_stats"] if d else []

    def pg_num_settled(self):
        pools = ceph_json("osd pool ls detail", quiet=True) or []
        return bool(pools) and all(
            p["pg_num"] == p["pg_num_target"] and
            p["pg_placement_num"] == p["pg_placement_num_target"] for p in pools)

    def stretch(self):
        if not self.multi_zone:
            return {}
        d = ceph_json("osd dump", quiet=True)
        return d.get("stretch_mode", {}) if d else None

    def quorum(self):
        d = ceph_json("quorum_status", quiet=True)
        return d.get("quorum_names", []) if d else []


class Workloads:
    def __init__(self, cluster, args, rundir):
        self.cluster = cluster
        self.args = args
        self.rundir = rundir
        self.procs = {}
        self.n = 0
        self.failed = []

    def specs(self):
        pool = self.cluster.pool
        pols = self.args.read_policies.split(",")
        here = os.path.dirname(os.path.abspath(__file__))
        rados_ops = ("--op read 100 --op write 50 --op append 50 --op delete 20 "
                     "--op snap_create 20 --op snap_remove 20 --op rollback 20 "
                     "--op copy_from 20 --op setattr 25 --op rmattr 25")
        no_omap = "" if self.cluster.omap else " --no-omap"
        common = (f"{BIN}/ceph_test_rados --pool {pool} --max-ops 4000 --objects 50 "
                  f"--max-in-flight 16 --size 400000 --min-stride-size 1000 "
                  f"--max-stride-size 80000{no_omap}")
        # without this the exerciser turns allow_ec_optimizations on
        ioseq_opts = " --disable_pool_ec_optimizations" \
            if self.cluster.erasure and not self.cluster.ec_optimized else ""
        specs = {}
        if "rados" in self.args.workloads:
            if "none" in pols:
                specs["rados-plain"] = f"{common} {rados_ops}"
            if "localize" in pols:
                if self.cluster.multi_zone:
                    for z in sorted(self.cluster.zones):
                        specs[f"rados-local-{z}"] = (
                            f'CEPH_ARGS="$CEPH_ARGS --crush_location=datacenter={z}" '
                            f"{common} --localize-reads {rados_ops}")
                else:
                    specs["rados-local"] = f"{common} --localize-reads {rados_ops}"
            if "balance" in pols:
                specs["rados-balanced"] = f"{common} --balance-reads {rados_ops}"
        if "ioseq" in self.args.workloads:
            pct = 100 if "balance" in pols else 0
            specs["ioseq"] = (
                f"{BIN}/ceph_test_rados_io_sequence --pool {pool} --parallel 4 "
                f"--allow_pool_scrubbing --allow_pool_deep_scrubbing "
                f"--balanced_read_percentage {pct} --seed {{seed}}{ioseq_opts}")
        if "ioseqrec" in self.args.workloads:
            specs["ioseqrec"] = (
                f"{BIN}/ceph_test_rados_io_sequence --pool {pool} --parallel 1 "
                f"--testrecovery --object ioseqrec --object_copy ioseqrec_copy "
                f"--allow_pool_scrubbing --allow_pool_deep_scrubbing "
                f"--balanced_read_percentage 0 --seed {{seed}}{ioseq_opts}")
        if "rbd" in self.args.workloads:
            specs["rbd"] = f"python3 {here}/rbd_roundtrip.py {{seed}} {pool}"
        if "probe" in self.args.workloads:
            zones = sorted(self.cluster.zones) if self.cluster.multi_zone else [None]
            for pol in pols:
                for z in zones:
                    name = f"probe-{pol}-{z}" if z else f"probe-{pol}"
                    zone_arg = f"--zone {z} " if z else ""
                    specs[name] = (f"python3 {here}/io_probe.py --pool {pool} "
                                   f"--policy {pol} {zone_arg}--duration 1800 "
                                   f"--out {self.rundir}/{name}.jsonl --name {name}")
        return specs

    def stuck_probes(self, limit):
        """Probes whose current op has been outstanding longer than limit."""
        bad = []
        for name in self.procs:
            if not name.startswith("probe-"):
                continue
            f = f"{self.rundir}/{name}.jsonl"
            try:
                with open(f, "rb") as fh:
                    fh.seek(max(0, os.path.getsize(f) - 4096))
                    lines = fh.read().decode(errors="replace").splitlines()
            except OSError:
                continue
            for line in reversed(lines):
                if '"inflight"' in line:
                    try:
                        r = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if r["inflight"] > limit:
                        bad.append((name, r["inflight"], r["op"]))
                    break
        return bad

    def launch(self, name, cmd):
        self.n += 1
        cmd = cmd.format(seed=random.randint(1, 1 << 30))
        logf = f"{self.rundir}/{name}.{self.n}.log"
        f = open(logf, "w")
        f.write(f"# {cmd}\n")
        f.flush()
        p = subprocess.Popen(cmd, shell=True, stdout=f, stderr=subprocess.STDOUT,
                             cwd=BUILD, start_new_session=True)
        self.procs[name] = (p, logf, cmd)
        log(f"  workload {name} started (pid {p.pid}) -> {logf}")

    def start_all(self):
        for name, cmd in self.specs().items():
            self.launch(name, cmd)

    def poll(self, restart=True):
        for name, (p, logf, cmd) in list(self.procs.items()):
            rc = p.poll()
            if rc is None:
                continue
            del self.procs[name]
            if rc != 0:
                log(f"!!! workload {name} FAILED rc={rc}, see {logf}")
                self.failed.append((name, rc, logf))
            else:
                log(f"  workload {name} completed OK")
                if name.startswith("rados-"):
                    self.remove_rados_objects(p.pid)
                if restart:
                    self.launch(name, self.specs()[name])
        return not self.failed

    def remove_rados_objects(self, pid):
        """Clean up after a finished ceph_test_rados (objects <hostname><pid>-<n>
        and the snaps holding their clones) in the background, so a long run
        does not fill the small test devices."""
        here = os.path.dirname(os.path.abspath(__file__))
        prefix = f"{socket.gethostname()}{pid}-"
        subprocess.Popen(
            ["timeout", "1800", "python3", f"{here}/rados_cleanup.py",
             self.cluster.pool, prefix],
            cwd=BUILD, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True)

    def stop_all(self):
        for name, (p, logf, cmd) in self.procs.items():
            try:
                os.killpg(p.pid, signal.SIGTERM)
            except OSError:
                pass
        self.procs = {}

    def wait_idle(self, timeout, ok):
        deadline = time.time() + timeout
        while self.procs and not self.failed and ok() and time.time() < deadline:
            self.poll(restart=False)
            time.sleep(5)


class LogWatcher:
    PATTERNS = ("FAILED ceph_assert", "Caught signal", "ceph_abort",
                "terminate called", "heap-use-after-free", "AddressSanitizer")

    def __init__(self):
        self.offsets = {f: os.path.getsize(f) for f in self.files()}

    def files(self):
        return [f"{OUT}/{f}" for f in os.listdir(OUT)
                if f.split(".")[0] in ("osd", "mon", "mgr") and f.endswith(".log")]

    def scan(self):
        hits = []
        for f in self.files():
            off = self.offsets.get(f, 0)
            size = os.path.getsize(f)
            if size < off:
                off = 0
            if size == off:
                continue
            with open(f, "rb") as fh:
                fh.seek(off)
                data = fh.read(size - off).decode(errors="replace")
            self.offsets[f] = size
            for line in data.splitlines():
                if any(p in line for p in self.PATTERNS):
                    hits.append((f, line.strip()[:300]))
        return hits


class ZoneFailover:
    """One zone failover. Each variant is a generator that yields once per
    chaos cycle, so the other actions and the workloads go on meanwhile."""

    VARIANTS = ("standard", "osds_first", "flap", "surviving_loss",
                "osds_only", "mon_only")

    def __init__(self, chaos, variant, zone):
        self.chaos = chaos
        self.cluster = chaos.cluster
        self.variant = variant
        self.zone = zone
        self.other = next(z for z in self.cluster.zones if z != zone)
        self.tag = f"zone_failover[{variant}:{zone}]"
        self.state = "start"
        self.t = time.time()
        self.hold_cycles = random.randint(*chaos.args.hold_cycles)
        self.sig = random.choice([signal.SIGKILL, signal.SIGTERM])
        self.steps = getattr(self, variant)()

    def step(self):
        try:
            next(self.steps)
        except StopIteration:
            self.state = "done"

    def done(self):
        return self.state == "done"

    # ---- the variants -----------------------------------------------------

    def standard(self, mon_first=True):
        self.kill_zone(self.zone)
        yield from self.wait_degraded()
        yield from self.hold()
        self.revive_zone(self.zone, mon_first)
        yield from self.recover()

    def osds_first(self):
        yield from self.standard(mon_first=False)

    def osds_only(self):
        self.kill_zone(self.zone, mon=False)
        yield from self.wait_degraded()
        yield from self.hold()
        self.revive_zone(self.zone)
        yield from self.recover()

    def surviving_loss(self):
        self.kill_zone(self.zone)
        yield from self.wait_degraded()
        others = self.cluster.zones[self.other]
        victims = random.sample(others, min(self.cluster.budget, len(others)))
        self.chaos.record(f"{self.tag} also killing surviving-zone OSDs {victims}")
        for o in victims:
            self.chaos.daemons.kill(("osd", o))
        yield from self.hold()
        self.chaos.record(f"{self.tag} reviving surviving-zone OSDs {victims}")
        for o in victims:
            self.chaos.daemons.start(("osd", o))
        self.revive_zone(self.zone)
        yield from self.recover()

    def flap(self):
        self.kill_zone(self.zone)
        yield from self.wait_degraded()
        yield from self.hold()
        self.revive_zone(self.zone)
        yield from self.recover(flap=True)

    def mon_only(self):
        self.kill_zone(self.zone, osds=False)
        yield from self.hold()
        self.revive_zone(self.zone, osds=False)
        yield from self.recover()

    # ---- the steps they share ---------------------------------------------

    def kill_zone(self, zone, osds=True, mon=True):
        if zone == self.zone:
            self.chaos.record(f"{self.tag} kill sig={self.sig.name} "
                              f"hold={self.hold_cycles} cycles")
        d = self.chaos.daemons
        if osds:
            for o in self.cluster.zones[zone]:
                d.kill(("osd", o), self.sig)
                self.chaos.pending_restart.pop(("osd", o), None)
        if mon and self.cluster.zone_mon.get(zone):
            d.kill(("mon", self.cluster.zone_mon[zone]), self.sig)

    def revive_zone(self, zone, mon_first=True, osds=True):
        if zone == self.zone:
            self.chaos.record(f"{self.tag} revive "
                              f"({'mon first' if mon_first else 'OSDs first'})")
        d = self.chaos.daemons
        mon = ("mon", self.cluster.zone_mon[zone]) if self.cluster.zone_mon.get(zone) else None
        if mon_first and mon:
            d.start(mon)
            if osds:
                self.chaos.wait_quorum(120)
        if osds:
            for o in self.cluster.zones[zone]:
                d.start(("osd", o))
        if not mon_first and mon:
            d.start(mon)

    def stretch_state(self):
        st = self.cluster.stretch() or {}
        return (st.get("degraded_stretch_mode", 0),
                st.get("recovering_stretch_mode", 0), time.time() - self.t)

    def wait_degraded(self):
        self.state, self.t = "wait_degraded", time.time()
        while True:
            yield
            degraded, _, age = self.stretch_state()
            if degraded:
                self.chaos.record(f"{self.tag} degraded stretch mode after {age:.0f}s")
                return
            if age > self.chaos.args.degrade_timeout:
                if self.variant == "osds_only":
                    self.chaos.record(f"{self.tag} no degraded mode with zone mon up")
                else:
                    self.chaos.finding(f"{self.tag} cluster did not enter degraded "
                                       f"stretch mode within {age:.0f}s")
                return

    def hold(self):
        self.state, self.t = "held", time.time()
        for _ in range(self.hold_cycles):
            yield

    def recover(self, flap=False):
        """Wait for healthy stretch mode and clean PGs. With flap, once the
        zone is recovering, also fail the other zone for 1 to 4 cycles."""
        chaos = self.chaos
        self.state, self.t = "recovering", time.time()
        nudged = False
        while True:
            yield
            degraded, recovering, age = self.stretch_state()
            # with both zones down there is no quorum, and an OSD still in
            # init gives up when it cannot get its rotating keys
            if flap and recovering and not chaos.daemons.initializing():
                flap = False
                chaos.record(f"{self.tag} FLAP: zone {self.zone} recovering, "
                             f"killing zone {self.other}")
                self.kill_zone(self.other)
                for _ in range(random.randint(1, 4)):
                    yield
                chaos.record(f"{self.tag} FLAP: reviving zone {self.other}")
                self.revive_zone(self.other)
                self.t = time.time()
                continue
            if (degraded and not recovering and not nudged and
                    age > chaos.args.nudge_after and
                    len(self.cluster.quorum()) == len(self.cluster.all_mons)):
                chaos.finding(f"{self.tag} stuck in degraded (not recovering) "
                              f"stretch mode {age:.0f}s after revive with full "
                              f"quorum; nudging with force_recovery_stretch_mode")
                ceph("osd force_recovery_stretch_mode --yes-i-really-mean-it")
                nudged = True
                continue
            pgs = self.cluster.pgs()
            clean = pgs and all(p["state"].startswith("active+clean") for p in pgs)
            if not degraded and not recovering and clean:
                chaos.record(f"{self.tag} healthy again after {age:.0f}s")
                return
            if age > chaos.args.revive_timeout:
                bad = [(p["pgid"], p["state"]) for p in pgs
                       if not p["state"].startswith("active+clean")][:10]
                chaos.fatal(f"{self.tag} not healthy {age:.0f}s after revive: "
                            f"degraded={degraded} recovering={recovering} pgs={bad}")


class Chaos:
    def __init__(self, args):
        self.args = args
        self.cluster = Cluster(args.pool)
        self.daemons = Daemons()
        self.cycle = 0
        self.rundir = args.rundir
        os.makedirs(self.rundir, exist_ok=True)
        self.workloads = Workloads(self.cluster, args, self.rundir)
        self.watch = LogWatcher()
        self.timeline = open(f"{self.rundir}/timeline.log", "a")
        self.failover = None
        self.failed = False
        self.pending_restart = {}
        self.partial_until = -1
        self.dup_acting_seen = set()
        self.stopped_by = None

    def on_sigterm(self, signum, frame):
        # a second SIGTERM stops at once, through the finally blocks
        if self.stopped_by == "SIGTERM":
            sys.exit(128 + signum)
        self.stopped_by = "SIGTERM"

    def record(self, what):
        line = f"{time.strftime('%H:%M:%S')} cycle={self.cycle} {what}"
        self.timeline.write(line + "\n")
        self.timeline.flush()
        log(f"  >> {what}")

    @staticmethod
    def dump(d, commands, pgs=()):
        """Save the output of each (file, ceph command), and the query and
        unfound objects of each PG in pgs, to directory d."""
        os.makedirs(d, exist_ok=True)
        for p in pgs:
            # a PG without a primary leaves the query waiting for one
            if p.get("acting_primary", -1) >= 0:
                commands += ((f"{p['pgid']}.query.json", f"pg {p['pgid']} query"),
                             (f"{p['pgid']}.unfound.json", f"pg {p['pgid']} list_unfound"))
        for name, cmd in commands:
            out = ceph(cmd, timeout=60, quiet=True)
            with open(f"{d}/{name}", "w") as f:
                f.write(out or "(no output)\n")
        return d

    def diag(self, label, pgs=()):
        return self.dump(f"{self.rundir}/diag-c{self.cycle}-{label}", (
            ("status.txt", "-s"), ("health.txt", "health detail"),
            ("osd_dump.txt", "osd dump"),
            ("pgs.txt", f"pg ls-by-pool {self.cluster.pool}"),
            ("mon_dump.txt", "mon dump")), pgs)

    def save_evidence(self):
        """After a failure, with the monitors up again."""
        unclean = [p for p in self.cluster.all_pgs() if not self.quiesced(p)]
        self.dump(f"{self.rundir}/fail-evidence", (
            ("health_detail.txt", "health detail"),
            ("osd_dump.json", "osd dump -f json"),
            ("pg_dump.json", "pg dump pgs -f json")), unclean)

    def finding(self, what, pgs=()):
        d = self.diag("finding", pgs)
        self.record(f"FINDING {what} (diag {d})")
        with open(f"{self.rundir}/findings.log", "a") as f:
            f.write(f"{time.strftime('%H:%M:%S')} cycle={self.cycle} {what}\n"
                    f"  diagnostics: {d}\n")

    def fatal(self, what):
        d = self.diag("fatal")
        self.record(f"FATAL {what} (diag {d})")
        self.failed = True

    # ---- budget helpers ---------------------------------------------------

    def unavailable(self):
        d = ceph_json("osd dump", quiet=True)
        down = {o["osd"] for o in d["osds"] if not o["up"]} if d else set()
        out = {o["osd"] for o in d["osds"] if not o["in"]} if d else set()
        down |= {i for (t, i) in self.daemons.stopped if t == "osd"}
        return down, out

    def zone_budget_ok(self, osd, down, out):
        """True if making osd unavailable keeps its zone within m failures."""
        z = self.cluster.osd_zone[osd]
        if self.failover and z == self.failover.zone:
            return False
        bad = {o for o in down | out if self.cluster.osd_zone.get(o) == z}
        return osd not in bad and len(bad) < self.cluster.budget + self.args.extra_per_zone

    def candidates(self):
        down, out = self.unavailable()
        return [o for o in self.cluster.all_osds if self.zone_budget_ok(o, down, out)]

    # ---- single-OSD / PG actions -----------------------------------------

    def a_mark_down(self):
        cands = self.candidates()
        if cands:
            o = random.choice(cands)
            self.record(f"mark_down osd.{o}")
            ceph(f"osd down {o}")

    def a_out_in(self):
        _, out = self.unavailable()
        if out and random.random() < 0.5:
            o = random.choice(sorted(out))
            self.record(f"in osd.{o}")
            ceph(f"osd in {o}")
            return
        cands = self.candidates()
        if cands:
            o = random.choice(cands)
            self.record(f"out osd.{o}")
            ceph(f"osd out {o}")

    def a_kill_restart(self):
        cands = self.candidates()
        if cands:
            o = random.choice(cands)
            when = self.cycle + random.randint(0, 4)
            self.record(f"kill_restart osd.{o} restart@{when}")
            self.daemons.kill(("osd", o))
            self.pending_restart[("osd", o)] = when

    def a_zone_partial(self):
        if self.failover:
            return
        down, out = self.unavailable()
        if down:
            return
        z = random.choice(sorted(self.cluster.zones))
        osds = self.cluster.zones[z]
        victims = random.sample(osds, len(osds) - self.cluster.k + 1)
        when = self.cycle + random.randint(1, 6)
        # the zone's PGs stay peered until the victims return, and the hold
        # is in cycles, so slow cycles can outlast --stuck-limit
        self.partial_until = when + 2
        self.record(f"zone_partial {z} {victims} restart@{when}")
        for o in victims:
            self.daemons.kill(("osd", o))
            self.pending_restart[("osd", o)] = when

    def a_zone_failover(self):
        if self.failover:
            return
        down, out = self.unavailable()
        if down:
            return
        variants = self.args.zf_variants.split(",")
        self.failover = ZoneFailover(self, random.choice(variants),
                               random.choice(sorted(self.cluster.zones)))
        self.failover.step()

    def _random_pg(self):
        pgs = self.cluster.pgs()
        return random.choice(pgs) if pgs else None

    def _zone_order(self, up):
        zs = self.cluster.zone_size
        return [self.cluster.osd_zone.get(up[i * zs]) if i * zs < len(up) else None
                for i in range(self.cluster.num_zones)]

    def a_upmap_zone(self, flip=False):
        pg = self._random_pg()
        if not pg:
            return
        order = self._zone_order(pg["up"])
        if None in order or len(set(order)) != len(order):
            return
        if flip:
            order = list(reversed(order))
        new = []
        for z in order:
            new += random.sample(self.cluster.zones[z], self.cluster.zone_size)
        self.record(f"upmap{'_flip' if flip else '_zone'} {pg['pgid']} "
                    f"{pg['up']} -> {new}")
        ceph(f"osd pg-upmap {pg['pgid']} {' '.join(map(str, new))}")

    def a_upmap_flip(self):
        self.a_upmap_zone(flip=True)

    def a_upmap_items(self):
        pg = self._random_pg()
        if not pg:
            return
        up = pg["up"]
        # a PG can have no up OSDs at all while a zone is down
        srcs = [o for o in up if o in self.cluster.osd_zone]
        if not srcs:
            return
        src = random.choice(srcs)
        z = self.cluster.osd_zone.get(src)
        cands = [o for o in self.cluster.zones.get(z, []) if o not in up]
        if cands:
            dst = random.choice(cands)
            self.record(f"upmap_items {pg['pgid']} {src}->{dst}")
            ceph(f"osd pg-upmap-items {pg['pgid']} {src} {dst}")

    def a_rm_upmap(self):
        d = ceph_json("osd dump", quiet=True) or {}
        ups = [u["pgid"] for u in d.get("pg_upmap", [])]
        items = [u["pgid"] for u in d.get("pg_upmap_items", [])]
        if ups and random.random() < 0.5:
            pg = random.choice(ups)
            self.record(f"rm_upmap {pg}")
            ceph(f"osd rm-pg-upmap {pg}")
        elif items:
            pg = random.choice(items)
            self.record(f"rm_upmap_items {pg}")
            ceph(f"osd rm-pg-upmap-items {pg}")

    def a_repeer(self):
        pg = self._random_pg()
        if pg:
            self.record(f"repeer {pg['pgid']}")
            ceph(f"pg repeer {pg['pgid']}")

    def a_deep_scrub(self):
        pg = self._random_pg()
        if pg:
            self.record(f"deep_scrub {pg['pgid']}")
            ceph(f"pg deep-scrub {pg['pgid']}")

    def a_reweight(self):
        _, out = self.unavailable()
        cands = [o for o in self.cluster.all_osds if o not in out]
        if cands:
            o = random.choice(cands)
            w = random.choice([0.5, 0.75, 1.0])
            self.record(f"reweight osd.{o} {w}")
            ceph(f"osd reweight {o} {w}")

    def a_primary_affinity(self):
        o = random.choice(self.cluster.all_osds)
        a = random.choice([0, 0.5, 1])
        self.record(f"primary_affinity osd.{o} {a}")
        ceph(f"osd primary-affinity osd.{o} {a}")

    def a_pg_num(self):
        pools = ceph_json("osd pool ls detail", quiet=True) or []
        p = next((x for x in pools if x["pool_name"] == self.cluster.pool), None)
        # one split or merge at a time
        if not p or p["pg_num"] != p["pg_num_target"] or \
                p["pg_placement_num"] != p["pg_placement_num_target"]:
            return
        n = p["pg_num"]
        new = random.choice([v for v in (n * 2, n // 2)
                             if self.args.pg_min <= v <= self.args.pg_max] or [n])
        if new != n:
            self.record(f"pg_num {self.cluster.pool} {n} -> {new}")
            ceph(f"osd pool set {self.cluster.pool} pg_num {new}")

    # ---- checks -----------------------------------------------------------

    def check(self):
        hits = self.watch.scan()
        ignored = {f for f, line in hits
                   if any(pat in line for pat in self.args.ignore_crash)}
        for f, line in hits:
            if f in ignored:
                log(f"  (ignored known crash in {os.path.basename(f)}: {line[:120]})")
                continue
            log(f"!!! crash signature in {f}: {line}")
            self.fatal(f"CRASH {os.path.basename(f)}: {line}")
        for f in ignored:
            osd = os.path.basename(f).split(".")[1]
            if os.path.basename(f).startswith("osd.") and \
                    ("osd", int(osd)) not in self.daemons.stopped:
                self.daemons.stopped.add(("osd", int(osd)))
                self.pending_restart[("osd", int(osd))] = self.cycle + 1
        keys = [("osd", o) for o in self.cluster.all_osds] + \
               [("mon", m) for m in self.cluster.all_mons]
        for key in keys:
            if self.daemons.is_dead(key):
                self.fatal(f"DEAD {key[0]}.{key[1]} (not killed by us)")
        if not self.workloads.poll():
            for name, rc, logf in self.workloads.failed:
                self.fatal(f"WORKLOAD_FAIL {name} rc={rc} {logf}")
            self.workloads.failed = []
        relaxed = self.failover or self.cycle <= self.partial_until
        limit = self.args.stuck_limit_zf if relaxed else self.args.stuck_limit
        for name, age, op in self.workloads.stuck_probes(limit):
            self.fatal(f"STUCK_OP {name} {op} outstanding {age:.0f}s")
        self.disk_ok()
        pgs = ceph_json(f"pg ls-by-pool {self.cluster.pool}", quiet=True)
        for p in (pgs or {}).get("pg_stats", []):
            # expected for EC while the cluster is changing; quiesce()
            # checks again once the PGs are clean
            if dup_acting(p) and p["pgid"] not in self.dup_acting_seen:
                self.dup_acting_seen.add(p["pgid"])
                log(f"  dup acting {p['pgid']} up {p['up']} acting {p['acting']} {p['state']}")
        df = ceph_json("osd df", quiet=True)
        if df:
            full = [(n["id"], n["utilization"]) for n in df.get("nodes", [])
                    if n.get("utilization", 0) > self.args.osd_full_pct]
            if full:
                self.fatal(f"ENV_OSD_FULL (test devices too small, not a Ceph bug): {full}")
        return not self.failed

    def disk_ok(self):
        # dev/ and out/ can be links to another file system than BUILD's
        free, where = min((free_gb(d), d) for d in (f"{BUILD}/dev", OUT))
        if free >= self.args.min_free_gb:
            return True
        log(f"!!! low disk space: {free:.1f}G free in {where}")
        self.fatal(f"ENV_LOW_DISK {free:.1f}G free in {where} "
                   f"(limit {self.args.min_free_gb}G)")
        return False

    def restart_due(self):
        for key, when in list(self.pending_restart.items()):
            if when <= self.cycle:
                self.daemons.start(key)
                del self.pending_restart[key]

    def wait_quorum(self, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if len(self.cluster.quorum()) == len(self.cluster.all_mons):
                return True
            time.sleep(3)
        self.finding(f"quorum not restored within {timeout}s "
                     f"(quorum={self.cluster.quorum()})")
        return False

    def revive_everything(self):
        for key in sorted(self.daemons.stopped, key=lambda k: k[0] != "mon"):
            self.daemons.start(key)
            if key[0] == "mon":
                self.wait_quorum(120)
        self.pending_restart = {}
        _, out = self.unavailable()
        for o in out:
            ceph(f"osd in {o}")

    @staticmethod
    def quiesced(p):
        s = p["state"].split("+")
        return s[:2] == ["active", "clean"] and "remapped" not in s

    def wait_healthy(self, timeout):
        deadline = time.time() + timeout
        nudged = False
        start = time.time()
        while time.time() < deadline:
            st = self.cluster.stretch() or {}
            pgs = self.cluster.all_pgs()
            deg = st.get("degraded_stretch_mode", 0)
            rec = st.get("recovering_stretch_mode", 0)
            # merges or splits still in progress change PG intervals, which
            # drops the deep scrubs requested next
            if not deg and not rec and pgs and \
               all(self.quiesced(p) for p in pgs) and \
               self.cluster.pg_num_settled():
                return True
            if deg and not rec and not nudged and \
               time.time() - start > self.args.nudge_after:
                self.finding("quiesce: still degraded (not recovering) stretch "
                             "mode with everything up; nudging")
                ceph("osd force_recovery_stretch_mode --yes-i-really-mean-it")
                nudged = True
            if not self.check():
                return False
            time.sleep(10)
        bad = [(p["pgid"], p["state"]) for p in self.cluster.all_pgs()
               if not self.quiesced(p)][:10]
        st = f" stretch={self.cluster.stretch()}" if self.cluster.multi_zone else ""
        self.fatal(f"NOT_HEALTHY after {timeout}s{st} pgs={bad}")
        return False

    def quiesce(self):
        log("=== quiesce ===")
        self.record("quiesce")
        self.failover = None
        self.revive_everything()
        for o in self.cluster.all_osds:
            ceph(f"osd reweight {o} 1", quiet=True)
            ceph(f"osd primary-affinity osd.{o} 1", quiet=True)
        if not self.wait_healthy(self.args.clean_timeout):
            return False
        ceph("osd unset noscrub", quiet=True)
        ceph("osd unset nodeep-scrub", quiet=True)
        stamps = {p["pgid"]: p.get("last_deep_scrub_stamp") for p in self.cluster.all_pgs()}
        for pgid in stamps:
            ceph(f"pg deep-scrub {pgid}", quiet=True)
        deadline = time.time() + self.args.clean_timeout
        late = list(stamps)
        while time.time() < deadline:
            pgs = self.cluster.all_pgs()
            late = [p["pgid"] for p in pgs
                    if p.get("last_deep_scrub_stamp") == stamps.get(p["pgid"])]
            if pgs and not late:
                break
            if not self.check():
                return False
            time.sleep(10)
        else:
            self.fatal(f"DEEP_SCRUB_TIMEOUT after {self.args.clean_timeout}s "
                       f"pgs={late[:10]}")
            return False
        incons = (ceph_json("pg ls inconsistent", quiet=True) or {}).get("pg_stats", [])
        if incons:
            self.fatal("INCONSISTENT after deep scrub: " +
                       ", ".join(f"{p['pgid']} {p['state']}" for p in incons))
            return False
        dups = [p for p in self.cluster.pgs() if self.quiesced(p) and dup_acting(p)]
        if dups:
            self.finding("DUP_ACTING in clean PGs: " + ", ".join(
                f"{p['pgid']} acting {p['acting']}" for p in dups), dups)
        return self.check()

    def finish(self):
        # client ops to a site without its monitor never finish
        self.failover = None
        self.revive_everything()
        self.workloads.wait_idle(self.args.clean_timeout, self.disk_ok)
        if self.workloads.procs and not self.workloads.failed and not self.failed:
            self.fatal(f"WORKLOAD_STUCK {sorted(self.workloads.procs)} still running "
                       f"{self.args.clean_timeout}s after everything was revived")
        self.workloads.stop_all()
        return not self.failed and self.check() and self.quiesce()

    def run(self):
        actions = {}
        for a in self.args.actions.split(","):
            name, weight = a.split("=")
            if name not in ACTIONS:
                sys.exit(f"unknown action {name}; the actions are {', '.join(ACTIONS)}")
            actions[name] = float(weight)
        skipped = [n for n in actions if not ACTIONS[n][1](self.cluster)]
        actions = {n: w for n, w in actions.items() if n not in skipped}
        log(f"actions: {sorted(actions)}"
            + (f" (not applicable to this pool: {sorted(skipped)})" if skipped else ""))
        deadline = time.time() + self.args.duration if self.args.duration else None
        self.workloads.start_all()
        try:
            while not self.stopped_by and \
                    (self.args.cycles == 0 or self.cycle < self.args.cycles):
                if deadline and time.time() >= deadline:
                    self.stopped_by = "time limit"
                    break
                self.cycle += 1
                zf = f"(zone failover {self.failover.variant}:{self.failover.state})" \
                    if self.failover else ""
                log(f"--- cycle {self.cycle} {zf} ---")
                self.restart_due()
                if self.failover:
                    self.failover.step()
                    if self.failover.done():
                        self.failover = None
                if self.failed:
                    return 1
                names = list(actions)
                if self.failover and self.failover.state == "recovering":
                    names = []
                for _ in range(random.randint(1, self.args.max_actions) if names else 0):
                    a = random.choices(names, [actions[n] for n in names])[0]
                    getattr(self, f"a_{a}")()
                time.sleep(self.args.delay)
                if not self.check():
                    return 1
                if self.args.quiesce_every and not self.failover and \
                   self.cycle % self.args.quiesce_every == 0:
                    if not self.quiesce():
                        return 1
            if self.stopped_by:
                log(f"stopping on the {self.stopped_by}; reviving the cluster")
            log("=== final quiesce ===")
            if not self.finish():
                return 1
            return EXIT_STOPPED if self.stopped_by else 0
        finally:
            self.workloads.stop_all()
            if self.failed:
                log(f"stopped on failure; cluster left as-is. see {self.rundir}")
            for key in sorted(k for k in self.daemons.stopped if k[0] == "mon"):
                log(f"reviving mon.{key[1]}, killed by this run: a site without "
                    f"its monitor floods the OSD logs")
                self.daemons.start(key)
            if self.failed:
                log(f"saving PG evidence in {self.rundir}/fail-evidence")
                self.save_evidence()


def any_pool(cluster):
    return True


def multi_zone(cluster):
    return cluster.multi_zone


def spare_osd(cluster):
    """Without an OSD to spare in a zone, an OSD out or reweighted leaves PGs
    that CRUSH cannot place, and a zone failover then never recovers."""
    return all(len(osds) > cluster.zone_size for osds in cluster.zones.values())


def multi_zone_ec(cluster):
    return cluster.multi_zone and cluster.erasure


def stretch_mode(cluster):
    return cluster.multi_zone and \
        bool((cluster.stretch() or {}).get("stretch_mode_enabled"))


# Chaos.a_<name>: (default weight, whether it applies to the pool)
ACTIONS = {
    "mark_down": (3, any_pool),
    "out_in": (2, spare_osd),
    "kill_restart": (3, any_pool),
    "zone_partial": (1, multi_zone_ec),
    "zone_failover": (3, stretch_mode),
    "upmap_zone": (2, multi_zone),
    "upmap_flip": (1, multi_zone),
    "upmap_items": (2, any_pool),
    "rm_upmap": (2, any_pool),
    "repeer": (1, any_pool),
    "deep_scrub": (1, any_pool),
    "reweight": (2, spare_osd),
    "primary_affinity": (1, any_pool),
    "pg_num": (1, any_pool),
}
DEFAULT_ACTIONS = ",".join(f"{n}={w}" for n, (w, _) in ACTIONS.items())

# the exit status of a run that a time limit or SIGTERM ended without a failure
EXIT_STOPPED = 124


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--pool", default="chaos")
    ap.add_argument("--cycles", type=int, default=100, help="0 = forever")
    ap.add_argument("--duration", type=int, default=0,
                    help="stop after this many seconds, then revive and quiesce "
                         "as after the last cycle (0 = no limit); SIGTERM does the "
                         "same, a second SIGTERM stops at once")
    ap.add_argument("--delay", type=float, default=8)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--max-actions", type=int, default=2)
    ap.add_argument("--extra-per-zone", type=int, default=0,
                    help="allow this many failures beyond the budget per zone")
    ap.add_argument("--actions", default=DEFAULT_ACTIONS)
    ap.add_argument("--pg-min", type=int, default=8)
    ap.add_argument("--pg-max", type=int, default=64)
    ap.add_argument("--zf-variants", default=",".join(ZoneFailover.VARIANTS))
    ap.add_argument("--hold-cycles", type=int, nargs=2, default=[2, 8])
    ap.add_argument("--degrade-timeout", type=int, default=180)
    ap.add_argument("--nudge-after", type=int, default=180)
    ap.add_argument("--revive-timeout", type=int, default=1200)
    ap.add_argument("--workloads", default="rados,ioseq,rbd,probe")
    ap.add_argument("--read-policies", default="none",
                    help="comma list of none,balance,localize for rados/probe/ioseq clients")
    ap.add_argument("--stuck-limit", type=int, default=240,
                    help="fatal if a probe op is outstanding longer than this")
    ap.add_argument("--stuck-limit-zf", type=int, default=900,
                    help="stuck limit while a zone failover or zone_partial is in progress "
                         "(flap can legitimately block IO until the zone returns)")
    ap.add_argument("--quiesce-every", type=int, default=30)
    ap.add_argument("--clean-timeout", type=int, default=1200)
    ap.add_argument("--rundir", default=None)
    ap.add_argument("--osd-full-pct", type=float, default=85.0)
    ap.add_argument("--min-free-gb", type=int, default=10,
                    help="stop when the build directory's dev/ or out/ has less "
                         "free space")
    ap.add_argument("--ignore-crash", action="append",
                    default=["_shutdown_cache", "get_nref() == 1"],
                    help="substring of a crash line to treat as known; the defaults "
                         "match the BlueStore::umount() assert of a slow OSD "
                         "shutdown, https://tracker.ceph.com/issues/82196")
    args = ap.parse_args()
    if args.seed is None:
        args.seed = random.randint(1, 1 << 30)
    random.seed(args.seed)
    if args.rundir is None:
        args.rundir = (f"{os.environ['CHAOS_RUNS']}/chaos-"
                       f"{time.strftime('%m%d-%H%M%S')}-s{args.seed}")
    log(f"seed={args.seed} rundir={args.rundir}")
    chaos = Chaos(args)
    signal.signal(signal.SIGTERM, chaos.on_sigterm)
    sys.exit(chaos.run())


if __name__ == "__main__":
    main()
