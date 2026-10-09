#!/usr/bin/env python3
"""Does client IO keep flowing while a stretch zone is down, and does it all
complete afterwards?

Starts io_probe.py clients for every (policy, client zone) combination (x
--copies), then runs --iterations zone failovers (all OSDs of a zone + its
monitor), alternating zones: kill, hold, revive (mon first or OSDs first),
wait for healthy stretch mode, settle.  Per probe and per phase it reports
ops completed, the longest stall, and errors.  Needs a multi-zone pool in
stretch mode (see chaos.py).

Stops immediately (leaving the cluster as it is) if a probe reports a stuck op
(> --stuck seconds, with --debug the probe dumps objecter_requests), a
miscompare/error, or the cluster does not return to healthy.
"""

import argparse
import json
import os
import random
import signal
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from chaos import Cluster, Daemons, log  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))


class Run:
    def __init__(self, args):
        self.args = args
        self.c = Cluster(args.pool)
        if not self.c.multi_zone:
            sys.exit(f"pool {args.pool} has a single zone; nothing to fail over")
        if not (self.c.stretch() or {}).get("stretch_mode_enabled"):
            sys.exit("stretch mode is not enabled")
        self.d = Daemons()
        self.phases = []
        self.probes = {}
        self.offsets = {}
        self.problem = None
        self.down = None

    def mark(self, p):
        self.phases.append((p, time.time()))
        log(f"=== phase {p}")

    def start_probes(self):
        a = self.args
        for pol in a.policies.split(","):
            for cz in sorted(self.c.zones):
                for i in range(a.copies):
                    name = f"{pol}-{cz}-{i}"
                    out = f"{a.rundir}/probe-{name}.jsonl"
                    cmd = ["python3", f"{HERE}/io_probe.py", "--pool", a.pool,
                           "--policy", pol, "--zone", cz, "--out", out,
                           "--name", name, "--stuck", str(a.stuck)]
                    if a.debug:
                        cmd.append("--debug")
                    logf = open(f"{a.rundir}/probe-{name}.log", "w")
                    self.probes[name] = (subprocess.Popen(
                        cmd, stdout=logf, stderr=subprocess.STDOUT, cwd=HERE,
                        start_new_session=True), out)

    def scan_probes(self):
        """Return a problem string if any probe is stuck/failed."""
        for name, (p, out) in self.probes.items():
            if p.poll() is not None:
                return f"probe {name} exited rc={p.returncode}"
            try:
                with open(out) as f:
                    f.seek(self.offsets.get(name, 0))
                    data = f.read()
                    self.offsets[name] = f.tell()
            except OSError:
                continue
            for line in data.splitlines():
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if "MISCOMPARE" in r or "ERROR" in r:
                    return f"probe {name}: {r}"
                if r.get("inflight", 0) > self.args.stuck:
                    return f"probe {name} op stuck {r['inflight']}s: {r['op']}"
        return None

    def wait(self, secs, cond=None):
        end = time.time() + secs
        while time.time() < end:
            st = os.statvfs(os.environ["CEPH_BUILD"])
            if st.f_bavail * st.f_frsize < 5 << 30:
                self.problem = "DISK: $CEPH_BUILD below 5G free"
                return None
            prob = self.scan_probes()
            if prob:
                self.problem = prob
                return None
            if cond:
                r = cond()
                if r:
                    return r
            time.sleep(5)
        return None

    def failover(self, it, zone):
        a, c, d = self.args, self.c, self.d
        sig = getattr(signal, f"SIG{random.choice(a.signals.split(','))}")
        order = random.choice(a.revive_orders.split(","))
        self.mark(f"i{it}-kill-{zone}-{sig.name}-{order}")
        for o in c.zones[zone]:
            d.kill(("osd", o), sig)
        mon = ("mon", c.zone_mon[zone]) if c.zone_mon.get(zone) and not a.keep_mon else None
        if mon:
            d.kill(mon, sig)
        self.down = (zone, mon)
        seen = {}

        def degraded():
            st = c.stretch() or {}
            if st.get("degraded_stretch_mode") and "deg" not in seen:
                seen["deg"] = 1
                self.mark(f"i{it}-degraded")
            return None
        self.wait(a.hold, degraded)
        if self.problem:
            return False
        self.mark(f"i{it}-revive")
        if mon and order == "mon_first":
            d.start(mon)
            self.wait(120, lambda: len(c.quorum()) == len(c.all_mons))
        for o in c.zones[zone]:
            d.start(("osd", o))
        if mon and order == "osds_first":
            d.start(mon)
        self.down = None

        def healthy():
            st = c.stretch() or {}
            if st.get("recovering_stretch_mode") and "rec" not in seen:
                seen["rec"] = 1
                self.mark(f"i{it}-recovering")
            pgs = c.pgs()
            return (not st.get("degraded_stretch_mode") and
                    not st.get("recovering_stretch_mode") and pgs and
                    all(p["state"] == "active+clean" for p in pgs))
        if self.problem:
            return False
        ok = self.wait(a.healthy_timeout, healthy)
        if self.problem:
            return False
        if not ok:
            self.problem = f"iteration {it}: not healthy after {a.healthy_timeout}s"
            return False
        self.mark(f"i{it}-healthy")
        self.wait(a.settle)
        return not self.problem

    def run(self):
        a = self.args
        self.start_probes()
        self.mark("baseline")
        self.wait(a.baseline)
        zones = sorted(self.c.zones)
        if a.zone:
            zones = [a.zone]
        it = 0
        while not self.problem and (a.iterations == 0 or it < a.iterations):
            it += 1
            if not self.failover(it, zones[(it - 1) % len(zones)]):
                break
        self.mark("end")
        if self.problem:
            log(f"!!! PROBLEM: {self.problem}")
            subprocess.run(f"ceph -s > {a.rundir}/problem_status.txt 2>&1; "
                           f"ceph osd dump > {a.rundir}/problem_osd_dump.txt 2>&1; "
                           f"ceph pg ls-by-pool {a.pool} > {a.rundir}/problem_pgs.txt 2>&1",
                           shell=True)
            if self.down:
                zone, mon = self.down
                log(f"reviving {zone} after the problem")
                if mon:
                    self.d.start(mon)
                    self.wait(120, lambda: len(self.c.quorum()) == len(self.c.all_mons))
                for o in self.c.zones[zone]:
                    self.d.start(("osd", o))
            if a.leave_probes:
                log("probes left running for inspection")
            else:
                self.stop_probes()
        else:
            self.stop_probes()
        self.analyse()
        return 1 if self.problem else 0

    def stop_probes(self):
        for name, (p, out) in self.probes.items():
            try:
                os.killpg(p.pid, signal.SIGTERM)
            except OSError:
                pass
        time.sleep(3)

    def analyse(self):
        bounds = self.phases + [("_", time.time() + 1)]
        summary = {"phases": {p: round(t, 1) for p, t in self.phases},
                   "problem": self.problem, "probes": {}}
        for name, (p, out) in self.probes.items():
            recs = [json.loads(l) for l in open(out) if l.strip().startswith("{")]
            beats = [r for r in recs if "done" in r]
            per = {}
            for (ph, t0), (_, t1) in zip(bounds, bounds[1:]):
                win = [b for b in beats if t0 <= b["t"] < t1]
                run_, worst = 0, 0
                for b in win:
                    if b["done"] == 0:
                        run_ += 1
                        worst = max(worst, run_, b.get("inflight", 0))
                    else:
                        run_ = 0
                per[ph] = {"ops": sum(b["done"] for b in win), "max_stall_s": worst}
            summary["probes"][name] = per
        json.dump(summary, open(f"{self.args.rundir}/summary.json", "w"), indent=1)
        worst = {}
        for name, per in summary["probes"].items():
            for ph, v in per.items():
                key = ph.split("-", 1)[-1].split("-")[0] if ph.startswith("i") else ph
                worst.setdefault(key, []).append(v["max_stall_s"])
        print("max stall by phase type (s): " +
              ", ".join(f"{k}={max(v)}" for k, v in worst.items()))
        zero = [(n, ph) for n, per in summary["probes"].items()
                for ph, v in per.items() if v["ops"] == 0 and ph != "end"]
        if zero:
            print(f"probe/phases with ZERO ops: {zero[:20]}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", default="chaos")
    ap.add_argument("--zone", default=None, help="only fail this zone")
    ap.add_argument("--iterations", type=int, default=1, help="0 = forever")
    ap.add_argument("--baseline", type=int, default=60)
    ap.add_argument("--hold", type=int, default=300)
    ap.add_argument("--settle", type=int, default=60)
    ap.add_argument("--revive-orders", default="mon_first")
    ap.add_argument("--signals", default="KILL")
    ap.add_argument("--keep-mon", action="store_true")
    ap.add_argument("--policies", default="none,balance,localize")
    ap.add_argument("--copies", type=int, default=1)
    ap.add_argument("--stuck", type=float, default=60)
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--leave-probes", action="store_true",
                    help="on a stuck op, leave probes running for inspection")
    ap.add_argument("--healthy-timeout", type=int, default=1200)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--rundir", required=True)
    args = ap.parse_args()
    random.seed(args.seed)
    os.makedirs(args.rundir, exist_ok=True)
    sys.exit(Run(args).run())


if __name__ == "__main__":
    main()
