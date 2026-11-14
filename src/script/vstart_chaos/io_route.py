#!/usr/bin/env python3
"""Where does read IO actually go?

For each client configuration (read policy x client crush_location) this
writes a set of objects, snapshots every OSD's perf counters, performs a burst
of reads with that policy, snapshots again, and reports per-OSD / per-zone
deltas of read-related counters together with which OSD is the primary of
each object's PG.  Expectations checked:

  none      client ops land on the PG primaries only
  localize  client ops land only in the client's zone (or zone 0 if unknown),
            apart from reads its replicas bounce to the primary
  balance   client ops spread over every zone (with one zone: not only
            on the primaries)

Run on an otherwise idle cluster; concurrent IO pollutes the deltas.
"""

import argparse
import json
import random
import sys
from collections import defaultdict

import rados
from chaos import ceph_json, datacenter_osds


def client(pool, policy="none", zone=None):
    conf = {"rados_replica_read_policy": "default" if policy == "none" else policy}
    if zone:
        conf["crush_location"] = f"datacenter={zone}"
    cluster = rados.Rados(conffile="", conf=conf)
    cluster.connect()
    return cluster, cluster.open_ioctx(pool)


def osd_counters(osds, sections):
    snap = {}
    for o in osds:
        d = ceph_json(f"tell osd.{o} perf dump") or {}
        flat = {}
        for sec, vals in d.items():
            if sections and not any(sec.startswith(s) for s in sections):
                continue
            for k, v in vals.items():
                if isinstance(v, (int, float)):
                    flat[f"{sec}.{k}"] = v
                elif isinstance(v, dict) and "avgcount" in v:
                    flat[f"{sec}.{k}.avgcount"] = v["avgcount"]
        snap[o] = flat
    return snap


def zones():
    return datacenter_osds() or {"all": sorted(ceph_json("osd ls"))}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", default="chaos")
    ap.add_argument("--objects", type=int, default=48)
    ap.add_argument("--size", type=int, default=64 << 10)
    ap.add_argument("--reads", type=int, default=20, help="reads per object")
    ap.add_argument("--configs", default=None,
                    help="comma list of policy:client_zone (default: every "
                         "policy without a zone, and localize from each zone)")
    ap.add_argument("--sections", default="osd",
                    help="comma list of perf dump sections to diff ('' = all)")
    ap.add_argument("--show", default="op_r,op_rw,subop,ec_,read",
                    help="substrings of counter names to report")
    ap.add_argument("--json", default=None)
    args = ap.parse_args()

    z = zones()
    if args.configs is None:
        multi = [zn for zn in sorted(z) if zn != "all"]
        args.configs = ",".join(["none:", "balance:", "localize:"] +
                                [f"localize:{zn}" for zn in multi])
    osd_zone = {o: name for name, osds in z.items() for o in osds}
    osds = sorted(osd_zone)
    sections = [s for s in args.sections.split(",") if s]
    show = [s for s in args.show.split(",") if s]
    report = []
    ok = True
    writer_cluster, writer = client(args.pool)
    oids = [f"route-{random.randint(0, 1 << 30)}-{i}" for i in range(args.objects)]
    for oid in oids:
        writer.write_full(oid, random.randbytes(args.size))
    primaries = {}
    for oid in oids:
        m = ceph_json(f"osd map {args.pool} {oid}")
        primaries[oid] = (m["pgid"], m["acting_primary"], m["acting"])
    prim_set = {p for _, p, _ in primaries.values()}

    for cfg in args.configs.split(","):
        policy, zone = cfg.split(":")
        cluster, ioctx = client(args.pool, policy, zone)
        before = osd_counters(osds, sections)
        for _ in range(args.reads):
            for oid in oids:
                off = random.choice([0, 0, random.randint(0, args.size - 1)])
                ioctx.read(oid, args.size - off, off)
        after = osd_counters(osds, sections)
        ioctx.close()
        cluster.shutdown()
        delta = {o: {k: after[o].get(k, 0) - before[o].get(k, 0)
                     for k in after[o] if after[o].get(k, 0) != before[o].get(k, 0)}
                 for o in osds}
        per_zone = defaultdict(lambda: defaultdict(int))
        for o in osds:
            for k, v in delta[o].items():
                per_zone[osd_zone[o]][k] += v
        op_r = {o: delta[o].get("osd.op_r", 0) for o in osds}
        total = sum(op_r.values())
        by_zone = {zn: sum(op_r[o] for o in zo) for zn, zo in z.items()}
        on_prim = sum(op_r[o] for o in prim_set)
        # a replica sends a read back to the primary while its read lease has
        # lapsed or it cannot yet tell that the object's last write committed
        bounced = {zn: per_zone[zn].get("osd.replica_read", 0) -
                   per_zone[zn].get("osd.replica_read_served", 0) for zn in z}
        verdict = []
        if policy == "none" and total and on_prim != total:
            verdict.append(f"none: {total - on_prim}/{total} client reads NOT on primaries")
        if policy == "localize":
            want = zone if zone in z else sorted(z)[0]
            off = total - by_zone.get(want, 0)
            if total and off > bounced.get(want, 0):
                verdict.append(f"localize {zone}: {off}/{total} client reads outside {want}")
        if policy == "balance" and total:
            if len(z) > 1 and min(by_zone.values()) < 0.2 * total:
                verdict.append(f"balance: skewed {by_zone}")
            if len(z) == 1 and on_prim == total:
                verdict.append("balance: every client read on a primary")
        if verdict:
            ok = False
        print(f"\n=== {policy} client_zone={zone or '-'}: op_r total={total} "
              f"by_zone={by_zone} on_primaries={on_prim} "
              f"bounced_by_replicas={bounced}")
        print("  per-OSD op_r: " + " ".join(
            f"{o}({osd_zone[o]}{'*' if o in prim_set else ''})={op_r[o]}" for o in osds))
        for zn in sorted(per_zone):
            items = {k: v for k, v in per_zone[zn].items()
                     if any(s in k for s in show)}
            print(f"  {zn}: " + ", ".join(f"{k}={v}" for k, v in sorted(items.items())))
        for v in verdict:
            print(f"  !!! {v}")
        report.append({"policy": policy, "zone": zone, "op_r": op_r,
                       "by_zone": by_zone, "on_primaries": on_prim,
                       "per_zone": {k: dict(v) for k, v in per_zone.items()},
                       "verdict": verdict})
    for oid in oids:
        writer.remove_object(oid)
    writer.close()
    writer_cluster.shutdown()
    if args.json:
        json.dump({"primaries": primaries, "report": report}, open(args.json, "w"),
                  indent=1)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
