#!/usr/bin/env python3
"""IO continuity + correctness probe for a pool.

One client (read policy none|balance|localize, optional crush_location) does a
steady mix of full writes, partial overwrites and reads of a small object set,
verifying every read against an in-memory model (exit 2 on a miscompare, e.g.
a stale shard served after a zone comes back).

A watchdog thread writes one JSON heartbeat per second to --out:
  {"t": epoch, "done": ops completed in the last second, "inflight": age of
   the op currently outstanding (s), "op": its description}
so a chaos driver can see exactly how long IO stalled during a zone failover
and whether it resumed while the zone was still down.
"""

import argparse
import json
import random
import sys
import threading
import time

import radosc

ap = argparse.ArgumentParser()
ap.add_argument("--pool", default="chaos")
ap.add_argument("--policy", choices=list(radosc.POLICY_FLAGS), default="none")
ap.add_argument("--zone", default=None, help="client datacenter for crush_location")
ap.add_argument("--objects", type=int, default=24)
ap.add_argument("--max-size", type=int, default=256 << 10)
ap.add_argument("--duration", type=float, default=0, help="0 = forever")
ap.add_argument("--seed", type=int, default=None)
ap.add_argument("--out", required=True)
ap.add_argument("--name", default="probe")
ap.add_argument("--debug", action="store_true",
                help="client log with debug_objecter=20 and an admin socket; "
                     "dump objecter_requests when an op is stuck > --stuck secs")
ap.add_argument("--stuck", type=float, default=30)
args = ap.parse_args()

seed = args.seed if args.seed is not None else random.randint(1, 1 << 30)
rng = random.Random(seed)
loc = f"datacenter={args.zone}" if args.zone else None
extra = {"rados_osd_op_timeout": 0}
base = args.out.rsplit(".", 1)[0]
if args.debug:
    extra.update({"log_file": f"{base}.client.log", "debug_objecter": "20",
                  "debug_ms": "1", "debug_rados": "10",
                  "admin_socket": f"{base}.asok"})
cl = radosc.Client(args.pool, crush_location=loc, extra=extra)
flags = radosc.POLICY_FLAGS[args.policy]
prefix = f"{args.name}-{seed}"

lock = threading.Lock()
state = {"done": 0, "op": None, "start": None, "stop": False,
         "reads": 0, "writes": 0, "max_lat": 0.0}
out = open(args.out, "a")


def emit(rec):
    out.write(json.dumps(rec) + "\n")
    out.flush()


def dump_stuck(desc, age):
    import subprocess
    f = f"{base}.stuck-{int(time.time())}.txt"
    with open(f, "w") as fh:
        fh.write(f"# stuck op {desc} age {age:.0f}s\n")
        r = subprocess.run(["ceph", "--admin-daemon", f"{base}.asok", "objecter_requests"],
                           capture_output=True, text=True, timeout=30)
        fh.write(f"## objecter_requests\n{r.stdout}{r.stderr}\n")
    emit({"t": time.time(), "STUCK_DUMP": f, "op": desc, "age": round(age, 1)})


def watchdog():
    dumped = None
    while not state["stop"]:
        time.sleep(1)
        with lock:
            now = time.time()
            age = now - state["start"] if state["start"] else 0
            rec = {"t": round(now, 1), "done": state["done"],
                   "inflight": round(age, 1), "op": state["op"]}
            state["done"] = 0
            start = state["start"]
        emit(rec)
        if args.debug and age > args.stuck and dumped != start:
            dumped = start
            try:
                dump_stuck(rec["op"], age)
            except Exception as e:
                emit({"t": time.time(), "dump_error": str(e)})


def run(desc, fn):
    with lock:
        state["op"], state["start"] = desc, time.time()
    t0 = time.time()
    r = fn()
    lat = time.time() - t0
    with lock:
        state["op"], state["start"] = None, None
        state["done"] += 1
        state["max_lat"] = max(state["max_lat"], lat)
    if lat > 5:
        emit({"t": round(time.time(), 1), "slow": desc, "lat": round(lat, 1)})
    return r


emit({"t": time.time(), "start": args.name, "policy": args.policy,
      "zone": args.zone, "seed": seed})
threading.Thread(target=watchdog, daemon=True).start()

model = {}
oids = [f"{prefix}-{i}" for i in range(args.objects)]
for oid in oids:
    data = rng.randbytes(rng.randint(1, args.max_size))
    run(f"write_full {oid}", lambda: cl.write_full(oid, data))
    model[oid] = bytearray(data)

end = time.time() + args.duration if args.duration else None
rc = 0
try:
    while end is None or time.time() < end:
        oid = rng.choice(oids)
        cur = model[oid]
        p = rng.random()
        if p < 0.15:
            data = rng.randbytes(rng.randint(1, args.max_size))
            run(f"write_full {oid}", lambda: cl.write_full(oid, data))
            model[oid] = bytearray(data)
            state["writes"] += 1
        elif p < 0.4:
            off = rng.randint(0, len(cur))
            data = rng.randbytes(rng.randint(1, 32 << 10))
            run(f"write {oid}@{off}+{len(data)}", lambda: cl.write(oid, data, off))
            if off > len(cur):
                cur.extend(b"\0" * (off - len(cur)))
            cur[off:off + len(data)] = data
            state["writes"] += 1
        else:
            if rng.random() < 0.5 or not cur:
                off, length = 0, len(cur) + rng.randint(0, 4096)
            else:
                off = rng.randint(0, len(cur) - 1)
                length = rng.randint(1, len(cur) - off + 4096)
            got = run(f"read {oid}@{off}+{length}",
                      lambda: cl.read(oid, off, length, flags))
            want = bytes(cur[off:off + length])
            state["reads"] += 1
            if got != want:
                bad = next((i for i in range(min(len(got), len(want)))
                            if got[i] != want[i]), min(len(got), len(want)))
                emit({"t": time.time(), "MISCOMPARE": oid, "off": off,
                      "len": length, "got_len": len(got), "want_len": len(want),
                      "first_bad": off + bad})
                print(f"MISCOMPARE {oid} off={off} len={length} got={len(got)} "
                      f"want={len(want)} first_bad={off + bad}", flush=True)
                rc = 2
                break
except radosc.RadosError as e:
    emit({"t": time.time(), "ERROR": str(e)})
    print(f"ERROR {e}", flush=True)
    rc = 3
finally:
    state["stop"] = True
    emit({"t": time.time(), "summary": True, "reads": state["reads"],
          "writes": state["writes"], "max_lat": round(state["max_lat"], 1),
          "rc": rc})
    if rc == 0:
        for oid in oids:
            cl.remove(oid)
    print(f"reads={state['reads']} writes={state['writes']} "
          f"max_lat={state['max_lat']:.1f}s rc={rc}", flush=True)
    cl.close()
sys.exit(rc)
