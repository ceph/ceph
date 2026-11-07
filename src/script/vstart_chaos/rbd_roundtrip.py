#!/usr/bin/env python3
"""Random-offset/length writes to an rbd image whose data lives in the
test pool, verified against an in-memory model. Exits non-zero on the
first miscompare."""

import os
import random
import sys
import time

import rados
import rbd

META_POOL = "rbd"
DATA_POOL = sys.argv[2] if len(sys.argv) > 2 else "chaos"
SEED = int(sys.argv[1]) if len(sys.argv) > 1 else int(time.time())
SIZE = 32 << 20
ROUNDS = 400

rng = random.Random(SEED)
name = f"chaos-{SEED}-{os.getpid()}"
print(f"seed={SEED} image={name} data_pool={DATA_POOL}", flush=True)

cluster = rados.Rados(conffile="")
cluster.connect()
meta = cluster.open_ioctx(META_POOL)
rbd.RBD().create(meta, name, SIZE, old_format=False, data_pool=DATA_POOL)
model = bytearray(SIZE)
try:
    with rbd.Image(meta, name) as img:
        for r in range(ROUNDS):
            kind = rng.random()
            if kind < 0.1:
                length = rng.choice([4 << 20, 8 << 20])
            elif kind < 0.5:
                length = rng.randint(1, 64 << 10)
            else:
                length = rng.choice([4096, 8192, 16384, 65536, 131072])
            off = rng.randrange(0, SIZE - length)
            if rng.random() < 0.3:
                off -= off % 4096
            data = rng.randbytes(length)
            img.write(data, off)
            model[off:off + length] = data
            if rng.random() < 0.2:
                ro = rng.randrange(0, SIZE - (1 << 20))
                got = img.read(ro, 1 << 20)
                if got != bytes(model[ro:ro + (1 << 20)]):
                    print(f"MISCOMPARE round {r} range {ro}+1M", flush=True)
                    sys.exit(2)
            if r % 50 == 49:
                got = img.read(0, SIZE)
                if got != bytes(model):
                    bad = next(i for i in range(SIZE) if got[i] != model[i])
                    print(f"MISCOMPARE full read round {r} first bad byte {bad}",
                          flush=True)
                    sys.exit(2)
                print(f"round {r} verified", flush=True)
finally:
    try:
        rbd.RBD().remove(meta, name)
    except Exception as e:
        print(f"remove failed: {e}", flush=True)
    meta.close()
    cluster.shutdown()
print("OK", flush=True)
