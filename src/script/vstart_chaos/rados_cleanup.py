#!/usr/bin/env python3
"""Remove what a finished ceph_test_rados run left in a pool: its objects
(named <prefix><n>) and the self-managed snaps holding their clones, which
the test never removes.  Those snaps are allocated by that run alone, so
removing them cannot affect other clients.

usage: rados_cleanup.py POOL PREFIX
"""

import json
import os
import subprocess
import sys

import rados

pool, prefix = sys.argv[1:3]
build = os.environ["CEPH_BUILD"]
binary = os.environ.get("CHAOS_BIN_DIR", f"{build}/bin") + "/rados"

with rados.Rados(conffile=os.environ.get("CEPH_CONF", f"{build}/ceph.conf")) as cluster:
    with cluster.open_ioctx(pool) as ioctx:
        names = [o.key for o in ioctx.list_objects() if o.key.startswith(prefix)]
        snaps = set()
        for name in names:
            r = subprocess.run([binary, "-p", pool, "--format", "json", "listsnaps", name],
                               capture_output=True, text=True, timeout=120)
            if r.returncode == 0:
                for clone in json.loads(r.stdout)["clones"]:
                    if clone["id"] != "head":
                        snaps.update(s["id"] for s in clone["snapshots"])
            try:
                ioctx.remove_object(name)
            except rados.ObjectNotFound:
                pass
        for snap in sorted(snaps):
            try:
                ioctx.remove_self_managed_snap(snap)
            except rados.Error:
                pass
        print(f"{prefix}: {len(names)} objects, {len(snaps)} snaps removed")
