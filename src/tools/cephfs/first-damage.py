# Ceph - scalable distributed file system
#
# Copyright (C) 2022 Red Hat, Inc.
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.

# Suggested recovery sequence (for single MDS cluster):
#
# 1) Unmount all clients.
#
# 2) Flush the journal (if possible):
#
#    ceph tell mds.<fs_name>:0 flush journal
#
# 3) Fail the file system:
#
#    ceph fs fail <fs_name>
#
# 4a) Recover dentries from the journal. This will be a no-op if the MDS flushed the journal successfully:
#
#    cephfs-journal-tool --rank=<fs_name>:0 event recover_dentries summary
#
# 4b) If all good so far, reset the journal:
#
#    cephfs-journal-tool --rank=<fs_name>:0 journal reset
#
# 5) Run this tool to see list of damaged dentries:
#
#    python3 first-damage.py --memo run.1 <pool>
#
# 6) Optionally, remove them:
#
#    python3 first-damage.py --memo run.2 --remove <pool>
#
# Note: use --memo to specify a different file to save objects that have
# already been traversed, for independent runs.
#
# This has the effect of removing that dentry from the snapshot or HEAD
# (current hierarchy).  Note: the inode's linkage will be lost. The inode may
# be recoverable in lost+found during a future data scan recovery.

import argparse
import logging
import os
import rados
import re
import sys
import struct

log = logging.getLogger("first-damage-traverse")

MEMO = None
REMOVE = False
POOL = None
NEXT_SNAP = None
CONF = os.environ.get('CEPH_CONF')
REPAIR_NOSNAP = None

CEPH_NOSNAP = 0xfffffffe # int32 -2

DIR_PATTERN = re.compile(r'[0-9a-fA-F]{8,}\.[0-9a-fA-F]+')

CACHE = set()

def traverse(MEMO, ioctx):
    for o in ioctx.list_objects():
        if not DIR_PATTERN.fullmatch(o.key):
            log.debug("skipping %s", o.key)
            continue
        elif o.key in CACHE:
            log.debug("skipping previously examined object %s", o.key)
            continue
        log.info("examining: %s", o.key)

        with rados.ReadOpCtx() as rctx:
            nkey = None
            while True:
                it = ioctx.get_omap_vals(rctx, nkey, None, 100, omap_key_type=bytes)[0]
                ioctx.operate_read_op(rctx, o.key)
                nkey = None
                for (dnk, val) in it:
                    log.debug(f'\t{dnk}: val size {len(val)}')
                    (first,) = struct.unpack('<I', val[:4])
                    if first > NEXT_SNAP:
                        log.warning(f"found {o.key}:{dnk} first (0x{first:x}) > NEXT_SNAP (0x{NEXT_SNAP:x})")
                        if REPAIR_NOSNAP and dnk.endswith(b"_head") and first == CEPH_NOSNAP:
                            log.warning(f"repairing first==CEPH_NOSNAP damage, setting to NEXT_SNAP (0x{NEXT_SNAP:x})")
                            first = NEXT_SNAP
                            nval = bytearray(val)
                            struct.pack_into("<I", nval, 0, NEXT_SNAP)
                            with rados.WriteOpCtx() as wctx:
                                ioctx.set_omap(wctx, (dnk,), (bytes(nval),))
                                ioctx.operate_write_op(wctx, o.key)
                        elif REMOVE:
                            log.warning(f"removing {o.key}:{dnk}")
                            with rados.WriteOpCtx() as wctx:
                                ioctx.remove_omap_keys(wctx, [dnk])
                                ioctx.operate_write_op(wctx, o.key)
                    nkey = dnk
                if nkey is None:
                    break
        MEMO.write(f"{o.key}\n")

if __name__ == '__main__':
    outpath = os.path.join(os.path.expanduser('~'), os.path.basename(sys.argv[0]))
    P = argparse.ArgumentParser(description="remove CephFS metadata dentries with invalid first snapshot")
    P.add_argument('--conf', action='store', help='Ceph conf file', type=str, default=CONF)
    P.add_argument('--debug', action='store', help='debug file', type=str, default=outpath+'.log')
    P.add_argument('--memo', action='store', help='db for traversed dirs', default=outpath+'.memo')
    P.add_argument('--next-snap', action='store', help='force next-snap (dev)', type=int)
    P.add_argument('--remove', action='store_true', help='remove bad dentries', default=False)
    P.add_argument('--repair-nosnap', action='store_true', help='repair first=CEPH_NOSNAP damage', default=False)
    P.add_argument('pool', action='store', help='metadata pool', type=str)
    NS = P.parse_args()

    logging.basicConfig(filename=NS.debug, level=logging.DEBUG)

    MEMO = NS.memo
    REMOVE = NS.remove
    POOL = NS.pool
    NEXT_SNAP = NS.next_snap
    CONF = NS.conf
    REPAIR_NOSNAP = NS.repair_nosnap

    log.info("running as pid %d", os.getpid())

    try:
        with open(MEMO) as f:
            for line in f.readlines():
                CACHE.add(line.rstrip())
    except FileNotFoundError:
        pass

    R = rados.Rados(conffile=CONF)
    R.connect()
    ioctx = R.open_ioctx(POOL)

    if NEXT_SNAP is None:
        data = ioctx.read("mds_snaptable")
        # skip "version" of MDSTable payload
        # V=$(dd if="$SNAPTABLE" bs=1 count=1 skip=8 | od --endian=little -An -t u1)
        V = struct.unpack('<b', data[8:9])[0]
        log.debug("version is %d", V)
        if V != 5:
            raise RuntimeError("incompatible snaptable")
        # skip version,struct_v,compat_v,length
        # NEXT_SNAP=$((1 + $(dd if="$SNAPTABLE" bs=1 count=8 skip=14 | od --endian=little -An -t u8)))
        NEXT_SNAP = 1 + struct.unpack('<Q', data[14:22])[0]
        log.debug("NEXT_SNAP = %d", NEXT_SNAP)

    with open(MEMO, 'a') as f:
        log.info("saving traversed keys to %s to allow resuming", MEMO)
        traverse(f, ioctx)
