#!/usr/bin/env bash
#
# Run on several clients at once: each creates files in one directory that
# ceph.dir.pin.distributed spreads over the active MDSs, from a few
# processes, and then removes them.
#
# Removes in such a directory used to crawl at a few hundred per second:
# ceph-fuse revalidates the directory with a lookup for every operation in
# it, and asked for Fs on it in each of them, which the MDS can only grant
# by gathering the directory's filelock from the other MDSs, waiting for
# their early-replied unlinks to be journaled; creates suffered alike.
# Fail when the creates or the removes take longer than TIMEOUT seconds.

set -ex

DIR=${CEPH_MNT:?}/shared_dir_unlink
NPROC=${NPROC:-4}
NFILES=${NFILES:-20000}
TIMEOUT=${TIMEOUT:-600}
NAME=$(hostname -s).$$

mkdir -p "$DIR"
setfattr -n ceph.dir.pin.distributed -v 1 "$DIR"

PROG=$(mktemp --suffix=.py)
trap 'rm -f "$PROG"' EXIT
cat > "$PROG" <<'EOF'
import os
import sys
import time
from multiprocessing import Process

op, d, name, nproc, nfiles = sys.argv[1:6]
nproc, nfiles = int(nproc), int(nfiles)


def work(p):
    for i in range(nfiles):
        path = os.path.join(d, f"{name}.{p}.{i}")
        if op == "create":
            os.close(os.open(path, os.O_CREAT | os.O_WRONLY, 0o644))
        else:
            os.unlink(path)


if __name__ == "__main__":
    start = time.time()
    procs = [Process(target=work, args=(p,)) for p in range(nproc)]
    for pr in procs:
        pr.start()
    for pr in procs:
        pr.join()
    elapsed = time.time() - start
    print(f"{op}: {nproc * nfiles} files in {elapsed:.1f}s, "
          f"{nproc * nfiles / elapsed:.0f}/s")
    sys.exit(1 if any(pr.exitcode for pr in procs) else 0)
EOF

timeout "$TIMEOUT" python3 "$PROG" create "$DIR" "$NAME" "$NPROC" "$NFILES"
timeout "$TIMEOUT" python3 "$PROG" unlink "$DIR" "$NAME" "$NPROC" "$NFILES"

# the other clients may still be removing theirs
rmdir "$DIR" 2> /dev/null || true
