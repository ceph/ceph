#!/bin/bash
# Snapshot daemon/tool binaries and shared libs of a build into a stable dir,
# with RPATH/RUNPATH removed so nothing loads from the (changing) build tree.
# Never modify a snapshot that running processes use - make a new one.
set -e
BUILD=${1:?build dir}
S=${2:?snapshot dir}
[ -e "$S" ] && { echo "$S exists"; exit 1; }
P=$(command -v patchelf || echo $HOME/.local/bin/patchelf)
[ -x "$P" ] || { echo "patchelf not found"; exit 1; }
mkdir -p $S/bin $S/lib
cp -a $BUILD/bin/ceph-osd $BUILD/bin/ceph-mon $BUILD/bin/ceph_test_rados \
      $BUILD/bin/ceph_test_rados_io_sequence $BUILD/bin/rados $S/bin/
cp -a $BUILD/lib/*.so $BUILD/lib/*.so.* $S/lib/ 2>/dev/null
for f in $S/bin/* $S/lib/*.so*; do
    [ -L "$f" ] && continue
    readelf -d "$f" 2>/dev/null | grep -qE 'RPATH|RUNPATH' && $P --remove-rpath "$f"
done
git -C $(dirname $BUILD) log -1 --format='%h %s' > $S/VERSION
cat $S/VERSION; du -sh $S
