#!/bin/bash -x

for f in `seq 1 8`
do
    echo testing failure point $f
    $bindir/ceph -c $conf mds injectargs 0 "--mds_kill_mdstable_at $f"
    sleep 1  # wait for mds command to go thru
    pushd . ; cd $bindir ; ./init-ceph -c $conf start mds ; popd
    touch $f
    ln $f $f.link
done

