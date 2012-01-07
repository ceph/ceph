#!/bin/bash -x

for f in `seq 1 8`
do
    echo testing failure point $f
    pushd . ; cd $bindir ; ./ceph -c $conf mds tell \* injectargs "--mds_kill_mdstable_at $f" ; popd
    sleep 1  # wait for mds command to go thru
    bash -c "pushd . ; cd $bindir ; sleep 10 ; ./init-ceph -c $conf start mds ; popd" &
    touch $f
    ln $f $f.link
    sleep 10
done

