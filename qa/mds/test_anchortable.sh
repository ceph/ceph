#!/bin/bash -x

mkdir links
for f in `seq 1 8`
do
    mkdir $f
    for g in `seq 1 20`
    do
	touch $f/$g
	ln $f/$g links/$f.$g
    done
done

for f in `seq 1 8`
do
    echo testing failure point $f
    bash -c "pushd . ; cd $bindir ; sleep 10; ./ceph -c $conf mds tell \* injectargs \"--mds_kill_mdstable_at $f\" ; popd" &
    bash -c "pushd . ; cd $bindir ; sleep 11 ; ./init-ceph -c $conf start mds ; popd" &
    for g in `seq 1 20`
    do
	rm $f/$g
	rm links/$f.$g
	sleep 1
    done
done

