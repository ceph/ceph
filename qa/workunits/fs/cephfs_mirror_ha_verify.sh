#!/bin/bash -ex
#
# cephfs_mirror_ha_verify.sh - verify synchronized snapshots
#

. $(dirname $0)/cephfs_mirror_helpers.sh

echo "running verifier on secondary file system..."

for i in `seq 1 $NR_DIRECTORIES`
do
    repo_name="${REPO_PATH_PFX}_$i"
    for j in `seq 1 $NR_SNAPSHOTS`
    do
        for s in 1 1 2 4 4 4 4 4 8 8 8 8 16 16 32 64 64 128 128
        do
            sleep $s
            snap_name=$repo_name/.snap/snap_$j
            if test -d $repo_name; then
                echo "checking snapshot [$snap_name] in $repo_name"
                if test -d $snap_name; then
                    echo "generating hash for $snap_name"
                    cksum=''
                    calc_checksum $snap_name cksum
                    ret=$(compare_checksum $cksum $snap_name)
                    if [ $ret -ne 0 ]; then
                        echo "checksum failed $snap_name ($cksum)"
                        return $ret
                    else
                        echo "checksum matched $snap_name ($cksum)"
                        break
                    fi
                fi
            fi
        done
        echo "couldn't complete verification for: $snap_name"
    done
done

echo "verify done!"
