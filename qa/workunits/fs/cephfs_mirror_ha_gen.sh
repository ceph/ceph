#!/bin/bash -ex
#
# cephfs_mirror_ha_gen.sh - generate workload to synchronize
#

. $(dirname $0)/cephfs_mirror_helpers.sh

cleanup()
{
    for i in `seq 1 $NR_DIRECTORIES`
    do
        local repo_name="${REPO_PATH_PFX}_$i"
        for j in `seq 1 $NR_SNAPSHOTS`
        do
            snap_name=$repo_name/.snap/snap_$j
            if test -d $snap_name; then
                rmdir $snap_name
            fi
        done
    done
    exit 1
}
trap cleanup EXIT

configure_peer()
{
    ceph mgr module enable mirroring
    # Ensure mirroring module is loaded
    while ! ceph fs snapshot mirror daemon status; do sleep 5; done
    ceph fs snapshot mirror enable $PRIMARY_FS
    ceph fs snapshot mirror peer_add $PRIMARY_FS client.mirror_remote@ceph $BACKUP_FS

    for i in `seq 1 $NR_DIRECTORIES`
    do
        local repo_name="${REPO_PATH_PFX}_$i"
        ceph fs snapshot mirror add $PRIMARY_FS "$MIRROR_SUBDIR/$repo_name"
    done
}

create_snaps()
{
    for i in `seq 1 $NR_DIRECTORIES`
    do
        local repo_name="${REPO_PATH_PFX}_$i"
        for j in `seq 1 $NR_SNAPSHOTS`
        do
            snap_name=$repo_name/.snap/snap_$j
            r=$(( $RANDOM % 100 + 5 ))
            arr=($repo_name "reset" "--hard" "HEAD~$r")
            exec_git_cmd "${arr[@]}"
            mkdir $snap_name
            store_checksum $snap_name
        done
    done
}

unset CEPH_CLI_TEST_DUP_COMMAND

echo "running generator on prmary file system..."

# setup git repos to be used as data set
setup_repos

# turn on mirroring, add peers...
configure_peer

# snapshots on primary
create_snaps

# do not cleanup when exiting on success..
trap - EXIT
