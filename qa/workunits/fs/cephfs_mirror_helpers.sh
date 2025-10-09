PRIMARY_FS='dc'
BACKUP_FS='dc-backup'

REPO=ceph-qa-suite
REPO_DIR=ceph_repo
REPO_PATH_PFX="$REPO_DIR/$REPO"

NR_DIRECTORIES=4
NR_SNAPSHOTS=4
MIRROR_SUBDIR='/mirror'

calc_checksum()
{
    local path=$1
    local -n ref=$2
    ref=`find -L $path -type f -exec md5sum {} +  | awk '{ print $1 }' | md5sum | awk '{ print $1 }'`
}

store_checksum()
{
    local path=$1
    local cksum='' #something invalid
    local fhash=`echo -n $path | md5sum | awk '{ print $1 }'`
    calc_checksum $path cksum
    echo -n $cksum > "/tmp/primary-$fhash"
}

compare_checksum()
{
    local ret=0
    local cksum=$1
    local path=$2
    local fhash=`echo -n $path | md5sum | awk '{ print $1 }'`
    local cksum_ondisk=`cat /tmp/primary-$fhash`
    if [ $cksum != $cksum_ondisk ]; then
        echo "$cksum <> $cksum_ondisk"
        ret=1
    fi
    echo $ret
}

exec_git_cmd()
{
    local arg=("$@")
    local repo_name=${arg[0]}
    local cmd=${arg[@]:1}
    git --git-dir "$repo_name/.git" $cmd
}

clone_repo()
{
    local repo_name=$1
    git clone --branch giant "http://github.com/ceph/$REPO" $repo_name
}

setup_repos()
{
    mkdir "$REPO_DIR"

    for i in `seq 1 $NR_DIRECTORIES`
    do
        local repo_name="${REPO_PATH_PFX}_$i"
        mkdir $repo_name
        clone_repo $repo_name
    done
}
