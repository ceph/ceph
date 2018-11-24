# create_all_pools_at_once.sh
#
# Script for pre-creating pools prior to Stage 4
#
# Pools are created with a number of PGs calculated to avoid health warnings
# that can arise during/after Stage 4 due to "too few" or "too many" PGs per
# OSD when DeepSea is allowed to create the pools with hard-coded number of
# PGs.
#
# see also https://github.com/SUSE/DeepSea/issues/536
#
# args: pools to be created
#
# example invocation: ./create_all_pools_at_once.sh foo bar baz

echo "Creating pools: $@"

set -ex

function json_total_osds {
    # total number of OSDs in the cluster
    ceph osd ls --format json | jq '. | length'
}

function pgs_per_pool {
    local TOTALPOOLS=$1
    test -n "$TOTALPOOLS"
    local TOTALOSDS=$(json_total_osds)
    test -n "$TOTALOSDS"
    # given the total number of pools and OSDs,
    # assume triple replication and equal number of PGs per pool
    # and aim for 100 PGs per OSD
    let "TOTALPGS = $TOTALOSDS * 100"
    let "PGSPEROSD = $TOTALPGS / $TOTALPOOLS / 3"
    echo $PGSPEROSD
}

function create_all_pools_at_once {
    # sample usage: create_all_pools_at_once foo bar
    local TOTALPOOLS="${#@}"
    local PGSPERPOOL=$(pgs_per_pool $TOTALPOOLS)
    for POOLNAME in "$@"
    do
        ceph osd pool create $POOLNAME $PGSPERPOOL $PGSPERPOOL replicated
    done
    ceph osd pool ls detail
}

CEPHFS=""
OPENSTACK=""
RBD=""
OTHER=""
for arg in "$@" ; do
    arg="${arg,,}"
    case "$arg" in
        cephfs) CEPHFS="$arg" ;;
        openstack) OPENSTACK="$arg" ;;
        rbd) RBD="$arg" ;;
        *) OTHER+=" $arg" ;;
    esac
done

POOLS=""
if [ $CEPHFS ] ; then
    POOLS+=" cephfs_data cephfs_metadata"
fi
if [ "$OPENSTACK" ] ; then
    POOLS+=" smoketest-cloud-backups smoketest-cloud-volumes smoketest-cloud-images"
    POOLS+=" smoketest-cloud-vms cloud-backups cloud-volumes cloud-images cloud-vms"
fi
if [ "$RBD" ] ; then
    POOLS+=" rbd"
fi
if [ "$OTHER" ] ; then
    POOLS+="$OTHER"
    APPLICATION_ENABLE="$OTHER"
fi
if [ -z "$POOLS" ] ; then
    echo "create_all_pools_at_once: bad arguments"
    exit 1
fi
echo "About to create pools ->$POOLS<-"
create_all_pools_at_once $POOLS
if [ "$APPLICATION_ENABLE" ] ; then
    for pool in "$APPLICATION_ENABLE" ; do
        ceph osd pool application enable $pool deepsea_qa
    done
fi
echo "OK" >/dev/null
