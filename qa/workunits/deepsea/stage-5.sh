#!/bin/bash
#
# DeepSea integration test "suites/basic/stage-5.sh"
#
# This script runs DeepSea stages 2 and 5 to remove a storage-only node from
# an existing Ceph cluster.
#
# In addition to the assumptions contained in README, this script assumes
# that:
# 1. DeepSea has already been used to deploy a cluster,
# 2. the cluster has at least one "storage-only" node (i.e. a node with role
#    "storage" and no other roles (except possibly "admin")), and
# 3. the cluster will be able to reach HEALTH_OK after one storage-only node
#    is dropped (typically this means the cluster needs at least 3 storage
#    nodes to start with).
#
# On success (HEALTH_OK is reached, number of storage nodes went down by 1,
# number of OSDs decreased), the script returns 0. On failure, for whatever
# reason, the script returns non-zero.
#
# The script produces verbose output on stdout, which can be captured for later
# forensic analysis.
#

set -e
set +x

SCRIPTNAME=$(basename ${0})
BASEDIR=$(readlink -f "$(dirname ${0})")
test -d $BASEDIR
[[ $BASEDIR =~ \/deepsea$ ]]

source $BASEDIR/common/common.sh

function usage {
    set +x
    echo "$SCRIPTNAME - script for testing HEALTH_OK deployment"
    echo "for use in SUSE Enterprise Storage testing"
    echo
    echo "Usage:"
    echo "  $SCRIPTNAME [-h,--help] [--cli]"
    echo
    echo "Options:"
    echo "    --cli           Use DeepSea CLI"
    echo "    --help          Display this usage message"
    exit 1
}

assert_enhanced_getopt

TEMP=$(getopt -o h \
--long "cli,help" \
-n 'health-ok.sh' -- "$@")

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

# Note the quotes around TEMP': they are essential!
eval set -- "$TEMP"

# process command-line options
CLI=""
while true ; do
    case "$1" in
        --cli) CLI="$1" ; shift ;;
        -h|--help) usage ;;    # does not return
        --) shift ; break ;;
        *) echo "Internal error" ; exit 1 ;;
    esac
done
echo "WWWW"
echo "stage-5.sh running with the following configuration:"
test -n "$CLI" && echo "- CLI"
set -x

# double-check there is a healthy cluster
ceph_health_test
STORAGE_NODES_BEFORE=$(number_of_hosts_in_ceph_osd_tree)
OSDS_BEFORE=$(number_of_osds_in_ceph_osd_tree)
test "$STORAGE_NODES_BEFORE"
test "$OSDS_BEFORE"
test "$STORAGE_NODES_BEFORE" -gt 1
test "$OSDS_BEFORE" -gt 0

# modify storage profile
STORAGE_PROFILE=$(storage_profile_from_policy_cfg)
FIRST_STORAGE_ONLY_NODE=$(_first_storage_only_node)
ls -lR $PROPOSALSDIR
PROPOSALS_BEFORE=$(find $PROPOSALSDIR -name \*$FIRST_STORAGE_ONLY_NODE\* | wc --lines)
policy_remove_storage_node $FIRST_STORAGE_ONLY_NODE
ls -lR $PROPOSALSDIR
PROPOSALS_AFTER=$(find $PROPOSALSDIR -name \*$FIRST_STORAGE_ONLY_NODE\* | wc --lines)

# run stages 2 and 5
run_stage_2 "$CLI"
ceph_cluster_status
run_stage_5 "$CLI"
ceph_cluster_status

# verification phase
ceph_health_test
STORAGE_NODES_AFTER=$(number_of_hosts_in_ceph_osd_tree)
OSDS_AFTER=$(number_of_osds_in_ceph_osd_tree)
test "$STORAGE_NODES_BEFORE"
test "$OSDS_BEFORE"
test "$STORAGE_NODES_AFTER" -eq "$((STORAGE_NODES_BEFORE - 1))"
test "$OSDS_AFTER" -lt "$OSDS_BEFORE"

# osd.report for good measure
salt -I roles:storage osd.report 2>/dev/null

echo "YYYY"
echo "stage-5 test result: PASS"
