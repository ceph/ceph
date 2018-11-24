# remove_storage_only_node.sh
#
# actually remove the node
#
# args: --cli (optional)

set -ex

CLI=$1

function number_of_hosts_in_ceph_osd_tree {
    ceph osd tree -f json-pretty | jq '[.nodes[] | select(.type == "host")] | length'
}

function number_of_osds_in_ceph_osd_tree {
    ceph osd tree -f json-pretty | jq '[.nodes[] | select(.type == "osd")] | length'
}

function run_stage {
    local stage_num=$1
    if [ "$CLI" ] ; then
        timeout 60m deepsea \
            --log-file=/var/log/salt/deepsea.log \
            --log-level=debug \
            salt-run state.orch ceph.stage.$stage_num \
            --simple-output
    else
        timeout 60m salt-run \
            state.orch ceph.stage.$stage_num \
            2> /dev/null
    fi
}

STORAGE_NODES_BEFORE=$(number_of_hosts_in_ceph_osd_tree)
OSDS_BEFORE=$(number_of_osds_in_ceph_osd_tree)
test "$STORAGE_NODES_BEFORE"
test "$OSDS_BEFORE"
test "$STORAGE_NODES_BEFORE" -gt 1
test "$OSDS_BEFORE" -gt 0

run_stage 2
ceph -s
run_stage 5
ceph -s

STORAGE_NODES_AFTER=$(number_of_hosts_in_ceph_osd_tree)
OSDS_AFTER=$(number_of_osds_in_ceph_osd_tree)
test "$STORAGE_NODES_BEFORE"
test "$OSDS_BEFORE"
test "$STORAGE_NODES_AFTER" -eq "$((STORAGE_NODES_BEFORE - 1))"
test "$OSDS_AFTER" -lt "$OSDS_BEFORE"
