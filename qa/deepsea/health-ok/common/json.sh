#
# This file is part of the DeepSea integration test suite.
# It contains various cluster introspection functions.
#

function json_total_nodes {
    # total number of nodes in the cluster
    salt --static --out json '*' test.ping 2>/dev/null | jq '. | length'
}

function _json_nodes_of_role_x {
    local ROLE=$1
    salt --static --out json -C "I@roles:$ROLE" test.ping 2>/dev/null | jq '. | length'
}

function json_storage_nodes {
    # number of storage nodes in the cluster
    _json_nodes_of_role_x storage
}

function json_total_osds {
    # total number of OSDs in the cluster
    ceph osd ls --format json | jq '. | length'
}
