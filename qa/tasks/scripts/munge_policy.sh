#!/bin/bash

# munge_policy.sh <path> <node> <node_add|node_rm> [<role>]

# <path> path to policy.cfg
# <node> node fqdn
# <role> a list of roles?

set -ex

POLICY_CFG="${1}"
NODE_FQDN="${2}" # target192168000063.teuthology
OPERATION="${3}" # node_add / node_rm
DEEPSEA_ROLE="${4}" # storage

function node_add {
    local fqdn=$1
    local role=$2
    echo "role-${role}/cluster/${fqdn}.sls" >> ${POLICY_CFG}
    cat ${POLICY_CFG}
}

function node_rm {
    local fqdn=$1
    sed -i -e "/${fqdn/\./\\\.}/d" ${POLICY_CFG}
    cat ${POLICY_CFG}
}

if [ "$OPERATION" = "node_add" ] || [ "$OPERATION" = "node_rm" ] ; then
    $OPERATION $NODE_FQDN $DEEPSEA_ROLE
else
    echo "Unknown operation $OPERATION"
    false
fi
