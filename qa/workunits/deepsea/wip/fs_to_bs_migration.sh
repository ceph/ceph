#!/bin/bash
#
# DeepSea integration test "suites/ceph-test/fs_to_bs_migration.sh"
#
# This script deploys a basic cluster with filestore type of OSDs,
# after cluster is healthy, migrate to bluestore OSD type. 
#
# On success, the script returns 0. On failure, for whatever reason, the script
# returns non-zero.
#
# The script produces verbose output on stdout, which can be captured for later
# forensic analysis.

set -ex
BASEDIR=$(pwd)
source $BASEDIR/common/common.sh

install_deps
cat_salt_config
disable_restart_in_stage_0
run_stage_0
run_stage_1
policy_cfg_base
policy_cfg_mon_flex
policy_cfg_storage # no node will be a "client"
configure_all_OSDs_to_filestore
cat_policy_cfg
run_stage_2
ceph_conf_small_cluster
ceph_conf_mon_allow_pool_delete
run_stage_3
ceph_cluster_status
ceph_health_test
check_OSD_type filestore
migrate_to_bluestore
ceph_health_test
check_OSD_type bluestore

echo "OK"

