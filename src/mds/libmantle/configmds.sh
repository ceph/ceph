#!/bin/bash
# Greedy spill balancer
# - this is a sample Mantle configuration that works wth vstart (assumes multiple MDSs)

CEPH_SET="sudo ../../ceph --admin-daemon ../../out/mds.a.asok config set "
DIR=`pwd`

{
    # Debugging
    $CEPH_SET debug_mds 0
    $CEPH_SET debug_ms 0
    $CEPH_SET debug_mds_migrator 0
    $CEPH_SET debug_mds_balancer 2
    $CEPH_SET mds_bal_debug_dfs 10
    $CEPH_SET mds_bal_debug_dfs_depth 4
    
    # Turn on/off features
    $CEPH_SET mds_bal_dir $DIR
    $CEPH_SET mds_log true
    $CEPH_SET mds_bal_frag 1
    $CEPH_SET mds_cache_size 0
    $CEPH_SET client_cache_size 0
    
    # Configuration parameters (i.e. tunables)
    $CEPH_SET mds_bal_need_min 0.98
    $CEPH_SET mds_bal_split_size 10000
    $CEPH_SET ms_async_op_threads 0
    
    # balancer
    $CEPH_SET mds_bal_metaload "IWR"
    $CEPH_SET mds_bal_mdsload "MDSs[i][\"all\"]"
    $CEPH_SET mds_bal_when "if_MDSs[whoami][\"load\"]>.01_and_MDSs[whoami+1][\"load\"]<0.01_then"
    $CEPH_SET mds_bal_where "targets[whoami+1]=MDSs[whoami][\"load\"]/2"
    $CEPH_SET mds_bal_howmuch "{\"half\"}"
} 2>&1 | grep "success\|failure\|error"
