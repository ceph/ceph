# This file is part of the DeepSea integration test suite

#
# separate file to house the deploy_ceph function
#

function _os_specific_install_deps {
    echo "Installing dependencies on the Salt Master node"
    local DEPENDENCIES="jq
    "
    _zypper_ref_on_master
    for d in $DEPENDENCIES ; do
        _zypper_install_on_master $d
    done
}

function _determine_master_minion {
    type hostname
    MASTER_MINION=$(hostname)
    salt $MASTER_MINION test.ping
}

function _os_specific_repos_and_packages_info {
    _dump_salt_master_zypper_repos
    type rpm
    rpm -q salt-master
    rpm -q salt-minion
    rpm -q salt-api
    rpm -q deepsea || true
}

function _set_deepsea_minions {
    #
    # set deepsea_minions to * - see https://github.com/SUSE/DeepSea/pull/526
    # (otherwise we would have to set deepsea grain on all minions)
    echo "deepsea_minions: '*'" > /srv/pillar/ceph/deepsea_minions.sls
    cat /srv/pillar/ceph/deepsea_minions.sls
}

function _initialize_minion_array {
    set +x
    local m=
    local i=0
    if type salt-key > /dev/null 2>&1; then
        MINION_LIST=$(salt-key -L -l acc | grep -v '^Accepted Keys')
        for m in $MINION_LIST ; do
            echo "Adding minion $m to minion array"
            MINION_ARRAY[0]=$m
            i=$((i+1))
        done
    else
        echo "Cannot find salt-key. Is Salt installed? Is this running on the Salt Master?"
        exit 1
    fi
    echo "There are $i minions in this Salt cluster"
    set -x
}

function _update_salt {
    # make sure we are running the latest Salt before Stage 0 starts,
    # otherwise Stage 0 will update Salt and then fail with cryptic
    # error messages
    TOTAL_NODES=$(json_total_nodes)
    salt '*' cmd.run 'zypper -n in -f python3-salt salt salt-api salt-master salt-minion' 2>/dev/null
    systemctl restart salt-api.service
    systemctl restart salt-master.service
    sleep 15
    salt '*' cmd.run 'systemctl restart salt-minion' 2>/dev/null
    _ping_minions_until_all_respond "$TOTAL_NODES"
    salt '*' saltutil.sync_all 2>/dev/null
}

function _initialize_storage_profile {
    test "$STORAGE_PROFILE"
    case "$STORAGE_PROFILE" in
        default)   echo "Storage profile: bluestore OSDs (default)" ;;
        dmcrypt)   echo "Storage profile: encrypted bluestore OSDs" ;;
        filestore) echo "Storage profile: filestore OSDs"           ;;
        random)    echo "Storage profile will be chosen randomly"   ;;
        *)
            CUSTOM_STORAGE_PROFILE="$STORAGE_PROFILE"
            STORAGE_PROFILE="custom"
            echo "Storage profile: custom ($CUSTOM_STORAGE_PROFILE)"
            ;;
    esac
}

function _initialize_and_vet_nodes {
    if [ -n "$MIN_NODES" ] ; then
        echo "MIN_NODES is set to $MIN_NODES"
        PROPOSED_MIN_NODES="$MIN_NODES"
    else
        echo "MIN_NODES was not set. Default is 1"
        PROPOSED_MIN_NODES=1
    fi
    if [ -n "$CLIENT_NODES" ] ; then
        echo "CLIENT_NODES is set to $CLIENT_NODES"
    else
        echo "CLIENT_NODES was not set. Default is 0"
        CLIENT_NODES=0
    fi
    MIN_NODES=$(($CLIENT_NODES + 1))
    if [ "$PROPOSED_MIN_NODES" -lt "$MIN_NODES" ] ; then
        echo "Proposed MIN_NODES value is too low. Need at least 1 + CLIENT_NODES"
        exit 1
    fi
    test "$PROPOSED_MIN_NODES" -gt "$MIN_NODES" && MIN_NODES="$PROPOSED_MIN_NODES"
    echo "Final MIN_NODES is $MIN_NODES"
    test -n "$TOTAL_NODES" # set in _update_salt
    test "$TOTAL_NODES" -ge "$MIN_NODES"
    STORAGE_NODES=$((TOTAL_NODES - CLIENT_NODES))
    echo "WWWW"
    echo "This script will use DeepSea to deploy a cluster of $TOTAL_NODES nodes total (including Salt Master)."
    echo "Of these, $CLIENT_NODES will be clients (nodes without any DeepSea roles except \"admin\")."
}

function _zypper_ps {
    salt '*' cmd.run 'zypper ps -s' 2>/dev/null || true
}

function initialization_sequence {
    set +x
    _determine_master_minion
    _os_specific_install_deps
    _os_specific_repos_and_packages_info
    python --version || true
    python2 --version || true
    python3 --version
    deepsea --version || true
    _set_deepsea_minions
    _initialize_minion_array
    _update_salt
    cat_salt_config
    _initialize_storage_profile
    _initialize_and_vet_nodes
    set -x
}

function deploy_ceph {
    initialization_sequence
    if _ceph_cluster_running ; then
        echo "Running ceph cluster detected: skipping deploy phase"
        return 0
    fi
    test $STORAGE_NODES -lt 4 && export DEV_ENV="true"
    run_stage_0 "$CLI"
    _zypper_ps
    salt_api_test
    test -n "$RGW" -a -n "$SSL" && rgw_ssl_init
    run_stage_1 "$CLI"
    policy_cfg_base
    policy_cfg_mon_flex
    test -n "$MDS" && policy_cfg_mds
    policy_cfg_openattic_rgw_igw_ganesha
    test "$NFS_GANESHA" -a "$RGW" && rgw_demo_users
    case "$STORAGE_PROFILE" in
        dmcrypt) proposal_populate_dmcrypt ;;
        filestore) proposal_populate_filestore ;;
        random) random_or_custom_storage_profile ;;
        custom) random_or_custom_storage_profile ;;
        default) ;;
        *) echo "Bad storage profile ->$STORAGE_PROFILE<-. Bailing out!" ; exit 1 ;;
    esac
    policy_cfg_storage
    cat_policy_cfg
    run_stage_2 "$CLI"
    ceph_conf_small_cluster
    ceph_conf_mon_allow_pool_delete
    ceph_conf_dashboard
    test "$RBD" && ceph_conf_upstream_rbd_default_features
    run_stage_3 "$CLI"
    pre_create_pools
    ceph_cluster_status
    if [ -z "$MDS" -a -z "$NFS_GANESHA" -a -z "$RGW" ] ; then
        echo "WWWW"
        echo "Stages 0-3 OK, no roles requiring Stage 4: deploy phase complete!"
        return 0
    fi
    test -n "$NFS_GANESHA" && nfs_ganesha_no_root_squash
    run_stage_4 "$CLI"
    if [ -n "$NFS_GANESHA" ] ; then
        nfs_ganesha_cat_config_file
        nfs_ganesha_debug_log
        echo "WWWW"
        echo "NFS-Ganesha set to debug logging"
    fi
    ceph_cluster_status
    _zypper_ps
    return 0
}
