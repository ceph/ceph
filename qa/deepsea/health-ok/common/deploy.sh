# This file is part of the DeepSea integration test suite

#
# separate file to house the deploy_ceph function
#

DEPLOY_PHASE_COMPLETE_MESSAGE="deploy phase complete!"


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
    MASTER_MINION=$(hostname --fqdn)
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
    local m=
    local i=0
    if type salt-key > /dev/null 2>&1; then
        MINION_LIST=$(salt-key -L -l acc | grep -v '^Accepted Keys')
        for m in $MINION_LIST ; do
            MINION_ARRAY[0]=$m
            i=$((i+1))
        done
    else
        echo "Cannot find salt-key. Is Salt installed? Is this running on the Salt Master?"
        exit 1
    fi
    echo $i
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
    echo "TOTAL_NODES is $TOTAL_NODES"
    test "$TOTAL_NODES"
    test "$TOTAL_NODES" -ge "$MIN_NODES"
    STORAGE_NODES=$((TOTAL_NODES - CLIENT_NODES))
    echo "WWWW"
    echo "This script will use DeepSea to deploy a cluster of $TOTAL_NODES nodes total (including Salt Master)."
    echo "Of these, $CLIENT_NODES will be clients (nodes without any DeepSea roles except \"admin\")."
}

function _zypper_ps {
    salt '*' cmd.run 'zypper ps -s' 2>/dev/null || true
}

function _python_versions {
    type python2 > /dev/null 2>&1 && python2 --version || echo "Python 2 not installed"
    type python3 > /dev/null 2>&1 && python3 --version || echo "Python 3 not installed"
}

function initialization_sequence {
    set +x
    _determine_master_minion
    _os_specific_install_deps
    _os_specific_repos_and_packages_info
    set +e
    _python_versions
    type deepsea > /dev/null 2>&1 && deepsea --version || echo "deepsea CLI not installed"
    TOTAL_MINIONS=$(_initialize_minion_array)
    echo "There are $TOTAL_MINIONS minions in this Salt cluster"
    set -e
    _set_deepsea_minions
    salt '*' saltutil.sync_all 2>/dev/null
    TOTAL_NODES=$(json_total_nodes)
    test "$TOTAL_NODES" = "$TOTAL_MINIONS"
    _ping_minions_until_all_respond
    cat_salt_config
    _initialize_storage_profile
    _initialize_and_vet_nodes
    set -x
}

function salt_api_test {
    local tmpfile=$(mktemp)
    echo "Salt API test: BEGIN"
    systemctl --no-pager --full status salt-api.service
    curl http://$(hostname):8000/ | tee $tmpfile # show curl output in log
    test -s $tmpfile
    jq . $tmpfile >/dev/null
    echo -en "\n" # this is just for log readability
    rm $tmpfile
    echo "Salt API test: END"
}

function deploy_ceph {
    if [ "$START_STAGE" -lt "0" -o "$START_STAGE" -gt "4" ] ; then
        echo "Received bad --start-stage value ->$START_STAGE<- (must be 0-4 inclusive)"
	exit 1
    fi
    if _ceph_cluster_running ; then
        echo "Running ceph cluster detected: skipping deploy phase"
        return 0
    fi
    if [ "$START_STAGE" = "0" ] ; then
        if [ -z "$TEUTHOLOGY" ] ; then
            initialization_sequence
            test $STORAGE_NODES -lt 4 && export DEV_ENV="true"
        fi
        run_stage_0 "$CLI"
        _zypper_ps
        salt_api_test
        test -n "$RGW" -a -n "$SSL" && rgw_ssl_init
    fi
    if [ "$TEUTHOLOGY" ] ; then
        echo "Development work in progress: finishing early!"
        exit 0
    fi
    if [ "$START_STAGE" -le "1" ] ; then
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
    fi
    if [ "$START_STAGE" -le "2" ] ; then
        run_stage_2 "$CLI"
        ceph_conf_small_cluster
        ceph_conf_mon_allow_pool_delete
        ceph_conf_dashboard
        test "$RBD" && ceph_conf_upstream_rbd_default_features
    fi
    if [ "$START_STAGE" -le "3" ] ; then
        run_stage_3 "$CLI"
        pre_create_pools
        ceph_cluster_status
        test "$RBD" && ceph_test_librbd_can_be_run
        if [ -z "$MDS" -a -z "$NFS_GANESHA" -a -z "$RGW" ] ; then
            echo "WWWW"
            echo "Stage 3 OK, no roles requiring Stage 4: $DEPLOY_PHASE_COMPLETE_MESSAGE"
            return 0
        fi
        test -n "$NFS_GANESHA" && nfs_ganesha_no_root_squash
    fi
    if [ "$START_STAGE" -le "4" ] ; then
        run_stage_4 "$CLI"
        if [ -n "$NFS_GANESHA" ] ; then
            nfs_ganesha_cat_config_file
            nfs_ganesha_debug_log
            echo "WWWW"
            echo "NFS-Ganesha set to debug logging"
        fi
        ceph_cluster_status
        _zypper_ps
        echo "Stage 4 OK: $DEPLOY_PHASE_COMPLETE_MESSAGE"
    fi
    return 0
}
