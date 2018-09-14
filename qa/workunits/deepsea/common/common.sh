#
# This file is part of the DeepSea integration test suite
#

# BASEDIR is set by the calling script
source $BASEDIR/common/deploy.sh
source $BASEDIR/common/helper.sh
source $BASEDIR/common/json.sh
source $BASEDIR/common/nfs-ganesha.sh
source $BASEDIR/common/policy.sh
source $BASEDIR/common/pool.sh
source $BASEDIR/common/rbd.sh
source $BASEDIR/common/rgw.sh
source $BASEDIR/common/zypper.sh


#
# functions that process command-line arguments
#

function assert_enhanced_getopt {
    set +e
    echo -n "Running 'getopt --test'... "
    getopt --test > /dev/null
    if [ $? -ne 4 ]; then
        echo "FAIL"
        echo "This script requires enhanced getopt. Bailing out."
        exit 1
    fi
    echo "PASS"
    set -e
}


#
# functions that run the DeepSea stages
#

function _disable_update_in_stage_0 {
    cp /srv/salt/ceph/stage/prep/master/default.sls /srv/salt/ceph/stage/prep/master/default-orig.sls
    cp /srv/salt/ceph/stage/prep/master/default-no-update-no-reboot.sls /srv/salt/ceph/stage/prep/master/default.sls
    cp /srv/salt/ceph/stage/prep/minion/default.sls /srv/salt/ceph/stage/prep/minion/default-orig.sls
    cp /srv/salt/ceph/stage/prep/minion/default-no-update-no-reboot.sls /srv/salt/ceph/stage/prep/minion/default.sls
}

function run_stage_0 {
    test "$NO_UPDATE" && _disable_update_in_stage_0
    _run_stage 0 "$@"
    if _root_fs_is_btrfs ; then
        echo "Root filesystem is btrfs: creating subvolumes for /var/lib/ceph"
        salt-run state.orch ceph.migrate.subvolume
    else
        echo "Root filesystem is *not* btrfs: skipping subvolume creation"
    fi
    test "$STAGE_SUCCEEDED"
}

function run_stage_1 {
    _run_stage 1 "$@"
    test "$STAGE_SUCCEEDED"
}

function run_stage_2 {
    # This was needed with SCC repos
    #salt '*' cmd.run "for delay in 60 60 60 60 ; do sudo zypper --non-interactive --gpg-auto-import-keys refresh && break ; sleep $delay ; done"
    _run_stage 2 "$@"
    salt_pillar_items 2>/dev/null
    test "$STAGE_SUCCEEDED"
}

function _disable_tuned {
    local prefix=/srv/salt/ceph/tuned
    mv $prefix/mgr/default.sls $prefix/mgr/default.sls-MOVED
    mv $prefix/mon/default.sls $prefix/mon/default.sls-MOVED
    mv $prefix/osd/default.sls $prefix/osd/default.sls-MOVED
    mv $prefix/mgr/default-off.sls $prefix/mgr/default.sls
    mv $prefix/mon/default-off.sls $prefix/mon/default.sls
    mv $prefix/osd/default-off.sls $prefix/osd/default.sls
}

function run_stage_3 {
    cat_global_conf
    lsblk_on_storage_node
    if [ "$TUNED" ] ; then
        echo "WWWW: tuned will be deployed as usual"
    else
        echo "WWWW: tuned will NOT be deployed"
        _disable_tuned
    fi
    _run_stage 3 "$@"
    ceph_disk_list_on_storage_node
    ceph osd tree
    cat_ceph_conf
    admin_auth_status
    test "$STAGE_SUCCEEDED"
}

function run_stage_4 {
    _run_stage 4 "$@"
    test "$STAGE_SUCCEEDED"
}

function run_stage_5 {
    _run_stage 5 "$@"
    test "$STAGE_SUCCEEDED"
}


#
# functions that generate /etc/ceph/ceph.conf
# see https://github.com/SUSE/DeepSea/tree/master/srv/salt/ceph/configuration/files/ceph.conf.d
#

function change_rgw_conf {
    cat <<'EOF' >> /srv/salt/ceph/configuration/files/ceph.conf.d/rgw.conf
foo = bar
EOF
}

function change_osd_conf {
    cat <<'EOF' >> /srv/salt/ceph/configuration/files/ceph.conf.d/osd.conf
foo = bar
EOF
}

function change_mon_conf {
    cat <<'EOF' >> /srv/salt/ceph/configuration/files/ceph.conf.d/mon.conf
foo = bar
EOF
}

function ceph_conf_small_cluster {
    local STORAGENODES=$(json_storage_nodes)
    test -n "$STORAGENODES"
    if [ "$STORAGENODES" -eq 1 ] ; then
        echo "Adjusting ceph.conf for operation with 1 storage node"
        cat <<'EOF' >> /srv/salt/ceph/configuration/files/ceph.conf.d/global.conf
mon pg warn min per osd = 16
osd pool default size = 2
osd crush chooseleaf type = 0 # failure domain == osd
EOF
    elif [ "$STORAGENODES" -eq 2 -o "$STORAGENODES" -eq 3 ] ; then
        echo "Adjusting ceph.conf for operation with 2 or 3 storage nodes"
        cat <<'EOF' >> /srv/salt/ceph/configuration/files/ceph.conf.d/global.conf
mon pg warn min per osd = 8
osd pool default size = 2
EOF
    else
        echo "Four or more storage nodes; not adjusting ceph.conf"
    fi
}

function ceph_conf_mon_allow_pool_delete {
    echo "Adjusting ceph.conf to allow pool deletes"
    cat <<'EOF' >> /srv/salt/ceph/configuration/files/ceph.conf.d/global.conf
mon allow pool delete = true
EOF
}

function ceph_conf_dashboard {
    echo "Adjusting ceph.conf for deployment of dashboard MGR module"
    cat <<'EOF' >> /srv/salt/ceph/configuration/files/ceph.conf.d/mon.conf
mgr initial modules = dashboard
EOF
}


#
# functions that print status information
#

function cat_deepsea_log {
    cat /var/log/deepsea.log
}

function cat_salt_config {
    cat /etc/salt/master
    cat /etc/salt/minion
}

function cat_policy_cfg {
    cat /srv/pillar/ceph/proposals/policy.cfg
}

function salt_pillar_items {
    salt '*' pillar.items
}

function salt_pillar_get_roles {
    salt '*' pillar.get roles
}

function salt_cmd_run_lsblk {
    salt '*' cmd.run lsblk
}

function cat_global_conf {
    cat /srv/salt/ceph/configuration/files/ceph.conf.d/global.conf || true
}

function cat_ceph_conf {
    salt '*' cmd.run "cat /etc/ceph/ceph.conf" 2>/dev/null
}

function admin_auth_status {
    ceph auth get client.admin
    ls -l /etc/ceph/ceph.client.admin.keyring
    cat /etc/ceph/ceph.client.admin.keyring
}

function number_of_hosts_in_ceph_osd_tree {
    ceph osd tree -f json-pretty | jq '[.nodes[] | select(.type == "host")] | length'
}

function number_of_osds_in_ceph_osd_tree {
    ceph osd tree -f json-pretty | jq '[.nodes[] | select(.type == "osd")] | length'
}

function ceph_cluster_status {
    ceph pg stat -f json-pretty
    _grace_period 1
    ceph health detail -f json-pretty
    _grace_period 1
    ceph osd tree
    _grace_period 1
    ceph osd pool ls detail -f json-pretty
    _grace_period 1
    ceph -s
}

function ceph_log_grep_enoent_eaccess {
    set +e
    grep -rH "Permission denied" /var/log/ceph
    grep -rH "No such file or directory" /var/log/ceph
    set -e
}


#
# core validation tests
#

function ceph_version_test {
# test that ceph RPM version matches "ceph --version"
# for a loose definition of "matches"
    rpm -q ceph
    local RPM_NAME=$(rpm -q ceph)
    local RPM_CEPH_VERSION=$(perl -e '"'"$RPM_NAME"'" =~ m/ceph-(\d+\.\d+\.\d+)/; print "$1\n";')
    echo "According to RPM, the ceph upstream version is ->$RPM_CEPH_VERSION<-"
    test -n "$RPM_CEPH_VERSION"
    ceph --version
    local BUFFER=$(ceph --version)
    local CEPH_CEPH_VERSION=$(perl -e '"'"$BUFFER"'" =~ m/ceph version (\d+\.\d+\.\d+)/; print "$1\n";')
    echo "According to \"ceph --version\", the ceph upstream version is ->$CEPH_CEPH_VERSION<-"
    test -n "$RPM_CEPH_VERSION"
    test "$RPM_CEPH_VERSION" = "$CEPH_CEPH_VERSION"
}

function ceph_health_test {
    local LOGFILE=/tmp/ceph_health_test.log
    echo "Waiting up to 15 minutes for HEALTH_OK..."
    salt -C 'I@roles:master' wait.until status=HEALTH_OK timeout=900 check=1 2>/dev/null | tee $LOGFILE
    # last line: determines return value of function
    ! grep -q 'Timeout expired' $LOGFILE
}

function rados_write_test {
    #
    # NOTE: function assumes the pool "write_test" already exists. Pool can be
    # created by calling e.g. "create_all_pools_at_once write_test" immediately
    # before calling this function.
    #
    ceph osd pool application enable write_test deepsea_qa
    echo "dummy_content" > verify.txt
    rados -p write_test put test_object verify.txt
    rados -p write_test get test_object verify_returned.txt
    test "x$(cat verify.txt)" = "x$(cat verify_returned.txt)"
}

function lsblk_on_storage_node {
    local TESTSCRIPT=/tmp/lsblk_test.sh
    local STORAGENODE=$(_first_x_node storage)
    cat << 'EOF' > $TESTSCRIPT
set -ex
trap 'echo "Result: NOT_OK"' ERR
echo "running lsblk as $(whoami) on $(hostname --fqdn)"
lsblk
echo "Result: OK"
EOF
    _run_test_script_on_node $TESTSCRIPT $STORAGENODE
}

function ceph_disk_list_on_storage_node {
    local TESTSCRIPT=/tmp/ceph_disk_list_test.sh
    local STORAGENODE=$(_first_x_node storage)
    cat << 'EOF' > $TESTSCRIPT
set -ex
trap 'echo "Result: NOT_OK"' ERR
echo "running lsblk and ceph-disk list as $(whoami) on $(hostname --fqdn)"
lsblk
ceph-disk list
echo "Result: OK"
EOF
    _run_test_script_on_node $TESTSCRIPT $STORAGENODE
}

function cephfs_mount_and_sanity_test {
    #
    # run cephfs mount test script on the client node
    # mounts cephfs in /mnt, touches a file, asserts that it exists
    #
    local TESTSCRIPT=/tmp/cephfs_test.sh
    local CLIENTNODE=$(_client_node)
    cat << 'EOF' > $TESTSCRIPT
set -ex
trap 'echo "Result: NOT_OK"' ERR
echo "cephfs mount test script running as $(whoami) on $(hostname --fqdn)"
TESTMONS=$(ceph-conf --lookup 'mon_initial_members' | tr -d '[:space:]')
TESTSECR=$(grep 'key =' /etc/ceph/ceph.client.admin.keyring | awk '{print $NF}')
echo "MONs: $TESTMONS"
echo "admin secret: $TESTSECR"
test -d /mnt
mount -t ceph ${TESTMONS}:/ /mnt -o name=admin,secret="$TESTSECR"
touch /mnt/bubba
test -f /mnt/bubba
umount /mnt
echo "Result: OK"
EOF
    # FIXME: assert no MDS running on $CLIENTNODE
    _run_test_script_on_node $TESTSCRIPT $CLIENTNODE
}

function iscsi_kludge {
    #
    # apply kludge to work around bsc#1049669
    #
    local TESTSCRIPT=/tmp/iscsi_kludge.sh
    local IGWNODE=$(_first_x_node igw)
    cat << 'EOF' > $TESTSCRIPT
set -ex
trap 'echo "Result: NOT_OK"' ERR
echo "igw kludge script running as $(whoami) on $(hostname --fqdn)"
sed -i -e 's/\("host": "target[[:digit:]]\+\)"/\1.teuthology"/' /tmp/lrbd.conf
cat /tmp/lrbd.conf
source /etc/sysconfig/lrbd; lrbd -v $LRBD_OPTIONS -f /tmp/lrbd.conf
systemctl restart lrbd.service
systemctl --no-pager --full status lrbd.service
echo "Result: OK"
EOF
    _run_test_script_on_node $TESTSCRIPT $IGWNODE
}

function igw_info {
    #
    # peek at igw information on the igw node
    #
    local TESTSCRIPT=/tmp/igw_info.sh
    local IGWNODE=$(_first_x_node igw)
    cat << 'EOF' > $TESTSCRIPT
set -ex
trap 'echo "Result: NOT_OK"' ERR
echo "igw info script running as $(whoami) on $(hostname --fqdn)"
rpm -q lrbd || true
lrbd --output || true
ls -lR /sys/kernel/config/target/ || true
ss --tcp --numeric state listening
echo "See 3260 there?"
echo "Result: OK"
EOF
    _run_test_script_on_node $TESTSCRIPT $IGWNODE
}

function iscsi_mount_and_sanity_test {
    #
    # run iscsi mount test script on the client node
    # mounts iscsi in /mnt, touches a file, asserts that it exists
    #
    local TESTSCRIPT=/tmp/iscsi_test.sh
    local CLIENTNODE=$(_client_node)
    local IGWNODE=$(_first_x_node igw)
    cat << EOF > $TESTSCRIPT
set -e
trap 'echo "Result: NOT_OK"' ERR
for delay in 60 60 60 60 ; do
    sudo zypper --non-interactive --gpg-auto-import-keys refresh && break
    sleep $delay
done
set -x
zypper --non-interactive install --no-recommends open-iscsi multipath-tools
systemctl start iscsid.service
sleep 5
systemctl --no-pager --full status iscsid.service
iscsiadm -m discovery -t st -p $IGWNODE
iscsiadm -m node -L all
systemctl start multipathd.service
sleep 5
systemctl --no-pager --full status multipathd.service
ls -lR /dev/mapper
ls -l /dev/disk/by-path
ls -l /dev/disk/by-*id
multipath -ll
mkfs -t xfs /dev/dm-0
test -d /mnt
mount /dev/dm-0 /mnt
df -h /mnt
touch /mnt/bubba
test -f /mnt/bubba
umount /mnt
echo "Result: OK"
EOF
    # FIXME: assert script not running on the iSCSI gateway node
    _run_test_script_on_node $TESTSCRIPT $CLIENTNODE
}

function validate_rgw_cert_perm {
    local TESTSCRIPT=/tmp/test_rados_put.sh
    local RGWNODE=$(_first_x_node rgw-ssl)
    cat << 'EOF' > $TESTSCRIPT
set -ex
trap 'echo "Result: NOT_OK"' ERR
RGW_PEM=/etc/ceph/rgw.pem
test -f "$RGW_PEM"
test "$(stat -c'%U' $RGW_PEM)" == "ceph"
test "$(stat -c'%G' $RGW_PEM)" == "ceph"
test "$(stat -c'%a' $RGW_PEM)" -eq 600
echo "Result: OK"
EOF
    _run_test_script_on_node $TESTSCRIPT $RGWNODE
}

function test_systemd_ceph_osd_target_wants {
    #
    # see bsc#1051598 in which ceph-disk was omitting --runtime when it enabled
    # ceph-osd@$ID.service units
    #
    local TESTSCRIPT=/tmp/test_systemd_ceph_osd_target_wants.sh
    local STORAGENODE=$(_first_x_node storage)
    cat << 'EOF' > $TESTSCRIPT
set -x
CEPH_OSD_WANTS="/systemd/system/ceph-osd.target.wants"
ETC_CEPH_OSD_WANTS="/etc$CEPH_OSD_WANTS"
RUN_CEPH_OSD_WANTS="/run$CEPH_OSD_WANTS"
ls -l $ETC_CEPH_OSD_WANTS
ls -l $RUN_CEPH_OSD_WANTS
set -e
trap 'echo "Result: NOT_OK"' ERR
echo "Asserting that there is no directory $ETC_CEPH_OSD_WANTS"
test -d "$ETC_CEPH_OSD_WANTS" && false
echo "Asserting that $RUN_CEPH_OSD_WANTS exists, is a directory, and is not empty"
test -d "$RUN_CEPH_OSD_WANTS"
test -n "$(ls --almost-all $RUN_CEPH_OSD_WANTS)"
echo "Result: OK"
EOF
    _run_test_script_on_node $TESTSCRIPT $STORAGENODE
}

function ceph_disk_list {
    local TESTSCRIPT=/tmp/ceph_disk_list.sh
    local STORAGENODE=$(_first_x_node storage)
    cat << 'EOF' > $TESTSCRIPT
set -x
ceph-disk list
echo "Result: OK"
EOF
    _run_test_script_on_node $TESTSCRIPT $STORAGENODE
}

function configure_all_OSDs_to_filestore {
    salt-run proposal.populate format=filestore name=filestore 2>/dev/null
    chown salt:salt /srv/pillar/ceph/proposals/policy.cfg
    sed -i 's/profile-default/profile-filestore/g' /srv/pillar/ceph/proposals/policy.cfg
}

function verify_OSD_type {
    # checking with 'ceph osd metadata' command
    # 1st input argument: type 'filestore' or 'bluestore'
    # 2nd input argument: OSD ID 
    osd_type=$(ceph osd metadata $2 -f json-pretty | jq '.osd_objectstore')
    if [[ $osd_type != \"$1\" ]]
        then 
        echo "Error: Object store type is not $1 for OSD.ID : $2"
        exit 1
    else
        echo OSD.${2} $osd_type
    fi
}

function check_OSD_type {  
    # expecting as argument 'filestore' or 'bluestore' 
    for i in $(ceph osd ls);do verify_OSD_type $1 $i;done
}

function migrate_to_bluestore {
    salt-run state.orch ceph.migrate.policy 2>/dev/null
    sed -i 's/profile-filestore/migrated-profile-filestore/g' /srv/pillar/ceph/proposals/policy.cfg
    salt-run disengage.safety 2>/dev/null
    salt-run state.orch ceph.migrate.osds 2>/dev/null
}
