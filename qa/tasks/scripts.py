from util import (
    remote_run_script_as_root,
    )

proposals_dir = "/srv/pillar/ceph/proposals"


class Scripts:

    script_dict = {
        "ceph_cluster_status": """# Display ceph cluster status
set -ex
ceph pg stat -f json-pretty
ceph health detail -f json-pretty
ceph osd tree
ceph osd pool ls detail -f json-pretty
ceph -s
""",
        "create_all_pools_at_once": """# Pre-create Stage 4 pools
# with calculated number of PGs so we don't get health warnings
# during/after Stage 4 due to "too few" or "too many" PGs per OSD
set -ex
#
function json_total_osds {
    # total number of OSDs in the cluster
    ceph osd ls --format json | jq '. | length'
}
#
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
#
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
#
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
#
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
""",
        "salt_api_test": """# Salt API test script
set -e
TMPFILE=$(mktemp)
curl --silent http://$(hostname):8000/ | tee $TMPFILE # show curl output in log
test -s $TMPFILE
jq . $TMPFILE >/dev/null
echo -en "\\n" # this is just for log readability
rm $TMPFILE
echo "Salt API test passed"
""",
        "rgw_init": """# Set up RGW
set -ex
USERSYML=/srv/salt/ceph/rgw/users/users.d/rgw.yml
cat <<EOF > $USERSYML
- { uid: "demo", name: "Demo", email: "demo@demo.nil" }
- { uid: "demo1", name: "Demo1", email: "demo1@demo.nil" }
EOF
cat $USERSYML
""",
        "rgw_init_ssl": """# Set up RGW-over-SSL
set -ex
CERTDIR=/srv/salt/ceph/rgw/cert
mkdir -p $CERTDIR
pushd $CERTDIR
openssl req -x509 \
        -nodes \
        -days 1095 \
        -newkey rsa:4096 \
        -keyout rgw.key \
        -out rgw.crt \
        -subj "/C=DE"
cat rgw.key > rgw.pem && cat rgw.crt >> rgw.pem
popd
GLOBALYML=/srv/pillar/ceph/stack/global.yml
cat <<EOF >> $GLOBALYML
rgw_init: default-ssl
EOF
cat $GLOBALYML
cp /srv/salt/ceph/configuration/files/rgw-ssl.conf \
    /srv/salt/ceph/configuration/files/ceph.conf.d/rgw.conf
""",
        "proposals_remove_storage_only_node": """# remove first storage-only node from proposals
set -ex
PROPOSALSDIR=$1
STORAGE_PROFILE=$2
NODE_TO_DELETE=$3
#
echo "Before"
ls -1 $PROPOSALSDIR/profile-$STORAGE_PROFILE/cluster/
ls -1 $PROPOSALSDIR/profile-$STORAGE_PROFILE/stack/default/ceph/minions/
#
basedirsls=$PROPOSALSDIR/profile-$STORAGE_PROFILE/cluster
basediryml=$PROPOSALSDIR/profile-$STORAGE_PROFILE/stack/default/ceph/minions
mv $basedirsls/${NODE_TO_DELETE}.sls $basedirsls/${NODE_TO_DELETE}.sls-DISABLED
mv $basediryml/${NODE_TO_DELETE}.yml $basedirsls/${NODE_TO_DELETE}.yml-DISABLED
#
echo "After"
ls -1 $PROPOSALSDIR/profile-$STORAGE_PROFILE/cluster/
ls -1 $PROPOSALSDIR/profile-$STORAGE_PROFILE/stack/default/ceph/minions/
""",
        "remove_storage_only_node": """# actually remove the node
set -ex
CLI=$1
function number_of_hosts_in_ceph_osd_tree {
    ceph osd tree -f json-pretty | jq '[.nodes[] | select(.type == "host")] | length'
}
#
function number_of_osds_in_ceph_osd_tree {
    ceph osd tree -f json-pretty | jq '[.nodes[] | select(.type == "osd")] | length'
}
#
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
#
STORAGE_NODES_BEFORE=$(number_of_hosts_in_ceph_osd_tree)
OSDS_BEFORE=$(number_of_osds_in_ceph_osd_tree)
test "$STORAGE_NODES_BEFORE"
test "$OSDS_BEFORE"
test "$STORAGE_NODES_BEFORE" -gt 1
test "$OSDS_BEFORE" -gt 0
#
run_stage 2
ceph -s
run_stage 5
ceph -s
#
STORAGE_NODES_AFTER=$(number_of_hosts_in_ceph_osd_tree)
OSDS_AFTER=$(number_of_osds_in_ceph_osd_tree)
test "$STORAGE_NODES_BEFORE"
test "$OSDS_BEFORE"
test "$STORAGE_NODES_AFTER" -eq "$((STORAGE_NODES_BEFORE - 1))"
test "$OSDS_AFTER" -lt "$OSDS_BEFORE"
""",
        "custom_storage_profile": """# custom storage profile
set -ex
PROPOSALSDIR=$1
SOURCEFILE=$2
#
function _initialize_minion_configs_array {
    local DIR=$1

    shopt -s nullglob
    pushd $DIR >/dev/null
    MINION_CONFIGS_ARRAY=(*.yaml *.yml)
    echo "Made global array containing the following files (from ->$DIR<-):"
    printf '%s\n' "${MINION_CONFIGS_ARRAY[@]}"
    popd >/dev/null
    shopt -u nullglob
}
#
test -f "$SOURCEFILE"
file $SOURCEFILE
#
# prepare new profile, which will be exactly the same as the default
# profile except the files in stack/default/ceph/minions/ will be
# overwritten with our chosen OSD configuration
#
cp -a $PROPOSALSDIR/profile-default $PROPOSALSDIR/profile-custom
DESTDIR="$PROPOSALSDIR/profile-custom/stack/default/ceph/minions"
_initialize_minion_configs_array $DESTDIR
for DESTFILE in "${MINION_CONFIGS_ARRAY[@]}" ; do
    cp $SOURCEFILE $DESTDIR/$DESTFILE
done
echo "Your custom storage profile $SOURCEFILE has the following contents:"
cat $DESTDIR/$DESTFILE
ls -lR $PROPOSALSDIR/profile-custom
""",
        "mgr_dashboard_module_smoke": """# smoke test the MGR dashbaord module
set -ex
URL=$(ceph mgr services 2>/dev/null | jq .dashboard | sed -e 's/"//g')
curl --insecure --silent $URL 2>&1 > dashboard.html
test -s dashboard.html
file dashboard.html | grep "HTML document"
echo "OK" >/dev/null
""",
        "ceph_version_sanity": """# Ceph version sanity test
# test that ceph RPM version matches "ceph --version"
# for a loose definition of "matches"
set -ex
rpm -q ceph
RPM_NAME=$(rpm -q ceph)
RPM_CEPH_VERSION=$(perl -e '"'"$RPM_NAME"'" =~ m/ceph-(\d+\.\d+\.\d+)/; print "$1\n";')
echo "According to RPM, the ceph upstream version is ->$RPM_CEPH_VERSION<-" >/dev/null
test -n "$RPM_CEPH_VERSION"
ceph --version
BUFFER=$(ceph --version)
CEPH_CEPH_VERSION=$(perl -e '"'"$BUFFER"'" =~ m/ceph version (\d+\.\d+\.\d+)/; print "$1\n";')
echo "According to \"ceph --version\", the ceph upstream version is ->$CEPH_CEPH_VERSION<-" \
    >/dev/null
test -n "$RPM_CEPH_VERSION"
test "$RPM_CEPH_VERSION" = "$CEPH_CEPH_VERSION"
""",
        "rados_write_test": """Write a RADOS object and read it back
#
# NOTE: function assumes the pool "write_test" already exists. Pool can be
# created by calling e.g. "create_all_pools_at_once write_test" immediately
# before calling this function.
#
set -ex
ceph osd pool application enable write_test deepsea_qa
echo "dummy_content" > verify.txt
rados -p write_test put test_object verify.txt
rados -p write_test get test_object verify_returned.txt
test "x$(cat verify.txt)" = "x$(cat verify_returned.txt)"
""",
        "mgr_plugin_influx": """# Influx MGR plugin smoke test
set -ex
influx -version
systemctl start influxdb.service
sleep 5
systemctl status --full --lines=0 influxdb.service
influx -execute 'create database ceph'
influx -database ceph \
    -execute "create user admin with password 'badpassword' with all privileges"
ceph mgr module enable influx
ceph influx config-set hostname $(hostname)
ceph influx config-set port 8086
ceph influx config-set username admin
ceph influx config-set password badpassword
ceph influx config-set database ceph
ceph influx config-set ssl false
ceph influx config-set verify_ssl false
ceph influx send
sleep 5
ceph -s
sleep 5
influx -database ceph -execute 'show series' | head -n1 | grep key
influx -database ceph -execute 'select * from ceph_daemon_stats limit 1' | \
    head -n1 | grep ceph_daemon_stats
echo "OK" >/dev/null
""",
        }

    def __init__(self, master_remote, logger):
        self.log = logger
        self.master_remote = master_remote

    def ceph_cluster_status(self, *args, **kwargs):
        remote_run_script_as_root(
            self.master_remote,
            'ceph_cluster_status.sh',
            self.script_dict["ceph_cluster_status"],
            )

    def ceph_version_sanity(self, *args, **kwargs):
        remote_run_script_as_root(
            self.master_remote,
            'ceph_version_sanity.sh',
            self.script_dict["ceph_version_sanity"],
            )

    def create_all_pools_at_once(self, *args, **kwargs):
        self.log.info("creating pools: {}".format(' '.join(args)))
        remote_run_script_as_root(
            self.master_remote,
            'create_all_pools_at_once.sh',
            self.script_dict["create_all_pools_at_once"],
            args=args,
            )

    def custom_storage_profile(self, *args, **kwargs):
        sourcefile = args[0]
        remote_run_script_as_root(
            self.master_remote,
            'custom_storage_profile.sh',
            self.script_dict["custom_storage_profile"],
            args=[proposals_dir, sourcefile],
            )

    def mgr_dashboard_module_smoke(self, *args, **kwargs):
        remote_run_script_as_root(
            self.master_remote,
            'mgr_dashboard_module_smoke.sh',
            self.script_dict["mgr_dashboard_module_smoke"],
            )

    def mgr_plugin_influx(self, *args, **kwargs):
        remote_run_script_as_root(
            self.master_remote,
            'mgr_plugin_influx.sh',
            self.script_dict["mgr_plugin_influx"],
            )

    def proposals_remove_storage_only_node(self, *args, **kwargs):
        hostname = args[0]
        storage_profile = args[1]
        remote_run_script_as_root(
            self.master_remote,
            'proposals_remove_storage_only_node.sh',
            self.script_dict["proposals_remove_storage_only_node"],
            args=[proposals_dir, storage_profile, hostname],
            )

    def rados_write_test(self, *args, **kwargs):
        remote_run_script_as_root(
            self.master_remote,
            'rados_write_test.sh',
            self.script_dict["rados_write_test"],
            )

    def remove_storage_only_node(self, *args, **kwargs):
        args = ['--cli'] if kwargs['cli'] else []
        remote_run_script_as_root(
            self.master_remote,
            'remove_storage_only_node.sh',
            self.script_dict["remove_storage_only_node"],
            args=args,
            )

    def rgw_init(self, *args, **kwargs):
        remote_run_script_as_root(
            self.master_remote,
            'rgw_init.sh',
            self.script_dict["rgw_init"],
            )

    def rgw_init_ssl(self, *args, **kwargs):
        remote_run_script_as_root(
            self.master_remote,
            'rgw_init_ssl.sh',
            self.script_dict["rgw_init_ssl"],
            )

    def salt_api_test(self, *args, **kwargs):
        remote_run_script_as_root(
            self.master_remote,
            'salt_api_test.sh',
            self.script_dict["salt_api_test"],
            )
