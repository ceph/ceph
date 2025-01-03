#!/bin/bash -ex

SCRIPT_NAME=$(basename ${BASH_SOURCE[0]})
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# cleanup during exit
[ -z "$CLEANUP" ] && CLEANUP=true

FSID='00000000-0000-0000-0000-0000deadbeef'

# images that are used
IMAGE_MAIN=${IMAGE_MAIN:-'quay.ceph.io/ceph-ci/ceph:main'}
IMAGE_REEF=${IMAGE_REEF:-'quay.ceph.io/ceph-ci/ceph:reef'}
IMAGE_SQUID=${IMAGE_SQUID:-'quay.ceph.io/ceph-ci/ceph:squid'}
IMAGE_DEFAULT=${IMAGE_MAIN}

OSD_IMAGE_NAME="${SCRIPT_NAME%.*}_osd.img"
OSD_IMAGE_SIZE='6G'
OSD_TO_CREATE=2
OSD_VG_NAME=${SCRIPT_NAME%.*}
OSD_LV_NAME=${SCRIPT_NAME%.*}

# TMPDIR for test data
[ -d "$TMPDIR" ] || TMPDIR=$(mktemp -d tmp.$SCRIPT_NAME.XXXXXX)
[ -d "$TMPDIR_TEST_MULTIPLE_MOUNTS" ] || TMPDIR_TEST_MULTIPLE_MOUNTS=$(mktemp -d tmp.$SCRIPT_NAME.XXXXXX)

CEPHADM_SRC_DIR=${SCRIPT_DIR}/../../../src/cephadm
CEPHADM_SAMPLES_DIR=${CEPHADM_SRC_DIR}/samples

[ -z "$SUDO" ] && SUDO=sudo

# If cephadm is already installed on the system, use that one, avoid building
# # one if we can.
if [ -z "$CEPHADM" ] && command -v cephadm >/dev/null ; then
    CEPHADM="$(command -v cephadm)"
fi

if [ -z "$CEPHADM" ]; then
    CEPHADM=`mktemp -p $TMPDIR tmp.cephadm.XXXXXX`
    ${CEPHADM_SRC_DIR}/build.sh "$CEPHADM"
    NO_BUILD_INFO=1
fi

# at this point, we need $CEPHADM set
if ! [ -x "$CEPHADM" ]; then
    echo "cephadm not found. Please set \$CEPHADM"
    exit 1
fi

# add image to args
CEPHADM_ARGS="$CEPHADM_ARGS --image $IMAGE_DEFAULT"

# combine into a single var
CEPHADM_BIN="$CEPHADM"
CEPHADM="$SUDO $CEPHADM_BIN $CEPHADM_ARGS"

# clean up previous run(s)?
$CEPHADM rm-cluster --fsid $FSID --force
$SUDO vgchange -an $OSD_VG_NAME || true
loopdev=$($SUDO losetup -a | grep $(basename $OSD_IMAGE_NAME) | awk -F : '{print $1}')
if ! [ "$loopdev" = "" ]; then
    $SUDO losetup -d $loopdev
fi

function cleanup()
{
    if [ $CLEANUP = false ]; then
        # preserve the TMPDIR state
        echo "========================"
        echo "!!! CLEANUP=$CLEANUP !!!"
        echo
        echo "TMPDIR=$TMPDIR"
        echo "========================"
        return
    fi

    dump_all_logs $FSID
    rm -rf $TMPDIR
}
trap cleanup EXIT

function expect_false()
{
        set -x
        if eval "$@"; then return 1; else return 0; fi
}

# expect_return_code $expected_code $command ...
function expect_return_code()
{
  set -x
  local expected_code="$1"
  shift
  local command="$@"

  set +e
  eval "$command"
  local return_code="$?"
  set -e

  if [ ! "$return_code" -eq "$expected_code" ]; then return 1; else return 0; fi
}

function is_available()
{
    local name="$1"
    local condition="$2"
    local tries="$3"

    local num=0
    while ! eval "$condition"; do
        num=$(($num + 1))
        if [ "$num" -ge $tries ]; then
            echo "$name is not available"
            false
        fi
        sleep 5
    done

    echo "$name is available"
    true
}

function dump_log()
{
    local fsid="$1"
    local name="$2"
    local num_lines="$3"

    if [ -z $num_lines ]; then
        num_lines=100
    fi

    echo '-------------------------'
    echo 'dump daemon log:' $name
    echo '-------------------------'

    $CEPHADM logs --fsid $fsid --name $name -- --no-pager -n $num_lines
}

function dump_all_logs()
{
    local fsid="$1"
    local names=$($CEPHADM ls | jq -r '.[] | select(.fsid == "'$fsid'").name')

    echo 'dumping logs for daemons: ' $names
    for name in $names; do
        dump_log $fsid $name
    done
}

function nfs_stop()
{
    # stop the running nfs server
    local units="nfs-server nfs-kernel-server"
    for unit in $units; do
        if systemctl --no-pager status $unit > /dev/null; then
            $SUDO systemctl stop $unit
        fi
    done

    # ensure the NFS port is no longer in use
    expect_false "$SUDO ss -tlnp '( sport = :nfs )' | grep LISTEN"
}

## prepare + check host
$SUDO $CEPHADM check-host

## run a gather-facts (output to stdout)
$SUDO $CEPHADM gather-facts

## NOTE: cephadm version is, as of around May 2023, no longer basing the
## output for `cephadm version` on the version of the containers. The version
## reported is that of the "binary" and is determined during the ceph build.
## `cephadm version` should NOT require sudo/root.
$CEPHADM_BIN version
$CEPHADM_BIN version | grep 'cephadm version'
# Typically cmake should be running the cephadm build script with CLI arguments
# that embed version info into the "binary". If not using a cephadm build via
# cmake you can set `NO_BUILD_INFO` to skip this check.
if [ -z "$NO_BUILD_INFO" ]; then
    $CEPHADM_BIN version | grep -v 'UNSET'
    $CEPHADM_BIN version | grep -v 'UNKNOWN'
fi


## test shell before bootstrap, when crash dir isn't (yet) present on this host
$CEPHADM shell --fsid $FSID -- ceph -v | grep 'ceph version'
$CEPHADM shell --fsid $FSID -e FOO=BAR -- printenv | grep FOO=BAR

# test stdin
echo foo | $CEPHADM shell -- cat | grep -q foo

# the shell commands a bit above this seems to cause the
# /var/lib/ceph/<fsid> directory to be made. Since we now
# check in bootstrap that there are no clusters with the same
# fsid based on the directory existing, we need to make sure
# this directory is gone before bootstrapping. We can
# accomplish this with another rm-cluster
$CEPHADM rm-cluster --fsid $FSID --force

## bootstrap
ORIG_CONFIG=`mktemp -p $TMPDIR`
CONFIG=`mktemp -p $TMPDIR`
MONCONFIG=`mktemp -p $TMPDIR`
KEYRING=`mktemp -p $TMPDIR`
IP=127.0.0.1
cat <<EOF > $ORIG_CONFIG
[global]
	log to file = true
        osd crush chooseleaf type = 0
EOF
$CEPHADM bootstrap \
      --mon-id a \
      --mgr-id x \
      --mon-ip $IP \
      --fsid $FSID \
      --config $ORIG_CONFIG \
      --output-config $CONFIG \
      --output-keyring $KEYRING \
      --output-pub-ssh-key $TMPDIR/ceph.pub \
      --allow-overwrite \
      --skip-mon-network \
      --skip-monitoring-stack
test -e $CONFIG
test -e $KEYRING
rm -f $ORIG_CONFIG

$SUDO test -e /var/log/ceph/$FSID/ceph-mon.a.log
$SUDO test -e /var/log/ceph/$FSID/ceph-mgr.x.log

for u in ceph.target \
	     ceph-$FSID.target \
	     ceph-$FSID@mon.a \
	     ceph-$FSID@mgr.x; do
    systemctl is-enabled $u
    systemctl is-active $u
done
systemctl | grep system-ceph | grep -q .slice  # naming is escaped and annoying

# check ceph -s works (via shell w/ passed config/keyring)
$CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
      ceph -s | grep $FSID

for t in mon mgr node-exporter prometheus grafana; do
    $CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
	     ceph orch apply $t --unmanaged
done

## ls
$CEPHADM ls | jq '.[]' | jq 'select(.name == "mon.a").fsid' \
    | grep $FSID
$CEPHADM ls | jq '.[]' | jq 'select(.name == "mgr.x").fsid' \
    | grep $FSID

# make sure the version is returned correctly
$CEPHADM ls | jq '.[]' | jq 'select(.name == "mon.a").version' | grep -q \\.

## deploy
# add mon.b
cp $CONFIG $MONCONFIG
echo "public addrv = [v2:$IP:3301,v1:$IP:6790]" >> $MONCONFIG
jq --null-input \
    --arg fsid $FSID \
    --arg name mon.b \
    --arg keyring /var/lib/ceph/$FSID/mon.a/keyring \
    --arg config "$MONCONFIG" \
    '{"fsid": $fsid, "name": $name, "params":{"keyring": $keyring, "config": $config}}' | \
    $CEPHADM _orch deploy
for u in ceph-$FSID@mon.b; do
    systemctl is-enabled $u
    systemctl is-active $u
done
cond="$CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
	    ceph mon stat | grep '2 mons'"
is_available "mon.b" "$cond" 30

# add mgr.y
$CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
      ceph auth get-or-create mgr.y \
      mon 'allow profile mgr' \
      osd 'allow *' \
      mds 'allow *' > $TMPDIR/keyring.mgr.y
jq --null-input \
    --arg fsid $FSID \
    --arg name mgr.y \
    --arg keyring $TMPDIR/keyring.mgr.y \
    --arg config "$CONFIG" \
    '{"fsid": $fsid, "name": $name, "params":{"keyring": $keyring, "config": $config}}' | \
    $CEPHADM _orch deploy
for u in ceph-$FSID@mgr.y; do
    systemctl is-enabled $u
    systemctl is-active $u
done

for f in `seq 1 30`; do
    if $CEPHADM shell --fsid $FSID \
	     --config $CONFIG --keyring $KEYRING -- \
	  ceph -s -f json-pretty \
	| jq '.mgrmap.num_standbys' | grep -q 1 ; then break; fi
    sleep 1
done
$CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
      ceph -s -f json-pretty \
    | jq '.mgrmap.num_standbys' | grep -q 1

# add osd.{1,2,..}
dd if=/dev/zero of=$TMPDIR/$OSD_IMAGE_NAME bs=1 count=0 seek=$OSD_IMAGE_SIZE
loop_dev=$($SUDO losetup -f)
$SUDO vgremove -f $OSD_VG_NAME || true
$SUDO losetup $loop_dev $TMPDIR/$OSD_IMAGE_NAME
$SUDO pvcreate $loop_dev && $SUDO vgcreate $OSD_VG_NAME $loop_dev

# osd bootstrap keyring
$CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
      ceph auth get client.bootstrap-osd > $TMPDIR/keyring.bootstrap.osd

# create lvs first so ceph-volume doesn't overlap with lv creation
for id in `seq 0 $((--OSD_TO_CREATE))`; do
    $SUDO lvcreate -l $((100/$OSD_TO_CREATE))%VG -n $OSD_LV_NAME.$id $OSD_VG_NAME
done

for id in `seq 0 $((--OSD_TO_CREATE))`; do
    device_name=/dev/$OSD_VG_NAME/$OSD_LV_NAME.$id
    CEPH_VOLUME="$CEPHADM ceph-volume \
                       --fsid $FSID \
                       --config $CONFIG \
                       --keyring $TMPDIR/keyring.bootstrap.osd --"

    # prepare the osd
    $CEPH_VOLUME lvm prepare --bluestore --data $device_name --no-systemd
    $CEPH_VOLUME lvm batch --no-auto $device_name --yes --no-systemd

    # osd id and osd fsid
    $CEPH_VOLUME lvm list --format json $device_name > $TMPDIR/osd.map
    osd_id=$($SUDO cat $TMPDIR/osd.map | jq -cr '.. | ."ceph.osd_id"? | select(.)')
    osd_fsid=$($SUDO cat $TMPDIR/osd.map | jq -cr '.. | ."ceph.osd_fsid"? | select(.)')

    # deploy the osd
    jq --null-input \
        --arg fsid $FSID \
        --arg name osd.$osd_id \
        --arg keyring $TMPDIR/keyring.bootstrap.osd \
        --arg config "$CONFIG" \
        --arg osd_fsid $osd_fsid \
        '{"fsid": $fsid, "name": $name, "params":{"keyring": $keyring, "config": $config, "osd_fsid": $osd_fsid}}' | \
        $CEPHADM _orch deploy
done

# add node-exporter
jq --null-input \
    --arg fsid $FSID \
    --arg name node-exporter.a \
    '{"fsid": $fsid, "name": $name}' | \
    ${CEPHADM//--image $IMAGE_DEFAULT/} _orch deploy
cond="curl 'http://localhost:9100' | grep -q 'Node Exporter'"
is_available "node-exporter" "$cond" 10

# add prometheus
jq --null-input \
    --arg fsid $FSID \
    --arg name prometheus.a \
    --argjson config_blobs "$(cat ${CEPHADM_SAMPLES_DIR}/prometheus.json)" \
    '{"fsid": $fsid, "name": $name, "config_blobs": $config_blobs}' | \
    ${CEPHADM//--image $IMAGE_DEFAULT/} _orch deploy
cond="curl 'localhost:9095/api/v1/query?query=up'"
is_available "prometheus" "$cond" 10

# add grafana
jq --null-input \
    --arg fsid $FSID \
    --arg name grafana.a \
    --argjson config_blobs "$(cat ${CEPHADM_SAMPLES_DIR}/grafana.json)" \
    '{"fsid": $fsid, "name": $name, "config_blobs": $config_blobs}' | \
    ${CEPHADM//--image $IMAGE_DEFAULT/} _orch deploy
cond="curl --insecure 'https://localhost:3000' | grep -q 'grafana'"
is_available "grafana" "$cond" 50

# add nfs-ganesha
nfs_stop
nfs_rados_pool=$(cat ${CEPHADM_SAMPLES_DIR}/nfs.json | jq -r '.["pool"]')
$CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
        ceph osd pool create $nfs_rados_pool 64
$CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
        rados --pool nfs-ganesha --namespace nfs-ns create conf-nfs.a
$CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
	 ceph orch pause
jq --null-input \
    --arg fsid $FSID \
    --arg name nfs.a \
    --arg keyring "$KEYRING" \
    --arg config "$CONFIG" \
    --argjson config_blobs "$(cat ${CEPHADM_SAMPLES_DIR}/nfs.json)" \
    '{"fsid": $fsid, "name": $name, "params": {"keyring": $keyring, "config": $config}, "config_blobs": $config_blobs}' | \
    ${CEPHADM} _orch deploy
cond="$SUDO ss -tlnp '( sport = :nfs )' | grep 'ganesha.nfsd'"
is_available "nfs" "$cond" 10
$CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
	 ceph orch resume

# add alertmanager via custom container
alertmanager_image=$(cat ${CEPHADM_SAMPLES_DIR}/custom_container.json | jq -r '.image')
tcp_ports=$(jq .ports ${CEPHADM_SAMPLES_DIR}/custom_container.json)
jq --null-input \
    --arg fsid $FSID \
    --arg name container.alertmanager.a \
    --arg keyring $TMPDIR/keyring.bootstrap.osd \
    --arg config "$CONFIG" \
    --arg image "$alertmanager_image" \
    --argjson tcp_ports "${tcp_ports}" \
    --argjson config_blobs "$(cat ${CEPHADM_SAMPLES_DIR}/custom_container.json)" \
    '{"fsid": $fsid, "name": $name, "image": $image, "params": {"keyring": $keyring, "config": $config, "tcp_ports": $tcp_ports}, "config_blobs": $config_blobs}' | \
    ${CEPHADM//--image $IMAGE_DEFAULT/} _orch deploy
cond="$CEPHADM enter --fsid $FSID --name container.alertmanager.a -- test -f \
      /etc/alertmanager/alertmanager.yml"
is_available "alertmanager.yml" "$cond" 10
cond="curl 'http://localhost:9093' | grep -q 'Alertmanager'"
is_available "alertmanager" "$cond" 10

## run
# WRITE ME

## unit
$CEPHADM unit --fsid $FSID --name mon.a -- is-enabled
$CEPHADM unit --fsid $FSID --name mon.a -- is-active
expect_false $CEPHADM unit --fsid $FSID --name mon.xyz -- is-active
$CEPHADM unit --fsid $FSID --name mon.a -- disable
expect_false $CEPHADM unit --fsid $FSID --name mon.a -- is-enabled
$CEPHADM unit --fsid $FSID --name mon.a -- enable
$CEPHADM unit --fsid $FSID --name mon.a -- is-enabled
$CEPHADM unit --fsid $FSID --name mon.a -- status
$CEPHADM unit --fsid $FSID --name mon.a -- stop
expect_return_code 3 $CEPHADM unit --fsid $FSID --name mon.a -- status
$CEPHADM unit --fsid $FSID --name mon.a -- start

## shell
$CEPHADM shell --fsid $FSID -- true
$CEPHADM shell --fsid $FSID -- test -d /var/log/ceph
expect_false $CEPHADM --timeout 10 shell --fsid $FSID -- sleep 60
$CEPHADM --timeout 60 shell --fsid $FSID -- sleep 10
$CEPHADM shell --fsid $FSID --mount $TMPDIR $TMPDIR_TEST_MULTIPLE_MOUNTS -- stat /mnt/$(basename $TMPDIR)

## enter
expect_false $CEPHADM enter
$CEPHADM enter --fsid $FSID --name mon.a -- test -d /var/lib/ceph/mon/ceph-a
$CEPHADM enter --fsid $FSID --name mgr.x -- test -d /var/lib/ceph/mgr/ceph-x
$CEPHADM enter --fsid $FSID --name mon.a -- pidof ceph-mon
expect_false $CEPHADM enter --fsid $FSID --name mgr.x -- pidof ceph-mon
$CEPHADM enter --fsid $FSID --name mgr.x -- pidof ceph-mgr
# this triggers a bug in older versions of podman, including 18.04's 1.6.2
#expect_false $CEPHADM --timeout 5 enter --fsid $FSID --name mon.a -- sleep 30
$CEPHADM --timeout 60 enter --fsid $FSID --name mon.a -- sleep 10

## ceph-volume
$CEPHADM ceph-volume --fsid $FSID -- inventory --format=json \
      | jq '.[]'

## preserve test state
[ $CLEANUP = false ] && exit 0

## rm-daemon
# mon and osd require --force
expect_false $CEPHADM rm-daemon --fsid $FSID --name mon.a
# mgr does not
$CEPHADM rm-daemon --fsid $FSID --name mgr.x

expect_false $CEPHADM zap-osds --fsid $FSID
$CEPHADM zap-osds --fsid $FSID --force

## rm-cluster
expect_false $CEPHADM rm-cluster --fsid $FSID --zap-osds
$CEPHADM rm-cluster --fsid $FSID --force --zap-osds

echo PASS
