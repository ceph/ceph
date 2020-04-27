#!/bin/bash -ex

SCRIPT_NAME=$(basename ${BASH_SOURCE[0]})
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# cleanup during exit
[ -z "$CLEANUP" ] && CLEANUP=true

FSID='00000000-0000-0000-0000-0000deadbeef'

# images that are used
IMAGE_MASTER=${IMAGE_MASTER:-'docker.io/ceph/daemon-base:latest-master-devel'}
IMAGE_OCTOPUS=${IMAGE_OCTOPUS:-'docker.io/ceph/daemon-base:latest-octopus'}
IMAGE_NAUTILUS=${IMAGE_NAUTILUS:-'docker.io/ceph/daemon-base:latest-nautilus'}
IMAGE_MIMIC=${IMAGE_MIMIC:-'docker.io/ceph/daemon-base:latest-mimic'}

OSD_IMAGE_NAME="${SCRIPT_NAME%.*}_osd.img"
OSD_IMAGE_SIZE='6G'
OSD_TO_CREATE=2
OSD_VG_NAME=${SCRIPT_NAME%.*}
OSD_LV_NAME=${SCRIPT_NAME%.*}

CEPHADM_SRC_DIR=${SCRIPT_DIR}/../../../src/cephadm
CEPHADM_SAMPLES_DIR=${CEPHADM_SRC_DIR}/samples

[ -z "$SUDO" ] && SUDO=sudo

if [ -z "$CEPHADM" ]; then
    CEPHADM=${CEPHADM_SRC_DIR}/cephadm
fi

# at this point, we need $CEPHADM set
if ! [ -x "$CEPHADM" ]; then
    echo "cephadm not found. Please set \$CEPHADM"
    exit 1
fi

# respawn ourselves with a shebang
if [ -z "$PYTHON_KLUDGE" ]; then
    # see which pythons we should test with
    PYTHONS=""
    which python3 && PYTHONS="$PYTHONS python3"
    which python2 && PYTHONS="$PYTHONS python2"
    echo "PYTHONS $PYTHONS"
    if [ -z "$PYTHONS" ]; then
	echo "No PYTHONS found!"
	exit 1
    fi

    TMPBINDIR=$(mktemp -d)
    trap "rm -rf $TMPBINDIR" EXIT
    ORIG_CEPHADM="$CEPHADM"
    CEPHADM="$TMPBINDIR/cephadm"
    for p in $PYTHONS; do
	echo "=== re-running with $p ==="
	ln -s `which $p` $TMPBINDIR/python
	echo "#!$TMPBINDIR/python" > $CEPHADM
	cat $ORIG_CEPHADM >> $CEPHADM
	chmod 700 $CEPHADM
	$TMPBINDIR/python --version
	PYTHON_KLUDGE=1 CEPHADM=$CEPHADM $0
	rm $TMPBINDIR/python
    done
    rm -rf $TMPBINDIR
    echo "PASS with all of: $PYTHONS"
    exit 0
fi

# add image to args
CEPHADM_ARGS="$CEPHADM_ARGS --image $IMAGE_MASTER"

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

# TMPDIR for test data
[ -d "$TMPDIR" ] || TMPDIR=$(mktemp -d tmp.$SCRIPT_NAME.XXXXXX)

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
        if systemctl status $unit; then
            $SUDO systemctl stop $unit
        fi
    done

    # ensure the NFS port is no longer in use
    expect_false "$SUDO ss -tlnp '( sport = :nfs )' | grep LISTEN"
}

## prepare + check host
$SUDO $CEPHADM check-host

## version + --image
$SUDO CEPHADM_IMAGE=$IMAGE_OCTOPUS $CEPHADM_BIN version
$SUDO CEPHADM_IMAGE=$IMAGE_OCTOPUS $CEPHADM_BIN version \
    | grep 'ceph version 15'
$SUDO CEPHADM_IMAGE=$IMAGE_NAUTILUS $CEPHADM_BIN version
$SUDO CEPHADM_IMAGE=$IMAGE_NAUTILUS $CEPHADM_BIN version \
    | grep 'ceph version 14'
$SUDO $CEPHADM_BIN --image $IMAGE_MIMIC version
$SUDO $CEPHADM_BIN --image $IMAGE_MIMIC version \
    | grep 'ceph version 13'
$SUDO $CEPHADM_BIN --image $IMAGE_MASTER version | grep 'ceph version'

# try force docker; this won't work if docker isn't installed
systemctl status docker && ( $CEPHADM --docker version | grep 'ceph version' )

## test shell before bootstrap, when crash dir isn't (yet) present on this host
$CEPHADM shell --fsid $FSID -- ceph -v | grep 'ceph version'
$CEPHADM shell --fsid $FSID -e FOO=BAR -- printenv | grep FOO=BAR

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
$CEPHADM deploy --name mon.b \
      --fsid $FSID \
      --keyring /var/lib/ceph/$FSID/mon.a/keyring \
      --config $MONCONFIG
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
$CEPHADM deploy --name mgr.y \
      --fsid $FSID \
      --keyring $TMPDIR/keyring.mgr.y \
      --config $CONFIG
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

# osd boostrap keyring
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
    $CEPHADM deploy --name osd.$osd_id \
          --fsid $FSID \
          --keyring $TMPDIR/keyring.bootstrap.osd \
          --config $CONFIG \
          --osd-fsid $osd_fsid
done

# add node-exporter
${CEPHADM//--image $IMAGE_MASTER/} deploy \
    --name node-exporter.a --fsid $FSID
cond="curl 'http://localhost:9100' | grep -q 'Node Exporter'"
is_available "node-exporter" "$cond" 10

# add prometheus
cat ${CEPHADM_SAMPLES_DIR}/prometheus.json | \
        ${CEPHADM//--image $IMAGE_MASTER/} deploy \
	    --name prometheus.a --fsid $FSID --config-json -
cond="curl 'localhost:9095/api/v1/query?query=up'"
is_available "prometheus" "$cond" 10

# add grafana
cat ${CEPHADM_SAMPLES_DIR}/grafana.json | \
        ${CEPHADM//--image $IMAGE_MASTER/} deploy \
            --name grafana.a --fsid $FSID --config-json -
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
$CEPHADM deploy --name nfs.a \
      --fsid $FSID \
      --keyring $KEYRING \
      --config $CONFIG \
      --config-json ${CEPHADM_SAMPLES_DIR}/nfs.json
cond="$SUDO ss -tlnp '( sport = :nfs )' | grep 'ganesha.nfsd'"
is_available "nfs" "$cond" 10
$CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
	 ceph orch resume

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

## shell
$CEPHADM shell --fsid $FSID -- true
$CEPHADM shell --fsid $FSID -- test -d /var/log/ceph
expect_false $CEPHADM --timeout 10 shell --fsid $FSID -- sleep 60
$CEPHADM --timeout 60 shell --fsid $FSID -- sleep 10

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

## rm-cluster
expect_false $CEPHADM rm-cluster --fsid $FSID
$CEPHADM rm-cluster --fsid $FSID --force

echo PASS
