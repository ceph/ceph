#!/bin/bash -ex

SCRIPT_NAME=$(basename ${BASH_SOURCE[0]})
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

FSID='00000000-0000-0000-0000-0000deadbeef'
FSID_LEGACY='00000000-0000-0000-0000-ffffdeadbeef'

# images that are used
IMAGE_MASTER=${IMAGE_MASTER:-'ceph/daemon-base:latest-master-devel'}
IMAGE_NAUTILUS=${IMAGE_NAUTILUS:-'ceph/daemon-base:latest-nautilus'}
IMAGE_MIMIC=${IMAGE_MIMIC:-'ceph/daemon-base:latest-mimic'}

TEST_TARS=${SCRIPT_DIR}/test_ceph_daemon/*.tgz

OSD_IMAGE_NAME="${SCRIPT_NAME%.*}_osd.img"
OSD_IMAGE_SIZE='6G'
OSD_TO_CREATE=6
OSD_VG_NAME=${SCRIPT_NAME%.*}
OSD_LV_NAME=${SCRIPT_NAME%.*}

[ -z "$SUDO" ] && SUDO=sudo

if [ -z "$CEPH_DAEMON" ]; then
    [ -x src/ceph-daemon/ceph-daemon ] && CEPH_DAEMON=src/ceph-daemon/ceph-daemon
    [ -x ../src/ceph-daemon/ceph-daemon ] && CEPH_DAEMON=../src/ceph-daemon/ceph-daemon
    [ -x ./ceph-daemon/ceph-daemon ] && CEPH_DAEMON=./ceph-daemon/ceph-daemon
    [ -x ./ceph-daemon ] && CEPH_DAEMON=.ceph-daemon
    which ceph-daemon && CEPH_DAEMON=$(which ceph-daemon)
fi

# at this point, we need $CEPH_DAEMON set
if [ -z "$CEPH_DAEMON" ]; then
    echo "ceph-daemon not found.Please set \$CEPH_DAEMON"
    exit 1
fi

# respawn ourselves with a shebang
PYTHONS="python3 python2"  # which pythons we test
if [ -z "$PYTHON_KLUDGE" ]; then
   TMPBINDIR=`mktemp -d $TMPDIR`
   trap "rm -rf $TMPBINDIR" TERM HUP INT
   ORIG_CEPH_DAEMON="$CEPH_DAEMON"
   CEPH_DAEMON="$TMPBINDIR/ceph-daemon"
   for p in $PYTHONS; do
       echo "=== re-running with $p ==="
       ln -s `which $p` $TMPBINDIR/python
       echo "#!$TMPBINDIR/python" > $CEPH_DAEMON
       cat $ORIG_CEPH_DAEMON >> $CEPH_DAEMON
       chmod 700 $CEPH_DAEMON
       $TMPBINDIR/python --version
       PYTHON_KLUDGE=1 CEPH_DAEMON=$CEPH_DAEMON $0
       rm $TMPBINDIR/python
   done
   rm -rf $TMPBINDIR
   echo "PASS with all of: $PYTHONS"
   exit 0
fi

# clean up previous run(s)?
$SUDO $CEPH_DAEMON rm-cluster --fsid $FSID --force
$SUDO $CEPH_DAEMON rm-cluster --fsid $FSID_LEGACY --force
vgchange -an $OSD_VG_NAME || true
loopdev=$(losetup -a | grep $(basename $OSD_IMAGE_NAME) | awk -F : '{print $1}')
if ! [ "$loopdev" = "" ]; then
    losetup -d $loopdev
fi

TMPDIR=`mktemp -d -p .`
trap "rm -rf $TMPDIR" TERM HUP INT

function expect_false()
{
        set -x
        if "$@"; then return 1; else return 0; fi
}

## version + --image
$SUDO $CEPH_DAEMON --image $IMAGE_NAUTILUS version \
    | grep 'ceph version 14'
$SUDO $CEPH_DAEMON --image $IMAGE_MIMIC version \
    | grep 'ceph version 13'
$SUDO $CEPH_DAEMON --image $IMAGE_MASTER version | grep 'ceph version'

# try force docker; this won't work if docker isn't installed
which docker && ( $SUDO $CEPH_DAEMON --docker version | grep 'ceph version' )

## test shell before bootstrap, when crash dir isn't (yet) present on this host
$SUDO $CEPH_DAEMON shell -- ceph -v | grep 'ceph version'
$SUDO $CEPH_DAEMON shell --fsid $FSID -- ceph -v | grep 'ceph version'

## bootstrap
ORIG_CONFIG=`mktemp -p $TMPDIR`
CONFIG=`mktemp -p $TMPDIR`
KEYRING=`mktemp -p $TMPDIR`
IP=127.0.0.1
cat <<EOF > $ORIG_CONFIG
[global]
log to file = true
EOF
$SUDO $CEPH_DAEMON --image $IMAGE_MASTER bootstrap \
      --mon-id a \
      --mgr-id x \
      --mon-ip $IP \
      --fsid $FSID \
      --config $ORIG_CONFIG \
      --output-config $CONFIG \
      --output-keyring $KEYRING \
      --allow-overwrite
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
$SUDO $CEPH_DAEMON shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
      ceph -s | grep $FSID

## ls
$SUDO $CEPH_DAEMON ls | jq '.[]' | jq 'select(.name == "mon.a").fsid' \
    | grep $FSID
$SUDO $CEPH_DAEMON ls | jq '.[]' | jq 'select(.name == "mgr.x").fsid' \
    | grep $FSID

## deploy
# add mon.b
$SUDO $CEPH_DAEMON --image $IMAGE_MASTER deploy --name mon.b \
      --fsid $FSID \
      --mon-ip $IP:3301 \
      --keyring /var/lib/ceph/$FSID/mon.a/keyring \
      --config $CONFIG
for u in ceph-$FSID@mon.b; do
    systemctl is-enabled $u
    systemctl is-active $u
done

# add mgr.y
$SUDO $CEPH_DAEMON shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
      ceph auth get-or-create mgr.y \
      mon 'allow profile mgr' \
      osd 'allow *' \
      mds 'allow *' > $TMPDIR/keyring.mgr.y
$SUDO $CEPH_DAEMON --image $IMAGE_MASTER deploy --name mgr.y \
      --fsid $FSID \
      --keyring $TMPDIR/keyring.mgr.y \
      --config $CONFIG
for u in ceph-$FSID@mgr.y; do
    systemctl is-enabled $u
    systemctl is-active $u
done
for f in `seq 1 30`; do
    if $SUDO $CEPH_DAEMON shell --fsid $FSID \
	     --config $CONFIG --keyring $KEYRING -- \
	  ceph -s -f json-pretty \
	| jq '.mgrmap.num_standbys' | grep -q 1 ; then break; fi
    sleep 1
done
$SUDO $CEPH_DAEMON shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
      ceph -s -f json-pretty \
    | jq '.mgrmap.num_standbys' | grep -q 1

# add osd.{1,2,..}
dd if=/dev/zero of=$TMPDIR/$OSD_IMAGE_NAME bs=1 count=0 seek=$OSD_IMAGE_SIZE
loop_dev=$(losetup -f)
losetup $loop_dev $TMPDIR/$OSD_IMAGE_NAME
pvcreate $loop_dev && vgcreate $OSD_VG_NAME $loop_dev
for id in `seq 0 $((--OSD_TO_CREATE))`; do
    lvcreate -l $((100/$OSD_TO_CREATE))%VG -n $OSD_LV_NAME.$id $OSD_VG_NAME
    $SUDO $CEPH_DAEMON shell --config $CONFIG --keyring $KEYRING -- \
            ceph orchestrator osd create \
                $(hostname):/dev/$OSD_VG_NAME/$OSD_LV_NAME.$id
done

## run
# WRITE ME

## adopt
for tarball in $TEST_TARS; do
    TMP_TAR_DIR=`mktemp -d -p $TMPDIR`
    tar xzvf $tarball -C $TMP_TAR_DIR
    NAMES=$($SUDO $CEPH_DAEMON ls --legacy-dir $TMP_TAR_DIR | jq -r '.[].name')
    for name in $NAMES; do
        # TODO: skip osd test for now
        if [[ $name =~ "osd" ]]; then
           continue
        fi
        $SUDO $CEPH_DAEMON --image $IMAGE_MASTER adopt \
                           --style legacy \
                           --legacy-dir $TMP_TAR_DIR \
                           --name $name
        # validate after adopt
        out=$($SUDO $CEPH_DAEMON ls | jq '.[]' \
                                    | jq 'select(.name == "'$name'")')
        echo $out | jq -r '.style' | grep 'ceph-daemon'
        echo $out | jq -r '.fsid' | grep $FSID_LEGACY
    done
    # clean-up before next iter
    $SUDO $CEPH_DAEMON rm-cluster --fsid $FSID_LEGACY --force
    rm -rf $TMP_TAR_DIR
done

## unit
$SUDO $CEPH_DAEMON unit --fsid $FSID --name mon.a -- is-enabled
$SUDO $CEPH_DAEMON unit --fsid $FSID --name mon.a -- is-active
expect_false $SUDO $CEPH_DAEMON unit --fsid $FSID --name mon.xyz -- is-active
$SUDO $CEPH_DAEMON unit --fsid $FSID --name mon.a -- disable
expect_false $SUDO $CEPH_DAEMON unit --fsid $FSID --name mon.a -- is-enabled
$SUDO $CEPH_DAEMON unit --fsid $FSID --name mon.a -- enable
$SUDO $CEPH_DAEMON unit --fsid $FSID --name mon.a -- is-enabled

## shell
$SUDO $CEPH_DAEMON --image $IMAGE_MASTER shell -- true
$SUDO $CEPH_DAEMON --image $IMAGE_MASTER shell --fsid $FSID -- test -d /var/log/ceph

## enter
expect_false $SUDO $CEPH_DAEMON enter
$SUDO $CEPH_DAEMON enter --fsid $FSID --name mon.a -- test -d /var/lib/ceph/mon/ceph-a
$SUDO $CEPH_DAEMON enter --fsid $FSID --name mgr.x -- test -d /var/lib/ceph/mgr/ceph-x
$SUDO $CEPH_DAEMON enter --fsid $FSID --name mon.a -- pidof ceph-mon
expect_false $SUDO $CEPH_DAEMON enter --fsid $FSID --name mgr.x -- pidof ceph-mon
$SUDO $CEPH_DAEMON enter --fsid $FSID --name mgr.x -- pidof ceph-mgr

## ceph-volume
$SUDO $CEPH_DAEMON --image $IMAGE_MASTER ceph-volume --fsid $FSID -- inventory --format=json \
      | jq '.[]'

## rm-daemon
# mon and osd require --force
expect_false $SUDO $CEPH_DAEMON rm-daemon --fsid $FSID --name mon.a
# mgr does not
$SUDO $CEPH_DAEMON rm-daemon --fsid $FSID --name mgr.x

## rm-cluster
expect_false $SUDO $CEPH_DAEMON rm-cluster --fsid $FSID
$SUDO $CEPH_DAEMON rm-cluster --fsid $FSID --force

rm -rf $TMPDIR
echo PASS
