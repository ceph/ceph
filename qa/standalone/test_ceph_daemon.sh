#!/bin/bash -ex

SCRIPT_NAME=$(basename ${BASH_SOURCE[0]})
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

FSID='00000000-0000-0000-0000-0000deadbeef'
FSID_LEGACY='00000000-0000-0000-0000-ffffdeadbeef'

# images that are used
IMAGE_MASTER=${IMAGE_MASTER:-'ceph/daemon-base:latest-master-devel'}
IMAGE_NAUTILUS=${IMAGE_NAUTILUS:-'ceph/daemon-base:latest-nautilus'}
IMAGE_MIMIC=${IMAGE_MIMIC:-'ceph/daemon-base:latest-mimic'}

CORPUS_GIT_SUBMOD="ceph-daemon-adoption-corpus"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT
git clone https://github.com/ceph/$CORPUS_GIT_SUBMOD $TMPDIR
CORPUS_DIR=${TMPDIR}/archive
TEST_TARS=$(find ${CORPUS_DIR} -type f -iname *.tgz)

OSD_IMAGE_NAME="${SCRIPT_NAME%.*}_osd.img"
OSD_IMAGE_SIZE='6G'
OSD_TO_CREATE=6
OSD_VG_NAME=${SCRIPT_NAME%.*}
OSD_LV_NAME=${SCRIPT_NAME%.*}

[ -z "$SUDO" ] && SUDO=sudo

if [ -z "$CEPH_DAEMON" ]; then
    CEPH_DAEMON=${SCRIPT_DIR}/../../src/ceph-daemon/ceph-daemon
fi

# at this point, we need $CEPH_DAEMON set
if ! [ -x "$CEPH_DAEMON" ]; then
    echo "ceph-daemon not found. Please set \$CEPH_DAEMON"
    exit 1
fi

# respawn ourselves with a shebang
PYTHONS="python3 python2"  # which pythons we test
if [ -z "$PYTHON_KLUDGE" ]; then
   TMPBINDIR=$(mktemp -d)
   trap "rm -rf $TMPBINDIR" EXIT
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

# add image to args
CEPH_DAEMON_ARGS="$CEPH_DAEMON_ARGS --image $IMAGE_MASTER"

# combine into a single var
CEPH_DAEMON_BIN="$CEPH_DAEMON"
CEPH_DAEMON="$SUDO $CEPH_DAEMON_BIN $CEPH_DAEMON_ARGS"

# clean up previous run(s)?
$CEPH_DAEMON rm-cluster --fsid $FSID --force
$CEPH_DAEMON rm-cluster --fsid $FSID_LEGACY --force
vgchange -an $OSD_VG_NAME || true
loopdev=$($SUDO losetup -a | grep $(basename $OSD_IMAGE_NAME) | awk -F : '{print $1}')
if ! [ "$loopdev" = "" ]; then
    $SUDO losetup -d $loopdev
fi

TMPDIR=`mktemp -d -p .`
trap "rm -rf $TMPDIR" EXIT

function expect_false()
{
        set -x
        if "$@"; then return 1; else return 0; fi
}

## prepare + check host
$SUDO $CEPH_DAEMON check-host

## version + --image
$SUDO CEPH_DAEMON_IMAGE=$IMAGE_NAUTILUS $CEPH_DAEMON_BIN version \
    | grep 'ceph version 14'
$SUDO $CEPH_DAEMON_BIN --image $IMAGE_MIMIC version \
    | grep 'ceph version 13'
$SUDO $CEPH_DAEMON_BIN --image $IMAGE_MASTER version | grep 'ceph version'

# try force docker; this won't work if docker isn't installed
which docker && ( $CEPH_DAEMON --docker version | grep 'ceph version' )

## test shell before bootstrap, when crash dir isn't (yet) present on this host
$CEPH_DAEMON shell -- ceph -v | grep 'ceph version'
$CEPH_DAEMON shell --fsid $FSID -- ceph -v | grep 'ceph version'

## bootstrap
ORIG_CONFIG=`mktemp -p $TMPDIR`
CONFIG=`mktemp -p $TMPDIR`
KEYRING=`mktemp -p $TMPDIR`
IP=127.0.0.1
cat <<EOF > $ORIG_CONFIG
[global]
	log to file = true
EOF
$CEPH_DAEMON bootstrap \
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
$CEPH_DAEMON shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
      ceph -s | grep $FSID

## ls
$CEPH_DAEMON ls | jq '.[]' | jq 'select(.name == "mon.a").fsid' \
    | grep $FSID
$CEPH_DAEMON ls | jq '.[]' | jq 'select(.name == "mgr.x").fsid' \
    | grep $FSID

## deploy
# add mon.b
$CEPH_DAEMON deploy --name mon.b \
      --fsid $FSID \
      --mon-ip $IP:3301 \
      --keyring /var/lib/ceph/$FSID/mon.a/keyring \
      --config $CONFIG
for u in ceph-$FSID@mon.b; do
    systemctl is-enabled $u
    systemctl is-active $u
done

# add mgr.y
$CEPH_DAEMON shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
      ceph auth get-or-create mgr.y \
      mon 'allow profile mgr' \
      osd 'allow *' \
      mds 'allow *' > $TMPDIR/keyring.mgr.y
$CEPH_DAEMON deploy --name mgr.y \
      --fsid $FSID \
      --keyring $TMPDIR/keyring.mgr.y \
      --config $CONFIG
for u in ceph-$FSID@mgr.y; do
    systemctl is-enabled $u
    systemctl is-active $u
done
for f in `seq 1 30`; do
    if $CEPH_DAEMON shell --fsid $FSID \
	     --config $CONFIG --keyring $KEYRING -- \
	  ceph -s -f json-pretty \
	| jq '.mgrmap.num_standbys' | grep -q 1 ; then break; fi
    sleep 1
done
$CEPH_DAEMON shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
      ceph -s -f json-pretty \
    | jq '.mgrmap.num_standbys' | grep -q 1

# add osd.{1,2,..}
dd if=/dev/zero of=$TMPDIR/$OSD_IMAGE_NAME bs=1 count=0 seek=$OSD_IMAGE_SIZE
loop_dev=$($SUDO losetup -f)
$SUDO vgremove -f $OSD_VG_NAME || true
$SUDO losetup $loop_dev $TMPDIR/$OSD_IMAGE_NAME
$SUDO pvcreate $loop_dev && $SUDO vgcreate $OSD_VG_NAME $loop_dev
for id in `seq 0 $((--OSD_TO_CREATE))`; do
    $SUDO lvcreate -l $((100/$OSD_TO_CREATE))%VG -n $OSD_LV_NAME.$id $OSD_VG_NAME
    $CEPH_DAEMON shell --config $CONFIG --keyring $KEYRING -- \
            ceph orchestrator osd create \
                $(hostname):/dev/$OSD_VG_NAME/$OSD_LV_NAME.$id
done

## run
# WRITE ME

## adopt
if false; then
for tarball in $TEST_TARS; do
    TMP_TAR_DIR=`mktemp -d -p $TMPDIR`
    $SUDO tar xzvf $tarball -C $TMP_TAR_DIR
    NAMES=$($CEPH_DAEMON ls --legacy-dir $TMP_TAR_DIR | jq -r '.[].name')
    for name in $NAMES; do
        $CEPH_DAEMON adopt \
                --style legacy \
                --legacy-dir $TMP_TAR_DIR \
                --name $name
        # validate after adopt
        out=$($CEPH_DAEMON ls | jq '.[]' \
                              | jq 'select(.name == "'$name'")')
        echo $out | jq -r '.style' | grep 'ceph-daemon'
        echo $out | jq -r '.fsid' | grep $FSID_LEGACY
    done
    # clean-up before next iter
    $CEPH_DAEMON rm-cluster --fsid $FSID_LEGACY --force
    $SUDO rm -rf $TMP_TAR_DIR
done
fi

## unit
$CEPH_DAEMON unit --fsid $FSID --name mon.a -- is-enabled
$CEPH_DAEMON unit --fsid $FSID --name mon.a -- is-active
expect_false $CEPH_DAEMON unit --fsid $FSID --name mon.xyz -- is-active
$CEPH_DAEMON unit --fsid $FSID --name mon.a -- disable
expect_false $CEPH_DAEMON unit --fsid $FSID --name mon.a -- is-enabled
$CEPH_DAEMON unit --fsid $FSID --name mon.a -- enable
$CEPH_DAEMON unit --fsid $FSID --name mon.a -- is-enabled

## shell
$CEPH_DAEMON shell -- true
$CEPH_DAEMON shell --fsid $FSID -- test -d /var/log/ceph

## enter
expect_false $CEPH_DAEMON enter
$CEPH_DAEMON enter --fsid $FSID --name mon.a -- test -d /var/lib/ceph/mon/ceph-a
$CEPH_DAEMON enter --fsid $FSID --name mgr.x -- test -d /var/lib/ceph/mgr/ceph-x
$CEPH_DAEMON enter --fsid $FSID --name mon.a -- pidof ceph-mon
expect_false $CEPH_DAEMON enter --fsid $FSID --name mgr.x -- pidof ceph-mon
$CEPH_DAEMON enter --fsid $FSID --name mgr.x -- pidof ceph-mgr

## ceph-volume
$CEPH_DAEMON ceph-volume --fsid $FSID -- inventory --format=json \
      | jq '.[]'

## rm-daemon
# mon and osd require --force
expect_false $CEPH_DAEMON rm-daemon --fsid $FSID --name mon.a
# mgr does not
$CEPH_DAEMON rm-daemon --fsid $FSID --name mgr.x

## rm-cluster
expect_false $CEPH_DAEMON rm-cluster --fsid $FSID
$CEPH_DAEMON rm-cluster --fsid $FSID --force

rm -rf $TMPDIR
echo PASS
