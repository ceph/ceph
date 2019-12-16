#!/bin/bash -ex

SCRIPT_NAME=$(basename ${BASH_SOURCE[0]})
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

FSID='00000000-0000-0000-0000-0000deadbeef'
FSID_LEGACY='00000000-0000-0000-0000-ffffdeadbeef'

# images that are used
IMAGE_MASTER=${IMAGE_MASTER:-'ceph/daemon-base:latest-master-devel'}
IMAGE_NAUTILUS=${IMAGE_NAUTILUS:-'ceph/daemon-base:latest-nautilus'}
IMAGE_MIMIC=${IMAGE_MIMIC:-'ceph/daemon-base:latest-mimic'}

CORPUS_GIT_SUBMOD="cephadm-adoption-corpus"
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

if [ -z "$CEPHADM" ]; then
    CEPHADM=${SCRIPT_DIR}/../../../src/cephadm/cephadm
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
    if [ -z $PYTHONS ]; then
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
$CEPHADM rm-cluster --fsid $FSID_LEGACY --force
vgchange -an $OSD_VG_NAME || true
loopdev=$($SUDO losetup -a | grep $(basename $OSD_IMAGE_NAME) | awk -F : '{print $1}')
if ! [ "$loopdev" = "" ]; then
    $SUDO losetup -d $loopdev
fi

function expect_false()
{
        set -x
        if "$@"; then return 1; else return 0; fi
}

## prepare + check host
$SUDO $CEPHADM check-host

## version + --image
$SUDO CEPHADM_IMAGE=$IMAGE_NAUTILUS $CEPHADM_BIN version
$SUDO CEPHADM_IMAGE=$IMAGE_NAUTILUS $CEPHADM_BIN version \
    | grep 'ceph version 14'
$SUDO $CEPHADM_BIN --image $IMAGE_MIMIC version
$SUDO $CEPHADM_BIN --image $IMAGE_MIMIC version \
    | grep 'ceph version 13'
$SUDO $CEPHADM_BIN --image $IMAGE_MASTER version | grep 'ceph version'

# try force docker; this won't work if docker isn't installed
which docker && ( $CEPHADM --docker version | grep 'ceph version' )

## test shell before bootstrap, when crash dir isn't (yet) present on this host
$CEPHADM shell --fsid $FSID -- ceph -v | grep 'ceph version'

## bootstrap
ORIG_CONFIG=`mktemp -p $TMPDIR`
CONFIG=`mktemp -p $TMPDIR`
MONCONFIG=`mktemp -p $TMPDIR`
KEYRING=`mktemp -p $TMPDIR`
IP=127.0.0.1
cat <<EOF > $ORIG_CONFIG
[global]
	log to file = true
EOF
$CEPHADM bootstrap \
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
$CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
      ceph -s | grep $FSID

## ls
$CEPHADM ls | jq '.[]' | jq 'select(.name == "mon.a").fsid' \
    | grep $FSID
$CEPHADM ls | jq '.[]' | jq 'select(.name == "mgr.x").fsid' \
    | grep $FSID

## deploy
# add mon.b
cp $CONFIG $MONCONFIG
echo "public addr = $IP:3301" >> $MONCONFIG
$CEPHADM deploy --name mon.b \
      --fsid $FSID \
      --keyring /var/lib/ceph/$FSID/mon.a/keyring \
      --config $CONFIG
for u in ceph-$FSID@mon.b; do
    systemctl is-enabled $u
    systemctl is-active $u
done

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

exit 0

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
for id in `seq 0 $((--OSD_TO_CREATE))`; do
    $SUDO lvcreate -l $((100/$OSD_TO_CREATE))%VG -n $OSD_LV_NAME.$id $OSD_VG_NAME
    $CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
            ceph orchestrator osd create \
                $(hostname):/dev/$OSD_VG_NAME/$OSD_LV_NAME.$id
done

## run
# WRITE ME

## adopt
for tarball in $TEST_TARS; do
    TMP_TAR_DIR=`mktemp -d -p $TMPDIR`
    $SUDO tar xzvf $tarball -C $TMP_TAR_DIR
    NAMES=$($CEPHADM ls --legacy-dir $TMP_TAR_DIR | jq -r '.[].name')
    for name in $NAMES; do
        $CEPHADM adopt \
                --style legacy \
                --legacy-dir $TMP_TAR_DIR \
                --name $name
        # validate after adopt
        out=$($CEPHADM ls | jq '.[]' \
                              | jq 'select(.name == "'$name'")')
        echo $out | jq -r '.style' | grep 'cephadm'
        echo $out | jq -r '.fsid' | grep $FSID_LEGACY
    done
    # clean-up before next iter
    $CEPHADM rm-cluster --fsid $FSID_LEGACY --force
    $SUDO rm -rf $TMP_TAR_DIR
done

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

## enter
expect_false $CEPHADM enter
$CEPHADM enter --fsid $FSID --name mon.a -- test -d /var/lib/ceph/mon/ceph-a
$CEPHADM enter --fsid $FSID --name mgr.x -- test -d /var/lib/ceph/mgr/ceph-x
$CEPHADM enter --fsid $FSID --name mon.a -- pidof ceph-mon
expect_false $CEPHADM enter --fsid $FSID --name mgr.x -- pidof ceph-mon
$CEPHADM enter --fsid $FSID --name mgr.x -- pidof ceph-mgr

## ceph-volume
$CEPHADM ceph-volume --fsid $FSID -- inventory --format=json \
      | jq '.[]'

## rm-daemon
# mon and osd require --force
expect_false $CEPHADM rm-daemon --fsid $FSID --name mon.a
# mgr does not
$CEPHADM rm-daemon --fsid $FSID --name mgr.x

## rm-cluster
expect_false $CEPHADM rm-cluster --fsid $FSID
$CEPHADM rm-cluster --fsid $FSID --force

rm -rf $TMPDIR
echo PASS
