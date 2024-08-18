#!/usr/bin/env bash
set -ex

CEPH_ID=${CEPH_ID:-admin}
TMP_FILES="/tmp/passphrase /tmp/testdata1 /tmp/testdata2 /tmp/cmpdata /tmp/rawexport /tmp/export.qcow2"

_sudo()
{
    local cmd

    if [ `id -u` -eq 0 ]
    then
	"$@"
	return $?
    fi

    # Look for the command in the user path. If it fails run it as is,
    # supposing it is in sudo path.
    cmd=`which $1 2>/dev/null` || cmd=$1
    shift
    sudo -nE "${cmd}" "$@"
}

function drop_caches {
  sudo sync
  echo 3 | sudo tee /proc/sys/vm/drop_caches
}

function test_encryption_format() {
  local format=$1

  # format
  rbd encryption format testimg $format /tmp/passphrase
  drop_caches

  # open encryption with cryptsetup
  sudo cryptsetup open $RAW_DEV --type $format cryptsetupdev -d /tmp/passphrase

  # open encryption with librbd
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg -t nbd -o encryption-format=$format,encryption-passphrase-file=/tmp/passphrase)

  # write via librbd && compare
  dd if=/tmp/testdata1 of=$LIBRBD_DEV conv=fsync bs=1M
  dd if=/dev/mapper/cryptsetupdev of=/tmp/cmpdata iflag=direct bs=4M count=4
  cmp -n 16MB /tmp/cmpdata /tmp/testdata1

  # write via cryptsetup && compare
  dd if=/tmp/testdata2 of=/dev/mapper/cryptsetupdev conv=fsync bs=1M
  dd if=$LIBRBD_DEV of=/tmp/cmpdata iflag=direct bs=4M count=4
  cmp -n 16MB /tmp/cmpdata /tmp/testdata2

  _sudo rbd device unmap -t nbd $LIBRBD_DEV
  sudo cryptsetup close cryptsetupdev
}

function test_migration_read_and_copyup() {
  local format=$1

  cp /tmp/testdata2 /tmp/cmpdata

  # test reading
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg1 -t nbd -o encryption-format=$format,encryption-passphrase-file=/tmp/passphrase)
  cmp $LIBRBD_DEV /tmp/cmpdata

  # trigger copyup at the beginning and at the end
  xfs_io -c 'pwrite -S 0xab -W 0 4k' $LIBRBD_DEV /tmp/cmpdata
  xfs_io -c 'pwrite -S 0xba -W 4095k 4k' $LIBRBD_DEV /tmp/cmpdata

  cmp $LIBRBD_DEV /tmp/cmpdata
  _sudo rbd device unmap -t nbd $LIBRBD_DEV

  # test reading on a fresh mapping
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg1 -t nbd -o encryption-format=$format,encryption-passphrase-file=/tmp/passphrase)
  cmp $LIBRBD_DEV /tmp/cmpdata
  _sudo rbd device unmap -t nbd $LIBRBD_DEV

  # test reading on a fresh mapping after migration is executed
  rbd migration execute testimg1
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg1 -t nbd -o encryption-format=$format,encryption-passphrase-file=/tmp/passphrase)
  cmp $LIBRBD_DEV /tmp/cmpdata
  _sudo rbd device unmap -t nbd $LIBRBD_DEV

  # test reading on a fresh mapping after migration is committed
  rbd migration commit testimg1
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg1 -t nbd -o encryption-format=$format,encryption-passphrase-file=/tmp/passphrase)
  cmp $LIBRBD_DEV /tmp/cmpdata
  _sudo rbd device unmap -t nbd $LIBRBD_DEV
}

function test_migration_native_with_snaps() {
  local format=$1

  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg1@snap1 -t nbd -o encryption-format=$format,encryption-passphrase-file=/tmp/passphrase)
  cmp $LIBRBD_DEV /tmp/testdata1
  _sudo rbd device unmap -t nbd $LIBRBD_DEV
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg1@snap2 -t nbd -o encryption-format=$format,encryption-passphrase-file=/tmp/passphrase)
  cmp $LIBRBD_DEV /tmp/testdata2
  _sudo rbd device unmap -t nbd $LIBRBD_DEV

  test_migration_read_and_copyup $format

  # check that snapshots aren't affected by copyups
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg1@snap1 -t nbd -o encryption-format=$format,encryption-passphrase-file=/tmp/passphrase)
  cmp $LIBRBD_DEV /tmp/testdata1
  _sudo rbd device unmap -t nbd $LIBRBD_DEV
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg1@snap2 -t nbd -o encryption-format=$format,encryption-passphrase-file=/tmp/passphrase)
  cmp $LIBRBD_DEV /tmp/testdata2
  _sudo rbd device unmap -t nbd $LIBRBD_DEV

  rbd snap rm testimg1@snap2
  rbd snap rm testimg1@snap1
  rbd rm testimg1
}

function test_migration() {
  local format=$1

  rbd encryption format testimg $format /tmp/passphrase

  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg -t nbd -o encryption-format=$format,encryption-passphrase-file=/tmp/passphrase)
  dd if=/tmp/testdata1 of=$LIBRBD_DEV conv=fsync bs=1M
  rbd snap create testimg@snap1
  dd if=/tmp/testdata2 of=$LIBRBD_DEV conv=fsync bs=1M
  rbd snap create testimg@snap2
  # FIXME: https://tracker.ceph.com/issues/67401
  # leave HEAD with the same data as snap2 as a workaround
  # dd if=/tmp/testdata3 of=$LIBRBD_DEV conv=fsync bs=1M
  _sudo rbd device unmap -t nbd $LIBRBD_DEV

  # live import a raw image
  rbd export testimg /tmp/rawexport
  rbd migration prepare --import-only --source-spec '{"type": "raw", "stream": {"type": "file", "file_path": "/tmp/rawexport"}}' testimg1
  test_migration_read_and_copyup $format
  rbd rm testimg1

  # live import a qcow image
  qemu-img convert -f raw -O qcow2 /tmp/rawexport /tmp/export.qcow2
  rbd migration prepare --import-only --source-spec '{"type": "qcow", "stream": {"type": "file", "file_path": "/tmp/export.qcow2"}}' testimg1
  test_migration_read_and_copyup $format
  rbd rm testimg1

  # live import a native image
  rbd migration prepare --import-only testimg@snap2 testimg1
  test_migration_native_with_snaps $format

  # live migrate a native image (removes testimg)
  rbd migration prepare testimg testimg1
  test_migration_native_with_snaps $format

  rm /tmp/rawexport /tmp/export.qcow2
}

function get_nbd_device_paths {
  rbd device list -t nbd | tail -n +2 | egrep "\s+rbd\s+testimg" | awk '{print $5;}'
}

function clean_up_cryptsetup() {
  ls /dev/mapper/cryptsetupdev && sudo cryptsetup close cryptsetupdev || true
}

function clean_up {
  sudo rm -f $TMP_FILES
  clean_up_cryptsetup
  for device in $(get_nbd_device_paths); do
    _sudo rbd device unmap -t nbd $device
  done

  rbd migration abort testimg1 || true
  rbd snap remove testimg1@snap2 || true
  rbd snap remove testimg1@snap1 || true
  rbd remove testimg1 || true
  rbd snap remove testimg@snap2 || true
  rbd snap remove testimg@snap1 || true
  rbd remove testimg || true
}

if [[ $(uname) != "Linux" ]]; then
	echo "LUKS encryption tests only supported on Linux"
	exit 0
fi


if [[ $(($(ceph-conf --name client.${CEPH_ID} --show-config-value rbd_default_features) & 64)) != 0 ]]; then
	echo "LUKS encryption tests not supported alongside image journaling feature"
	exit 0
fi

clean_up

trap clean_up INT TERM EXIT

# generate test data
dd if=/dev/urandom of=/tmp/testdata1 bs=4M count=4
dd if=/dev/urandom of=/tmp/testdata2 bs=4M count=4

# create passphrase file
echo -n "password" > /tmp/passphrase

# create an image
rbd create testimg --size=32M

# map raw data to nbd device
RAW_DEV=$(_sudo rbd -p rbd map testimg -t nbd)

test_encryption_format luks1
test_encryption_format luks2

_sudo rbd device unmap -t nbd $RAW_DEV
rbd rm testimg

rbd create --size 20M testimg
test_migration luks1

rbd create --size 32M testimg
test_migration luks2

echo OK
