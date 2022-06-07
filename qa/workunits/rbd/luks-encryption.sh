#!/usr/bin/env bash
set -ex

CEPH_ID=${CEPH_ID:-admin}
TMP_FILES="/tmp/passphrase /tmp/testdata1 /tmp/testdata2 /tmp/cmpdata"

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
  clean_up_cryptsetup

  # format
  rbd encryption format testimg $format /tmp/passphrase
  drop_caches

  # open encryption with cryptsetup
  sudo cryptsetup open $RAW_DEV --type $format cryptsetupdev -d /tmp/passphrase
  sudo chmod 666 /dev/mapper/cryptsetupdev

  # open encryption with librbd
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg -t nbd -o encryption-format=$format,encryption-passphrase-file=/tmp/passphrase)
  sudo chmod 666 $LIBRBD_DEV

  # write via librbd && compare
  dd if=/tmp/testdata1 of=$LIBRBD_DEV oflag=direct bs=1M
  dd if=/dev/mapper/cryptsetupdev of=/tmp/cmpdata iflag=direct bs=4M count=4
  cmp -n 16MB /tmp/cmpdata /tmp/testdata1

  # write via cryptsetup && compare
  dd if=/tmp/testdata2 of=/dev/mapper/cryptsetupdev oflag=direct bs=1M
  dd if=$LIBRBD_DEV of=/tmp/cmpdata iflag=direct bs=4M count=4
  cmp -n 16MB /tmp/cmpdata /tmp/testdata2
}

function get_nbd_device_paths {
	rbd device list -t nbd | tail -n +2 | egrep "\s+rbd\s+testimg\s+" | awk '{print $5;}'
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
	rbd ls | grep testimg > /dev/null && rbd rm testimg || true
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

echo OK
