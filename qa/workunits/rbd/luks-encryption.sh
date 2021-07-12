#!/usr/bin/env bash
set -ex

CEPH_ID=${CEPH_ID:-admin}
TMP_FILES="/tmp/passphrase /tmp/testdata1 /tmp/testdata2"

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

  # open encryption with librbd
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg -t nbd -o encryption-format=$format,encryption-passphrase-file=/tmp/passphrase)

  # write via librbd && compare
  sudo dd if=/tmp/testdata1 of=$LIBRBD_DEV conv=fdatasync
  drop_caches
  sudo cmp -n 16MB /tmp/testdata1 /dev/mapper/cryptsetupdev

  # write via cryptsetup && compare
  sudo dd if=/tmp/testdata2 of=/dev/mapper/cryptsetupdev conv=fdatasync
  drop_caches
  sudo cmp -n 16MB $LIBRBD_DEV /tmp/testdata2
}

function test_clone_encryption() {
  clean_up_cryptsetup

  # write 1MB plaintext
  sudo dd if=/tmp/testdata1 of=$RAW_DEV conv=fdatasync bs=1M count=1
  drop_caches

  # clone (luks1)
  rbd snap create testimg@snap
  rbd snap protect testimg@snap
  rbd clone testimg@snap testimg1 --child-encryption-format luks1 --child-encryption-passphrase-file /tmp/passphrase

  # open encryption with librbd, write one more MB, close
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg1 -t nbd -o encryption-format=luks1,encryption-passphrase-file=/tmp/passphrase)
  sudo cmp -n 1MB $LIBRBD_DEV /tmp/testdata1
  sudo dd if=/tmp/testdata1 of=$LIBRBD_DEV seek=1 skip=1 conv=fdatasync bs=1M count=1
  sudo rbd device unmap -t nbd $LIBRBD_DEV

  # second clone (luks2)
  rbd snap create testimg1@snap
  rbd snap protect testimg1@snap
  rbd clone testimg1@snap testimg2 --parent-encryption-format luks1 --parent-encryption-passphrase-file /tmp/passphrase \
    --child-encryption-format luks2 --child-encryption-passphrase-file /tmp/passphrase

  # open encryption with librbd, write one more MB, close
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg2 -t nbd -o encryption-format=luks2,encryption-passphrase-file=/tmp/passphrase)
  sudo cmp -n 2MB $LIBRBD_DEV /tmp/testdata1
  sudo dd if=/tmp/testdata1 of=$LIBRBD_DEV seek=2 skip=2 conv=fdatasync bs=1M count=1
  sudo rbd device unmap -t nbd $LIBRBD_DEV

  # verify flattening without specifying encryption fails
  rbd deep copy testimg2 testimg3 --flatten && exit 1 || true
  rbd migration prepare testimg2 testimg3 --flatten && exit 1 || true
  rbd flatten testimg2 && exit 1 || true

  # flatten
  rbd flatten testimg2 --encryption-format luks2 --encryption-passphrase-file /tmp/passphrase

  # verify with cryptsetup
  RAW_DEV=$(_sudo rbd -p rbd map testimg2 -t nbd)
  sudo cryptsetup open $RAW_DEV --type luks2 cryptsetupdev -d /tmp/passphrase
  sudo cmp -n 3MB /dev/mapper/cryptsetupdev /tmp/testdata1
  sudo rbd device unmap -t nbd $RAW_DEV
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

  rbd remove testimg3 || true
  rbd remove testimg2 || true
  rbd snap unprotect testimg1@snap || true
  rbd snap remove testimg1@snap || true
  rbd remove testimg1 || true
  rbd snap unprotect testimg@snap || true
  rbd snap remove testimg@snap || true
  rbd remove testimg || true
}

if [[ $(uname) != "Linux" ]]; then
	echo "LUKS encryption tests only supported on Linux"
	exit 0
fi


if [[ $(($(ceph-conf --name client.${CEPH_ID} rbd_default_features) & 64)) != 0 ]]; then
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

test_clone_encryption

echo OK
