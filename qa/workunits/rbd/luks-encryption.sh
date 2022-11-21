#!/usr/bin/env bash
set -ex

CEPH_ID=${CEPH_ID:-admin}
TMP_FILES="/tmp/passphrase /tmp/passphrase2 /tmp/testdata1 /tmp/testdata2 /tmp/cmpdata"

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

function expect_false() {
  if "$@"; then return 1; else return 0; fi
}

function test_encryption_format() {
  local format=$1
  clean_up_cryptsetup

  # format
  rbd encryption format testimg $format /tmp/passphrase
  drop_caches

  # open encryption with cryptsetup
  sudo cryptsetup open $RAW_DEV --type luks cryptsetupdev -d /tmp/passphrase
  sudo chmod 666 /dev/mapper/cryptsetupdev

  # open encryption with librbd
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg -t nbd -o encryption-passphrase-file=/tmp/passphrase)
  sudo chmod 666 $LIBRBD_DEV

  # write via librbd && compare
  dd if=/tmp/testdata1 of=$LIBRBD_DEV oflag=direct bs=1M
  dd if=/dev/mapper/cryptsetupdev of=/tmp/cmpdata iflag=direct bs=4M count=4
  cmp -n 16MB /tmp/cmpdata /tmp/testdata1

  # write via cryptsetup && compare
  dd if=/tmp/testdata2 of=/dev/mapper/cryptsetupdev oflag=direct bs=1M
  dd if=$LIBRBD_DEV of=/tmp/cmpdata iflag=direct bs=4M count=4
  cmp -n 16MB /tmp/cmpdata /tmp/testdata2

  sudo rbd device unmap -t nbd $LIBRBD_DEV
}

function test_clone_encryption() {
  clean_up_cryptsetup

  # write 1MB plaintext
  dd if=/tmp/testdata1 of=$RAW_DEV oflag=direct bs=1M count=1

  # clone (luks1)
  rbd snap create testimg@snap
  rbd snap protect testimg@snap
  rbd clone testimg@snap testimg1
  rbd encryption format testimg1 luks1 /tmp/passphrase

  # open encryption with librbd, write one more MB, close
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg1 -t nbd -o encryption-format=luks1,encryption-passphrase-file=/tmp/passphrase)
  sudo chmod 666 $LIBRBD_DEV
  dd if=$LIBRBD_DEV of=/tmp/cmpdata iflag=direct bs=1M count=1
  cmp -n 1MB /tmp/cmpdata /tmp/testdata1
  dd if=/tmp/testdata1 of=$LIBRBD_DEV seek=1 skip=1 oflag=direct bs=1M count=1
  sudo rbd device unmap -t nbd $LIBRBD_DEV

  # second clone (luks2)
  rbd snap create testimg1@snap
  rbd snap protect testimg1@snap
  rbd clone testimg1@snap testimg2
  rbd encryption format testimg2 luks2 /tmp/passphrase2

  # open encryption with librbd, write one more MB, close
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg2 -t nbd -o encryption-format=luks2,encryption-passphrase-file=/tmp/passphrase2,encryption-format=luks1,encryption-passphrase-file=/tmp/passphrase)
  sudo chmod 666 $LIBRBD_DEV
  dd if=$LIBRBD_DEV of=/tmp/cmpdata iflag=direct bs=1M count=2
  cmp -n 2MB /tmp/cmpdata /tmp/testdata1
  dd if=/tmp/testdata1 of=$LIBRBD_DEV seek=2 skip=2 oflag=direct bs=1M count=1
  sudo rbd device unmap -t nbd $LIBRBD_DEV

  # flatten
  expect_false rbd flatten testimg2 --encryption-format luks1 --encryption-format luks2 --encryption-passphrase-file /tmp/passphrase2 --encryption-passphrase-file /tmp/passphrase
  rbd flatten testimg2 --encryption-format luks2 --encryption-format luks1 --encryption-passphrase-file /tmp/passphrase2 --encryption-passphrase-file /tmp/passphrase

  # verify with cryptsetup
  RAW_FLAT_DEV=$(_sudo rbd -p rbd map testimg2 -t nbd)
  sudo cryptsetup open $RAW_FLAT_DEV --type luks cryptsetupdev -d /tmp/passphrase2
  sudo chmod 666 /dev/mapper/cryptsetupdev
  dd if=/dev/mapper/cryptsetupdev of=/tmp/cmpdata iflag=direct bs=1M count=3
  cmp -n 3MB /tmp/cmpdata /tmp/testdata1
  sudo rbd device unmap -t nbd $RAW_FLAT_DEV
}

function test_clone_and_load_with_a_single_passphrase {
  local expectedfail=$1

  # clone and format
  rbd snap create testimg@snap
  rbd snap protect testimg@snap
  rbd clone testimg@snap testimg1
  rbd encryption format testimg1 luks2 /tmp/passphrase2

  if [ "$expectedfail" = "true" ]
  then
    expect_false rbd flatten testimg1 --encryption-passphrase-file /tmp/passphrase2
    rbd flatten testimg1 --encryption-passphrase-file /tmp/passphrase2 --encryption-passphrase-file /tmp/passphrase
  else
    rbd flatten testimg1 --encryption-passphrase-file /tmp/passphrase2
  fi

  rbd remove testimg1
  rbd snap unprotect testimg@snap
  rbd snap remove testimg@snap
}

function test_plaintext_detection {
  # 16k LUKS header
  sudo cryptsetup -q luksFormat --type luks2 --luks2-metadata-size 16k $RAW_DEV /tmp/passphrase
  test_clone_and_load_with_a_single_passphrase true

  # 4m LUKS header
  sudo cryptsetup -q luksFormat --type luks2 --luks2-metadata-size 4m $RAW_DEV /tmp/passphrase
  test_clone_and_load_with_a_single_passphrase true

  # no luks header
  dd if=/dev/zero of=$RAW_DEV oflag=direct bs=4M count=8
  test_clone_and_load_with_a_single_passphrase false
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


if [[ $(($(ceph-conf --name client.${CEPH_ID} --show-config-value rbd_default_features) & 64)) != 0 ]]; then
	echo "LUKS encryption tests not supported alongside image journaling feature"
	exit 0
fi

clean_up

trap clean_up INT TERM EXIT

# generate test data
dd if=/dev/urandom of=/tmp/testdata1 bs=4M count=4
dd if=/dev/urandom of=/tmp/testdata2 bs=4M count=4

# create passphrase files
printf "pass\0word\n" > /tmp/passphrase
printf "\t password2   " > /tmp/passphrase2

# create an image
rbd create testimg --size=32M

# map raw data to nbd device
RAW_DEV=$(_sudo rbd -p rbd map testimg -t nbd)
sudo chmod 666 $RAW_DEV

test_plaintext_detection

test_encryption_format luks1
test_encryption_format luks2

test_clone_encryption

echo OK
