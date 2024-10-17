#!/usr/bin/env bash
set -ex

CEPH_ID=${CEPH_ID:-admin}
TMP_FILES="/tmp/passphrase /tmp/passphrase1 /tmp/passphrase2 /tmp/testdata1 /tmp/testdata2 /tmp/cmpdata /tmp/rawexport /tmp/export.qcow2"

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

  # format
  rbd encryption format testimg $format /tmp/passphrase
  drop_caches

  # open encryption with cryptsetup
  sudo cryptsetup open $RAW_DEV --type luks cryptsetupdev -d /tmp/passphrase

  # open encryption with librbd
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg -t nbd -o encryption-passphrase-file=/tmp/passphrase)

  # write via librbd && compare
  dd if=/tmp/testdata1 of=$LIBRBD_DEV conv=fsync bs=1M
  dd if=/dev/mapper/cryptsetupdev of=/tmp/cmpdata iflag=direct bs=4M count=4
  cmp -n 16MB /tmp/cmpdata /tmp/testdata1

  # write via cryptsetup && compare
  dd if=/tmp/testdata2 of=/dev/mapper/cryptsetupdev conv=fsync bs=1M
  dd if=$LIBRBD_DEV of=/tmp/cmpdata iflag=direct bs=4M count=4
  cmp -n 16MB /tmp/cmpdata /tmp/testdata2

  # FIXME: encryption-aware flatten/resize misbehave if proxied to
  # RAW_DEV mapping (i.e. if RAW_DEV mapping ows the lock)
  # (acquire and) release the lock as a side effect
  rbd bench --io-type read --io-size 1 --io-threads 1 --io-total 1 testimg

  # check that encryption-aware resize compensates LUKS header overhead
  (( $(sudo blockdev --getsize64 $LIBRBD_DEV) < (32 << 20) ))
  expect_false rbd resize --size 32M testimg
  rbd resize --size 32M --encryption-passphrase-file /tmp/passphrase testimg
  (( $(sudo blockdev --getsize64 $LIBRBD_DEV) == (32 << 20) ))

  _sudo rbd device unmap -t nbd $LIBRBD_DEV
  sudo cryptsetup close cryptsetupdev
}

function test_clone_encryption() {
  # write 1MB plaintext
  dd if=/tmp/testdata1 of=$RAW_DEV conv=fsync bs=1M count=1

  # clone (luks1)
  rbd snap create testimg@snap
  rbd snap protect testimg@snap
  rbd clone testimg@snap testimg1
  rbd encryption format testimg1 luks1 /tmp/passphrase

  # open encryption with librbd, write one more MB, close
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg1 -t nbd -o encryption-format=luks1,encryption-passphrase-file=/tmp/passphrase)
  dd if=$LIBRBD_DEV of=/tmp/cmpdata bs=1M count=1
  cmp -n 1MB /tmp/cmpdata /tmp/testdata1
  dd if=/tmp/testdata1 of=$LIBRBD_DEV seek=1 skip=1 conv=fsync bs=1M count=1
  _sudo rbd device unmap -t nbd $LIBRBD_DEV

  # second clone (luks2)
  rbd snap create testimg1@snap
  rbd snap protect testimg1@snap
  rbd clone testimg1@snap testimg2
  rbd encryption format testimg2 luks2 /tmp/passphrase2

  # open encryption with librbd, write one more MB, close
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg2 -t nbd -o encryption-format=luks2,encryption-passphrase-file=/tmp/passphrase2,encryption-format=luks1,encryption-passphrase-file=/tmp/passphrase)
  dd if=$LIBRBD_DEV of=/tmp/cmpdata bs=1M count=2
  cmp -n 2MB /tmp/cmpdata /tmp/testdata1
  dd if=/tmp/testdata1 of=$LIBRBD_DEV seek=2 skip=2 conv=fsync bs=1M count=1
  _sudo rbd device unmap -t nbd $LIBRBD_DEV

  # flatten
  expect_false rbd flatten testimg2 --encryption-format luks1 --encryption-format luks2 --encryption-passphrase-file /tmp/passphrase2 --encryption-passphrase-file /tmp/passphrase
  rbd flatten testimg2 --encryption-format luks2 --encryption-format luks1 --encryption-passphrase-file /tmp/passphrase2 --encryption-passphrase-file /tmp/passphrase

  # verify with cryptsetup
  RAW_FLAT_DEV=$(_sudo rbd -p rbd map testimg2 -t nbd)
  sudo cryptsetup open $RAW_FLAT_DEV --type luks cryptsetupdev -d /tmp/passphrase2
  dd if=/dev/mapper/cryptsetupdev of=/tmp/cmpdata bs=1M count=3
  cmp -n 3MB /tmp/cmpdata /tmp/testdata1
  sudo cryptsetup close cryptsetupdev
  _sudo rbd device unmap -t nbd $RAW_FLAT_DEV

  rbd rm testimg2
  rbd snap unprotect testimg1@snap
  rbd snap rm testimg1@snap
  rbd rm testimg1
  rbd snap unprotect testimg@snap
  rbd snap rm testimg@snap
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
  dd if=/dev/zero of=$RAW_DEV conv=fsync bs=4M count=8
  test_clone_and_load_with_a_single_passphrase false
}

function test_migration_read_and_copyup() {
  cp /tmp/testdata2 /tmp/cmpdata

  # test reading
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg1 -t nbd -o encryption-passphrase-file=/tmp/passphrase)
  cmp $LIBRBD_DEV /tmp/cmpdata

  # trigger copyup at the beginning and at the end
  xfs_io -c 'pwrite -S 0xab -W 0 4k' $LIBRBD_DEV /tmp/cmpdata
  xfs_io -c 'pwrite -S 0xba -W 4095k 4k' $LIBRBD_DEV /tmp/cmpdata

  cmp $LIBRBD_DEV /tmp/cmpdata
  _sudo rbd device unmap -t nbd $LIBRBD_DEV

  # test reading on a fresh mapping
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg1 -t nbd -o encryption-passphrase-file=/tmp/passphrase)
  cmp $LIBRBD_DEV /tmp/cmpdata
  _sudo rbd device unmap -t nbd $LIBRBD_DEV

  # test reading on a fresh mapping after migration is executed
  rbd migration execute testimg1
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg1 -t nbd -o encryption-passphrase-file=/tmp/passphrase)
  cmp $LIBRBD_DEV /tmp/cmpdata
  _sudo rbd device unmap -t nbd $LIBRBD_DEV

  # test reading on a fresh mapping after migration is committed
  rbd migration commit testimg1
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg1 -t nbd -o encryption-passphrase-file=/tmp/passphrase)
  cmp $LIBRBD_DEV /tmp/cmpdata
  _sudo rbd device unmap -t nbd $LIBRBD_DEV
}

function test_migration_native_with_snaps() {
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg1@snap1 -t nbd -o encryption-passphrase-file=/tmp/passphrase)
  cmp $LIBRBD_DEV /tmp/testdata1
  _sudo rbd device unmap -t nbd $LIBRBD_DEV
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg1@snap2 -t nbd -o encryption-passphrase-file=/tmp/passphrase)
  cmp $LIBRBD_DEV /tmp/testdata2
  _sudo rbd device unmap -t nbd $LIBRBD_DEV

  test_migration_read_and_copyup

  # check that snapshots aren't affected by copyups
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg1@snap1 -t nbd -o encryption-passphrase-file=/tmp/passphrase)
  cmp $LIBRBD_DEV /tmp/testdata1
  _sudo rbd device unmap -t nbd $LIBRBD_DEV
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg1@snap2 -t nbd -o encryption-passphrase-file=/tmp/passphrase)
  cmp $LIBRBD_DEV /tmp/testdata2
  _sudo rbd device unmap -t nbd $LIBRBD_DEV

  rbd snap rm testimg1@snap2
  rbd snap rm testimg1@snap1
  rbd rm testimg1
}

function test_migration() {
  local format=$1

  rbd encryption format testimg $format /tmp/passphrase

  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg -t nbd -o encryption-passphrase-file=/tmp/passphrase)
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
  test_migration_read_and_copyup
  rbd rm testimg1

  # live import a qcow image
  qemu-img convert -f raw -O qcow2 /tmp/rawexport /tmp/export.qcow2
  rbd migration prepare --import-only --source-spec '{"type": "qcow", "stream": {"type": "file", "file_path": "/tmp/export.qcow2"}}' testimg1
  test_migration_read_and_copyup
  rbd rm testimg1

  # live import a native image
  rbd migration prepare --import-only testimg@snap2 testimg1
  test_migration_native_with_snaps

  # live migrate a native image (removes testimg)
  rbd migration prepare testimg testimg1
  test_migration_native_with_snaps

  rm /tmp/rawexport /tmp/export.qcow2
}

function test_migration_clone() {
  local format=$1

  truncate -s 0 /tmp/cmpdata
  truncate -s 32M /tmp/cmpdata

  rbd encryption format testimg $format /tmp/passphrase
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg -t nbd -o encryption-passphrase-file=/tmp/passphrase)
  xfs_io -c 'pwrite -S 0xaa -W 4M 1M' $LIBRBD_DEV /tmp/cmpdata
  xfs_io -c 'pwrite -S 0xaa -W 14M 1M' $LIBRBD_DEV /tmp/cmpdata
  xfs_io -c 'pwrite -S 0xaa -W 25M 1M' $LIBRBD_DEV /tmp/cmpdata
  _sudo rbd device unmap -t nbd $LIBRBD_DEV

  rbd snap create testimg@snap
  rbd snap protect testimg@snap
  rbd clone testimg@snap testimg1

  rbd encryption format testimg1 $format /tmp/passphrase2
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg1 -t nbd -o encryption-passphrase-file=/tmp/passphrase2,encryption-passphrase-file=/tmp/passphrase)
  xfs_io -c 'pwrite -S 0xbb -W 2M 1M' $LIBRBD_DEV /tmp/cmpdata
  xfs_io -c 'pwrite -S 0xbb -W 19M 1M' $LIBRBD_DEV /tmp/cmpdata
  xfs_io -c 'pwrite -S 0xbb -W 28M 1M' $LIBRBD_DEV /tmp/cmpdata
  _sudo rbd device unmap -t nbd $LIBRBD_DEV

  # FIXME: https://tracker.ceph.com/issues/67402
  rbd config image set testimg1 rbd_sparse_read_threshold_bytes 1

  # live migrate a native clone image (removes testimg1)
  rbd migration prepare testimg1 testimg2

  # test reading
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg2 -t nbd -o encryption-passphrase-file=/tmp/passphrase2,encryption-passphrase-file=/tmp/passphrase)
  cmp $LIBRBD_DEV /tmp/cmpdata

  # trigger copyup for an unwritten area
  xfs_io -c 'pwrite -S 0xcc -W 24167k 4k' $LIBRBD_DEV /tmp/cmpdata

  # trigger copyup for areas written in testimg (parent)
  xfs_io -c 'pwrite -S 0xcc -W 4245k 4k' $LIBRBD_DEV /tmp/cmpdata
  xfs_io -c 'pwrite -S 0xcc -W 13320k 4k' $LIBRBD_DEV /tmp/cmpdata

  # trigger copyup for areas written in testimg1 (clone)
  xfs_io -c 'pwrite -S 0xcc -W 2084k 4k' $LIBRBD_DEV /tmp/cmpdata
  xfs_io -c 'pwrite -S 0xcc -W 32612k 4k' $LIBRBD_DEV /tmp/cmpdata

  cmp $LIBRBD_DEV /tmp/cmpdata
  _sudo rbd device unmap -t nbd $LIBRBD_DEV

  # test reading on a fresh mapping
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg2 -t nbd -o encryption-passphrase-file=/tmp/passphrase2,encryption-passphrase-file=/tmp/passphrase)
  cmp $LIBRBD_DEV /tmp/cmpdata
  _sudo rbd device unmap -t nbd $LIBRBD_DEV

  # test reading on a fresh mapping after migration is executed
  rbd migration execute testimg2
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg2 -t nbd -o encryption-passphrase-file=/tmp/passphrase2,encryption-passphrase-file=/tmp/passphrase)
  cmp $LIBRBD_DEV /tmp/cmpdata
  _sudo rbd device unmap -t nbd $LIBRBD_DEV

  # test reading on a fresh mapping after migration is committed
  rbd migration commit testimg2
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg2 -t nbd -o encryption-passphrase-file=/tmp/passphrase2,encryption-passphrase-file=/tmp/passphrase)
  cmp $LIBRBD_DEV /tmp/cmpdata
  _sudo rbd device unmap -t nbd $LIBRBD_DEV

  rbd rm testimg2
  rbd snap unprotect testimg@snap
  rbd snap rm testimg@snap
  rbd rm testimg
}

function test_migration_open_clone_chain() {
  rbd create --size 32M testimg
  rbd encryption format testimg luks1 /tmp/passphrase
  rbd snap create testimg@snap
  rbd snap protect testimg@snap

  rbd clone testimg@snap testimg1
  rbd encryption format testimg1 luks2 /tmp/passphrase1
  rbd snap create testimg1@snap
  rbd snap protect testimg1@snap

  rbd clone testimg1@snap testimg2
  rbd encryption format testimg2 luks1 /tmp/passphrase2

  # 1. X <-- X <-- X
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg2 -t nbd -o encryption-passphrase-file=/tmp/passphrase2,encryption-passphrase-file=/tmp/passphrase1,encryption-passphrase-file=/tmp/passphrase)
  _sudo rbd device unmap -t nbd $LIBRBD_DEV

  # 2. X <-- X <-- migrating
  rbd migration prepare testimg2 testimg2
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg2 -t nbd -o encryption-passphrase-file=/tmp/passphrase2,encryption-passphrase-file=/tmp/passphrase1,encryption-passphrase-file=/tmp/passphrase)
  _sudo rbd device unmap -t nbd $LIBRBD_DEV
  rbd migration abort testimg2

  # 3. X <-- migrating <-- X
  rbd migration prepare testimg1 testimg1
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg2 -t nbd -o encryption-passphrase-file=/tmp/passphrase2,encryption-passphrase-file=/tmp/passphrase1,encryption-passphrase-file=/tmp/passphrase)
  _sudo rbd device unmap -t nbd $LIBRBD_DEV
  rbd migration abort testimg1

  # 4. migrating <-- X <-- X
  rbd migration prepare testimg testimg
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg2 -t nbd -o encryption-passphrase-file=/tmp/passphrase2,encryption-passphrase-file=/tmp/passphrase1,encryption-passphrase-file=/tmp/passphrase)
  _sudo rbd device unmap -t nbd $LIBRBD_DEV
  rbd migration abort testimg

  # 5. migrating <-- migrating <-- X
  rbd migration prepare testimg testimg
  rbd migration prepare testimg1 testimg1
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg2 -t nbd -o encryption-passphrase-file=/tmp/passphrase2,encryption-passphrase-file=/tmp/passphrase1,encryption-passphrase-file=/tmp/passphrase)
  _sudo rbd device unmap -t nbd $LIBRBD_DEV
  rbd migration abort testimg1
  rbd migration abort testimg

  # 6. migrating <-- X <-- migrating
  rbd migration prepare testimg testimg
  rbd migration prepare testimg2 testimg2
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg2 -t nbd -o encryption-passphrase-file=/tmp/passphrase2,encryption-passphrase-file=/tmp/passphrase1,encryption-passphrase-file=/tmp/passphrase)
  _sudo rbd device unmap -t nbd $LIBRBD_DEV
  rbd migration abort testimg2
  rbd migration abort testimg

  # 7. X <-- migrating <-- migrating
  rbd migration prepare testimg1 testimg1
  rbd migration prepare testimg2 testimg2
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg2 -t nbd -o encryption-passphrase-file=/tmp/passphrase2,encryption-passphrase-file=/tmp/passphrase1,encryption-passphrase-file=/tmp/passphrase)
  _sudo rbd device unmap -t nbd $LIBRBD_DEV
  rbd migration abort testimg2
  rbd migration abort testimg1

  # 8. migrating <-- migrating <-- migrating
  rbd migration prepare testimg testimg
  rbd migration prepare testimg1 testimg1
  rbd migration prepare testimg2 testimg2
  LIBRBD_DEV=$(_sudo rbd -p rbd map testimg2 -t nbd -o encryption-passphrase-file=/tmp/passphrase2,encryption-passphrase-file=/tmp/passphrase1,encryption-passphrase-file=/tmp/passphrase)
  _sudo rbd device unmap -t nbd $LIBRBD_DEV

  rbd migration abort testimg2
  rbd rm testimg2
  rbd migration abort testimg1
  rbd snap unprotect testimg1@snap
  rbd snap rm testimg1@snap
  rbd rm testimg1
  rbd migration abort testimg
  rbd snap unprotect testimg@snap
  rbd snap rm testimg@snap
  rbd rm testimg
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

  rbd migration abort testimg2 || true
  rbd remove testimg2 || true
  rbd migration abort testimg1 || true
  rbd snap remove testimg1@snap2 || true
  rbd snap remove testimg1@snap1 || true
  rbd snap unprotect testimg1@snap || true
  rbd snap remove testimg1@snap || true
  rbd remove testimg1 || true
  rbd migration abort testimg || true
  rbd snap remove testimg@snap2 || true
  rbd snap remove testimg@snap1 || true
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
printf "  passwo\nrd 1,1" > /tmp/passphrase1
printf "\t password2   " > /tmp/passphrase2

# create an image
rbd create testimg --size=32M

# map raw data to nbd device
RAW_DEV=$(_sudo rbd -p rbd map testimg -t nbd)

test_plaintext_detection

test_encryption_format luks1
test_encryption_format luks2

test_clone_encryption

_sudo rbd device unmap -t nbd $RAW_DEV
rbd rm testimg

rbd create --size 20M testimg
test_migration luks1

rbd create --size 32M testimg
test_migration luks2

rbd create --size 36M testimg
test_migration_clone luks1

rbd create --size 48M testimg
test_migration_clone luks2

test_migration_open_clone_chain

echo OK
