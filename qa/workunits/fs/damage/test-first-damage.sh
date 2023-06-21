#!/bin/bash

set -ex

FIRST_DAMAGE="first-damage.py"
FS=cephfs
METADATA_POOL=cephfs_meta
MOUNT=~/mnt/mnt.0
PYTHON=python3

function usage {
  printf '%s: [--fs=<fs_name>] [--metadata-pool=<pool>] [--first-damage=</path/to/first-damage.py>]\n'
  exit 1
}


function create {
  ceph config set mds mds_bal_fragment_dirs 0
  mkdir dir
  DIR_INODE=$(stat -c '%i' dir)
  touch dir/a
  touch dir/"a space"
  touch -- $(printf 'dir/\xff')
  mkdir dir/.snap/1
  mkdir dir/.snap/2
  # two snaps
  rm dir/a
  mkdir dir/.snap/3
  # not present in HEAD
  touch dir/a
  mkdir dir/.snap/4
  # one snap
  rm dir/a
  touch dir/a
  mkdir dir/.snap/5
  # unlink then create
  rm dir/a
  touch dir/a
  # unlink then create, HEAD not snapped
  ls dir/.snap/*/
  mkdir big
  BIG_DIR_INODE=$(stat -c '%i' big)
  for i in `seq 1 15000`; do
    touch $(printf 'big/%08d' $i)
  done
}

function flush {
  ceph tell mds."$FS":0 flush journal
}

function damage {
  local IS=$(printf '%llx.%08llx' "$DIR_INODE" 0)
  local LS=$(ceph tell mds."$FS":0 dump snaps | jq .last_created)

  local T=$(mktemp -p /tmp)

  # nuke snap 1 version of "a"
  rados --pool="$METADATA_POOL" getomapval "$IS" a_$(printf %x $((LS-4))) "$T"
  printf '\xff\xff\xff\xf0' | dd of="$T" count=4 bs=1 conv=notrunc,nocreat
  rados --pool="$METADATA_POOL" setomapval "$IS" a_$(printf %x $((LS-4))) --input-file="$T"

  # nuke snap 4 version of "a"
  rados --pool="$METADATA_POOL" getomapval "$IS" a_$(printf %x $((LS-1))) "$T"
  printf '\xff\xff\xff\xff' | dd of="$T" count=4 bs=1 conv=notrunc,nocreat
  rados --pool="$METADATA_POOL" setomapval "$IS" a_$(printf %x $((LS-1))) --input-file="$T"

  # screw up HEAD
  rados --pool="$METADATA_POOL" getomapval "$IS" a_head "$T"
  printf '\xfe\xff\xff\xff' | dd of="$T" count=4 bs=1 conv=notrunc,nocreat
  rados --pool="$METADATA_POOL" setomapval "$IS" a_head --input-file="$T"

  # screw up HEAD on what dentry in big
  IS=$(printf '%llx.%08llx' "$BIG_DIR_INODE" 0)
  rados --pool="$METADATA_POOL" getomapval "$IS" 00009999_head "$T"
  printf '\xfe\xff\xff\xff' | dd of="$T" count=4 bs=1 conv=notrunc,nocreat
  rados --pool="$METADATA_POOL" setomapval "$IS" 00009999_head --input-file="$T"

  rm -f "$T"
}

function recover {
  flush
  ceph fs fail "$FS"
  sleep 5
  cephfs-journal-tool --rank="$FS":0 event recover_dentries summary
  cephfs-journal-tool --rank="$FS":0 journal reset
  "$PYTHON" $FIRST_DAMAGE --debug /tmp/debug1 --memo /tmp/memo1 "$METADATA_POOL"
  "$PYTHON" $FIRST_DAMAGE --debug /tmp/debug2 --memo /tmp/memo2 --repair-nosnap  "$METADATA_POOL"
  "$PYTHON" $FIRST_DAMAGE --debug /tmp/debug3 --memo /tmp/memo3 --remove "$METADATA_POOL"
  ceph fs set "$FS" joinable true
}

function check {
  stat dir || exit 1
  stat dir/a || exit 1
  for i in `seq 1 5`; do
    stat dir/.snap/$i || exit 2
  done
  stat dir/.snap/2/a || exit 3
  stat dir/.snap/5/a || exit 4
  if stat dir/.snap/1/a; then
    echo should be gone
    exit 5
  fi
  if stat dir/.snap/3/a; then
    echo should not ever exist
    exit 6
  fi
  if stat dir/.snap/4/a; then
    echo should be gone
    exit 7
  fi
}

function cleanup {
  rmdir dir/.snap/*
  find dir
  rm -rf dir
}

function mount {
  sudo --preserve-env=CEPH_CONF bin/mount.ceph :/ "$MOUNT" -o name=admin,noshare
  df -h "$MOUNT"
}

function main {
  eval set -- $(getopt --name "$0" --options '' --longoptions 'help,fs:,metadata-pool:,first-damage:,mount:,python:' -- "$@")

  while [ "$#" -gt 0 ]; do
      echo "$*"
      echo "$1"
      case "$1" in
          -h|--help)
              usage
              ;;
          --fs)
              FS="$2"
              shift 2
              ;;
          --metadata-pool)
              METADATA_POOL="$2"
              shift 2
              ;;
          --mount)
              MOUNT="$2"
              shift 2
              ;;
          --first-damage)
              FIRST_DAMAGE="$2"
              shift 2
              ;;
          --python)
              PYTHON="$2"
              shift 2
              ;;
          --)
              shift
              break
              ;;
          *)
              usage
              ;;
      esac
  done

  mount

  pushd "$MOUNT"
  create
  popd

  sudo umount -f "$MOUNT"

  # flush dentries/inodes to omap
  flush

  damage

  recover

  sleep 5 # for mds to join

  mount

  pushd "$MOUNT"
  check
  cleanup
  popd

  sudo umount -f "$MOUNT"
}

main "$@"
