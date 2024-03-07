#!/bin/bash

set -ex

FIRST_DAMAGE="first-damage.py"
FS=cephfs
METADATA_POOL=cephfs.a.meta
DATA_POOL=cephfs.a.data
MOUNT=/mnt1
PYTHON=python3

function usage {
  printf '%s: [--fs=<fs_name>] [--metadata-pool=<pool>] [--first-damage=</path/to/first-damage.py>]\n'
  exit 1
}


function create {
  ceph config set mds mds_bal_fragment_dirs 0
  mkdir dir1
  DIR1_INODE=$(stat -c '%i' dir1)
  touch dir1/file1
  DIR1_FILE1_INODE=$(stat -c '%i' dir1/file1)
}

function flush {
  ceph tell mds."$FS":0 flush journal
}

function damage_backtrace {
  flush
  ceph fs fail "$FS"
  sleep 5

  cephfs-journal-tool --rank="$FS":0 event recover_dentries summary
  # required here as the flush would re-write the below deleted omap
  cephfs-journal-tool --rank="$FS":0 journal reset

  #remove dir1/file1 omap entry from metadata pool
  local DIS=$(printf '%llx.%08llx' "$DIR1_INODE" 0)
  rados --pool="$METADATA_POOL" rmomapkey "$DIS" "file1_head"

  #remove backtrace
  local FIS=$(printf '%llx.%08llx' "$DIR1_FILE1_INODE" 0)
  rados --pool="$DATA_POOL" rmxattr "$FIS" "parent"

  ceph fs set "$FS" joinable true
  sleep 5
}

function damage_lost_found {
  flush
  ceph fs fail "$FS"
  sleep 5
  local IS=$(printf '%llx.%08llx' "1" 0)

  local T=$(mktemp -p /tmp)
  # nuke head version of "lost+found"
  rados --pool="$METADATA_POOL" getomapval "$IS" lost+found_head "$T"
  printf '\xff\xff\xff\xf0' | dd of="$T" count=4 bs=1 conv=notrunc,nocreat
  rados --pool="$METADATA_POOL" setomapval "$IS" lost+found_head --input-file="$T"
  ceph fs set "$FS" joinable true
  sleep 5
}

function recover_damaged_backtrace_file {
  flush
  ceph fs fail "$FS"
  sleep 5

  cephfs-journal-tool --rank="$FS":0 journal reset

  #creates lost+found directory and recovers the damaged backtrace file
  cephfs-data-scan cleanup
  cephfs-data-scan init
  cephfs-data-scan scan_extents
  cephfs-data-scan scan_inodes
  cephfs-data-scan scan_links

  ceph fs set "$FS" joinable true
  sleep 5
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
  sleep 5
}

function check_lost_found {
  stat lost+found || exit 2
}
function check {
  if stat lost+found; then
    echo should be gone
    exit 1
  fi
}

function mount {
  #sudo --preserve-env=CEPH_CONF bin/mount.ceph :/ "$MOUNT" -o name=admin,noshare
  sudo bin/ceph-fuse -c ./ceph.conf /mnt1
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

  damage_backtrace
  # creates lost+found directory
  recover_damaged_backtrace_file

  sleep 5 # for mds to join
  mount
  pushd "$MOUNT"
  sleep 5 # wait for mount to complete

  # check lost+found is created
  check_lost_found
  popd
  sudo umount -f "$MOUNT"
  # flush dentries/inodes to omap
  flush

  # damage lost+found directory
  damage_lost_found
  recover

  mount

  pushd "$MOUNT"
  sleep 5 # wait for mount to complete

  #check 'lost+found' dentry should be gone
  check
  popd

  sudo umount -f "$MOUNT"
}

main "$@"
