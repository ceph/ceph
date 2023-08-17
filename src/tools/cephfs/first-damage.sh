#!/bin/bash

# Ceph - scalable distributed file system
#
# Copyright (C) 2022 Red Hat, Inc.
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.

# Suggested recovery sequence (for single MDS cluster):
#
# 1) Unmount all clients.
#
# 2) Flush the journal (if possible):
#
#    ceph tell mds.<fs_name>:0 flush journal
#
# 3) Fail the file system:
#
#    ceph fs fail <fs_name>
#
# 4a) Recover dentries from the journal. This will be a no-op if the MDS flushed the journal successfully:
#
#    cephfs-journal-tool --rank=<fs_name>:0 event recover_dentries summary
#
# 4b) If all good so far, reset the journal:
#
#    cephfs-journal-tool --rank=<fs_name>:0 journal reset
#
# 5) Run this tool to see list of damaged dentries:
#
#    first-damage.sh <pool>
#
# 6) Optionally, remove them:
#
#    first-damage.sh --remove <pool>
#
# This has the effect of removing that dentry from the snapshot or HEAD
# (current hierarchy).  Note: the inode's linkage will be lost. The inode may
# be recoverable in lost+found during a future data scan recovery.

set -ex

function usage {
  printf '%s: [--remove] <metadata pool> [newest snapid]\n' "$0"
  printf '  remove CephFS metadata dentries with invalid first snapshot'
  exit 1
}

function mrados {
  rados --pool="$METADATA_POOL" "$@"
}

function traverse {
  local T=$(mktemp -p /tmp MDS_TRAVERSAL.XXXXXX)
  mrados ls | grep -E '[[:xdigit:]]{8,}\.[[:xdigit:]]+' > "$T"
  while read obj; do
    local O=$(mktemp -p /tmp "$obj".XXXXXX)
    for dnk in $(mrados listomapkeys "$obj"); do
      mrados getomapval "$obj" "$dnk" "$O"
      local first=$(dd if="$O" bs=1 count=4 | od --endian=little -An -t u8)
      if [ "$first" -gt "$NEXT_SNAP" ]; then
        printf 'found "%s:%s" first (0x%x) > NEXT_SNAP (0x%x)\n' "$obj" "$dnk" "$first" "$NEXT_SNAP"
        if [ "$REMOVE" -ne 0 ]; then
          printf 'removing "%s:%s"\n' "$obj" "$dnk"
          mrados rmomapkey "$obj" "$dnk"
        fi
      fi
    done
    rm "$O"
  done < "$T"
}

function main {
    eval set -- $(getopt --name "$0" --options 'r' --longoptions 'help,remove' -- "$@")

    while [ "$#" -gt 0 ]; do
        case "$1" in
            -h|--help)
                usage
                ;;
            --remove)
                REMOVE=1
                shift
                ;;
            --)
                shift
                break
                ;;
        esac
    done

  if [ -z "$1" ]; then
    usage
  fi
  METADATA_POOL="$1"
  NEXT_SNAP="$2"

  if [ -z "$NEXT_SNAP" ]; then
    SNAPTABLE=$(mktemp -p /tmp MDS_SNAPTABLE.XXXXXX)
    rados --pool="$METADATA_POOL" get mds_snaptable "$SNAPTABLE"
    # skip "version" of MDSTable payload
    V=$(dd if="$SNAPTABLE" bs=1 count=1 skip=8 | od --endian=little -An -t u1)
    if [ "$V" -ne 5 ]; then
      printf 'incompatible snaptable\n'
      exit 2
    fi
    # skip version,struct_v,compat_v,length
    NEXT_SNAP=$((1 + $(dd if="$SNAPTABLE" bs=1 count=8 skip=14 | od --endian=little -An -t u8)))
    printf 'found latest snap: %d\n' "$NEXT_SNAP"
  fi

  traverse
}

main "$@"
