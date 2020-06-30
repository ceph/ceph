#! /usr/bin/env bash
ed /usr/share/ceph-ansible/group_vars/osds << EOF
$
/^devices:
.+1
i
   - /dev/sdb
   - /dev/sdc
   - /dev/sdd
.
w
q
EOF
