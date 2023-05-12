
#pragma once

struct ceph_mount_info;

void libcephfs_test_set_mount_call(int (*mount_call)(struct ceph_mount_info *cmount, const char *root));
