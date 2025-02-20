// -*- mode:C++; tab-width:2; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 smarttab

#include <stdlib.h>
#include "include/cephfs/libcephfs.h"
#include "include/rados/librados.h"

#ifdef __cplusplus
#error "test invalid: only use C mode"
#endif

int main(int argc, char **argv)
{
	int ret;

	/* librados.h */
	rados_t cluster;
	ret = rados_create(&cluster, NULL);
	if (ret < 0) {
		return EXIT_FAILURE;
	}
	/* libcephfs.h */
	struct ceph_mount_info *cmount;
	ret = ceph_create(&cmount, NULL);
	if (ret < 0) {
		return EXIT_FAILURE;
	}

	return 0;
}
