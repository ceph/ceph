#include "include/cephfs/libcephfs.h"
#include "include/rados/librados.h"

#ifdef __cplusplus
#error "test invalid: only use C mode"
#endif

int main(int argc, char **argv)
{
	int ret;
	(void)ret; // squash unused warning

	/* librados.h */
	rados_t cluster;
	ret = rados_create(&cluster, NULL);

	/* libcephfs.h */
	struct ceph_mount_info *cmount;
	ret = ceph_create(&cmount, NULL);

	return 0;
}
