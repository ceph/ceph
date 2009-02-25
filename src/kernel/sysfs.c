#include <linux/kobject.h>

#include "super.h"

struct kobject *ceph_kobj;

struct kobj_type client_type = {
	.sysfs_ops = &kobj_sysfs_ops,
};

#define to_client(c) container_of(c, struct ceph_client, kobj)


ssize_t fsid_show(struct kobject *k_client, struct kobj_attribute *attr,
		  char *buf)
{
	struct ceph_client *client = to_client(k_client);
	
	return sprintf(buf, "%llx.%llx\n",
	       le64_to_cpu(__ceph_fsid_major(&client->monc.monmap->fsid)),
	       le64_to_cpu(__ceph_fsid_minor(&client->monc.monmap->fsid)));
}

#define ADD_ATTR(a, n, m, sh, st) \
	client->a.attr.name = n; \
	client->a.attr.mode = m; \
	client->a.show = sh; \
	client->a.store = st; \
	sysfs_create_file(&client->kobj, &client->a.attr);

int ceph_sysfs_client_init(struct ceph_client *client)
{
	int ret = 0;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 25)
	ret = kobject_init_and_add(&client->kobj, &client_type,
				   ceph_kobj, "client%d", client->whoami);
	if (ret)
		goto out;

	ADD_ATTR(k_fsid, "fsid", 0400, fsid_show, NULL);

out:
#endif
	return ret;
}

void ceph_sysfs_client_cleanup(struct ceph_client *client)
{
}

/*
 * sys/fs/ceph
 */

int ceph_sysfs_init()
{
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 25)
	ceph_kobj = kobject_create_and_add("ceph", fs_kobj);
	if (!ceph_kobj) {
		return -ENOMEM;
	}
#endif
	return 0;
}

void ceph_sysfs_cleanup()
{
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 25)
	kobject_put(ceph_kobj);
	ceph_kobj = NULL;
#endif
}
