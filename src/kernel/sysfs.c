#include <linux/ctype.h>
#include <linux/kobject.h>

#include "super.h"

struct kobject ceph_kobj;

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
	ret = sysfs_create_file(&client->kobj, &client->a.attr);

int ceph_sysfs_client_init(struct ceph_client *client)
{
	int ret = 0;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 25)
	ret = kobject_init_and_add(&client->kobj, &client_type,
				   &ceph_kobj, "client%d", client->whoami);
	if (ret)
		goto out;

	ADD_ATTR(k_fsid, "fsid", 0400, fsid_show, NULL);
	return 0;

out:
#endif
	return ret;
}

void ceph_sysfs_client_cleanup(struct ceph_client *client)
{
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 25)
	kobject_del(&client->kobj);
#endif	
}

/*
 * /sys/fs/ceph attrs
 */
struct ceph_attr {
	struct attribute attr;
	ssize_t (*show)(struct kobject *, struct attribute *, char *);
	ssize_t (*store)(struct kobject *, struct attribute *, const char *, size_t);
	int *val;
};

static ssize_t ceph_show(struct kobject *ko, struct attribute *a, char *buf)
{
	return container_of(a, struct ceph_attr, attr)->show(ko, a, buf);
}

static ssize_t ceph_store(struct kobject *ko, struct attribute *a,
			  const char *buf, size_t len)
{
	return container_of(a, struct ceph_attr, attr)->store(ko, a, buf, len);
}

struct sysfs_ops ceph_sysfs_ops = {
	.show = ceph_show,
	.store = ceph_store,
};

struct kobj_type ceph_type = {
	.sysfs_ops = &ceph_sysfs_ops,
};

/*
 * simple int attrs
 */
static ssize_t attr_show(struct kobject *ko, struct attribute *a, char *buf)
{
	struct ceph_attr *ca = container_of(a, struct ceph_attr, attr);

	return sprintf(buf, "%d\n", *ca->val);
}

static ssize_t attr_store(struct kobject *ko, struct attribute *a,
			  const char *buf, size_t len)
{
	struct ceph_attr *ca = container_of(a, struct ceph_attr, attr);

	if (sscanf(buf, "%d", ca->val) < 1)
		return 0;
	return len;
}

#define DECLARE_DEBUG_ATTR(_name)				      \
	struct ceph_attr ceph_attr_##_name = {		      \
		.attr = { .name = __stringify(_name), .mode = 0600 }, \
		.show = attr_show,				      \
		.store = attr_store,				      \
		.val = &ceph_##_name,				      \
	};

DECLARE_DEBUG_ATTR(debug);
DECLARE_DEBUG_ATTR(debug_msgr);
DECLARE_DEBUG_ATTR(debug_console);

/*
 * ceph_debug_mask
 */
static ssize_t debug_mask_show(struct kobject *ko, struct attribute *a,
			       char *buf)
{
	int i = 0, pos;

	pos = sprintf(buf, "0x%x", ceph_debug_mask);

	while (_debug_mask_names[i].mask) {
		if (ceph_debug_mask & _debug_mask_names[i].mask)
			pos += sprintf(buf+pos, " %s",
				       _debug_mask_names[i].name);
		i++;
	}
	pos += sprintf(buf+pos, "\n");
	return pos;
}

static ssize_t debug_mask_store(struct kobject *ko, struct attribute *a,
				const char *buf, size_t len)
{
	const char *next = buf, *tok;

	do {
		tok = next;
		next = strpbrk(tok, " \t\r\n") + 1;
		printk("tok %p next %p\n", tok, next);
		if (isdigit(*tok)) {
			ceph_debug_mask = simple_strtol(tok, NULL, 0);
		} else {
			int remove = 0;
			int mask;

			if (*tok == '-') {
				remove = 1;
				tok++;
			} else if (*tok == '+')
				tok++;
			mask = ceph_get_debug_mask(tok);
			if (mask) {
				if (remove)
					ceph_debug_mask &= ~mask;
				else
					ceph_debug_mask |= mask;
			}
		}
	} while (next);

	return len;
}

struct ceph_attr ceph_attr_debug_mask = {
	.attr = { .name = "debug_mask", .mode = 0600 },
	.show = debug_mask_show,
	.store = debug_mask_store,
};

int ceph_sysfs_init()
{
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 25)
	int ret;

	ret = kobject_init_and_add(&ceph_kobj, &ceph_type, fs_kobj, "ceph");
	if (ret)
		goto out;
	ret = sysfs_create_file(&ceph_kobj, &ceph_attr_debug.attr);
	if (ret)
		goto out_del;
	ret = sysfs_create_file(&ceph_kobj, &ceph_attr_debug_msgr.attr);
	if (ret)
		goto out_del;
	ret = sysfs_create_file(&ceph_kobj, &ceph_attr_debug_console.attr);
	if (ret)
		goto out_del;
	ret = sysfs_create_file(&ceph_kobj, &ceph_attr_debug_mask.attr);
	if (ret)
		goto out_del;
	return 0;

out_del:
	kobject_del(&ceph_kobj);
out:
	return ret;
#else
	return 0;
#endif
}

void ceph_sysfs_cleanup()
{
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 25)
#endif
}
