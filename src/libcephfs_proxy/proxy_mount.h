
#ifndef __LIBCEPHFSD_PROXY_MOUNT_H__
#define __LIBCEPHFSD_PROXY_MOUNT_H__

#include "proxy.h"
#include "proxy_list.h"

#include "include/cephfs/libcephfs.h"

struct _proxy_instance {
	uint8_t hash[32];
	list_t list;
	list_t siblings;
	list_t changes;
	struct ceph_mount_info *cmount;
	proxy_inode_t *root;
	bool inited;
	bool mounted;
};

struct _proxy_mount {
	proxy_instance_t *instance;
	UserPerm *perms;
	proxy_inode_t *root;
	proxy_inode_t *cwd;
	char *cwd_path;
	uint32_t cwd_path_len;
};

static inline struct ceph_mount_info *proxy_cmount(proxy_mount_t *mount)
{
	return mount->instance->cmount;
}

int32_t proxy_mount_create(proxy_mount_t **pmount, const char *id);

int32_t proxy_mount_mount(proxy_mount_t *mount, const char *root);

int32_t proxy_mount_unmount(proxy_mount_t *mount);

int32_t proxy_mount_release(proxy_mount_t *mount);

int32_t proxy_instance_config(proxy_instance_t *mount, const char *config);

int32_t proxy_instance_set(proxy_instance_t *mount, const char *name,
			   const char *value);

int32_t proxy_instance_get(proxy_instance_t *mount, const char *name, char *value,
			   size_t size);

int32_t proxy_instance_select(proxy_instance_t *mount, const char *fs);

int32_t proxy_instance_init(proxy_instance_t *mount);

int32_t proxy_path_resolve(proxy_mount_t *mount, const char *path,
			   proxy_inode_t **inode, struct ceph_statx *stx,
			   uint32_t want, uint32_t flags, UserPerm *perms,
			   char **realpath);

#endif
