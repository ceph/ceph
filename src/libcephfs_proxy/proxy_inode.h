
#ifndef __LIBCEPHFS_PROXY_INODE_H__
#define __LIBCEPHFS_PROXY_INODE_H__

#include "proxy.h"
#include "proxy_mount.h"
#include "proxy_helpers.h"

#include "include/cephfs/libcephfs.h"

#include <urcu/uatomic.h>

struct _proxy_inode {
	struct Inode *in;
	uint32_t refs;
};

static inline void proxy_inode_put(proxy_instance_t *instance,
				   proxy_inode_t *inode)
{
	if (uatomic_sub_return(&inode->refs, 1) == 0) {
		ceph_ll_put(instance->cmount, inode->in);
		proxy_free(inode);
	}
}

static inline proxy_inode_t *proxy_inode_get(proxy_inode_t *inode)
{
	uatomic_inc(&inode->refs);

	return inode;
}

int32_t proxy_inode_lookup(proxy_instance_t *instance, proxy_inode_t *parent,
			   const char *name, proxy_inode_t **pinode,
			   struct ceph_statx *stx, uint32_t want,
			   uint32_t flags, const UserPerm *perms);

int32_t proxy_inode_lookup_root(proxy_instance_t *instance,
				proxy_inode_t **pinode);

int32_t proxy_inode_lookup_inode(proxy_instance_t *instance,
				 struct inodeno_t ino, proxy_inode_t **pinode);

int32_t proxy_inode_create(proxy_instance_t *instance, proxy_inode_t *parent,
			   const char *name, mode_t mode, int32_t oflags,
			   proxy_inode_t **pinode, Fh **pfh,
			   struct ceph_statx *stx, uint32_t want,
			   uint32_t flags, const UserPerm *perms);

int32_t proxy_inode_mknod(proxy_instance_t *instance, proxy_inode_t *parent,
			  const char *name, mode_t mode, dev_t rdev,
			  proxy_inode_t **pinode, struct ceph_statx *src,
			  uint32_t want, uint32_t flags, const UserPerm *perms);

int32_t proxy_inode_symlink(proxy_instance_t *instance, proxy_inode_t *parent,
			    const char *name, const char *value,
			    proxy_inode_t **pinode, struct ceph_statx *stx,
			    uint32_t want, uint32_t flags,
			    const UserPerm *perms);

int32_t proxy_inode_mkdir(proxy_instance_t *instance, proxy_inode_t *parent,
			  const char *name, mode_t mode, proxy_inode_t **pinode,
			  struct ceph_statx *stx, uint32_t want, uint32_t flags,
			  const UserPerm *perms);

int32_t proxy_inode_getattr(proxy_instance_t *instance, proxy_inode_t *inode,
			    struct ceph_statx *stx, uint32_t want,
			    uint32_t flags, UserPerm *perms);

#endif /* __LIBCEPHFS_PROXY_INODE_H__ */
