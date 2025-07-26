
#include "proxy_inode.h"
#include "proxy_helpers.h"

static int32_t proxy_inode_allocate(struct Inode *in, proxy_inode_t **pinode)
{
	proxy_inode_t *inode;

	inode = proxy_malloc(sizeof(proxy_inode_t));
	if (inode == NULL) {
		return -ENOMEM;
	}

	inode->in = in;
	inode->refs = 1;

	*pinode = inode;

	return 0;
}

int32_t proxy_inode_lookup(proxy_instance_t *instance, proxy_inode_t *parent,
			   const char *name, proxy_inode_t **pinode,
			   struct ceph_statx *stx, uint32_t want,
			   uint32_t flags, const UserPerm *perms)
{
	struct Inode *in;
	int32_t err;

	err = ceph_ll_lookup(instance->cmount, parent->in, name, &in, stx,
			     want, flags, perms);
	if (err < 0) {
		return err;
	}

	err = proxy_inode_allocate(in, pinode);
	if (err < 0) {
		ceph_ll_put(instance->cmount, in);
	}

	return err;
}

int32_t proxy_inode_lookup_root(proxy_instance_t *instance,
				proxy_inode_t **pinode)
{
	struct Inode *in;
	int32_t err;

	err = ceph_ll_lookup_root(instance->cmount, &in);
	if (err < 0) {
		return err;
	}

	err = proxy_inode_allocate(in, pinode);
	if (err < 0) {
		ceph_ll_put(instance->cmount, in);
	}

	return err;
}

int32_t proxy_inode_lookup_inode(proxy_instance_t *instance,
				 struct inodeno_t ino, proxy_inode_t **pinode)
{
	struct Inode *in;
	int32_t err;

	err = ceph_ll_lookup_inode(instance->cmount, ino, &in);
	if (err < 0) {
		return err;
	}

	err = proxy_inode_allocate(in, pinode);
	if (err < 0) {
		ceph_ll_put(instance->cmount, in);
	}

	return err;
}

int32_t proxy_inode_create(proxy_instance_t *instance, proxy_inode_t *parent,
			   const char *name, mode_t mode, int32_t oflags,
			   proxy_inode_t **pinode, Fh **pfh,
			   struct ceph_statx *stx, uint32_t want,
			   uint32_t flags, const UserPerm *perms)
{
	proxy_inode_t *inode;
	int32_t err;

	err = proxy_inode_allocate(NULL, &inode);
	if (err < 0) {
		return err;
	}

	err = ceph_ll_create(instance->cmount, parent->in, name, mode, oflags,
			     &inode->in, pfh, stx, want, flags, perms);
	if (err < 0) {
		proxy_free(inode);
	} else {
		*pinode = inode;
	}

	return err;
}

int32_t proxy_inode_mknod(proxy_instance_t *instance, proxy_inode_t *parent,
			  const char *name, mode_t mode, dev_t rdev,
			  proxy_inode_t **pinode, struct ceph_statx *stx,
			  uint32_t want, uint32_t flags, const UserPerm *perms)
{
	proxy_inode_t *inode;
	int32_t err;

	err = proxy_inode_allocate(NULL, &inode);
	if (err < 0) {
		return err;
	}

	err = ceph_ll_mknod(instance->cmount, parent->in, name, mode, rdev,
			    &inode->in, stx, want, flags, perms);
	if (err < 0) {
		proxy_free(inode);
	} else {
		*pinode = inode;
	}

	return err;
}

int32_t proxy_inode_symlink(proxy_instance_t *instance, proxy_inode_t *parent,
			    const char *name, const char *value,
			    proxy_inode_t **pinode, struct ceph_statx *stx,
			    uint32_t want, uint32_t flags,
			    const UserPerm *perms)
{
	proxy_inode_t *inode;
	struct Inode *in;
	int32_t err;

	err = proxy_inode_allocate(NULL, &inode);
	if (err < 0) {
		return err;
	}

	err = ceph_ll_symlink(instance->cmount, parent->in, name, value, &in,
			      stx, want, flags, perms);
	if (err < 0) {
		proxy_free(inode);
	} else {
		*pinode = inode;
	}

	return err;
}

int32_t proxy_inode_mkdir(proxy_instance_t *instance, proxy_inode_t *parent,
			  const char *name, mode_t mode, proxy_inode_t **pinode,
			  struct ceph_statx *stx, uint32_t want, uint32_t flags,
			  const UserPerm *perms)
{
	proxy_inode_t *inode;
	struct Inode *in;
	int32_t err;

	err = proxy_inode_allocate(NULL, &inode);
	if (err < 0) {
		return err;
	}

	err = ceph_ll_mkdir(instance->cmount, parent->in, name, mode, &in,
			    stx, want, flags, perms);
	if (err < 0) {
		proxy_free(inode);
	} else {
		*pinode = inode;
	}

	return err;
}

int32_t proxy_inode_getattr(proxy_instance_t *instance, proxy_inode_t *inode,
			    struct ceph_statx *stx, uint32_t want,
			    uint32_t flags, UserPerm *perms)
{
	return ceph_ll_getattr(instance->cmount, inode->in, stx, want, flags,
			       perms);
}
