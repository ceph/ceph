#ifndef CEPH_RICH_ACL
#define CEPH_RICH_ACL

#define SYSTEM_RICHACL "system.richacl"

#ifdef HAVE_LIBRICHACL
extern "C" {
#include <sys/richacl.h>
}

struct UserPerm;

int rich_acl_from_xattr(const void *xattr, size_t size,
			struct richacl **outp);
int rich_acl_setxattr(const void *xattr, size_t size, mode_t *mode_p);
int rich_acl_inherit(const struct richacl *dir_acl, bufferptr& out_acl,
		     mode_t *mode_p, mode_t (*umask)(void *), void *umask_arg);
int rich_acl_chmod(struct richacl *acl, bufferptr& out_acl, mode_t mode);
int rich_acl_permits(struct richacl *acl, uid_t i_uid, gid_t i_gid,
		     const UserPerm& perms, unsigned want);
static inline void rich_acl_free(struct richacl *acl) {
    richacl_free(acl);
}
#endif
#endif
