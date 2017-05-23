#include "acconfig.h"
#include "common/config.h"
#include "include/types.h"
#include "sys/stat.h"
#include "errno.h"
#include "rich_acl.h"
#include "UserPerm.h"

int rich_acl_from_xattr(const void *xattr, size_t size,
			struct richacl **outp)
{
  struct richacl *acl = richacl_from_xattr(xattr, size);
  if (!acl)
    return -errno;
  *outp = acl;
  return 0;
}

int rich_acl_setxattr(const void *xattr, size_t size, mode_t *mode_p)
{
  struct richacl *acl = richacl_from_xattr(xattr, size);
  if (!acl)
    return -errno;
  int r = richacl_equiv_mode(acl, mode_p);
  if (r) {
    *mode_p &= ~ACCESSPERMS;
    *mode_p |= richacl_masks_to_mode(acl);
  }
  richacl_free(acl);
  return r ? 1 : 0;
}

int rich_acl_chmod(struct richacl *acl, bufferptr& out_acl, mode_t mode)
{
  richacl_chmod(acl, mode);
  bufferptr buf(richacl_xattr_size(acl));
  richacl_to_xattr(acl, buf.c_str());
  out_acl = buf;
  return 0;
}

static mode_t zero_umask(void *arg)
{
  return 0;
}

int rich_acl_inherit(const struct richacl *dir_acl, bufferptr& out_acl,
		     mode_t *mode_p, mode_t (*umask)(void *), void *umask_arg)
{
  struct richacl *acl = richacl_inherit_inode(dir_acl, mode_p,
					      umask ? : zero_umask, umask_arg);
  if (acl) {
    *mode_p &= ~ACCESSPERMS;
    *mode_p |= richacl_masks_to_mode(acl);
    bufferptr buf(richacl_xattr_size(acl));
    richacl_to_xattr(acl, buf.c_str());
    richacl_free(acl);
    out_acl = buf;
    return 1;
  } else {
    return 0;
  }
}

int rich_acl_permits(struct richacl *acl, uid_t i_uid, gid_t i_gid,
		     const UserPerm& perms, unsigned mask)
{
  bool ok;
  const gid_t *gids;
  int count = perms.get_gids(&gids);
  if (count > 1) {
    ok = richacl_permission(acl, i_uid, i_gid, perms.uid(), gids, count, mask);
  } else {
    gid_t gid = perms.gid();
    ok = richacl_permission(acl, i_uid, i_gid, perms.uid(), &gid, 1, mask);
  }
  return ok ? 0 : -EACCES;
}
