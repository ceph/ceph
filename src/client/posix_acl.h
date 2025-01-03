#ifndef CEPH_POSIX_ACL
#define CEPH_POSIX_ACL

#define ACL_EA_VERSION          0x0002

#define ACL_USER_OBJ            0x01
#define ACL_USER                0x02
#define ACL_GROUP_OBJ           0x04
#define ACL_GROUP               0x08
#define ACL_MASK                0x10
#define ACL_OTHER               0x20

#define ACL_EA_ACCESS  "system.posix_acl_access"
#define ACL_EA_DEFAULT "system.posix_acl_default"

typedef struct {
  ceph_le16       e_tag;
  ceph_le16       e_perm;
  ceph_le32       e_id;
} acl_ea_entry;

typedef struct {
  ceph_le32       a_version;
  acl_ea_entry    a_entries[0];
} acl_ea_header;

class UserPerm;

int posix_acl_check(const void *xattr, size_t size);
int posix_acl_equiv_mode(const void *xattr, size_t size, mode_t *mode_p);
int posix_acl_inherit_mode(bufferptr& acl, mode_t *mode_p);
int posix_acl_access_chmod(bufferptr& acl, mode_t mode);
int posix_acl_permits(const bufferptr& acl, uid_t i_uid, gid_t i_gid,
		      const UserPerm& groups, unsigned want);
#endif
