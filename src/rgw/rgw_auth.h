// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#ifndef CEPH_RGW_AUTH_H
#define CEPH_RGW_AUTH_H

#include "rgw_common.h"
#define RGW_USER_ANON_ID "anonymous"


/* Load information about identity that will be used by RGWOp to authorize
 * any operation that comes from an authenticated user. */
class RGWIdentityApplier {
public:
  typedef std::map<std::string, int> aclspec_t;

  virtual ~RGWIdentityApplier() {};

  /* Translate the ACL provided in @aclspec into concrete permission set that
   * can be used in authorization phase (particularly in verify_permission
   * method of a given RGWOp).
   *
   * XXX: implementation is responsible for giving the real semantic to the
   * items in @aclspec. That is, their meaning may depend on particular auth
   * engine that was used. */
  virtual int get_perms_from_aclspec(const aclspec_t& aclspec) const = 0;

  /* Verify whether a given identity *can be treated as* an admin of
   * the rgw_user (account in Swift's terminology) specified in @uid. */
  virtual bool is_admin_of(const rgw_user& uid) const = 0;

  /* Verify whether a given identity *is* the owner of the rgw_user
  * (account in Swift's terminology) specified in @uid. */
  virtual bool is_owner_of(const rgw_user& uid) const = 0;

  virtual bool is_anonymous() const final {
    /* If the identity owns the anonymous account (rgw_user), it's considered
     * the anonymous identity. */
    return is_owner_of(rgw_user(RGW_USER_ANON_ID));
  }

  virtual int get_perm_mask() const = 0;
};

inline ostream& operator<<(ostream& out, const RGWIdentityApplier &id) {
//  return out << id.to_str();
  return out;
}

std::unique_ptr<RGWIdentityApplier>
rgw_auth_transform_old_authinfo(req_state * const s);

#endif /* CEPH_RGW_AUTH_H */
