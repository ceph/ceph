// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_common.h"
#include "rgw_auth.h"

#define dout_subsys ceph_subsys_rgw


std::unique_ptr<RGWIdentityApplier>
rgw_auth_transform_old_authinfo(req_state * const s)
{
  /* This class is not intended for public use. Should be removed altogether
   * with this function after moving all our APIs to the new authentication
   * infrastructure. */
  class RGWDummyIdentityApplier : public RGWIdentityApplier {
    CephContext * const cct;

    /* For this particular case it's OK to use rgw_user structure to convey
     * the identity info as this was the policy for doing that before the
     * new auth. */
    const rgw_user id;
    const int perm_mask;
    const bool is_admin;
  public:
    RGWDummyIdentityApplier(CephContext * const cct,
                            const rgw_user& auth_id,
                            const int perm_mask,
                            const bool is_admin)
      : cct(cct),
        id(auth_id),
        perm_mask(perm_mask),
        is_admin(is_admin) {
    }

    int get_perms_from_aclspec(const aclspec_t& aclspec) const {
      ldout(cct, 5) << "Searching permissions for uid=" << id
                    << " mask=" << perm_mask << dendl;

      const auto iter = aclspec.find(id.to_str());
      if (std::end(aclspec) != iter) {
        ldout(cct, 5) << "Found permission: " << iter->second << dendl;
        return iter->second & perm_mask;
      }

      ldout(cct, 5) << "Permissions for user not found" << dendl;
      return 0;
    }

    bool is_admin_of(const rgw_user& acct_id) const {
      return is_admin;
    }

    bool is_owner_of(const rgw_user& acct_id) const {
      return id == acct_id;
    }

    int get_perm_mask() const {
      return perm_mask;
    }
  };

  return std::unique_ptr<RGWIdentityApplier>(
        new RGWDummyIdentityApplier(s->cct,
                                    s->user->user_id,
                                    s->perm_mask,
                                    s->system_request));
}
