// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_common.h"
#include "rgw_auth.h"
#include "rgw_user.h"
#include "rgw_http_client.h"
#include "rgw_keystone.h"
#include "rgw_swift.h"

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
      return rgw_perms_from_aclspec_default_strategy(id, aclspec);
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
  /* System user has admin permissions by default - it's supposed to pass
   * through any security check. */
                                    s->system_request));
}


int rgw_perms_from_aclspec_default_strategy(const rgw_user& uid,
                                            const RGWIdentityApplier::aclspec_t& aclspec)
{
  dout(5) << "Searching permissions for uid=" << uid <<  dendl;

  const auto iter = aclspec.find(uid.to_str());
  if (std::end(aclspec) != iter) {
    dout(5) << "Found permission: " << iter->second << dendl;
    return iter->second;
  }

  dout(5) << "Permissions for user not found" << dendl;
  return 0;
}


/* RGWRemoteAuthApplier */
int RGWRemoteAuthApplier::get_perms_from_aclspec(const aclspec_t& aclspec) const
{
  int perm = 0;

  /* For backward compatibility with ACLOwner. */
  perm |= rgw_perms_from_aclspec_default_strategy(info.acct_user,
                                                  aclspec);

  /* We also need to cover cases where rgw_keystone_implicit_tenants
   * was enabled. */
  if (info.acct_user.tenant.empty()) {
    const rgw_user tenanted_acct_user(info.acct_user.id, info.acct_user.id);

    perm |= rgw_perms_from_aclspec_default_strategy(tenanted_acct_user,
                                                    aclspec);
  }

  /* Now it's a time for invoking additional strategy that was supplied by
   * a specific auth engine. */
  if (extra_acl_strategy) {
    perm |= extra_acl_strategy(aclspec);
  }

  ldout(cct, 20) << "from ACL got perm=" << perm << dendl;
  return perm;
}

bool RGWRemoteAuthApplier::is_admin_of(const rgw_user& uid) const
{
  return info.is_admin;
}

bool RGWRemoteAuthApplier::is_owner_of(const rgw_user& uid) const
{
  if (info.acct_user.tenant.empty()) {
    const rgw_user tenanted_acct_user(info.acct_user.id, info.acct_user.id);

    if (tenanted_acct_user == uid) {
      return true;
    }
  }

  return info.acct_user == uid;
}

void RGWRemoteAuthApplier::create_account(const rgw_user acct_user,
                                          RGWUserInfo& user_info) const      /* out */
{
  rgw_user new_acct_user = acct_user;

  /* Administrator may enforce creating new accounts within their own tenants.
   * The config parameter name is kept due to legacy. */
  if (new_acct_user.tenant.empty() && g_conf->rgw_keystone_implicit_tenants) {
    new_acct_user.tenant = new_acct_user.id;
  }

  user_info.user_id = new_acct_user;
  user_info.display_name = info.acct_name;

  int ret = rgw_store_user_info(store, user_info, nullptr, nullptr,
                                real_time(), true);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to store new user info: user="
                  << user_info.user_id << " ret=" << ret << dendl;
    throw ret;
  }
}

/* TODO(rzarzynski): we need to handle display_name changes. */
void RGWRemoteAuthApplier::load_acct_info(RGWUserInfo& user_info) const      /* out */
{
  /* It's supposed that RGWRemoteAuthApplier tries to load account info
   * that belongs to the authenticated identity. Another policy may be
   * applied by using a RGWThirdPartyAccountAuthApplier decorator. */
  const rgw_user& acct_user = info.acct_user;

  /* Normally, empty "tenant" field of acct_user means the authenticated
   * identity has the legacy, global tenant. However, due to inclusion
   * of multi-tenancy, we got some special compatibility kludge for remote
   * backends like Keystone.
   * If the global tenant is the requested one, we try the same tenant as
   * the user name first. If that RGWUserInfo exists, we use it. This way,
   * migrated OpenStack users can get their namespaced containers and nobody's
   * the wiser.
   * If that fails, we look up in the requested (possibly empty) tenant.
   * If that fails too, we create the account within the global or separated
   * namespace depending on rgw_keystone_implicit_tenants. */
  if (acct_user.tenant.empty()) {
    const rgw_user tenanted_uid(acct_user.id, acct_user.id);

    if (rgw_get_user_info_by_uid(store, tenanted_uid, user_info) >= 0) {
      /* Succeeded. */
      return;
    }
  }

  if (rgw_get_user_info_by_uid(store, acct_user, user_info) < 0) {
    ldout(cct, 0) << "NOTICE: couldn't map swift user " << acct_user << dendl;
    create_account(acct_user, user_info);
  }

  /* Succeeded if we are here (create_account() hasn't throwed). */
}


/* LocalAuthApplier */
/* static declaration */
const std::string RGWLocalAuthApplier::NO_SUBUSER;

int RGWLocalAuthApplier::get_perms_from_aclspec(const aclspec_t& aclspec) const
{
  return rgw_perms_from_aclspec_default_strategy(user_info.user_id, aclspec);
}

bool RGWLocalAuthApplier::is_admin_of(const rgw_user& uid) const
{
  return user_info.admin;
}

bool RGWLocalAuthApplier::is_owner_of(const rgw_user& uid) const
{
  return uid == user_info.user_id;
}

uint32_t RGWLocalAuthApplier::get_perm_mask(const std::string& subuser_name,
                                            const RGWUserInfo &uinfo) const
{
  if (!subuser_name.empty() && subuser_name != NO_SUBUSER) {
    const auto iter = uinfo.subusers.find(subuser_name);

    if (iter != std::end(uinfo.subusers)) {
      return iter->second.perm_mask;
    } else {
      /* Subuser specified but not found. */
      return RGW_PERM_NONE;
    }
  } else {
    /* Due to backward compatibility. */
    return RGW_PERM_FULL_CONTROL;
  }
}

void RGWLocalAuthApplier::load_acct_info(RGWUserInfo& user_info) const      /* out */
{
  /* Load the account that belongs to the authenticated identity. An extra call
   * to RADOS may be safely skipped in this case. */
  user_info = this->user_info;
}


RGWAuthApplier::aplptr_t RGWAnonymousAuthEngine::authenticate() const
{
  RGWUserInfo user_info;
  rgw_get_anon_user(user_info);

  return apl_factory->create_apl_local(cct, user_info,
                                       RGWLocalAuthApplier::NO_SUBUSER);
}
