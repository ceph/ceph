// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_common.h"
#include "rgw_auth.h"
#include "rgw_user.h"
#include "rgw_http_client.h"
#include "rgw_keystone.h"
#include "rgw_swift.h"

#define dout_subsys ceph_subsys_rgw

void RGWRemoteAuthApplier::create_account(const rgw_user acct_user,
                                          RGWUserInfo& user_info) const      /* out */
{
  rgw_user new_acct_user = acct_user;

  /* Administrator may request creating new accounts within their own
   * tenants. The config parameter name is kept unchanged due to legacy. */
  if (new_acct_user.tenant.empty() && g_conf->rgw_keystone_implicit_tenants) {
    new_acct_user.tenant = new_acct_user.id;
  }

  user_info.user_id = new_acct_user;
  user_info.display_name = info.display_name;

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
  const rgw_user& acct_user = info.auth_user;

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

void RGWRemoteAuthApplier::load_user_info(rgw_user& auth_user,               /* out */
                                          uint32_t& perm_mask,               /* out */
                                          bool& admin_request) const         /* out */
{
  auth_user = info.auth_user;
  perm_mask = info.perm_mask;
  admin_request = info.is_admin;
}


/* static declaration */
const rgw_user RGWThirdPartyAccountAuthApplier::UNKNOWN_ACCT;

void RGWThirdPartyAccountAuthApplier::load_acct_info(RGWUserInfo& user_info) const
{
  rgw_user auth_user;
  uint32_t perm_mask;
  bool is_admin;

  RGWDecoratoringAuthApplier::load_user_info(auth_user, perm_mask, is_admin);


  if (UNKNOWN_ACCT == acct_user_override) {
    /* There is no override specified by the upper layer. This means that we'll
     * load the account owned by the authenticated identity (aka auth_user). */
    RGWDecoratoringAuthApplier::load_acct_info(user_info);
  } else if (acct_user_override == auth_user) {
    /* The override has been specified but the account belongs to the authenticated
     * identity. We may safely forward the call to a next stage. */
    RGWDecoratoringAuthApplier::load_acct_info(user_info);
  } else {
    int ret = rgw_get_user_info_by_uid(store, acct_user_override, user_info);
    if (ret < 0) {
      /* We aren't trying to recover from ENOENT here. It's supposed that creating
       * someone else's account isn't a thing we want to support. */
      throw ret;
    }
  }
}


/* LocalAuthApplier */
/* static declaration */
const std::string RGWLocalAuthApplier::NO_SUBUSER;

uint32_t RGWLocalAuthApplier::get_perm_mask(const std::string& subuser_name,
                                            const RGWUserInfo &uinfo) const
{
  if (!subuser_name.empty()) {
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

void RGWLocalAuthApplier::load_user_info(rgw_user& auth_user,               /* out */
                                         uint32_t& perm_mask,               /* out */
                                         bool& admin_request) const         /* out */
{
  auth_user = user_info.user_id;
  perm_mask = get_perm_mask(subuser, user_info);
  admin_request = user_info.admin;
}


RGWAuthApplier::aplptr_t RGWAnonymousAuthEngine::authenticate() const
{
  RGWUserInfo user_info;
  rgw_get_anon_user(user_info, user_info.user_id);

  return apl_factory->create_loader(cct, user_info, RGWLocalAuthApplier::NO_SUBUSER);
}
