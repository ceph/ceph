// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_common.h"
#include "rgw_auth.h"
#include "rgw_user.h"
#include "rgw_http_client.h"
#include "rgw_keystone.h"

#include "include/str_list.h"

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


/* Keystone */
class RGWKeystoneHTTPTransceiver : public RGWHTTPTransceiver {
public:
  RGWKeystoneHTTPTransceiver(CephContext * const cct,
                             bufferlist * const token_body_bl)
    : RGWHTTPTransceiver(cct, token_body_bl,
                         cct->_conf->rgw_keystone_verify_ssl,
                         { "X-Subject-Token" }) {
  }

  std::string get_subject_token() const {
    try {
      return get_header_value("X-Subject-Token");
    } catch (std::out_of_range&) {
      return header_value_t();
    }
  }
};

typedef RGWKeystoneHTTPTransceiver RGWValidateKeystoneToken;
typedef RGWKeystoneHTTPTransceiver RGWGetKeystoneAdminToken;
typedef RGWKeystoneHTTPTransceiver RGWGetRevokedTokens;


bool RGWKeystoneAuthEngine::is_applicable() const noexcept
{
  if (false == RGWTokenBasedAuthEngine::is_applicable()) {
    return false;
  }

  return false == cct->_conf->rgw_keystone_url.empty();
}

KeystoneToken RGWKeystoneAuthEngine::decode_pki_token(const std::string token) const
{
  bufferlist token_body_bl;
  int ret = rgw_decode_b64_cms(cct, token, token_body_bl);
  if (ret < 0) {
    ldout(cct, 20) << "cannot decode pki token" << dendl;
    throw ret;
  } else {
    ldout(cct, 20) << "successfully decoded pki token" << dendl;
  }

  KeystoneToken token_body;
  ret = token_body.parse(cct, token, token_body_bl);
  if (ret < 0) {
    throw ret;
  }

  return token_body;
}

KeystoneToken RGWKeystoneAuthEngine::get_from_keystone(const std::string token) const
{
  bufferlist token_body_bl;
  RGWValidateKeystoneToken validate(cct, &token_body_bl);

  std::string url;
  if (KeystoneService::get_keystone_url(cct, url) < 0) {
    throw -EINVAL;
  }

  const auto keystone_version = KeystoneService::get_api_version();
  if (keystone_version == KeystoneApiVersion::VER_2) {
    url.append("v2.0/tokens/" + token);
  } else if (keystone_version == KeystoneApiVersion::VER_3) {
    url.append("v3/auth/tokens");
    validate.append_header("X-Subject-Token", token);
  }

  std::string admin_token;
  if (KeystoneService::get_keystone_admin_token(cct, admin_token) < 0) {
    throw -EINVAL;
  }

  validate.append_header("X-Auth-Token", admin_token);
  validate.set_send_length(0);

  int ret = validate.process(url.c_str());
  if (ret < 0) {
    throw ret;
  }
  token_body_bl.append((char)0); // NULL terminate for debug output

  ldout(cct, 20) << "received response: " << token_body_bl.c_str() << dendl;

  KeystoneToken token_body;
  ret = token_body.parse(cct, token, token_body_bl);
  if (ret < 0) {
    throw ret;
  }

  return token_body;
}

RGWRemoteAuthApplier::AuthInfo
RGWKeystoneAuthEngine::get_creds_info(const KeystoneToken& token,
                                      const std::vector<std::string>& admin_roles
                                    ) const noexcept
{
  /* Check whether the user has an admin status. */
  bool is_admin = false;
  for (const auto admin_role : admin_roles) {
    if (token.has_role(admin_role)) {
      is_admin = true;
      break;
    }
  }

  return {
    /* Suggested account name for the authenticated user. */
    rgw_user(token.get_project_id()),
    /* The authenticated identity. */
    rgw_user(token.get_project_id()),
    /* User's display name (aka real name). */
    token.get_project_name(),
    /* Keystone doesn't support RGW's subuser concept, so we cannot cut down
     * the access rights through the perm_mask. At least at this layer. */
    RGW_PERM_FULL_CONTROL,
    is_admin
  };
}

RGWAuthApplier::aplptr_t RGWKeystoneAuthEngine::authenticate() const
{
  KeystoneToken t;

  /* This will be initialized on the first call to this method. In C++11 it's
   * also thread-safe. */
  static struct RolesCacher {
    RolesCacher(CephContext * const cct) {
      get_str_vec(cct->_conf->rgw_keystone_accepted_roles, plain);
      get_str_vec(cct->_conf->rgw_keystone_accepted_admin_roles, admin);

      /* Let's suppose that having an admin role implies also a regular one. */
      plain.insert(std::end(plain), std::begin(admin), std::end(admin));
    }

    std::vector<std::string> plain;
    std::vector<std::string> admin;
  } roles(cct);

  const auto token_id = rgw_get_token_id(token);
  ldout(cct, 20) << "token_id=" << token_id << dendl;

  /* Check cache first, */
  if (RGWKeystoneTokenCache::get_instance().find(token_id, t)) {
    ldout(cct, 20) << "cached token.project.id=" << t.get_project_id()
                   << dendl;
      return apl_factory->create_loader(cct, get_creds_info(t, roles.admin));
  }

  if (rgw_is_pki_token(token)) {
    try {
      t = decode_pki_token(token);
    } catch (...) {
      /* Last ressort. */
      t = get_from_keystone(token);
    }
  } else {
    /* Can't decode, just go to the Keystone server for validation. */
    t = get_from_keystone(token);
  }

  /* Verify expiration. */
  if (t.expired()) {
    ldout(cct, 0) << "got expired token: " << t.get_project_name()
                  << ":" << t.get_user_name()
                  << " expired: " << t.get_expires() << dendl;
    return nullptr;
  }

  /* Check for necessary roles. */
  for (const auto role : roles.plain) {
    if (t.has_role(role) == true) {
      ldout(cct, 0) << "validated token: " << t.get_project_name()
                    << ":" << t.get_user_name()
                    << " expires: " << t.get_expires() << dendl;
      RGWKeystoneTokenCache::get_instance().add(token_id, t);
      return apl_factory->create_loader(cct, get_creds_info(t, roles.admin));
    }
  }

  ldout(cct, 0) << "user does not hold a matching role; required roles: "
                << g_conf->rgw_keystone_accepted_roles << dendl;

  return nullptr;
}
