// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <array>

#include "rgw_common.h"
#include "rgw_auth.h"
#include "rgw_user.h"
#include "rgw_http_client.h"
#include "rgw_keystone.h"

#include "include/str_list.h"

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

    uint32_t get_perms_from_aclspec(const aclspec_t& aclspec) const {
      return rgw_perms_from_aclspec_default_strategy(id, aclspec);
    }

    bool is_admin_of(const rgw_user& acct_id) const {
      return is_admin;
    }

    bool is_owner_of(const rgw_user& acct_id) const {
      return id == acct_id;
    }

    uint32_t get_perm_mask() const {
      return perm_mask;
    }

    void to_str(std::ostream& out) const {
      out << "RGWDummyIdentityApplier(auth_id=" << id
          << ", perm_mask=" << perm_mask
          << ", is_admin=" << is_admin << ")";
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


uint32_t rgw_perms_from_aclspec_default_strategy(
  const rgw_user& uid,
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
uint32_t RGWRemoteAuthApplier::get_perms_from_aclspec(const aclspec_t& aclspec) const
{
  uint32_t perm = 0;

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

void RGWRemoteAuthApplier::to_str(std::ostream& out) const
{
  out << "RGWRemoteAuthApplier(acct_user=" << info.acct_user
      << ", acct_name=" << info.acct_name
      << ", perm_mask=" << info.perm_mask
      << ", is_admin=" << info.is_admin << ")";
}

void RGWRemoteAuthApplier::create_account(const rgw_user& acct_user,
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

uint32_t RGWLocalAuthApplier::get_perms_from_aclspec(const aclspec_t& aclspec) const
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

void RGWLocalAuthApplier::to_str(std::ostream& out) const
{
  out << "RGWLocalAuthApplier(acct_user=" << user_info.user_id
      << ", acct_name=" << user_info.display_name
      << ", subuser=" << subuser
      << ", perm_mask=" << get_perm_mask()
      << ", is_admin=" << user_info.admin << ")";
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


/* Keystone */
bool RGWKeystoneAuthEngine::is_applicable() const noexcept
{
  if (! RGWTokenBasedAuthEngine::is_applicable()) {
    return false;
  }

  return ! cct->_conf->rgw_keystone_url.empty();
}

KeystoneToken RGWKeystoneAuthEngine::decode_pki_token(const std::string& token) const
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

KeystoneToken RGWKeystoneAuthEngine::get_from_keystone(const std::string& token) const
{
  using RGWValidateKeystoneToken = KeystoneService::RGWValidateKeystoneToken;

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

  /* NULL terminate for debug output. */
  token_body_bl.append(static_cast<char>(0));
  ldout(cct, 20) << "received response status=" << validate.get_http_status()
                 << ", body=" << token_body_bl.c_str() << dendl;

  /* Detect Keystone rejection earlier than during the token parsing.
   * Although failure at the parsing phase doesn't impose a threat,
   * this allows to return proper error code (EACCESS instead of EINVAL
   * or similar) and thus improves logging. */
  if (validate.get_http_status() ==
          /* Most likely: wrong admin credentials or admin token. */
          RGWValidateKeystoneToken::HTTP_STATUS_UNAUTHORIZED ||
      validate.get_http_status() ==
          /* Most likely: non-existent token supplied by the client. */
          RGWValidateKeystoneToken::HTTP_STATUS_NOTFOUND) {
    throw -EACCES;
  }

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
  for (const auto& admin_role : admin_roles) {
    if (token.has_role(admin_role)) {
      is_admin = true;
      break;
    }
  }

  return RGWRemoteAuthApplier::AuthInfo {
    /* Suggested account name for the authenticated user. */
    rgw_user(token.get_project_id()),
    /* User's display name (aka real name). */
    token.get_project_name(),
    /* Keystone doesn't support RGW's subuser concept, so we cannot cut down
     * the access rights through the perm_mask. At least at this layer. */
    RGW_PERM_FULL_CONTROL,
    is_admin,
  };
}

static inline const std::string make_spec_item(const std::string& tenant,
                                               const std::string& id)
{
  return tenant + ":" + id;
}

RGWKeystoneAuthEngine::acl_strategy_t
RGWKeystoneAuthEngine::get_acl_strategy(const KeystoneToken& token) const
{
  /* The primary identity is constructed upon UUIDs. */
  const auto& tenant_uuid = token.get_project_id();
  const auto& user_uuid = token.get_user_id();

  /* For Keystone v2 an alias may be also used. */
  const auto& tenant_name = token.get_project_name();
  const auto& user_name = token.get_user_name();

  /* Construct all possible combinations including Swift's wildcards. */
  const std::array<std::string, 6> allowed_items = {
    make_spec_item(tenant_uuid, user_uuid),
    make_spec_item(tenant_name, user_name),

    /* Wildcards. */
    make_spec_item(tenant_uuid, "*"),
    make_spec_item(tenant_name, "*"),
    make_spec_item("*", user_uuid),
    make_spec_item("*", user_name),
  };

  /* Lambda will obtain a copy of (not a reference to!) allowed_items. */
  return [allowed_items](const RGWIdentityApplier::aclspec_t& aclspec) {
    uint32_t perm = 0;

    for (const auto& allowed_item : allowed_items) {
      const auto iter = aclspec.find(allowed_item);

      if (std::end(aclspec) != iter) {
        perm |= iter->second;
      }
    }

    return perm;
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

  /* Token ID is a concept that makes dealing with PKI tokens more effective.
   * Instead of storing several kilobytes, a short hash can be burried. */
  const auto& token_id = rgw_get_token_id(token);
  ldout(cct, 20) << "token_id=" << token_id << dendl;

  /* Check cache first. */
  if (RGWKeystoneTokenCache::get_instance().find(token_id, t)) {
    ldout(cct, 20) << "cached token.project.id=" << t.get_project_id()
                   << dendl;
      return apl_factory->create_apl_remote(cct,
                                            get_acl_strategy(t),
                                            get_creds_info(t, roles.admin));
  }

  /* Retrieve token. */
  if (rgw_is_pki_token(token)) {
    try {
      t = decode_pki_token(token);
    } catch (...) {
      /* Last resort. */
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
  for (const auto& role : roles.plain) {
    if (t.has_role(role) == true) {
      ldout(cct, 0) << "validated token: " << t.get_project_name()
                    << ":" << t.get_user_name()
                    << " expires: " << t.get_expires() << dendl;
      RGWKeystoneTokenCache::get_instance().add(token_id, t);
      return apl_factory->create_apl_remote(cct,
                                            get_acl_strategy(t),
                                            get_creds_info(t, roles.admin));
    }
  }

  ldout(cct, 0) << "user does not hold a matching role; required roles: "
                << g_conf->rgw_keystone_accepted_roles << dendl;

  return nullptr;
}
