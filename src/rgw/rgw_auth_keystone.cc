// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string>
#include <vector>

#include <errno.h>
#include <fnmatch.h>


#include "common/errno.h"
#include "common/ceph_json.h"
#include "include/types.h"
#include "include/str_list.h"

#include "rgw_common.h"
#include "rgw_keystone.h"
#include "rgw_auth_keystone.h"
#include "rgw_keystone.h"

#include "common/ceph_crypto_cms.h"
#include "common/armor.h"
#include "common/Cond.h"

#define dout_subsys ceph_subsys_rgw


namespace rgw {
namespace auth {
namespace keystone {

bool
TokenEngine::is_applicable() const noexcept
{
  return ! cct->_conf->rgw_keystone_url.empty();
}

TokenEngine::token_envelope_t
TokenEngine::decode_pki_token(const std::string& token) const
{
  ceph::buffer::list token_body_bl;
  int ret = rgw_decode_b64_cms(cct, token, token_body_bl);
  if (ret < 0) {
    ldout(cct, 20) << "cannot decode pki token" << dendl;
    throw ret;
  } else {
    ldout(cct, 20) << "successfully decoded pki token" << dendl;
  }

  TokenEngine::token_envelope_t token_body;
  ret = token_body.parse(cct, token, token_body_bl, config.get_api_version());
  if (ret < 0) {
    throw ret;
  }

  return token_body;
}

TokenEngine::token_envelope_t
TokenEngine::get_from_keystone(const std::string& token) const
{
  using RGWValidateKeystoneToken
    = rgw::keystone::Service::RGWValidateKeystoneToken;

  bufferlist token_body_bl;
  RGWValidateKeystoneToken validate(cct, &token_body_bl);

  std::string url = config.get_endpoint_url();
  if (url.empty()) {
    throw -EINVAL;
  }

  const auto keystone_version = config.get_api_version();
  if (keystone_version == rgw::keystone::ApiVersion::VER_2) {
    url.append("v2.0/tokens/" + token);
  } else if (keystone_version == rgw::keystone::ApiVersion::VER_3) {
    url.append("v3/auth/tokens");
    validate.append_header("X-Subject-Token", token);
  }

  std::string admin_token;
  if (rgw::keystone::Service::get_admin_token(cct, token_cache,
          rgw::keystone::CephCtxConfig::get_instance(), admin_token) < 0) {
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

  TokenEngine::token_envelope_t token_body;
  ret = token_body.parse(cct, token, token_body_bl, config.get_api_version());
  if (ret < 0) {
    throw ret;
  }

  return token_body;
}

TokenEngine::auth_info_t
TokenEngine::get_creds_info(const TokenEngine::token_envelope_t& token,
                            const std::vector<std::string>& admin_roles
                           ) const noexcept
{
  using acct_privilege_t = rgw::auth::RemoteApplier::AuthInfo::acct_privilege_t;

  /* Check whether the user has an admin status. */
  acct_privilege_t level = acct_privilege_t::IS_PLAIN_ACCT;
  for (const auto& admin_role : admin_roles) {
    if (token.has_role(admin_role)) {
      level = acct_privilege_t::IS_ADMIN_ACCT;
      break;
    }
  }

  return auth_info_t {
    /* Suggested account name for the authenticated user. */
    rgw_user(token.get_project_id()),
    /* User's display name (aka real name). */
    token.get_project_name(),
    /* Keystone doesn't support RGW's subuser concept, so we cannot cut down
     * the access rights through the perm_mask. At least at this layer. */
    RGW_PERM_FULL_CONTROL,
    level,
  };
}

static inline const std::string
make_spec_item(const std::string& tenant, const std::string& id)
{
  return tenant + ":" + id;
}

TokenEngine::acl_strategy_t
TokenEngine::get_acl_strategy(const TokenEngine::token_envelope_t& token) const
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

TokenEngine::result_t
TokenEngine::authenticate(const std::string& token) const
{
  TokenEngine::token_envelope_t t;

  /* This will be initialized on the first call to this method. In C++11 it's
   * also thread-safe. */
  static const struct RolesCacher {
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
  if (token_cache.find(token_id, t)) {
    ldout(cct, 20) << "cached token.project.id=" << t.get_project_id()
                   << dendl;
      auto apl = apl_factory->create_apl_remote(cct, get_acl_strategy(t),
                                                get_creds_info(t, roles.admin));
      return std::make_pair(std::move(apl), nullptr);
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
    return std::make_pair(nullptr, nullptr);
  }

  /* Check for necessary roles. */
  for (const auto& role : roles.plain) {
    if (t.has_role(role) == true) {
      ldout(cct, 0) << "validated token: " << t.get_project_name()
                    << ":" << t.get_user_name()
                    << " expires: " << t.get_expires() << dendl;
      token_cache.add(token_id, t);
      auto apl = apl_factory->create_apl_remote(cct, get_acl_strategy(t),
                                            get_creds_info(t, roles.admin));
      return std::make_pair(std::move(apl), nullptr);
    }
  }

  ldout(cct, 0) << "user does not hold a matching role; required roles: "
                << g_conf->rgw_keystone_accepted_roles << dendl;

  return std::make_pair(nullptr, nullptr);
}

}; /* namespace keystone */
}; /* namespace auth */
}; /* namespace rgw */
