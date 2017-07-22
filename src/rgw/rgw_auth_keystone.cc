// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string>
#include <vector>

#include <errno.h>
#include <fnmatch.h>

#include "rgw_b64.h"

#include "common/errno.h"
#include "common/ceph_json.h"
#include "include/types.h"
#include "include/str_list.h"

#include "rgw_common.h"
#include "rgw_keystone.h"
#include "rgw_auth_keystone.h"
#include "rgw_keystone.h"
#include "rgw_rest_s3.h"
#include "rgw_auth_s3.h"

#include "common/ceph_crypto_cms.h"
#include "common/armor.h"
#include "common/Cond.h"

#define dout_subsys ceph_subsys_rgw


namespace rgw {
namespace auth {
namespace keystone {

bool
TokenEngine::is_applicable(const std::string& token) const noexcept
{
  return ! token.empty() && ! cct->_conf->rgw_keystone_url.empty();
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

boost::optional<TokenEngine::token_envelope_t>
TokenEngine::get_from_keystone(const std::string& token) const
{
  /* Unfortunately, we can't use the short form of "using" here. It's because
   * we're aliasing a class' member, not namespace. */
  using RGWValidateKeystoneToken = \
    rgw::keystone::Service::RGWValidateKeystoneToken;

  /* The container for plain response obtained from Keystone. It will be
   * parsed token_envelope_t::parse method. */
  ceph::bufferlist token_body_bl;
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
  if (rgw::keystone::Service::get_admin_token(cct, token_cache, config,
                                              admin_token) < 0) {
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
    return boost::none;
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
    TYPE_KEYSTONE,
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
  return [allowed_items](const rgw::auth::Identity::aclspec_t& aclspec) {
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
TokenEngine::authenticate(const std::string& token,
                          const req_state* const s) const
{
  boost::optional<TokenEngine::token_envelope_t> t;

  /* This will be initialized on the first call to this method. In C++11 it's
   * also thread-safe. */
  static const struct RolesCacher {
    RolesCacher(CephContext* const cct) {
      get_str_vec(cct->_conf->rgw_keystone_accepted_roles, plain);
      get_str_vec(cct->_conf->rgw_keystone_accepted_admin_roles, admin);

      /* Let's suppose that having an admin role implies also a regular one. */
      plain.insert(std::end(plain), std::begin(admin), std::end(admin));
    }

    std::vector<std::string> plain;
    std::vector<std::string> admin;
  } roles(cct);

  if (! is_applicable(token)) {
    return result_t::deny();
  }

  /* Token ID is a concept that makes dealing with PKI tokens more effective.
   * Instead of storing several kilobytes, a short hash can be burried. */
  const auto& token_id = rgw_get_token_id(token);
  ldout(cct, 20) << "token_id=" << token_id << dendl;

  /* Check cache first. */
  t = token_cache.find(token_id);
  if (t) {
    ldout(cct, 20) << "cached token.project.id=" << t->get_project_id()
                   << dendl;
    auto apl = apl_factory->create_apl_remote(cct, s, get_acl_strategy(*t),
                                              get_creds_info(*t, roles.admin));
    return result_t::grant(std::move(apl));
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

  if (! t) {
    return result_t::deny(-EACCES);
  }

  /* Verify expiration. */
  if (t->expired()) {
    ldout(cct, 0) << "got expired token: " << t->get_project_name()
                  << ":" << t->get_user_name()
                  << " expired: " << t->get_expires() << dendl;
    return result_t::deny(-EPERM);
  }

  /* Check for necessary roles. */
  for (const auto& role : roles.plain) {
    if (t->has_role(role) == true) {
      ldout(cct, 0) << "validated token: " << t->get_project_name()
                    << ":" << t->get_user_name()
                    << " expires: " << t->get_expires() << dendl;
      token_cache.add(token_id, *t);
      auto apl = apl_factory->create_apl_remote(cct, s, get_acl_strategy(*t),
                                            get_creds_info(*t, roles.admin));
      return result_t::grant(std::move(apl));
    }
  }

  ldout(cct, 0) << "user does not hold a matching role; required roles: "
                << g_conf->rgw_keystone_accepted_roles << dendl;

  return result_t::deny(-EPERM);
}


/*
 * Try to validate S3 auth against keystone s3token interface
 */
std::pair<boost::optional<rgw::keystone::TokenEnvelope>, int>
EC2Engine::get_from_keystone(const boost::string_view& access_key_id,
                             const std::string& string_to_sign,
                             const boost::string_view& signature) const
{
  /* prepare keystone url */
  std::string keystone_url = config.get_endpoint_url();
  if (keystone_url.empty()) {
    throw -EINVAL;
  }

  const auto api_version = config.get_api_version();
  if (config.get_api_version() == rgw::keystone::ApiVersion::VER_3) {
    keystone_url.append("v3/s3tokens");
  } else {
    keystone_url.append("v2.0/s3tokens");
  }

  /* get authentication token for Keystone. */
  std::string admin_token;
  int ret = rgw::keystone::Service::get_admin_token(cct, token_cache, config,
                                                    admin_token);
  if (ret < 0) {
    ldout(cct, 2) << "s3 keystone: cannot get token for keystone access"
                  << dendl;
    throw ret;
  }

  using RGWValidateKeystoneToken
    = rgw::keystone::Service::RGWValidateKeystoneToken;

  /* The container for plain response obtained from Keystone. It will be
   * parsed token_envelope_t::parse method. */
  ceph::bufferlist token_body_bl;
  RGWValidateKeystoneToken validate(cct, &token_body_bl);

  /* set required headers for keystone request */
  validate.append_header("X-Auth-Token", admin_token);
  validate.append_header("Content-Type", "application/json");

  /* check if we want to verify keystone's ssl certs */
  validate.set_verify_ssl(cct->_conf->rgw_keystone_verify_ssl);

  /* create json credentials request body */
  JSONFormatter credentials(false);
  credentials.open_object_section("");
  credentials.open_object_section("credentials");
  credentials.dump_string("access", sview2cstr(access_key_id).data());
  credentials.dump_string("token", rgw::to_base64(string_to_sign));
  credentials.dump_string("signature", sview2cstr(signature).data());
  credentials.close_section();
  credentials.close_section();

  std::stringstream os;
  credentials.flush(os);
  validate.set_post_data(os.str());
  validate.set_send_length(os.str().length());

  /* send request */
  ret = validate.process("POST", keystone_url.c_str());
  if (ret < 0) {
    ldout(cct, 2) << "s3 keystone: token validation ERROR: "
                  << token_body_bl.c_str() << dendl;
    throw ret;
  }

  /* if the supplied signature is wrong, we will get 401 from Keystone */
  if (validate.get_http_status() ==
          decltype(validate)::HTTP_STATUS_UNAUTHORIZED) {
    return std::make_pair(boost::none, -ERR_SIGNATURE_NO_MATCH);
  } else if (validate.get_http_status() ==
          decltype(validate)::HTTP_STATUS_NOTFOUND) {
    return std::make_pair(boost::none, -ERR_INVALID_ACCESS_KEY);
  }

  /* now parse response */
  rgw::keystone::TokenEnvelope token_envelope;
  ret = token_envelope.parse(cct, std::string(), token_body_bl, api_version);
  if (ret < 0) {
    ldout(cct, 2) << "s3 keystone: token parsing failed, ret=0" << ret
                  << dendl;
    throw ret;
  }

  return std::make_pair(std::move(token_envelope), 0);
}

EC2Engine::acl_strategy_t
EC2Engine::get_acl_strategy(const EC2Engine::token_envelope_t&) const
{
  /* This is based on the assumption that the default acl strategy in
   * get_perms_from_aclspec, will take care. Extra acl spec is not required. */
  return nullptr;
}

EC2Engine::auth_info_t
EC2Engine::get_creds_info(const EC2Engine::token_envelope_t& token,
                          const std::vector<std::string>& admin_roles
                         ) const noexcept
{
  using acct_privilege_t = \
    rgw::auth::RemoteApplier::AuthInfo::acct_privilege_t;

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
    TYPE_KEYSTONE,
  };
}

rgw::auth::Engine::result_t EC2Engine::authenticate(
  const boost::string_view& access_key_id,
  const boost::string_view& signature,
  const string_to_sign_t& string_to_sign,
  const signature_factory_t&,
  const completer_factory_t& completer_factory,
  /* Passthorugh only! */
  const req_state* s) const
{
  /* This will be initialized on the first call to this method. In C++11 it's
   * also thread-safe. */
  static const struct RolesCacher {
    RolesCacher(CephContext* const cct) {
      get_str_vec(cct->_conf->rgw_keystone_accepted_roles, plain);
      get_str_vec(cct->_conf->rgw_keystone_accepted_admin_roles, admin);

      /* Let's suppose that having an admin role implies also a regular one. */
      plain.insert(std::end(plain), std::begin(admin), std::end(admin));
    }

    std::vector<std::string> plain;
    std::vector<std::string> admin;
  } accepted_roles(cct);

  boost::optional<token_envelope_t> t;
  int failure_reason;
  std::tie(t, failure_reason) = \
    get_from_keystone(access_key_id, string_to_sign, signature);
  if (! t) {
    return result_t::deny(failure_reason);
  }

  /* Verify expiration. */
  if (t->expired()) {
    ldout(cct, 0) << "got expired token: " << t->get_project_name()
                  << ":" << t->get_user_name()
                  << " expired: " << t->get_expires() << dendl;
    return result_t::deny();
  }

  /* check if we have a valid role */
  bool found = false;
  for (const auto& role : accepted_roles.plain) {
    if (t->has_role(role) == true) {
      found = true;
      break;
    }
  }

  if (! found) {
    ldout(cct, 5) << "s3 keystone: user does not hold a matching role;"
                     " required roles: "
                  << cct->_conf->rgw_keystone_accepted_roles << dendl;
    return result_t::deny();
  } else {
    /* everything seems fine, continue with this user */
    ldout(cct, 5) << "s3 keystone: validated token: " << t->get_project_name()
                  << ":" << t->get_user_name()
                  << " expires: " << t->get_expires() << dendl;

    auto apl = apl_factory->create_apl_remote(cct, s, get_acl_strategy(*t),
                                              get_creds_info(*t, accepted_roles.admin));
    return result_t::grant(std::move(apl), completer_factory(boost::none));
  }
}

}; /* namespace keystone */
}; /* namespace auth */
}; /* namespace rgw */
