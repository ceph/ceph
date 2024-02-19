// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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
#include "rgw_rest_s3.h"
#include "rgw_auth_s3.h"

#include "common/ceph_crypto.h"
#include "common/Cond.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw {
namespace auth {
namespace keystone {

bool
TokenEngine::is_applicable(const std::string& token) const noexcept
{
  return ! token.empty() && ! cct->_conf->rgw_keystone_url.empty();
}

boost::optional<TokenEngine::token_envelope_t>
TokenEngine::get_from_keystone(const DoutPrefixProvider* dpp,
                               const std::string& token,
                               bool allow_expired,
                               optional_yield y) const
{
  /* Unfortunately, we can't use the short form of "using" here. It's because
   * we're aliasing a class' member, not namespace. */
  using RGWValidateKeystoneToken = \
    rgw::keystone::Service::RGWValidateKeystoneToken;

  /* The container for plain response obtained from Keystone. It will be
   * parsed token_envelope_t::parse method. */
  ceph::bufferlist token_body_bl;
  RGWValidateKeystoneToken validate(cct, "GET", "", &token_body_bl);

  std::string url = config.get_endpoint_url();
  if (url.empty()) {
    throw -EINVAL;
  }

  const auto keystone_version = config.get_api_version();
  if (keystone_version == rgw::keystone::ApiVersion::VER_2) {
    url.append("v2.0/tokens/" + token);
  } else if (keystone_version == rgw::keystone::ApiVersion::VER_3) {
    url.append("v3/auth/tokens");

    if (allow_expired) {
      url.append("?allow_expired=1");
    }

    validate.append_header("X-Subject-Token", token);
  }

  std::string admin_token;
  if (rgw::keystone::Service::get_admin_token(dpp, token_cache, config,
                                              y, admin_token) < 0) {
    throw -EINVAL;
  }

  validate.append_header("X-Auth-Token", admin_token);
  validate.set_send_length(0);

  validate.set_url(url);

  int ret = validate.process(y);

  /* NULL terminate for debug output. */
  token_body_bl.append(static_cast<char>(0));

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
    ldpp_dout(dpp, 5) << "Failed keystone auth from " << url << " with "
                  << validate.get_http_status() << dendl;
    return boost::none;
  }
  // throw any other http or connection errors
  if (ret < 0) {
    throw ret;
  }

  ldpp_dout(dpp, 20) << "received response status=" << validate.get_http_status()
                 << ", body=" << token_body_bl.c_str() << dendl;

  TokenEngine::token_envelope_t token_body;
  ret = token_body.parse(dpp, token, token_body_bl, config.get_api_version());
  if (ret < 0) {
    throw ret;
  }

  return token_body;
}

TokenEngine::auth_info_t
TokenEngine::get_creds_info(const TokenEngine::token_envelope_t& token
                           ) const noexcept
{
  using acct_privilege_t = rgw::auth::RemoteApplier::AuthInfo::acct_privilege_t;

  /* Check whether the user has an admin status. */
  acct_privilege_t level = acct_privilege_t::IS_PLAIN_ACCT;
  for (const auto& role : token.roles) {
    if (role.is_admin && !role.is_reader) {
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
    rgw::auth::RemoteApplier::AuthInfo::NO_ACCESS_KEY,
    rgw::auth::RemoteApplier::AuthInfo::NO_SUBUSER,
    TYPE_KEYSTONE
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
  return [allowed_items, token_roles=token.roles](const rgw::auth::Identity::aclspec_t& aclspec) {
    uint32_t perm = 0;

    for (const auto& allowed_item : allowed_items) {
      const auto iter = aclspec.find(allowed_item);

      if (std::end(aclspec) != iter) {
        perm |= iter->second;
      }
    }

    for (const auto& r : token_roles) {
      if (r.is_reader) {
        if (r.is_admin) {    /* system scope reader persona */
          /*
           * Because system reader defeats permissions,
           * we don't even look at the aclspec.
           */
          perm |= RGW_OP_TYPE_READ;
        }
      }
    }

    return perm;
  };
}

TokenEngine::result_t
TokenEngine::authenticate(const DoutPrefixProvider* dpp,
                          const std::string& token,
                          const std::string& service_token,
                          const req_state* const s,
                          optional_yield y) const
{
  bool allow_expired = false;
  boost::optional<TokenEngine::token_envelope_t> t;

  /* This will be initialized on the first call to this method. In C++11 it's
   * also thread-safe. */
  static const struct RolesCacher {
    explicit RolesCacher(CephContext* const cct) {
      get_str_vec(cct->_conf->rgw_keystone_accepted_roles, plain);
      get_str_vec(cct->_conf->rgw_keystone_accepted_admin_roles, admin);
      get_str_vec(cct->_conf->rgw_keystone_accepted_reader_roles, reader);

      /* Let's suppose that having an admin role implies also a regular one. */
      plain.insert(std::end(plain), std::begin(admin), std::end(admin));
    }

    std::vector<std::string> plain;
    std::vector<std::string> admin;
    std::vector<std::string> reader;
  } roles(cct);

  static const struct ServiceTokenRolesCacher {
    explicit ServiceTokenRolesCacher(CephContext* const cct) {
      get_str_vec(cct->_conf->rgw_keystone_service_token_accepted_roles, plain);
    }

    std::vector<std::string> plain;
  } service_token_roles(cct);

  if (! is_applicable(token)) {
    return result_t::deny();
  }

  /* Token ID is a legacy of supporting the service-side validation
   * of PKI/PKIz token type which are already-removed-in-OpenStack.
   * The idea was to bury in cache only a short hash instead of few
   * kilobytes. RadosGW doesn't do the local validation anymore. */
  const auto& token_id = rgw_get_token_id(token);
  ldpp_dout(dpp, 20) << "token_id=" << token_id << dendl;

  /* Check cache first. */
  t = token_cache.find(token_id);
  if (t) {
    ldpp_dout(dpp, 20) << "cached token.project.id=" << t->get_project_id()
                   << dendl;
    auto apl = apl_factory->create_apl_remote(cct, s, get_acl_strategy(*t),
                                              get_creds_info(*t));
    return result_t::grant(std::move(apl));
  }

  /* We have a service token and a token so we verify the service
   * token and if it's invalid the request is invalid. If it's valid
   * we allow an expired token to be used when doing lookup in Keystone.
   * We never get to this if the token is in the cache. */
  if (g_conf()->rgw_keystone_service_token_enabled && ! service_token.empty()) {
    boost::optional<TokenEngine::token_envelope_t> st;

    const auto& service_token_id = rgw_get_token_id(service_token);
    ldpp_dout(dpp, 20) << "service_token_id=" << service_token_id << dendl;

    /* Check cache for service token first. */
    st = token_cache.find_service(service_token_id);
    if (st) {
      ldpp_dout(dpp, 20) << "cached service_token.project.id=" << st->get_project_id()
                     << dendl;

      /* We found the service token in the cache so we allow using an expired
       * token for this request. */
      allow_expired = true;
      ldpp_dout(dpp, 20) << "allowing expired tokens because service_token_id="
                     << service_token_id
                     << " was found in cache" << dendl;
    } else {
      /* Service token was not found in cache. Go to Keystone for validating
       * the token. The allow_expired here must always be false. */
      ceph_assert(allow_expired == false);
      st = get_from_keystone(dpp, service_token, allow_expired, y);

      if (! st) {
        return result_t::deny(-EACCES);
      }

      /* Verify expiration of service token. */
      if (st->expired()) {
        ldpp_dout(dpp, 0) << "got expired service token: " << st->get_project_name()
                       << ":" << st->get_user_name()
                       << " expired " << st->get_expires() << dendl;
        return result_t::deny(-EPERM);
      }

      /* Check for necessary roles for service token. */
      for (const auto& role : service_token_roles.plain) {
        if (st->has_role(role) == true) {
          /* Service token is valid so we allow using an expired token for
           * this request. */
          ldpp_dout(dpp, 20) << "allowing expired tokens because service_token_id="
                         << service_token_id
                         << " is valid, role: "
                         << role << dendl;
          allow_expired = true;
          token_cache.add_service(service_token_id, *st);
          break;
        }
      }

      if (!allow_expired) {
        ldpp_dout(dpp, 0) << "service token user does not hold a matching role; required roles: "
                  << g_conf()->rgw_keystone_service_token_accepted_roles << dendl;
        return result_t::deny(-EPERM);
      }
    }
  }

  /* Token not in cache. Go to the Keystone for validation. This happens even
   * for the legacy PKI/PKIz token types. That's it, after the PKI/PKIz
   * RadosGW-side validation has been removed, we always ask Keystone. */
  t = get_from_keystone(dpp, token, allow_expired, y);
  if (! t) {
    return result_t::deny(-EACCES);
  }
  t->update_roles(roles.admin, roles.reader);

  /* Verify expiration. */
  if (t->expired()) {
    if (allow_expired) {
      ldpp_dout(dpp, 20) << "allowing expired token: " << t->get_project_name()
                    << ":" << t->get_user_name()
                    << " expired: " << t->get_expires()
                    << " because of valid service token" << dendl;
    } else {
      ldpp_dout(dpp, 0) << "got expired token: " << t->get_project_name()
                    << ":" << t->get_user_name()
                    << " expired: " << t->get_expires() << dendl;
      return result_t::deny(-EPERM);
    }
  }

  /* Check for necessary roles. */
  for (const auto& role : roles.plain) {
    if (t->has_role(role) == true) {
      /* If this token was an allowed expired token because we got a
       * service token we need to update the expiration before we cache it. */
      if (allow_expired) {
        time_t now = ceph_clock_now().sec();
        time_t new_expires = now + g_conf()->rgw_keystone_expired_token_cache_expiration;
        ldpp_dout(dpp, 20) << "updating expiration of allowed expired token"
                           << " from old " << t->get_expires() << " to now " << now << " + "
                           << g_conf()->rgw_keystone_expired_token_cache_expiration
                           << " secs = "
                           << new_expires << dendl;
        t->set_expires(new_expires);
      }
      ldpp_dout(dpp, 0) << "validated token: " << t->get_project_name()
                    << ":" << t->get_user_name()
                    << " expires: " << t->get_expires() << dendl;
      token_cache.add(token_id, *t);
      auto apl = apl_factory->create_apl_remote(cct, s, get_acl_strategy(*t),
                                                get_creds_info(*t));
      return result_t::grant(std::move(apl));
    }
  }

  ldpp_dout(dpp, 0) << "user does not hold a matching role; required roles: "
                << g_conf()->rgw_keystone_accepted_roles << dendl;

  return result_t::deny(-EPERM);
}


/*
 * Try to validate S3 auth against keystone s3token interface
 */
std::pair<boost::optional<rgw::keystone::TokenEnvelope>, int>
EC2Engine::get_from_keystone(const DoutPrefixProvider* dpp, const std::string_view& access_key_id,
                             const std::string& string_to_sign,
                             const std::string_view& signature,
                             optional_yield y) const
{
  /* prepare keystone url */
  std::string keystone_url = config.get_endpoint_url();
  if (keystone_url.empty()) {
    throw -EINVAL;
  }

  const auto api_version = config.get_api_version();
  if (api_version == rgw::keystone::ApiVersion::VER_3) {
    keystone_url.append("v3/s3tokens");
  } else {
    keystone_url.append("v2.0/s3tokens");
  }

  /* get authentication token for Keystone. */
  std::string admin_token;
  int ret = rgw::keystone::Service::get_admin_token(dpp, token_cache, config,
                                                    y, admin_token);
  if (ret < 0) {
    ldpp_dout(dpp, 2) << "s3 keystone: cannot get token for keystone access"
                  << dendl;
    throw ret;
  }

  using RGWValidateKeystoneToken
    = rgw::keystone::Service::RGWValidateKeystoneToken;

  /* The container for plain response obtained from Keystone. It will be
   * parsed token_envelope_t::parse method. */
  ceph::bufferlist token_body_bl;
  RGWValidateKeystoneToken validate(cct, "POST", keystone_url, &token_body_bl);

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
  ret = validate.process(y);

  /* if the supplied signature is wrong, we will get 401 from Keystone */
  if (validate.get_http_status() ==
          decltype(validate)::HTTP_STATUS_UNAUTHORIZED) {
    return std::make_pair(boost::none, -ERR_SIGNATURE_NO_MATCH);
  } else if (validate.get_http_status() ==
          decltype(validate)::HTTP_STATUS_NOTFOUND) {
    return std::make_pair(boost::none, -ERR_INVALID_ACCESS_KEY);
  }
  // throw any other http or connection errors
  if (ret < 0) {
    ldpp_dout(dpp, 2) << "s3 keystone: token validation ERROR: "
                  << token_body_bl.c_str() << dendl;
    throw ret;
  }

  /* now parse response */
  rgw::keystone::TokenEnvelope token_envelope;
  ret = token_envelope.parse(dpp, std::string(), token_body_bl, api_version);
  if (ret < 0) {
    ldpp_dout(dpp, 2) << "s3 keystone: token parsing failed, ret=0" << ret
                  << dendl;
    throw ret;
  }

  return std::make_pair(std::move(token_envelope), 0);
}

auto EC2Engine::get_secret_from_keystone(const DoutPrefixProvider* dpp,
                                         const std::string& user_id,
                                         const std::string_view& access_key_id,
                                         optional_yield y) const
    -> std::pair<boost::optional<std::string>, int>
{
  /*  Fetch from /users/{USER_ID}/credentials/OS-EC2/{ACCESS_KEY_ID} */
  /* Should return json with response key "credential" which contains entry "secret"*/

  /* prepare keystone url */
  std::string keystone_url = config.get_endpoint_url();
  if (keystone_url.empty()) {
    return make_pair(boost::none, -EINVAL);
  }

  const auto api_version = config.get_api_version();
  if (api_version == rgw::keystone::ApiVersion::VER_3) {
    keystone_url.append("v3/");
  } else {
    keystone_url.append("v2.0/");
  }
  keystone_url.append("users/");
  keystone_url.append(user_id);
  keystone_url.append("/credentials/OS-EC2/");
  keystone_url.append(std::string(access_key_id));

  /* get authentication token for Keystone. */
  std::string admin_token;
  int ret = rgw::keystone::Service::get_admin_token(dpp, token_cache, config,
                                                    y, admin_token);
  if (ret < 0) {
    ldpp_dout(dpp, 2) << "s3 keystone: cannot get token for keystone access"
                  << dendl;
    return make_pair(boost::none, ret);
  }

  using RGWGetAccessSecret
    = rgw::keystone::Service::RGWKeystoneHTTPTransceiver;

  /* The container for plain response obtained from Keystone.*/
  ceph::bufferlist token_body_bl;
  RGWGetAccessSecret secret(cct, "GET", keystone_url, &token_body_bl);

  /* set required headers for keystone request */
  secret.append_header("X-Auth-Token", admin_token);

  /* check if we want to verify keystone's ssl certs */
  secret.set_verify_ssl(cct->_conf->rgw_keystone_verify_ssl);

  /* send request */
  ret = secret.process(y);

  /* if the supplied access key isn't found, we will get 404 from Keystone */
  if (secret.get_http_status() ==
          decltype(secret)::HTTP_STATUS_NOTFOUND) {
    return make_pair(boost::none, -ERR_INVALID_ACCESS_KEY);
  }
  // return any other http or connection errors
  if (ret < 0) {
    ldpp_dout(dpp, 2) << "s3 keystone: secret fetching error: "
                  << token_body_bl.c_str() << dendl;
    return make_pair(boost::none, ret);
  }

  /* now parse response */

  JSONParser parser;
  if (! parser.parse(token_body_bl.c_str(), token_body_bl.length())) {
    ldpp_dout(dpp, 0) << "Keystone credential parse error: malformed json" << dendl;
    return make_pair(boost::none, -EINVAL);
  }

  JSONObjIter credential_iter = parser.find_first("credential");
  std::string secret_string;

  try {
    if (!credential_iter.end()) {
      JSONDecoder::decode_json("secret", secret_string, *credential_iter, true);
    } else {
      ldpp_dout(dpp, 0) << "Keystone credential not present in return from server" << dendl;
      return make_pair(boost::none, -EINVAL);
    }
  } catch (const JSONDecoder::err& err) {
    ldpp_dout(dpp, 0) << "Keystone credential parse error: " << err.what() << dendl;
    return make_pair(boost::none, -EINVAL);
  }

  return make_pair(secret_string, 0);
}

/*
 * Try to get a token for S3 authentication, using a secret cache if available
 */
auto EC2Engine::get_access_token(const DoutPrefixProvider* dpp,
                                 const std::string_view& access_key_id,
                                 const std::string& string_to_sign,
                                 const std::string_view& signature,
                                 const signature_factory_t& signature_factory,
                                 bool ignore_signature,
                                 optional_yield y) const
    -> access_token_result
{
  using server_signature_t = VersionAbstractor::server_signature_t;
  boost::optional<rgw::keystone::TokenEnvelope> token;
  boost::optional<std::string> secret;
  int failure_reason;

  /* Get a token from the cache if one has already been stored */
  boost::optional<boost::tuple<rgw::keystone::TokenEnvelope, std::string>>
    t = secret_cache.find(std::string(access_key_id));

  /* Check that credentials can correctly be used to sign data */
  if (t) {
    /* We should ignore checking signature in cache if caller tells us to which
     * means we're handling a HTTP OPTIONS call. */
    if (ignore_signature) {
      ldpp_dout(dpp, 20) << "ignore_signature set and found in cache" << dendl;
      return {t->get<0>(), t->get<1>(), 0};
    } else {
      std::string sig(signature);
      server_signature_t server_signature = signature_factory(cct, t->get<1>(), string_to_sign);
      if (sig.compare(server_signature) == 0) {
        return {t->get<0>(), t->get<1>(), 0};
      } else {
        ldpp_dout(dpp, 0) << "Secret string does not correctly sign payload, cache miss" << dendl;
      }
    }
  } else {
    ldpp_dout(dpp, 0) << "No stored secret string, cache miss" << dendl;
  }

  /* No cached token, token expired, or secret invalid: fall back to keystone */
  std::tie(token, failure_reason) =
      get_from_keystone(dpp, access_key_id, string_to_sign, signature, y);

  if (token) {
    /* Fetch secret from keystone for the access_key_id */
    std::tie(secret, failure_reason) =
        get_secret_from_keystone(dpp, token->get_user_id(), access_key_id, y);

    if (secret) {
      /* Add token, secret pair to cache, and set timeout */
      secret_cache.add(std::string(access_key_id), *token, *secret);
    }
  }

  return {token, secret, failure_reason};
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
                          const std::vector<std::string>& admin_roles,
                          const std::string& access_key_id
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
    access_key_id,
    rgw::auth::RemoteApplier::AuthInfo::NO_SUBUSER,
    TYPE_KEYSTONE
  };
}

rgw::auth::Engine::result_t EC2Engine::authenticate(
  const DoutPrefixProvider* dpp,
  const std::string_view& access_key_id,
  const std::string_view& signature,
  const std::string_view& session_token,
  const string_to_sign_t& string_to_sign,
  const signature_factory_t& signature_factory,
  const completer_factory_t& completer_factory,
  const req_state* s,
  optional_yield y) const
{
  /* This will be initialized on the first call to this method. In C++11 it's
   * also thread-safe. */
  static const struct RolesCacher {
    explicit RolesCacher(CephContext* const cct) {
      get_str_vec(cct->_conf->rgw_keystone_accepted_roles, plain);
      get_str_vec(cct->_conf->rgw_keystone_accepted_admin_roles, admin);

      /* Let's suppose that having an admin role implies also a regular one. */
      plain.insert(std::end(plain), std::begin(admin), std::end(admin));
    }

    std::vector<std::string> plain;
    std::vector<std::string> admin;
  } accepted_roles(cct);

  /* When we handle a HTTP OPTIONS call we must ignore the signature */
  bool ignore_signature = (s->op_type == RGW_OP_OPTIONS_CORS);

  auto [t, secret_key, failure_reason] =
    get_access_token(dpp, access_key_id, string_to_sign,
                     signature, signature_factory, ignore_signature, y);
  if (! t) {
    if (failure_reason == -ERR_SIGNATURE_NO_MATCH) {
      // we looked up a secret but it didn't generate the same signature as
      // the client. since we found this access key in keystone, we should
      // reject the request instead of trying other engines
      return result_t::reject(failure_reason);
    }
    return result_t::deny(failure_reason);
  }

  /* Verify expiration. */
  if (t->expired()) {
    ldpp_dout(dpp, 0) << "got expired token: " << t->get_project_name()
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
    ldpp_dout(dpp, 5) << "s3 keystone: user does not hold a matching role;"
                     " required roles: "
                  << cct->_conf->rgw_keystone_accepted_roles << dendl;
    return result_t::deny();
  } else {
    /* everything seems fine, continue with this user */
    ldpp_dout(dpp, 5) << "s3 keystone: validated token: " << t->get_project_name()
                  << ":" << t->get_user_name()
                  << " expires: " << t->get_expires() << dendl;

    auto apl = apl_factory->create_apl_remote(cct, s, get_acl_strategy(*t),
                                              get_creds_info(*t, accepted_roles.admin, std::string(access_key_id)));
    return result_t::grant(std::move(apl), completer_factory(secret_key));
  }
}

bool SecretCache::find(const std::string& token_id,
                       SecretCache::token_envelope_t& token,
		       std::string &secret)
{
  std::lock_guard<std::mutex> l(lock);

  map<std::string, secret_entry>::iterator iter = secrets.find(token_id);
  if (iter == secrets.end()) {
    return false;
  }

  secret_entry& entry = iter->second;
  secrets_lru.erase(entry.lru_iter);

  const utime_t now = ceph_clock_now();
  if (entry.token.expired() || now > entry.expires) {
    secrets.erase(iter);
    return false;
  }
  token = entry.token;
  secret = entry.secret;

  secrets_lru.push_front(token_id);
  entry.lru_iter = secrets_lru.begin();

  return true;
}

void SecretCache::add(const std::string& token_id,
                      const SecretCache::token_envelope_t& token,
		      const std::string& secret)
{
  std::lock_guard<std::mutex> l(lock);

  map<string, secret_entry>::iterator iter = secrets.find(token_id);
  if (iter != secrets.end()) {
    secret_entry& e = iter->second;
    secrets_lru.erase(e.lru_iter);
  }

  const utime_t now = ceph_clock_now();
  secrets_lru.push_front(token_id);
  secret_entry& entry = secrets[token_id];
  entry.token = token;
  entry.secret = secret;
  entry.expires = now + s3_token_expiry_length;
  entry.lru_iter = secrets_lru.begin();

  while (secrets_lru.size() > max) {
    list<string>::reverse_iterator riter = secrets_lru.rbegin();
    iter = secrets.find(*riter);
    assert(iter != secrets.end());
    secrets.erase(iter);
    secrets_lru.pop_back();
  }
}

}; /* namespace keystone */
}; /* namespace auth */
}; /* namespace rgw */
