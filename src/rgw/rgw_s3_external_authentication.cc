// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <string>

#include "common/errno.h"
#include "common/ceph_json.h"
#include "include/types.h"
#include "include/str_list.h"

#include "rgw_common.h"
#include "rgw_s3_external_authentication.h"
#include "rgw_rest_s3.h"
#include "rgw_auth_s3.h"

#include "common/ceph_crypto.h"
#include "common/Cond.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::auth::s3 {
namespace external_authentication {

std::pair<boost::optional<rgw::auth::s3::external_authentication::EAUserInfo>, int>
EAEngine::get_user_info(const DoutPrefixProvider* dpp,
                        const boost::string_view& access_key_id,
                        const std::string& string_to_sign,
                        const boost::string_view& signature,
                        const signature_factory_t& signature_factory) const
{
  using server_signature_t = VersionAbstractor::server_signature_t;
  boost::optional<boost::external_authentication::EAUserInfo> user_info;
  int failure_reason;

  /* Get a token from the cache if one has already been stored */
  boost::optional<boost::tuple<rgw::auth::s3::external_authentication::EAUserInfo, std::string>>
    u = secret_cache.find(access_key_id.to_string());
  
  /* Check that credentials can correctly be used to sign data */
  if (u) {
    std::string sig(signature);
    server_signature_t server_signature = signature_factory(cct, u->get<1>(), string_to_sign);
    if (sig.compare(server_signature) == 0) {
      return std::make_pair(t->get<0>(), 0);
    } else {
      ldpp_dout(dpp, 0) << "Secret string does not correctly sign payload, cache miss" << dendl;
    }
  } else {
    ldpp_dout(dpp, 0) << "No stored secret string, cache miss" << dendl;
  }

  std::tie(user_info, failure_reason) = get_from_server(dpp, access_key_id, string_to_sign, signature);

  if (user_info) {
    /* Fetch secret from server for the access_key_id */
    boost::optional<std::string> secret;
    srd::tie(secret, failure_reason) = get_secret_from_server(dpp, user_info.get_user_id(), access_key_id);

    if (secret) {
      /* Add token, secret pair to cache, and set timeout */
      secret_cache.add(access_key_id.to_string(), *user_info, *secret);
    }
  }

  return std::make_pair(user_info, failure_reason);
}

std::pair<boost::optional<std::string>, int>
EAEngine::get_secret_from_server(const DoutPrefixProvider* dpp,
                                  const std::string& user_id,
                                  const boost::string_view& access_key_id) const
{
  std::string secret_url = config.get_secret_endpoint_url();
  if (secret_url.empty()) {
    return make_pair(boost::none, -EINVAL);
  }

  secret_url.append("?access_key_id=");
  secret_url.append(access_key_id.to_string());

  const string& ea_token = config.get_token();

  int ret;
  ceph::bufferlist bl;
  RGWHTTPTransceiver req(cct, "GET", secret_url, &bl);

  req.append_header("X-Auth-Token", ea_token);

  req.set_verify_ssl(config.verify_ssl());

  ret = req.process(null_yield);
  if (ret < 0) {
    ldpp_dout(dpp, 2) << "s3 external authentication: secret fetching ERROR: "
                  << bl.c_str() << dendl;
    return make_pair(boost::none, ret);
  }

  if (req.get_http_status() == decltype(req)::HTTP_STATUS_NOTFOUND) {
    return make_pair(boost::none, -EINVAL);
  }

  JSONParser parser;
  if (! parser.parse(bl.c_str(), bl.length())) {
    ldpp_dout(dpp, 0) << "s3 external authentication: secret parse ERROR: malformed json" << dendl;
    return make_pair(boost::none, -EINVAL);
  }

  std::string secret_string;

  try {
    JSONDecoder::decode_json("secret", secret_string, parser, true);
  } catch(const JSONDecoder::err& err) {
    ldpp_dout(dpp, 0) << "s3 external authentication: credential parse ERROR: " << err.what() << dendl;
    return make_pair(boost::none, -EINVAL);
  }
  
  return make_pair(secret_string, 0);
}

std::pair<boost::optional<rgw::auth::s3::external_authentication::EAUserInfo>, int>
EAEngine::get_from_server(const DoutPrefixProvider* dpp, const boost::string_view& access_key_id,
                          const std::string& string_to_sign,
                          const boost::string_view& signature) const
{
  std::string server_url = config.get_auth_endpoint_url();
  if (server_url.empty()) {
    return -EINVAL;
  }

  const string& ea_token = config.get_token();

  int ret;
  ceph::bufferlist bl;
  RGWHTTPTransceiver req(cct, "POST", server_url.c_str(), &bl);

  /* set required headers for OPA request */
  req.append_header("X-Auth-Token", ea_token);
  req.append_header("Content-Type", "application/json");

  req.set_verify_ssl(config.verify_ssl());

  /* create json request body */
  JSONFormatter jf;
  jf.open_object_section("");
  jf.open_object_section("credentials");
  jf.dump_string("access_key_id", sview2cstr(access_key_id).data());
  jf.dump_string("signature", sview2cstr(signature).data());
  jf.close_section();
  jf.close_section();

  std::stringstream ss;
  jf.flush(ss);
  req.set_post_data(ss.str());
  req.set_send_length(ss.str().length());

  /* send request */
  ret = req.process(null_yield);
  if (ret < 0) {
    ldpp_dout(op, 2) << "s3 external authentication: access key validation ERROR: "
                << bl.c_str() << dendl;
    throw ret;
  }

  /* if the supplied signature is wrong, we will get 401 from Server */
  if (req.get_http_status() == decltype(req)::HTTP_STATUS_UNAUTHORIZED) {
    return std::make_pair(boost::none, -ERR_SIGNATURE_NO_MATCH);
  } else if (req.get_http_status() == decltype(req)::HTTP_STATUS_NOTFOUND) {
    return std::make_pair(boost::none, -ERR_INVALID_ACCESS_KEY);
  }

  rgw::auth::s3::external_authentication::EAUserInfo user_info;
  ret = user_info.parse(cct, access_key_id, bl);
  if (ret < 0) {
    ldpp_dout(dpp, 2) << "s3 external authentication: response parsing failed, ret=" << ret << dendl;
    throw ret;
  }

  return std::make_pair(std::move(user_info), 0);
}

rgw::auth::Engine::result_t EAEngine::authentication(
  const DoutPrefixProvider* dpp,
  const boost::string_view& access_key_id,
  const boost::string_view& signature,
  const boost::string_view& session_token,
  const string_to_sign_t& string_to_sign,
  const signature_factory_t& signature_factory,
  const completer_factory_t& completer_factory,
  /* Passthorugh only! */
  const req_state* s) const
{
  boost::optional<user_info_t> u;
  int failure_reason;
  std::tie(u, failure_reason) = \
    get_access_id(dpp, access_key_id, string_to_sign, signature, signature_factory);
  if (!u) {
    return result_t::deny(failure_reason);
  }

  auto apl = apl_factory->create_apl_remote(cct, s, get_acl_strategy(*u),
                                            get_creds_info(*u));
  return result_t::grant(std::move(apl), completer_factory(boost::none));
}

EAEngine::acl_strategy_t EAEngine::get_acl_strategy() const
{
  return nullptr;
}

/* rgw::auth::RemoteApplier::AuthInfo */
/* static declaration */
const std::string rgw::auth::RemoteApplier::AuthInfo::NO_SUBUSER;

EAEngine::auth_info_t
EAEngine::get_creds_info(const EAEngine::user_info_t& user_info) const noexcept
{
  using acct_privilege_t = rgw::auth::RemoteApplier::AuthInfo::acct_privilege_t;

  acct_privilege_t level = acct_privilege_t::IS_PLAIN_ACCT;
  if (user_info.is_admin()) {
    level = acct_privilege_t::IS_ADMIN_ACCT;
  }

  rgw_user user;
  if (user_info.get_tenant().empty()) {
    user = rgw_user(user_info.get_user_id());
  } else {
    user = rgw_user(user_info.get_tenant(), user_info.get_user_id());
  }

  if (!user_info.get_subuser().name.empty()) {
    return auth_info_t {
      rgw_user(user_info.get_tenant(), user_info.get_user_id()),
      user_info.get_user_name(),
      user_info.get_subuser().name,
      user_info.get_subuser().perm_mask,
      level,
      TYPE_EXTERNAL_AUTHENTICATION,
    };
  }

  return auth_info_t {
    rgw_user(user_info.get_tenant(), user_info.get_user_id()),
    user_info.get_user_name(),
    rgw::auth::RemoteApplier::AuthInfo::NO_SUBUSER,
    RGW_PERM_FULL_CONTROL,
    level,
    TYPE_EXTERNAL_AUTHENTICATION,
  };
}

bool SecretCache::find(const std::string& access_key_id, user_info_t& user_info, std::string& secret)
{
  std::lock_guard<std::mutex> l(lock);

  map<std::string, secret_entry>::iterator iter = secrets.find(access_key_id);
  if (iter == secrets.end()) {
    return false;
  }

  secret_entry& entry = iter->second;
  secrets_lru.erase(entry.lru_iter);

  const utime_t now = ceph_clock_now();
  if (now > entry.expires) {
    secrets.erase(iter);
    return false;
  }

  secrets_lru.push_front(access_key_id);
  entry.lru_iter = secrets_lru.begin();

  return true;
}

void SecretCache::add(const std::string& access_key_id,
                      const SecretCache::user_info_t& user_info,
		                  const std::string& secret)
{
  std::lock_guard<std::mutex> l(lock);

  map<string, secret_entry>::iterator iter = secrets.find(access_key_id);
  if (iter != secrets.end()) {
    secret_entry& e = iter->second;
    secrets_lru.erase(e.lru_iter);
  }

  const utime_t now = ceph_clock_now();
  secrets_lru.push_front(access_key_id);
  secret_entry& entry = secrets[access_key_id];
  entry.user_info = user_info;
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

int EAUserInfo::parse(CephContext* const cct,
                      const std::string& access_key_id_str,
                      ceph::bufferlist& bl)
{
  JSONParser parser;
  if (! parser.parse(bl.c_str(), bl.length())) {
    ldout(cct, 0) << "s3 external authentication: user info parse ERROR: malformed json" << dendl;
    return -EINVAL;
  }

  try {
    JSONDecoder::decode_json("user_id", user_id, parser, true);
    JSONDecoder::decode_json("user_name", name, parser, true);
    JSONDecoder::decode_json("is_admin", admin, parser, false);
    JSONDecoder::decode_json("tenant", tenant, parser, false);
    JSONObjIter subuser_iter = parser.find_first("subuser");
    subuser.decode_json(*subuser_iter);
  } catch(const JSONDecoder::err& err) {
    ldout(cct, 0) << "s3 external authentication: user info parse ERROR: " << err.what() << dendl;
    return -EINVAL;
  }

  access_key_id = access_key_id_str;

  return 0;
}

}; /* namespace external_authentication */  
}; /* namespace rgw::auth::s3 */
