// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>
#include <fnmatch.h>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string.hpp>
#include <fstream>

#include "common/errno.h"
#include "common/ceph_json.h"
#include "include/types.h"
#include "include/str_list.h"

#include "rgw_common.h"
#include "rgw_keystone.h"
#include "common/armor.h"
#include "common/Cond.h"
#include "rgw_perf_counters.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw
#define PKI_ANS1_PREFIX "MII"

using namespace std;

bool rgw_is_pki_token(const string& token)
{
  return token.compare(0, sizeof(PKI_ANS1_PREFIX) - 1, PKI_ANS1_PREFIX) == 0;
}

void rgw_get_token_id(const string& token, string& token_id)
{
  if (!rgw_is_pki_token(token)) {
    token_id = token;
    return;
  }

  unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];

  MD5 hash;
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  hash.Update((const unsigned char *)token.c_str(), token.size());
  hash.Final(m);

  char calc_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  buf_to_hex(m, CEPH_CRYPTO_MD5_DIGESTSIZE, calc_md5);
  token_id = calc_md5;
}


namespace rgw {
namespace keystone {

ApiVersion CephCtxConfig::get_api_version() const noexcept
{
  switch (g_ceph_context->_conf->rgw_keystone_api_version) {
  case 3:
    return ApiVersion::VER_3;
  case 2:
    return ApiVersion::VER_2;
  default:
    dout(0) << "ERROR: wrong Keystone API version: "
            << g_ceph_context->_conf->rgw_keystone_api_version
            << "; falling back to v2" <<  dendl;
    return ApiVersion::VER_2;
  }
}

std::string CephCtxConfig::get_endpoint_url() const noexcept
{
  static const std::string url = g_ceph_context->_conf->rgw_keystone_url;

  if (url.empty() || boost::algorithm::ends_with(url, "/")) {
    return url;
  } else {
    static const std::string url_normalised = url + '/';
    return url_normalised;
  }
}

/* secrets */
const std::string CephCtxConfig::empty{""};

static inline std::string read_secret(const std::string& file_path)
{
  using namespace std;

  constexpr int16_t size{1024};
  char buf[size];
  string s;

  s.reserve(size);
  ifstream ifs(file_path, ios::in | ios::binary);
  if (ifs) {
    while (true) {
      auto sbuf = ifs.rdbuf();
      auto len =  sbuf->sgetn(buf, size);
      if (!len)
	break;
      s.append(buf, len);
    }
    boost::algorithm::trim(s);
    if (s.back() == '\n')
      s.pop_back();
  }
  return s;
}

std::string CephCtxConfig::get_admin_token() const noexcept
{
  auto& atv = g_ceph_context->_conf->rgw_keystone_admin_token_path;
  if (!atv.empty()) {
    return read_secret(atv);
  } else {
    auto& atv = g_ceph_context->_conf->rgw_keystone_admin_token;
    if (!atv.empty()) {
      return atv;
    }
  }
  return empty;
}

std::string CephCtxConfig::get_admin_password() const noexcept  {
  auto& apv = g_ceph_context->_conf->rgw_keystone_admin_password_path;
  if (!apv.empty()) {
    return read_secret(apv);
  } else {
    auto& apv = g_ceph_context->_conf->rgw_keystone_admin_password;
    if (!apv.empty()) {
      return apv;
    }
  }
  return empty;
}

int Service::get_admin_token(const DoutPrefixProvider *dpp,
                             TokenCache& token_cache,
                             const Config& config,
                             optional_yield y,
                             std::string& token,
                             bool& token_cached)
{
  /* Let's check whether someone uses the deprecated "admin token" feature
   * based on a shared secret from keystone.conf file. */
  const auto& admin_token = config.get_admin_token();
  if (! admin_token.empty()) {
    token = std::string(admin_token.data(), admin_token.length());
    return 0;
  }

  TokenEnvelope t;

  /* Try cache first before calling Keystone for a new admin token. */
  if (token_cache.find_admin(t)) {
    ldpp_dout(dpp, 20) << "found cached admin token" << dendl;
    token = t.token.id;
    token_cached = true;
    return 0;
  }

  /* Call Keystone now. */
  const auto ret = issue_admin_token_request(dpp, config, y, t);
  if (! ret) {
    token_cache.add_admin(t);
    token = t.token.id;
  }

  return ret;
}

int Service::issue_admin_token_request(const DoutPrefixProvider *dpp,
                                       const Config& config,
                                       optional_yield y,
                                       TokenEnvelope& t)
{
  std::string token_url = config.get_endpoint_url();
  if (token_url.empty()) {
    return -EINVAL;
  }

  bufferlist token_bl;
  RGWGetKeystoneAdminToken token_req(dpp->get_cct(), "POST", "", &token_bl);
  token_req.append_header("Content-Type", "application/json");
  JSONFormatter jf;

  const auto keystone_version = config.get_api_version();
  if (keystone_version == ApiVersion::VER_2) {
    AdminTokenRequestVer2 req_serializer(config);
    req_serializer.dump(&jf);

    std::stringstream ss;
    jf.flush(ss);
    token_req.set_post_data(ss.str());
    token_req.set_send_length(ss.str().length());
    token_url.append("v2.0/tokens");

  } else if (keystone_version == ApiVersion::VER_3) {
    AdminTokenRequestVer3 req_serializer(config);
    req_serializer.dump(&jf);

    std::stringstream ss;
    jf.flush(ss);
    token_req.set_post_data(ss.str());
    token_req.set_send_length(ss.str().length());
    token_url.append("v3/auth/tokens");
  } else {
    return -ENOTSUP;
  }

  token_req.set_url(token_url);

  const int ret = token_req.process(y);

  /* Detect rejection earlier than during the token parsing step. */
  if (token_req.get_http_status() ==
          RGWGetKeystoneAdminToken::HTTP_STATUS_UNAUTHORIZED) {
    return -EACCES;
  }

  // throw any other http or connection errors
  if (ret < 0) {
    return ret;
  }

  if (t.parse(dpp, token_req.get_subject_token(), token_bl,
              keystone_version) != 0) {
    return -EINVAL;
  }

  return 0;
}

int Service::get_keystone_barbican_token(const DoutPrefixProvider *dpp,
                                         optional_yield y,
                                         std::string& token)
{
  using keystone_config_t = rgw::keystone::CephCtxConfig;
  using keystone_cache_t = rgw::keystone::TokenCache;

  CephContext* cct = dpp->get_cct();
  auto& config = keystone_config_t::get_instance();
  auto& token_cache = keystone_cache_t::get_instance<keystone_config_t>();

  std::string token_url = config.get_endpoint_url();
  if (token_url.empty()) {
    return -EINVAL;
  }

  rgw::keystone::TokenEnvelope t;

  /* Try cache first. */
  if (token_cache.find_barbican(t)) {
    ldpp_dout(dpp, 20) << "found cached barbican token" << dendl;
    token = t.token.id;
    return 0;
  }

  bufferlist token_bl;
  RGWKeystoneHTTPTransceiver token_req(cct, "POST", "", &token_bl);
  token_req.append_header("Content-Type", "application/json");
  JSONFormatter jf;

  const auto keystone_version = config.get_api_version();
  if (keystone_version == ApiVersion::VER_2) {
    rgw::keystone::BarbicanTokenRequestVer2 req_serializer(cct);
    req_serializer.dump(&jf);

    std::stringstream ss;
    jf.flush(ss);
    token_req.set_post_data(ss.str());
    token_req.set_send_length(ss.str().length());
    token_url.append("v2.0/tokens");

  } else if (keystone_version == ApiVersion::VER_3) {
    BarbicanTokenRequestVer3 req_serializer(cct);
    req_serializer.dump(&jf);

    std::stringstream ss;
    jf.flush(ss);
    token_req.set_post_data(ss.str());
    token_req.set_send_length(ss.str().length());
    token_url.append("v3/auth/tokens");
  } else {
    return -ENOTSUP;
  }

  token_req.set_url(token_url);

  ldpp_dout(dpp, 20) << "Requesting secret from barbican url=" << token_url << dendl;
  const int ret = token_req.process(y);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "Barbican process error:" << token_bl.c_str() << dendl;
    return ret;
  }

  /* Detect rejection earlier than during the token parsing step. */
  if (token_req.get_http_status() ==
      RGWKeystoneHTTPTransceiver::HTTP_STATUS_UNAUTHORIZED) {
    return -EACCES;
  }

  if (t.parse(dpp, token_req.get_subject_token(), token_bl,
              keystone_version) != 0) {
    return -EINVAL;
  }

  token_cache.add_barbican(t);
  token = t.token.id;
  return 0;
}


bool TokenEnvelope::has_role(const std::string& r) const
{
  list<Role>::const_iterator iter;
  for (iter = roles.cbegin(); iter != roles.cend(); ++iter) {
      if (fnmatch(r.c_str(), ((*iter).name.c_str()), 0) == 0) {
        return true;
      }
  }
  return false;
}

int TokenEnvelope::parse(const DoutPrefixProvider *dpp,
                         const std::string& token_str,
                         ceph::bufferlist& bl,
                         const ApiVersion version)
{
  JSONParser parser;
  if (! parser.parse(bl.c_str(), bl.length())) {
    ldpp_dout(dpp, 0) << "Keystone token parse error: malformed json" << dendl;
    return -EINVAL;
  }

  JSONObjIter token_iter = parser.find_first("token");
  JSONObjIter access_iter = parser.find_first("access");

  try {
    if (version == rgw::keystone::ApiVersion::VER_2) {
      if (! access_iter.end()) {
        decode_v2(*access_iter);
      } else if (! token_iter.end()) {
        /* TokenEnvelope structure doesn't follow Identity API v2, so let's
         * fallback to v3. Otherwise we can assume it's wrongly formatted.
         * The whole mechanism is a workaround for s3_token middleware that
         * speaks in v2 disregarding the promise to go with v3. */
        decode_v3(*token_iter);

        /* Identity v3 conveys the token information not as a part of JSON but
         * in the X-Subject-Token HTTP header we're getting from caller. */
        token.id = token_str;
      } else {
        return -EINVAL;
      }
    } else if (version == rgw::keystone::ApiVersion::VER_3) {
      if (! token_iter.end()) {
        decode_v3(*token_iter);
        /* v3 succeeded. We have to fill token.id from external input as it
         * isn't a part of the JSON response anymore. It has been moved
         * to X-Subject-Token HTTP header instead. */
        token.id = token_str;
      } else if (! access_iter.end()) {
        /* If the token cannot be parsed according to V3, try V2. */
        decode_v2(*access_iter);
      } else {
        return -EINVAL;
      }
    } else {
      return -ENOTSUP;
    }
  } catch (const JSONDecoder::err& err) {
    ldpp_dout(dpp, 0) << "Keystone token parse error: " << err.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

/*
 * Maybe one day we'll have the parser find this in Keystone replies.
 * But for now, we use the confguration to augment the list of roles.
 */
void TokenEnvelope::update_roles(const std::vector<std::string> & admin,
                                 const std::vector<std::string> & reader)
{
  for (auto& iter: roles) {
    for (const auto& r : admin) {
      if (fnmatch(r.c_str(), iter.name.c_str(), 0) == 0) {
        iter.is_admin = true;
        break;
      }
    }
    for (const auto& r : reader) {
      if (fnmatch(r.c_str(), iter.name.c_str(), 0) == 0) {
        iter.is_reader = true;
        break;
      }
    }
  }
}

bool TokenCache::find(const std::string& token_id,
                      rgw::keystone::TokenEnvelope& token)
{
  std::lock_guard l{lock};
  return find_locked(token_id, token, tokens, tokens_lru);
}

bool TokenCache::find_service(const std::string& token_id,
                              rgw::keystone::TokenEnvelope& token)
{
  std::lock_guard l{lock};
  return find_locked(token_id, token, service_tokens, service_tokens_lru);
}

bool TokenCache::find_locked(const std::string& token_id, rgw::keystone::TokenEnvelope& token,
                             std::map<std::string, token_entry>& tokens, std::list<std::string>& tokens_lru)
{
  ceph_assert(ceph_mutex_is_locked_by_me(lock));
  map<string, token_entry>::iterator iter = tokens.find(token_id);
  if (iter == tokens.end()) {
    if (perfcounter) perfcounter->inc(l_rgw_keystone_token_cache_miss);
    return false;
  }

  token_entry& entry = iter->second;
  tokens_lru.erase(entry.lru_iter);

  if (entry.token.expired()) {
    tokens.erase(iter);
    if (perfcounter) perfcounter->inc(l_rgw_keystone_token_cache_hit);
    return false;
  }
  token = entry.token;

  tokens_lru.push_front(token_id);
  entry.lru_iter = tokens_lru.begin();

  if (perfcounter) perfcounter->inc(l_rgw_keystone_token_cache_hit);

  return true;
}

bool TokenCache::find_admin(rgw::keystone::TokenEnvelope& token)
{
  std::lock_guard l{lock};

  return find_locked(admin_token_id, token, tokens, tokens_lru);
}

bool TokenCache::find_barbican(rgw::keystone::TokenEnvelope& token)
{
  std::lock_guard l{lock};

  return find_locked(barbican_token_id, token, tokens, tokens_lru);
}

void TokenCache::add(const std::string& token_id,
                     const rgw::keystone::TokenEnvelope& token)
{
  std::lock_guard l{lock};
  add_locked(token_id, token, tokens, tokens_lru);
}

void TokenCache::add_service(const std::string& token_id,
                             const rgw::keystone::TokenEnvelope& token)
{
  std::lock_guard l{lock};
  add_locked(token_id, token, service_tokens, service_tokens_lru);
}

void TokenCache::add_locked(const std::string& token_id, const rgw::keystone::TokenEnvelope& token,
                            std::map<std::string, token_entry>& tokens, std::list<std::string>& tokens_lru)
{
  ceph_assert(ceph_mutex_is_locked_by_me(lock));
  map<string, token_entry>::iterator iter = tokens.find(token_id);
  if (iter != tokens.end()) {
    token_entry& e = iter->second;
    tokens_lru.erase(e.lru_iter);
  }

  tokens_lru.push_front(token_id);
  token_entry& entry = tokens[token_id];
  entry.token = token;
  entry.lru_iter = tokens_lru.begin();

  while (tokens_lru.size() > max) {
    list<string>::reverse_iterator riter = tokens_lru.rbegin();
    iter = tokens.find(*riter);
    ceph_assert(iter != tokens.end());
    tokens.erase(iter);
    tokens_lru.pop_back();
  }
}

void TokenCache::add_admin(const rgw::keystone::TokenEnvelope& token)
{
  std::lock_guard l{lock};

  rgw_get_token_id(token.token.id, admin_token_id);
  add_locked(admin_token_id, token, tokens, tokens_lru);
}

void TokenCache::add_barbican(const rgw::keystone::TokenEnvelope& token)
{
  std::lock_guard l{lock};

  rgw_get_token_id(token.token.id, barbican_token_id);
  add_locked(barbican_token_id, token, tokens, tokens_lru);
}

void TokenCache::invalidate(const DoutPrefixProvider *dpp, const std::string& token_id)
{
  std::lock_guard l{lock};
  map<string, token_entry>::iterator iter = tokens.find(token_id);
  if (iter == tokens.end())
    return;

  ldpp_dout(dpp, 20) << "invalidating revoked token id=" << token_id << dendl;
  token_entry& e = iter->second;
  tokens_lru.erase(e.lru_iter);
  tokens.erase(iter);
}

void TokenCache::invalidate_admin(const DoutPrefixProvider *dpp)
{
  invalidate(dpp, admin_token_id);
}

bool TokenCache::going_down() const
{
  return down_flag;
}

}; /* namespace keystone */
}; /* namespace rgw */

void rgw::keystone::TokenEnvelope::Token::decode_json(JSONObj *obj)
{
  string expires_iso8601;
  struct tm t;

  JSONDecoder::decode_json("id", id, obj, true);
  JSONDecoder::decode_json("tenant", tenant_v2, obj, true);
  JSONDecoder::decode_json("expires", expires_iso8601, obj, true);

  if (parse_iso8601(expires_iso8601.c_str(), &t)) {
    expires = internal_timegm(&t);
  } else {
    expires = 0;
    throw JSONDecoder::err("Failed to parse ISO8601 expiration date from Keystone response.");
  }
}

void rgw::keystone::TokenEnvelope::Role::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("name", name, obj, true);
}

void rgw::keystone::TokenEnvelope::Domain::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj, true);
  JSONDecoder::decode_json("name", name, obj, true);
}

void rgw::keystone::TokenEnvelope::Project::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj, true);
  JSONDecoder::decode_json("name", name, obj, true);
  JSONDecoder::decode_json("domain", domain, obj);
}

void rgw::keystone::TokenEnvelope::User::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj, true);
  JSONDecoder::decode_json("name", name, obj, true);
  JSONDecoder::decode_json("domain", domain, obj);
  JSONDecoder::decode_json("roles", roles_v2, obj);
}

void rgw::keystone::TokenEnvelope::decode_v3(JSONObj* const root_obj)
{
  std::string expires_iso8601;

  JSONDecoder::decode_json("user", user, root_obj, true);
  JSONDecoder::decode_json("expires_at", expires_iso8601, root_obj, true);
  JSONDecoder::decode_json("roles", roles, root_obj, true);
  JSONDecoder::decode_json("project", project, root_obj, true);

  struct tm t;
  if (parse_iso8601(expires_iso8601.c_str(), &t)) {
    token.expires = internal_timegm(&t);
  } else {
    token.expires = 0;
    throw JSONDecoder::err("Failed to parse ISO8601 expiration date"
                           "from Keystone response.");
  }
}

void rgw::keystone::TokenEnvelope::decode_v2(JSONObj* const root_obj)
{
  JSONDecoder::decode_json("user", user, root_obj, true);
  JSONDecoder::decode_json("token", token, root_obj, true);

  roles = user.roles_v2;
  project = token.tenant_v2;
}

/* This utility function shouldn't conflict with the overload of std::to_string
 * provided by string_ref since Boost 1.54 as it's defined outside of the std
 * namespace. I hope we'll remove it soon - just after merging the Matt's PR
 * for bundled Boost. It would allow us to forget that CentOS 7 has Boost 1.53. */
static inline std::string to_string(const std::string_view& s)
{
  return std::string(s.data(), s.length());
}

void rgw::keystone::AdminTokenRequestVer2::dump(Formatter* const f) const
{
  f->open_object_section("token_request");
    f->open_object_section("auth");
      f->open_object_section("passwordCredentials");
        encode_json("username", ::to_string(conf.get_admin_user()), f);
        encode_json("password", ::to_string(conf.get_admin_password()), f);
      f->close_section();
      encode_json("tenantName", ::to_string(conf.get_admin_tenant()), f);
    f->close_section();
  f->close_section();
}

void rgw::keystone::AdminTokenRequestVer3::dump(Formatter* const f) const
{
  f->open_object_section("token_request");
    f->open_object_section("auth");
      f->open_object_section("identity");
        f->open_array_section("methods");
          f->dump_string("", "password");
        f->close_section();
        f->open_object_section("password");
          f->open_object_section("user");
            f->open_object_section("domain");
              encode_json("name", ::to_string(conf.get_admin_domain()), f);
            f->close_section();
            encode_json("name", ::to_string(conf.get_admin_user()), f);
            encode_json("password", ::to_string(conf.get_admin_password()), f);
          f->close_section();
        f->close_section();
      f->close_section();
      f->open_object_section("scope");
        f->open_object_section("project");
          if (! conf.get_admin_project().empty()) {
            encode_json("name", ::to_string(conf.get_admin_project()), f);
          } else {
            encode_json("name", ::to_string(conf.get_admin_tenant()), f);
          }
          f->open_object_section("domain");
            encode_json("name", ::to_string(conf.get_admin_domain()), f);
          f->close_section();
        f->close_section();
      f->close_section();
    f->close_section();
  f->close_section();
}

void rgw::keystone::BarbicanTokenRequestVer2::dump(Formatter* const f) const
{
  f->open_object_section("token_request");
    f->open_object_section("auth");
      f->open_object_section("passwordCredentials");
        encode_json("username", cct->_conf->rgw_keystone_barbican_user, f);
        encode_json("password", cct->_conf->rgw_keystone_barbican_password, f);
      f->close_section();
      encode_json("tenantName", cct->_conf->rgw_keystone_barbican_tenant, f);
    f->close_section();
  f->close_section();
}

void rgw::keystone::BarbicanTokenRequestVer3::dump(Formatter* const f) const
{
  f->open_object_section("token_request");
    f->open_object_section("auth");
      f->open_object_section("identity");
        f->open_array_section("methods");
          f->dump_string("", "password");
        f->close_section();
        f->open_object_section("password");
          f->open_object_section("user");
            f->open_object_section("domain");
              encode_json("name", cct->_conf->rgw_keystone_barbican_domain, f);
            f->close_section();
            encode_json("name", cct->_conf->rgw_keystone_barbican_user, f);
            encode_json("password", cct->_conf->rgw_keystone_barbican_password, f);
          f->close_section();
        f->close_section();
      f->close_section();
      f->open_object_section("scope");
        f->open_object_section("project");
          if (!cct->_conf->rgw_keystone_barbican_project.empty()) {
            encode_json("name", cct->_conf->rgw_keystone_barbican_project, f);
          } else {
            encode_json("name", cct->_conf->rgw_keystone_barbican_tenant, f);
          }
          f->open_object_section("domain");
            encode_json("name", cct->_conf->rgw_keystone_barbican_domain, f);
          f->close_section();
        f->close_section();
      f->close_section();
    f->close_section();
  f->close_section();
}


