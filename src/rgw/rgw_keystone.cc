// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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
#include "common/ceph_crypto_cms.h"
#include "common/armor.h"
#include "common/Cond.h"
#include "rgw_perf_counters.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

int rgw_open_cms_envelope(CephContext * const cct,
                          const std::string& src,
                          std::string& dst)             /* out */
{
#define BEGIN_CMS "-----BEGIN CMS-----"
#define END_CMS "-----END CMS-----"

  int start = src.find(BEGIN_CMS);
  if (start < 0) {
    ldout(cct, 0) << "failed to find " << BEGIN_CMS << " in response" << dendl;
    return -EINVAL;
  }
  start += sizeof(BEGIN_CMS) - 1;

  int end = src.find(END_CMS);
  if (end < 0) {
    ldout(cct, 0) << "failed to find " << END_CMS << " in response" << dendl;
    return -EINVAL;
  }

  string s = src.substr(start, end - start);

  int pos = 0;

  do {
    int next = s.find('\n', pos);
    if (next < 0) {
      dst.append(s.substr(pos));
      break;
    } else {
      dst.append(s.substr(pos, next - pos));
    }
    pos = next + 1;
  } while (pos < (int)s.size());

  return 0;
}

int rgw_decode_b64_cms(CephContext * const cct,
                       const string& signed_b64,
                       bufferlist& bl)
{
  bufferptr signed_ber(signed_b64.size() * 2);
  char *dest = signed_ber.c_str();
  const char *src = signed_b64.c_str();
  size_t len = signed_b64.size();
  char buf[len + 1];
  buf[len] = '\0';

  for (size_t i = 0; i < len; i++, src++) {
    if (*src != '-') {
      buf[i] = *src;
    } else {
      buf[i] = '/';
    }
  }

  int ret = ceph_unarmor(dest, dest + signed_ber.length(), buf,
                         buf + signed_b64.size());
  if (ret < 0) {
    ldout(cct, 0) << "ceph_unarmor() failed, ret=" << ret << dendl;
    return ret;
  }

  bufferlist signed_ber_bl;
  signed_ber_bl.append(signed_ber);

  ret = ceph_decode_cms(cct, signed_ber_bl, bl);
  if (ret < 0) {
    ldout(cct, 0) << "ceph_decode_cms returned " << ret << dendl;
    return ret;
  }

  return 0;
}

#define PKI_ANS1_PREFIX "MII"

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

int Service::get_admin_token(CephContext* const cct,
                             TokenCache& token_cache,
                             const Config& config,
                             std::string& token)
{
  /* Let's check whether someone uses the deprecated "admin token" feauture
   * based on a shared secret from keystone.conf file. */
  const auto& admin_token = config.get_admin_token();
  if (! admin_token.empty()) {
    token = std::string(admin_token.data(), admin_token.length());
    return 0;
  }

  TokenEnvelope t;

  /* Try cache first before calling Keystone for a new admin token. */
  if (token_cache.find_admin(t)) {
    ldout(cct, 20) << "found cached admin token" << dendl;
    token = t.token.id;
    return 0;
  }

  /* Call Keystone now. */
  const auto ret = issue_admin_token_request(cct, config, t);
  if (! ret) {
    token_cache.add_admin(t);
    token = t.token.id;
  }

  return ret;
}

int Service::issue_admin_token_request(CephContext* const cct,
                                       const Config& config,
                                       TokenEnvelope& t)
{
  std::string token_url = config.get_endpoint_url();
  if (token_url.empty()) {
    return -EINVAL;
  }

  bufferlist token_bl;
  RGWGetKeystoneAdminToken token_req(cct, "POST", "", &token_bl);
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

  const int ret = token_req.process(null_yield);
  if (ret < 0) {
    return ret;
  }

  /* Detect rejection earlier than during the token parsing step. */
  if (token_req.get_http_status() ==
          RGWGetKeystoneAdminToken::HTTP_STATUS_UNAUTHORIZED) {
    return -EACCES;
  }

  if (t.parse(cct, token_req.get_subject_token(), token_bl,
              keystone_version) != 0) {
    return -EINVAL;
  }

  return 0;
}

int Service::get_keystone_barbican_token(CephContext * const cct,
                                         std::string& token)
{
  using keystone_config_t = rgw::keystone::CephCtxConfig;
  using keystone_cache_t = rgw::keystone::TokenCache;

  auto& config = keystone_config_t::get_instance();
  auto& token_cache = keystone_cache_t::get_instance<keystone_config_t>();

  std::string token_url = config.get_endpoint_url();
  if (token_url.empty()) {
    return -EINVAL;
  }

  rgw::keystone::TokenEnvelope t;

  /* Try cache first. */
  if (token_cache.find_barbican(t)) {
    ldout(cct, 20) << "found cached barbican token" << dendl;
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

  ldout(cct, 20) << "Requesting secret from barbican url=" << token_url << dendl;
  const int ret = token_req.process(null_yield);
  if (ret < 0) {
    ldout(cct, 20) << "Barbican process error:" << token_bl.c_str() << dendl;
    return ret;
  }

  /* Detect rejection earlier than during the token parsing step. */
  if (token_req.get_http_status() ==
      RGWKeystoneHTTPTransceiver::HTTP_STATUS_UNAUTHORIZED) {
    return -EACCES;
  }

  if (t.parse(cct, token_req.get_subject_token(), token_bl,
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

int TokenEnvelope::parse(CephContext* const cct,
                         const std::string& token_str,
                         ceph::bufferlist& bl,
                         const ApiVersion version)
{
  JSONParser parser;
  if (! parser.parse(bl.c_str(), bl.length())) {
    ldout(cct, 0) << "Keystone token parse error: malformed json" << dendl;
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

        /* Identity v3 conveys the token inforamtion not as a part of JSON but
         * in the X-Subject-Token HTTP header we're getting from caller. */
        token.id = token_str;
      } else {
        return -EINVAL;
      }
    } else if (version == rgw::keystone::ApiVersion::VER_3) {
      if (! token_iter.end()) {
        decode_v3(*token_iter);
        /* v3 suceeded. We have to fill token.id from external input as it
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
  } catch (JSONDecoder::err& err) {
    ldout(cct, 0) << "Keystone token parse error: " << err.message << dendl;
    return -EINVAL;
  }

  return 0;
}

bool TokenCache::find(const std::string& token_id,
                      rgw::keystone::TokenEnvelope& token)
{
  Mutex::Locker l(lock);
  return find_locked(token_id, token);
}

bool TokenCache::find_locked(const std::string& token_id,
                             rgw::keystone::TokenEnvelope& token)
{
  ceph_assert(lock.is_locked_by_me());
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
  Mutex::Locker l(lock);

  return find_locked(admin_token_id, token);
}

bool TokenCache::find_barbican(rgw::keystone::TokenEnvelope& token)
{
  Mutex::Locker l(lock);

  return find_locked(barbican_token_id, token);
}

void TokenCache::add(const std::string& token_id,
                     const rgw::keystone::TokenEnvelope& token)
{
  Mutex::Locker l(lock);
  add_locked(token_id, token);
}

void TokenCache::add_locked(const std::string& token_id,
                            const rgw::keystone::TokenEnvelope& token)
{
  ceph_assert(lock.is_locked_by_me());
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
  Mutex::Locker l(lock);

  rgw_get_token_id(token.token.id, admin_token_id);
  add_locked(admin_token_id, token);
}

void TokenCache::add_barbican(const rgw::keystone::TokenEnvelope& token)
{
  Mutex::Locker l(lock);

  rgw_get_token_id(token.token.id, barbican_token_id);
  add_locked(barbican_token_id, token);
}

void TokenCache::invalidate(const std::string& token_id)
{
  Mutex::Locker l(lock);
  map<string, token_entry>::iterator iter = tokens.find(token_id);
  if (iter == tokens.end())
    return;

  ldout(cct, 20) << "invalidating revoked token id=" << token_id << dendl;
  token_entry& e = iter->second;
  tokens_lru.erase(e.lru_iter);
  tokens.erase(iter);
}

int TokenCache::RevokeThread::check_revoked()
{
  std::string url;
  std::string token;

  bufferlist bl;
  RGWGetRevokedTokens req(cct, "GET", "", &bl);

  if (rgw::keystone::Service::get_admin_token(cct, *cache, config, token) < 0) {
    return -EINVAL;
  }

  url = config.get_endpoint_url();
  if (url.empty()) {
    return -EINVAL;
  }

  req.append_header("X-Auth-Token", token);

  const auto keystone_version = config.get_api_version();
  if (keystone_version == rgw::keystone::ApiVersion::VER_2) {
    url.append("v2.0/tokens/revoked");
  } else if (keystone_version == rgw::keystone::ApiVersion::VER_3) {
    url.append("v3/auth/tokens/OS-PKI/revoked");
  }

  req.set_url(url);

  req.set_send_length(0);
  int ret = req.process(null_yield);
  if (ret < 0) {
    return ret;
  }

  bl.append((char)0); // NULL terminate for debug output

  ldout(cct, 10) << "request returned " << bl.c_str() << dendl;

  JSONParser parser;

  if (!parser.parse(bl.c_str(), bl.length())) {
    ldout(cct, 0) << "malformed json" << dendl;
    return -EINVAL;
  }

  JSONObjIter iter = parser.find_first("signed");
  if (iter.end()) {
    ldout(cct, 0) << "revoked tokens response is missing signed section" << dendl;
    return -EINVAL;
  }

  JSONObj *signed_obj = *iter;
  const std::string signed_str = signed_obj->get_data();

  ldout(cct, 10) << "signed=" << signed_str << dendl;

  std::string signed_b64;
  ret = rgw_open_cms_envelope(cct, signed_str, signed_b64);
  if (ret < 0) {
    return ret;
  }

  ldout(cct, 10) << "content=" << signed_b64 << dendl;
  
  bufferlist json;
  ret = rgw_decode_b64_cms(cct, signed_b64, json);
  if (ret < 0) {
    return ret;
  }

  ldout(cct, 10) << "ceph_decode_cms: decoded: " << json.c_str() << dendl;

  JSONParser list_parser;
  if (!list_parser.parse(json.c_str(), json.length())) {
    ldout(cct, 0) << "malformed json" << dendl;
    return -EINVAL;
  }

  JSONObjIter revoked_iter = list_parser.find_first("revoked");
  if (revoked_iter.end()) {
    ldout(cct, 0) << "no revoked section in json" << dendl;
    return -EINVAL;
  }

  JSONObj *revoked_obj = *revoked_iter;

  JSONObjIter tokens_iter = revoked_obj->find_first();
  for (; !tokens_iter.end(); ++tokens_iter) {
    JSONObj *o = *tokens_iter;

    JSONObj *token = o->find_obj("id");
    if (!token) {
      ldout(cct, 0) << "bad token in array, missing id" << dendl;
      continue;
    }

    const std::string token_id = token->get_data();
    cache->invalidate(token_id);
  }
  
  return 0;
}

bool TokenCache::going_down() const
{
  return down_flag;
}

void* TokenCache::RevokeThread::entry()
{
  do {
    ldout(cct, 2) << "keystone revoke thread: start" << dendl;
    int r = check_revoked();
    if (r < 0) {
      ldout(cct, 0) << "ERROR: keystone revocation processing returned error r="
                    << r << dendl;
    }

    if (cache->going_down()) {
      break;
    }

    lock.Lock();
    cond.WaitInterval(lock,
		      utime_t(cct->_conf->rgw_keystone_revocation_interval, 0));
    lock.Unlock();
  } while (!cache->going_down());

  return nullptr;
}

void TokenCache::RevokeThread::stop()
{
  Mutex::Locker l(lock);
  cond.Signal();
}

}; /* namespace keystone */
}; /* namespace rgw */
