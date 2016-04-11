// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <fnmatch.h>

#include "common/errno.h"
#include "common/ceph_json.h"
#include "include/types.h"
#include "include/str_list.h"

#include "rgw_common.h"
#include "rgw_keystone.h"
#include "common/ceph_crypto_cms.h"
#include "common/armor.h"
#include "common/Cond.h"

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
  hash.Update((const byte *)token.c_str(), token.size());
  hash.Final(m);

  char calc_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  buf_to_hex(m, CEPH_CRYPTO_MD5_DIGESTSIZE, calc_md5);
  token_id = calc_md5;
}

bool rgw_decode_pki_token(CephContext * const cct,
                          const string& token,
                          bufferlist& bl)
{
  if (!rgw_is_pki_token(token)) {
    return false;
  }

  int ret = rgw_decode_b64_cms(cct, token, bl);
  if (ret < 0) {
    return false;
  }

  ldout(cct, 20) << "successfully decoded pki token" << dendl;

  return true;
}


KeystoneApiVersion KeystoneService::get_api_version()
{
  const int keystone_version = g_ceph_context->_conf->rgw_keystone_api_version;

  if (keystone_version == 3) {
    return KeystoneApiVersion::VER_3;
  } else if (keystone_version == 2) {
    return KeystoneApiVersion::VER_2;
  } else {
    dout(0) << "ERROR: wrong Keystone API version: " << keystone_version
            << "; falling back to v2" <<  dendl;
    return KeystoneApiVersion::VER_2;
  }
}

int KeystoneService::get_keystone_url(CephContext * const cct,
                                      std::string& url)
{
  url = cct->_conf->rgw_keystone_url;
  if (url.empty()) {
    ldout(cct, 0) << "ERROR: keystone url is not configured" << dendl;
    return -EINVAL;
  }

  if (url[url.size() - 1] != '/') {
    url.append("/");
  }

  return 0;
}

int KeystoneService::get_keystone_admin_token(CephContext * const cct,
                                              std::string& token)
{
  std::string token_url;

  if (get_keystone_url(cct, token_url) < 0) {
    return -EINVAL;
  }

  if (!cct->_conf->rgw_keystone_admin_token.empty()) {
    token = cct->_conf->rgw_keystone_admin_token;
    return 0;
  }

  KeystoneToken t;

  /* Try cache first. */
  if (RGWKeystoneTokenCache::get_instance().find_admin(t)) {
    ldout(cct, 20) << "found cached admin token" << dendl;
    token = t.token.id;
    return 0;
  }

  bufferlist token_bl;
  RGWGetKeystoneAdminToken token_req(cct, &token_bl);
  token_req.append_header("Content-Type", "application/json");
  JSONFormatter jf;

  const auto keystone_version = KeystoneService::get_api_version();
  if (keystone_version == KeystoneApiVersion::VER_2) {
    KeystoneAdminTokenRequestVer2 req_serializer(cct);
    req_serializer.dump(&jf);

    std::stringstream ss;
    jf.flush(ss);
    token_req.set_post_data(ss.str());
    token_req.set_send_length(ss.str().length());
    token_url.append("v2.0/tokens");

  } else if (keystone_version == KeystoneApiVersion::VER_3) {
    KeystoneAdminTokenRequestVer3 req_serializer(cct);
    req_serializer.dump(&jf);

    std::stringstream ss;
    jf.flush(ss);
    token_req.set_post_data(ss.str());
    token_req.set_send_length(ss.str().length());
    token_url.append("v3/auth/tokens");
  } else {
    return -ENOTSUP;
  }

  const int ret = token_req.process("POST", token_url.c_str());
  if (ret < 0) {
    return ret;
  }
  if (t.parse(cct, token_req.get_subject_token(), token_bl) != 0) {
    return -EINVAL;
  }

  RGWKeystoneTokenCache::get_instance().add_admin(t);
  token = t.token.id;
  return 0;
}

bool KeystoneToken::has_role(const string& r) const
{
  list<Role>::const_iterator iter;
  for (iter = roles.cbegin(); iter != roles.cend(); ++iter) {
      if (fnmatch(r.c_str(), ((*iter).name.c_str()), 0) == 0) {
        return true;
      }
  }
  return false;
}

int KeystoneToken::parse(CephContext * const cct,
                         const string& token_str,
                         bufferlist& bl)
{
  JSONParser parser;
  if (!parser.parse(bl.c_str(), bl.length())) {
    ldout(cct, 0) << "Keystone token parse error: malformed json" << dendl;
    return -EINVAL;
  }

  try {
    const auto version = KeystoneService::get_api_version();

    if (version == KeystoneApiVersion::VER_2) {
      if (!JSONDecoder::decode_json("access", *this, &parser)) {
        /* Token structure doesn't follow Identity API v2, so the token
         * must be in v3. Otherwise we can assume it's wrongly formatted. */
        JSONDecoder::decode_json("token", *this, &parser, true);
        token.id = token_str;
      }
    } else if (version == KeystoneApiVersion::VER_3) {
      if (!JSONDecoder::decode_json("token", *this, &parser)) {
        /* If the token cannot be parsed according to V3, try V2. */
        JSONDecoder::decode_json("access", *this, &parser, true);
      } else {
        /* v3 suceeded. We have to fill token.id from external input as it
         * isn't a part of the JSON response anymore. It has been moved
         * to X-Subject-Token HTTP header instead. */
        token.id = token_str;
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

RGWKeystoneTokenCache& RGWKeystoneTokenCache::get_instance()
{
  /* In C++11 this is thread safe. */
  static RGWKeystoneTokenCache instance;
  return instance;
}

bool RGWKeystoneTokenCache::find(const string& token_id, KeystoneToken& token)
{
  lock.Lock();
  map<string, token_entry>::iterator iter = tokens.find(token_id);
  if (iter == tokens.end()) {
    lock.Unlock();
    if (perfcounter) perfcounter->inc(l_rgw_keystone_token_cache_miss);
    return false;
  }

  token_entry& entry = iter->second;
  tokens_lru.erase(entry.lru_iter);

  if (entry.token.expired()) {
    tokens.erase(iter);
    lock.Unlock();
    if (perfcounter) perfcounter->inc(l_rgw_keystone_token_cache_hit);
    return false;
  }
  token = entry.token;

  tokens_lru.push_front(token_id);
  entry.lru_iter = tokens_lru.begin();

  lock.Unlock();
  if (perfcounter) perfcounter->inc(l_rgw_keystone_token_cache_hit);

  return true;
}

bool RGWKeystoneTokenCache::find_admin(KeystoneToken& token)
{
  Mutex::Locker l(lock);

  return find(admin_token_id, token);
}

void RGWKeystoneTokenCache::add(const string& token_id,
                                const KeystoneToken& token)
{
  lock.Lock();
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
    assert(iter != tokens.end());
    tokens.erase(iter);
    tokens_lru.pop_back();
  }

  lock.Unlock();
}

void RGWKeystoneTokenCache::add_admin(const KeystoneToken& token)
{
  Mutex::Locker l(lock);

  rgw_get_token_id(token.token.id, admin_token_id);
  add(admin_token_id, token);
}

void RGWKeystoneTokenCache::invalidate(const string& token_id)
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

int RGWKeystoneTokenCache::RevokeThread::check_revoked()
{
  std::string url;
  std::string token;

  bufferlist bl;
  RGWGetRevokedTokens req(cct, &bl);

  if (KeystoneService::get_keystone_admin_token(cct, token) < 0) {
    return -EINVAL;
  }
  if (KeystoneService::get_keystone_url(cct, url) < 0) {
    return -EINVAL;
  }
  req.append_header("X-Auth-Token", token);

  const auto keystone_version = KeystoneService::get_api_version();
  if (keystone_version == KeystoneApiVersion::VER_2) {
    url.append("v2.0/tokens/revoked");
  } else if (keystone_version == KeystoneApiVersion::VER_3) {
    url.append("v3/auth/tokens/OS-PKI/revoked");
  }

  req.set_send_length(0);
  int ret = req.process(url.c_str());
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

bool RGWKeystoneTokenCache::going_down() const
{
  return (down_flag.read() != 0);
}

void * RGWKeystoneTokenCache::RevokeThread::entry()
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
    cond.WaitInterval(cct, lock,
                      utime_t(cct->_conf->rgw_keystone_revocation_interval, 0));
    lock.Unlock();
  } while (!cache->going_down());

  return nullptr;
}

void RGWKeystoneTokenCache::RevokeThread::stop()
{
  Mutex::Locker l(lock);
  cond.Signal();
}
