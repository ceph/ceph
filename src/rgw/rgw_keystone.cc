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

#define dout_subsys ceph_subsys_rgw

int rgw_open_cms_envelope(CephContext * const cct, string& src, string& dst)
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

bool KeystoneToken::has_role(const string& r)
{
  list<Role>::iterator iter;
  for (iter = roles.begin(); iter != roles.end(); ++iter) {
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
