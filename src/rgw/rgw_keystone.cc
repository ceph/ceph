// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stdio.h>
#include <errno.h>
#include <fnmatch.h>

#include "common/errno.h"
#include "common/ceph_json.h"
#include "common/ceph_crypto_cms.h"

#include "include/types.h"
#include "include/str_list.h"

#include "rgw_common.h"
#include "rgw_http_client.h"
#include "rgw_keystone.h"
#include "rgw_cms.h"

#define dout_subsys ceph_subsys_rgw

static list<string> roles_list;

bool KeystoneToken::has_role(const string &r) {
  list<Role>::iterator iter;
  for (iter = roles.begin(); iter != roles.end(); ++iter) {
      if (fnmatch(r.c_str(), ((*iter).name.c_str()), 0) == 0) {
        return true;
      }
  }
  return false;
}

int KeystoneToken::parse(CephContext *cct, bufferlist& bl)
{
  JSONParser parser;
  if (!parser.parse(bl.c_str(), bl.length())) {
    ldout(cct, 0) << "Keystone token parse error: malformed json" << dendl;
    return -EINVAL;
  }

  try {
    if (version == "2.0") {
      JSONDecoder::decode_json("access", *this, &parser);
    }
    if (version == "3") {
      JSONDecoder::decode_json("token", *this, &parser);
    }
  } catch (JSONDecoder::err& err) {
    ldout(cct, 0) << "Keystone token parse error: " << err.message << dendl;
    return -EINVAL;
  }
  return 0;
}

bool Keystone::RGWKeystoneTokenCache::find(const string& token_id, KeystoneToken& token)
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
    if (perfcounter) perfcounter->inc(l_rgw_keystone_token_cache_miss);
    return false;
  }
  token = entry.token;

  tokens_lru.push_front(token_id);
  entry.lru_iter = tokens_lru.begin();

  lock.Unlock();
  if (perfcounter) perfcounter->inc(l_rgw_keystone_token_cache_hit);

  return true;
}

void Keystone::RGWKeystoneTokenCache::add(const string& token_id, KeystoneToken& token)
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

void Keystone::RGWKeystoneTokenCache::invalidate(const string& token_id)
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

int RGWPostHTTPData::receive_header(void *ptr, size_t len)
{
  char line[len + 1];

  char *s = (char *)ptr, *end = (char *)ptr + len;
  char *p = line;
  ldout(cct, 10) << "read_http_header" << dendl;

  while (s != end) {
    if (*s == '\r') {
      s++;
      continue;
    }
    if (*s == '\n') {
      *p = '\0';
      ldout(cct, 10) << "os_auth:" << line << dendl;
      // TODO: fill whatever data required here
      char *l = line;
      char *tok = strsep(&l, " \t:");
      if (tok) {
        while (l && *l == ' ')
          l++;

        if (strcasecmp(tok, "X-Subject-Token") == 0) {
          subject_token = l;
        }
      }
    }
    if (s != end)
      *p++ = *s++;
  }
  return 0;
}

int Keystone::get_keystone_url(std::string &url) {
  bufferlist bl;

  url = cct->_conf->rgw_keystone_url;
  if (url.empty()) {
    ldout(cct, 0) << "ERROR: keystone url is not configured" << dendl;
    return -EINVAL;
  }
  if (url[url.size() - 1] != '/')
    url.append("/");
  return 0;
}

int Keystone::get_keystone_admin_token(std::string &token) {
  std::string token_url;

  if (get_keystone_url(token_url) < 0)
    return -EINVAL;
  if (!cct->_conf->rgw_keystone_admin_token.empty()) {
    token = cct->_conf->rgw_keystone_admin_token;
    return 0;
  }
  bufferlist token_bl;
  RGWGetKeystoneAdminToken token_req(cct, &token_bl);
  token_req.append_header("Content-Type", "application/json");
  JSONFormatter jf;
  std::string keystone_version = cct->_conf->rgw_keystone_api_version;
  if (keystone_version == "2.0") {
    jf.open_object_section("token_request");
      jf.open_object_section("auth");
        jf.open_object_section("passwordCredentials");
          encode_json("username", cct->_conf->rgw_keystone_admin_user, &jf);
          encode_json("password", cct->_conf->rgw_keystone_admin_password, &jf);
        jf.close_section();
        encode_json("tenantName", cct->_conf->rgw_keystone_admin_tenant, &jf);
      jf.close_section();
    jf.close_section();
    std::stringstream ss;
    jf.flush(ss);
    token_req.set_post_data(ss.str());
    token_req.set_send_length(ss.str().length());
    token_url.append("v2.0/tokens");
    int ret = token_req.process("POST", token_url.c_str());
    if (ret < 0)
      return ret;
    KeystoneToken t = KeystoneToken(keystone_version);
    if (t.parse(cct, token_bl) != 0)
      return -EINVAL;
    token = t.token.id;
    return 0;
  }
  else if (keystone_version == "3") {
    jf.open_object_section("auth");
      jf.open_object_section("identity");
        jf.open_array_section("methods");
          jf.dump_string("", "password");
        jf.close_section();
        jf.open_object_section("password");
          jf.open_object_section("user");
            jf.open_object_section("domain");
              encode_json("name", cct->_conf->rgw_keystone_admin_domain, &jf);
            jf.close_section();
            encode_json("name", cct->_conf->rgw_keystone_admin_user, &jf);
            encode_json("password", cct->_conf->rgw_keystone_admin_password, &jf);
          jf.close_section();
        jf.close_section();
      jf.close_section();
      jf.open_object_section("scope");
        jf.open_object_section("project");
          if (!cct->_conf->rgw_keystone_admin_project.empty()) {
            encode_json("name", cct->_conf->rgw_keystone_admin_project, &jf);
          }
          else {
            encode_json("name", cct->_conf->rgw_keystone_admin_tenant, &jf);
          }
          jf.open_object_section("domain");
            encode_json("name", cct->_conf->rgw_keystone_admin_domain, &jf);
          jf.close_section();
        jf.close_section();
      jf.close_section();
    jf.close_section();
    std::stringstream ss;
    jf.flush(ss);
    token_req.set_post_data(ss.str());
    token_req.set_send_length(ss.str().length());
    token_url.append("v3/auth/tokens");
    int ret = token_req.process("POST", token_url.c_str());
    if (ret < 0)
      return ret;
    token = token_req.get_subject_token();
    return 0;
  }
  return -EINVAL;
}

int Keystone::parse_response(const string &token, bufferlist bl, KeystoneToken &t)
{
  int ret = t.parse(cct, bl);
  if (ret < 0)
    return ret;

  bool found = false;
  list<string>::iterator iter;
  for (iter = roles_list.begin(); iter != roles_list.end(); ++iter) {
    const string& role = *iter;
    if ((found=t.has_role(role))==true)
      break;
  }

  if (!found) {
    ldout(cct, 0) << "user does not hold a matching role; required roles: " << g_conf->rgw_keystone_accepted_roles << dendl;
    return -EPERM;
  }
  ldout(cct, 0) << "validated token: " << t.get_project_name() << ":" << t.get_user_name() << " expires: " << t.get_expires() << dendl;
  return 0;
}

static void get_token_id(const string& token, string& token_id)
{
  if (!is_pki_token(token)) {
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

static bool decode_pki_token(CephContext *cct, const string& token, bufferlist& bl)
{
  if (!is_pki_token(token))
    return false;

  int ret = decode_b64_cms(cct, token, bl);
  if (ret < 0)
    return false;

  ldout(cct, 20) << "successfully decoded pki token" << dendl;

  return true;
}

int Keystone::validate_token(const string& token, KeystoneToken& t)
{
  string token_id;
  get_token_id(token, token_id);

  /* check cache first */
  if (cache->find(token_id, t)) {
    ldout(cct, 20) << "cached token.project.id=" << t.get_project_id() << dendl;
    return 0;
  }

  std::string url;
  if (get_keystone_url(url) < 0)
    return -EINVAL;

  std::string admin_token;
  if (get_keystone_admin_token(admin_token) < 0)
    return -EINVAL;

  bufferlist bl;
  int ret;
  /* check if that's a self signed token that we can decode */
  if (!decode_pki_token(cct, token, bl)) {

    /* can't decode, just go to the keystone server for validation */
    RGWValidateKeystoneToken validate(cct, &bl);
    validate.append_header("X-Auth-Token", admin_token);

    std::string keystone_version = cct->_conf->rgw_keystone_api_version;
    if (keystone_version == "2.0") {
      url.append("v2.0/tokens/");
      url.append(token);
    }
    if (keystone_version == "3") {
      url.append("v3/auth/tokens");
      validate.append_header("X-Subject-Token", token);
    }
    validate.set_send_length(0);

    ret = validate.process(url.c_str());
    if (ret < 0)
      return ret;
  }

  bl.append((char)0); // NULL terminate for debug output

  ldout(cct, 20) << "received response: " << bl.c_str() << dendl;

  ret = parse_response(token, bl, t);
  if (ret < 0)
    return ret;

  if (t.expired()) {
    ldout(cct, 0) << "got expired token: " << t.get_project_name() << ":" << t.get_user_name() << " expired: " << t.get_expires() << dendl;
    return -EPERM;
  }

  cache->add(token_id, t);
  return 0;
}


int Keystone::check_revoked() {
  string url;
  string token;

  bufferlist bl;
  RGWGetRevokedTokens req(cct, &bl);

  if (get_keystone_admin_token(token) < 0)
    return -EINVAL;
  if (get_keystone_url(url) < 0)
    return -EINVAL;
  req.append_header("X-Auth-Token", token);
  std::string keystone_version = cct->_conf->rgw_keystone_api_version;
  if (keystone_version == "2.0") {
    url.append("v2.0/tokens/revoked");
  }
  if (keystone_version == "3") {
    url.append("v3/auth/tokens/OS-PKI/revoked");
  }
  req.set_send_length(0);
  int ret = req.process(url.c_str());
  if (ret < 0)
    return ret;

  bl.append((char) 0); // NULL terminate for debug output

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

  string signed_str = signed_obj->get_data();

  ldout(cct, 10) << "signed=" << signed_str << dendl;

  string signed_b64;
  ret = open_cms_envelope(cct, signed_str, signed_b64);
  if (ret < 0)
    return ret;

  ldout(cct, 10) << "content=" << signed_b64 << dendl;

  bufferlist json;
  ret = decode_b64_cms(cct, signed_b64, json);
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

    string token_id = token->get_data();
    cache->invalidate(token_id);
  }

  return 0;
}

void Keystone::init() {
  if (enabled()) {
    get_str_list(cct->_conf->rgw_keystone_accepted_roles, roles_list);
    cache = new RGWKeystoneTokenCache(cct, cct->_conf->rgw_keystone_token_cache_size);

    keystone_revoke_thread = new KeystoneRevokeThread(cct, this);
    keystone_revoke_thread->create();
  }
}

void Keystone::stop() {
  if (enabled()) {
    delete cache;
    cache = NULL;

    stopping_flag.inc();
    if (keystone_revoke_thread) {
      keystone_revoke_thread->stop();
      keystone_revoke_thread->join();
    }
    delete keystone_revoke_thread;
    keystone_revoke_thread = NULL;
  }
}

void Keystone::finalize() {
  stop();
}

bool Keystone::stopping() {
  return (stopping_flag.read() != 0);
}

void *Keystone::KeystoneRevokeThread::entry() {
  do {
    dout(2) << "keystone revoke thread: start" << dendl;
    int r = keystone->check_revoked();
    if (r < 0) {
      dout(0) << "ERROR: keystone revocation processing returned error r=" << r << dendl;
    }

    if (keystone->stopping())
      break;

    lock.Lock();
    cond.WaitInterval(cct, lock, utime_t(cct->_conf->rgw_keystone_revocation_interval, 0));
    lock.Unlock();
  } while (!keystone->stopping());

  return NULL;
}

void Keystone::KeystoneRevokeThread::stop() {
  Mutex::Locker l(lock);
  cond.Signal();
}
