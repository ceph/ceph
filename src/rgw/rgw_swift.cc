// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "common/ceph_json.h"
#include "rgw_common.h"
#include "rgw_swift.h"
#include "rgw_swift_auth.h"
#include "rgw_user.h"
#include "rgw_http_client.h"
#include "rgw_keystone.h"

#include "include/str_list.h"

#include "common/ceph_crypto_cms.h"
#include "common/armor.h"

#define dout_subsys ceph_subsys_rgw

static list<string> roles_list;

class RGWValidateSwiftToken : public RGWHTTPClient {
  struct rgw_swift_auth_info *info;

protected:
  RGWValidateSwiftToken() : RGWHTTPClient(NULL), info(NULL) {}
public:
  RGWValidateSwiftToken(CephContext *_cct, struct rgw_swift_auth_info *_info) : RGWHTTPClient(_cct), info(_info) {}

  int receive_header(void *ptr, size_t len);
  int receive_data(void *ptr, size_t len) {
    return 0;
  }
  int send_data(void *ptr, size_t len) {
    return 0;
  }

  friend class RGWKeystoneTokenCache;
};

int RGWValidateSwiftToken::receive_header(void *ptr, size_t len)
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
 
        if (strcmp(tok, "HTTP") == 0) {
          info->status = atoi(l);
        } else if (strcasecmp(tok, "X-Auth-Groups") == 0) {
          info->auth_groups = l;
          char *s = strchr(l, ',');
          if (s) {
            *s = '\0';
            info->user = l;
          }
        } else if (strcasecmp(tok, "X-Auth-Ttl") == 0) {
          info->ttl = atoll(l);
        }
      }
    }
    if (s != end)
      *p++ = *s++;
  }
  return 0;
}

int RGWSwift::validate_token(const char *token, struct rgw_swift_auth_info *info)
{
  if (g_conf->rgw_swift_auth_url.empty())
    return -EINVAL;

  string auth_url = g_conf->rgw_swift_auth_url;
  if (auth_url[auth_url.size() - 1] != '/')
    auth_url.append("/");
  auth_url.append("token");
  char url_buf[auth_url.size() + 1 + strlen(token) + 1];
  sprintf(url_buf, "%s/%s", auth_url.c_str(), token);

  RGWValidateSwiftToken validate(cct, info);

  ldout(cct, 10) << "rgw_swift_validate_token url=" << url_buf << dendl;

  int ret = validate.process(url_buf);
  if (ret < 0)
    return ret;

  return 0;
}

class RGWPostHTTPData : public RGWHTTPClient {
  bufferlist *bl;
  std::string post_data;
  size_t post_data_index;
public:
  RGWPostHTTPData(CephContext *_cct, bufferlist *_bl) : RGWHTTPClient(_cct), bl(_bl), post_data_index(0) {}

  void set_post_data(const std::string& _post_data) {
    this->post_data = _post_data;
  }

  int send_data(void* ptr, size_t len) {
    int length_to_copy = 0;
    if (post_data_index < post_data.length()) {
      length_to_copy = min(post_data.length() - post_data_index, len);
      memcpy(ptr, post_data.data() + post_data_index, length_to_copy);
      post_data_index += length_to_copy;
    }
    return length_to_copy;
  }

  int receive_data(void *ptr, size_t len) {
    bl->append((char *)ptr, len);
    return 0;
  }

  int receive_header(void *ptr, size_t len) {
    return 0;
  }
};

typedef RGWPostHTTPData RGWGetKeystoneAdminToken;
typedef RGWPostHTTPData RGWGetRevokedTokens;

class RGWValidateKeystoneToken : public RGWHTTPClient {
  bufferlist *bl;
public:
  RGWValidateKeystoneToken(CephContext *_cct, bufferlist *_bl) : RGWHTTPClient(_cct), bl(_bl) {}

  int receive_data(void *ptr, size_t len) {
    bl->append((char *)ptr, len);
    return 0;
  }
  int receive_header(void *ptr, size_t len) {
    return 0;
  }
  int send_data(void *ptr, size_t len) {
    return 0;
  }

};

static RGWKeystoneTokenCache *keystone_token_cache = NULL;

static int open_cms_envelope(CephContext *cct, string& src, string& dst)
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

static int decode_b64_cms(CephContext *cct, const string& signed_b64, bufferlist& bl)
{
  bufferptr signed_ber(signed_b64.size() * 2);
  char *dest = signed_ber.c_str();
  const char *src = signed_b64.c_str();
  size_t len = signed_b64.size();
  char buf[len + 1];
  buf[len] = '\0';
  for (size_t i = 0; i < len; i++, src++) {
    if (*src != '-')
      buf[i] = *src;
    else
      buf[i] = '/';
  }
  int ret = ceph_unarmor(dest, dest + signed_ber.length(), buf, buf + signed_b64.size());
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

int	RGWSwift::get_keystone_url(std::string& url)
{
  bufferlist bl;
  RGWGetRevokedTokens req(cct, &bl);

  url = cct->_conf->rgw_keystone_url;
  if (url.empty()) {
    ldout(cct, 0) << "ERROR: keystone url is not configured" << dendl;
    return -EINVAL;
  }
  if (url[url.size() - 1] != '/')
    url.append("/");
  return 0;
}

int	RGWSwift::get_keystone_admin_token(std::string& token)
{
  std::string token_url;

  if (get_keystone_url(token_url) < 0)
    return -EINVAL;
  if (cct->_conf->rgw_keystone_admin_token.empty()) {
    token_url.append("v2.0/tokens");
    KeystoneToken t;
    bufferlist token_bl;
    RGWGetKeystoneAdminToken token_req(cct, &token_bl);
    token_req.append_header("Content-Type", "application/json");
    JSONFormatter jf;
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
    int ret = token_req.process("POST", token_url.c_str());
    if (ret < 0)
      return ret;
    if (t.parse(cct, token_bl) != 0)
      return -EINVAL;
    token = t.token.id;
  } else {
    token = cct->_conf->rgw_keystone_admin_token;
  }
  return 0; 
}


int RGWSwift::check_revoked()
{
  string url;
  string token;

  bufferlist bl;
  RGWGetRevokedTokens req(cct, &bl);

  if (get_keystone_admin_token(token) < 0)
    return -EINVAL;
  if (get_keystone_url(url) < 0)
    return -EINVAL;
  url.append("v2.0/tokens/revoked");
  req.append_header("X-Auth-Token", token);
  int ret = req.process(url.c_str());
  if (ret < 0)
    return ret;

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
    keystone_token_cache->invalidate(token_id);
  }
  
  return 0;
}

static void rgw_set_keystone_token_auth_info(KeystoneToken& token, struct rgw_swift_auth_info *info)
{
  info->user = token.token.tenant.id;
  info->display_name = token.token.tenant.name;
  info->status = 200;
}

int RGWSwift::parse_keystone_token_response(const string& token, bufferlist& bl, struct rgw_swift_auth_info *info, KeystoneToken& t)
{
  int ret = t.parse(cct, bl);
  if (ret < 0)
    return ret;

  bool found = false;
  list<string>::iterator iter;
  for (iter = roles_list.begin(); iter != roles_list.end(); ++iter) {
    const string& role = *iter;
    if ((found=t.user.has_role(role))==true)
      break;
  }

  if (!found) {
    ldout(cct, 0) << "user does not hold a matching role; required roles: " << g_conf->rgw_keystone_accepted_roles << dendl;
    return -EPERM;
  }

  ldout(cct, 0) << "validated token: " << t.token.tenant.name << ":" << t.user.name << " expires: " << t.token.expires << dendl;

  rgw_set_keystone_token_auth_info(t, info);

  return 0;
}

int RGWSwift::update_user_info(RGWRados *store, struct rgw_swift_auth_info *info, RGWUserInfo& user_info)
{
  if (rgw_get_user_info_by_uid(store, info->user, user_info) < 0) {
    ldout(cct, 0) << "NOTICE: couldn't map swift user" << dendl;
    user_info.user_id = info->user;
    user_info.display_name = info->display_name;

    int ret = rgw_store_user_info(store, user_info, NULL, NULL, 0, true);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: failed to store new user's info: ret=" << ret << dendl;
      return ret;
    }
  }
  return 0;
}

#define PKI_ANS1_PREFIX "MII"

static bool is_pki_token(const string& token)
{
  return token.compare(0, sizeof(PKI_ANS1_PREFIX) - 1, PKI_ANS1_PREFIX) == 0;
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

int RGWSwift::validate_keystone_token(RGWRados *store, const string& token, struct rgw_swift_auth_info *info,
				      RGWUserInfo& rgw_user)
{
  KeystoneToken t;

  string token_id;
  get_token_id(token, token_id);

  ldout(cct, 20) << "token_id=" << token_id << dendl;

  /* check cache first */
  if (keystone_token_cache->find(token_id, t)) {
    rgw_set_keystone_token_auth_info(t, info);

    ldout(cct, 20) << "cached token.tenant.id=" << t.token.tenant.id << dendl;

    int ret = update_user_info(store, info, rgw_user);
    if (ret < 0)
      return ret;

    return 0;
  }

  bufferlist bl;

  /* check if that's a self signed token that we can decode */
  if (!decode_pki_token(cct, token, bl)) {

    /* can't decode, just go to the keystone server for validation */

    RGWValidateKeystoneToken validate(cct, &bl);

    string url = g_conf->rgw_keystone_url;
    if (url.empty()) {
      ldout(cct, 0) << "ERROR: keystone url is not configured" << dendl;
      return -EINVAL;
    }
    if (url[url.size() - 1] != '/')
      url.append("/");
    std::string admin_token;
    if (get_keystone_admin_token(admin_token) < 0)
      return -EINVAL;
    if (get_keystone_url(url) < 0)
      return -EINVAL;

    url.append("v2.0/tokens/");
    url.append(token);

    validate.append_header("X-Auth-Token", admin_token);

    validate.set_send_length(0);

    int ret = validate.process(url.c_str());
    if (ret < 0)
      return ret;
  }

  bl.append((char)0); // NULL terminate for debug output

  ldout(cct, 20) << "received response: " << bl.c_str() << dendl;

  int ret = parse_keystone_token_response(token, bl, info, t);
  if (ret < 0)
    return ret;

  ret = update_user_info(store, info, rgw_user);
  if (ret < 0)
    return ret;

  return 0;
}

int authenticate_temp_url(RGWRados *store, req_state *s)
{
  /* temp url requires bucket and object specified in the requets */
  if (s->bucket_name_str.empty())
    return -EPERM;

  if (s->object.empty())
    return -EPERM;

  string temp_url_sig = s->info.args.get("temp_url_sig");
  if (temp_url_sig.empty())
    return -EPERM;

  string temp_url_expires = s->info.args.get("temp_url_expires");
  if (temp_url_expires.empty())
    return -EPERM;

  /* need to get user info of bucket owner */
  RGWBucketInfo bucket_info;

  int ret = store->get_bucket_info(*static_cast<RGWObjectCtx *>(s->obj_ctx), s->bucket_name_str, bucket_info, NULL);
  if (ret < 0)
    return -EPERM;

  dout(20) << "temp url user (bucket owner): " << bucket_info.owner << dendl;
  if (rgw_get_user_info_by_uid(store, bucket_info.owner, s->user) < 0) {
    return -EPERM;
  }

  if (s->user.temp_url_keys.empty()) {
    dout(5) << "user does not have temp url key set, aborting" << dendl;
    return -EPERM;
  }

  if (!s->info.method)
    return -EPERM;

  utime_t now = ceph_clock_now(g_ceph_context);

  string err;
  uint64_t expiration = (uint64_t)strict_strtoll(temp_url_expires.c_str(), 10, &err);
  if (!err.empty()) {
    dout(5) << "failed to parse temp_url_expires: " << err << dendl;
    return -EPERM;
  }
  if (expiration <= (uint64_t)now.sec()) {
    dout(5) << "temp url expired: " << expiration << " <= " << now.sec() << dendl;
    return -EPERM;
  }

  /* strip the swift prefix from the uri */
  int pos = g_conf->rgw_swift_url_prefix.find_last_not_of('/') + 1;
  string object_path = s->info.request_uri.substr(pos + 1);
  string str = string(s->info.method) + "\n" + temp_url_expires + "\n" + object_path;

  dout(20) << "temp url signature (plain text): " << str << dendl;

  map<int, string>::iterator iter;
  for (iter = s->user.temp_url_keys.begin(); iter != s->user.temp_url_keys.end(); ++iter) {
    string& temp_url_key = iter->second;

    if (temp_url_key.empty())
      continue;

    char dest[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE];
    calc_hmac_sha1(temp_url_key.c_str(), temp_url_key.size(),
                   str.c_str(), str.size(), dest);

    char dest_str[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE * 2 + 1];
    buf_to_hex((const unsigned char *)dest, sizeof(dest), dest_str);
    dout(20) << "temp url signature [" << iter->first << "] (calculated): " << dest_str << dendl;

    if (dest_str != temp_url_sig) {
      dout(5) << "temp url signature mismatch: " << dest_str << " != " << temp_url_sig << dendl;
    } else {
      return 0;
    }
  }

  return -EPERM;
}

bool RGWSwift::verify_swift_token(RGWRados *store, req_state *s)
{
  if (!do_verify_swift_token(store, s)) {
    return false;
  }

  if (!s->swift_user.empty()) {
    string subuser;
    ssize_t pos = s->swift_user.find(':');
    if (pos < 0) {
      subuser = s->swift_user;
    } else {
      subuser = s->swift_user.substr(pos + 1);
    }
    s->perm_mask = 0;
    map<string, RGWSubUser>::iterator iter = s->user.subusers.find(subuser);
    if (iter != s->user.subusers.end()) {
      RGWSubUser& subuser = iter->second;
      s->perm_mask = subuser.perm_mask;
    }
  } else {
    s->perm_mask = RGW_PERM_FULL_CONTROL;
  }

  return true;

}

bool RGWSwift::do_verify_swift_token(RGWRados *store, req_state *s)
{
  if (!s->os_auth_token) {
    int ret = authenticate_temp_url(store, s);
    return (ret >= 0);
  }

  if (strncmp(s->os_auth_token, "AUTH_rgwtk", 10) == 0) {
    int ret = rgw_swift_verify_signed_token(s->cct, store, s->os_auth_token, s->user, &s->swift_user);
    if (ret < 0)
      return false;

    return  true;
  }

  struct rgw_swift_auth_info info;

  info.status = 401; // start with access denied, validate_token might change that

  int ret;

  if (supports_keystone()) {
    ret = validate_keystone_token(store, s->os_auth_token, &info, s->user);
    return (ret >= 0);
  }

  ret = validate_token(s->os_auth_token, &info);
  if (ret < 0)
    return false;

  if (info.user.empty()) {
    ldout(cct, 5) << "swift auth didn't authorize a user" << dendl;
    return false;
  }

  s->swift_user = info.user;
  s->swift_groups = info.auth_groups;

  string swift_user = s->swift_user;

  ldout(cct, 10) << "swift user=" << s->swift_user << dendl;

  if (rgw_get_user_info_by_swift(store, swift_user, s->user) < 0) {
    ldout(cct, 0) << "NOTICE: couldn't map swift user" << dendl;
    return false;
  }

  ldout(cct, 10) << "user_id=" << s->user.user_id << dendl;

  return true;
}

void RGWSwift::init()
{
  get_str_list(cct->_conf->rgw_keystone_accepted_roles, roles_list);
  if (supports_keystone())
      init_keystone();
}


void RGWSwift::init_keystone()
{
  keystone_token_cache = new RGWKeystoneTokenCache(cct, cct->_conf->rgw_keystone_token_cache_size);

  keystone_revoke_thread = new KeystoneRevokeThread(cct, this);
  keystone_revoke_thread->create();
}


void RGWSwift::finalize()
{
  if (supports_keystone())
    finalize_keystone();
}

void RGWSwift::finalize_keystone()
{
  delete keystone_token_cache;
  keystone_token_cache = NULL;

  down_flag.set(1);
  if (keystone_revoke_thread) {
    keystone_revoke_thread->stop();
    keystone_revoke_thread->join();
  }
  delete keystone_revoke_thread;
  keystone_revoke_thread = NULL;
}

RGWSwift *rgw_swift = NULL;

void swift_init(CephContext *cct)
{
  rgw_swift = new RGWSwift(cct);
}

void swift_finalize()
{
  delete rgw_swift;
}

bool RGWSwift::going_down()
{
  return (down_flag.read() != 0);
}

void *RGWSwift::KeystoneRevokeThread::entry() {
  do {
    dout(2) << "keystone revoke thread: start" << dendl;
    int r = swift->check_revoked();
    if (r < 0) {
      dout(0) << "ERROR: keystone revocation processing returned error r=" << r << dendl;
    }

    if (swift->going_down())
      break;

    lock.Lock();
    cond.WaitInterval(cct, lock, utime_t(cct->_conf->rgw_keystone_revocation_interval, 0));
    lock.Unlock();
  } while (!swift->going_down());

  return NULL;
}

void RGWSwift::KeystoneRevokeThread::stop()
{
  Mutex::Locker l(lock);
  cond.Signal();
}

