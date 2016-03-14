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
  std::string subject_token;
public:
  RGWPostHTTPData(CephContext *_cct, bufferlist *_bl) : RGWHTTPClient(_cct), bl(_bl), post_data_index(0) {}
  RGWPostHTTPData(CephContext *_cct, bufferlist *_bl, bool verify_ssl) : RGWHTTPClient(_cct), bl(_bl), post_data_index(0){
    set_verify_ssl(verify_ssl);
  }

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
    char line[len + 1];

    char *s = (char *)ptr, *end = (char *)ptr + len;
    char *p = line;
    ldout(cct, 20) << "RGWPostHTTPData::receive_header parsing HTTP headers" << dendl;

    while (s != end) {
      if (*s == '\r') {
        s++;
        continue;
      }
      if (*s == '\n') {
        *p = '\0';
        ldout(cct, 20) << "RGWPostHTTPData::receive_header: line="
                       << line << dendl;
        // TODO: fill whatever data required here
        char *l = line;
        char *tok = strsep(&l, " \t:");
        if (tok) {
          while (l && *l == ' ') {
            l++;
          }

          if (strcasecmp(tok, "X-Subject-Token") == 0) {
            subject_token = l;
          }
        }
      }
      if (s != end) {
        *p++ = *s++;
      }
    }
    return 0;
  }

  std::string get_subject_token() {
    return subject_token;
  }
};

typedef RGWPostHTTPData RGWValidateKeystoneToken;
typedef RGWPostHTTPData RGWGetKeystoneAdminToken;
typedef RGWPostHTTPData RGWGetRevokedTokens;

static RGWKeystoneTokenCache *keystone_token_cache = NULL;

int RGWSwift::get_keystone_url(CephContext * const cct,
                               std::string& url)
{
  bufferlist bl;
  RGWGetRevokedTokens req(cct, &bl, cct->_conf->rgw_keystone_verify_ssl);

  url = cct->_conf->rgw_keystone_url;
  if (url.empty()) {
    ldout(cct, 0) << "ERROR: keystone url is not configured" << dendl;
    return -EINVAL;
  }
  if (url[url.size() - 1] != '/')
    url.append("/");
  return 0;
}

int RGWSwift::get_keystone_url(std::string& url)
{
  return RGWSwift::get_keystone_url(cct, url);
}

int RGWSwift::get_keystone_admin_token(std::string& token)
{
  return RGWSwift::get_keystone_admin_token(cct, token);
}

int RGWSwift::get_keystone_admin_token(CephContext * const cct,
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
  if (keystone_token_cache->find_admin(t)) {
    ldout(cct, 20) << "found cached admin token" << dendl;
    token = t.token.id;
    return 0;
  }

  bufferlist token_bl;
  RGWGetKeystoneAdminToken token_req(cct, &token_bl, cct->_conf->rgw_keystone_verify_ssl);
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

  keystone_token_cache->add_admin(t);
  token = t.token.id;
  return 0;
}


int RGWSwift::check_revoked()
{
  string url;
  string token;

  bufferlist bl;
  RGWGetRevokedTokens req(cct, &bl);

  if (get_keystone_admin_token(token) < 0) {
    return -EINVAL;
  }
  if (get_keystone_url(url) < 0) {
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

  string signed_str = signed_obj->get_data();

  ldout(cct, 10) << "signed=" << signed_str << dendl;

  string signed_b64;
  ret = rgw_open_cms_envelope(cct, signed_str, signed_b64);
  if (ret < 0)
    return ret;

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

    string token_id = token->get_data();
    keystone_token_cache->invalidate(token_id);
  }
  
  return 0;
}

static void rgw_set_keystone_token_auth_info(KeystoneToken& token, struct rgw_swift_auth_info *info)
{
  info->user = token.get_project_id();
  info->display_name = token.get_project_name();
  info->status = 200;
}

int RGWSwift::parse_keystone_token_response(const string& token,
                                            bufferlist& bl,
                                            struct rgw_swift_auth_info *info,
                                            KeystoneToken& t)
{
  int ret = t.parse(cct, token, bl);
  if (ret < 0) {
    return ret;
  }

  bool found = false;
  list<string>::iterator iter;
  for (iter = roles_list.begin(); iter != roles_list.end(); ++iter) {
    const string& role = *iter;
    if ((found=t.has_role(role))==true)
      break;
  }

  if (!found) {
    ldout(cct, 0) << "user does not hold a matching role; required roles: "
                  << g_conf->rgw_keystone_accepted_roles << dendl;
    return -EPERM;
  }

  ldout(cct, 0) << "validated token: " << t.get_project_name()
                << ":" << t.get_user_name()
                << " expires: " << t.get_expires() << dendl;

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

int RGWSwift::validate_keystone_token(RGWRados *store, const string& token, struct rgw_swift_auth_info *info,
				      RGWUserInfo& rgw_user)
{
  KeystoneToken t;

  string token_id;
  rgw_get_token_id(token, token_id);

  ldout(cct, 20) << "token_id=" << token_id << dendl;

  /* check cache first */
  if (keystone_token_cache->find(token_id, t)) {
    rgw_set_keystone_token_auth_info(t, info);

    ldout(cct, 20) << "cached token.project.id=" << t.get_project_id() << dendl;

    int ret = update_user_info(store, info, rgw_user);
    if (ret < 0)
      return ret;

    return 0;
  }

  bufferlist bl;

  /* check if that's a self signed token that we can decode */
  if (!rgw_decode_pki_token(cct, token, bl)) {

    /* can't decode, just go to the keystone server for validation */

    RGWValidateKeystoneToken validate(cct, &bl, cct->_conf->rgw_keystone_verify_ssl);

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

    validate.append_header("X-Auth-Token", admin_token);

    const auto keystone_version = KeystoneService::get_api_version();
    if (keystone_version == KeystoneApiVersion::VER_2) {
      url.append("v2.0/tokens/");
      url.append(token);
    }
    if (keystone_version == KeystoneApiVersion::VER_3) {
      url.append("v3/auth/tokens");
      validate.append_header("X-Subject-Token", token);
    }

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

  if (t.expired()) {
    ldout(cct, 0) << "got expired token: " << t.get_project_name()
                  << ":" << t.get_user_name()
                  << " expired: " << t.get_expires() << dendl;
    return -EPERM;
  }

  keystone_token_cache->add(token_id, t);

  ret = update_user_info(store, info, rgw_user);
  if (ret < 0)
    return ret;

  return 0;
}

static void temp_url_make_content_disp(req_state * const s)
{
  bool inline_exists = false;
  string filename = s->info.args.get("filename");

  s->info.args.get("inline", &inline_exists);
  if (inline_exists) {
    s->content_disp.override = "inline";
  } else if (!filename.empty()) {
    string fenc;
    url_encode(filename, fenc);
    s->content_disp.override = "attachment; filename=\"" + fenc + "\"";
  } else {
    string fenc;
    url_encode(s->object.name, fenc);
    s->content_disp.fallback = "attachment; filename=\"" + fenc + "\"";
  }
}

static string temp_url_gen_sig(const string& key,
                               const string& method,
                               const string& path,
                               const string& expires)
{
  const string str = method + "\n" + expires + "\n" + path;
  //dout(20) << "temp url signature (plain text): " << str << dendl;

  /* unsigned */ char dest[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE];
  calc_hmac_sha1(key.c_str(), key.size(),
                 str.c_str(), str.size(),
                 dest);

  char dest_str[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE * 2 + 1];
  buf_to_hex((const unsigned char *)dest, sizeof(dest), dest_str);

  return dest_str;
}

int authenticate_temp_url(RGWRados * const store, req_state * const s)
{
  /* We cannot use req_state::bucket_name because it isn't available
   * now. It will be initialized in RGWHandler_REST_SWIFT::postauth_init(). */
  const string& bucket_name = s->init_state.url_bucket;

  /* temp url requires bucket and object specified in the requets */
  if (bucket_name.empty()) {
    return -EPERM;
  }

  if (s->object.empty()) {
    return -EPERM;
  }

  const string temp_url_sig = s->info.args.get("temp_url_sig");
  if (temp_url_sig.empty()) {
    return -EPERM;
  }

  const string temp_url_expires = s->info.args.get("temp_url_expires");
  if (temp_url_expires.empty()) {
    return -EPERM;
  }

  /* TempURL case is completely different than the Keystone auth - you may
   * get account name only through extraction from URL. In turn, knowledge
   * about account is neccessary to obtain its bucket tenant. Without that,
   * the access would be limited to accounts with empty tenant. */
  string bucket_tenant;
  if (!s->account_name.empty()) {
    RGWUserInfo uinfo;

    if (rgw_get_user_info_by_uid(store, s->account_name, uinfo) < 0) {
      return -EPERM;
    }

    bucket_tenant = uinfo.user_id.tenant;
  }

  /* Need to get user info of bucket owner. */
  RGWBucketInfo bucket_info;
  int ret = store->get_bucket_info(*static_cast<RGWObjectCtx *>(s->obj_ctx),
                                   bucket_tenant, bucket_name,
                                   bucket_info, NULL);
  if (ret < 0) {
    return -EPERM;
  }

  ldout(s->cct, 20) << "temp url user (bucket owner): " << bucket_info.owner
                    << dendl;
  if (rgw_get_user_info_by_uid(store, bucket_info.owner, *(s->user)) < 0) {
    return -EPERM;
  }

  if (s->user->temp_url_keys.empty()) {
    dout(5) << "user does not have temp url key set, aborting" << dendl;
    return -EPERM;
  }

  if (!s->info.method) {
    return -EPERM;
  }

  const utime_t now = ceph_clock_now(g_ceph_context);

  string err;
  uint64_t expiration = (uint64_t)strict_strtoll(temp_url_expires.c_str(),
                                                 10, &err);
  if (!err.empty()) {
    dout(5) << "failed to parse temp_url_expires: " << err << dendl;
    return -EPERM;
  }
  if (expiration <= (uint64_t)now.sec()) {
    dout(5) << "temp url expired: " << expiration << " <= " << now.sec() << dendl;
    return -EPERM;
  }

  /* We need to verify two paths because of compliance with Swift, Tempest
   * and old versions of RadosGW. The second item will have the prefix
   * of Swift API entry point removed. */
  const size_t pos = g_conf->rgw_swift_url_prefix.find_last_not_of('/') + 1;
  const vector<string> allowed_paths = {
    s->info.request_uri,
    s->info.request_uri.substr(pos + 1)
  };

  vector<string> allowed_methods;
  if (strcmp("HEAD", s->info.method) == 0) {
    /* HEAD requests are specially handled. */
    allowed_methods = { s->info.method, "PUT", "GET" };
  } else {
    allowed_methods = { s->info.method };
  }

  /* Need to try each combination of keys, allowed path and methods. */
  for (const auto kv : s->user->temp_url_keys) {
    const int temp_url_key_num = kv.first;
    const string& temp_url_key = kv.second;

    if (temp_url_key.empty()) {
      continue;
    }

    for (const auto path : allowed_paths) {
      for (const auto method : allowed_methods) {
        const string local_sig = temp_url_gen_sig(temp_url_key,
                                                  method, path,
                                                  temp_url_expires);

        ldout(s->cct, 20) << "temp url signature [" << temp_url_key_num
                          << "] (calculated): " << local_sig
                          << dendl;

        if (local_sig != temp_url_sig) {
          ldout(s->cct,  5) << "temp url signature mismatch: " << local_sig
                            << " != "
                            << temp_url_sig
                            << dendl;
        } else {
          temp_url_make_content_disp(s);
          ldout(s->cct, 20) << "temp url signature match: " << local_sig
                            << " content_disp override " << s->content_disp.override
                            << " content_disp fallback " << s->content_disp.fallback
                            << dendl;
          return 0;
        }
      }
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
    map<string, RGWSubUser>::iterator iter = s->user->subusers.find(subuser);
    if (iter != s->user->subusers.end()) {
      RGWSubUser& subuser_ = iter->second;
      s->perm_mask = subuser_.perm_mask;
    }
  } else {
    s->perm_mask = RGW_PERM_FULL_CONTROL;
  }

  return true;

}

bool RGWSwift::do_verify_swift_token(RGWRados *store, req_state *s)
{
  if (s->info.args.exists("temp_url_sig") ||
      s->info.args.exists("temp_url_expires")) {
    int ret = authenticate_temp_url(store, s);
    return (ret >= 0);
  }

  if (strncmp(s->os_auth_token, "AUTH_rgwtk", 10) == 0 ||
      strncmp(s->os_auth_token, "AUTH_rgwts", 10) == 0) {
    int ret = rgw_swift_verify_signed_token(s->cct, store, s->os_auth_token,
					    *(s->user), &s->swift_user);
    return (ret >= 0);
  }

  struct rgw_swift_auth_info info;

  info.status = 401; // start with access denied, validate_token might change that

  int ret;

  if (supports_keystone()) {
    ret = validate_keystone_token(store, s->os_auth_token, &info, *(s->user));
    return (ret >= 0);
  }

  ret = validate_token(s->os_auth_token, &info);
  if (ret < 0)
    return false;

  if (info.user.empty()) {
    ldout(cct, 5) << "swift auth didn't authorize a user" << dendl;
    return false;
  }

  s->swift_user = info.user.to_str();
  s->swift_groups = info.auth_groups;

  string swift_user = s->swift_user;

  ldout(cct, 10) << "swift user=" << s->swift_user << dendl;

  if (rgw_get_user_info_by_swift(store, swift_user, *(s->user)) < 0) {
    ldout(cct, 0) << "NOTICE: couldn't map swift user" << dendl;
    return false;
  }

  ldout(cct, 10) << "user_id=" << s->user->user_id << dendl;

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
  keystone_revoke_thread->create("rgw_swift_k_rev");
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

