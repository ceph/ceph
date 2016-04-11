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

static vector<string> accepted_roles;
static vector<string> accepted_admin_roles;

class RGWValidateSwiftToken : public RGWHTTPClient {
  struct rgw_swift_auth_info *info;

protected:
  RGWValidateSwiftToken()
    : RGWHTTPClient(nullptr),
      info(nullptr) {
  }
public:
  RGWValidateSwiftToken(CephContext *_cct,
                        struct rgw_swift_auth_info *_info)
    : RGWHTTPClient(_cct),
      info(_info) {
  }

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
          ;
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

int RGWSwift::validate_token(RGWRados * const store,
                             const char *token,
                             rgw_swift_auth_info& auth_info)    /* out */
{
  if (g_conf->rgw_swift_auth_url.empty()) {
    return -EINVAL;
  }

  string auth_url = g_conf->rgw_swift_auth_url;

  if (auth_url[auth_url.size() - 1] != '/') {
    auth_url.append("/");
  }

  auth_url.append("token");
  char url_buf[auth_url.size() + 1 + strlen(token) + 1];
  sprintf(url_buf, "%s/%s", auth_url.c_str(), token);

  RGWValidateSwiftToken validate(cct, &auth_info);

  ldout(cct, 10) << "rgw_swift_validate_token url=" << url_buf << dendl;

  int ret = validate.process(url_buf);
  if (ret < 0) {
    return ret;
  }

  if (auth_info.user.empty()) {
    ldout(cct, 5) << "swift auth didn't authorize a user" << dendl;
    return -EPERM;
  }

  string swift_user = auth_info.user.to_str();
  ldout(cct, 10) << "swift user=" << swift_user << dendl;

  RGWUserInfo tmp_uinfo;
  ret = rgw_get_user_info_by_swift(store, swift_user, tmp_uinfo);
  if (ret < 0) {
    ldout(cct, 0) << "NOTICE: couldn't map swift user" << dendl;
    return ret;
  }

  auth_info.perm_mask = get_perm_mask(swift_user, tmp_uinfo);
  auth_info.is_admin = false;

  return 0;
}


class RGWKeystoneHTTPTransceiver : public RGWHTTPTransceiver {
public:
  RGWKeystoneHTTPTransceiver(CephContext * const cct,
                             bufferlist * const token_body_bl)
    : RGWHTTPTransceiver(cct, token_body_bl,
                         cct->_conf->rgw_keystone_verify_ssl,
                         { "X-Subject-Token" }) {
  }

  std::string get_subject_token() const {
    try {
      return get_header_value("X-Subject-Token");
    } catch (std::out_of_range&) {
      return header_value_t();
    }
  }
};

typedef RGWKeystoneHTTPTransceiver RGWValidateKeystoneToken;
typedef RGWKeystoneHTTPTransceiver RGWGetKeystoneAdminToken;
typedef RGWKeystoneHTTPTransceiver RGWGetRevokedTokens;

int RGWSwift::get_keystone_url(CephContext * const cct,
                               std::string& url)
{
  // FIXME: it seems we don't need RGWGetRevokedToken here
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


int RGWSwift::check_revoked()
{
  string url;
  string token;

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
    RGWKeystoneTokenCache::get_instance().invalidate(token_id);
  }
  
  return 0;
}

static void rgw_set_keystone_token_auth_info(const KeystoneToken& token,
                                             struct rgw_swift_auth_info * const info)
{
  info->user = token.get_project_id();
  info->display_name = token.get_project_name();
  info->status = 200;

  /* Check whether the user has an admin status. */
  bool is_admin = false;
  for (const auto admin_role : accepted_admin_roles) {
    if (token.has_role(admin_role)) {
      is_admin = true;
      break;
    }
  }
  info->is_admin = is_admin;
  info->perm_mask = RGW_PERM_FULL_CONTROL;
}

int RGWSwift::parse_keystone_token_response(const string& token,
                                            bufferlist& bl,
                                            struct rgw_swift_auth_info * const info,
                                            KeystoneToken& t)
{
  int ret = t.parse(cct, token, bl);
  if (ret < 0) {
    return ret;
  }

  bool found = false;
  for (const auto role : accepted_roles) {
    if (t.has_role(role) == true) {
      found = true;
      break;
    }
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

int RGWSwift::load_acct_info(RGWRados * const store,
                             const string& account_name,
                             const struct rgw_swift_auth_info& info,
                             RGWUserInfo& user_info)                /* out */
{
  ldout(cct, 20) << "updating user=" << info.user << dendl; // P3 XXX
  /*
   * Normally once someone parsed the token, the tenant and user are set
   * in rgw_swift_auth_info. If .tenant is empty in it, the client has
   * authenticated with the empty legacy tenant. But when we authenticate
   * with Keystone, we have a special compatibility kludge. First, we try
   * the same tenant as the user. If that user exists, we use it. This way,
   * migrated OpenStack users can get their namespaced containers and
   * nobody's the wiser. If that fails, we look up the user in the empty
   * tenant. If neither is found, make one, and those migrating can
   * set a special configurable rgw_keystone_implicit_tenants to create
   * suitable tenantized users.
   */
  if (info.user.tenant.empty()) {
    rgw_user uid = !account_name.empty() ? account_name : info.user;

    if (rgw_get_user_info_by_uid(store, uid, user_info) < 0) {
      uid.tenant.clear();

      if (rgw_get_user_info_by_uid(store, uid, user_info) < 0) {
        ldout(cct, 0) << "NOTICE: couldn't map swift user " << uid << dendl;

        if (g_conf->rgw_keystone_implicit_tenants) {
          uid.tenant = info.user.id;
        }

        if (uid != info.user) {
          ldout(cct, 0) << "ERROR: only owner may create the account" << dendl;
          return -EPERM;
        }

        user_info.user_id = uid;
        user_info.display_name = info.display_name;
        int ret = rgw_store_user_info(store, user_info, nullptr, nullptr,
                                      real_time(), true);
        if (ret < 0) {
          ldout(cct, 0) << "ERROR: failed to store new user info: user="
                        << user_info.user_id << " ret=" << ret << dendl;
          return ret;
        }
      }
    }
  } else {
    if (rgw_get_user_info_by_uid(store, info.user, user_info) < 0) {
      ldout(cct, 0) << "NOTICE: couldn't map swift user " << info.user << dendl;

      user_info.user_id = info.user;
      user_info.display_name = info.display_name;

      int ret = rgw_store_user_info(store, user_info, NULL, NULL, real_time(), true);
      if (ret < 0) {
        ldout(cct, 0) << "ERROR: failed to store new user info: user="
                      << user_info.user_id << " ret=" << ret << dendl;
        return ret;
      }
    }
  }
  return 0;
}

int RGWSwift::load_user_info(RGWRados *const store,
                             const struct rgw_swift_auth_info& auth_info,
                             rgw_user& auth_user,               /* out */
                             uint32_t& perm_mask,               /* out */
                             bool& admin_request)               /* out */
{
  if (auth_info.status != 200) {
    return -EPERM;
  }

  auth_user = auth_info.user;
  perm_mask = auth_info.perm_mask;
  admin_request = auth_info.is_admin;

  return 0;
}

int RGWSwift::validate_keystone_token(RGWRados * const store,
                                      const string& token,
                                      struct rgw_swift_auth_info *info) /* out */
{
  KeystoneToken t;

  string token_id;
  rgw_get_token_id(token, token_id);

  ldout(cct, 20) << "token_id=" << token_id << dendl;

  /* check cache first */
  if (RGWKeystoneTokenCache::get_instance().find(token_id, t)) {
    ldout(cct, 20) << "cached token.project.id=" << t.get_project_id() << dendl;
    rgw_set_keystone_token_auth_info(t, info);
    return 0;
  }

  bufferlist bl;

  /* check if that's a self signed token that we can decode */
  if (!rgw_decode_pki_token(cct, token, bl)) {

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
    if (KeystoneService::get_keystone_admin_token(cct, admin_token) < 0)
      return -EINVAL;
    if (KeystoneService::get_keystone_url(cct, url) < 0)
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

  RGWKeystoneTokenCache::get_instance().add(token_id, t);

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

int authenticate_temp_url(RGWRados * const store,
                          /* const */ req_state * const s,
                          rgw_swift_auth_info& auth_info)
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
  RGWUserInfo owner_info;
  if (rgw_get_user_info_by_uid(store, bucket_info.owner, owner_info) < 0) {
    return -EPERM;
  }

  if (owner_info.temp_url_keys.empty()) {
    dout(5) << "user does not have temp url key set, aborting" << dendl;
    return -EPERM;
  }

  if (!s->info.method) {
    return -EPERM;
  }

  const utime_t now = ceph_clock_now(g_ceph_context);

  string err;
  const uint64_t expiration = (uint64_t)strict_strtoll(temp_url_expires.c_str(),
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
  for (const auto kv : owner_info.temp_url_keys) {
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

          auth_info.is_admin = false;
          auth_info.user = owner_info.user_id;
          auth_info.perm_mask = RGW_PERM_FULL_CONTROL;
          auth_info.status = 200;

          return 0;
        }
      }
    }
  }

  return -EPERM;
}

uint32_t RGWSwift::get_perm_mask(const string& swift_user,
                                 const RGWUserInfo &uinfo)
{
  uint32_t perm_mask = 0;

  if (!swift_user.empty()) {
    string subuser;
    ssize_t pos = swift_user.find(':');
    if (pos < 0) {
      subuser = swift_user;
    } else {
      subuser = swift_user.substr(pos + 1);
    }

    auto iter = uinfo.subusers.find(subuser);
    if (iter != uinfo.subusers.end()) {
      const RGWSubUser& subuser_ = iter->second;
      perm_mask = subuser_.perm_mask;
    }
  } else {
    perm_mask = RGW_PERM_FULL_CONTROL;
  }

  return perm_mask;
}

bool RGWSwift::verify_swift_token(RGWRados *store, req_state *s)
{
  if (!do_verify_swift_token(store, s)) {
    return false;
  }

  return true;

}

bool RGWSwift::do_verify_swift_token(RGWRados *store, req_state *s)
{
  struct rgw_swift_auth_info auth_info;

  if (s->info.args.exists("temp_url_sig") ||
      s->info.args.exists("temp_url_expires")) {
    if (authenticate_temp_url(store, s /* const! */, auth_info) < 0) {
      return false;
    }

    if (load_acct_info(store, s->account_name, auth_info, *(s->user)) < 0) {
      return false;
    }

    if (load_user_info(store, auth_info, s->auth_user, s->perm_mask,
                       s->admin_request) < 0) {
      return false;
    }

    return  true;
  }

  if (strncmp(s->os_auth_token, "AUTH_rgwtk", 10) == 0) {
    if (rgw_swift_verify_signed_token(s->cct, store, s->os_auth_token,
                                      auth_info) < 0) {
      return false;
    }

    if (load_acct_info(store, s->account_name, auth_info, *(s->user)) < 0) {
      return false;
    }

    if (load_user_info(store, auth_info, s->auth_user, s->perm_mask,
                       s->admin_request) < 0) {
      return false;
    }

    return  true;
  }

  if (supports_keystone()) {
    if (validate_keystone_token(store, s->os_auth_token, &auth_info) < 0) {
      /* Authentication failed. */
      return false;
    }

    if (load_acct_info(store, s->account_name, auth_info, *(s->user)) < 0) {
      return false;
    }

    if (load_user_info(store, auth_info, s->auth_user, s->perm_mask,
                       s->admin_request) < 0) {
      return false;
    }

    return true;
  }

  if (validate_token(store, s->os_auth_token, auth_info) < 0) {
    ldout(cct, 5) << "swift auth didn't authorize a user" << dendl;
    return false;
  }

  if (load_acct_info(store, s->account_name, auth_info, *(s->user)) < 0) {
    return false;
  }

  if (load_user_info(store, auth_info, s->auth_user, s->perm_mask,
                     s->admin_request) < 0) {
    return false;
  }

  ldout(cct, 10) << "user_id=" << s->user->user_id << dendl;

  return true;
}

void RGWSwift::init()
{
  get_str_vec(cct->_conf->rgw_keystone_accepted_roles, accepted_roles);
  get_str_vec(cct->_conf->rgw_keystone_accepted_admin_roles,
              accepted_admin_roles);

  accepted_roles.insert(accepted_roles.end(), accepted_admin_roles.begin(),
                        accepted_admin_roles.end());

  if (supports_keystone()) {
    init_keystone();
  }
}


void RGWSwift::init_keystone()
{
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
