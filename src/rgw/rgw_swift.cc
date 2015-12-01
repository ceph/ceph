// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "common/ceph_json.h"
#include "common/ceph_crypto_cms.h"
#include "rgw_common.h"
#include "rgw_swift.h"
#include "rgw_swift_auth.h"
#include "rgw_user.h"
#include "rgw_http_client.h"
#include "rgw_keystone.h"
#include "rgw_cms.h"

#include "include/str_list.h"

#define dout_subsys ceph_subsys_rgw

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

static void rgw_set_keystone_token_auth_info(KeystoneToken& token, struct rgw_swift_auth_info *info)
{
  info->user = token.get_project_id();
  info->display_name = token.get_project_name();
  info->status = 200;
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
  KeystoneToken t = KeystoneToken(g_conf->rgw_keystone_api_version);

  int ret = keystone->validate_token(token, t);
  if (ret < 0) {
    return ret;
  }

  rgw_set_keystone_token_auth_info(t, info);
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

  if (keystone->enabled()) {
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

Keystone *rgw_keystone = NULL;
RGWSwift *rgw_swift = NULL;

void swift_init(CephContext *cct)
{
  rgw_keystone = new Keystone(cct);
  rgw_swift = new RGWSwift(cct, rgw_keystone);
}

void swift_finalize()
{
  delete rgw_keystone;
}
