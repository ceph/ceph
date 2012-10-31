#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "rgw_json.h"
#include "rgw_common.h"
#include "rgw_swift.h"
#include "rgw_swift_auth.h"
#include "rgw_user.h"
#include "rgw_http_client.h"

#include "include/str_list.h"

#define dout_subsys ceph_subsys_rgw

class RGWValidateSwiftToken : public RGWHTTPClient {
  struct rgw_swift_auth_info *info;
public:
  RGWValidateSwiftToken(struct rgw_swift_auth_info *_info) :info(_info) {}

  int read_header(void *ptr, size_t len);
};

int RGWValidateSwiftToken::read_header(void *ptr, size_t len)
{
  char line[len + 1];

  char *s = (char *)ptr, *end = (char *)ptr + len;
  char *p = line;
  dout(10) << "read_http_header" << dendl;

  while (s != end) {
    if (*s == '\r') {
      s++;
      continue;
    }
    if (*s == '\n') {
      *p = '\0';
      dout(10) << "os_auth:" << line << dendl;
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

static int rgw_swift_validate_token(const char *token, struct rgw_swift_auth_info *info)
{
  if (g_conf->rgw_swift_auth_url.empty())
    return -EINVAL;

  string auth_url = g_conf->rgw_swift_auth_url;
  if (auth_url[auth_url.size() - 1] != '/')
    auth_url.append("/");
  auth_url.append("token");
  char url_buf[auth_url.size() + 1 + strlen(token) + 1];
  sprintf(url_buf, "%s/%s", auth_url.c_str(), token);

  RGWValidateSwiftToken validate(info);

  dout(10) << "rgw_swift_validate_token url=" << url_buf << dendl;

  int ret = validate.process(url_buf);
  if (ret < 0)
    return ret;

  return 0;
}

class RGWValidateKeystoneToken : public RGWHTTPClient {
  bufferlist *bl;
public:
  RGWValidateKeystoneToken(bufferlist *_bl) : bl(_bl) {}

  int read_data(void *ptr, size_t len) {
    bl->append((char *)ptr, len);
    return 0;
  }
};

class KeystoneTokenResponseParser {
public:
  string tenant_name;
  string tenant_id;
  string user_name;
  string expires;

  map<string, bool> roles;

  KeystoneTokenResponseParser() {}

  int parse(bufferlist& bl);
};

int KeystoneTokenResponseParser::parse(bufferlist& bl)
{
  RGWJSONParser parser;

  if (!parser.parse(bl.c_str(), bl.length())) {
    dout(0) << "malformed json" << dendl;
    return -EINVAL;
  }

  JSONObjIter iter = parser.find_first("access");
  if (iter.end()) {
    dout(0) << "token response is missing access section" << dendl;
    return -EINVAL;
  }  

  JSONObj *access_obj = *iter;
  JSONObj *user = access_obj->find_obj("user");
  if (!user) {
    dout(0) << "token response is missing user section" << dendl;
    return -EINVAL;
  }

  if (!user->get_data("username", &user_name)) {
    dout(0) << "token response is missing user username field" << dendl;
    return -EINVAL;
  }

  JSONObjIter riter = user->find("roles");
  if (riter.end()) {
    dout(0) << "token response is missing roles section" << dendl;
    return -EINVAL;
  }

  for (; !riter.end(); ++riter) {
    JSONObj *o = *riter;
    JSONObj *role_name = o->find_obj("name");
    if (!role_name) {
      dout(0) << "token response is missing role name section" << dendl;
      return -EINVAL;
    }
    string role = role_name->get_data();
    roles[role] = true;
  }

  JSONObj *token = access_obj->find_obj("token");
  if (!user) {
    dout(0) << "missing token section in response" << dendl;
    return -EINVAL;
  }

  if (!token->get_data("expires", &expires)) {
    dout(0) << "token response is missing expiration field" << dendl;
    return -EINVAL;
  }

  JSONObj *tenant = token->find_obj("tenant");
  if (!tenant) {
    dout(0) << "token response is missing tenant section" << dendl;
    return -EINVAL;
  }

  if (!tenant->get_data("id", &tenant_id)) {
    dout(0) << "tenant is missing id field" << dendl;
    return -EINVAL;
  }


  if (!tenant->get_data("name", &tenant_name)) {
    dout(0) << "tenant is missing name field" << dendl;
    return -EINVAL;
  }

  return 0;
}

static int rgw_parse_keystone_token_response(bufferlist& bl, struct rgw_swift_auth_info *info)
{
  RGWJSONParser parser;

  if (!parser.parse(bl.c_str(), bl.length())) {
    dout(0) << "malformed json" << dendl;
    return -EINVAL;
  }

  KeystoneTokenResponseParser p;
  int ret = p.parse(bl);
  if (ret < 0)
    return ret;

  list<string> roles_list;

  get_str_list(g_conf->rgw_swift_keystone_operator_roles, roles_list);

  bool found = false;
  list<string>::iterator iter;
  for (iter = roles_list.begin(); iter != roles_list.end(); ++iter) {
    const string& role = *iter;
    if (p.roles.find(role) != p.roles.end()) {
      found = true;
      break;
    }
  }

  if (!found) {
    dout(0) << "user does not hold a matching role; required roles: " << g_conf->rgw_swift_keystone_operator_roles << dendl;
    return -EPERM;
  }

  dout(0) << "validated token: " << p.tenant_name << ":" << p.user_name << " expires: " << p.expires << dendl;

  info->user = p.tenant_id;
  info->display_name = p.tenant_name;
  info->status = 200;

  return 0;
}

static int rgw_swift_validate_keystone_token(RGWRados *store, const char *token, struct rgw_swift_auth_info *info,
					     RGWUserInfo& rgw_user)
{
  bufferlist bl;
  RGWValidateKeystoneToken validate(&bl);

  string url = g_conf->rgw_swift_keystone_url;
  if (url[url.size() - 1] != '/')
    url.append("/");
  url.append("v2.0/tokens/");
  url.append(token);

  validate.append_header("X-Auth-Token", g_conf->rgw_swift_keystone_admin_token);

  int ret = validate.process(url);
  if (ret < 0)
    return ret;

  dout(0) << "received response: " << bl.c_str() << dendl;

  ret = rgw_parse_keystone_token_response(bl, info);
  if (ret < 0)
    return ret;

  if (rgw_get_user_info_by_uid(store, info->user, rgw_user) < 0) {
    dout(0) << "NOTICE: couldn't map swift user" << dendl;
    rgw_user.user_id = info->user;
    rgw_user.display_name = info->display_name;

    ret = rgw_store_user_info(store, rgw_user, true);
    if (ret < 0) {
      dout(0) << "ERROR: failed to store new user's info: ret=" << ret << dendl;
      return ret;
    }
  }

  return 0;
}


bool rgw_verify_swift_token(RGWRados *store, req_state *s)
{
  if (!s->os_auth_token)
    return false;

  if (strncmp(s->os_auth_token, "AUTH_rgwtk", 10) == 0) {
    int ret = rgw_swift_verify_signed_token(s->cct, store, s->os_auth_token, s->user);
    if (ret < 0)
      return false;

    return  true;
  }

  struct rgw_swift_auth_info info;

  info.status = 401; // start with access denied, validate_token might change that

  int ret;

  if (g_conf->rgw_swift_use_keystone) {
    ret = rgw_swift_validate_keystone_token(store, s->os_auth_token, &info, s->user);
    return (ret >= 0);
  }

  ret = rgw_swift_validate_token(s->os_auth_token, &info);
  if (ret < 0)
    return ret;

  if (info.user.empty()) {
    dout(5) << "swift auth didn't authorize a user" << dendl;
    return false;
  }

  s->swift_user = info.user;
  s->swift_groups = info.auth_groups;

  string swift_user = s->swift_user;

  dout(10) << "swift user=" << s->swift_user << dendl;

  if (rgw_get_user_info_by_swift(store, swift_user, s->user) < 0) {
    dout(0) << "NOTICE: couldn't map swift user" << dendl;
    return false;
  }

  dout(10) << "user_id=" << s->user.user_id << dendl;

  return true;
}
