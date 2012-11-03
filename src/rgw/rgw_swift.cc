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

#include "common/ceph_crypto_cms.h"
#include "common/armor.h"

#define dout_subsys ceph_subsys_rgw

static list<string> roles_list;

class RGWKeystoneTokenCache;

class RGWValidateSwiftToken : public RGWHTTPClient {
  struct rgw_swift_auth_info *info;

protected:
  RGWValidateSwiftToken() {}
public:
  RGWValidateSwiftToken(struct rgw_swift_auth_info *_info) : info(_info) {}

  int read_header(void *ptr, size_t len);

  friend class RGWKeystoneTokenCache;
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

class KeystoneToken {
public:
  string token_id;
  string tenant_name;
  string tenant_id;
  string user_name;
  time_t expiration;

  map<string, bool> roles;

  KeystoneToken() {}

  int parse(bufferlist& bl);

  bool expired() {
    uint64_t now = ceph_clock_now(NULL).sec();
    return (now < (uint64_t)expiration);
  }
};

int KeystoneToken::parse(bufferlist& bl)
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
    dout(0) << "token response is missing roles section, or section empty" << dendl;
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
  if (!token) {
    dout(0) << "missing token section in response" << dendl;
    return -EINVAL;
  }

  string expires;

  if (!token->get_data("expires", &expires)) {
    dout(0) << "token response is missing expiration field" << dendl;
    return -EINVAL;
  }

  struct tm t;
  if (!parse_iso8601(expires.c_str(), &t)) {
    dout(0) << "failed to parse token expiration (" << expires << ")" << dendl;
    return -EINVAL;
  }

  expiration = timegm(&t);

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

struct token_entry {
  KeystoneToken token;
  list<string>::iterator lru_iter;
};

class RGWKeystoneTokenCache {
  map<string, token_entry> tokens;
  map<string, map<string, token_entry>::iterator> token_id_map;
  list<string> tokens_lru;

  Mutex lock;

  size_t max;

  void _remove_token_id(const string& token_id) {
    map<string, map<string, token_entry>::iterator>::iterator iter = token_id_map.find(token_id);
    if (iter != token_id_map.end())
      token_id_map.erase(iter);
  }

public:
  RGWKeystoneTokenCache(int _max) : lock("RGWKeystoneTokenCache"), max(_max) {}

  bool find(const string& token_str, KeystoneToken& token);
  void add(const string& token_str, const string& token_id, KeystoneToken& token);
  void invalidate(const string& token_str, const string& token_id, KeystoneToken& token);
};

bool RGWKeystoneTokenCache::find(const string& token_str, KeystoneToken& token)
{
  lock.Lock();
  map<string, token_entry>::iterator iter = tokens.find(token_str);
  if (iter == tokens.end()) {
    lock.Unlock();
    if (perfcounter) perfcounter->inc(l_rgw_keystone_token_cache_miss);
    return false;
  }

  token_entry& entry = iter->second;
  tokens_lru.erase(entry.lru_iter);

  if (entry.token.expired()) {
    _remove_token_id(entry.token.token_id);
    tokens.erase(iter);
    lock.Unlock();
    if (perfcounter) perfcounter->inc(l_rgw_keystone_token_cache_hit);
    return false;
  }
  token = entry.token;

  tokens_lru.push_front(token_str);
  entry.lru_iter = tokens_lru.begin();

  lock.Unlock();
  if (perfcounter) perfcounter->inc(l_rgw_keystone_token_cache_hit);

  return true;
}

void RGWKeystoneTokenCache::add(const string& token_str, const string& token_id, KeystoneToken& token)
{
  lock.Lock();
  map<string, token_entry>::iterator iter = tokens.find(token_str);
  if (iter != tokens.end()) {
    token_entry& e = iter->second;
    _remove_token_id(e.token.token_id);
    tokens_lru.erase(e.lru_iter);
  }

  tokens_lru.push_front(token_str);
  token_entry& entry = tokens[token_str];
  entry.token = token;
  entry.lru_iter = tokens_lru.begin();

  token_id_map[entry.token.token_id] = tokens.find(token_str);

  while (tokens_lru.size() > max) {
    list<string>::reverse_iterator riter = tokens_lru.rbegin();
    iter = tokens.find(*riter);
    assert(iter != tokens.end());
    _remove_token_id(*riter);
    tokens.erase(iter);
    tokens_lru.pop_back();
  }
  
  lock.Unlock();
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

static RGWKeystoneTokenCache *keystone_token_cache = NULL;

class RGWGetRevokedTokens : public RGWHTTPClient {
  bufferlist *bl;
public:
  RGWGetRevokedTokens(bufferlist *_bl) : bl(_bl) {}

  int read_data(void *ptr, size_t len) {
    bl->append((char *)ptr, len);
    return 0;
  }
};
static int open_cms_envelope(string& src, string& dst)
{
#define BEGIN_CMS "-----BEGIN CMS-----"
#define END_CMS "-----END CMS-----"

  int start = src.find(BEGIN_CMS);
  if (start < 0) {
    dout(0) << "failed to find " << BEGIN_CMS << " in response" << dendl;
    return -EINVAL;
  }
  start += sizeof(BEGIN_CMS) - 1;

  int end = src.find(END_CMS);
  if (end < 0) {
    dout(0) << "failed to find " << END_CMS << " in response" << dendl;
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
  
static int rgw_check_revoked()
{
  bufferlist bl;
  RGWGetRevokedTokens req(&bl);

  string url = g_conf->rgw_keystone_url;
  if (url.empty()) {
    dout(0) << "ERROR: keystone url is not configured" << dendl;
    return -EINVAL;
  }
  if (url[url.size() - 1] != '/')
    url.append("/");
  url.append("v2.0/tokens/revoked");

  req.append_header("X-Auth-Token", g_conf->rgw_keystone_admin_token);

  int ret = req.process(url);
  if (ret < 0)
    return ret;

  bl.append((char)0); // NULL terminate

  dout(10) << "request returned " << bl.c_str() << dendl;

  RGWJSONParser parser;

  if (!parser.parse(bl.c_str(), bl.length())) {
    dout(0) << "malformed json" << dendl;
    return -EINVAL;
  }

  JSONObjIter iter = parser.find_first("signed");
  if (iter.end()) {
    dout(0) << "revoked tokens response is missing signed section" << dendl;
    return -EINVAL;
  }  

  JSONObj *signed_obj = *iter;

  string signed_str = signed_obj->get_data();

  dout(10) << "signed=" << signed_str << dendl;

  string signed_b64;
  ret = open_cms_envelope(signed_str, signed_b64);
  if (ret < 0)
    return ret;

  dout(10) << "content=" << signed_b64 << dendl;
  
  bufferptr signed_ber(signed_b64.size() * 2);
  char *dest = signed_ber.c_str();
  const char *src = signed_b64.c_str();
  ret = ceph_unarmor(dest, dest + signed_ber.length(), src, src + signed_b64.size());
  if (ret < 0) {
    dout(0) << "ceph_unarmor() failed, ret=" << ret << dendl;
    return ret;
  }

  bufferlist signed_ber_bl;
  signed_ber_bl.append(signed_ber);

  bufferlist json;

  ret = ceph_decode_cms(signed_ber_bl, json);
  if (ret < 0) {
    dout(0) << "ceph_decode_cms returned " << ret << dendl;
    return ret;
  }

  dout(10) << "ceph_decode_cms: decoded: " << json.c_str() << dendl;

  RGWJSONParser list_parser;
  if (!list_parser.parse(json.c_str(), json.length())) {
    dout(0) << "malformed json" << dendl;
    return -EINVAL;
  }

  JSONObjIter revoked_iter = list_parser.find_first("revoked");
  if (revoked_iter.end()) {
    dout(0) << "no revoked section in json" << dendl;
    return -EINVAL;
  }

  JSONObj *revoked_obj = *revoked_iter;

  JSONObjIter tokens_iter = revoked_obj->find_first();
  for (; !tokens_iter.end(); ++tokens_iter) {
    JSONObj *o = *tokens_iter;

    JSONObj *token = o->find_obj("id");
    if (!token) {
      dout(0) << "bad token in array, missing id" << dendl;
      continue;
    }

    dout(20) << "token id=" << token->get_data() << dendl;
  }
  
  return 0;
}

static void rgw_set_keystone_token_auth_info(KeystoneToken& token, struct rgw_swift_auth_info *info)
{
  info->user = token.tenant_id;
  info->display_name = token.tenant_name;
  info->status = 200;
}

static int rgw_parse_keystone_token_response(const string& token, bufferlist& bl, struct rgw_swift_auth_info *info)
{
  KeystoneToken t;
  int ret = t.parse(bl);
  if (ret < 0)
    return ret;

  bool found = false;
  list<string>::iterator iter;
  for (iter = roles_list.begin(); iter != roles_list.end(); ++iter) {
    const string& role = *iter;
    if (t.roles.find(role) != t.roles.end()) {
      found = true;
      break;
    }
  }

  if (!found) {
    dout(0) << "user does not hold a matching role; required roles: " << g_conf->rgw_keystone_operator_roles << dendl;
    return -EPERM;
  }

  dout(0) << "validated token: " << t.tenant_name << ":" << t.user_name << " expires: " << t.expiration << dendl;

  rgw_set_keystone_token_auth_info(t, info);
  keystone_token_cache->add(token, t.token_id, t);

  return 0;
}

static int update_user_info(RGWRados *store, struct rgw_swift_auth_info *info, RGWUserInfo& user_info)
{
  if (rgw_get_user_info_by_uid(store, info->user, user_info) < 0) {
    dout(0) << "NOTICE: couldn't map swift user" << dendl;
    user_info.user_id = info->user;
    user_info.display_name = info->display_name;

    int ret = rgw_store_user_info(store, user_info, true);
    if (ret < 0) {
      dout(0) << "ERROR: failed to store new user's info: ret=" << ret << dendl;
      return ret;
    }
  }
  return 0;
}

static int rgw_swift_validate_keystone_token(RGWRados *store, const string& token, struct rgw_swift_auth_info *info,
					     RGWUserInfo& rgw_user)
{
  KeystoneToken t;

  rgw_check_revoked();

  if (keystone_token_cache->find(token, t)) {
    rgw_set_keystone_token_auth_info(t, info);
    int ret = update_user_info(store, info, rgw_user);
    if (ret < 0)
      return ret;

    return 0;
  }

  bufferlist bl;
  RGWValidateKeystoneToken validate(&bl);

  string url = g_conf->rgw_keystone_url;
  if (url.empty()) {
    dout(0) << "ERROR: keystone url is not configured" << dendl;
    return -EINVAL;
  }
  if (url[url.size() - 1] != '/')
    url.append("/");
  url.append("v2.0/tokens/");
  url.append(token);

  validate.append_header("X-Auth-Token", g_conf->rgw_keystone_admin_token);

  int ret = validate.process(url);
  if (ret < 0)
    return ret;

  bl.append((char)0); // NULL terminate

  dout(20) << "received response: " << bl.c_str() << dendl;

  ret = rgw_parse_keystone_token_response(token, bl, info);
  if (ret < 0)
    return ret;

  ret = update_user_info(store, info, rgw_user);
  if (ret < 0)
    return ret;

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

void swift_init(CephContext *cct)
{
  get_str_list(cct->_conf->rgw_keystone_operator_roles, roles_list);

  keystone_token_cache = new RGWKeystoneTokenCache(cct->_conf->rgw_keystone_token_cache_size);
}


void swift_finalize()
{
  delete keystone_token_cache;
  keystone_token_cache = NULL;
}

