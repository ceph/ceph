// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_swift_auth.h"
#include "rgw_rest.h"

#include "common/ceph_crypto.h"
#include "common/Clock.h"

#include "auth/Crypto.h"

#include "rgw_client_io.h"
#include "rgw_http_client.h"
#include "include/str_list.h"

#define dout_subsys ceph_subsys_rgw

#define DEFAULT_SWIFT_PREFIX "swift"

using namespace ceph::crypto;


/* TempURL: applier */
void RGWTempURLAuthApplier::modify_request_state(req_state * s) const       /* in/out */
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

  ldout(s->cct, 20) << "finished applying changes to req_state for TempURL: "
                    << " content_disp override " << s->content_disp.override
                    << " content_disp fallback " << s->content_disp.fallback
                    << dendl;

}

/* TempURL: engine */
bool RGWTempURLAuthEngine::is_applicable() const noexcept
{
  return s->info.args.exists("temp_url_sig") ||
         s->info.args.exists("temp_url_expires");
}

void RGWTempURLAuthEngine::get_owner_info(RGWUserInfo& owner_info) const
{
  /* We cannot use req_state::bucket_name because it isn't available
   * now. It will be initialized in RGWHandler_REST_SWIFT::postauth_init(). */
  const string& bucket_name = s->init_state.url_bucket;

  /* TempURL requires bucket and object specified in the requets. */
  if (bucket_name.empty() || s->object.empty()) {
    throw -EPERM;
  }

  /* TempURL case is completely different than the Keystone auth - you may
   * get account name only through extraction from URL. In turn, knowledge
   * about account is neccessary to obtain its bucket tenant. Without that,
   * the access would be limited to accounts with empty tenant. */
  string bucket_tenant;
  if (!s->account_name.empty()) {
    RGWUserInfo uinfo;

    if (rgw_get_user_info_by_uid(store, s->account_name, uinfo) < 0) {
      throw -EPERM;
    } else {
      bucket_tenant = uinfo.user_id.tenant;
    }
  }

  /* Need to get user info of bucket owner. */
  RGWBucketInfo bucket_info;
  int ret = store->get_bucket_info(*static_cast<RGWObjectCtx *>(s->obj_ctx),
                                   bucket_tenant, bucket_name,
                                   bucket_info, nullptr);
  if (ret < 0) {
    throw ret;
  }

  ldout(s->cct, 20) << "temp url user (bucket owner): " << bucket_info.owner
                    << dendl;

  if (rgw_get_user_info_by_uid(store, bucket_info.owner, owner_info) < 0) {
    throw -EPERM;
  }
}

bool RGWTempURLAuthEngine::is_expired(const std::string& expires) const
{
  string err;
  const utime_t now = ceph_clock_now(g_ceph_context);
  const uint64_t expiration = (uint64_t)strict_strtoll(expires.c_str(),
                                                       10, &err);
  if (!err.empty()) {
    dout(5) << "failed to parse temp_url_expires: " << err << dendl;
    return true;
  }

  if (expiration <= (uint64_t)now.sec()) {
    dout(5) << "temp url expired: " << expiration << " <= " << now.sec() << dendl;
    return true;
  }

  return false;
}

std::string extract_swift_subuser(const std::string& swift_user_name) {
  size_t pos = swift_user_name.find(':');
  if (std::string::npos == pos) {
    return swift_user_name;
  } else {
    return swift_user_name.substr(pos + 1);
  }
}

std::string RGWTempURLAuthEngine::generate_signature(const string& key,
                                                     const string& method,
                                                     const string& path,
                                                     const string& expires) const
{
  const string str = method + "\n" + expires + "\n" + path;
  ldout(cct, 20) << "temp url signature (plain text): " << str << dendl;

  /* unsigned */ char dest[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE];
  calc_hmac_sha1(key.c_str(), key.size(),
                 str.c_str(), str.size(),
                 dest);

  char dest_str[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE * 2 + 1];
  buf_to_hex((const unsigned char *)dest, sizeof(dest), dest_str);

  return dest_str;
}

RGWAuthApplier::aplptr_t RGWTempURLAuthEngine::authenticate() const
{
  const string temp_url_sig = s->info.args.get("temp_url_sig");
  const string temp_url_expires = s->info.args.get("temp_url_expires");
  if (temp_url_sig.empty() || temp_url_expires.empty()) {
    return nullptr;
  }

  RGWUserInfo owner_info;
  try {
    get_owner_info(owner_info);
  } catch (...) {
    ldout(cct, 5) << "cannot get user_info of account's owner" << dendl;
    return nullptr;
  }

  if (owner_info.temp_url_keys.empty()) {
    ldout(cct, 5) << "user does not have temp url key set, aborting" << dendl;
    return nullptr;
  }

  if (is_expired(temp_url_expires)) {
    ldout(cct, 5) << "temp url link expired" << dendl;
    return nullptr;
  }

  /* We need to verify two paths because of compliance with Swift, Tempest
   * and old versions of RadosGW. The second item will have the prefix
   * of Swift API entry point removed. */
  const size_t pos = g_conf->rgw_swift_url_prefix.find_last_not_of('/') + 1;
  const std::vector<std::string> allowed_paths = {
    s->info.request_uri,
    s->info.request_uri.substr(pos + 1)
  };

  /* Account owner calculates the signature also against a HTTP method. */
  std::vector<std::string> allowed_methods;
  if (strcmp("HEAD", s->info.method) == 0) {
    /* HEAD requests are specially handled. */
    allowed_methods = { "HEAD", "GET", "PUT" };
  } else if (strlen(s->info.method) > 0) {
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
        const std::string local_sig = generate_signature(temp_url_key,
                                                         method, path,
                                                         temp_url_expires);

        ldout(s->cct, 20) << "temp url signature [" << temp_url_key_num
                          << "] (calculated): " << local_sig
                          << dendl;

        if (local_sig != temp_url_sig) {
          ldout(s->cct,  5) << "temp url signature mismatch: " << local_sig
                            << " != " << temp_url_sig  << dendl;
        } else {
          return apl_factory->create_loader(cct, owner_info);
        }
      }
    }
  }

  return nullptr;
}


/* External token */
bool RGWExternalTokenAuthEngine::is_applicable() const noexcept
{
  if (false == RGWTokenBasedAuthEngine::is_applicable()) {
    return false;
  }

  return false == g_conf->rgw_swift_auth_url.empty();
}

RGWAuthApplier::aplptr_t RGWExternalTokenAuthEngine::authenticate() const
{
  string auth_url = g_conf->rgw_swift_auth_url;
  if (auth_url[auth_url.length() - 1] != '/') {
    auth_url.append("/");
  }

  auth_url.append("token");
  char url_buf[auth_url.size() + 1 + token.length() + 1];
  sprintf(url_buf, "%s/%s", auth_url.c_str(), token.c_str());

  RGWHTTPHeadersCollector validator(cct, { "X-Auth-Groups", "X-Auth-Ttl" });

  ldout(cct, 10) << "rgw_swift_validate_token url=" << url_buf << dendl;

  int ret = validator.process(url_buf);
  if (ret < 0) {
    throw ret;
  }

  std::string swift_user;
  try {
    std::vector<std::string> swift_groups;
    get_str_vec(validator.get_header_value("X-Auth-Groups"),
                ",", swift_groups);

    if (0 == swift_groups.size()) {
      return nullptr;
    } else {
      swift_user = swift_groups[0];
    }
  } catch (std::out_of_range) {
    /* The X-Auth-Groups header isn't present in the response. */
    return nullptr;
  }

  if (swift_user.empty()) {
    return nullptr;
  }

  ldout(cct, 10) << "swift user=" << swift_user << dendl;

  RGWUserInfo tmp_uinfo;
  ret = rgw_get_user_info_by_swift(store, swift_user, tmp_uinfo);
  if (ret < 0) {
    ldout(cct, 0) << "NOTICE: couldn't map swift user" << dendl;
    throw ret;
  }

  return apl_factory->create_loader(cct, tmp_uinfo,
                                    extract_swift_subuser(swift_user));
}


static int build_token(const string& swift_user,
                       const string& key,
                       const uint64_t nonce,
                       const utime_t& expiration,
                       bufferlist& bl)
{
  ::encode(swift_user, bl);
  ::encode(nonce, bl);
  ::encode(expiration, bl);

  bufferptr p(CEPH_CRYPTO_HMACSHA1_DIGESTSIZE);

  char buf[bl.length() * 2 + 1];
  buf_to_hex((const unsigned char *)bl.c_str(), bl.length(), buf);
  dout(20) << "build_token token=" << buf << dendl;

  char k[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE];
  memset(k, 0, sizeof(k));
  const char *s = key.c_str();
  for (int i = 0; i < (int)key.length(); i++, s++) {
    k[i % CEPH_CRYPTO_HMACSHA1_DIGESTSIZE] |= *s;
  }
  calc_hmac_sha1(k, sizeof(k), bl.c_str(), bl.length(), p.c_str());

  bl.append(p);

  return 0;

}

static int encode_token(CephContext *cct, string& swift_user, string& key,
			bufferlist& bl)
{
  uint64_t nonce;

  int ret = get_random_bytes((char *)&nonce, sizeof(nonce));
  if (ret < 0)
    return ret;

  utime_t expiration = ceph_clock_now(cct);
  expiration += cct->_conf->rgw_swift_token_expiration;

  ret = build_token(swift_user, key, nonce, expiration, bl);

  return ret;
}


/* AUTH_rgwtk (signed token): engine */
bool RGWSignedTokenAuthEngine::is_applicable() const noexcept
{
  if (false == RGWTokenBasedAuthEngine::is_applicable()) {
    return false;
  }

  return !token.compare(0, 10, "AUTH_rgwtk");
}

RGWAuthApplier::aplptr_t RGWSignedTokenAuthEngine::authenticate() const
{
  /* Effective token string is the part after the prefix. */
  const std::string etoken = token.substr(strlen("AUTH_rgwtk"));
  const size_t etoken_len = etoken.length();

  if (etoken_len & 1) {
    ldout(cct, 0) << "NOTICE: failed to verify token: odd token length="
	          << etoken_len << dendl;
    throw -EINVAL;
  }

  bufferptr p(etoken_len/2);
  int ret = hex_to_buf(token.c_str(), p.c_str(), etoken_len);
  if (ret < 0) {
    throw ret;
  }

  bufferlist tok_bl;
  tok_bl.append(p);

  uint64_t nonce;
  utime_t expiration;
  std::string swift_user;

  try {
    /*const*/ auto iter = tok_bl.begin();

    ::decode(swift_user, iter);
    ::decode(nonce, iter);
    ::decode(expiration, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "NOTICE: failed to decode token" << dendl;
    throw -EINVAL;
  }

  const utime_t now = ceph_clock_now(cct);
  if (expiration < now) {
    ldout(cct, 0) << "NOTICE: old timed out token was used now=" << now
	          << " token.expiration=" << expiration
                  << dendl;
    return nullptr;
  }

  RGWUserInfo user_info;
  ret = rgw_get_user_info_by_swift(store, swift_user, user_info);
  if (ret < 0) {
    throw ret;
  }

  ldout(cct, 10) << "swift_user=" << swift_user << dendl;

  const auto siter = user_info.swift_keys.find(swift_user);
  if (siter == std::end(user_info.swift_keys)) {
    return nullptr;
  }

  const auto swift_key = siter->second;

  bufferlist local_tok_bl;
  ret = build_token(swift_user, swift_key.key, nonce, expiration, local_tok_bl);
  if (ret < 0) {
    throw ret;
  }

  if (local_tok_bl.length() != tok_bl.length()) {
    ldout(cct, 0) << "NOTICE: tokens length mismatch:"
                  << " tok_bl.length()=" << tok_bl.length()
	          << " local_tok_bl.length()=" << local_tok_bl.length()
                  << dendl;
    return nullptr;
  }

  if (memcmp(local_tok_bl.c_str(), tok_bl.c_str(),
             local_tok_bl.length()) != 0) {
    char buf[local_tok_bl.length() * 2 + 1];

    buf_to_hex(reinterpret_cast<const unsigned char *>(local_tok_bl.c_str()),
               local_tok_bl.length(), buf);

    ldout(cct, 0) << "NOTICE: tokens mismatch tok=" << buf << dendl;
    return nullptr;
  }

  return apl_factory->create_loader(cct, user_info,
                                    extract_swift_subuser(swift_user));
}


void RGW_SWIFT_Auth_Get::execute()
{
  int ret = -EPERM;

  const char *key = s->info.env->get("HTTP_X_AUTH_KEY");
  const char *user = s->info.env->get("HTTP_X_AUTH_USER");

  string user_str;
  RGWUserInfo info;
  bufferlist bl;
  RGWAccessKey *swift_key;
  map<string, RGWAccessKey>::iterator siter;

  string swift_url = g_conf->rgw_swift_url;
  string swift_prefix = g_conf->rgw_swift_url_prefix;
  string tenant_path;

  if (swift_prefix.size() == 0) {
    swift_prefix = DEFAULT_SWIFT_PREFIX;
  }

  if (swift_url.size() == 0) {
    bool add_port = false;
    const char *server_port = s->info.env->get("SERVER_PORT_SECURE");
    const char *protocol;
    if (server_port) {
      add_port = (strcmp(server_port, "443") != 0);
      protocol = "https";
    } else {
      server_port = s->info.env->get("SERVER_PORT");
      add_port = (strcmp(server_port, "80") != 0);
      protocol = "http";
    }
    const char *host = s->info.env->get("HTTP_HOST");
    if (!host) {
      dout(0) << "NOTICE: server is misconfigured, missing rgw_swift_url_prefix or rgw_swift_url, HTTP_HOST is not set" << dendl;
      ret = -EINVAL;
      goto done;
    }
    swift_url = protocol;
    swift_url.append("://");
    swift_url.append(host);
    if (add_port && !strchr(host, ':')) {
      swift_url.append(":");
      swift_url.append(server_port);
    }
  }

  if (!key || !user)
    goto done;

  user_str = user;

  if ((ret = rgw_get_user_info_by_swift(store, user_str, info)) < 0)
  {
    ret = -EACCES;
    goto done;
  }

  siter = info.swift_keys.find(user_str);
  if (siter == info.swift_keys.end()) {
    ret = -EPERM;
    goto done;
  }
  swift_key = &siter->second;

  if (swift_key->key.compare(key) != 0) {
    dout(0) << "NOTICE: RGW_SWIFT_Auth_Get::execute(): bad swift key" << dendl;
    ret = -EPERM;
    goto done;
  }

  if (!g_conf->rgw_swift_tenant_name.empty()) {
    tenant_path = "/AUTH_";
    tenant_path.append(g_conf->rgw_swift_tenant_name);
  } else if (g_conf->rgw_swift_account_in_url) {
    tenant_path = "/AUTH_";
    tenant_path.append(user_str);
  }

  STREAM_IO(s)->print("X-Storage-Url: %s/%s/v1%s\r\n", swift_url.c_str(),
		swift_prefix.c_str(), tenant_path.c_str());

  if ((ret = encode_token(s->cct, swift_key->id, swift_key->key, bl)) < 0)
    goto done;

  {
    char buf[bl.length() * 2 + 1];
    buf_to_hex((const unsigned char *)bl.c_str(), bl.length(), buf);

    STREAM_IO(s)->print("X-Storage-Token: AUTH_rgwtk%s\r\n", buf);
    STREAM_IO(s)->print("X-Auth-Token: AUTH_rgwtk%s\r\n", buf);
  }

  ret = STATUS_NO_CONTENT;

done:
  set_req_state_err(s, ret);
  dump_errno(s);
  end_header(s);
}

int RGWHandler_SWIFT_Auth::init(RGWRados *store, struct req_state *state,
				RGWClientIO *cio)
{
  state->dialect = "swift-auth";
  state->formatter = new JSONFormatter;
  state->format = RGW_FORMAT_JSON;

  return RGWHandler::init(store, state, cio);
}

int RGWHandler_SWIFT_Auth::authorize()
{
  return 0;
}

RGWOp *RGWHandler_SWIFT_Auth::op_get()
{
  return new RGW_SWIFT_Auth_Get;
}

