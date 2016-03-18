// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_swift_auth.h"
#include "rgw_rest.h"

#include "common/ceph_crypto.h"
#include "common/Clock.h"

#include "auth/Crypto.h"

#include "rgw_client_io.h"

#define dout_subsys ceph_subsys_rgw

#define DEFAULT_SWIFT_PREFIX "swift"

using namespace ceph::crypto;

static int build_token(string& swift_user, string& key, uint64_t nonce,
		       utime_t& expiration, bufferlist& bl)
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

int rgw_swift_verify_signed_token(CephContext *cct, RGWRados *store,
				  const char *token, RGWUserInfo& info,
				  string *pswift_user)
{
  if (strncmp(token, "AUTH_rgwtk", 10) != 0)
    return -EINVAL;

  token += 10;

  int len = strlen(token);
  if (len & 1) {
    dout(0) << "NOTICE: failed to verify token: invalid token length len="
	    << len << dendl;
    return -EINVAL;
  }

  bufferptr p(len/2);
  int ret = hex_to_buf(token, p.c_str(), len);
  if (ret < 0)
    return ret;

  bufferlist bl;
  bl.append(p);

  bufferlist::iterator iter = bl.begin();

  uint64_t nonce;
  utime_t expiration;
  string swift_user;

  try {
    ::decode(swift_user, iter);
    ::decode(nonce, iter);
    ::decode(expiration, iter);
  } catch (buffer::error& err) {
    dout(0) << "NOTICE: failed to decode token: caught exception" << dendl;
    return -EINVAL;
  }
  utime_t now = ceph_clock_now(cct);
  if (expiration < now) {
    dout(0) << "NOTICE: old timed out token was used now=" << now
	    << " token.expiration=" << expiration << dendl;
    return -EPERM;
  }

  if ((ret = rgw_get_user_info_by_swift(store, swift_user, info)) < 0)
    return ret;

  dout(10) << "swift_user=" << swift_user << dendl;

  map<string, RGWAccessKey>::iterator siter = info.swift_keys.find(swift_user);
  if (siter == info.swift_keys.end())
    return -EPERM;
  RGWAccessKey& swift_key = siter->second;

  bufferlist tok;
  ret = build_token(swift_user, swift_key.key, nonce, expiration, tok);
  if (ret < 0)
    return ret;

  if (tok.length() != bl.length()) {
    dout(0) << "NOTICE: tokens length mismatch: bl.length()=" << bl.length()
	    << " tok.length()=" << tok.length() << dendl;
    return -EPERM;
  }

  if (memcmp(tok.c_str(), bl.c_str(), tok.length()) != 0) {
    char buf[tok.length() * 2 + 1];
    buf_to_hex((const unsigned char *)tok.c_str(), tok.length(), buf);
    dout(0) << "NOTICE: tokens mismatch tok=" << buf << dendl;
    return -EPERM;
  }
  *pswift_user = swift_user;

  return 0;
}

void RGW_SWIFT_Auth_Get::execute()
{
  int ret = -EPERM;

  const char *key = s->info.env->get("HTTP_X_AUTH_KEY");
  const char *user = s->info.env->get("HTTP_X_AUTH_USER");

  s->prot_flags |= RGW_REST_SWIFT;

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

