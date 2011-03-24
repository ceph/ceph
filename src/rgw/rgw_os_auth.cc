#include "rgw_os_auth.h"
#include "rgw_rest.h"

#include "common/ceph_crypto.h"
#include "common/Clock.h"

#include "auth/Crypto.h"

using namespace ceph::crypto;

static RGW_OS_Auth_Get rgw_os_auth_get;

static int build_token(string& os_user, string& key, uint64_t nonce, utime_t& expiration, bufferlist& bl)
{
  ::encode(os_user, bl);
  ::encode(nonce, bl);
  ::encode(expiration, bl);

  bufferptr p(CEPH_CRYPTO_HMACSHA1_DIGESTSIZE);

  char buf[bl.length() * 2 + 1];
  buf_to_hex((const unsigned char *)bl.c_str(), bl.length(), buf);
  RGW_LOG(0) << "bl=" << buf << std::endl;

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

static int encode_token(string& os_user, string& key, bufferlist& bl)
{
  uint64_t nonce;

  int ret = get_random_bytes((char *)&nonce, sizeof(nonce));
  if (ret < 0)
    return ret;

  utime_t expiration = g_clock.now();
  expiration += RGW_OS_TOKEN_EXPIRATION; // 15 minutes

  ret = build_token(os_user, key, nonce, expiration, bl);

  return ret;
}

int hexdigit(char c)
{
  if (c >= '0' && c <= '9')
    return (c - '0');
  c = toupper(c);
  if (c >= 'A' && c <= 'F')
    return c - 'A' + 0xa;
  return -EINVAL;
}

int hex_to_buf(const char *hex, char *buf, int len)
{
  int i = 0;
  const char *p = hex;
  while (*p) {
    if (i >= len)
      return -EINVAL;
    buf[i] = 0;
    int d = hexdigit(*p);
    if (d < 0)
      return d;
    buf[i] = d << 4;
    p++;
    if (!*p)
      return -EINVAL;
    d = hexdigit(*p);
    if (d < 0)
      return -d;
    buf[i] += d;
    i++;
    p++;
  }
  return i;
}

int rgw_os_verify_signed_token(const char *token, RGWUserInfo& info)
{
  if (strncmp(token, "AUTH_rgwtk", 10) != 0)
    return -EINVAL;

  token += 10;

  int len = strlen(token);
  if (len & 1) {
    RGW_LOG(0) << "invalid token length" << std::endl;
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
  string os_user;
  string s3_user;

  try {
    ::decode(os_user, iter);
    ::decode(nonce, iter);
    ::decode(expiration, iter);
  } catch (buffer::error *err) {
    RGW_LOG(0) << "failed to decode token" << std::endl;
    return -EINVAL;
  }
  if (expiration < g_clock.now()) {
    RGW_LOG(0) << "old timed out token was used now=" << g_clock.now() << " token.expiration=" << expiration << std::endl;
    return -EPERM;
  }

  if ((ret = rgw_get_uid_by_openstack(os_user, s3_user, info)) < 0)
    return ret;

  RGW_LOG(0) << "os_user=" << os_user << std::endl;

  bufferlist tok;
  ret = build_token(os_user, info.openstack_key, nonce, expiration, tok);
  if (ret < 0)
    return ret;

  if (tok.length() != bl.length()) {
    RGW_LOG(0) << "tokens length mismatch: bl.length()=" << bl.length() << " tok.length()=" << tok.length() << std::endl;
    return -EPERM;
  }

  if (memcmp(tok.c_str(), bl.c_str(), tok.length()) != 0) {
    char buf[tok.length() * 2 + 1];
    buf_to_hex((const unsigned char *)tok.c_str(), tok.length(), buf);
    RGW_LOG(0) << "tokens mismatch tok=" << buf << std::endl;
    return -EPERM;
  }

  return 0;
}

void RGW_OS_Auth_Get::execute()
{
  int ret = -EPERM;

  RGW_LOG(0) << "RGW_OS_Auth_Get::execute()" << std::endl;

  const char *key = FCGX_GetParam("HTTP_X_AUTH_KEY", s->fcgx->envp);
  const char *user = FCGX_GetParam("HTTP_X_AUTH_USER", s->fcgx->envp);
  const char *url_prefix = FCGX_GetParam("RGW_OPENSTACK_URL_PREFIX", s->fcgx->envp);
  const char *os_url = FCGX_GetParam("RGW_OPENSTACK_URL", s->fcgx->envp);

  string user_str = user;
  string user_id;
  RGWUserInfo info;
  bufferlist bl;


  if (!key || !user)
    goto done;

  if ((ret = rgw_get_uid_by_openstack(user_str, user_id, info)) < 0)
    goto done;

  if (info.openstack_key.compare(key) != 0) {
    RGW_LOG(0) << "RGW_OS_Auth_Get::execute(): bad openstack key" << std::endl;
    ret = -EPERM;
    goto done;
  }

  CGI_PRINTF(s, "X-Storage-Url: %s/%s/v1\n", os_url, url_prefix);

  if ((ret = encode_token(info.openstack_name, info.openstack_key, bl)) < 0)
    goto done;

  {
    char buf[bl.length() * 2 + 1];
    buf_to_hex((const unsigned char *)bl.c_str(), bl.length(), buf);

    CGI_PRINTF(s, "X-Storage-Token: AUTH_rgwtk%s\n", buf);
  }

  ret = 204;

done:
  dump_errno(s, ret);
  end_header(s);

}

bool RGWHandler_OS_Auth::authorize(struct req_state *s)
{
  return true;
}

RGWOp *RGWHandler_OS_Auth::get_op()
{
  RGWOp *op;
  switch (s->op) {
   case OP_GET:
     op = &rgw_os_auth_get;
     break;
   default:
     return NULL;
  }

  if (op) {
    op->init(s);
  }
  return op;
}

