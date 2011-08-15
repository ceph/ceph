#include <errno.h>

#include "rgw_common.h"
#include "rgw_acl.h"

#include "common/ceph_crypto.h"
#include "common/armor.h"
#include "common/errno.h"
#include "common/Clock.h"
#include "common/Formatter.h"
#include "auth/Crypto.h"

#include <sstream>

using namespace ceph::crypto;

string rgw_obj_category_main = "rgw.main";
string rgw_obj_category_shadow = "rgw.shadow";
string rgw_obj_category_multimeta = "rgw.multimeta";
string rgw_obj_category_none;

rgw_err::
rgw_err()
{
  clear();
}

rgw_err::
rgw_err(int http, const std::string& s3)
    : http_ret(http), s3_code(s3)
{
}

void rgw_err::
clear()
{
  http_ret = 200;
  ret = 0;
  s3_code.clear();
}

bool rgw_err::
is_clear() const
{
  return (http_ret == 200);
}

bool rgw_err::
is_err() const
{
  return !(http_ret >= 200 && http_ret <= 299);
}


req_state::req_state(struct RGWEnv *e) : acl(NULL), os_auth_token(NULL), os_user(NULL), os_groups(NULL), env(e)
{
  should_log = env->conf->should_log;
  content_started = false;
  format = 0;
  acl = new RGWAccessControlPolicy;
  expect_cont = false;

  os_auth_token = NULL;
  os_user = NULL;
  os_groups = NULL;
  time = ceph_clock_now(g_ceph_context);
  perm_mask = 0;
  content_length = 0;
  object = NULL;
  bucket = NULL;
}

req_state::~req_state() {
  delete formatter;
  free(os_user);
  free(os_groups);
  free((void *)object);
  free((void *)bucket);
}

void flush_formatter_to_req_state(struct req_state *s, Formatter *formatter)
{
  std::ostringstream oss;
  formatter->flush(oss);
  std::string outs(oss.str());
  if (!outs.empty()) {
    CGI_PutStr(s, outs.c_str(), outs.size());
  }
  s->formatter->reset();
}

std::ostream& operator<<(std::ostream& oss, const rgw_err &err)
{
  oss << "rgw_err(http_ret=" << err.http_ret << ", s3='" << err.s3_code << "') ";
  return oss;
}

int parse_time(const char *time_str, time_t *time)
{
  struct tm tm;
  memset(&tm, 0, sizeof(struct tm));
  if (!strptime(time_str, "%a, %d %b %Y %H:%M:%S %Z", &tm))
    return -EINVAL;

  *time = mktime(&tm);

  return 0;
}

/*
 * calculate the sha1 value of a given msg and key
 */
void calc_hmac_sha1(const char *key, int key_len,
                    const char *msg, int msg_len, char *dest)
/* destination should be CEPH_CRYPTO_HMACSHA1_DIGESTSIZE bytes long */
{
  HMACSHA1 hmac((const unsigned char *)key, key_len);
  hmac.Update((const unsigned char *)msg, msg_len);
  hmac.Final((unsigned char *)dest);
  
  char hex_str[(CEPH_CRYPTO_HMACSHA1_DIGESTSIZE * 2) + 1];
  buf_to_hex((unsigned char *)dest, CEPH_CRYPTO_HMACSHA1_DIGESTSIZE, hex_str);

  RGW_LOG(15) << "hmac=" << hex_str << dendl;
}

int gen_rand_base64(char *dest, int size) /* size should be the required string size + 1 */
{
  char buf[size];
  char tmp_dest[size + 4]; /* so that there's space for the extra '=' characters, and some */
  int ret;

  ret = get_random_bytes(buf, sizeof(buf));
  if (ret < 0) {
    cerr << "cannot get random bytes: " << cpp_strerror(-ret) << std::endl;
    return -1;
  }

  ret = ceph_armor(tmp_dest, &tmp_dest[sizeof(tmp_dest)],
		   (const char *)buf, ((const char *)buf) + ((size - 1) * 3 + 4 - 1) / 4);
  if (ret < 0) {
    cerr << "ceph_armor failed" << std::endl;
    return -1;
  }
  tmp_dest[ret] = '\0';
  memcpy(dest, tmp_dest, size);
  dest[size] = '\0';

  return 0;
}

static const char alphanum_upper_table[]="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

int gen_rand_alphanumeric_upper(char *dest, int size) /* size should be the required string size + 1 */
{
  int ret = get_random_bytes(dest, size);
  if (ret < 0) {
    cerr << "cannot get random bytes: " << cpp_strerror(-ret) << std::endl;
    return -1;
  }

  int i;
  for (i=0; i<size - 1; i++) {
    int pos = (unsigned)dest[i];
    dest[i] = alphanum_upper_table[pos % (sizeof(alphanum_upper_table) - 1)];
  }
  dest[i] = '\0';

  return 0;
}


// this is basically a modified base64 charset, url friendly
static const char alphanum_table[]="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

int gen_rand_alphanumeric(char *dest, int size) /* size should be the required string size + 1 */
{
  int ret = get_random_bytes(dest, size);
  if (ret < 0) {
    cerr << "cannot get random bytes: " << cpp_strerror(-ret) << std::endl;
    return -1;
  }

  int i;
  for (i=0; i<size - 1; i++) {
    int pos = (unsigned)dest[i];
    dest[i] = alphanum_table[pos & 63];
  }
  dest[i] = '\0';

  return 0;
}

int NameVal::parse()
{
  int delim_pos = str.find('=');
  int ret = 0;

  if (delim_pos < 0) {
    name = str;
    val = "";
    ret = 1;
  } else {
    name = str.substr(0, delim_pos);
    val = str.substr(delim_pos + 1);
  }

  RGW_LOG(10) << "parsed: name=" << name << " val=" << val << dendl;
  return ret; 
}

int XMLArgs::parse()
{
  int pos = 0, fpos;
  bool end = false;
  if (str[pos] == '?') pos++;

  while (!end) {
    fpos = str.find('&', pos);
    if (fpos  < pos) {
       end = true;
       fpos = str.size(); 
    }
    NameVal nv(str.substr(pos, fpos - pos));
    int ret = nv.parse();
    if (ret >= 0) {
      val_map[nv.get_name()] = nv.get_val();

      if ((nv.get_name().compare("acl") == 0) ||
          (nv.get_name().compare("location") == 0) ||
          (nv.get_name().compare("uploads") == 0) ||
          (nv.get_name().compare("partNumber") == 0) ||
          (nv.get_name().compare("uploadId") == 0) ||
          (nv.get_name().compare("versionid") == 0) ||
          (nv.get_name().compare("torrent") == 0)) {
        sub_resources[nv.get_name()] = nv.get_val();
      }
    }

    pos = fpos + 1;  
  }

  return 0;
}

string& XMLArgs::get(string& name)
{
  map<string, string>::iterator iter;
  iter = val_map.find(name);
  if (iter == val_map.end())
    return empty_str;
  return iter->second;
}

string& XMLArgs::get(const char *name)
{
  string s(name);
  return get(s);
}

bool verify_permission(RGWAccessControlPolicy *policy, string& uid, int user_perm_mask, int perm)
{
   if (!policy)
     return false;

   int acl_perm = policy->get_perm(g_ceph_context, uid, perm) & user_perm_mask;

   return (perm == acl_perm);
}

bool verify_permission(struct req_state *s, int perm)
{
  return verify_permission(s->acl, s->user.user_id, s->perm_mask, perm);
}

static char hex_to_num(char c)
{
  static char table[256];
  static bool initialized = false;


  if (!initialized) {
    memset(table, -1, sizeof(table));
    int i;
    for (i = '0'; i<='9'; i++)
      table[i] = i - '0';
    for (i = 'A'; i<='F'; i++)
      table[i] = i - 'A' + 0xa;
    for (i = 'a'; i<='f'; i++)
      table[i] = i - 'a' + 0xa;
  }
  return table[(int)c];
}

bool url_decode(string& src_str, string& dest_str)
{
  RGW_LOG(10) << "in url_decode with " << src_str << dendl;
  const char *src = src_str.c_str();
  char dest[src_str.size()];
  int pos = 0;
  char c;

  RGW_LOG(10) << "src=" << (void *)src << dendl;

  while (*src) {
    if (*src != '%') {
      if (*src != '+') {
	dest[pos++] = *src++;
      } else {
	dest[pos++] = ' ';
	++src;
      }
    } else {
      src++;
      if (!*src)
        break;
      char c1 = hex_to_num(*src++);
      if (!*src)
        break;
      c = c1 << 4;
      if (c1 < 0)
        return false;
      c1 = hex_to_num(*src++);
      if (c1 < 0)
        return false;
      c |= c1;
      dest[pos++] = c;
    }
  }
  dest[pos] = 0;
  dest_str = dest;

  return true;
}
