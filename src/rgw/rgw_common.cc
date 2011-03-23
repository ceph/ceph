#include <errno.h>

#include "rgw_common.h"
#include "rgw_acl.h"

#include "common/ceph_crypto.h"

using namespace ceph::crypto;

/* Loglevel of the gateway */
int rgw_log_level = 20;

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
int calc_hmac_sha1(const char *key, int key_len,
                   const char *msg, int msg_len,
                   char *dest, int *len) /* dest should be large enough to hold result */
{
  if (*len < CEPH_CRYPTO_HMACSHA1_DIGESTSIZE)
    return -EINVAL;

  char hex_str[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE * 2 + 1];
  char key_buf[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE];
  key_len = max(key_len, CEPH_CRYPTO_HMACSHA1_DIGESTSIZE);
  memcpy(key_buf, key, key_len);
  memset(key_buf + key_len, 0, CEPH_CRYPTO_HMACSHA1_DIGESTSIZE - key_len);

  HMACSHA1 hmac((const unsigned char *)key, key_len);
  hmac.Update((const unsigned char *)msg, msg_len);
  hmac.Final((unsigned char *)dest);
  *len = CEPH_CRYPTO_HMACSHA1_DIGESTSIZE;
  
  buf_to_hex((unsigned char *)dest, *len, hex_str);

  RGW_LOG(15) << "hmac=" << hex_str << endl;

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

  RGW_LOG(10) << "parsed: name=" << name << " val=" << val << endl;
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

      if (ret > 0) { /* this might be a sub-resource */
        if ((nv.get_name().compare("acl") == 0) ||
            (nv.get_name().compare("location") == 0) ||
            (nv.get_name().compare("torrent") == 0))
          sub_resource = nv.get_name();
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

bool verify_permission(RGWAccessControlPolicy *policy, string& uid, int perm)
{
   if (!policy)
     return false;

   int acl_perm = policy->get_perm(uid, perm);

   return (perm == acl_perm);
}

bool verify_permission(struct req_state *s, int perm)
{
  return verify_permission(s->acl, s->user.user_id, perm);
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
  RGW_LOG(10) << "in url_decode with " << src_str << endl;
  const char *src = src_str.c_str();
  char dest[src_str.size()];
  int pos = 0;
  char c;

  RGW_LOG(10) << "src=" << (void *)src << std::endl;

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
      char c1 = hex_to_num(*src++);
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

void RGWFormatter::write_data(const char *fmt, ...)
{
#define LARGE_ENOUGH_LEN 128
  int n, size = LARGE_ENOUGH_LEN;
  char s[size];
  char *p, *np;
  bool p_on_stack;
  va_list ap;
  int pos;

  p = s;
  p_on_stack = true;

  while (1) {
    va_start(ap, fmt);
    n = vsnprintf(p, size, fmt, ap);
    va_end(ap);

    if (n > -1 && n < size)
      goto done;
    /* Else try again with more space. */
    if (n > -1)    /* glibc 2.1 */
      size = n+1; /* precisely what is needed */
    else           /* glibc 2.0 */
      size *= 2;  /* twice the old size */
    if (p_on_stack)
      np = (char *)malloc(size);
    else
      np = (char *)realloc(p, size);
    if (!np)
      goto done_free;
    p = np;
    p_on_stack = false;
  }
done:
#define LARGE_ENOUGH_BUF 4096
  if (!buf) {
    max_len = max(LARGE_ENOUGH_BUF, size);
    buf = (char *)malloc(max_len);
  }

  if (len + size > max_len) {
    max_len = len + size + LARGE_ENOUGH_BUF;
    buf = (char *)realloc(buf, max_len);
  }
  if (!buf) {
    RGW_LOG(0) << "RGWFormatter::write_data: failed allocating " << max_len << " bytes" << std::endl;
    goto done_free;
  }
  pos = len;
  if (len)
    pos--; // squash null termination
  strcpy(buf + pos, p);
  len = pos + strlen(p) + 1;
  RGW_LOG(0) << "RGWFormatter::write_data: len= " << len << " bytes" << std::endl;
done_free:
  if (!p_on_stack)
    free(p);
}

void RGWFormatter::flush()
{
  if (!buf)
    return;

  RGW_LOG(0) << "flush(): buf='" << buf << "'  strlen(buf)=" << strlen(buf) << std::endl;
  FCGX_PutStr(buf, len - 1, s->fcgx->out);
  free(buf);
  buf = NULL;
  len = 0;
  max_len = 0;
}
