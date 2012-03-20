#include <errno.h>

#include "rgw_common.h"
#include "rgw_acl.h"

#include "common/ceph_crypto.h"
#include "common/armor.h"
#include "common/errno.h"
#include "common/Clock.h"
#include "common/Formatter.h"
#include "common/perf_counters.h"
#include "auth/Crypto.h"

#include <sstream>

#define DOUT_SUBSYS rgw

PerfCounters *perfcounter = NULL;

int rgw_perf_start(CephContext *cct)
{
  PerfCountersBuilder plb(cct, cct->_conf->name.to_str(), l_rgw_first, l_rgw_last);

  plb.add_u64_counter(l_rgw_req, "req");
  plb.add_u64_counter(l_rgw_failed_req, "failed_req");

  plb.add_u64_counter(l_rgw_get, "get");
  plb.add_u64_counter(l_rgw_get_b, "get_b");
  plb.add_fl_avg(l_rgw_get_lat, "get_initial_lat");
  plb.add_u64_counter(l_rgw_put, "put");
  plb.add_u64_counter(l_rgw_put_b, "put_b");
  plb.add_fl_avg(l_rgw_put_lat, "put_initial_lat");

  plb.add_u64(l_rgw_qlen, "qlen");
  plb.add_u64(l_rgw_qactive, "qactive");

  plb.add_u64_counter(l_rgw_cache_hit, "cache_hit");
  plb.add_u64_counter(l_rgw_cache_miss, "cache_miss");

  perfcounter = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(perfcounter);
  return 0;
}

void rgw_perf_stop(CephContext *cct)
{
  assert(perfcounter);
  cct->get_perfcounters_collection()->remove(perfcounter);
  delete perfcounter;
}

void RGWAccessKey::generate_test_instances(list<RGWAccessKey*>& o)
{
  RGWAccessKey *k = new RGWAccessKey;
  k->id = "id";
  k->key = "key";
  k->subuser = "subuser";
  o.push_back(k);
  o.push_back(new RGWAccessKey);
}

void RGWAccessKey::dump(Formatter *f) const
{
  f->dump_string("id", id);
  f->dump_string("key", key);
  f->dump_string("subuser", subuser);
}

void RGWSubUser::generate_test_instances(list<RGWSubUser*>& o)
{
  RGWSubUser *u = new RGWSubUser;
  u->name = "name";
  u->perm_mask = 0xf;
  o.push_back(u);
  o.push_back(new RGWSubUser);
}

void RGWSubUser::dump(Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_unsigned("perm_mask", perm_mask);
}

void RGWUserInfo::generate_test_instances(list<RGWUserInfo*>& o)
{
  RGWUserInfo *i = new RGWUserInfo;
  i->auid = 1;
  i->user_id = "user_id";
  i->display_name =  "display_name";
  i->user_email = "user@email";
  RGWAccessKey k1, k2;
  k1.id = "id1";
  k1.key = "key1";
  k2.id = "id2";
  k2.subuser = "subuser";
  RGWSubUser u;
  u.name = "id2";
  u.perm_mask = 0x1;
  i->access_keys[k1.id] = k1;
  i->swift_keys[k2.id] = k2;
  i->subusers[u.name] = u;
  o.push_back(i);

  o.push_back(new RGWUserInfo);
}

void RGWUserInfo::dump(Formatter *f) const
{
  f->dump_unsigned("auid", auid);
  f->dump_string("user_id", user_id);
  f->dump_string("display_name", display_name);
  f->dump_string("user_email", user_email);

  map<string, RGWAccessKey>::const_iterator aiter = access_keys.begin();
  f->open_array_section("access_keys");
  for (; aiter != access_keys.end(); ++aiter) {
    f->open_object_section("entry");
    f->dump_string("uid", aiter->first);
    f->open_object_section("access_key");
    aiter->second.dump(f);
    f->close_section();
    f->close_section();
  }

  aiter = swift_keys.begin();
  for (; aiter != swift_keys.end(); ++aiter) {
    f->open_object_section("entry");
    f->dump_string("subuser", aiter->first);
    f->open_object_section("key");
    aiter->second.dump(f);
    f->close_section();
    f->close_section();
  }
  map<string, RGWSubUser>::const_iterator siter = subusers.begin();
  for (; siter != subusers.end(); ++siter) {
    f->open_object_section("entry");
    f->dump_string("id", siter->first);
    f->open_object_section("subuser");
    siter->second.dump(f);
    f->close_section();
    f->close_section();
  }
  f->dump_int("suspended", (int)suspended);
}

void rgw_bucket::generate_test_instances(list<rgw_bucket*>& o)
{
  rgw_bucket *b = new rgw_bucket("name", "pool", "marker", "123");
  o.push_back(b);
  o.push_back(new rgw_bucket);
}

void rgw_bucket::dump(Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_string("pool", pool);
  f->dump_string("marker", marker);
  f->dump_string("bucket_id", bucket_id);
}

void RGWBucketInfo::generate_test_instances(list<RGWBucketInfo*>& o)
{
  RGWBucketInfo *i = new RGWBucketInfo;
  i->bucket = rgw_bucket("bucket", "pool", "marker", "10");
  i->owner = "owner";
  i->flags = BUCKET_SUSPENDED;
  o.push_back(i);
  o.push_back(new RGWBucketInfo);
}

void RGWBucketInfo::dump(Formatter *f) const
{
  f->open_object_section("bucket");
  bucket.dump(f);
  f->close_section();
  f->dump_string("owner", owner);
  f->dump_unsigned("flags", flags);
}

void RGWBucketEnt::generate_test_instances(list<RGWBucketEnt*>& o)
{
  RGWBucketEnt *e = new RGWBucketEnt;
  e->bucket = rgw_bucket("bucket", "pool", "marker", "10");
  e->size = 1024;
  e->size_rounded = 4096;
  e->count = 1;
  o.push_back(e);
  o.push_back(new RGWBucketEnt);
}

void RGWBucketEnt::dump(Formatter *f) const
{
  f->open_object_section("bucket");
  bucket.dump(f);
  f->close_section();
  f->dump_unsigned("size", size);
  f->dump_unsigned("size_rounded", size_rounded);
  f->dump_stream("mtime") << mtime;
  f->dump_unsigned("count", count);
}

void RGWUploadPartInfo::generate_test_instances(list<RGWUploadPartInfo*>& o)
{
  RGWUploadPartInfo *i = new RGWUploadPartInfo;
  i->num = 1;
  i->size = 10 * 1024 * 1024;
  i->etag = "etag";
  o.push_back(i);
  o.push_back(new RGWUploadPartInfo);
}

void RGWUploadPartInfo::dump(Formatter *f) const
{
  f->dump_unsigned("num", num);
  f->dump_unsigned("size", size);
  f->dump_string("etag", etag);
  f->dump_stream("modified") << modified;
}

void rgw_obj::generate_test_instances(list<rgw_obj*>& o)
{
  rgw_bucket b = rgw_bucket("bucket", "pool", "marker", "10");
  rgw_obj *obj = new rgw_obj(b, "object");
  o.push_back(obj);
  o.push_back(new rgw_obj);
}

void rgw_obj::dump(Formatter *f) const
{
  f->open_object_section("bucket");
  bucket.dump(f);
  f->close_section();
  f->dump_string("key", key);
  f->dump_string("ns", ns);
  f->dump_string("object", object);
}


using namespace ceph::crypto;

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


req_state::req_state(CephContext *_cct, struct RGWEnv *e) : cct(_cct), os_auth_token(NULL), os_user(NULL), os_groups(NULL), env(e)
{
  should_log = env->conf->should_log;
  content_started = false;
  format = 0;
  bucket_acl = NULL;
  object_acl = NULL;
  expect_cont = false;

  os_auth_token = NULL;
  os_user = NULL;
  os_groups = NULL;
  time = ceph_clock_now(cct);
  perm_mask = 0;
  content_length = 0;
  object = NULL;
  bucket_name = NULL;
  has_bad_meta = false;
  method = NULL;
  host_bucket = NULL;
}

req_state::~req_state() {
  delete formatter;
  delete bucket_acl;
  delete object_acl;
  free(os_user);
  free(os_groups);
  free((void *)object);
  free((void *)bucket_name);
}

std::ostream& operator<<(std::ostream& oss, const rgw_err &err)
{
  oss << "rgw_err(http_ret=" << err.http_ret << ", s3='" << err.s3_code << "') ";
  return oss;
}

static bool check_str_end(const char *s)
{
  if (!s)
    return false;

  while (*s) {
    if (!isspace(*s))
      return false;
    s++;
  }
  return true;
}

static bool parse_rfc850(const char *s, struct tm *t)
{
  memset(t, 0, sizeof(*t));
  return check_str_end(strptime(s, "%A, %d-%b-%y %H:%M:%S GMT", t));
}

static bool parse_asctime(const char *s, struct tm *t)
{
  memset(t, 0, sizeof(*t));
  return check_str_end(strptime(s, "%a %b %d %H:%M:%S %Y", t));
}

static bool parse_rfc1123(const char *s, struct tm *t)
{
  memset(t, 0, sizeof(*t));
  return check_str_end(strptime(s, "%a, %d %b %Y %H:%M:%S GMT", t));
}

static bool parse_rfc1123_alt(const char *s, struct tm *t)
{
  memset(t, 0, sizeof(*t));
  return check_str_end(strptime(s, "%a, %d %b %Y %H:%M:%S %z", t));
}

bool parse_rfc2616(const char *s, struct tm *t)
{
  return parse_rfc850(s, t) || parse_asctime(s, t) || parse_rfc1123(s, t) || parse_rfc1123_alt(s,t);
}

int parse_time(const char *time_str, time_t *time)
{
  struct tm tm;

  if (!parse_rfc2616(time_str, &tm))
    return -EINVAL;

  *time = timegm(&tm);

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
}

int gen_rand_base64(CephContext *cct, char *dest, int size) /* size should be the required string size + 1 */
{
  char buf[size];
  char tmp_dest[size + 4]; /* so that there's space for the extra '=' characters, and some */
  int ret;

  ret = get_random_bytes(buf, sizeof(buf));
  if (ret < 0) {
    lderr(cct) << "cannot get random bytes: " << cpp_strerror(-ret) << dendl;
    return -1;
  }

  ret = ceph_armor(tmp_dest, &tmp_dest[sizeof(tmp_dest)],
		   (const char *)buf, ((const char *)buf) + ((size - 1) * 3 + 4 - 1) / 4);
  if (ret < 0) {
    lderr(cct) << "ceph_armor failed" << dendl;
    return -1;
  }
  tmp_dest[ret] = '\0';
  memcpy(dest, tmp_dest, size);
  dest[size] = '\0';

  return 0;
}

static const char alphanum_upper_table[]="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

int gen_rand_alphanumeric_upper(CephContext *cct, char *dest, int size) /* size should be the required string size + 1 */
{
  int ret = get_random_bytes(dest, size);
  if (ret < 0) {
    lderr(cct) << "cannot get random bytes: " << cpp_strerror(-ret) << dendl;
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

int gen_rand_alphanumeric(CephContext *cct, char *dest, int size) /* size should be the required string size + 1 */
{
  int ret = get_random_bytes(dest, size);
  if (ret < 0) {
    lderr(cct) << "cannot get random bytes: " << cpp_strerror(-ret) << dendl;
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
    string substr, nameval;
    substr = str.substr(pos, fpos - pos);
    url_decode(substr, nameval);
    NameVal nv(nameval);
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

bool verify_bucket_permission(struct req_state *s, int perm)
{
  if (!s->bucket_acl)
    return false;

  if ((perm & (int)s->perm_mask) != perm)
    return false;

  if (s->bucket_acl->verify_permission(s->user.user_id, perm, perm))
    return true;

  if (perm & (RGW_PERM_READ | RGW_PERM_READ_ACP))
    perm |= RGW_PERM_READ_OBJS;
  if (perm & RGW_PERM_WRITE)
    perm |= RGW_PERM_WRITE_OBJS;

  return s->bucket_acl->verify_permission(s->user.user_id, perm, perm);
}

bool verify_object_permission(struct req_state *s, int perm)
{
  if (!s->object_acl)
    return false;

  bool ret = s->object_acl->verify_permission(s->user.user_id, s->perm_mask, perm);
  if (ret)
    return true;

  if (!s->cct->_conf->rgw_enforce_swift_acls)
    return ret;

  if ((perm & (int)s->perm_mask) != perm)
    return false;

  int swift_perm = 0;
  if (perm & (RGW_PERM_READ | RGW_PERM_READ_ACP))
    swift_perm |= RGW_PERM_READ_OBJS;
  if (perm & RGW_PERM_WRITE)
    swift_perm |= RGW_PERM_WRITE_OBJS;

  if (!swift_perm)
    return false;
  /* we already verified the user mask above, so we pass swift_perm as the mask here,
     otherwise the mask might not cover the swift permissions bits */
  return s->bucket_acl->verify_permission(s->user.user_id, swift_perm, swift_perm);
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
  const char *src = src_str.c_str();
  char dest[src_str.size()];
  int pos = 0;
  char c;

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
