// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_RGW_COMMON_H
#define CEPH_RGW_COMMON_H

#include "common/ceph_crypto.h"
#include "common/debug.h"
#include "fcgiapp.h"

#include <errno.h>
#include <string.h>
#include <string>
#include <map>
#include "include/types.h"
#include "include/utime.h"

using namespace std;

namespace ceph {
  class Formatter;
}

using ceph::crypto::MD5;

extern string rgw_root_bucket;

extern string rgw_obj_category_main;
extern string rgw_obj_category_shadow;
extern string rgw_obj_category_multimeta;
extern string rgw_obj_category_none;

#define RGW_ROOT_BUCKET ".rgw"

#define RGW_CONTROL_BUCKET ".rgw.control"

#define RGW_ATTR_PREFIX  "user.rgw."

#define RGW_ATTR_ACL		RGW_ATTR_PREFIX "acl"
#define RGW_ATTR_ETAG    	RGW_ATTR_PREFIX "etag"
#define RGW_ATTR_BUCKETS	RGW_ATTR_PREFIX "buckets"
#define RGW_ATTR_META_PREFIX	RGW_ATTR_PREFIX "x-amz-meta-"
#define RGW_ATTR_CONTENT_TYPE	RGW_ATTR_PREFIX "content_type"
#define RGW_ATTR_ID_TAG    	RGW_ATTR_PREFIX "idtag"
#define RGW_ATTR_SHADOW_OBJ    	RGW_ATTR_PREFIX "shadow_name"

#define RGW_BUCKETS_OBJ_PREFIX ".buckets"

#define USER_INFO_VER 7

#define RGW_MAX_CHUNK_SIZE	(512*1024)
#define RGW_MAX_PENDING_CHUNKS  16

#define RGW_LOG_BEGIN "RADOS S3 Gateway:"
#define RGW_LOG(x) pdout(x, g_conf->rgw_log)
#define LRGW_LOG(cct, x) lpdout(cct, x, cct->_conf->rgw_log)

#define RGW_FORMAT_XML          1
#define RGW_FORMAT_JSON         2

#define RGW_REST_OPENSTACK      0x1
#define RGW_REST_OPENSTACK_AUTH 0x2

#define RGW_SUSPENDED_USER_AUID (uint64_t)-2

#define CGI_PRINTF(state, format, ...) do { \
   int __ret = FCGX_FPrintF(state->fcgx->out, format, __VA_ARGS__); \
   if (state->header_ended) \
     state->bytes_sent += __ret; \
   int l = 32, n; \
   while (1) { \
     char __buf[l]; \
     n = snprintf(__buf, sizeof(__buf), format, __VA_ARGS__); \
     if (n != l) \
       RGW_LOG(0) << "--> " << __buf << dendl; \
       break; \
     l *= 2; \
   } \
} while (0)

#define CGI_PutStr(state, buf, len) do { \
  FCGX_PutStr(buf, len, state->fcgx->out); \
  if (state->header_ended) \
    state->bytes_sent += len; \
} while (0)

#define CGI_GetStr(state, buf, buf_len, olen) do { \
  olen = FCGX_GetStr(buf, buf_len, state->fcgx->in); \
  state->bytes_received += olen; \
} while (0)

#define ERR_INVALID_BUCKET_NAME 2000
#define ERR_INVALID_OBJECT_NAME 2001
#define ERR_NO_SUCH_BUCKET      2002
#define ERR_METHOD_NOT_ALLOWED  2003
#define ERR_INVALID_DIGEST      2004
#define ERR_BAD_DIGEST          2005
#define ERR_UNRESOLVABLE_EMAIL  2006
#define ERR_INVALID_PART        2007
#define ERR_INVALID_PART_ORDER  2008
#define ERR_NO_SUCH_UPLOAD      2009
#define ERR_REQUEST_TIMEOUT     2010
#define ERR_LENGTH_REQUIRED     2011
#define ERR_REQUEST_TIME_SKEWED 2012
#define ERR_USER_SUSPENDED      2100

typedef void *RGWAccessHandle;

 /* size should be the required string size + 1 */
extern int gen_rand_base64(char *dest, int size);
extern int gen_rand_alphanumeric(char *dest, int size);
extern int gen_rand_alphanumeric_upper(char *dest, int size);

enum RGWIntentEvent {
  DEL_OBJ,
};

/** Store error returns for output at a different point in the program */
struct rgw_err {
  rgw_err();
  rgw_err(int http, const std::string &s3);
  void clear();
  bool is_clear() const;
  bool is_err() const;
  friend std::ostream& operator<<(std::ostream& oss, const rgw_err &err);

  int http_ret;
  int ret;
  std::string s3_code;
  std::string message;
};

/* Helper class used for XMLArgs parsing */
class NameVal
{
   string str;
   string name;
   string val;
 public:
    NameVal(string nv) : str(nv) {}

    int parse();

    string& get_name() { return name; }
    string& get_val() { return val; }
};

/** Stores the XML arguments associated with the HTTP request in req_state*/
class XMLArgs
{
  string str, empty_str;
  map<string, string> val_map;
  map<string, string> sub_resources;
 public:
   XMLArgs() {}
   XMLArgs(string s) : str(s) {}
   /** Set the arguments; as received */
   void set(string s) { val_map.clear(); sub_resources.clear(); str = s; }
   /** parse the received arguments */
   int parse();
   /** Get the value for a specific argument parameter */
   string& get(string& name);
   string& get(const char *name);
   /** see if a parameter is contained in this XMLArgs */
   bool exists(const char *name) {
     map<string, string>::iterator iter = val_map.find(name);
     return (iter != val_map.end());
   }
   map<string, string>& get_sub_resources() { return sub_resources; }
};

class RGWConf;

class RGWEnv {
  std::map<string, string> env_map;
public:
  RGWConf *conf; 

  RGWEnv();
  ~RGWEnv();
  void init(char **envp);
  const char *get(const char *name, const char *def_val = NULL);
  int get_int(const char *name, int def_val = 0);
  bool get_bool(const char *name, bool def_val = 0);
  size_t get_size(const char *name, size_t def_val = 0);
};

class RGWConf {
  friend class RGWEnv;
protected:
  void init(RGWEnv * env);
public:
  RGWConf() :
    log_level(0),
    should_log(1) {}

  int log_level;
  int should_log;
};

enum http_op {
  OP_GET,
  OP_PUT,
  OP_DELETE,
  OP_HEAD,
  OP_POST,
  OP_UNKNOWN,
};

class RGWAccessControlPolicy;

struct RGWAccessKey {
  string id;
  string key;
  string subuser;

  RGWAccessKey() {}
  void encode(bufferlist& bl) const {
     __u32 ver = 1;
    ::encode(ver, bl);
    ::encode(id, bl);
    ::encode(key, bl);
    ::encode(subuser, bl);
  }

  void decode(bufferlist::iterator& bl) {
     __u32 ver;
     ::decode(ver, bl);
     ::decode(id, bl);
     ::decode(key, bl);
     ::decode(subuser, bl);
  }
};
WRITE_CLASS_ENCODER(RGWAccessKey);

struct RGWSubUser {
  string name;
  uint32_t perm_mask;

  RGWSubUser() : perm_mask(0) {}
  void encode(bufferlist& bl) const {
     __u32 ver = 1;
    ::encode(ver, bl);
    ::encode(name, bl);
    ::encode(perm_mask, bl);
  }

  void decode(bufferlist::iterator& bl) {
     __u32 ver;
     ::decode(ver, bl);
     ::decode(name, bl);
     ::decode(perm_mask, bl);
  }
};
WRITE_CLASS_ENCODER(RGWSubUser);


struct RGWUserInfo
{
  uint64_t auid;
  string user_id;
  string display_name;
  string user_email;
  string openstack_name;
  string openstack_key;
  map<string, RGWAccessKey> access_keys;
  map<string, RGWSubUser> subusers;
  __u8 suspended;

  RGWUserInfo() : auid(0), suspended(0) {}

  void encode(bufferlist& bl) const {
     __u32 ver = USER_INFO_VER;
     ::encode(ver, bl);
     ::encode(auid, bl);
     string access_key;
     string secret_key;
     if (!access_keys.empty()) {
       map<string, RGWAccessKey>::const_iterator iter = access_keys.begin();
       const RGWAccessKey& k = iter->second;
       access_key = k.id;
       secret_key = k.key;
     }
     ::encode(access_key, bl);
     ::encode(secret_key, bl);
     ::encode(display_name, bl);
     ::encode(user_email, bl);
     ::encode(openstack_name, bl);
     ::encode(openstack_key, bl);
     ::encode(user_id, bl);
     ::encode(access_keys, bl);
     ::encode(subusers, bl);
     ::encode(suspended, bl);
  }
  void decode(bufferlist::iterator& bl) {
     __u32 ver;
    ::decode(ver, bl);
     if (ver >= 2) ::decode(auid, bl);
     else auid = CEPH_AUTH_UID_DEFAULT;
     string access_key;
     string secret_key;
    ::decode(access_key, bl);
    ::decode(secret_key, bl);
    if (ver < 6) {
      RGWAccessKey k;
      k.id = access_key;
      k.key = secret_key;
      access_keys[access_key] = k;
    }
    ::decode(display_name, bl);
    ::decode(user_email, bl);
    if (ver >= 3) ::decode(openstack_name, bl);
    if (ver >= 4) ::decode(openstack_key, bl);
    if (ver >= 5)
      ::decode(user_id, bl);
    else
      user_id = access_key;
    if (ver >= 6) {
      ::decode(access_keys, bl);
      ::decode(subusers, bl);
    }
    suspended = 0;
    if (ver >= 7) {
      ::decode(suspended, bl);
    }
  }

  void clear() {
    user_id.clear();
    display_name.clear();
    user_email.clear();
    auid = CEPH_AUTH_UID_DEFAULT;
    access_keys.clear();
    suspended = 0;
  }
};
WRITE_CLASS_ENCODER(RGWUserInfo)

struct RGWPoolInfo
{
  string bucket;
  string owner;

  void encode(bufferlist& bl) const {
     __u32 ver = 1;
     ::encode(ver, bl);
     ::encode(bucket, bl);
     ::encode(owner, bl);
  }
  void decode(bufferlist::iterator& bl) {
     __u32 ver;
    ::decode(ver, bl);
    ::decode(bucket, bl);
    ::decode(owner, bl);
  }
};
WRITE_CLASS_ENCODER(RGWPoolInfo)

struct req_state;

struct RGWEnv;

/** Store all the state necessary to complete and respond to an HTTP request*/
struct req_state {
   FCGX_Request *fcgx;
   http_op op;
   bool content_started;
   int format;
   ceph::Formatter *formatter;
   const char *path_name;
   string path_name_url;
   const char *host;
   const char *method;
   const char *query;
   const char *length;
   uint64_t content_length;
   const char *content_type;
   struct rgw_err err;
   bool expect_cont;
   bool header_ended;
   uint64_t bytes_sent; // bytes sent as a response, excluding header
   uint64_t bytes_received; // data received
   uint64_t obj_size;
   bool should_log;
   uint32_t perm_mask;
   utime_t header_time;

   XMLArgs args;

   const char *bucket;
   const char *object;

   const char *host_bucket;

   string bucket_str;
   string object_str;

   map<string, string> x_amz_map;

   RGWUserInfo user; 
   RGWAccessControlPolicy *acl;

   string canned_acl;
   const char *copy_source;
   const char *http_auth;

   int prot_flags;

   const char *os_auth_token;
   char *os_user;
   char *os_groups;

   utime_t time;

   int pool_id;

   struct RGWEnv *env;

   void *obj_ctx;

   req_state(struct RGWEnv *e);
   ~req_state();
};

extern void flush_formatter_to_req_state(struct req_state *s,
					 ceph::Formatter *formatter);

/** Store basic data on an object */
struct RGWObjEnt {
  std::string name;
  std::string owner;
  std::string owner_display_name;
  size_t size;
  time_t mtime;
  // two md5 digests and a terminator
  char etag[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  string content_type;

  void clear() { // not clearing etag
    name="";
    size = 0;
    mtime = 0;
    content_type="";
  }
};

/** Store basic data on an object */
struct RGWBucketEnt {
  std::string name;
  size_t size;
  time_t mtime;
  char etag[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  uint64_t count;

  void encode(bufferlist& bl) const {
    __u8 struct_v = 2;
    ::encode(struct_v, bl);
    uint64_t s = size;
    __u32 mt = mtime;
    ::encode(name, bl);
    ::encode(s, bl);
    ::encode(mt, bl);
    ::encode(count, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    __u32 mt;
    uint64_t s;
    ::decode(name, bl);
    ::decode(s, bl);
    ::decode(mt, bl);
    size = s;
    mtime = mt;
    if (struct_v >= 2)
      ::decode(count, bl);
  }
  void clear() {
    name="";
    size = 0;
    mtime = 0;
    memset(etag, 0, sizeof(etag));
    count = 0;
  }
};
WRITE_CLASS_ENCODER(RGWBucketEnt)

struct RGWUploadPartInfo {
  uint32_t num;
  uint64_t size;
  string etag;
  utime_t modified;

  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(num, bl);
    ::encode(size, bl);
    ::encode(etag, bl);
    ::encode(modified, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(num, bl);
    ::decode(size, bl);
    ::decode(etag, bl);
    ::decode(modified, bl);
  }
};
WRITE_CLASS_ENCODER(RGWUploadPartInfo)

class rgw_obj {
  std::string orig_obj;
  std::string orig_key;
public:
  std::string bucket;
  std::string key;
  std::string ns;
  std::string object;

  rgw_obj() {}
  rgw_obj(std::string& b, std::string& o) {
    init(b, o);
  }
  rgw_obj(std::string& b, std::string& o, std::string& k) {
    init(b, o, k);
  }
  rgw_obj(std::string& b, std::string& o, std::string& k, std::string& n) {
    init(b, o, k, n);
  }
  void init(std::string& b, std::string& o, std::string& k, std::string& n) {
    bucket = b;
    set_ns(n);
    set_obj(o);
    set_key(k);
  }
  void init(std::string& b, std::string& o, std::string& k) {
    bucket = b;
    set_obj(o);
    set_key(k);
  }
  void init(std::string& b, std::string& o) {
    bucket = b;
    set_obj(o);
    orig_key = key = "";
  }
  int set_ns(const char *n) {
    if (!n)
      return -EINVAL;
    string ns_str(n);
    return set_ns(ns_str);
  }
  int set_ns(string& n) {
    if (n[0] == '_')
      return -EINVAL;
    ns = n;
    set_obj(orig_obj);
    return 0;
  }

  void set_key(string& k) {
    orig_key = k;
    if (k.compare(object) == 0)
      key = "";
    else
      key = k;
  }

  void set_obj(string& o) {
    orig_obj = o;
    if (ns.empty()) {
      if (o.empty())
        return;
      if (o.size() < 1 || o[0] != '_') {
        object = o;
        return;
      }
      object = "__";
      object.append(o);
    } else {
      object = "_";
      object.append(ns);
      object.append("_");
      object.append(o);
    }
    set_key(orig_key);
  }

  string loc() {
    if (orig_key.empty())
      return orig_obj;
    else
      return orig_key;
  }

  static bool translate_raw_obj(string& obj, string& ns) {
    if (ns.empty()) {
      if (obj[0] != '_')
        return true;

      if (obj.size() >= 2 && obj[1] == '_') {
        obj = obj.substr(1);
        return true;
      }

      return false;
    }

    if (obj[0] != '_' || obj.size() < 3) // for namespace, min size would be 3: _x_
      return false;

    int pos = obj.find('_', 1);
    if (pos <= 1) // if it starts with __, it's not in our namespace
      return false;

    string obj_ns = obj.substr(1, pos - 1);
    if (obj_ns.compare(ns) != 0)
        return false;

    obj = obj.substr(pos + 1);
    return true;
  }

  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(bucket, bl);
    ::encode(key, bl);
    ::encode(ns, bl);
    ::encode(object, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(bucket, bl);
    ::decode(key, bl);
    ::decode(ns, bl);
    ::decode(object, bl);
  }

  bool operator<(const rgw_obj& o) const {
    return  (bucket.compare(o.bucket) < 0) ||
            (object.compare(o.object) < 0) ||
            (ns.compare(o.ns) < 0);
  }
};
WRITE_CLASS_ENCODER(rgw_obj)

inline ostream& operator<<(ostream& out, const rgw_obj o) {
  return out << o.bucket << ":" << o.object;
}

static inline void buf_to_hex(const unsigned char *buf, int len, char *str)
{
  int i;
  str[0] = '\0';
  for (i = 0; i < len; i++) {
    sprintf(&str[i*2], "%02x", (int)buf[i]);
  }
}

static inline int hexdigit(char c)
{
  if (c >= '0' && c <= '9')
    return (c - '0');
  c = toupper(c);
  if (c >= 'A' && c <= 'F')
    return c - 'A' + 0xa;
  return -EINVAL;
}

static inline int hex_to_buf(const char *hex, char *buf, int len)
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

static inline int rgw_str_to_bool(const char *s, int def_val)
{
  if (!s)
    return def_val;

  return (strcasecmp(s, "on") == 0 ||
          strcasecmp(s, "yes") == 0 ||
          strcasecmp(s, "1") == 0);
}

static inline void append_rand_alpha(string& src, string& dest, int len)
{
  dest = src;
  char buf[len + 1];
  gen_rand_alphanumeric(buf, len);
  dest.append("_");
  dest.append(buf);
}

/** */
extern int parse_time(const char *time_str, time_t *time);
/** Check if a user has a permission on that ACL */
extern bool verify_permission(RGWAccessControlPolicy *policy, string& uid, int user_perm_mask, int perm);
/** Check if the req_state's user has the necessary permissions
 * to do the requested action */
extern bool verify_permission(struct req_state *s, int perm);
/** Convert an input URL into a sane object name
 * by converting %-escaped strings into characters, etc*/
extern bool url_decode(string& src_str, string& dest_str);

extern void calc_hmac_sha1(const char *key, int key_len,
                          const char *msg, int msg_len, char *dest);
/* destination should be CEPH_CRYPTO_HMACSHA1_DIGESTSIZE bytes long */

#endif
