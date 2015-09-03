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
#include "common/perf_counters.h"

#include "acconfig.h"

#include <errno.h>
#include <string.h>
#include <string>
#include <map>
#include "include/types.h"
#include "include/utime.h"
#include "rgw_acl.h"
#include "rgw_cors.h"
#include "rgw_quota.h"
#include "rgw_string.h"
#include "cls/version/cls_version_types.h"
#include "cls/user/cls_user_types.h"
#include "cls/rgw/cls_rgw_types.h"
#include "include/rados/librados.hpp"

using namespace std;

namespace ceph {
  class Formatter;
}

using ceph::crypto::MD5;


#define RGW_ATTR_PREFIX  "user.rgw."

#define RGW_HTTP_RGWX_ATTR_PREFIX "RGWX_ATTR_"
#define RGW_HTTP_RGWX_ATTR_PREFIX_OUT "Rgwx-Attr-"

#define RGW_AMZ_META_PREFIX "x-amz-meta-"

#define RGW_SYS_PARAM_PREFIX "rgwx-"

#define RGW_ATTR_ACL		RGW_ATTR_PREFIX "acl"
#define RGW_ATTR_CORS		RGW_ATTR_PREFIX "cors"
#define RGW_ATTR_ETAG    	RGW_ATTR_PREFIX "etag"
#define RGW_ATTR_BUCKETS	RGW_ATTR_PREFIX "buckets"
#define RGW_ATTR_META_PREFIX	RGW_ATTR_PREFIX RGW_AMZ_META_PREFIX
#define RGW_ATTR_CONTENT_TYPE	RGW_ATTR_PREFIX "content_type"
#define RGW_ATTR_CACHE_CONTROL	RGW_ATTR_PREFIX "cache_control"
#define RGW_ATTR_CONTENT_DISP	RGW_ATTR_PREFIX "content_disposition"
#define RGW_ATTR_CONTENT_ENC	RGW_ATTR_PREFIX "content_encoding"
#define RGW_ATTR_CONTENT_LANG	RGW_ATTR_PREFIX "content_language"
#define RGW_ATTR_EXPIRES	RGW_ATTR_PREFIX "expires"
#define RGW_ATTR_ID_TAG    	RGW_ATTR_PREFIX "idtag"
#define RGW_ATTR_SHADOW_OBJ    	RGW_ATTR_PREFIX "shadow_name"
#define RGW_ATTR_MANIFEST    	RGW_ATTR_PREFIX "manifest"
#define RGW_ATTR_USER_MANIFEST  RGW_ATTR_PREFIX "user_manifest"

#define RGW_ATTR_OLH_PREFIX     RGW_ATTR_PREFIX "olh."

#define RGW_ATTR_OLH_INFO       RGW_ATTR_OLH_PREFIX "info"
#define RGW_ATTR_OLH_VER        RGW_ATTR_OLH_PREFIX "ver"
#define RGW_ATTR_OLH_ID_TAG     RGW_ATTR_OLH_PREFIX "idtag"
#define RGW_ATTR_OLH_PENDING_PREFIX RGW_ATTR_OLH_PREFIX "pending."

#define RGW_BUCKETS_OBJ_SUFFIX ".buckets"

#define RGW_MAX_PENDING_CHUNKS  16
#define RGW_MAX_PUT_SIZE        (5ULL*1024*1024*1024)
#define RGW_MIN_MULTIPART_SIZE (5ULL*1024*1024)

#define RGW_FORMAT_PLAIN        0
#define RGW_FORMAT_XML          1
#define RGW_FORMAT_JSON         2

#define RGW_CAP_READ            0x1
#define RGW_CAP_WRITE           0x2
#define RGW_CAP_ALL             (RGW_CAP_READ | RGW_CAP_WRITE)

#define RGW_REST_SWIFT          0x1
#define RGW_REST_SWIFT_AUTH     0x2

#define RGW_SUSPENDED_USER_AUID (uint64_t)-2

#define RGW_OP_TYPE_READ         0x01
#define RGW_OP_TYPE_WRITE        0x02
#define RGW_OP_TYPE_DELETE       0x04

#define RGW_OP_TYPE_MODIFY       (RGW_OP_TYPE_WRITE | RGW_OP_TYPE_DELETE)
#define RGW_OP_TYPE_ALL          (RGW_OP_TYPE_READ | RGW_OP_TYPE_WRITE | RGW_OP_TYPE_DELETE)

#define RGW_DEFAULT_MAX_BUCKETS 1000

#define RGW_DEFER_TO_BUCKET_ACLS_RECURSE 1
#define RGW_DEFER_TO_BUCKET_ACLS_FULL_CONTROL 2

#define STATUS_CREATED           1900
#define STATUS_ACCEPTED          1901
#define STATUS_NO_CONTENT        1902
#define STATUS_PARTIAL_CONTENT   1903
#define STATUS_REDIRECT          1904
#define STATUS_NO_APPLY          1905
#define STATUS_APPLIED           1906

#define ERR_INVALID_BUCKET_NAME  2000
#define ERR_INVALID_OBJECT_NAME  2001
#define ERR_NO_SUCH_BUCKET       2002
#define ERR_METHOD_NOT_ALLOWED   2003
#define ERR_INVALID_DIGEST       2004
#define ERR_BAD_DIGEST           2005
#define ERR_UNRESOLVABLE_EMAIL   2006
#define ERR_INVALID_PART         2007
#define ERR_INVALID_PART_ORDER   2008
#define ERR_NO_SUCH_UPLOAD       2009
#define ERR_REQUEST_TIMEOUT      2010
#define ERR_LENGTH_REQUIRED      2011
#define ERR_REQUEST_TIME_SKEWED  2012
#define ERR_BUCKET_EXISTS        2013
#define ERR_BAD_URL              2014
#define ERR_PRECONDITION_FAILED  2015
#define ERR_NOT_MODIFIED         2016
#define ERR_INVALID_UTF8         2017
#define ERR_UNPROCESSABLE_ENTITY 2018
#define ERR_TOO_LARGE            2019
#define ERR_TOO_MANY_BUCKETS     2020
#define ERR_INVALID_REQUEST      2021
#define ERR_TOO_SMALL            2022
#define ERR_NOT_FOUND            2023
#define ERR_PERMANENT_REDIRECT   2024
#define ERR_LOCKED               2025
#define ERR_QUOTA_EXCEEDED       2026
#define ERR_SIGNATURE_NO_MATCH   2027
#define ERR_INVALID_ACCESS_KEY   2028
#define ERR_USER_SUSPENDED       2100
#define ERR_INTERNAL_ERROR       2200

#ifndef UINT32_MAX
#define UINT32_MAX (4294967295)
#endif

typedef void *RGWAccessHandle;


/* perf counter */

extern PerfCounters *perfcounter;

extern int rgw_perf_start(CephContext *cct);
extern void rgw_perf_stop(CephContext *cct);

enum {
  l_rgw_first = 15000,
  l_rgw_req,
  l_rgw_failed_req,

  l_rgw_get,
  l_rgw_get_b,
  l_rgw_get_lat,

  l_rgw_put,
  l_rgw_put_b,
  l_rgw_put_lat,

  l_rgw_qlen,
  l_rgw_qactive,

  l_rgw_cache_hit,
  l_rgw_cache_miss,

  l_rgw_keystone_token_cache_hit,
  l_rgw_keystone_token_cache_miss,

  l_rgw_last,
};


 /* size should be the required string size + 1 */
extern int gen_rand_base64(CephContext *cct, char *dest, int size);
extern int gen_rand_alphanumeric(CephContext *cct, char *dest, int size);
extern int gen_rand_alphanumeric_lower(CephContext *cct, char *dest, int size);
extern int gen_rand_alphanumeric_upper(CephContext *cct, char *dest, int size);
extern int gen_rand_alphanumeric_no_underscore(CephContext *cct, char *dest, int size);

extern int gen_rand_alphanumeric_lower(CephContext *cct, string *str, int length);

enum RGWIntentEvent {
  DEL_OBJ = 0,
  DEL_DIR = 1,
};

enum RGWObjCategory {
  RGW_OBJ_CATEGORY_NONE      = 0,
  RGW_OBJ_CATEGORY_MAIN      = 1,
  RGW_OBJ_CATEGORY_SHADOW    = 2,
  RGW_OBJ_CATEGORY_MULTIMETA = 3,
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

/* Helper class used for RGWHTTPArgs parsing */
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
class RGWHTTPArgs
{
  string str, empty_str;
  map<string, string> val_map;
  map<string, string> sys_val_map;
  map<string, string> sub_resources;

  bool has_resp_modifier;
 public:
  RGWHTTPArgs() : has_resp_modifier(false) {}
  /** Set the arguments; as received */
  void set(string s) {
    has_resp_modifier = false;
    val_map.clear();
    sub_resources.clear();
    str = s;
  }
  /** parse the received arguments */
  int parse();
  /** Get the value for a specific argument parameter */
  string& get(const string& name, bool *exists = NULL);
  string& get(const char *name, bool *exists = NULL);
  int get_bool(const string& name, bool *val, bool *exists);
  int get_bool(const char *name, bool *val, bool *exists);
  void get_bool(const char *name, bool *val, bool def_val);

  /** see if a parameter is contained in this RGWHTTPArgs */
  bool exists(const char *name) {
    map<string, string>::iterator iter = val_map.find(name);
    return (iter != val_map.end());
  }
  bool sub_resource_exists(const char *name) {
    map<string, string>::iterator iter = sub_resources.find(name);
    return (iter != sub_resources.end());
  }
  map<string, string>& get_params() {
    return val_map;
  }
  map<string, string>& get_sub_resources() { return sub_resources; }
  unsigned get_num_params() {
    return val_map.size();
  }
  bool has_response_modifier() {
    return has_resp_modifier;
  }
  void set_system() { /* make all system params visible */
    map<string, string>::iterator iter;
    for (iter = sys_val_map.begin(); iter != sys_val_map.end(); ++iter) {
      val_map[iter->first] = iter->second;
    }
  }
};

class RGWConf;

class RGWEnv {
  std::map<string, string, ltstr_nocase> env_map;
public:
  RGWConf *conf; 

  RGWEnv();
  ~RGWEnv();
  void init(CephContext *cct);
  void init(CephContext *cct, char **envp);
  void set(const char *name, const char *val);
  const char *get(const char *name, const char *def_val = NULL);
  int get_int(const char *name, int def_val = 0);
  bool get_bool(const char *name, bool def_val = 0);
  size_t get_size(const char *name, size_t def_val = 0);
  bool exists(const char *name);
  bool exists_prefix(const char *prefix);

  void remove(const char *name);

  std::map<string, string, ltstr_nocase>& get_map() { return env_map; }
};

class RGWConf {
  friend class RGWEnv;
protected:
  void init(CephContext *cct, RGWEnv * env);
public:
  RGWConf() :
    enable_ops_log(1), enable_usage_log(1), defer_to_bucket_acls(0) {}

  int enable_ops_log;
  int enable_usage_log;
  uint8_t defer_to_bucket_acls;
};

enum http_op {
  OP_GET,
  OP_PUT,
  OP_DELETE,
  OP_HEAD,
  OP_POST,
  OP_COPY,
  OP_OPTIONS,
  OP_UNKNOWN,
};

class RGWAccessControlPolicy;
class JSONObj;

struct RGWAccessKey {
  string id; // AccessKey
  string key; // SecretKey
  string subuser;

  RGWAccessKey() {}
  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    ::encode(id, bl);
    ::encode(key, bl);
    ::encode(subuser, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START_LEGACY_COMPAT_LEN_32(2, 2, 2, bl);
     ::decode(id, bl);
     ::decode(key, bl);
     ::decode(subuser, bl);
     DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void dump_plain(Formatter *f) const;
  void dump(Formatter *f, const string& user, bool swift) const;
  static void generate_test_instances(list<RGWAccessKey*>& o);

  void decode_json(JSONObj *obj);
  void decode_json(JSONObj *obj, bool swift);
};
WRITE_CLASS_ENCODER(RGWAccessKey)

struct RGWSubUser {
  string name;
  uint32_t perm_mask;

  RGWSubUser() : perm_mask(0) {}
  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    ::encode(name, bl);
    ::encode(perm_mask, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START_LEGACY_COMPAT_LEN_32(2, 2, 2, bl);
     ::decode(name, bl);
     ::decode(perm_mask, bl);
     DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void dump(Formatter *f, const string& user) const;
  static void generate_test_instances(list<RGWSubUser*>& o);

  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWSubUser)

class RGWUserCaps
{
  map<string, uint32_t> caps;

  int get_cap(const string& cap, string& type, uint32_t *perm);
  int add_cap(const string& cap);
  int remove_cap(const string& cap);
public:
  static int parse_cap_perm(const string& str, uint32_t *perm);
  int add_from_string(const string& str);
  int remove_from_string(const string& str);

  void encode(bufferlist& bl) const {
     ENCODE_START(1, 1, bl);
     ::encode(caps, bl);
     ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
     DECODE_START(1, bl);
     ::decode(caps, bl);
     DECODE_FINISH(bl);
  }
  int check_cap(const string& cap, uint32_t perm);
  void dump(Formatter *f) const;
  void dump(Formatter *f, const char *name) const;

  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWUserCaps)

void encode_json(const char *name, const obj_version& v, Formatter *f);
void encode_json(const char *name, const RGWUserCaps& val, Formatter *f);

void decode_json_obj(obj_version& v, JSONObj *obj);

struct RGWUserInfo
{
  uint64_t auid;
  string user_id;
  string display_name;
  string user_email;
  map<string, RGWAccessKey> access_keys;
  map<string, RGWAccessKey> swift_keys;
  map<string, RGWSubUser> subusers;
  __u8 suspended;
  uint32_t max_buckets;
  uint32_t op_mask;
  RGWUserCaps caps;
  __u8 system;
  string default_placement;
  list<string> placement_tags;
  RGWQuotaInfo bucket_quota;
  map<int, string> temp_url_keys;
  RGWQuotaInfo user_quota;

  RGWUserInfo() : auid(0), suspended(0), max_buckets(RGW_DEFAULT_MAX_BUCKETS), op_mask(RGW_OP_TYPE_ALL), system(0) {}

  void encode(bufferlist& bl) const {
     ENCODE_START(16, 9, bl);
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
     string swift_name;
     string swift_key;
     if (!swift_keys.empty()) {
       map<string, RGWAccessKey>::const_iterator iter = swift_keys.begin();
       const RGWAccessKey& k = iter->second;
       swift_name = k.id;
       swift_key = k.key;
     }
     ::encode(swift_name, bl);
     ::encode(swift_key, bl);
     ::encode(user_id, bl);
     ::encode(access_keys, bl);
     ::encode(subusers, bl);
     ::encode(suspended, bl);
     ::encode(swift_keys, bl);
     ::encode(max_buckets, bl);
     ::encode(caps, bl);
     ::encode(op_mask, bl);
     ::encode(system, bl);
     ::encode(default_placement, bl);
     ::encode(placement_tags, bl);
     ::encode(bucket_quota, bl);
     ::encode(temp_url_keys, bl);
     ::encode(user_quota, bl);
     ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
     DECODE_START_LEGACY_COMPAT_LEN_32(16, 9, 9, bl);
     if (struct_v >= 2) ::decode(auid, bl);
     else auid = CEPH_AUTH_UID_DEFAULT;
     string access_key;
     string secret_key;
    ::decode(access_key, bl);
    ::decode(secret_key, bl);
    if (struct_v < 6) {
      RGWAccessKey k;
      k.id = access_key;
      k.key = secret_key;
      access_keys[access_key] = k;
    }
    ::decode(display_name, bl);
    ::decode(user_email, bl);
    string swift_name;
    string swift_key;
    if (struct_v >= 3) ::decode(swift_name, bl);
    if (struct_v >= 4) ::decode(swift_key, bl);
    if (struct_v >= 5)
      ::decode(user_id, bl);
    else
      user_id = access_key;
    if (struct_v >= 6) {
      ::decode(access_keys, bl);
      ::decode(subusers, bl);
    }
    suspended = 0;
    if (struct_v >= 7) {
      ::decode(suspended, bl);
    }
    if (struct_v >= 8) {
      ::decode(swift_keys, bl);
    }
    if (struct_v >= 10) {
      ::decode(max_buckets, bl);
    } else {
      max_buckets = RGW_DEFAULT_MAX_BUCKETS;
    }
    if (struct_v >= 11) {
      ::decode(caps, bl);
    }
    if (struct_v >= 12) {
      ::decode(op_mask, bl);
    } else {
      op_mask = RGW_OP_TYPE_ALL;
    }
    system = 0;
    if (struct_v >= 13) {
      ::decode(system, bl);
      ::decode(default_placement, bl);
      ::decode(placement_tags, bl); /* tags of allowed placement rules */
    }
    if (struct_v >= 14) {
      ::decode(bucket_quota, bl);
    }
    if (struct_v >= 15) {
     ::decode(temp_url_keys, bl);
    }
    if (struct_v >= 16) {
      ::decode(user_quota, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<RGWUserInfo*>& o);

  void decode_json(JSONObj *obj);

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

struct rgw_bucket {
  std::string name;
  std::string data_pool;
  std::string data_extra_pool; /* if not set, then we should use data_pool instead */
  std::string index_pool;
  std::string marker;
  std::string bucket_id;

  std::string oid; /*
                    * runtime in-memory only info. If not empty, points to the bucket instance object
                    */

  rgw_bucket() { }
  rgw_bucket(const cls_user_bucket& b) {
    name = b.name;
    data_pool = b.data_pool;
    data_extra_pool = b.data_extra_pool;
    index_pool = b.index_pool;
    marker = b.marker;
    bucket_id = b.bucket_id;
  }
  rgw_bucket(const char *n) : name(n) {
    assert(*n == '.'); // only rgw private buckets should be initialized without pool
    data_pool = index_pool = n;
    marker = "";
  }
  rgw_bucket(const char *n, const char *dp, const char *ip, const char *m, const char *id, const char *h) :
    name(n), data_pool(dp), index_pool(ip), marker(m), bucket_id(id) {}

  void convert(cls_user_bucket *b) {
    b->name = name;
    b->data_pool = data_pool;
    b->data_extra_pool = data_extra_pool;
    b->index_pool = index_pool;
    b->marker = marker;
    b->bucket_id = bucket_id;
  }

  void encode(bufferlist& bl) const {
     ENCODE_START(7, 3, bl);
    ::encode(name, bl);
    ::encode(data_pool, bl);
    ::encode(marker, bl);
    ::encode(bucket_id, bl);
    ::encode(index_pool, bl);
    ::encode(data_extra_pool, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(7, 3, 3, bl);
    ::decode(name, bl);
    ::decode(data_pool, bl);
    if (struct_v >= 2) {
      ::decode(marker, bl);
      if (struct_v <= 3) {
        uint64_t id;
        ::decode(id, bl);
        char buf[16];
        snprintf(buf, sizeof(buf), "%llu", (long long)id);
        bucket_id = buf;
      } else {
        ::decode(bucket_id, bl);
      }
    }
    if (struct_v >= 5) {
      ::decode(index_pool, bl);
    } else {
      index_pool = data_pool;
    }
    if (struct_v >= 7) {
      ::decode(data_extra_pool, bl);
    }
    DECODE_FINISH(bl);
  }

  const string& get_data_extra_pool() {
    if (data_extra_pool.empty()) {
      return data_pool;
    }
    return data_extra_pool;
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(list<rgw_bucket*>& o);

  bool operator<(const rgw_bucket& b) const {
    return name.compare(b.name) < 0;
  }
};
WRITE_CLASS_ENCODER(rgw_bucket)

inline ostream& operator<<(ostream& out, const rgw_bucket &b) {
  out << b.name;
  if (b.name.compare(b.data_pool)) {
    out << "(@";
    string s;
    if (!b.index_pool.empty() && b.data_pool.compare(b.index_pool))
      s = "i=" + b.index_pool;
    if (!b.data_extra_pool.empty() && b.data_pool.compare(b.data_extra_pool)) {
      if (!s.empty()) {
        s += ",";
      }
      s += "e=" + b.data_extra_pool;
    }
    if (!s.empty()) {
      out << "{"  << s << "}";
    }

    out << b.data_pool << "[" << b.marker << "])";
  }
  return out;
}

struct rgw_bucket_shard {
  rgw_bucket bucket;
  int shard_id;

  rgw_bucket_shard() : shard_id(-1) {}
  rgw_bucket_shard(rgw_bucket& _b, int _sid) : bucket(_b), shard_id(_sid) {}

  bool operator<(const rgw_bucket_shard& b) const {
    if (bucket < b.bucket) {
      return true;
    }
    if (b.bucket < bucket) {
      return false;
    }
    return shard_id < b.shard_id;
  }
};


struct RGWObjVersionTracker {
  obj_version read_version;
  obj_version write_version;

  obj_version *version_for_read() {
    return &read_version;
  }

  obj_version *version_for_write() {
    if (write_version.ver == 0)
      return NULL;

    return &write_version;
  }

  obj_version *version_for_check() {
    if (read_version.ver == 0)
      return NULL;

    return &read_version;
  }

  void prepare_op_for_read(librados::ObjectReadOperation *op);
  void prepare_op_for_write(librados::ObjectWriteOperation *op);

  void apply_write() {
    read_version = write_version;
    write_version = obj_version();
  }

  void clear() {
    read_version = obj_version();
    write_version = obj_version();
  }

  void generate_new_write_ver(CephContext *cct);
};

enum RGWBucketFlags {
  BUCKET_SUSPENDED = 0x1,
  BUCKET_VERSIONED = 0x2,
  BUCKET_VERSIONS_SUSPENDED = 0x4,
};

struct RGWBucketInfo
{
  enum BIShardsHashType {
    MOD = 0
  };

  rgw_bucket bucket;
  string owner;
  uint32_t flags;
  string region;
  time_t creation_time;
  string placement_rule;
  bool has_instance_obj;
  RGWObjVersionTracker objv_tracker; /* we don't need to serialize this, for runtime tracking */
  obj_version ep_objv; /* entry point object version, for runtime tracking only */
  RGWQuotaInfo quota;

  // Represents the number of bucket index object shards:
  //   - value of 0 indicates there is no sharding (this is by default before this
  //     feature is implemented).
  //   - value of UINT32_T::MAX indicates this is a blind bucket.
  uint32_t num_shards;

  // Represents the bucket index shard hash type.
  uint8_t bucket_index_shard_hash_type;

  // Represents the shard number for blind bucket.
  const static uint32_t NUM_SHARDS_BLIND_BUCKET;

  void encode(bufferlist& bl) const {
     ENCODE_START(11, 4, bl);
     ::encode(bucket, bl);
     ::encode(owner, bl);
     ::encode(flags, bl);
     ::encode(region, bl);
     uint64_t ct = (uint64_t)creation_time;
     ::encode(ct, bl);
     ::encode(placement_rule, bl);
     ::encode(has_instance_obj, bl);
     ::encode(quota, bl);
     ::encode(num_shards, bl);
     ::encode(bucket_index_shard_hash_type, bl);
     ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN_32(9, 4, 4, bl);
     ::decode(bucket, bl);
     if (struct_v >= 2)
       ::decode(owner, bl);
     if (struct_v >= 3)
       ::decode(flags, bl);
     if (struct_v >= 5)
       ::decode(region, bl);
     if (struct_v >= 6) {
       uint64_t ct;
       ::decode(ct, bl);
       creation_time = (time_t)ct;
     }
     if (struct_v >= 7)
       ::decode(placement_rule, bl);
     if (struct_v >= 8)
       ::decode(has_instance_obj, bl);
     if (struct_v >= 9)
       ::decode(quota, bl);
     if (struct_v >= 10)
       ::decode(num_shards, bl);
     if (struct_v >= 11)
       ::decode(bucket_index_shard_hash_type, bl);
     DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<RGWBucketInfo*>& o);

  void decode_json(JSONObj *obj);

  bool versioned() { return (flags & BUCKET_VERSIONED) != 0; }
  int versioning_status() { return flags & (BUCKET_VERSIONED | BUCKET_VERSIONS_SUSPENDED); }
  bool versioning_enabled() { return versioning_status() == BUCKET_VERSIONED; }

  RGWBucketInfo() : flags(0), creation_time(0), has_instance_obj(false), num_shards(0), bucket_index_shard_hash_type(MOD) {}
};
WRITE_CLASS_ENCODER(RGWBucketInfo)

struct RGWBucketEntryPoint
{
  rgw_bucket bucket;
  string owner;
  time_t creation_time;
  bool linked;

  bool has_bucket_info;
  RGWBucketInfo old_bucket_info;

  RGWBucketEntryPoint() : creation_time(0), linked(false), has_bucket_info(false) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(8, 8, bl);
    ::encode(bucket, bl);
    ::encode(owner, bl);
    ::encode(linked, bl);
    uint64_t ctime = (uint64_t)creation_time;
    ::encode(ctime, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    bufferlist::iterator orig_iter = bl;
    DECODE_START_LEGACY_COMPAT_LEN_32(8, 4, 4, bl);
    if (struct_v < 8) {
      /* ouch, old entry, contains the bucket info itself */
      old_bucket_info.decode(orig_iter);
      has_bucket_info = true;
      return;
    }
    has_bucket_info = false;
    ::decode(bucket, bl);
    ::decode(owner, bl);
    ::decode(linked, bl);
    uint64_t ctime;
    ::decode(ctime, bl);
    creation_time = (uint64_t)ctime;
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWBucketEntryPoint)

struct RGWStorageStats
{
  RGWObjCategory category;
  uint64_t num_kb;
  uint64_t num_kb_rounded;
  uint64_t num_objects;

  RGWStorageStats() : category(RGW_OBJ_CATEGORY_NONE), num_kb(0), num_kb_rounded(0), num_objects(0) {}

  void dump(Formatter *f) const;
};

struct req_state;

class RGWEnv;

class RGWClientIO;

struct req_info {
  RGWEnv *env;
  RGWHTTPArgs args;
  map<string, string> x_meta_map;

  const char *host;
  const char *method;
  string script_uri;
  string request_uri;
  string effective_uri;
  string request_params;
  string domain;

  req_info(CephContext *cct, RGWEnv *_env);
  void rebuild_from(req_info& src);
  void init_meta_info(bool *found_bad_meta);
};

struct rgw_obj_key {
  string name;
  string instance;

  rgw_obj_key() {}
  rgw_obj_key(const string& n) {
    set(n);
  }
  rgw_obj_key(const string& n, const string& i) {
    set(n, i);
  }

  void set(const cls_rgw_obj_key& k) {
    name = k.name;
    instance = k.instance;
  }

  void transform(cls_rgw_obj_key *k) {
    k->name = name;
    k->instance = instance;
  }

  void set(const string& n) {
    name = n;
    instance.clear();
  }

  void set(const string& n, const string& i) {
    name = n;
    instance = i;
  }

  bool empty() {
    return name.empty();
  }
  bool operator==(const rgw_obj_key& k) const {
    return (name.compare(k.name) == 0) &&
           (instance.compare(k.instance) == 0);
  }
  bool operator<(const rgw_obj_key& k) const {
    int r = name.compare(k.name);
    if (r == 0) {
      r = instance.compare(k.instance);
    }
    return (r < 0);
  }
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(name, bl);
    ::encode(instance, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(name, bl);
    ::decode(instance, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_obj_key)

inline ostream& operator<<(ostream& out, const rgw_obj_key &o) {
  if (o.instance.empty()) {
    return out << o.name;
  } else {
    return out << o.name << "[" << o.instance << "]";
  }
}

/** Store all the state necessary to complete and respond to an HTTP request*/
struct req_state {
   CephContext *cct;
   RGWClientIO *cio;
   http_op op;
   bool content_started;
   int format;
   ceph::Formatter *formatter;
   string decoded_uri;
   string relative_uri;
   const char *length;
   int64_t content_length;
   map<string, string> generic_attrs;
   struct rgw_err err;
   bool expect_cont;
   bool header_ended;
   uint64_t obj_size;
   bool enable_ops_log;
   bool enable_usage_log;
   uint8_t defer_to_bucket_acls;
   uint32_t perm_mask;
   utime_t header_time;

   rgw_bucket bucket;
   string bucket_name_str;
   rgw_obj_key object;
   string src_bucket_name;
   rgw_obj_key src_object;
   ACLOwner bucket_owner;
   ACLOwner owner;

   string region_endpoint;
   string bucket_instance_id;

   RGWBucketInfo bucket_info;
   map<string, bufferlist> bucket_attrs;
   bool bucket_exists;

   bool has_bad_meta;

   RGWUserInfo user; 
   RGWAccessControlPolicy *bucket_acl;
   RGWAccessControlPolicy *object_acl;

   bool system_request;

   string canned_acl;
   bool has_acl_header;
   const char *copy_source;
   const char *http_auth;
   bool local_source; /* source is local */

   int prot_flags;

   const char *os_auth_token;
   string swift_user;
   string swift_groups;

   utime_t time;

   void *obj_ctx;

   string dialect;

   string req_id;

   string trans_id;

   req_info info;

   req_state(CephContext *_cct, class RGWEnv *e);
   ~req_state();
};

/** Store basic data on an object */
struct RGWObjEnt {
  rgw_obj_key key;
  std::string ns;
  std::string owner;
  std::string owner_display_name;
  uint64_t size;
  utime_t mtime;
  string etag;
  string content_type;
  string tag;
  uint32_t flags;
  uint64_t versioned_epoch;

  RGWObjEnt() : size(0), flags(0), versioned_epoch(0) {}

  void dump(Formatter *f) const;

  bool is_current() {
    uint32_t test_flags = RGW_BUCKET_DIRENT_FLAG_VER | RGW_BUCKET_DIRENT_FLAG_CURRENT;
    return (flags & RGW_BUCKET_DIRENT_FLAG_VER) == 0 ||
           (flags & test_flags) == test_flags;
  }
  bool is_delete_marker() { return (flags & RGW_BUCKET_DIRENT_FLAG_DELETE_MARKER) != 0; }
  bool is_visible() {
    return is_current() && !is_delete_marker();
  }
};

/** Store basic data on bucket */
struct RGWBucketEnt {
  rgw_bucket bucket;
  size_t size;
  size_t size_rounded;
  time_t creation_time;
  uint64_t count;

  RGWBucketEnt() : size(0), size_rounded(0), creation_time(0), count(0) {}

  RGWBucketEnt(const cls_user_bucket_entry& e) {
    bucket = e.bucket;
    size = e.size;
    size_rounded = e.size_rounded;
    creation_time = e.creation_time;
    count = e.count;
  }

  void convert(cls_user_bucket_entry *b) {
    bucket.convert(&b->bucket);
    b->size = size;
    b->size_rounded = size_rounded;
    b->creation_time = creation_time;
    b->count = count;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(5, 5, bl);
    uint64_t s = size;
    __u32 mt = creation_time;
    string empty_str;  // originally had the bucket name here, but we encode bucket later
    ::encode(empty_str, bl);
    ::encode(s, bl);
    ::encode(mt, bl);
    ::encode(count, bl);
    ::encode(bucket, bl);
    s = size_rounded;
    ::encode(s, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(5, 5, 5, bl);
    __u32 mt;
    uint64_t s;
    string empty_str;  // backward compatibility
    ::decode(empty_str, bl);
    ::decode(s, bl);
    ::decode(mt, bl);
    size = s;
    creation_time = mt;
    if (struct_v >= 2)
      ::decode(count, bl);
    if (struct_v >= 3)
      ::decode(bucket, bl);
    if (struct_v >= 4)
      ::decode(s, bl);
    size_rounded = s;
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<RGWBucketEnt*>& o);
};
WRITE_CLASS_ENCODER(RGWBucketEnt)

class rgw_obj {
  std::string orig_obj;
  std::string loc;
  std::string object;
  std::string instance;
public:
  const std::string& get_object() const { return object; }
  const std::string& get_orig_obj() const { return orig_obj; }
  const std::string& get_loc() const { return loc; }
  const std::string& get_instance() const { return instance; }
  rgw_bucket bucket;
  std::string ns;

  bool in_extra_data; /* in-memory only member, does not serialize */

  // Represents the hash index source for this object once it is set (non-empty)
  std::string index_hash_source;

  rgw_obj() : in_extra_data(false) {}
  rgw_obj(rgw_bucket& b, const std::string& o) : in_extra_data(false) {
    init(b, o);
  }
  rgw_obj(rgw_bucket& b, const rgw_obj_key& k) : in_extra_data(false) {
    init(b, k.name);
    set_instance(k.instance);
  }
  void init(rgw_bucket& b, const std::string& o) {
    bucket = b;
    set_obj(o);
    reset_loc();
  }
  void init_ns(rgw_bucket& b, const std::string& o, const std::string& n) {
    bucket = b;
    set_ns(n);
    set_obj(o);
    reset_loc();
  }
  int set_ns(const char *n) {
    if (!n)
      return -EINVAL;
    string ns_str(n);
    return set_ns(ns_str);
  }
  int set_ns(const string& n) {
    if (n[0] == '_')
      return -EINVAL;
    ns = n;
    set_obj(orig_obj);
    return 0;
  }
  int set_instance(const string& i) {
    if (i[0] == '_')
      return -EINVAL;
    instance = i;
    set_obj(orig_obj);
    return 0;
  }

  int clear_instance() {
    return set_instance(string());
  }

  void set_loc(const string& k) {
    loc = k;
  }

  void reset_loc() {
    loc.clear();
    /*
     * For backward compatibility. Older versions used to have object locator on all objects,
     * however, the orig_obj was the effective object locator. This had the same effect as not
     * having object locator at all for most objects but the ones that started with underscore as
     * these were escaped.
     */
    if (orig_obj[0] == '_' && ns.empty()) {
      loc = orig_obj;
    }
  }

  bool have_null_instance() {
    return instance == "null";
  }

  bool have_instance() {
    return !instance.empty();
  }

  bool need_to_encode_instance() {
    return have_instance() && !have_null_instance();
  }

  void set_obj(const string& o) {
    object.reserve(128);

    orig_obj = o;
    if (ns.empty() && !need_to_encode_instance()) {
      if (o.empty()) {
        return;
      }
      if (o.size() < 1 || o[0] != '_') {
        object = o;
        return;
      }
      object = "_";
      object.append(o);
    } else {
      object = "_";
      object.append(ns);
      if (need_to_encode_instance()) {
        object.append(string(":") + instance);
      }
      object.append("_");
      object.append(o);
    }
    reset_loc();
  }

  /*
   * get the object's key name as being referred to by the bucket index.
   */
  string get_index_key_name() const {
    if (ns.empty()) {
      if (orig_obj.size() < 1 || orig_obj[0] != '_') {
        return orig_obj;
      }
      return string("_") + orig_obj;
    };

    char buf[ns.size() + 16];
    snprintf(buf, sizeof(buf), "_%s_", ns.c_str());
    return string(buf) + orig_obj;
  };

  void get_index_key(rgw_obj_key *key) const {
    key->name = get_index_key_name();
    key->instance = instance;
  }

  static void parse_ns_field(string& ns, string& instance) {
    int pos = ns.find(':');
    if (pos >= 0) {
      instance = ns.substr(pos + 1);
      ns = ns.substr(0, pos);
    } else {
      instance.clear();
    }
  }

  string& get_hash_object() {
    return index_hash_source.empty() ? orig_obj : index_hash_source;
  }
  /**
   * Translate a namespace-mangled object name to the user-facing name
   * existing in the given namespace.
   *
   * If the object is part of the given namespace, it returns true
   * and cuts down the name to the unmangled version. If it is not
   * part of the given namespace, it returns false.
   */
  static bool translate_raw_obj_to_obj_in_ns(string& obj, string& instance, string& ns) {
    if (obj[0] != '_') {
      if (ns.empty()) {
        return true;
      }
      return false;
    }

    string obj_ns;
    bool ret = parse_raw_oid(obj, &obj, &instance, &obj_ns);
    if (!ret) {
      return ret;
    }

    return (ns == obj_ns);
  }

  static bool parse_raw_oid(const string& oid, string *obj_name, string *obj_instance, string *obj_ns) {
    obj_instance->clear();
    obj_ns->clear();
    if (oid[0] != '_') {
      *obj_name = oid;
      return true;
    }

    if (oid.size() >= 2 && oid[1] == '_') {
      *obj_name = oid.substr(1);
      return true;
    }

    if (oid[0] != '_' || oid.size() < 3) // for namespace, min size would be 3: _x_
      return false;

    int pos = oid.find('_', 1);
    if (pos <= 1) // if it starts with __, it's not in our namespace
      return false;

    *obj_ns = oid.substr(1, pos - 1);
    parse_ns_field(*obj_ns, *obj_instance);

    *obj_name = oid.substr(pos + 1);
    return true;
  }

  /**
   * Given a mangled object name and an empty namespace string, this
   * function extracts the namespace into the string and sets the object
   * name to be the unmangled version.
   *
   * It returns true after successfully doing so, or
   * false if it fails.
   */
  static bool strip_namespace_from_object(string& obj, string& ns, string& instance) {
    ns.clear();
    instance.clear();
    if (obj[0] != '_') {
      return true;
    }

    size_t pos = obj.find('_', 1);
    if (pos == string::npos) {
      return false;
    }

    size_t period_pos = obj.find('.');
    if (period_pos < pos) {
      return false;
    }

    ns = obj.substr(1, pos-1);
    obj = obj.substr(pos+1, string::npos);

    parse_ns_field(ns, instance);
    return true;
  }

  void set_in_extra_data(bool val) {
    in_extra_data = val;
  }

  bool is_in_extra_data() const {
    return in_extra_data;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(5, 3, bl);
    ::encode(bucket.name, bl);
    ::encode(loc, bl);
    ::encode(ns, bl);
    ::encode(object, bl);
    ::encode(bucket, bl);
    ::encode(instance, bl);
    if (!ns.empty() || !instance.empty()) {
      ::encode(orig_obj, bl);
    }
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(5, 3, 3, bl);
    ::decode(bucket.name, bl);
    ::decode(loc, bl);
    ::decode(ns, bl);
    ::decode(object, bl);
    if (struct_v >= 2)
      ::decode(bucket, bl);
    if (struct_v >= 4)
      ::decode(instance, bl);
    if (ns.empty() && instance.empty()) {
      orig_obj = object;
    } else {
      if (struct_v >= 5) {
        ::decode(orig_obj, bl);
      } else {
        ssize_t pos = object.find('_', 1);
        if (pos < 0) {
          throw buffer::error();
        }
        orig_obj = object.substr(pos);
      }
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<rgw_obj*>& o);

  bool operator==(const rgw_obj& o) const {
    return (object.compare(o.object) == 0) &&
           (bucket.name.compare(o.bucket.name) == 0) &&
           (ns.compare(o.ns) == 0) &&
           (instance.compare(o.instance) == 0);
  }
  bool operator<(const rgw_obj& o) const {
    int r = bucket.name.compare(o.bucket.name);
    if (r == 0) {
     r = object.compare(o.object);
     if (r == 0) {
       r = ns.compare(o.ns);
       if (r == 0) {
         r = instance.compare(o.instance);
       }
     }
    }

    return (r < 0);
  }
};
WRITE_CLASS_ENCODER(rgw_obj)

struct rgw_cache_entry_info {
  string cache_locator;
  uint64_t gen;

  rgw_cache_entry_info() : gen(0) {}
};

inline ostream& operator<<(ostream& out, const rgw_obj &o) {
  return out << o.bucket.name << ":" << o.get_object();
}

static inline bool str_startswith(const string& str, const string& prefix)
{
  return (str.compare(0, prefix.size(), prefix) == 0);
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

static inline void append_rand_alpha(CephContext *cct, const string& src, string& dest, int len)
{
  dest = src;
  char buf[len + 1];
  gen_rand_alphanumeric(cct, buf, len);
  dest.append("_");
  dest.append(buf);
}

static inline const char *rgw_obj_category_name(RGWObjCategory category)
{
  switch (category) {
  case RGW_OBJ_CATEGORY_NONE:
    return "rgw.none";
  case RGW_OBJ_CATEGORY_MAIN:
    return "rgw.main";
  case RGW_OBJ_CATEGORY_SHADOW:
    return "rgw.shadow";
  case RGW_OBJ_CATEGORY_MULTIMETA:
    return "rgw.multimeta";
  }

  return "unknown";
}

static inline uint64_t rgw_rounded_kb(uint64_t bytes)
{
  return (bytes + 1023) / 1024;
}

static inline uint64_t rgw_rounded_objsize_kb(uint64_t bytes)
{
  return ((bytes + 4095) & ~4095) / 1024;
}

extern string rgw_string_unquote(const string& s);
extern void parse_csv_string(const string& ival, vector<string>& ovals);
extern int parse_key_value(string& in_str, string& key, string& val);
extern int parse_key_value(string& in_str, const char *delim, string& key, string& val);
/** time parsing */
extern int parse_time(const char *time_str, time_t *time);
extern bool parse_rfc2616(const char *s, struct tm *t);
extern bool parse_iso8601(const char *s, struct tm *t);
extern string rgw_trim_whitespace(const string& src);
extern string rgw_trim_quotes(const string& val);


/** Check if the req_state's user has the necessary permissions
 * to do the requested action */
extern bool verify_bucket_permission(struct req_state *s, int perm);
extern bool verify_object_permission(struct req_state *s, RGWAccessControlPolicy *bucket_acl, RGWAccessControlPolicy *object_acl, int perm);
extern bool verify_object_permission(struct req_state *s, int perm);
/** Convert an input URL into a sane object name
 * by converting %-escaped strings into characters, etc*/
extern bool url_decode(string& src_str, string& dest_str, bool in_query = false);
extern void url_encode(const string& src, string& dst);

extern void calc_hmac_sha1(const char *key, int key_len,
                          const char *msg, int msg_len, char *dest);
/* destination should be CEPH_CRYPTO_HMACSHA1_DIGESTSIZE bytes long */

extern int rgw_parse_op_type_list(const string& str, uint32_t *perm);

#endif
