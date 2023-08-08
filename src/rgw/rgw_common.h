// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 * Copyright (C) 2015 Yehuda Sadeh <yehuda@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#pragma once

#include <array>
#include <string_view>
#include <atomic>
#include <unordered_map>

#include "common/ceph_crypto.h"
#include "common/random_string.h"
#include "rgw_acl.h"
#include "rgw_bucket_layout.h"
#include "rgw_cors.h"
#include "rgw_basic_types.h"
#include "rgw_iam_policy.h"
#include "rgw_quota_types.h"
#include "rgw_string.h"
#include "common/async/yield_context.h"
#include "rgw_website.h"
#include "rgw_object_lock.h"
#include "rgw_tag.h"
#include "rgw_op_type.h"
#include "rgw_sync_policy.h"
#include "cls/version/cls_version_types.h"
#include "cls/user/cls_user_types.h"
#include "cls/rgw/cls_rgw_types.h"
#include "include/rados/librados.hpp"
#include "rgw_public_access.h"
#include "common/tracer.h"

namespace ceph {
  class Formatter;
}

namespace rgw::sal {
  class User;
  class Bucket;
  class Object;
  using Attrs = std::map<std::string, ceph::buffer::list>;
}

using ceph::crypto::MD5;

#define RGW_ATTR_PREFIX  "user.rgw."

#define RGW_HTTP_RGWX_ATTR_PREFIX "RGWX_ATTR_"
#define RGW_HTTP_RGWX_ATTR_PREFIX_OUT "Rgwx-Attr-"

#define RGW_AMZ_PREFIX "x-amz-"
#define RGW_AMZ_META_PREFIX RGW_AMZ_PREFIX "meta-"
#define RGW_AMZ_WEBSITE_REDIRECT_LOCATION RGW_AMZ_PREFIX "website-redirect-location"
#define RGW_AMZ_TAG_COUNT RGW_AMZ_PREFIX "tagging-count"

#define RGW_SYS_PARAM_PREFIX "rgwx-"

#define RGW_ATTR_ACL		RGW_ATTR_PREFIX "acl"
#define RGW_ATTR_RATELIMIT		RGW_ATTR_PREFIX "ratelimit"
#define RGW_ATTR_LC            RGW_ATTR_PREFIX "lc"
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
#define RGW_ATTR_DELETE_AT 	RGW_ATTR_PREFIX "delete_at"
#define RGW_ATTR_ID_TAG    	RGW_ATTR_PREFIX "idtag"
#define RGW_ATTR_TAIL_TAG    	RGW_ATTR_PREFIX "tail_tag"
#define RGW_ATTR_SHADOW_OBJ    	RGW_ATTR_PREFIX "shadow_name"
#define RGW_ATTR_MANIFEST    	RGW_ATTR_PREFIX "manifest"
#define RGW_ATTR_USER_MANIFEST  RGW_ATTR_PREFIX "user_manifest"
#define RGW_ATTR_AMZ_WEBSITE_REDIRECT_LOCATION	RGW_ATTR_PREFIX RGW_AMZ_WEBSITE_REDIRECT_LOCATION
#define RGW_ATTR_SLO_MANIFEST   RGW_ATTR_PREFIX "slo_manifest"
/* Information whether an object is SLO or not must be exposed to
 * user through custom HTTP header named X-Static-Large-Object. */
#define RGW_ATTR_SLO_UINDICATOR RGW_ATTR_META_PREFIX "static-large-object"
#define RGW_ATTR_X_ROBOTS_TAG	RGW_ATTR_PREFIX "x-robots-tag"
#define RGW_ATTR_STORAGE_CLASS  RGW_ATTR_PREFIX "storage_class"

/* S3 Object Lock*/
#define RGW_ATTR_OBJECT_LOCK        RGW_ATTR_PREFIX "object-lock"
#define RGW_ATTR_OBJECT_RETENTION   RGW_ATTR_PREFIX "object-retention"
#define RGW_ATTR_OBJECT_LEGAL_HOLD  RGW_ATTR_PREFIX "object-legal-hold"


#define RGW_ATTR_PG_VER 	RGW_ATTR_PREFIX "pg_ver"
#define RGW_ATTR_SOURCE_ZONE    RGW_ATTR_PREFIX "source_zone"
#define RGW_ATTR_TAGS           RGW_ATTR_PREFIX RGW_AMZ_PREFIX "tagging"

#define RGW_ATTR_TEMPURL_KEY1   RGW_ATTR_META_PREFIX "temp-url-key"
#define RGW_ATTR_TEMPURL_KEY2   RGW_ATTR_META_PREFIX "temp-url-key-2"

/* Account/container quota of the Swift API. */
#define RGW_ATTR_QUOTA_NOBJS    RGW_ATTR_META_PREFIX "quota-count"
#define RGW_ATTR_QUOTA_MSIZE    RGW_ATTR_META_PREFIX "quota-bytes"

/* Static Web Site of Swift API. */
#define RGW_ATTR_WEB_INDEX      RGW_ATTR_META_PREFIX "web-index"
#define RGW_ATTR_WEB_ERROR      RGW_ATTR_META_PREFIX "web-error"
#define RGW_ATTR_WEB_LISTINGS   RGW_ATTR_META_PREFIX "web-listings"
#define RGW_ATTR_WEB_LIST_CSS   RGW_ATTR_META_PREFIX "web-listings-css"
#define RGW_ATTR_SUBDIR_MARKER  RGW_ATTR_META_PREFIX "web-directory-type"

#define RGW_ATTR_OLH_PREFIX     RGW_ATTR_PREFIX "olh."

#define RGW_ATTR_OLH_INFO       RGW_ATTR_OLH_PREFIX "info"
#define RGW_ATTR_OLH_VER        RGW_ATTR_OLH_PREFIX "ver"
#define RGW_ATTR_OLH_ID_TAG     RGW_ATTR_OLH_PREFIX "idtag"
#define RGW_ATTR_OLH_PENDING_PREFIX RGW_ATTR_OLH_PREFIX "pending."

#define RGW_ATTR_COMPRESSION    RGW_ATTR_PREFIX "compression"

#define RGW_ATTR_APPEND_PART_NUM    RGW_ATTR_PREFIX "append_part_num"

/* IAM Policy */
#define RGW_ATTR_IAM_POLICY	RGW_ATTR_PREFIX "iam-policy"
#define RGW_ATTR_USER_POLICY    RGW_ATTR_PREFIX "user-policy"
#define RGW_ATTR_PUBLIC_ACCESS  RGW_ATTR_PREFIX "public-access"

/* RGW File Attributes */
#define RGW_ATTR_UNIX_KEY1      RGW_ATTR_PREFIX "unix-key1"
#define RGW_ATTR_UNIX1          RGW_ATTR_PREFIX "unix1"

#define RGW_ATTR_CRYPT_PREFIX   RGW_ATTR_PREFIX "crypt."
#define RGW_ATTR_CRYPT_MODE     RGW_ATTR_CRYPT_PREFIX "mode"
#define RGW_ATTR_CRYPT_KEYMD5   RGW_ATTR_CRYPT_PREFIX "keymd5"
#define RGW_ATTR_CRYPT_KEYID    RGW_ATTR_CRYPT_PREFIX "keyid"
#define RGW_ATTR_CRYPT_KEYSEL   RGW_ATTR_CRYPT_PREFIX "keysel"
#define RGW_ATTR_CRYPT_CONTEXT  RGW_ATTR_CRYPT_PREFIX "context"
#define RGW_ATTR_CRYPT_DATAKEY  RGW_ATTR_CRYPT_PREFIX "datakey"
#define RGW_ATTR_CRYPT_PARTS    RGW_ATTR_CRYPT_PREFIX "part-lengths"

/* SSE-S3 Encryption Attributes */
#define RGW_ATTR_BUCKET_ENCRYPTION_PREFIX RGW_ATTR_PREFIX "sse-s3."
#define RGW_ATTR_BUCKET_ENCRYPTION_POLICY RGW_ATTR_BUCKET_ENCRYPTION_PREFIX "policy"
#define RGW_ATTR_BUCKET_ENCRYPTION_KEY_ID RGW_ATTR_BUCKET_ENCRYPTION_PREFIX "key-id"

#define RGW_ATTR_TRACE RGW_ATTR_PREFIX "trace"

#define RGW_FORMAT_PLAIN        0
#define RGW_FORMAT_XML          1
#define RGW_FORMAT_JSON         2
#define RGW_FORMAT_HTML         3

#define RGW_CAP_READ            0x1
#define RGW_CAP_WRITE           0x2
#define RGW_CAP_ALL             (RGW_CAP_READ | RGW_CAP_WRITE)

#define RGW_REST_SWIFT          0x1
#define RGW_REST_SWIFT_AUTH     0x2
#define RGW_REST_S3             0x4
#define RGW_REST_WEBSITE     0x8
#define RGW_REST_STS            0x10
#define RGW_REST_IAM            0x20

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
#define ERR_MALFORMED_XML        2029
#define ERR_USER_EXIST           2030
#define ERR_NOT_SLO_MANIFEST     2031
#define ERR_EMAIL_EXIST          2032
#define ERR_KEY_EXIST            2033
#define ERR_INVALID_SECRET_KEY   2034
#define ERR_INVALID_KEY_TYPE     2035
#define ERR_INVALID_CAP          2036
#define ERR_INVALID_TENANT_NAME  2037
#define ERR_WEBSITE_REDIRECT     2038
#define ERR_NO_SUCH_WEBSITE_CONFIGURATION 2039
#define ERR_AMZ_CONTENT_SHA256_MISMATCH 2040
#define ERR_NO_SUCH_LC           2041
#define ERR_NO_SUCH_USER         2042
#define ERR_NO_SUCH_SUBUSER      2043
#define ERR_MFA_REQUIRED         2044
#define ERR_NO_SUCH_CORS_CONFIGURATION 2045
#define ERR_NO_SUCH_OBJECT_LOCK_CONFIGURATION  2046
#define ERR_INVALID_RETENTION_PERIOD 2047
#define ERR_NO_SUCH_BUCKET_ENCRYPTION_CONFIGURATION 2048
#define ERR_USER_SUSPENDED       2100
#define ERR_INTERNAL_ERROR       2200
#define ERR_NOT_IMPLEMENTED      2201
#define ERR_SERVICE_UNAVAILABLE  2202
#define ERR_ROLE_EXISTS          2203
#define ERR_MALFORMED_DOC        2204
#define ERR_NO_ROLE_FOUND        2205
#define ERR_DELETE_CONFLICT      2206
#define ERR_NO_SUCH_BUCKET_POLICY  2207
#define ERR_INVALID_LOCATION_CONSTRAINT 2208
#define ERR_TAG_CONFLICT         2209
#define ERR_INVALID_TAG          2210
#define ERR_ZERO_IN_URL          2211
#define ERR_MALFORMED_ACL_ERROR  2212
#define ERR_ZONEGROUP_DEFAULT_PLACEMENT_MISCONFIGURATION 2213
#define ERR_INVALID_ENCRYPTION_ALGORITHM                 2214
#define ERR_INVALID_CORS_RULES_ERROR                     2215
#define ERR_NO_CORS_FOUND        2216
#define ERR_INVALID_WEBSITE_ROUTING_RULES_ERROR          2217
#define ERR_RATE_LIMITED         2218
#define ERR_POSITION_NOT_EQUAL_TO_LENGTH                 2219
#define ERR_OBJECT_NOT_APPENDABLE                        2220
#define ERR_INVALID_BUCKET_STATE                         2221
#define ERR_INVALID_OBJECT_STATE			 2222

#define ERR_BUSY_RESHARDING      2300
#define ERR_NO_SUCH_ENTITY       2301
#define ERR_LIMIT_EXCEEDED       2302

// STS Errors
#define ERR_PACKED_POLICY_TOO_LARGE 2400
#define ERR_INVALID_IDENTITY_TOKEN  2401

#define ERR_NO_SUCH_TAG_SET 2402

#ifndef UINT32_MAX
#define UINT32_MAX (0xffffffffu)
#endif

struct req_state;

typedef void *RGWAccessHandle;

/* Helper class used for RGWHTTPArgs parsing */
class NameVal
{
   const std::string str;
   std::string name;
   std::string val;
 public:
    explicit NameVal(const std::string& nv) : str(nv) {}

    int parse();

    std::string& get_name() { return name; }
    std::string& get_val() { return val; }
};

/** Stores the XML arguments associated with the HTTP request in req_state*/
class RGWHTTPArgs {
  std::string str, empty_str;
  std::map<std::string, std::string> val_map;
  std::map<std::string, std::string> sys_val_map;
  std::map<std::string, std::string> sub_resources;
  bool has_resp_modifier = false;
  bool admin_subresource_added = false;
 public:
  RGWHTTPArgs() = default;
  explicit RGWHTTPArgs(const std::string& s, const DoutPrefixProvider *dpp) {
      set(s);
      parse(dpp);
  }

  /** Set the arguments; as received */
  void set(const std::string& s) {
    has_resp_modifier = false;
    val_map.clear();
    sub_resources.clear();
    str = s;
  }
  /** parse the received arguments */
  int parse(const DoutPrefixProvider *dpp);
  void append(const std::string& name, const std::string& val);
  void remove(const std::string& name);
  /** Get the value for a specific argument parameter */
  const std::string& get(const std::string& name, bool *exists = NULL) const;
  boost::optional<const std::string&>
  get_optional(const std::string& name) const;
  int get_bool(const std::string& name, bool *val, bool *exists) const;
  int get_bool(const char *name, bool *val, bool *exists) const;
  void get_bool(const char *name, bool *val, bool def_val) const;
  int get_int(const char *name, int *val, int def_val) const;

  /** Get the value for specific system argument parameter */
  std::string sys_get(const std::string& name, bool *exists = nullptr) const;

  /** see if a parameter is contained in this RGWHTTPArgs */
  bool exists(const char *name) const {
    return (val_map.find(name) != std::end(val_map));
  }
  bool sub_resource_exists(const char *name) const {
    return (sub_resources.find(name) != std::end(sub_resources));
  }
  bool exist_obj_excl_sub_resource() const {
    const char* const obj_sub_resource[] = {"append", "torrent", "uploadId",
                                            "partNumber", "versionId"};
    for (unsigned i = 0; i != std::size(obj_sub_resource); i++) {
      if (sub_resource_exists(obj_sub_resource[i])) return true;
    }
    return false;
  }

  std::map<std::string, std::string>& get_params() {
    return val_map;
  }
  const std::map<std::string, std::string>& get_params() const {
    return val_map;
  }
  std::map<std::string, std::string>& get_sys_params() {
    return sys_val_map;
  }
  const std::map<std::string, std::string>& get_sys_params() const {
    return sys_val_map;
  }
  const std::map<std::string, std::string>& get_sub_resources() const {
    return sub_resources;
  }
  unsigned get_num_params() const {
    return val_map.size();
  }
  bool has_response_modifier() const {
    return has_resp_modifier;
  }
  void set_system() { /* make all system params visible */
    std::map<std::string, std::string>::iterator iter;
    for (iter = sys_val_map.begin(); iter != sys_val_map.end(); ++iter) {
      val_map[iter->first] = iter->second;
    }
  }
  const std::string& get_str() {
    return str;
  }
}; // RGWHTTPArgs

const char *rgw_conf_get(const std::map<std::string, std::string, ltstr_nocase>& conf_map, const char *name, const char *def_val);
boost::optional<const std::string&> rgw_conf_get_optional(const std::map<std::string, std::string, ltstr_nocase>& conf_map, const std::string& name);
int rgw_conf_get_int(const std::map<std::string, std::string, ltstr_nocase>& conf_map, const char *name, int def_val);
bool rgw_conf_get_bool(const std::map<std::string, std::string, ltstr_nocase>& conf_map, const char *name, bool def_val);

class RGWEnv;

class RGWConf {
  friend class RGWEnv;
  int enable_ops_log;
  int enable_usage_log;
  uint8_t defer_to_bucket_acls;
  void init(CephContext *cct);
public:
  RGWConf()
    : enable_ops_log(1),
      enable_usage_log(1),
      defer_to_bucket_acls(0) {
  }
};

class RGWEnv {
  std::map<std::string, std::string, ltstr_nocase> env_map;
  RGWConf conf;
public:
  void init(CephContext *cct);
  void init(CephContext *cct, char **envp);
  void set(std::string name, std::string val);
  const char *get(const char *name, const char *def_val = nullptr) const;
  boost::optional<const std::string&>
  get_optional(const std::string& name) const;
  int get_int(const char *name, int def_val = 0) const;
  bool get_bool(const char *name, bool def_val = 0);
  size_t get_size(const char *name, size_t def_val = 0) const;
  bool exists(const char *name) const;
  bool exists_prefix(const char *prefix) const;
  void remove(const char *name);
  const std::map<std::string, std::string, ltstr_nocase>& get_map() const { return env_map; }
  int get_enable_ops_log() const {
    return conf.enable_ops_log;
  }

  int get_enable_usage_log() const {
    return conf.enable_usage_log;
  }

  int get_defer_to_bucket_acls() const {
    return conf.defer_to_bucket_acls;
  }
};

// return true if the connection is secure. this either means that the
// connection arrived via ssl, or was forwarded as https by a trusted proxy
bool rgw_transport_is_secure(CephContext *cct, const RGWEnv& env);

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

void encode_json(const char *name, const obj_version& v, Formatter *f);
void encode_json(const char *name, const RGWUserCaps& val, Formatter *f);

void decode_json_obj(obj_version& v, JSONObj *obj);

enum RGWIdentityType
{
  TYPE_NONE=0,
  TYPE_RGW=1,
  TYPE_KEYSTONE=2,
  TYPE_LDAP=3,
  TYPE_ROLE=4,
  TYPE_WEB=5,
};

void encode_json(const char *name, const rgw_placement_rule& val, ceph::Formatter *f);
void decode_json_obj(rgw_placement_rule& v, JSONObj *obj);

inline std::ostream& operator<<(std::ostream& out, const rgw_placement_rule& rule) {
  return out << rule.to_str();
}

class RateLimiter;
struct RGWRateLimitInfo {
  int64_t max_write_ops;
  int64_t max_read_ops;
  int64_t max_write_bytes;
  int64_t max_read_bytes;
  bool enabled = false;
  RGWRateLimitInfo()
    : max_write_ops(0), max_read_ops(0), max_write_bytes(0), max_read_bytes(0)  {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(max_write_ops, bl);
    encode(max_read_ops, bl);
    encode(max_write_bytes, bl);
    encode(max_read_bytes, bl);
    encode(enabled, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(max_write_ops,bl);
    decode(max_read_ops, bl);
    decode(max_write_bytes,bl);
    decode(max_read_bytes, bl);
    decode(enabled, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;

  void decode_json(JSONObj *obj);

};
WRITE_CLASS_ENCODER(RGWRateLimitInfo)

struct RGWUserInfo
{
  rgw_user user_id;
  std::string display_name;
  std::string user_email;
  std::map<std::string, RGWAccessKey> access_keys;
  std::map<std::string, RGWAccessKey> swift_keys;
  std::map<std::string, RGWSubUser> subusers;
  __u8 suspended;
  int32_t max_buckets;
  uint32_t op_mask;
  RGWUserCaps caps;
  __u8 admin;
  __u8 system;
  rgw_placement_rule default_placement;
  std::list<std::string> placement_tags;
  RGWQuotaInfo bucket_quota;
  std::map<int, std::string> temp_url_keys;
  RGWQuotaInfo user_quota;
  uint32_t type;
  std::set<std::string> mfa_ids;

  RGWUserInfo()
    : suspended(0),
      max_buckets(RGW_DEFAULT_MAX_BUCKETS),
      op_mask(RGW_OP_TYPE_ALL),
      admin(0),
      system(0),
      type(TYPE_NONE) {
  }

  RGWAccessKey* get_key(const std::string& access_key) {
    if (access_keys.empty())
      return nullptr;

    auto k = access_keys.find(access_key);
    if (k == access_keys.end())
      return nullptr;
    else
      return &(k->second);
  }

  void encode(bufferlist& bl) const {
     ENCODE_START(22, 9, bl);
     encode((uint64_t)0, bl); // old auid
     std::string access_key;
     std::string secret_key;
     if (!access_keys.empty()) {
       std::map<std::string, RGWAccessKey>::const_iterator iter = access_keys.begin();
       const RGWAccessKey& k = iter->second;
       access_key = k.id;
       secret_key = k.key;
     }
     encode(access_key, bl);
     encode(secret_key, bl);
     encode(display_name, bl);
     encode(user_email, bl);
     std::string swift_name;
     std::string swift_key;
     if (!swift_keys.empty()) {
       std::map<std::string, RGWAccessKey>::const_iterator iter = swift_keys.begin();
       const RGWAccessKey& k = iter->second;
       swift_name = k.id;
       swift_key = k.key;
     }
     encode(swift_name, bl);
     encode(swift_key, bl);
     encode(user_id.id, bl);
     encode(access_keys, bl);
     encode(subusers, bl);
     encode(suspended, bl);
     encode(swift_keys, bl);
     encode(max_buckets, bl);
     encode(caps, bl);
     encode(op_mask, bl);
     encode(system, bl);
     encode(default_placement, bl);
     encode(placement_tags, bl);
     encode(bucket_quota, bl);
     encode(temp_url_keys, bl);
     encode(user_quota, bl);
     encode(user_id.tenant, bl);
     encode(admin, bl);
     encode(type, bl);
     encode(mfa_ids, bl);
     {
       std::string assumed_role_arn; // removed
       encode(assumed_role_arn, bl);
     }
     encode(user_id.ns, bl);
     ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
     DECODE_START_LEGACY_COMPAT_LEN_32(22, 9, 9, bl);
     if (struct_v >= 2) {
       uint64_t old_auid;
       decode(old_auid, bl);
     }
     std::string access_key;
     std::string secret_key;
    decode(access_key, bl);
    decode(secret_key, bl);
    if (struct_v < 6) {
      RGWAccessKey k;
      k.id = access_key;
      k.key = secret_key;
      access_keys[access_key] = k;
    }
    decode(display_name, bl);
    decode(user_email, bl);
    /* We populate swift_keys map later nowadays, but we have to decode. */
    std::string swift_name;
    std::string swift_key;
    if (struct_v >= 3) decode(swift_name, bl);
    if (struct_v >= 4) decode(swift_key, bl);
    if (struct_v >= 5)
      decode(user_id.id, bl);
    else
      user_id.id = access_key;
    if (struct_v >= 6) {
      decode(access_keys, bl);
      decode(subusers, bl);
    }
    suspended = 0;
    if (struct_v >= 7) {
      decode(suspended, bl);
    }
    if (struct_v >= 8) {
      decode(swift_keys, bl);
    }
    if (struct_v >= 10) {
      decode(max_buckets, bl);
    } else {
      max_buckets = RGW_DEFAULT_MAX_BUCKETS;
    }
    if (struct_v >= 11) {
      decode(caps, bl);
    }
    if (struct_v >= 12) {
      decode(op_mask, bl);
    } else {
      op_mask = RGW_OP_TYPE_ALL;
    }
    if (struct_v >= 13) {
      decode(system, bl);
      decode(default_placement, bl);
      decode(placement_tags, bl); /* tags of allowed placement rules */
    }
    if (struct_v >= 14) {
      decode(bucket_quota, bl);
    }
    if (struct_v >= 15) {
     decode(temp_url_keys, bl);
    }
    if (struct_v >= 16) {
      decode(user_quota, bl);
    }
    if (struct_v >= 17) {
      decode(user_id.tenant, bl);
    } else {
      user_id.tenant.clear();
    }
    if (struct_v >= 18) {
      decode(admin, bl);
    }
    if (struct_v >= 19) {
      decode(type, bl);
    }
    if (struct_v >= 20) {
      decode(mfa_ids, bl);
    }
    if (struct_v >= 21) {
      std::string assumed_role_arn; // removed
      decode(assumed_role_arn, bl);
    }
    if (struct_v >= 22) {
      decode(user_id.ns, bl);
    } else {
      user_id.ns.clear();
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<RGWUserInfo*>& o);

  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWUserInfo)

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

  void apply_write();

  void clear() {
    read_version = obj_version();
    write_version = obj_version();
  }

  void generate_new_write_ver(CephContext *cct);
};

inline std::ostream& operator<<(std::ostream& out, const obj_version &v)
{
  out << v.tag << ":" << v.ver;
  return out;
}

inline std::ostream& operator<<(std::ostream& out, const RGWObjVersionTracker &ot)
{
  out << "{r=" << ot.read_version << ",w=" << ot.write_version << "}";
  return out;
}

enum RGWBucketFlags {
  BUCKET_SUSPENDED = 0x1,
  BUCKET_VERSIONED = 0x2,
  BUCKET_VERSIONS_SUSPENDED = 0x4,
  BUCKET_DATASYNC_DISABLED = 0X8,
  BUCKET_MFA_ENABLED = 0X10,
  BUCKET_OBJ_LOCK_ENABLED = 0X20,
};

class RGWSI_Zone;

struct RGWBucketInfo {
  rgw_bucket bucket;
  rgw_user owner;
  uint32_t flags{0};
  std::string zonegroup;
  ceph::real_time creation_time;
  rgw_placement_rule placement_rule;
  bool has_instance_obj{false};
  RGWObjVersionTracker objv_tracker; /* we don't need to serialize this, for runtime tracking */
  RGWQuotaInfo quota;

  // layout of bucket index objects
  rgw::BucketLayout layout;

  // Represents the number of bucket index object shards:
  //   - value of 0 indicates there is no sharding (this is by default
  //     before this feature is implemented).
  //   - value of UINT32_T::MAX indicates this is a blind bucket.

  // Represents the shard number for blind bucket.
  const static uint32_t NUM_SHARDS_BLIND_BUCKET;

  bool requester_pays{false};

  bool has_website{false};
  RGWBucketWebsiteConf website_conf;

  bool swift_versioning{false};
  std::string swift_ver_location;

  std::map<std::string, uint32_t> mdsearch_config;

  // resharding
  cls_rgw_reshard_status reshard_status{cls_rgw_reshard_status::NOT_RESHARDING};
  std::string new_bucket_instance_id;

  RGWObjectLock obj_lock;

  std::optional<rgw_sync_policy_info> sync_policy;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& bl);

  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<RGWBucketInfo*>& o);

  void decode_json(JSONObj *obj);

  bool versioned() const { return (flags & BUCKET_VERSIONED) != 0; }
  int versioning_status() const { return flags & (BUCKET_VERSIONED | BUCKET_VERSIONS_SUSPENDED | BUCKET_MFA_ENABLED); }
  bool versioning_enabled() const { return (versioning_status() & (BUCKET_VERSIONED | BUCKET_VERSIONS_SUSPENDED)) == BUCKET_VERSIONED; }
  bool mfa_enabled() const { return (versioning_status() & BUCKET_MFA_ENABLED) != 0; }
  bool datasync_flag_enabled() const { return (flags & BUCKET_DATASYNC_DISABLED) == 0; }
  bool obj_lock_enabled() const { return (flags & BUCKET_OBJ_LOCK_ENABLED) != 0; }

  bool has_swift_versioning() const {
    /* A bucket may be versioned through one mechanism only. */
    return swift_versioning && !versioned();
  }

  void set_sync_policy(rgw_sync_policy_info&& policy);

  bool empty_sync_policy() const;

  RGWBucketInfo();
  ~RGWBucketInfo();
};
WRITE_CLASS_ENCODER(RGWBucketInfo)

struct RGWBucketEntryPoint
{
  rgw_bucket bucket;
  rgw_user owner;
  ceph::real_time creation_time;
  bool linked;

  bool has_bucket_info;
  RGWBucketInfo old_bucket_info;

  RGWBucketEntryPoint() : linked(false), has_bucket_info(false) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(10, 8, bl);
    encode(bucket, bl);
    encode(owner.id, bl);
    encode(linked, bl);
    uint64_t ctime = (uint64_t)real_clock::to_time_t(creation_time);
    encode(ctime, bl);
    encode(owner, bl);
    encode(creation_time, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    auto orig_iter = bl;
    DECODE_START_LEGACY_COMPAT_LEN_32(10, 4, 4, bl);
    if (struct_v < 8) {
      /* ouch, old entry, contains the bucket info itself */
      old_bucket_info.decode(orig_iter);
      has_bucket_info = true;
      return;
    }
    has_bucket_info = false;
    decode(bucket, bl);
    decode(owner.id, bl);
    decode(linked, bl);
    uint64_t ctime;
    decode(ctime, bl);
    if (struct_v < 10) {
      creation_time = real_clock::from_time_t((time_t)ctime);
    }
    if (struct_v >= 9) {
      decode(owner, bl);
    }
    if (struct_v >= 10) {
      decode(creation_time, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<RGWBucketEntryPoint*>& o);
};
WRITE_CLASS_ENCODER(RGWBucketEntryPoint)

struct RGWStorageStats
{
  RGWObjCategory category;
  uint64_t size;
  uint64_t size_rounded;
  uint64_t num_objects;
  uint64_t size_utilized{0}; //< size after compression, encryption
  bool dump_utilized;        // whether dump should include utilized values

  RGWStorageStats(bool _dump_utilized=true)
    : category(RGWObjCategory::None),
      size(0),
      size_rounded(0),
      num_objects(0),
      dump_utilized(_dump_utilized)
  {}

  void dump(Formatter *f) const;
}; // RGWStorageStats

class RGWEnv;

/* Namespaced forward declarations. */
namespace rgw {
  namespace auth {
    namespace s3 {
      class AWSBrowserUploadAbstractor;
      class STSEngine;
    }
    class Completer;
  }
  namespace io {
    class BasicClient;
  }
}

using meta_map_t = boost::container::flat_map <std::string, std::string>;

struct req_info {
  const RGWEnv *env;
  RGWHTTPArgs args;
  meta_map_t x_meta_map;
  meta_map_t crypt_attribute_map;

  std::string host;
  const char *method;
  std::string script_uri;
  std::string request_uri;
  std::string request_uri_aws4;
  std::string effective_uri;
  std::string request_params;
  std::string domain;
  std::string storage_class;

  req_info(CephContext *cct, const RGWEnv *env);
  void rebuild_from(req_info& src);
  void init_meta_info(const DoutPrefixProvider *dpp, bool *found_bad_meta, const int prot_flags);
};

struct req_init_state {
  /* Keeps [[tenant]:]bucket until we parse the token. */
  std::string url_bucket;
  std::string src_bucket;
};

#include "rgw_auth.h"

class RGWObjectCtx;
class RGWSysObjectCtx;

/** Store all the state necessary to complete and respond to an HTTP request*/
struct req_state : DoutPrefixProvider {
  CephContext *cct;
  rgw::io::BasicClient *cio{nullptr};
  http_op op{OP_UNKNOWN};
  RGWOpType op_type{};
  std::shared_ptr<RateLimiter> ratelimit_data;
  RGWRateLimitInfo user_ratelimit;
  RGWRateLimitInfo bucket_ratelimit;
  std::string ratelimit_bucket_marker;
  std::string ratelimit_user_name;
  bool content_started{false};
  int format{0};
  ceph::Formatter *formatter{nullptr};
  std::string decoded_uri;
  std::string relative_uri;
  const char *length{nullptr};
  int64_t content_length{0};
  std::map<std::string, std::string> generic_attrs;
  rgw_err err;
  bool expect_cont{false};
  uint64_t obj_size{0};
  bool enable_ops_log;
  bool enable_usage_log;
  uint8_t defer_to_bucket_acls;
  uint32_t perm_mask{0};

  /* Set once when url_bucket is parsed and not violated thereafter. */
  std::string account_name;

  std::string bucket_tenant;
  std::string bucket_name;

  /* bucket is only created in rgw_build_bucket_policies() and should never be
   * overwritten */
  std::unique_ptr<rgw::sal::Bucket> bucket;
  std::unique_ptr<rgw::sal::Object> object;
  std::string src_tenant_name;
  std::string src_bucket_name;
  std::unique_ptr<rgw::sal::Object> src_object;
  ACLOwner bucket_owner;
  ACLOwner owner;

  std::string zonegroup_name;
  std::string zonegroup_endpoint;
  std::string bucket_instance_id;
  int bucket_instance_shard_id{-1};
  std::string redirect_zone_endpoint;

  std::string redirect;

  real_time bucket_mtime;
  std::map<std::string, ceph::bufferlist> bucket_attrs;
  bool bucket_exists{false};
  rgw_placement_rule dest_placement;

  bool has_bad_meta{false};

  std::unique_ptr<rgw::sal::User> user;

  struct {
    /* TODO(rzarzynski): switch out to the static_ptr for both members. */

    /* Object having the knowledge about an authenticated identity and allowing
     * to apply it during the authorization phase (verify_permission() methods
     * of a given RGWOp). Thus, it bounds authentication and authorization steps
     * through a well-defined interface. For more details, see rgw_auth.h. */
    std::unique_ptr<rgw::auth::Identity> identity;

    std::shared_ptr<rgw::auth::Completer> completer;

    /* A container for credentials of the S3's browser upload. It's necessary
     * because: 1) the ::authenticate() method of auth engines and strategies
     * take req_state only; 2) auth strategies live much longer than RGWOps -
     * there is no way to pass additional data dependencies through ctors. */
    class {
      /* Writer. */
      friend class RGWPostObj_ObjStore_S3;
      /* Reader. */
      friend class rgw::auth::s3::AWSBrowserUploadAbstractor;
      friend class rgw::auth::s3::STSEngine;

      std::string access_key;
      std::string signature;
      std::string x_amz_algorithm;
      std::string x_amz_credential;
      std::string x_amz_date;
      std::string x_amz_security_token;
      ceph::bufferlist encoded_policy;
    } s3_postobj_creds;
  } auth;

  std::unique_ptr<RGWAccessControlPolicy> user_acl;
  std::unique_ptr<RGWAccessControlPolicy> bucket_acl;
  std::unique_ptr<RGWAccessControlPolicy> object_acl;

  rgw::IAM::Environment env;
  boost::optional<rgw::IAM::Policy> iam_policy;
  boost::optional<PublicAccessBlockConfiguration> bucket_access_conf;
  std::vector<rgw::IAM::Policy> iam_user_policies;

  /* Is the request made by an user marked as a system one?
   * Being system user means we also have the admin status. */
  bool system_request{false};

  std::string canned_acl;
  bool has_acl_header{false};
  bool local_source{false}; /* source is local */

  int prot_flags{0};

  /* Content-Disposition override for TempURL of Swift API. */
  struct {
    std::string override;
    std::string fallback;
  } content_disp;

  std::string host_id;

  req_info info;
  req_init_state init_state;

  using Clock = ceph::coarse_real_clock;
  Clock::time_point time;

  Clock::duration time_elapsed() const { return Clock::now() - time; }

  RGWObjectCtx *obj_ctx{nullptr};
  std::string dialect;
  std::string req_id;
  std::string trans_id;
  uint64_t id;

  RGWObjTags tagset;

  bool mfa_verified{false};

  /// optional coroutine context
  optional_yield yield{null_yield};

  //token claims from STS token for ops log (can be used for Keystone token also)
  std::vector<std::string> token_claims;

  std::vector<rgw::IAM::Policy> session_policies;

  jspan trace;

  //Principal tags that come in as part of AssumeRoleWithWebIdentity
  std::vector<std::pair<std::string, std::string>> principal_tags;

  req_state(CephContext* _cct, RGWEnv* e, uint64_t id);
  ~req_state();


  void set_user(std::unique_ptr<rgw::sal::User>& u) { user.swap(u); }
  bool is_err() const { return err.is_err(); }

  // implements DoutPrefixProvider
  std::ostream& gen_prefix(std::ostream& out) const override;
  CephContext* get_cct() const override { return cct; }
  unsigned get_subsys() const override { return ceph_subsys_rgw; }
};

void set_req_state_err(struct req_state*, int);
void set_req_state_err(struct req_state*, int, const std::string&);
void set_req_state_err(struct rgw_err&, int, const int);
void dump(struct req_state*);

/** Store basic data on bucket */
struct RGWBucketEnt {
  rgw_bucket bucket;
  size_t size;
  size_t size_rounded;
  ceph::real_time creation_time;
  uint64_t count;

  /* The placement_rule is necessary to calculate per-storage-policy statics
   * of the Swift API. Although the info available in RGWBucketInfo, we need
   * to duplicate it here to not affect the performance of buckets listing. */
  rgw_placement_rule placement_rule;

  RGWBucketEnt()
    : size(0),
      size_rounded(0),
      count(0) {
  }
  RGWBucketEnt(const RGWBucketEnt&) = default;
  RGWBucketEnt(RGWBucketEnt&&) = default;
  explicit RGWBucketEnt(const rgw_user& u, cls_user_bucket_entry&& e)
    : bucket(u, std::move(e.bucket)),
      size(e.size),
      size_rounded(e.size_rounded),
      creation_time(e.creation_time),
      count(e.count) {
  }

  RGWBucketEnt& operator=(const RGWBucketEnt&) = default;

  void convert(cls_user_bucket_entry *b) const {
    bucket.convert(&b->bucket);
    b->size = size;
    b->size_rounded = size_rounded;
    b->creation_time = creation_time;
    b->count = count;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(7, 5, bl);
    uint64_t s = size;
    __u32 mt = ceph::real_clock::to_time_t(creation_time);
    std::string empty_str;  // originally had the bucket name here, but we encode bucket later
    encode(empty_str, bl);
    encode(s, bl);
    encode(mt, bl);
    encode(count, bl);
    encode(bucket, bl);
    s = size_rounded;
    encode(s, bl);
    encode(creation_time, bl);
    encode(placement_rule, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(7, 5, 5, bl);
    __u32 mt;
    uint64_t s;
    std::string empty_str;  // backward compatibility
    decode(empty_str, bl);
    decode(s, bl);
    decode(mt, bl);
    size = s;
    if (struct_v < 6) {
      creation_time = ceph::real_clock::from_time_t(mt);
    }
    if (struct_v >= 2)
      decode(count, bl);
    if (struct_v >= 3)
      decode(bucket, bl);
    if (struct_v >= 4)
      decode(s, bl);
    size_rounded = s;
    if (struct_v >= 6)
      decode(creation_time, bl);
    if (struct_v >= 7)
      decode(placement_rule, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<RGWBucketEnt*>& o);
};
WRITE_CLASS_ENCODER(RGWBucketEnt)

struct rgw_cache_entry_info {
  std::string cache_locator;
  uint64_t gen;

  rgw_cache_entry_info() : gen(0) {}
};

inline std::ostream& operator<<(std::ostream& out, const rgw_obj &o) {
  return out << o.bucket.name << ":" << o.get_oid();
}

struct multipart_upload_info
{
  rgw_placement_rule dest_placement;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(dest_placement, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(dest_placement, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(multipart_upload_info)

static inline void buf_to_hex(const unsigned char* const buf,
                              const size_t len,
                              char* const str)
{
  str[0] = '\0';
  for (size_t i = 0; i < len; i++) {
    ::sprintf(&str[i*2], "%02x", static_cast<int>(buf[i]));
  }
}

template<size_t N> static inline std::array<char, N * 2 + 1>
buf_to_hex(const std::array<unsigned char, N>& buf)
{
  static_assert(N > 0, "The input array must be at least one element long");

  std::array<char, N * 2 + 1> hex_dest;
  buf_to_hex(buf.data(), N, hex_dest.data());
  return hex_dest;
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
      return d;
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

  return (strcasecmp(s, "true") == 0 ||
          strcasecmp(s, "on") == 0 ||
          strcasecmp(s, "yes") == 0 ||
          strcasecmp(s, "1") == 0);
}

static inline void append_rand_alpha(CephContext *cct, const std::string& src, std::string& dest, int len)
{
  dest = src;
  char buf[len + 1];
  gen_rand_alphanumeric(cct, buf, len);
  dest.append("_");
  dest.append(buf);
}

static inline uint64_t rgw_rounded_kb(uint64_t bytes)
{
  return (bytes + 1023) / 1024;
}

static inline uint64_t rgw_rounded_objsize(uint64_t bytes)
{
  return ((bytes + 4095) & ~4095);
}

static inline uint64_t rgw_rounded_objsize_kb(uint64_t bytes)
{
  return ((bytes + 4095) & ~4095) / 1024;
}

/* implement combining step, S3 header canonicalization;  k is a
 * valid header and in lc form */
void rgw_add_amz_meta_header(
  meta_map_t& x_meta_map,
  const std::string& k,
  const std::string& v);

enum rgw_set_action_if_set {
  DISCARD=0, OVERWRITE, APPEND
};

bool rgw_set_amz_meta_header(
  meta_map_t& x_meta_map,
  const std::string& k,
  const std::string& v, rgw_set_action_if_set f);

extern std::string rgw_string_unquote(const std::string& s);
extern void parse_csv_string(const std::string& ival, std::vector<std::string>& ovals);
extern int parse_key_value(std::string& in_str, std::string& key, std::string& val);
extern int parse_key_value(std::string& in_str, const char *delim, std::string& key, std::string& val);

extern boost::optional<std::pair<std::string_view,std::string_view>>
parse_key_value(const std::string_view& in_str,
                const std::string_view& delim);
extern boost::optional<std::pair<std::string_view,std::string_view>>
parse_key_value(const std::string_view& in_str);


/** time parsing */
extern int parse_time(const char *time_str, real_time *time);
extern bool parse_rfc2616(const char *s, struct tm *t);
extern bool parse_iso8601(const char *s, struct tm *t, uint32_t *pns = NULL, bool extended_format = true);
extern std::string rgw_trim_whitespace(const std::string& src);
extern std::string_view rgw_trim_whitespace(const std::string_view& src);
extern std::string rgw_trim_quotes(const std::string& val);

extern void rgw_to_iso8601(const real_time& t, char *dest, int buf_size);
extern void rgw_to_iso8601(const real_time& t, std::string *dest);
extern std::string rgw_to_asctime(const utime_t& t);

struct perm_state_base {
  CephContext *cct;
  const rgw::IAM::Environment& env;
  rgw::auth::Identity *identity;
  const RGWBucketInfo bucket_info;
  int perm_mask;
  bool defer_to_bucket_acls;
  boost::optional<PublicAccessBlockConfiguration> bucket_access_conf;

  perm_state_base(CephContext *_cct,
                  const rgw::IAM::Environment& _env,
                  rgw::auth::Identity *_identity,
                  const RGWBucketInfo& _bucket_info,
                  int _perm_mask,
                  bool _defer_to_bucket_acls,
                  boost::optional<PublicAccessBlockConfiguration> _bucket_acess_conf = boost::none) :
                                                cct(_cct),
                                                env(_env),
                                                identity(_identity),
                                                bucket_info(_bucket_info),
                                                perm_mask(_perm_mask),
                                                defer_to_bucket_acls(_defer_to_bucket_acls),
                                                bucket_access_conf(_bucket_acess_conf)
  {}

  virtual ~perm_state_base() {}

  virtual const char *get_referer() const = 0;
  virtual std::optional<bool> get_request_payer() const = 0; /*
                                                              * empty state means that request_payer param was not passed in
                                                              */

};

struct perm_state : public perm_state_base {
  const char *referer;
  bool request_payer;

  perm_state(CephContext *_cct,
             const rgw::IAM::Environment& _env,
             rgw::auth::Identity *_identity,
             const RGWBucketInfo& _bucket_info,
             int _perm_mask,
             bool _defer_to_bucket_acls,
             const char *_referer,
             bool _request_payer) : perm_state_base(_cct,
                                                    _env,
                                                    _identity,
                                                    _bucket_info,
                                                    _perm_mask,
                                                    _defer_to_bucket_acls),
                                    referer(_referer),
                                    request_payer(_request_payer) {}

  const char *get_referer() const override {
    return referer;
  }

  std::optional<bool> get_request_payer() const override {
    return request_payer;
  }
};

/** Check if the req_state's user has the necessary permissions
 * to do the requested action  */
bool verify_bucket_permission_no_policy(
  const DoutPrefixProvider* dpp,
  struct perm_state_base * const s,
  RGWAccessControlPolicy * const user_acl,
  RGWAccessControlPolicy * const bucket_acl,
  const int perm);

bool verify_user_permission_no_policy(const DoutPrefixProvider* dpp,
                                      struct perm_state_base * const s,
                                      RGWAccessControlPolicy * const user_acl,
                                      const int perm);

bool verify_object_permission_no_policy(const DoutPrefixProvider* dpp,
                                        struct perm_state_base * const s,
					RGWAccessControlPolicy * const user_acl,
					RGWAccessControlPolicy * const bucket_acl,
					RGWAccessControlPolicy * const object_acl,
					const int perm);

/** Check if the req_state's user has the necessary permissions
 * to do the requested action */
rgw::IAM::Effect eval_identity_or_session_policies(const std::vector<rgw::IAM::Policy>& user_policies,
                          const rgw::IAM::Environment& env,
                          const uint64_t op,
                          const rgw::ARN& arn);
bool verify_user_permission(const DoutPrefixProvider* dpp,
                            struct req_state * const s,
                            RGWAccessControlPolicy * const user_acl,
                            const std::vector<rgw::IAM::Policy>& user_policies,
                            const std::vector<rgw::IAM::Policy>& session_policies,
                            const rgw::ARN& res,
                            const uint64_t op);
bool verify_user_permission_no_policy(const DoutPrefixProvider* dpp,
                                      struct req_state * const s,
                                      RGWAccessControlPolicy * const user_acl,
                                      const int perm);
bool verify_user_permission(const DoutPrefixProvider* dpp,
                            struct req_state * const s,
                            const rgw::ARN& res,
                            const uint64_t op);
bool verify_user_permission_no_policy(const DoutPrefixProvider* dpp,
                                      struct req_state * const s,
                                      int perm);
bool verify_bucket_permission(
  const DoutPrefixProvider* dpp,
  struct req_state * const s,
  const rgw_bucket& bucket,
  RGWAccessControlPolicy * const user_acl,
  RGWAccessControlPolicy * const bucket_acl,
  const boost::optional<rgw::IAM::Policy>& bucket_policy,
  const std::vector<rgw::IAM::Policy>& identity_policies,
  const std::vector<rgw::IAM::Policy>& session_policies,
  const uint64_t op);
bool verify_bucket_permission(const DoutPrefixProvider* dpp, struct req_state * const s, const uint64_t op);
bool verify_bucket_permission_no_policy(
  const DoutPrefixProvider* dpp,
  struct req_state * const s,
  RGWAccessControlPolicy * const user_acl,
  RGWAccessControlPolicy * const bucket_acl,
  const int perm);
bool verify_bucket_permission_no_policy(const DoutPrefixProvider* dpp,
                                        struct req_state * const s,
					const int perm);
int verify_bucket_owner_or_policy(struct req_state* const s,
				  const uint64_t op);
extern bool verify_object_permission(
  const DoutPrefixProvider* dpp,
  struct req_state * const s,
  const rgw_obj& obj,
  RGWAccessControlPolicy * const user_acl,
  RGWAccessControlPolicy * const bucket_acl,
  RGWAccessControlPolicy * const object_acl,
  const boost::optional<rgw::IAM::Policy>& bucket_policy,
  const std::vector<rgw::IAM::Policy>& identity_policies,
  const std::vector<rgw::IAM::Policy>& session_policies,
  const uint64_t op);
extern bool verify_object_permission(const DoutPrefixProvider* dpp, struct req_state *s, uint64_t op);
extern bool verify_object_permission_no_policy(
  const DoutPrefixProvider* dpp,
  struct req_state * const s,
  RGWAccessControlPolicy * const user_acl,
  RGWAccessControlPolicy * const bucket_acl,
  RGWAccessControlPolicy * const object_acl,
  int perm);
extern bool verify_object_permission_no_policy(const DoutPrefixProvider* dpp, struct req_state *s,
					       int perm);
extern int verify_object_lock(
  const DoutPrefixProvider* dpp,
  const rgw::sal::Attrs& attrs,
  const bool bypass_perm,
  const bool bypass_governance_mode);

/** Convert an input URL into a sane object name
 * by converting %-escaped std::strings into characters, etc*/
extern void rgw_uri_escape_char(char c, std::string& dst);
extern std::string url_decode(const std::string_view& src_str,
                              bool in_query = false);
extern void url_encode(const std::string& src, std::string& dst,
                       bool encode_slash = true);
extern std::string url_encode(const std::string& src, bool encode_slash = true);
extern std::string url_remove_prefix(const std::string& url); // Removes hhtp, https and www from url
/* destination should be CEPH_CRYPTO_HMACSHA1_DIGESTSIZE bytes long */
extern void calc_hmac_sha1(const char *key, int key_len,
                          const char *msg, int msg_len, char *dest);

static inline sha1_digest_t
calc_hmac_sha1(const std::string_view& key, const std::string_view& msg) {
  sha1_digest_t dest;
  calc_hmac_sha1(key.data(), key.size(), msg.data(), msg.size(),
                 reinterpret_cast<char*>(dest.v));
  return dest;
}

/* destination should be CEPH_CRYPTO_HMACSHA256_DIGESTSIZE bytes long */
extern void calc_hmac_sha256(const char *key, int key_len,
                             const char *msg, int msg_len,
                             char *dest);

static inline sha256_digest_t
calc_hmac_sha256(const char *key, const int key_len,
                 const char *msg, const int msg_len) {
  sha256_digest_t dest;
  calc_hmac_sha256(key, key_len, msg, msg_len,
                   reinterpret_cast<char*>(dest.v));
  return dest;
}

static inline sha256_digest_t
calc_hmac_sha256(const std::string_view& key, const std::string_view& msg) {
  sha256_digest_t dest;
  calc_hmac_sha256(key.data(), key.size(),
                   msg.data(), msg.size(),
                   reinterpret_cast<char*>(dest.v));
  return dest;
}

static inline sha256_digest_t
calc_hmac_sha256(const sha256_digest_t &key,
                 const std::string_view& msg) {
  sha256_digest_t dest;
  calc_hmac_sha256(reinterpret_cast<const char*>(key.v), sha256_digest_t::SIZE,
                   msg.data(), msg.size(),
                   reinterpret_cast<char*>(dest.v));
  return dest;
}

static inline sha256_digest_t
calc_hmac_sha256(const std::vector<unsigned char>& key,
                 const std::string_view& msg) {
  sha256_digest_t dest;
  calc_hmac_sha256(reinterpret_cast<const char*>(key.data()), key.size(),
                   msg.data(), msg.size(),
                   reinterpret_cast<char*>(dest.v));
  return dest;
}

template<size_t KeyLenN>
static inline sha256_digest_t
calc_hmac_sha256(const std::array<unsigned char, KeyLenN>& key,
                 const std::string_view& msg) {
  sha256_digest_t dest;
  calc_hmac_sha256(reinterpret_cast<const char*>(key.data()), key.size(),
                   msg.data(), msg.size(),
                   reinterpret_cast<char*>(dest.v));
  return dest;
}

extern sha256_digest_t calc_hash_sha256(const std::string_view& msg);

extern ceph::crypto::SHA256* calc_hash_sha256_open_stream();
extern void calc_hash_sha256_update_stream(ceph::crypto::SHA256* hash,
                                           const char* msg,
                                           int len);
extern std::string calc_hash_sha256_close_stream(ceph::crypto::SHA256** phash);
extern std::string calc_hash_sha256_restart_stream(ceph::crypto::SHA256** phash);

extern int rgw_parse_op_type_list(const std::string& str, uint32_t *perm);

static constexpr uint32_t MATCH_POLICY_ACTION = 0x01;
static constexpr uint32_t MATCH_POLICY_RESOURCE = 0x02;
static constexpr uint32_t MATCH_POLICY_ARN = 0x04;
static constexpr uint32_t MATCH_POLICY_STRING = 0x08;

extern bool match_policy(std::string_view pattern, std::string_view input,
                         uint32_t flag);

extern std::string camelcase_dash_http_attr(const std::string& orig, bool convert2dash = true);
extern std::string lowercase_dash_http_attr(const std::string& orig, bool bidirection = false);

void rgw_setup_saved_curl_handles();
void rgw_release_all_curl_handles();

static inline void rgw_escape_str(const std::string& s, char esc_char,
				  char special_char, std::string *dest)
{
  const char *src = s.c_str();
  char dest_buf[s.size() * 2 + 1];
  char *destp = dest_buf;

  for (size_t i = 0; i < s.size(); i++) {
    char c = src[i];
    if (c == esc_char || c == special_char) {
      *destp++ = esc_char;
    }
    *destp++ = c;
  }
  *destp++ = '\0';
  *dest = dest_buf;
}

static inline ssize_t rgw_unescape_str(const std::string& s, ssize_t ofs,
				       char esc_char, char special_char,
				       std::string *dest)
{
  const char *src = s.c_str();
  char dest_buf[s.size() + 1];
  char *destp = dest_buf;
  bool esc = false;

  dest_buf[0] = '\0';

  for (size_t i = ofs; i < s.size(); i++) {
    char c = src[i];
    if (!esc && c == esc_char) {
      esc = true;
      continue;
    }
    if (!esc && c == special_char) {
      *destp = '\0';
      *dest = dest_buf;
      return (ssize_t)i + 1;
    }
    *destp++ = c;
    esc = false;
  }
  *destp = '\0';
  *dest = dest_buf;
  return std::string::npos;
}

static inline std::string rgw_bl_str(ceph::buffer::list& raw)
{
  size_t len = raw.length();
  std::string s(raw.c_str(), len);
  while (len && !s[len - 1]) {
    --len;
    s.resize(len);
  }
  return s;
}

template <typename T>
int decode_bl(bufferlist& bl, T& t)
{
  auto iter = bl.cbegin();
  try {
    decode(t, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }
  return 0;
}
