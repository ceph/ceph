// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

/* N.B., this header defines fundamental serialized types.  Do not
 * introduce changes or include files which can only be compiled in
 * radosgw or OSD contexts (e.g., rgw_sal.h, rgw_common.h)
 */

#pragma once

#include <string>
#include <fmt/format.h>

#include "include/types.h"
#include "rgw_compression_types.h"
#include "rgw_pool_types.h"
#include "rgw_acl_types.h"
#include "rgw_zone_types.h"
#include "rgw_user_types.h"
#include "rgw_bucket_types.h"
#include "rgw_obj_types.h"

#include "driver/rados/rgw_obj_manifest.h" // FIXME: subclass dependency

#include "common/Formatter.h"

class JSONObj;
class cls_user_bucket;

enum RGWIntentEvent {
  DEL_OBJ = 0,
  DEL_DIR = 1,
};

/** Store error returns for output at a different point in the program */
struct rgw_err {
  rgw_err();
  void clear();
  bool is_clear() const;
  bool is_err() const;
  friend std::ostream& operator<<(std::ostream& oss, const rgw_err &err);

  int http_ret;
  int ret;
  std::string err_code;
  std::string message;
}; /* rgw_err */

struct rgw_zone_id {
  std::string id;

  rgw_zone_id() {}
  rgw_zone_id(const std::string& _id) : id(_id) {}
  rgw_zone_id(std::string&& _id) : id(std::move(_id)) {}

  void encode(ceph::buffer::list& bl) const {
    /* backward compatibility, not using ENCODE_{START,END} macros */
    ceph::encode(id, bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    /* backward compatibility, not using DECODE_{START,END} macros */
    ceph::decode(id, bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_string("id", id);
  }

  static void generate_test_instances(std::list<rgw_zone_id*>& o) {
    o.push_back(new rgw_zone_id);
    o.push_back(new rgw_zone_id("id"));
  }

  void clear() {
    id.clear();
  }

  bool operator==(const std::string& _id) const {
    return (id == _id);
  }
  bool operator==(const rgw_zone_id& zid) const {
    return (id == zid.id);
  }
  bool operator!=(const rgw_zone_id& zid) const {
    return (id != zid.id);
  }
  bool operator<(const rgw_zone_id& zid) const {
    return (id < zid.id);
  }
  bool operator>(const rgw_zone_id& zid) const {
    return (id > zid.id);
  }

  bool empty() const {
    return id.empty();
  }
};
WRITE_CLASS_ENCODER(rgw_zone_id)

inline std::ostream& operator<<(std::ostream& os, const rgw_zone_id& zid) {
  os << zid.id;
  return os;
}

struct obj_version;
struct rgw_placement_rule;
struct RGWAccessKey;
class RGWUserCaps;

extern void encode_json(const char *name, const obj_version& v, Formatter *f);
extern void encode_json(const char *name, const RGWUserCaps& val, Formatter *f);
extern void encode_json(const char *name, const rgw_pool& pool, Formatter *f);
extern void encode_json(const char *name, const rgw_placement_rule& r, Formatter *f);
extern void encode_json_impl(const char *name, const rgw_zone_id& zid, ceph::Formatter *f);
extern void encode_json_plain(const char *name, const RGWAccessKey& val, Formatter *f);

extern void decode_json_obj(obj_version& v, JSONObj *obj);
extern void decode_json_obj(rgw_zone_id& zid, JSONObj *obj);
extern void decode_json_obj(rgw_pool& pool, JSONObj *obj);
extern void decode_json_obj(rgw_placement_rule& v, JSONObj *obj);

// Represents an identity. This is more wide-ranging than a
// 'User'. Its purposes is to be matched against by an
// IdentityApplier. The internal representation will doubtless change as
// more types are added. We may want to expose the type enum and make
// the member public so people can switch/case on it.

namespace rgw {
namespace auth {
class Principal {
  enum types { User, Role, Account, Wildcard, OidcProvider, AssumedRole };
  types t;
  rgw_user u;
  std::string idp_url;

  explicit Principal(types t)
    : t(t) {}

  Principal(types t, std::string&& n, std::string i)
    : t(t), u(std::move(n), std::move(i)) {}

  Principal(std::string&& idp_url)
    : t(OidcProvider), idp_url(std::move(idp_url)) {}

public:

  static Principal wildcard() {
    return Principal(Wildcard);
  }

  static Principal user(std::string&& t, std::string&& u) {
    return Principal(User, std::move(t), std::move(u));
  }

  static Principal role(std::string&& t, std::string&& u) {
    return Principal(Role, std::move(t), std::move(u));
  }

  static Principal account(std::string&& t) {
    return Principal(Account, std::move(t), {});
  }

  static Principal oidc_provider(std::string&& idp_url) {
    return Principal(std::move(idp_url));
  }

  static Principal assumed_role(std::string&& t, std::string&& u) {
    return Principal(AssumedRole, std::move(t), std::move(u));
  }

  bool is_wildcard() const {
    return t == Wildcard;
  }

  bool is_user() const {
    return t == User;
  }

  bool is_role() const {
    return t == Role;
  }

  bool is_account() const {
    return t == Account;
  }

  bool is_oidc_provider() const {
    return t == OidcProvider;
  }

  bool is_assumed_role() const {
    return t == AssumedRole;
  }

  const std::string& get_account() const {
    return u.tenant;
  }

  const std::string& get_id() const {
    return u.id;
  }

  const std::string& get_idp_url() const {
    return idp_url;
  }

  const std::string& get_role_session() const {
    return u.id;
  }

  const std::string& get_role() const {
    return u.id;
  }

  bool operator ==(const Principal& o) const {
    return (t == o.t) && (u == o.u);
  }

  bool operator <(const Principal& o) const {
    return (t < o.t) || ((t == o.t) && (u < o.u));
  }
};

std::ostream& operator <<(std::ostream& m, const Principal& p);
}
}

class JSONObj;

void decode_json_obj(rgw_user& val, JSONObj *obj);
void encode_json(const char *name, const rgw_user& val, ceph::Formatter *f);
void encode_xml(const char *name, const rgw_user& val, ceph::Formatter *f);

inline std::ostream& operator<<(std::ostream& out, const rgw_user &u) {
  std::string s;
  u.to_str(s);
  return out << s;
}

struct RGWUploadPartInfo {
  uint32_t num;
  uint64_t size;
  uint64_t accounted_size{0};
  std::string etag;
  ceph::real_time modified;
  RGWObjManifest manifest;
  RGWCompressionInfo cs_info;

  // Previous part obj prefixes. Recorded here for later cleanup.
  std::set<std::string> past_prefixes; 

  RGWUploadPartInfo() : num(0), size(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(5, 2, bl);
    encode(num, bl);
    encode(size, bl);
    encode(etag, bl);
    encode(modified, bl);
    encode(manifest, bl);
    encode(cs_info, bl);
    encode(accounted_size, bl);
    encode(past_prefixes, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(5, 2, 2, bl);
    decode(num, bl);
    decode(size, bl);
    decode(etag, bl);
    decode(modified, bl);
    if (struct_v >= 3)
      decode(manifest, bl);
    if (struct_v >= 4) {
      decode(cs_info, bl);
      decode(accounted_size, bl);
    } else {
      accounted_size = size;
    }
    if (struct_v >= 5) {
      decode(past_prefixes, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<RGWUploadPartInfo*>& o);
};
WRITE_CLASS_ENCODER(RGWUploadPartInfo)
