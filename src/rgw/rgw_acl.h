// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_ACL_H
#define CEPH_RGW_ACL_H

#include <map>
#include <string>
#include <include/types.h>

#include "common/debug.h"

#include "rgw_basic_types.h"

using namespace std;

#define RGW_PERM_NONE            0x00
#define RGW_PERM_READ            0x01
#define RGW_PERM_WRITE           0x02
#define RGW_PERM_READ_ACP        0x04
#define RGW_PERM_WRITE_ACP       0x08
#define RGW_PERM_READ_OBJS       0x10
#define RGW_PERM_WRITE_OBJS      0x20
#define RGW_PERM_FULL_CONTROL    ( RGW_PERM_READ | RGW_PERM_WRITE | \
                                  RGW_PERM_READ_ACP | RGW_PERM_WRITE_ACP )
#define RGW_PERM_ALL_S3          RGW_PERM_FULL_CONTROL
#define RGW_PERM_INVALID         0xFF00

enum ACLGranteeTypeEnum {
/* numbers are encoded, should not change */
  ACL_TYPE_CANON_USER = 0,
  ACL_TYPE_EMAIL_USER = 1,
  ACL_TYPE_GROUP      = 2,
  ACL_TYPE_UNKNOWN    = 3,
  ACL_TYPE_REFERER    = 4,
};

enum ACLGroupTypeEnum {
/* numbers are encoded should not change */
  ACL_GROUP_NONE                = 0,
  ACL_GROUP_ALL_USERS           = 1,
  ACL_GROUP_AUTHENTICATED_USERS = 2,
};

class ACLPermission
{
protected:
  int flags;
public:
  ACLPermission() : flags(0) {}
  ~ACLPermission() {}
  int get_permissions() const { return flags; }
  void set_permissions(int perm) { flags = perm; }

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    ::encode(flags, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    ::decode(flags, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ACLPermission*>& o);
};
WRITE_CLASS_ENCODER(ACLPermission)

class ACLGranteeType
{
protected:
  __u32 type;
public:
  ACLGranteeType() : type(ACL_TYPE_UNKNOWN) {}
  virtual ~ACLGranteeType() {}
//  virtual const char *to_string() = 0;
  ACLGranteeTypeEnum get_type() const { return (ACLGranteeTypeEnum)type; }
  void set(ACLGranteeTypeEnum t) { type = t; }
//  virtual void set(const char *s) = 0;
  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    ::encode(type, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    ::decode(type, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ACLGranteeType*>& o);
};
WRITE_CLASS_ENCODER(ACLGranteeType)

class ACLGrantee
{
public:
  ACLGrantee() {}
  ~ACLGrantee() {}
};


class ACLGrant
{
protected:
  ACLGranteeType type;
  rgw_user id;
  string email;
  ACLPermission permission;
  string name;
  ACLGroupTypeEnum group;
  string url_spec;

public:
  ACLGrant() : group(ACL_GROUP_NONE) {}
  virtual ~ACLGrant() {}

  /* there's an assumption here that email/uri/id encodings are
     different and there can't be any overlap */
  bool get_id(rgw_user& _id) const {
    switch(type.get_type()) {
    case ACL_TYPE_EMAIL_USER:
      _id = email; // implies from_str() that parses the 't:u' syntax
      return true;
    case ACL_TYPE_GROUP:
    case ACL_TYPE_REFERER:
      return false;
    default:
      _id = id;
      return true;
    }
  }
  ACLGranteeType& get_type() { return type; }
  const ACLGranteeType& get_type() const { return type; }
  ACLPermission& get_permission() { return permission; }
  const ACLPermission& get_permission() const { return permission; }
  ACLGroupTypeEnum get_group() const { return group; }
  const string& get_referer() const { return url_spec; }

  void encode(bufferlist& bl) const {
    ENCODE_START(5, 3, bl);
    ::encode(type, bl);
    string s;
    id.to_str(s);
    ::encode(s, bl);
    string uri;
    ::encode(uri, bl);
    ::encode(email, bl);
    ::encode(permission, bl);
    ::encode(name, bl);
    __u32 g = (__u32)group;
    ::encode(g, bl);
    ::encode(url_spec, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(5, 3, 3, bl);
    ::decode(type, bl);
    string s;
    ::decode(s, bl);
    id.from_str(s);
    string uri;
    ::decode(uri, bl);
    ::decode(email, bl);
    ::decode(permission, bl);
    ::decode(name, bl);
    if (struct_v > 1) {
      __u32 g;
      ::decode(g, bl);
      group = (ACLGroupTypeEnum)g;
    } else {
      group = uri_to_group(uri);
    }
    if (struct_v >= 5) {
      ::decode(url_spec, bl);
    } else {
      url_spec.clear();
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ACLGrant*>& o);

  ACLGroupTypeEnum uri_to_group(string& uri);
  
  void set_canon(const rgw_user& _id, string& _name, int perm) {
    type.set(ACL_TYPE_CANON_USER);
    id = _id;
    name = _name;
    permission.set_permissions(perm);
  }
  void set_group(ACLGroupTypeEnum _group, int perm) {
    type.set(ACL_TYPE_GROUP);
    group = _group;
    permission.set_permissions(perm);
  }
  void set_referer(const std::string& _url_spec, int perm) {
    type.set(ACL_TYPE_REFERER);
    url_spec = _url_spec;
    permission.set_permissions(perm);
  }
};
WRITE_CLASS_ENCODER(ACLGrant)

struct ACLReferer {
  std::string url_spec;
  int perm;

  ACLReferer() : perm(0) {}
  ACLReferer(const std::string& url_spec,
             const int perm)
    : url_spec(url_spec),
      perm(perm) {
  }

  bool is_match(std::string http_referer) const {
    if (http_referer == url_spec) {
      return true;
    }

    if (http_referer.length() < url_spec.length()) {
      return false;
    }

    if ('.' == http_referer[0]) {
      /* Wildcard support: a referer matches the spec when its last char are
       * perfectly equal to spec. */
      return !http_referer.compare(http_referer.length() - url_spec.length(),
                                   url_spec.length(), url_spec);
    }

    return false;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(url_spec, bl);
    ::encode(perm, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    ::decode(url_spec, bl);
    ::decode(perm, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(ACLReferer)

class RGWAccessControlList
{
protected:
  CephContext *cct;
  map<string, int> acl_user_map;
  map<uint32_t, int> acl_group_map;
  list<ACLReferer> referer_list;
  multimap<string, ACLGrant> grant_map;
  void _add_grant(ACLGrant *grant);
public:
  explicit RGWAccessControlList(CephContext *_cct) : cct(_cct) {}
  RGWAccessControlList() : cct(NULL) {}

  void set_ctx(CephContext *ctx) {
    cct = ctx;
  }

  virtual ~RGWAccessControlList() {}

  int get_perm(const rgw_user& id, int perm_mask);
  int get_group_perm(ACLGroupTypeEnum group, int perm_mask);
  int get_referer_perm(const std::string http_referer, int perm_mask);
  void encode(bufferlist& bl) const {
    ENCODE_START(4, 3, bl);
    bool maps_initialized = true;
    ::encode(maps_initialized, bl);
    ::encode(acl_user_map, bl);
    ::encode(grant_map, bl);
    ::encode(acl_group_map, bl);
    ::encode(referer_list, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(4, 3, 3, bl);
    bool maps_initialized;
    ::decode(maps_initialized, bl);
    ::decode(acl_user_map, bl);
    ::decode(grant_map, bl);
    if (struct_v >= 2) {
      ::decode(acl_group_map, bl);
    } else if (!maps_initialized) {
      multimap<string, ACLGrant>::iterator iter;
      for (iter = grant_map.begin(); iter != grant_map.end(); ++iter) {
        ACLGrant& grant = iter->second;
        _add_grant(&grant);
      }
    }
    if (struct_v >= 4) {
      ::decode(referer_list, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<RGWAccessControlList*>& o);

  void add_grant(ACLGrant *grant);

  multimap<string, ACLGrant>& get_grant_map() { return grant_map; }
  const multimap<string, ACLGrant>& get_grant_map() const { return grant_map; }

  void create_default(const rgw_user& id, string name) {
    acl_user_map.clear();
    acl_group_map.clear();
    referer_list.clear();

    ACLGrant grant;
    grant.set_canon(id, name, RGW_PERM_FULL_CONTROL);
    add_grant(&grant);
  }
};
WRITE_CLASS_ENCODER(RGWAccessControlList)

class ACLOwner
{
protected:
  rgw_user id;
  string display_name;
public:
  ACLOwner() {}
  ~ACLOwner() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(3, 2, bl);
    string s;
    id.to_str(s);
    ::encode(s, bl);
    ::encode(display_name, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
    string s;
    ::decode(s, bl);
    id.from_str(s);
    ::decode(display_name, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ACLOwner*>& o);
  void set_id(const rgw_user& _id) { id = _id; }
  void set_name(const string& name) { display_name = name; }

  rgw_user& get_id() { return id; }
  string& get_display_name() { return display_name; }
};
WRITE_CLASS_ENCODER(ACLOwner)

class RGWAccessControlPolicy
{
protected:
  CephContext *cct;
  RGWAccessControlList acl;
  ACLOwner owner;

public:
  explicit RGWAccessControlPolicy(CephContext *_cct) : cct(_cct), acl(_cct) {}
  RGWAccessControlPolicy() : cct(NULL), acl(NULL) {}
  virtual ~RGWAccessControlPolicy() {}

  void set_ctx(CephContext *ctx) {
    cct = ctx;
    acl.set_ctx(ctx);
  }

  int get_perm(const rgw_user& id, int perm_mask, const char * http_referer);
  int get_group_perm(ACLGroupTypeEnum group, int perm_mask);
  bool verify_permission(const rgw_user& uid,
                         int user_perm_mask,
                         int perm,
                         const char * http_referer = nullptr);

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    ::encode(owner, bl);
    ::encode(acl, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    ::decode(owner, bl);
    ::decode(acl, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<RGWAccessControlPolicy*>& o);
  void decode_owner(bufferlist::iterator& bl) { // sometimes we only need that, should be faster
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    ::decode(owner, bl);
    DECODE_FINISH(bl);
  }

  void set_owner(ACLOwner& o) { owner = o; }
  ACLOwner& get_owner() {
    return owner;
  }

  void create_default(const rgw_user& id, string& name) {
    acl.create_default(id, name);
    owner.set_id(id);
    owner.set_name(name);
  }
  RGWAccessControlList& get_acl() {
    return acl;
  }
  const RGWAccessControlList& get_acl() const {
    return acl;
  }

  virtual bool compare_group_name(string& id, ACLGroupTypeEnum group) { return false; }
};
WRITE_CLASS_ENCODER(RGWAccessControlPolicy)

#endif
