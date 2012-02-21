#ifndef CEPH_RGW_ACL_H
#define CEPH_RGW_ACL_H

#include <map>
#include <string>
#include <iostream>
#include <include/types.h>

#include "common/debug.h"

using namespace std;


#define RGW_PERM_READ            0x01
#define RGW_PERM_WRITE           0x02
#define RGW_PERM_READ_ACP        0x04
#define RGW_PERM_WRITE_ACP       0x08
#define RGW_PERM_READ_OBJS       0x10
#define RGW_PERM_WRITE_OBJS      0x20
#define RGW_PERM_FULL_CONTROL    ( RGW_PERM_READ | RGW_PERM_WRITE | \
                                  RGW_PERM_READ_ACP | RGW_PERM_WRITE_ACP )
#define RGW_PERM_ALL             RGW_PERM_FULL_CONTROL

enum ACLGranteeTypeEnum {
/* numbers are encoded, should not change */
  ACL_TYPE_CANON_USER = 0,
  ACL_TYPE_EMAIL_USER = 1,
  ACL_TYPE_GROUP      = 2,
  ACL_TYPE_UNKNOWN    = 3,
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
  int get_permissions() { return flags; }
  void set_permissions(int perm) { flags = perm; }

  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(flags, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(flags, bl);
  }
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
  ACLGranteeTypeEnum get_type() { return (ACLGranteeTypeEnum)type; }
  void set(ACLGranteeTypeEnum t) { type = t; }
//  virtual void set(const char *s) = 0;
  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(type, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(type, bl);
  }
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
  string id;
  string uri;
  string email;
  ACLPermission permission;
  string name;
  ACLGroupTypeEnum group;

public:
  ACLGrant() : group(ACL_GROUP_NONE) {}
  virtual ~ACLGrant() {}

  /* there's an assumption here that email/uri/id encodings are
     different and there can't be any overlap */
  string& get_id() {
    switch(type.get_type()) {
    case ACL_TYPE_EMAIL_USER:
      return email;
    case ACL_TYPE_GROUP:
      return uri;
    default:
      return id;
    }
  }
  ACLGranteeType& get_type() { return type; }
  ACLPermission& get_permission() { return permission; }
  ACLGroupTypeEnum get_group() { return group; }

  void encode(bufferlist& bl) const {
    __u8 struct_v = 2;
    ::encode(struct_v, bl);
    ::encode(type, bl);
    ::encode(id, bl);
    ::encode(uri, bl);
    ::encode(email, bl);
    ::encode(permission, bl);
    ::encode(name, bl);
    __u32 g = (__u32)group;
    ::encode(g, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(type, bl);
    ::decode(id, bl);
    ::decode(uri, bl);
    ::decode(email, bl);
    ::decode(permission, bl);
    ::decode(name, bl);
    if (struct_v > 1) {
      __u32 g;
      ::decode(g, bl);
      group = (ACLGroupTypeEnum)g;
    } else {
      group = uri_to_group();
    }
  }

  ACLGroupTypeEnum uri_to_group();
  
  void set_canon(string& _id, string& _name, int perm) {
    type.set(ACL_TYPE_CANON_USER);
    id = _id;
    name = _name;
    permission.set_permissions(perm);
  }
  void set_group(string& _uri, ACLGroupTypeEnum _group, int perm) {
    type.set(ACL_TYPE_GROUP);
    uri = _uri;
    group = _group;
    permission.set_permissions(perm);
  }
};
WRITE_CLASS_ENCODER(ACLGrant)

class RGWAccessControlList
{
protected:
  map<string, int> acl_user_map;
  map<uint32_t, int> acl_group_map;
  multimap<string, ACLGrant> grant_map;
  void _add_grant(ACLGrant *grant);
public:
  RGWAccessControlList() {}

  virtual ~RGWAccessControlList() {}

  int get_perm(string& id, int perm_mask);
  int get_group_perm(ACLGroupTypeEnum group, int perm_mask);
  void encode(bufferlist& bl) const {
    __u8 struct_v = 2;
    ::encode(struct_v, bl);
    bool maps_initialized = true;
    ::encode(maps_initialized, bl);
    ::encode(acl_user_map, bl);
    ::encode(grant_map, bl);
    ::encode(acl_group_map, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
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
  }
  void add_grant(ACLGrant *grant);

  multimap<string, ACLGrant>& get_grant_map() { return grant_map; }

  void create_default(string id, string name) {
    acl_user_map.clear();
    acl_group_map.clear();

    ACLGrant grant;
    grant.set_canon(id, name, RGW_PERM_FULL_CONTROL);
    add_grant(&grant);
  }
};
WRITE_CLASS_ENCODER(RGWAccessControlList)

class ACLOwner
{
protected:
  string id;
  string display_name;
public:
  ACLOwner() {}
  ~ACLOwner() {}

  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(id, bl);
    ::encode(display_name, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(id, bl);
    ::decode(display_name, bl);
  }
  void set_id(string& _id) { id = _id; }
  void set_name(string& name) { display_name = name; }

  string& get_id() { return id; }
  string& get_display_name() { return display_name; }
};
WRITE_CLASS_ENCODER(ACLOwner)

class RGWAccessControlPolicy
{
protected:
  RGWAccessControlList acl;
  ACLOwner owner;

public:
  RGWAccessControlPolicy() {}
  virtual ~RGWAccessControlPolicy() {}

  int get_perm(string& id, int perm_mask);
  int get_group_perm(ACLGroupTypeEnum group, int perm_mask);
  bool verify_permission(string& uid, int user_perm_mask, int perm);

  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(owner, bl);
    ::encode(acl, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(owner, bl);
    ::decode(acl, bl);
   }
  void decode_owner(bufferlist::iterator& bl) { // sometimes we only need that, should be faster
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(owner, bl);
  }

  void set_owner(ACLOwner& o) { owner = o; }
  ACLOwner& get_owner() {
    return owner;
  }

  void create_default(string& id, string& name) {
    acl.create_default(id, name);
    owner.set_id(id);
    owner.set_name(name);
  }
  RGWAccessControlList& get_acl() {
    return acl;
  }

  virtual bool compare_group_name(string& id, ACLGroupTypeEnum group) { return false; }
};
WRITE_CLASS_ENCODER(RGWAccessControlPolicy)

#endif
