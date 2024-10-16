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
#include <list>
#include <fmt/format.h>

#include "include/types.h"
#include "common/Formatter.h"

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

static constexpr char RGW_REFERER_WILDCARD[] = "*";

struct RGWAccessKey {
  std::string id; // AccessKey
  std::string key; // SecretKey
  std::string subuser;
  bool active = true;
  ceph::real_time create_date;

  RGWAccessKey() {}
  RGWAccessKey(std::string _id, std::string _key)
    : id(std::move(_id)), key(std::move(_key)) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(4, 2, bl);
    encode(id, bl);
    encode(key, bl);
    encode(subuser, bl);
    encode(active, bl);
    encode(create_date, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START_LEGACY_COMPAT_LEN_32(4, 2, 2, bl);
     decode(id, bl);
     decode(key, bl);
     decode(subuser, bl);
     if (struct_v >= 3) {
       decode(active, bl);
     }
     if (struct_v >= 4) {
       decode(create_date, bl);
     }
     DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void dump_plain(Formatter *f) const;
  void dump(Formatter *f, const std::string& user, bool swift) const;
  static void generate_test_instances(std::list<RGWAccessKey*>& o);

  void decode_json(JSONObj *obj);
  void decode_json(JSONObj *obj, bool swift);
};
WRITE_CLASS_ENCODER(RGWAccessKey)

struct RGWSubUser {
  std::string name;
  uint32_t perm_mask;

  RGWSubUser() : perm_mask(0) {}
  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    encode(name, bl);
    encode(perm_mask, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START_LEGACY_COMPAT_LEN_32(2, 2, 2, bl);
     decode(name, bl);
     decode(perm_mask, bl);
     DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void dump(Formatter *f, const std::string& user) const;
  static void generate_test_instances(std::list<RGWSubUser*>& o);

  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWSubUser)

class RGWUserCaps
{
  std::map<std::string, uint32_t> caps;

  int get_cap(const std::string& cap, std::string& type, uint32_t *perm);
  int add_cap(const std::string& cap);
  int remove_cap(const std::string& cap);
public:
  static int parse_cap_perm(const std::string& str, uint32_t *perm);
  int add_from_string(const std::string& str);
  int remove_from_string(const std::string& str);

  void encode(bufferlist& bl) const {
     ENCODE_START(1, 1, bl);
     encode(caps, bl);
     ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
     decode(caps, bl);
     DECODE_FINISH(bl);
  }
  int check_cap(const std::string& cap, uint32_t perm) const;
  bool is_valid_cap_type(const std::string& tp);
  void dump(Formatter *f) const;
  void dump(Formatter *f, const char *name) const;
  static void generate_test_instances(std::list<RGWUserCaps*>& o);
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWUserCaps)

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
  uint32_t get_permissions() const { return flags; }
  void set_permissions(uint32_t perm) { flags = perm; }

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    encode(flags, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    decode(flags, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<ACLPermission*>& o);

  friend bool operator==(const ACLPermission& lhs, const ACLPermission& rhs);
  friend bool operator!=(const ACLPermission& lhs, const ACLPermission& rhs);
};
WRITE_CLASS_ENCODER(ACLPermission)

class ACLGranteeType
{
protected:
  __u32 type;
public:
  ACLGranteeType(ACLGranteeTypeEnum t = ACL_TYPE_UNKNOWN) : type(t) {}

  ACLGranteeTypeEnum get_type() const { return (ACLGranteeTypeEnum)type; }
  operator ACLGranteeTypeEnum() const { return get_type(); }

  void set(ACLGranteeTypeEnum t) { type = t; }
  ACLGranteeType& operator=(ACLGranteeTypeEnum t) { set(t); return *this; }

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    encode(type, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    decode(type, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<ACLGranteeType*>& o);

  friend bool operator==(const ACLGranteeType& lhs, const ACLGranteeType& rhs);
  friend bool operator!=(const ACLGranteeType& lhs, const ACLGranteeType& rhs);
};
WRITE_CLASS_ENCODER(ACLGranteeType)
