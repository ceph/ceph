// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once
#include <string>
#include <map>

#include "rgw_arn.h"
#include "common/ceph_json.h"
#include "common/Formatter.h"
#include "rgw_user_types.h"
#include "include/encoding.h"
#include "rgw_common.h"

struct ManagedPolicyAttachment {
  std::string arn;
  std::string  status;

  void encode(bufferlist& bl) const
  {
    ENCODE_START(2, 1, bl);
    encode(arn, bl);
    encode(status, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl)
  {
    DECODE_START(2, bl);
     decode(arn, bl);
     decode(status, bl);
     DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<ManagedPolicyAttachment*>& o);
};
WRITE_CLASS_ENCODER(ManagedPolicyAttachment)

struct ManagedPolicyInfo {
  std::string id;
  std::string name;
  std::string path;
  std::string arn;
  std::string policy_document;
  std::string description;
  rgw_account_id account_id;
  ceph::real_time update_date;
  ceph::real_time creation_date;
  std::string default_version{"v1"};
  bool is_attachable{true};
  uint32_t attachment_count{0};
  uint32_t permissions_boundary_usage_count{0};
  std::multimap<std::string, std::string> tags;
  std::map<std::string, ManagedPolicyAttachment> attachments;
  std::map<std::string, std::string> versions;

  void encode(bufferlist &bl) const
  {
    ENCODE_START(2, 1, bl);
    encode(id, bl);
    encode(name, bl);
    encode(path, bl);
    encode(update_date, bl);
    encode(creation_date, bl);
    encode(policy_document, bl);
    encode(description, bl);
    encode(default_version, bl);
    encode(account_id, bl);
    encode(tags, bl);
    encode(arn, bl);
    encode(attachment_count, bl);
    encode(permissions_boundary_usage_count, bl);
    encode(is_attachable, bl);
    encode(attachments, bl);
    encode(versions, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &bl)
  {
    DECODE_START(2, bl);
    decode(id, bl);
    decode(name, bl);
    decode(path, bl);
    decode(update_date, bl);
    decode(creation_date, bl);
    decode(policy_document, bl);
    decode(description, bl);
    decode(default_version, bl);
    decode(account_id, bl);
    decode(tags, bl);
    decode(arn, bl);
    decode(attachment_count, bl);
    decode(permissions_boundary_usage_count, bl);
    decode(is_attachable, bl);
    decode(attachments, bl);
    decode(versions, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj* obj); 
  static void generate_test_instances(std::list<ManagedPolicyInfo*>& ls);
};
WRITE_CLASS_ENCODER(ManagedPolicyInfo)
