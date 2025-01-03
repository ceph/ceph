// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <map>
#include <string>

#include "include/encoding.h"
#include "common/async/yield_context.h"
#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"
#include "rgw_common.h"
#include "rgw_iam_managed_policy.h"

struct RGWRoleInfo // TODO: move to rgw_common.h
{
  std::string id;
  std::string name;
  std::string path;
  std::string arn;
  std::string creation_date;
  std::string trust_policy;
  // map from PolicyName to an inline policy document from PutRolePolicy
  std::map<std::string, std::string> perm_policy_map;
  // set of managed policy arns from AttachRolePolicy
  rgw::IAM::ManagedPolicies managed_policies;
  std::string tenant;
  std::string description;
  uint64_t max_session_duration = 0;
  std::multimap<std::string,std::string> tags;
  RGWObjVersionTracker objv_tracker;
  ceph::real_time mtime;
  rgw_account_id account_id;

  RGWRoleInfo() = default;

  ~RGWRoleInfo() = default;

  void encode(bufferlist& bl) const {
    ENCODE_START(4, 1, bl);
    encode(id, bl);
    encode(name, bl);
    encode(path, bl);
    encode(arn, bl);
    encode(creation_date, bl);
    encode(trust_policy, bl);
    encode(perm_policy_map, bl);
    encode(tenant, bl);
    encode(max_session_duration, bl);
    encode(account_id, bl);
    encode(description, bl);
    encode(managed_policies, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(4, bl);
    decode(id, bl);
    decode(name, bl);
    decode(path, bl);
    decode(arn, bl);
    decode(creation_date, bl);
    decode(trust_policy, bl);
    decode(perm_policy_map, bl);
    if (struct_v >= 2) {
      decode(tenant, bl);
    }
    if (struct_v >= 3) {
      decode(max_session_duration, bl);
    }
    if (struct_v >= 4) {
      decode(account_id, bl);
      decode(description, bl);
      decode(managed_policies, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWRoleInfo)

namespace rgw::sal {

class RGWRole
{
public:
  static const std::string role_arn_prefix;
  static constexpr int MAX_ROLE_NAME_LEN = 64;
  static constexpr int MAX_PATH_NAME_LEN = 512;
  static constexpr uint64_t SESSION_DURATION_MIN = 3600; // in seconds
  static constexpr uint64_t SESSION_DURATION_MAX = 43200; // in seconds
protected:
  RGWRoleInfo info;
public:
  bool validate_max_session_duration(const DoutPrefixProvider* dpp);
  bool validate_input(const DoutPrefixProvider* dpp);
  void extract_name_tenant(const std::string& str);

  RGWRole(std::string name,
              std::string tenant,
              rgw_account_id account_id,
              std::string path="",
              std::string trust_policy="",
              std::string description="",
              std::string max_session_duration_str="",
              std::multimap<std::string,std::string> tags={});

  explicit RGWRole(std::string id);

  explicit RGWRole(const RGWRoleInfo& info) : info(info) {}

  RGWRole() = default;

  virtual ~RGWRole() = default;

  // virtual interface
  virtual int load_by_name(const DoutPrefixProvider *dpp, optional_yield y) = 0;
  virtual int load_by_id(const DoutPrefixProvider *dpp, optional_yield y) = 0;
  virtual int store_info(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y) = 0;
  virtual int delete_obj(const DoutPrefixProvider *dpp, optional_yield y) = 0;

  const std::string& get_id() const { return info.id; }
  const std::string& get_name() const { return info.name; }
  const std::string& get_tenant() const { return info.tenant; }
  const rgw_account_id& get_account_id() const { return info.account_id; }
  const std::string& get_path() const { return info.path; }
  const std::string& get_create_date() const { return info.creation_date; }
  const std::string& get_assume_role_policy() const { return info.trust_policy;}
  const uint64_t& get_max_session_duration() const { return info.max_session_duration; }
  RGWObjVersionTracker& get_objv_tracker() { return info.objv_tracker; }
  const RGWObjVersionTracker& get_objv_tracker() const { return info.objv_tracker; }
  const real_time& get_mtime() const { return info.mtime; }
  RGWRoleInfo& get_info() { return info; }

  void set_id(const std::string& id) { this->info.id = id; }
  void set_mtime(const real_time& mtime) { this->info.mtime = mtime; }

  int create(const DoutPrefixProvider *dpp, const std::string &role_id, optional_yield y);
  void update_trust_policy(std::string& trust_policy);
  void set_perm_policy(const std::string& policy_name, const std::string& perm_policy);
  std::vector<std::string> get_role_policy_names();
  int get_role_policy(const DoutPrefixProvider* dpp, const std::string& policy_name, std::string& perm_policy);
  int delete_policy(const DoutPrefixProvider* dpp, const std::string& policy_name);
  int set_tags(const DoutPrefixProvider* dpp, const std::multimap<std::string,std::string>& tags_map);
  boost::optional<std::multimap<std::string,std::string>> get_tags();
  void erase_tags(const std::vector<std::string>& tagKeys);
  void update_max_session_duration(const std::string& max_session_duration_str);
};

} // namespace rgw::sal
