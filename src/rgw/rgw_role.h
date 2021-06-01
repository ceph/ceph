// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_ROLE_H
#define CEPH_RGW_ROLE_H

#include <string>

#include "common/async/yield_context.h"

#include "common/ceph_json.h"
#include "common/ceph_context.h"

#include "rgw/rgw_rados.h"

struct RGWCtl;

class RGWRole
{
  using string = std::string;
  static const string role_name_oid_prefix;
  static const string role_oid_prefix;
  static const string role_path_oid_prefix;
  static const string role_arn_prefix;
  static constexpr int MAX_ROLE_NAME_LEN = 64;
  static constexpr int MAX_PATH_NAME_LEN = 512;
  static constexpr uint64_t SESSION_DURATION_MIN = 3600; // in seconds
  static constexpr uint64_t SESSION_DURATION_MAX = 43200; // in seconds

  CephContext *cct;
  RGWCtl *ctl;
  string id;
  string name;
  string path;
  string arn;
  string creation_date;
  string trust_policy;
  map<string, string> perm_policy_map;
  string tenant;
  uint64_t max_session_duration;
  std::multimap<std::string,std::string> tags;

  int store_info(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y);
  int store_name(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y);
  int store_path(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y);
  int read_id(const DoutPrefixProvider *dpp, const string& role_name, const string& tenant, string& role_id, optional_yield y);
  int read_name(const DoutPrefixProvider *dpp, optional_yield y);
  int read_info(const DoutPrefixProvider *dpp, optional_yield y);
  bool validate_input();
  void extract_name_tenant(const std::string& str);

public:
  RGWRole(CephContext *cct,
          RGWCtl *ctl,
          string name,
          string path,
          string trust_policy,
          string tenant,
          string max_session_duration_str="",
          std::multimap<std::string,std::string> tags={})
  : cct(cct),
    ctl(ctl),
    name(std::move(name)),
    path(std::move(path)),
    trust_policy(std::move(trust_policy)),
    tenant(std::move(tenant)),
    tags(std::move(tags)) {
    if (this->path.empty())
      this->path = "/";
    extract_name_tenant(this->name);
    if (max_session_duration_str.empty()) {
      max_session_duration = SESSION_DURATION_MIN;
    } else {
      max_session_duration = std::stoull(max_session_duration_str);
    }
  }

  RGWRole(CephContext *cct,
          RGWCtl *ctl,
          string name,
          string tenant)
  : cct(cct),
    ctl(ctl),
    name(std::move(name)),
    tenant(std::move(tenant)) {
    extract_name_tenant(this->name);
  }

  RGWRole(CephContext *cct,
          RGWCtl *ctl,
          string id)
  : cct(cct),
    ctl(ctl),
    id(std::move(id)) {}

  RGWRole(CephContext *cct,
          RGWCtl *ctl)
  : cct(cct),
    ctl(ctl) {}

  RGWRole() {}

  ~RGWRole() = default;

  void encode(bufferlist& bl) const {
    ENCODE_START(3, 1, bl);
    encode(id, bl);
    encode(name, bl);
    encode(path, bl);
    encode(arn, bl);
    encode(creation_date, bl);
    encode(trust_policy, bl);
    encode(perm_policy_map, bl);
    encode(tenant, bl);
    encode(max_session_duration, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(3, bl);
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
    DECODE_FINISH(bl);
  }

  const string& get_id() const { return id; }
  const string& get_name() const { return name; }
  const string& get_tenant() const { return tenant; }
  const string& get_path() const { return path; }
  const string& get_create_date() const { return creation_date; }
  const string& get_assume_role_policy() const { return trust_policy;}
  const uint64_t& get_max_session_duration() const { return max_session_duration; }

  void set_id(const string& id) { this->id = id; }

  int create(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y);
  int delete_obj(const DoutPrefixProvider *dpp, optional_yield y);
  int get(const DoutPrefixProvider *dpp, optional_yield y);
  int get_by_id(const DoutPrefixProvider *dpp, optional_yield y);
  int update(const DoutPrefixProvider *dpp, optional_yield y);
  void update_trust_policy(string& trust_policy);
  void set_perm_policy(const string& policy_name, const string& perm_policy);
  vector<string> get_role_policy_names();
  int get_role_policy(const string& policy_name, string& perm_policy);
  int delete_policy(const string& policy_name);
  int set_tags(const DoutPrefixProvider* dpp, const std::multimap<std::string,std::string>& tags_map);
  boost::optional<std::multimap<std::string,std::string>> get_tags();
  void erase_tags(const std::vector<std::string>& tagKeys);
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);

  static const string& get_names_oid_prefix();
  static const string& get_info_oid_prefix();
  static const string& get_path_oid_prefix();
  static int get_roles_by_path_prefix(const DoutPrefixProvider *dpp, 
                                      RGWRados *store,
                                      CephContext *cct,
                                      const string& path_prefix,
                                      const string& tenant,
                                      vector<RGWRole>& roles,
				      optional_yield y);
};
WRITE_CLASS_ENCODER(RGWRole)
#endif /* CEPH_RGW_ROLE_H */
