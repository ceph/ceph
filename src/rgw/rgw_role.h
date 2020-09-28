// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_ROLE_H
#define CEPH_RGW_ROLE_H

#include <string>

#include "common/ceph_context.h"

class RGWCtl;
class RGWRados;

struct RGWRoleInfo
{

  static constexpr int MAX_ROLE_NAME_LEN = 64;
  static constexpr int MAX_PATH_NAME_LEN = 512;
  static constexpr uint64_t SESSION_DURATION_MIN = 3600; // in seconds
  static constexpr uint64_t SESSION_DURATION_MAX = 43200; // in seconds

  using string = std::string;
  string id;
  string name;
  string path;
  string arn;
  string creation_date;
  string trust_policy;
  map<string, string> perm_policy_map;
  string tenant;
  uint64_t max_session_duration;

  void extract_name_tenant(std::string_view str);
  void set_perm_policy(const std::string& policy_name,
		       const std::string& perm_policy);
  bool validate_input();
  vector<string> get_role_policy_names();
  int get_role_policy(const std::string& policy_name,
		      std::string& perm_policy);
  int delete_policy(const string& policy_name);

  RGWRoleInfo() = default;
  ~RGWRoleInfo() = default;

  RGWRoleInfo(std::string name,
	      std::string path,
	      std::string trust_policy,
	      std::string tenant,
	      std::string max_session_duration_str="") :
    path(std::move(path)),
    trust_policy(std::move(trust_policy)) {
    if (this->path.empty())
      this->path = "/";
    extract_name_tenant(name);
    if (max_session_duration_str.empty()) {
      max_session_duration = SESSION_DURATION_MIN;
    } else {
      max_session_duration = std::stoull(max_session_duration_str);
    }
  }

  RGWRoleInfo(std::string _name,
	      std::string _tenant) {
    extract_name_tenant(_name);
    if (tenant.empty()) {
      tenant = std::move(_tenant);
    }
  }

  RGWRoleInfo(std::string _id) :
    id(std::move(_id)) {}

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
    DECODE_START(2, bl);
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

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWRoleInfo)


class RGWRole
{
  using string = std::string;
  static const string role_name_oid_prefix;
  static const string role_oid_prefix;
  static const string role_path_oid_prefix;
  static const string role_arn_prefix;

  CephContext *cct;
  RGWCtl *ctl;
  RGWRoleInfo info;

  int store_info(bool exclusive) { return 0; }
  int store_name(bool exclusive) { return 0; }
  int store_path(bool exclusive) { return 0; }
  int read_id(const string& role_name, const string& tenant, string& role_id);
  int read_name() { return 0; }
  int read_info() { return 0; };
  bool validate_input();
  void extract_name_tenant(const std::string& str);
  void get_role_policy(const string& policy_name, string& perm_policy);
public:
  // args for RGWRoleInfo
  template <typename ...Args>
  RGWRole(CephContext *cct,
          RGWCtl *ctl,
          Args&& ...args)
  : cct(cct),
    ctl(ctl),
    info(std::forward<Args>(args)...)
 {}

  RGWRole(CephContext *cct,
          RGWCtl *ctl)
  : cct(cct),
    ctl(ctl) {}

  RGWRole() {}

  ~RGWRole() = default;

  const string& get_id() const { return info.id; }
  const string& get_name() const { return info.name; }
  const string& get_tenant() const { return info.tenant; }
  const string& get_path() const { return info.path; }
  const string& get_create_date() const { return info.creation_date; }
  const string& get_assume_role_policy() const { return info.trust_policy;}
  const uint64_t& get_max_session_duration() const { return info.max_session_duration; }
  const RGWRoleInfo& get_info() const { return info; }

  void set_id(const string& id) { this->info.id = id; }

  int create(bool exclusive) { return 0; }
  int delete_obj() {return 0;}
  int get();
  int get_by_id();
  int update();
  void update_trust_policy(string& trust_policy);
  template <typename ...Args>
  void set_perm_policy(Args&& ...args) {
    info.set_perm_policy(std::forward<Args>(args)...);
  }
  auto get_role_policy_names() {
    return info.get_role_policy_names();
  }
  template <typename ...Args>
  int get_role_policy(Args&& ...args) {
    return info.get_role_policy(std::forward<Args>(args)...);
  }

  int delete_policy(const string& policy_name) {
    return info.delete_policy(policy_name);
  }

  static const string& get_names_oid_prefix();
  static const string& get_info_oid_prefix();
  static const string& get_path_oid_prefix();
  static int get_roles_by_path_prefix(RGWRados *store,
                                      CephContext *cct,
                                      const string& path_prefix,
                                      const string& tenant,
                                      vector<RGWRole>& roles);

  void dump(Formatter *f) const {
    info.dump(f);
  }
};
class RGWRoleMetadataHandler;
class RGWSI_Role;
class RGWSI_MetaBackend_Handler;

class RGWRoleCtl {
  struct Svc {
    RGWSI_Role *role {nullptr};
  } svc;
  RGWRoleMetadataHandler *rmhandler;
  RGWSI_MetaBackend_Handler *be_handler{nullptr};
public:
  RGWRoleCtl(RGWSI_Role *_role_svc,
	     RGWRoleMetadataHandler *_rmhander);

  struct PutParams {
    ceph::real_time mtime;
    bool exclusive {false};
    RGWObjVersionTracker *objv_tracker {nullptr};
    std::map<std::string, bufferlist> *attrs {nullptr};

    PutParams() {};
  };

  int store_info(const RGWRoleInfo& role,
		 optional_yield y,
		 const PutParams& params = {});
};

#endif /* CEPH_RGW_ROLE_H */
