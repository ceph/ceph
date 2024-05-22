// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>

#include "common/async/yield_context.h"

#include "common/ceph_json.h"
#include "common/ceph_context.h"
#include "rgw_rados.h"
#include "rgw_metadata.h"

class RGWRados;

namespace rgw { namespace sal {
struct RGWRoleInfo
{
  std::string id;
  std::string name;
  std::string path;
  std::string arn;
  std::string creation_date;
  std::string trust_policy;
  std::map<std::string, std::string> perm_policy_map;
  std::string tenant;
  uint64_t max_session_duration;
  std::multimap<std::string,std::string> tags;
  std::map<std::string, bufferlist> attrs;
  RGWObjVersionTracker objv_tracker;
  real_time mtime;

  RGWRoleInfo() = default;

  ~RGWRoleInfo() = default;

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

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWRoleInfo)

class RGWRole
{
public:
  static const std::string role_name_oid_prefix;
  static const std::string role_oid_prefix;
  static const std::string role_path_oid_prefix;
  static const std::string role_arn_prefix;
  static constexpr int MAX_ROLE_NAME_LEN = 64;
  static constexpr int MAX_PATH_NAME_LEN = 512;
  static constexpr uint64_t SESSION_DURATION_MIN = 3600; // in seconds
  static constexpr uint64_t SESSION_DURATION_MAX = 43200; // in seconds
protected:
  RGWRoleInfo info;
public:
  virtual int store_info(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y) = 0;
  virtual int store_name(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y) = 0;
  virtual int store_path(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y) = 0;
  virtual int read_id(const DoutPrefixProvider *dpp, const std::string& role_name, const std::string& tenant, std::string& role_id, optional_yield y) = 0;
  virtual int read_name(const DoutPrefixProvider *dpp, optional_yield y) = 0;
  virtual int read_info(const DoutPrefixProvider *dpp, optional_yield y) = 0;
  bool validate_max_session_duration(const DoutPrefixProvider* dpp);
  bool validate_input(const DoutPrefixProvider* dpp);
  void extract_name_tenant(const std::string& str);

  RGWRole(std::string name,
              std::string tenant,
              std::string path="",
              std::string trust_policy="",
              std::string max_session_duration_str="",
              std::multimap<std::string,std::string> tags={});

  explicit RGWRole(std::string id);

  explicit RGWRole(const RGWRoleInfo& info) : info(info) {}

  RGWRole() = default;

  virtual ~RGWRole() = default;

  const std::string& get_id() const { return info.id; }
  const std::string& get_name() const { return info.name; }
  const std::string& get_tenant() const { return info.tenant; }
  const std::string& get_path() const { return info.path; }
  const std::string& get_create_date() const { return info.creation_date; }
  const std::string& get_assume_role_policy() const { return info.trust_policy;}
  const uint64_t& get_max_session_duration() const { return info.max_session_duration; }
  const RGWObjVersionTracker& get_objv_tracker() const { return info.objv_tracker; }
  const real_time& get_mtime() const { return info.mtime; }
  std::map<std::string, bufferlist>& get_attrs() { return info.attrs; }
  RGWRoleInfo& get_info() { return info; }

  void set_id(const std::string& id) { this->info.id = id; }
  void set_mtime(const real_time& mtime) { this->info.mtime = mtime; }

  virtual int create(const DoutPrefixProvider *dpp, bool exclusive, const std::string &role_id, optional_yield y) = 0;
  virtual int delete_obj(const DoutPrefixProvider *dpp, optional_yield y) = 0;
  int get(const DoutPrefixProvider *dpp, optional_yield y);
  int get_by_id(const DoutPrefixProvider *dpp, optional_yield y);
  int update(const DoutPrefixProvider *dpp, optional_yield y);
  void update_trust_policy(std::string& trust_policy);
  void set_perm_policy(const std::string& policy_name, const std::string& perm_policy);
  std::vector<std::string> get_role_policy_names();
  int get_role_policy(const DoutPrefixProvider* dpp, const std::string& policy_name, std::string& perm_policy);
  int delete_policy(const DoutPrefixProvider* dpp, const std::string& policy_name);
  int set_tags(const DoutPrefixProvider* dpp, const std::multimap<std::string,std::string>& tags_map);
  boost::optional<std::multimap<std::string,std::string>> get_tags();
  void erase_tags(const std::vector<std::string>& tagKeys);
  void update_max_session_duration(const std::string& max_session_duration_str);
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);

  static const std::string& get_names_oid_prefix();
  static const std::string& get_info_oid_prefix();
  static const std::string& get_path_oid_prefix();
};

class RGWRoleMetadataObject: public RGWMetadataObject {
  RGWRoleInfo info;
  Driver* driver;
public:
  RGWRoleMetadataObject() = default;
  RGWRoleMetadataObject(RGWRoleInfo& info,
			const obj_version& v,
			real_time m,
      Driver* driver) : RGWMetadataObject(v,m), info(info), driver(driver) {}

  void dump(Formatter *f) const override {
    info.dump(f);
  }

  RGWRoleInfo& get_role_info() {
    return info;
  }

  Driver* get_driver() {
    return driver;
  }
};

class RGWRoleMetadataHandler: public RGWMetadataHandler_GenericMetaBE
{
public:
  RGWRoleMetadataHandler(Driver* driver, RGWSI_Role_RADOS *role_svc);

  std::string get_type() final { return "roles";  }

  RGWMetadataObject *get_meta_obj(JSONObj *jo,
				  const obj_version& objv,
				  const ceph::real_time& mtime);

  int do_get(RGWSI_MetaBackend_Handler::Op *op,
	     std::string& entry,
	     RGWMetadataObject **obj,
	     optional_yield y,
       const DoutPrefixProvider *dpp) final;

  int do_remove(RGWSI_MetaBackend_Handler::Op *op,
		std::string& entry,
		RGWObjVersionTracker& objv_tracker,
		optional_yield y,
    const DoutPrefixProvider *dpp) final;

  int do_put(RGWSI_MetaBackend_Handler::Op *op,
	     std::string& entr,
	     RGWMetadataObject *obj,
	     RGWObjVersionTracker& objv_tracker,
	     optional_yield y,
       const DoutPrefixProvider *dpp,
	     RGWMDLogSyncType type,
       bool from_remote_zone) override;

private:
  Driver* driver;
};
} } // namespace rgw::sal
