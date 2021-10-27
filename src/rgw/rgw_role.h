// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_ROLE_H
#define CEPH_RGW_ROLE_H

#include <string>

#include "common/async/yield_context.h"

#include "common/ceph_json.h"
#include "common/ceph_context.h"
#include "rgw/rgw_rados.h"
#include "rgw_metadata.h"

class RGWCtl;
class RGWRados;
class RGWSI_Role;
class RGWSI_MetaBackend_Handler;
class RGWRoleCtl;

namespace rgw { namespace sal {
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

  RGWRoleCtl *role_ctl;

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

public:
  virtual int store_info(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y) = 0;
  virtual int store_name(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y) = 0;
  virtual int store_path(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y) = 0;
  virtual int read_id(const DoutPrefixProvider *dpp, const std::string& role_name, const std::string& tenant, std::string& role_id, optional_yield y) = 0;
  virtual int read_name(const DoutPrefixProvider *dpp, optional_yield y) = 0;
  virtual int read_info(const DoutPrefixProvider *dpp, optional_yield y) = 0;
  bool validate_input(const DoutPrefixProvider* dpp);
  void extract_name_tenant(const std::string& str);

  RGWRole(std::string name,
          std::string tenant,
          std::string path="",
          std::string trust_policy="",
          std::string max_session_duration_str="",
          std::multimap<std::string,std::string> tags={})
  : name(std::move(name)),
    path(std::move(path)),
    trust_policy(std::move(trust_policy)),
    tenant(std::move(tenant)),
    tags(std::move(tags)) {
    if (this->path.empty())
      this->path = "/";
    extract_name_tenant(name);
    if (max_session_duration_str.empty()) {
      max_session_duration = SESSION_DURATION_MIN;
    } else {
      max_session_duration = std::stoull(max_session_duration_str);
    }
    mtime = real_time();
  }

  RGWRole(std::string id) : id(std::move(id)) {}

  RGWRole() = default;

  virtual ~RGWRole() = default;

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

  const std::string& get_id() const { return id; }
  const std::string& get_name() const { return name; }
  const std::string& get_tenant() const { return tenant; }
  const std::string& get_path() const { return path; }
  const std::string& get_create_date() const { return creation_date; }
  const std::string& get_assume_role_policy() const { return trust_policy;}
  const uint64_t& get_max_session_duration() const { return max_session_duration; }
  const RGWObjVersionTracker& get_objv_tracker() const { return objv_tracker; }
  const real_time& get_mtime() const { return mtime; }

  void set_id(const std::string& id) { this->id = id; }
  //TODO: Remove the following two
  void set_arn(const std::string& arn) { this->arn = arn; }
  void set_creation_date(const std::string& creation_date) { this->creation_date = creation_date; }
  void set_mtime(const real_time& mtime) { this->mtime = mtime; }

  virtual int create(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y) = 0;
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
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);

  static const std::string& get_names_oid_prefix();
  static const std::string& get_info_oid_prefix();
  static const std::string& get_path_oid_prefix();
};
WRITE_CLASS_ENCODER(RGWRole)

struct RGWRoleCompleteInfo {
  RGWRole* info;
  std::map<std::string, bufferlist> attrs;
  bool has_attrs;

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};

class RGWRoleMetadataObject: public RGWMetadataObject {
  RGWRoleCompleteInfo rci;
public:
  RGWRoleMetadataObject() = default;
  RGWRoleMetadataObject(const RGWRoleCompleteInfo& _rci,
			const obj_version& v,
			real_time m) : RGWMetadataObject(v,m), rci(_rci) {}

  void dump(Formatter *f) const override {
    rci.dump(f);
  }

  RGWRoleCompleteInfo& get_rci() {
    return rci;
  }
};

class RGWRoleMetadataHandler: public RGWMetadataHandler_GenericMetaBE
{
public:
  struct Svc {
    RGWSI_Zone *zone{nullptr};
    RGWSI_Meta *meta{nullptr};
    RGWSI_MetaBackend *meta_be{nullptr};
    RGWSI_SysObj *sysobj{nullptr};
  } svc;

  void init(RGWSI_Zone *_zone_svc,
	    RGWSI_Meta *_meta_svc,
	    RGWSI_MetaBackend *_meta_be_svc,
	    RGWSI_SysObj *_sysobj_svc);

  RGWSI_MetaBackend_Handler * get_be_handler();

  //int do_start(optional_yield y, const DoutPrefixProvider *dpp);

  RGWRoleMetadataHandler(CephContext *cct, Store* store,
                          RGWSI_Zone *_zone_svc,
                          RGWSI_Meta *_meta_svc,
                          RGWSI_MetaBackend *_meta_be_svc,
                          RGWSI_SysObj *_sysobj_svc);

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
  RGWSI_MetaBackend_Handler *be_handler;
  std::unique_ptr<RGWSI_MetaBackend::Module> be_module;
  std::unique_ptr<Store> store;
  CephContext *cct;
};
} } // namespace rgw::sal

#endif /* CEPH_RGW_ROLE_H */
