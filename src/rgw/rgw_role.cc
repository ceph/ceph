// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>
#include <ctime>
#include <regex>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"
#include "rgw_rados.h"
#include "rgw_zone.h"

#include "include/types.h"
#include "rgw_string.h"

#include "rgw_common.h"
#include "rgw_tools.h"
#include "rgw_role.h"

#include "services/svc_zone.h"
#include "services/svc_sys_obj.h"
#include "services/svc_role.h"
#include "services/svc_meta_be_sobj.h"
#include "services/svc_meta.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw { namespace sal {

const string RGWRole::role_name_oid_prefix = "role_names.";
const string RGWRole::role_oid_prefix = "roles.";
const string RGWRole::role_path_oid_prefix = "role_paths.";
const string RGWRole::role_arn_prefix = "arn:aws:iam::";

int RGWRole::get(const DoutPrefixProvider *dpp, optional_yield y)
{
  int ret = read_name(dpp, y);
  if (ret < 0) {
    return ret;
  }

  ret = read_info(dpp, y);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWRole::get_by_id(const DoutPrefixProvider *dpp, optional_yield y)
{
  int ret = read_info(dpp, y);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWRole::update(const DoutPrefixProvider *dpp, optional_yield y)
{
  int ret = store_info(dpp, false, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR:  storing info in Role pool: "
                  << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return 0;
}

void RGWRole::set_perm_policy(const string& policy_name, const string& perm_policy)
{
  perm_policy_map[policy_name] = perm_policy;
}

vector<string> RGWRole::get_role_policy_names()
{
  vector<string> policy_names;
  for (const auto& it : perm_policy_map)
  {
    policy_names.push_back(std::move(it.first));
  }

  return policy_names;
}

int RGWRole::get_role_policy(const DoutPrefixProvider* dpp, const string& policy_name, string& perm_policy)
{
  const auto it = perm_policy_map.find(policy_name);
  if (it == perm_policy_map.end()) {
    ldpp_dout(dpp, 0) << "ERROR: Policy name: " << policy_name << " not found" << dendl;
    return -ENOENT;
  } else {
    perm_policy = it->second;
  }
  return 0;
}

int RGWRole::delete_policy(const DoutPrefixProvider* dpp, const string& policy_name)
{
  const auto& it = perm_policy_map.find(policy_name);
  if (it == perm_policy_map.end()) {
    ldpp_dout(dpp, 0) << "ERROR: Policy name: " << policy_name << " not found" << dendl;
    return -ENOENT;
  } else {
    perm_policy_map.erase(it);
  }
  return 0;
}

void RGWRole::dump(Formatter *f) const
{
  encode_json("RoleId", id , f);
  encode_json("RoleName", name , f);
  encode_json("Path", path, f);
  encode_json("Arn", arn, f);
  encode_json("CreateDate", creation_date, f);
  encode_json("MaxSessionDuration", max_session_duration, f);
  encode_json("AssumeRolePolicyDocument", trust_policy, f);
  if (!tags.empty()) {
    f->open_array_section("Tags");
    for (const auto& it : tags) {
      f->open_object_section("Key");
      encode_json("Key", it.first, f);
      f->close_section();
      f->open_object_section("Value");
      encode_json("Value", it.second, f);
      f->close_section();
    }
    f->close_section();
  }
}

void RGWRole::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("RoleId", id, obj);
  JSONDecoder::decode_json("RoleName", name, obj);
  JSONDecoder::decode_json("Path", path, obj);
  JSONDecoder::decode_json("Arn", arn, obj);
  JSONDecoder::decode_json("CreateDate", creation_date, obj);
  JSONDecoder::decode_json("MaxSessionDuration", max_session_duration, obj);
  JSONDecoder::decode_json("AssumeRolePolicyDocument", trust_policy, obj);
}

bool RGWRole::validate_input(const DoutPrefixProvider* dpp)
{
  if (name.length() > MAX_ROLE_NAME_LEN) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid name length " << dendl;
    return false;
  }

  if (path.length() > MAX_PATH_NAME_LEN) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid path length " << dendl;
    return false;
  }

  std::regex regex_name("[A-Za-z0-9:=,.@-]+");
  if (! std::regex_match(name, regex_name)) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid chars in name " << dendl;
    return false;
  }

  std::regex regex_path("(/[!-~]+/)|(/)");
  if (! std::regex_match(path,regex_path)) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid chars in path " << dendl;
    return false;
  }

  if (max_session_duration < SESSION_DURATION_MIN ||
          max_session_duration > SESSION_DURATION_MAX) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid session duration, should be between 3600 and 43200 seconds " << dendl;
    return false;
  }
  return true;
}

void RGWRole::extract_name_tenant(const std::string& str) {
  if (auto pos = str.find('$');
      pos != std::string::npos) {
    tenant = str.substr(0, pos);
    name = str.substr(pos+1);
  }
}

void RGWRole::update_trust_policy(string& trust_policy)
{
  this->trust_policy = trust_policy;
}

int RGWRole::set_tags(const DoutPrefixProvider* dpp, const multimap<string,string>& tags_map)
{
  for (auto& it : tags_map) {
    this->tags.emplace(it.first, it.second);
  }
  if (this->tags.size() > 50) {
    ldpp_dout(dpp, 0) << "No. of tags is greater than 50" << dendl;
    return -EINVAL;
  }
  return 0;
}

boost::optional<multimap<string,string>> RGWRole::get_tags()
{
  if(this->tags.empty()) {
    return boost::none;
  }
  return this->tags;
}

void RGWRole::erase_tags(const vector<string>& tagKeys)
{
  for (auto& it : tagKeys) {
    this->tags.erase(it);
  }
}

const string& RGWRole::get_names_oid_prefix()
{
  return role_name_oid_prefix;
}

const string& RGWRole::get_info_oid_prefix()
{
  return role_oid_prefix;
}

const string& RGWRole::get_path_oid_prefix()
{
  return role_path_oid_prefix;
}

class RGWSI_Role_Module : public RGWSI_MBSObj_Handler_Module {
  RGWRoleMetadataHandler::Svc& svc;
  const std::string prefix;
public:
  RGWSI_Role_Module(RGWRoleMetadataHandler::Svc& _svc): RGWSI_MBSObj_Handler_Module("roles"),
                                                  svc(_svc),
                                                  prefix(rgw::sal::RGWRole::get_info_oid_prefix()) {}

  void get_pool_and_oid(const std::string& key,
                        rgw_pool *pool,
                        std::string *oid) override
  {
    if (pool) {
      *pool = svc.zone->get_zone_params().roles_pool;
    }

    if (oid) {
      *oid = key_to_oid(key);
    }
  }

  bool is_valid_oid(const std::string& oid) override {
    return boost::algorithm::starts_with(oid, prefix);
  }

  std::string key_to_oid(const std::string& key) override {
    return prefix + key;
  }

  // This is called after `is_valid_oid` and is assumed to be a valid oid
  std::string oid_to_key(const std::string& oid) override {
    return oid.substr(prefix.size());
  }

  const std::string& get_oid_prefix() {
    return prefix;
  }
};

RGWSI_MetaBackend_Handler* RGWRoleMetadataHandler::get_be_handler()
{
  return be_handler;
}

void RGWRoleMetadataHandler::init(RGWSI_Zone *_zone_svc,
                                  RGWSI_Meta *_meta_svc,
                                  RGWSI_MetaBackend *_meta_be_svc,
                                  RGWSI_SysObj *_sysobj_svc)
{
  svc.zone = _zone_svc;
  svc.meta = _meta_svc;
  svc.meta_be = _meta_be_svc;
  svc.sysobj = _sysobj_svc;
}

int RGWRoleMetadataHandler::do_start(optional_yield y, const DoutPrefixProvider *dpp)
{

  int r = svc.meta->create_be_handler(RGWSI_MetaBackend::Type::MDBE_SOBJ,
                                      &be_handler);
  if (r < 0) {
    //ldout(ctx(), 0) << "ERROR: failed to create be_handler for Roles: r="
    //                << r <<dendl;
    return r;
  }

  auto module = new RGWSI_Role_Module(svc);
  RGWSI_MetaBackend_Handler_SObj* bh= static_cast<RGWSI_MetaBackend_Handler_SObj *>(be_handler);
  be_module.reset(module);
  bh->set_module(module);
  return 0;
}

RGWRoleMetadataHandler::RGWRoleMetadataHandler(CephContext *cct, Store* store)
{
  base_init(cct, get_be_handler());
  store = store;
}

void RGWRoleCompleteInfo::dump(ceph::Formatter *f) const
{
  info->dump(f);
  if (has_attrs) {
    encode_json("attrs", attrs, f);
  }
}

void RGWRoleCompleteInfo::decode_json(JSONObj *obj)
{
  decode_json_obj(*info, obj);
  has_attrs = JSONDecoder::decode_json("attrs", attrs, obj);
}


RGWMetadataObject *RGWRoleMetadataHandler::get_meta_obj(JSONObj *jo,
							const obj_version& objv,
							const ceph::real_time& mtime)
{
  RGWRoleCompleteInfo rci;

  try {
    decode_json_obj(rci, jo);
  } catch (JSONDecoder:: err& e) {
    return nullptr;
  }

  return new RGWRoleMetadataObject(rci, objv, mtime);
}

int RGWRoleMetadataHandler::do_get(RGWSI_MetaBackend_Handler::Op *op,
                                   std::string& entry,
                                   RGWMetadataObject **obj,
                                   optional_yield y,
                                   const DoutPrefixProvider *dpp)
{
  RGWRoleCompleteInfo rci;
  RGWObjVersionTracker objv_tracker;
  real_time mtime;

#if 0
  int ret = svc.role->read_info(op->ctx(),
                                entry,
                                &rci.info,
                                &objv_tracker,
                                &mtime,
                                &rci.attrs,
                                y,
                                dpp);
#endif
  rci.info = store->get_role(entry).get();
  int ret = rci.info->read_info(dpp, y);
  if (ret < 0) {
    return ret;
  }

  RGWRoleMetadataObject *rdo = new RGWRoleMetadataObject(rci, objv_tracker.read_version,
                                                         mtime);
  *obj = rdo;

  return 0;
}

int RGWRoleMetadataHandler::do_remove(RGWSI_MetaBackend_Handler::Op *op,
                                      std::string& entry,
                                      RGWObjVersionTracker& objv_tracker,
                                      optional_yield y,
                                      const DoutPrefixProvider *dpp)
{
#if 0
  int ret = svc.role->read_info(op->ctx(), entry, &info, &objv_tracker,
				&_mtime, nullptr, y, dpp);
  if (ret < 0) {
    return ret == -ENOENT ? 0 : ret;
  }
  return svc.role->delete_role(op->ctx(), info, &objv_tracker, y, dpp);
#endif
  std::unique_ptr<rgw::sal::RGWRole> role = store->get_role(entry);
  int ret = role->read_info(dpp, y);
  if (ret < 0) {
    return ret == -ENOENT? 0 : ret;
  }

  return role->delete_obj(dpp, y);
}

class RGWMetadataHandlerPut_Role : public RGWMetadataHandlerPut_SObj
{
  RGWRoleMetadataHandler *rhandler;
  RGWRoleMetadataObject *mdo;
public:
  RGWMetadataHandlerPut_Role(RGWRoleMetadataHandler *handler,
                             RGWSI_MetaBackend_Handler::Op *op,
                             std::string& entry,
                             RGWMetadataObject *obj,
                             RGWObjVersionTracker& objv_tracker,
                             optional_yield y,
                             RGWMDLogSyncType type,
                             bool from_remote_zone) :
    RGWMetadataHandlerPut_SObj(handler, op, entry, obj, objv_tracker, y, type, from_remote_zone),
    rhandler(handler) {
    mdo = static_cast<RGWRoleMetadataObject*>(obj);
  }

  int put_checked(const DoutPrefixProvider *dpp) override {
    auto& rci = mdo->get_rci();
    auto mtime = mdo->get_mtime();
    map<std::string, bufferlist> *pattrs = rci.has_attrs ? &rci.attrs : nullptr;
#if 0
    int ret = rhandler->role->create(op->ctx(),
					 rci.info,
					 &objv_tracker,
					 mtime,
					 false,
					 pattrs,
					 y,
           dpp);

    return ret < 0 ? ret : STATUS_APPLIED;
#endif
    int ret = rci.info->create(dpp, true, y);
    return ret < 0 ? ret : STATUS_APPLIED;
  }
};

int RGWRoleMetadataHandler::do_put(RGWSI_MetaBackend_Handler::Op *op,
                                   std::string& entry,
                                   RGWMetadataObject *obj,
                                   RGWObjVersionTracker& objv_tracker,
                                   optional_yield y,
                                   const DoutPrefixProvider *dpp,
                                   RGWMDLogSyncType type,
                                   bool from_remote_zone)
{
  RGWMetadataHandlerPut_Role put_op(this, op , entry, obj, objv_tracker, y, type, from_remote_zone);
  return do_put_operate(&put_op, dpp);
}

} } // namespace rgw::sal
