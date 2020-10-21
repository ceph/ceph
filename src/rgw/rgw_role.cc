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

#define dout_subsys ceph_subsys_rgw


const string RGWRole::role_name_oid_prefix = "role_names.";
const string RGWRole::role_oid_prefix = "roles.";
const string RGWRole::role_path_oid_prefix = "role_paths.";
const string RGWRole::role_arn_prefix = "arn:aws:iam::";

int RGWRole::store_info(bool exclusive, optional_yield y)
{
  RGWObjVersionTracker objv_tracker;
  return role_ctl->store_info(info,
			      y,
			      RGWRoleCtl::PutParams().
			      set_exclusive(exclusive).
			      set_objv_tracker(&objv_tracker));
}

int RGWRole::store_name(bool exclusive, optional_yield y)
{
  RGWObjVersionTracker objv_tracker;
  return role_ctl->store_name(info.id,
			      info.name,
			      info.tenant,
			      y,
			      RGWRoleCtl::PutParams().
			      set_exclusive(exclusive).
			      set_objv_tracker(&objv_tracker)
			      );
}

int RGWRole::store_path(bool exclusive, optional_yield y)
{
  RGWObjVersionTracker objv_tracker;
  return role_ctl->store_path(info.id,
			      info.path,
			      info.tenant,
			      y,
			      RGWRoleCtl::PutParams().
			      set_exclusive(exclusive).
			      set_objv_tracker(&objv_tracker));
}

// Creation time
auto generate_ctime() {
  real_clock::time_point t = real_clock::now();

  struct timeval tv;
  real_clock::to_timeval(t, tv);

  char buf[30];
  struct tm result;
  gmtime_r(&tv.tv_sec, &result);
  strftime(buf,30,"%Y-%m-%dT%H:%M:%S", &result);
  sprintf(buf + strlen(buf),".%dZ",(int)tv.tv_usec/1000);
  string ct;
  ct.assign(buf, strlen(buf));
  return ct;
}


int RGWRole::create(bool exclusive, optional_yield y)
{
  if (! info.validate_input()) {
    ldout(cct, 0) << "ERROR: invalid input " << dendl;
    return -EINVAL;
  }

  uuid_d new_role_id;
  new_role_id.generate_random();

  info.id = new_role_id.to_string();
  info.arn = role_arn_prefix + info.tenant + ":role" + info.path + info.name;
  info.creation_date = generate_ctime();


  RGWObjVersionTracker objv_tracker;
  return role_ctl->create(info, y,
			  RGWRoleCtl::PutParams().
			  set_exclusive(exclusive).
			  set_objv_tracker(&objv_tracker));
}

int RGWRole::delete_obj()
{

  int ret = read_name();
  if (ret < 0) {
    return ret;
  }

  ret = read_info();
  if (ret < 0) {
    return ret;
  }

  if (! info.perm_policy_map.empty()) {
    return -ERR_DELETE_CONFLICT;
  }

  // Delete id
  ret = role_ctl->delete_info(info.id, null_yield);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: deleting role id: "
                  << info.id << ": " << cpp_strerror(-ret) << dendl;
  }

  // Delete name
  ret = role_ctl->delete_name(info.name, info.tenant, null_yield);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: deleting role: "
                  << info.name << ":" << cpp_strerror(-ret) << dendl;
  }

  // Delete path
  ret = role_ctl->delete_path(info.id, info.path, info.tenant, null_yield);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: deleting role path:"
                  << info.path << ": " << cpp_strerror(-ret) << dendl;
  }
  return ret;
}

int RGWRole::get()
{
  int ret = read_name();
  if (ret < 0) {
    return ret;
  }

  ret = read_info();
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWRole::get_by_id()
{
  int ret = read_info();
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWRole::update()
{
  int ret = store_info(false);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR:  updating info "
                  << info.id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return 0;
}

void RGWRoleInfo::set_perm_policy(const string& policy_name, const string& perm_policy)
{
  perm_policy_map[policy_name] = perm_policy;
}

vector<string> RGWRoleInfo::get_role_policy_names()
{
  vector<string> policy_names;
  for (const auto& it : perm_policy_map)
  {
    policy_names.push_back(std::move(it.first));
  }

  return policy_names;
}

int RGWRoleInfo::get_role_policy(const string& policy_name, string& perm_policy)
{
  const auto it = perm_policy_map.find(policy_name);
  if (it == perm_policy_map.end()) {
    //ldout(cct, 0) << "ERROR: Policy name: " << policy_name << " not found" << dendl;
    return -ENOENT;
  } else {
    perm_policy = it->second;
  }
  return 0;
}

int RGWRoleInfo::delete_policy(const string& policy_name)
{
  const auto& it = perm_policy_map.find(policy_name);
  if (it == perm_policy_map.end()) {
    //ldout(cct, 0) << "ERROR: Policy name: " << policy_name << " not found" << dendl;
    return -ENOENT;
  } else {
    perm_policy_map.erase(it);
  }
    return 0;
}

void RGWRoleInfo::dump(Formatter *f) const
{
  encode_json("RoleId", id , f);
  encode_json("RoleName", name , f);
  encode_json("Path", path, f);
  encode_json("Arn", arn, f);
  encode_json("CreateDate", creation_date, f);
  encode_json("MaxSessionDuration", max_session_duration, f);
  encode_json("AssumeRolePolicyDocument", trust_policy, f);
}

void RGWRoleInfo::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("RoleId", id, obj);
  JSONDecoder::decode_json("RoleName", name, obj);
  JSONDecoder::decode_json("Path", path, obj);
  JSONDecoder::decode_json("Arn", arn, obj);
  JSONDecoder::decode_json("CreateDate", creation_date, obj);
  JSONDecoder::decode_json("MaxSessionDuration", max_session_duration, obj);
  JSONDecoder::decode_json("AssumeRolePolicyDocument", trust_policy, obj);
}

int RGWRole::read_id(const string& role_name, const string& tenant, string& role_id)
{
  int ret;
  RGWObjVersionTracker ot;
  std::tie(ret, role_id) = role_ctl->read_name(role_name, tenant, null_yield,
					       RGWRoleCtl::GetParams().
					       set_objv_tracker(&ot));
  if (ret < 0) {
    ldout(cct,0) << "ERROR: failed reading role id with params"
		 << role_name << ", " << tenant << ":"
		 << cpp_strerror(ret) << dendl;
  }

  return ret;
}

int RGWRole::read_info()
{

  RGWObjVersionTracker ot;
  auto [ret, _info] = role_ctl->read_info(info.id, null_yield,
					  RGWRoleCtl::GetParams().
					  set_objv_tracker(&ot));
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed reading role info from pool: "
                  ": " << info.id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  info = std::move(_info);
  return 0;
}

int RGWRole::read_name()
{
  RGWObjVersionTracker ot;
  auto [ret, _id] = role_ctl->read_name(info.name, info.tenant, null_yield,
					RGWRoleCtl::GetParams().
					set_objv_tracker(&ot));
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed reading role name from pool: "
                  << info.name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  info.id = _id;
  return 0;
}

bool RGWRoleInfo::validate_input()
{
  if (name.length() > MAX_ROLE_NAME_LEN) {
    //ldout(cct, 0) << "ERROR: Invalid name length " << dendl;
    return false;
  }

  if (path.length() > MAX_PATH_NAME_LEN) {
    //ldout(cct, 0) << "ERROR: Invalid path length " << dendl;
    return false;
  }

  std::regex regex_name("[A-Za-z0-9:=,.@-]+");
  if (! std::regex_match(name, regex_name)) {
    //ldout(cct, 0) << "ERROR: Invalid chars in name " << dendl;
    return false;
  }

  std::regex regex_path("(/[!-~]+/)|(/)");
  if (! std::regex_match(path,regex_path)) {
    //ldout(cct, 0) << "ERROR: Invalid chars in path " << dendl;
    return false;
  }

  if (max_session_duration < SESSION_DURATION_MIN ||
          max_session_duration > SESSION_DURATION_MAX) {
    //ldout(cct, 0) << "ERROR: Invalid session duration, should be between 3600 and 43200 seconds " << dendl;
    return false;
  }
  return true;
}

void RGWRoleInfo::extract_name_tenant(std::string_view str) {
  if (auto pos = str.find('$');
      pos != std::string::npos) {
    tenant = str.substr(0, pos);
    name = str.substr(pos+1);
  }
}

void RGWRole::update_trust_policy(string& trust_policy)
{
  this->info.trust_policy = trust_policy;
}

int RGWRole::get_roles_by_path_prefix(RGWRados *store,
                                      CephContext *cct,
                                      const string& path_prefix,
                                      const string& tenant,
                                      vector<RGWRole>& roles)
{
  auto pool = store->svc.zone->get_zone_params().roles_pool;
  string prefix;

  // List all roles if path prefix is empty
  if (! path_prefix.empty()) {
    prefix = tenant + role_path_oid_prefix + path_prefix;
  } else {
    prefix = tenant + role_path_oid_prefix;
  }

  //Get the filtered objects
  list<string> result;
  bool is_truncated;
  RGWListRawObjsCtx ctx;
  do {
    list<string> oids;
    int r = store->list_raw_objects(pool, prefix, 1000, ctx, oids, &is_truncated);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: listing filtered objects failed: " << pool.name << ": "
                  << prefix << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
    for (const auto& iter : oids) {
      result.push_back(iter.substr(role_path_oid_prefix.size()));
    }
  } while (is_truncated);

  for (const auto& it : result) {
    //Find the role oid prefix from the end
    size_t pos = it.rfind(role_oid_prefix);
    if (pos == string::npos) {
        continue;
    }
    // Split the result into path and info_oid + id
    string path = it.substr(0, pos);

    /*Make sure that prefix is part of path (False results could've been returned)
      because of the role info oid + id appended to the path)*/
    if(path_prefix.empty() || path.find(path_prefix) != string::npos) {
      //Get id from info oid prefix + id
      string id = it.substr(pos + role_oid_prefix.length());

      RGWRole role(cct, store->ctl.role);
      role.set_id(id);
      int ret = role.read_info();
      if (ret < 0) {
        return ret;
      }
      roles.push_back(std::move(role));
    }
  }

  return 0;
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

int RGWRoleCtl::create(RGWRoleInfo& role,
		       optional_yield y,
		       const PutParams& params)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.role->create(op->ctx(),
			    role,
			    params.objv_tracker,
			    params.mtime,
			    params.exclusive,
			    params.attrs,
			    y);
  });
}

int RGWRoleCtl::store_info(const RGWRoleInfo& role,
			   optional_yield y,
			   const PutParams& params)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.role->store_info(op->ctx(),
				role,
				params.objv_tracker,
				params.mtime,
				params.exclusive,
				params.attrs,
				y);
  });
}

int RGWRoleCtl::store_name(const std::string& role_id,
			   const std::string& name,
			   const std::string& tenant,
			   optional_yield y,
			   const PutParams& params)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.role->store_name(op->ctx(),
				role_id,
				name,
				tenant,
				params.objv_tracker,
				params.mtime,
				params.exclusive,
				y);
  });
}

int RGWRoleCtl::store_path(const std::string& role_id,
			   const std::string& path,
			   const std::string& tenant,
			   optional_yield y,
			   const PutParams& params)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.role->store_path(op->ctx(),
				role_id,
				path,
				tenant,
				params.objv_tracker,
				params.mtime,
				params.exclusive,
				y);
  });
}

std::pair<int, RGWRoleInfo>
RGWRoleCtl::read_info(const std::string& role_id,
		      optional_yield y,
		      const GetParams& params)
{
  RGWRoleInfo info;
  int ret = be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.role->read_info(op->ctx(),
			       role_id,
			       &info,
			       params.objv_tracker,
			       params.mtime,
			       params.attrs,
			       y);
  });
  return make_pair(ret, info);
}

std::pair<int, std::string>
RGWRoleCtl::read_name(const std::string& name,
		      const std::string& tenant,
		      optional_yield y,
		      const GetParams& params)
{
  std::string role_id;
  int ret = be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.role->read_name(op->ctx(),
			       name,
			       tenant,
			       role_id,
			       params.objv_tracker,
			       params.mtime,
			       y);
  });
  return make_pair(ret, role_id);
}

int RGWRoleCtl::delete_info(const RGWRoleInfo& info,
			    optional_yield y,
			    const RemoveParams& params)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.role->delete_info(op->ctx(),
				 info,
				 params.objv_tracker,
				 y);
  });
  return 0;
}

int RGWRoleCtl::delete_name(const std::string& name,
			    const std::string& tenant,
			    optional_yield y,
			    const RemoveParams& params)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.role->delete_name(op->ctx(),
				 name,
				 tenant,
				 params.objv_tracker,
				 y);
  });
}

int RGWRoleCtl::delete_path(const std::string& role_id,
			    const std::string& path,
			    const std::string& tenant,
			    optional_yield y,
			    const RemoveParams& params)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.role->delete_path(op->ctx(),
				 role_id,
				 path,
				 tenant,
				 params.objv_tracker,
				 y);
  });
}

RGWRoleMetadataHandler::RGWRoleMetadataHandler(RGWSI_Role *role_svc)
{
  base_init(role_svc->ctx(), role_svc->get_be_handler());
  svc.role = role_svc;
}

void RGWRoleCompleteInfo::dump(ceph::Formatter *f) const
{
  info.dump(f);
  if (has_attrs) {
    encode_json("attrs", attrs, f);
  }
}

void RGWRoleCompleteInfo::decode_json(JSONObj *obj)
{
  decode_json_obj(info, obj);
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
                                   optional_yield y)
{
  RGWRoleCompleteInfo rci;
  RGWObjVersionTracker objv_tracker;
  real_time mtime;

  int ret = svc.role->read_info(op->ctx(),
                                entry,
                                &rci.info,
                                &objv_tracker,
                                &mtime,
                                &rci.attrs,
                                y);

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
                                      optional_yield y)
{
  return svc.role->delete_info(op->ctx(), entry, &objv_tracker, y);
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
                             RGWMDLogSyncType type) :
    RGWMetadataHandlerPut_SObj(handler, op, entry, obj, objv_tracker, y, type),
    rhandler(handler) {
    mdo = static_cast<RGWRoleMetadataObject*>(obj);
  }

  int put_checked() override {
    auto& rci = mdo->get_rci();
    auto mtime = mdo->get_mtime();
    map<std::string, bufferlist> *pattrs = rci.has_attrs ? &rci.attrs : nullptr;

    int ret = rhandler->svc.role->create(op->ctx(),
					 rci.info,
					 &objv_tracker,
					 mtime,
					 false,
					 pattrs,
					 y);

    return ret < 0 ? ret : STATUS_APPLIED;
  }
};

int RGWRoleMetadataHandler::do_put(RGWSI_MetaBackend_Handler::Op *op,
                                   std::string& entry,
                                   RGWMetadataObject *obj,
                                   RGWObjVersionTracker& objv_tracker,
                                   optional_yield y,
                                   RGWMDLogSyncType type)
{
  RGWMetadataHandlerPut_Role put_op(this, op , entry, obj, objv_tracker, y, type);
  return do_put_operate(&put_op);
}
