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

using namespace std;

namespace rgw { namespace sal {

const string RGWRole::role_name_oid_prefix = "role_names.";
const string RGWRole::role_oid_prefix = "roles.";
const string RGWRole::role_path_oid_prefix = "role_paths.";
const string RGWRole::role_arn_prefix = "arn:aws:iam::";

int RGWRole::store_info(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  RGWObjVersionTracker objv_tracker;
  return role_ctl->store_info(*this,
			      y,
            dpp,
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


int RGWRole::create(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  if (!validate_input(dpp)) {
    ldpp_dout(dpp, 0) << "ERROR: invalid input " << dendl;
    return -EINVAL;
  }

  uuid_d new_role_id;
  new_role_id.generate_random();

  id = new_role_id.to_string();
  arn = role_arn_prefix + tenant + ":role" + path + name;
  creation_date = generate_ctime();


  RGWObjVersionTracker objv_tracker;
  return role_ctl->create(*this, y, dpp,
			  RGWRoleCtl::PutParams().
			  set_exclusive(exclusive).
			  set_objv_tracker(&objv_tracker));
}

int RGWRole::delete_obj(const DoutPrefixProvider *dpp, optional_yield y)
{

  int ret = read_name(dpp, y);
  if (ret < 0) {
    return ret;
  }

  ret = read_info(dpp, y);
  if (ret < 0) {
    return ret;
  }

  if (! perm_policy_map.empty()) {
    return -ERR_DELETE_CONFLICT;
  }

  RGWObjVersionTracker ot;
  ret = role_ctl->delete_role(*this, y, dpp,
			      RGWRoleCtl::RemoveParams().
			      set_objv_tracker(&ot));
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: deleting role: "<< id
		  << cpp_strerror(-ret) << dendl;
  }

  return ret;
}

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

int RGWRole::read_id(const DoutPrefixProvider *dpp, const std::string& role_name, const std::string& tenant, std::string& role_id, optional_yield y)
{
  int ret;
  RGWObjVersionTracker ot;
  std::tie(ret, role_id) = role_ctl->read_name(role_name, tenant, y, dpp,
					       RGWRoleCtl::GetParams().
					       set_objv_tracker(&ot));
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed reading role id with params"
		 << role_name << ", " << tenant << ":"
		 << cpp_strerror(ret) << dendl;
  }

  return ret;
}

int RGWRole::read_info(const DoutPrefixProvider *dpp, optional_yield y)
{

  RGWObjVersionTracker ot;
  auto ret = role_ctl->read_info(id, y, dpp, this,
					  RGWRoleCtl::GetParams().
					  set_objv_tracker(&ot));
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed reading role info from pool: "
                  ": " << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return 0;
}

int RGWRole::read_name(const DoutPrefixProvider *dpp, optional_yield y)
{
  RGWObjVersionTracker ot;
  auto [ret, _id] = role_ctl->read_name(name, tenant, y, dpp,
					RGWRoleCtl::GetParams().
					set_objv_tracker(&ot));
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed reading role name from pool: "
                  << name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  id = _id;
  return 0;
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
} } // namespace rgw::sal

int RGWRoleCtl::create(rgw::sal::RGWRole& role,
		       optional_yield y,
           const DoutPrefixProvider *dpp,
		       const PutParams& params)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.role->create(op->ctx(),
			    role,
			    params.objv_tracker,
			    params.mtime,
			    params.exclusive,
			    params.attrs,
			    y,
          dpp);
  });
}

int RGWRoleCtl::store_info(const rgw::sal::RGWRole& role,
			   optional_yield y,
         const DoutPrefixProvider *dpp,
			   const PutParams& params)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.role->store_info(op->ctx(),
				role,
				params.objv_tracker,
				params.mtime,
				params.exclusive,
				params.attrs,
				y, dpp);
  });
}

int RGWRoleCtl::store_name(const std::string& role_id,
			   const std::string& name,
			   const std::string& tenant,
			   optional_yield y,
         const DoutPrefixProvider *dpp,
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
				y, dpp);
  });
}

int RGWRoleCtl::store_path(const std::string& role_id,
			   const std::string& path,
			   const std::string& tenant,
			   optional_yield y,
         const DoutPrefixProvider *dpp,
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
				y, dpp);
  });
}

int
RGWRoleCtl::read_info(const std::string& role_id,
		      optional_yield y,
          const DoutPrefixProvider *dpp,
          rgw::sal::RGWRole* info,
		      const GetParams& params)
{
  int ret = be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.role->read_info(op->ctx(),
			       role_id,
			       info,
			       params.objv_tracker,
			       params.mtime,
			       params.attrs,
			       y, dpp);
  });
  return ret;
}

std::pair<int, std::string>
RGWRoleCtl::read_name(const std::string& name,
		      const std::string& tenant,
		      optional_yield y,
          const DoutPrefixProvider *dpp,
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
			       y, dpp);
  });
  return make_pair(ret, role_id);
}

int RGWRoleCtl::delete_role(const rgw::sal::RGWRole& info,
			    optional_yield y,
          const DoutPrefixProvider *dpp,
			    const RemoveParams& params)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.role->delete_role(op->ctx(),
				 info,
				 params.objv_tracker,
				 y, dpp);
  });
  return 0;
}

int RGWRoleCtl::delete_info(const std::string& role_id,
			    optional_yield y,
          const DoutPrefixProvider *dpp,
			    const RemoveParams& params)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.role->delete_info(op->ctx(),
				 role_id,
				 params.objv_tracker,
				 y, dpp);
  });
  return 0;
}

int RGWRoleCtl::delete_name(const std::string& name,
			    const std::string& tenant,
			    optional_yield y,
          const DoutPrefixProvider *dpp,
			    const RemoveParams& params)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.role->delete_name(op->ctx(),
				 name,
				 tenant,
				 params.objv_tracker,
				 y, dpp);
  });
}

int RGWRoleCtl::delete_path(const std::string& role_id,
			    const std::string& path,
			    const std::string& tenant,
			    optional_yield y,
          const DoutPrefixProvider *dpp,
			    const RemoveParams& params)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.role->delete_path(op->ctx(),
				 role_id,
				 path,
				 tenant,
				 params.objv_tracker,
				 y, dpp);
  });
}

int RGWRoleCtl::list_roles_by_path_prefix(const std::string& path_prefix,
					  const std::string& tenant,
					  std::vector<rgw::sal::RGWRole>& roles,
					  optional_yield y,
            const DoutPrefixProvider *dpp)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.role->list_roles_by_path_prefix(op->ctx(),
					       path_prefix,
					       tenant,
					       roles,
					       y,
                 dpp);
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
                                   optional_yield y,
                                   const DoutPrefixProvider *dpp)
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
                                y,
                                dpp);

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
  rgw::sal::RGWRole info;
  real_time _mtime;
  int ret = svc.role->read_info(op->ctx(), entry, &info, &objv_tracker,
				&_mtime, nullptr, y, dpp);
  if (ret < 0) {
    return ret == -ENOENT ? 0 : ret;
  }
  return svc.role->delete_role(op->ctx(), info, &objv_tracker, y, dpp);
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

    int ret = rhandler->svc.role->create(op->ctx(),
					 rci.info,
					 &objv_tracker,
					 mtime,
					 false,
					 pattrs,
					 y,
           dpp);

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
