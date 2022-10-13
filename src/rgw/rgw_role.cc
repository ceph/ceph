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

#define dout_subsys ceph_subsys_rgw


const string RGWRole::role_name_oid_prefix = "role_names.";
const string RGWRole::role_oid_prefix = "roles.";
const string RGWRole::role_path_oid_prefix = "role_paths.";
const string RGWRole::role_arn_prefix = "arn:aws:iam::";

int RGWRole::store_info(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  using ceph::encode;
  string oid = get_info_oid_prefix() + id;

  bufferlist bl;
  encode(*this, bl);

  auto svc = ctl->svc;

  auto obj_ctx = ctl->svc->sysobj->init_obj_ctx();

  if (!this->tags.empty()) {
    bufferlist bl_tags;
    encode(this->tags, bl_tags);
    map<string, bufferlist> attrs;
    attrs.emplace("tagging", bl_tags);
    return rgw_put_system_obj(dpp, obj_ctx, svc->zone->get_zone_params().roles_pool, oid, bl, exclusive, nullptr, real_time(), y, &attrs);
  }

  return rgw_put_system_obj(dpp, obj_ctx, svc->zone->get_zone_params().roles_pool, oid,
                            bl, exclusive, NULL, real_time(), y, NULL);
}

int RGWRole::store_name(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  RGWNameToId nameToId;
  nameToId.obj_id = id;

  string oid = tenant + get_names_oid_prefix() + name;

  bufferlist bl;
  using ceph::encode;
  encode(nameToId, bl);

  auto svc = ctl->svc;

  auto obj_ctx = svc->sysobj->init_obj_ctx();
  return rgw_put_system_obj(dpp, obj_ctx, svc->zone->get_zone_params().roles_pool, oid,
			    bl, exclusive, NULL, real_time(), y, NULL);
}

int RGWRole::store_path(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  string oid = tenant + get_path_oid_prefix() + path + get_info_oid_prefix() + id;

  auto svc = ctl->svc;

  bufferlist bl;
  auto obj_ctx = svc->sysobj->init_obj_ctx();
  return rgw_put_system_obj(dpp, obj_ctx, svc->zone->get_zone_params().roles_pool, oid,
			    bl, exclusive, NULL, real_time(), y, NULL);
}

int RGWRole::create(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  int ret;

  if (! validate_input()) {
    return -EINVAL;
  }

  /* check to see the name is not used */
  ret = read_id(dpp, name, tenant, id, y);
  if (exclusive && ret == 0) {
    ldpp_dout(dpp, 0) << "ERROR: name " << name << " already in use for role id "
                    << id << dendl;
    return -EEXIST;
  } else if ( ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) << "failed reading role id  " << id << ": "
                  << cpp_strerror(-ret) << dendl;
    return ret;
  }

  /* create unique id */
  uuid_d new_uuid;
  char uuid_str[37];
  new_uuid.generate_random();
  new_uuid.print(uuid_str);
  id = uuid_str;

  //arn
  arn = role_arn_prefix + tenant + ":role" + path + name;

  // Creation time
  real_clock::time_point t = real_clock::now();

  struct timeval tv;
  real_clock::to_timeval(t, tv);

  char buf[30];
  struct tm result;
  gmtime_r(&tv.tv_sec, &result);
  strftime(buf,30,"%Y-%m-%dT%H:%M:%S", &result);
  sprintf(buf + strlen(buf),".%dZ",(int)tv.tv_usec/1000);
  creation_date.assign(buf, strlen(buf));

  auto svc = ctl->svc;

  auto& pool = svc->zone->get_zone_params().roles_pool;
  ret = store_info(dpp, exclusive, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR:  storing role info in pool: " << pool.name << ": "
                  << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ret = store_name(dpp, exclusive, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: storing role name in pool: " << pool.name << ": "
                  << name << ": " << cpp_strerror(-ret) << dendl;

    //Delete the role info that was stored in the previous call
    string oid = get_info_oid_prefix() + id;
    int info_ret = rgw_delete_system_obj(dpp, svc->sysobj, pool, oid, NULL, y);
    if (info_ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: cleanup of role id from pool: " << pool.name << ": "
                  << id << ": " << cpp_strerror(-info_ret) << dendl;
    }
    return ret;
  }

  ret = store_path(dpp, exclusive, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: storing role path in pool: " << pool.name << ": "
                  << path << ": " << cpp_strerror(-ret) << dendl;
    //Delete the role info that was stored in the previous call
    string oid = get_info_oid_prefix() + id;
    int info_ret = rgw_delete_system_obj(dpp, svc->sysobj, pool, oid, NULL, y);
    if (info_ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: cleanup of role id from pool: " << pool.name << ": "
                  << id << ": " << cpp_strerror(-info_ret) << dendl;
    }
    //Delete role name that was stored in previous call
    oid = tenant + get_names_oid_prefix() + name;
    int name_ret = rgw_delete_system_obj(dpp, svc->sysobj, pool, oid, NULL, y);
    if (name_ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: cleanup of role name from pool: " << pool.name << ": "
                  << name << ": " << cpp_strerror(-name_ret) << dendl;
    }
    return ret;
  }
  return 0;
}

int RGWRole::delete_obj(const DoutPrefixProvider *dpp, optional_yield y)
{
  auto svc = ctl->svc;
  auto& pool = svc->zone->get_zone_params().roles_pool;

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

  // Delete id
  string oid = get_info_oid_prefix() + id;
  ret = rgw_delete_system_obj(dpp, svc->sysobj, pool, oid, NULL, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: deleting role id from pool: " << pool.name << ": "
                  << id << ": " << cpp_strerror(-ret) << dendl;
  }

  // Delete name
  oid = tenant + get_names_oid_prefix() + name;
  ret = rgw_delete_system_obj(dpp, svc->sysobj, pool, oid, NULL, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: deleting role name from pool: " << pool.name << ": "
                  << name << ": " << cpp_strerror(-ret) << dendl;
  }

  // Delete path
  oid = tenant + get_path_oid_prefix() + path + get_info_oid_prefix() + id;
  ret = rgw_delete_system_obj(dpp, svc->sysobj, pool, oid, NULL, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: deleting role path from pool: " << pool.name << ": "
                  << path << ": " << cpp_strerror(-ret) << dendl;
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
  auto& pool = ctl->svc->zone->get_zone_params().roles_pool;

  int ret = store_info(dpp, false, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR:  storing info in pool: " << pool.name << ": "
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

int RGWRole::get_role_policy(const string& policy_name, string& perm_policy)
{
  const auto it = perm_policy_map.find(policy_name);
  if (it == perm_policy_map.end()) {
    ldout(cct, 0) << "ERROR: Policy name: " << policy_name << " not found" << dendl;
    return -ENOENT;
  } else {
    perm_policy = it->second;
  }
  return 0;
}

int RGWRole::delete_policy(const string& policy_name)
{
  const auto& it = perm_policy_map.find(policy_name);
  if (it == perm_policy_map.end()) {
    ldout(cct, 0) << "ERROR: Policy name: " << policy_name << " not found" << dendl;
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
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("path", path, obj);
  JSONDecoder::decode_json("arn", arn, obj);
  JSONDecoder::decode_json("create_date", creation_date, obj);
  JSONDecoder::decode_json("max_session_duration", max_session_duration, obj);
  JSONDecoder::decode_json("assume_role_policy_document", trust_policy, obj);
}

int RGWRole::read_id(const DoutPrefixProvider *dpp, const string& role_name, const string& tenant, string& role_id, optional_yield y)
{
  auto svc = ctl->svc;
  auto& pool = svc->zone->get_zone_params().roles_pool;
  string oid = tenant + get_names_oid_prefix() + role_name;
  bufferlist bl;
  auto obj_ctx = svc->sysobj->init_obj_ctx();

  int ret = rgw_get_system_obj(obj_ctx, pool, oid, bl, NULL, NULL, y, dpp);
  if (ret < 0) {
    return ret;
  }

  RGWNameToId nameToId;
  try {
    auto iter = bl.cbegin();
    using ceph::decode;
    decode(nameToId, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode role from pool: " << pool.name << ": "
                  << role_name << dendl;
    return -EIO;
  }
  role_id = nameToId.obj_id;
  return 0;
}

int RGWRole::read_info(const DoutPrefixProvider *dpp, optional_yield y)
{
  auto svc = ctl->svc;
  auto& pool = svc->zone->get_zone_params().roles_pool;
  string oid = get_info_oid_prefix() + id;
  bufferlist bl;
  auto obj_ctx = svc->sysobj->init_obj_ctx();

  map<string, bufferlist> attrs;

  int ret = rgw_get_system_obj(obj_ctx, pool, oid, bl, NULL, NULL, y, dpp, &attrs, nullptr, boost::none, true);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed reading role info from pool: " << pool.name <<
                  ": " << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  try {
    using ceph::decode;
    auto iter = bl.cbegin();
    decode(*this, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode role info from pool: " << pool.name <<
                  ": " << id << dendl;
    return -EIO;
  }

  auto it = attrs.find("tagging");
  if (it != attrs.end()) {
    bufferlist bl_tags = it->second;
    try {
      using ceph::decode;
      auto iter = bl_tags.cbegin();
      decode(tags, iter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: failed to decode attrs" << id << dendl;
      return -EIO;
    }
  }

  return 0;
}

int RGWRole::read_name(const DoutPrefixProvider *dpp, optional_yield y)
{
  auto svc = ctl->svc;
  auto& pool = svc->zone->get_zone_params().roles_pool;
  string oid = tenant + get_names_oid_prefix() + name;
  bufferlist bl;
  auto obj_ctx = svc->sysobj->init_obj_ctx();

  int ret = rgw_get_system_obj(obj_ctx, pool, oid, bl, NULL, NULL, y, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed reading role name from pool: " << pool.name << ": "
                  << name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  RGWNameToId nameToId;
  try {
    using ceph::decode;
    auto iter = bl.cbegin();
    decode(nameToId, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode role name from pool: " << pool.name << ": "
                  << name << dendl;
    return -EIO;
  }
  id = nameToId.obj_id;
  return 0;
}

bool RGWRole::validate_input()
{
  if (name.length() > MAX_ROLE_NAME_LEN) {
    ldout(cct, 0) << "ERROR: Invalid name length " << dendl;
    return false;
  }

  if (path.length() > MAX_PATH_NAME_LEN) {
    ldout(cct, 0) << "ERROR: Invalid path length " << dendl;
    return false;
  }

  std::regex regex_name("[A-Za-z0-9:=,.@-]+");
  if (! std::regex_match(name, regex_name)) {
    ldout(cct, 0) << "ERROR: Invalid chars in name " << dendl;
    return false;
  }

  std::regex regex_path("(/[!-~]+/)|(/)");
  if (! std::regex_match(path,regex_path)) {
    ldout(cct, 0) << "ERROR: Invalid chars in path " << dendl;
    return false;
  }

  if (max_session_duration < SESSION_DURATION_MIN ||
          max_session_duration > SESSION_DURATION_MAX) {
    ldout(cct, 0) << "ERROR: Invalid session duration, should be between 3600 and 43200 seconds " << dendl;
    return false;
  }
  return true;
}

void RGWRole::extract_name_tenant(const std::string& str)
{
  size_t pos = str.find('$');
  if (pos != std::string::npos) {
    tenant = str.substr(0, pos);
    name = str.substr(pos + 1);
  }
}

void RGWRole::update_trust_policy(string& trust_policy)
{
  this->trust_policy = trust_policy;
}

int RGWRole::get_roles_by_path_prefix(const DoutPrefixProvider *dpp, 
                                      RGWRados *store,
                                      CephContext *cct,
                                      const string& path_prefix,
                                      const string& tenant,
                                      vector<RGWRole>& roles,
				      optional_yield y)
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
    int r = store->list_raw_objects(dpp, pool, prefix, 1000, ctx, oids, &is_truncated);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: listing filtered objects failed: " << pool.name << ": "
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

      RGWRole role(cct, store->pctl);
      role.set_id(id);
      int ret = role.read_info(dpp, y);
      if (ret < 0) {
        return ret;
      }
      roles.push_back(std::move(role));
    }
  }

  return 0;
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
