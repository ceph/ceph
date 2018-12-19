// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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

int RGWRole::store_info(bool exclusive)
{
  using ceph::encode;
  string oid = get_info_oid_prefix() + id;

  bufferlist bl;
  encode(*this, bl);
  return rgw_put_system_obj(store, store->svc.zone->get_zone_params().roles_pool, oid,
                bl, exclusive, NULL, real_time(), NULL);
}

int RGWRole::store_name(bool exclusive)
{
  RGWNameToId nameToId;
  nameToId.obj_id = id;

  string oid = tenant + get_names_oid_prefix() + name;

  bufferlist bl;
  using ceph::encode;
  encode(nameToId, bl);
  return rgw_put_system_obj(store, store->svc.zone->get_zone_params().roles_pool, oid,
              bl, exclusive, NULL, real_time(), NULL);
}

int RGWRole::store_path(bool exclusive)
{
  string oid = tenant + get_path_oid_prefix() + path + get_info_oid_prefix() + id;

  bufferlist bl;
  return rgw_put_system_obj(store, store->svc.zone->get_zone_params().roles_pool, oid,
              bl, exclusive, NULL, real_time(), NULL);
}

int RGWRole::create(bool exclusive)
{
  int ret;

  if (! validate_input()) {
    return -EINVAL;
  }

  /* check to see the name is not used */
  ret = read_id(name, tenant, id);
  if (exclusive && ret == 0) {
    ldout(cct, 0) << "ERROR: name " << name << " already in use for role id "
                    << id << dendl;
    return -EEXIST;
  } else if ( ret < 0 && ret != -ENOENT) {
    ldout(cct, 0) << "failed reading role id  " << id << ": "
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

  auto& pool = store->svc.zone->get_zone_params().roles_pool;
  ret = store_info(exclusive);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR:  storing role info in pool: " << pool.name << ": "
                  << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ret = store_name(exclusive);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: storing role name in pool: " << pool.name << ": "
                  << name << ": " << cpp_strerror(-ret) << dendl;

    //Delete the role info that was stored in the previous call
    string oid = get_info_oid_prefix() + id;
    int info_ret = rgw_delete_system_obj(store, pool, oid, NULL);
    if (info_ret < 0) {
      ldout(cct, 0) << "ERROR: cleanup of role id from pool: " << pool.name << ": "
                  << id << ": " << cpp_strerror(-info_ret) << dendl;
    }
    return ret;
  }

  ret = store_path(exclusive);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: storing role path in pool: " << pool.name << ": "
                  << path << ": " << cpp_strerror(-ret) << dendl;
    //Delete the role info that was stored in the previous call
    string oid = get_info_oid_prefix() + id;
    int info_ret = rgw_delete_system_obj(store, pool, oid, NULL);
    if (info_ret < 0) {
      ldout(cct, 0) << "ERROR: cleanup of role id from pool: " << pool.name << ": "
                  << id << ": " << cpp_strerror(-info_ret) << dendl;
    }
    //Delete role name that was stored in previous call
    oid = tenant + get_names_oid_prefix() + name;
    int name_ret = rgw_delete_system_obj(store, pool, oid, NULL);
    if (name_ret < 0) {
      ldout(cct, 0) << "ERROR: cleanup of role name from pool: " << pool.name << ": "
                  << name << ": " << cpp_strerror(-name_ret) << dendl;
    }
    return ret;
  }
  return 0;
}

int RGWRole::delete_obj()
{
  auto& pool = store->svc.zone->get_zone_params().roles_pool;

  int ret = read_name();
  if (ret < 0) {
    return ret;
  }

  ret = read_info();
  if (ret < 0) {
    return ret;
  }

  if (! perm_policy_map.empty()) {
    return -ERR_DELETE_CONFLICT;
  }

  // Delete id
  string oid = get_info_oid_prefix() + id;
  ret = rgw_delete_system_obj(store, pool, oid, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: deleting role id from pool: " << pool.name << ": "
                  << id << ": " << cpp_strerror(-ret) << dendl;
  }

  // Delete name
  oid = tenant + get_names_oid_prefix() + name;
  ret = rgw_delete_system_obj(store, pool, oid, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: deleting role name from pool: " << pool.name << ": "
                  << name << ": " << cpp_strerror(-ret) << dendl;
  }

  // Delete path
  oid = tenant + get_path_oid_prefix() + path + get_info_oid_prefix() + id;
  ret = rgw_delete_system_obj(store, pool, oid, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: deleting role path from pool: " << pool.name << ": "
                  << path << ": " << cpp_strerror(-ret) << dendl;
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
  auto& pool = store->svc.zone->get_zone_params().roles_pool;

  int ret = store_info(false);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR:  storing info in pool: " << pool.name << ": "
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
    return -EINVAL;
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
  encode_json("id", id , f);
  encode_json("name", name , f);
  encode_json("path", path, f);
  encode_json("arn", arn, f);
  encode_json("create_date", creation_date, f);
  encode_json("max_session_duration", max_session_duration, f);
  encode_json("assume_role_policy_document", trust_policy, f);
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

int RGWRole::read_id(const string& role_name, const string& tenant, string& role_id)
{
  auto& pool = store->svc.zone->get_zone_params().roles_pool;
  string oid = tenant + get_names_oid_prefix() + role_name;
  bufferlist bl;
  auto obj_ctx = store->svc.sysobj->init_obj_ctx();

  int ret = rgw_get_system_obj(store, obj_ctx, pool, oid, bl, NULL, NULL);
  if (ret < 0) {
    return ret;
  }

  RGWNameToId nameToId;
  try {
    auto iter = bl.cbegin();
    using ceph::decode;
    decode(nameToId, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode role from pool: " << pool.name << ": "
                  << role_name << dendl;
    return -EIO;
  }
  role_id = nameToId.obj_id;
  return 0;
}

int RGWRole::read_info()
{
  auto& pool = store->svc.zone->get_zone_params().roles_pool;
  string oid = get_info_oid_prefix() + id;
  bufferlist bl;
  auto obj_ctx = store->svc.sysobj->init_obj_ctx();

  int ret = rgw_get_system_obj(store, obj_ctx, pool, oid, bl, NULL, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed reading role info from pool: " << pool.name <<
                  ": " << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  try {
    using ceph::decode;
    auto iter = bl.cbegin();
    decode(*this, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode role info from pool: " << pool.name <<
                  ": " << id << dendl;
    return -EIO;
  }

  return 0;
}

int RGWRole::read_name()
{
  auto& pool = store->svc.zone->get_zone_params().roles_pool;
  string oid = tenant + get_names_oid_prefix() + name;
  bufferlist bl;
  auto obj_ctx = store->svc.sysobj->init_obj_ctx();

  int ret = rgw_get_system_obj(store, obj_ctx, pool, oid, bl, NULL, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed reading role name from pool: " << pool.name << ": "
                  << name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  RGWNameToId nameToId;
  try {
    using ceph::decode;
    auto iter = bl.cbegin();
    decode(nameToId, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode role name from pool: " << pool.name << ": "
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

      RGWRole role(cct, store);
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
