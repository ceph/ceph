#include <errno.h>
#include <ctime>
#include <regex>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"
#include "rgw_rados.h"

#include "include/types.h"
#include "rgw_string.h"

#include "rgw_common.h"
#include "rgw_tools.h"
#include "rgw_group.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

const string RGWGroup::group_name_oid_prefix = "group_names.";
const string RGWGroup::group_oid_prefix = "group.";
const string RGWGroup::group_path_oid_prefix = "group_paths.";
const string RGWGroup::group_arn_prefix = "arn:aws:iam::";

int RGWGroup::store_info(bool exclusive)
{
  string oid = get_info_oid_prefix() + id;

  bufferlist bl;
  ::encode(*this, bl);
  return rgw_put_system_obj(store, store->get_zone_params().groups_pool, oid,
                bl.c_str(), bl.length(), exclusive, NULL, real_time(), NULL);
}

int RGWGroup::store_name(bool exclusive)
{
  RGWNameToId nameToId;
  nameToId.obj_id = id;

  string oid = tenant + get_names_oid_prefix() + name;

  bufferlist bl;
  ::encode(nameToId, bl);
  return rgw_put_system_obj(store, store->get_zone_params().groups_pool, oid,
              bl.c_str(), bl.length(), exclusive, NULL, real_time(), NULL);
}

int RGWGroup::store_path(bool exclusive)
{
  string oid = tenant + get_path_oid_prefix() + path + get_info_oid_prefix() + id;

  return rgw_put_system_obj(store, store->get_zone_params().groups_pool, oid,
              NULL, 0, exclusive, NULL, real_time(), NULL);
}

int RGWGroup::create(bool exclusive)
{
  int ret;

  if (! validate_input()) {
    return -EINVAL;
  }

  /* check to see the name is not used */
  ret = read_id(name, tenant, id);
  if (exclusive && ret == 0) {
    ldout(cct, 0) << "ERROR: name " << name << " already in use for group id "
                    << id << dendl;
    return -EEXIST;
  } else if ( ret < 0 && ret != -ENOENT) {
    ldout(cct, 0) << "failed reading group id  " << id << ": "
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
  arn = group_arn_prefix + tenant + ":group" + path + name;

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

  auto& pool = store->get_zone_params().groups_pool;
  ret = store_info(exclusive);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR:  storing group info in pool: " << pool.name << ": "
                  << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ret = store_name(exclusive);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: storing group name in pool: " << pool.name << ": "
                  << name << ": " << cpp_strerror(-ret) << dendl;

    //Delete the role info that was stored in the previous call
    string oid = get_info_oid_prefix() + id;
    int info_ret = rgw_delete_system_obj(store, pool, oid, NULL);
    if (info_ret < 0) {
      ldout(cct, 0) << "ERROR: cleanup of group id from pool: " << pool.name << ": "
                  << id << ": " << cpp_strerror(-info_ret) << dendl;
    }
    return ret;
  }

  ret = store_path(exclusive);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: storing group path in pool: " << pool.name << ": "
                  << path << ": " << cpp_strerror(-ret) << dendl;
    //Delete the group info that was stored in the previous call
    string oid = get_info_oid_prefix() + id;
    int info_ret = rgw_delete_system_obj(store, pool, oid, NULL);
    if (info_ret < 0) {
      ldout(cct, 0) << "ERROR: cleanup of group id from pool: " << pool.name << ": "
                  << id << ": " << cpp_strerror(-info_ret) << dendl;
    }
    //Delete group name that was stored in previous call
    oid = tenant + get_names_oid_prefix() + name;
    int name_ret = rgw_delete_system_obj(store, pool, oid, NULL);
    if (name_ret < 0) {
      ldout(cct, 0) << "ERROR: cleanup of group name from pool: " << pool.name << ": "
                  << name << ": " << cpp_strerror(-name_ret) << dendl;
    }
    return ret;
  }
  return 0;
}

int RGWGroup::delete_obj()
{
  auto& pool = store->get_zone_params().groups_pool;

  int ret = read_name();
  if (ret < 0) {
    return ret;
  }

  ret = read_info();
  if (ret < 0) {
    return ret;
  }
  // Delete id
  string oid = get_info_oid_prefix() + id;
  ret = rgw_delete_system_obj(store, pool, oid, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: deleting group id from pool: " << pool.name << ": "
                  << id << ": " << cpp_strerror(-ret) << dendl;
  }

  // Delete name
  oid = tenant + get_names_oid_prefix() + name;
  ret = rgw_delete_system_obj(store, pool, oid, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: deleting group name from pool: " << pool.name << ": "
                  << name << ": " << cpp_strerror(-ret) << dendl;
  }

  // Delete path
  oid = tenant + get_path_oid_prefix() + path + get_info_oid_prefix() + id;
  ret = rgw_delete_system_obj(store, pool, oid, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: deleting group path from pool: " << pool.name << ": "
                  << path << ": " << cpp_strerror(-ret) << dendl;
  }
  return ret;
}

int RGWGroup::get()
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

int RGWGroup::get_by_id()
{
  int ret = read_info();
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWGroup::update()
{
  auto& pool = store->get_zone_params().groups_pool;

  int ret = store_info(false);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR:  storing info in pool: " << pool.name << ": "
                  << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return 0;
}

void RGWGroup::dump(Formatter *f) const
{
  encode_json("id", id , f);
  encode_json("name", name , f);
  encode_json("path", path, f);
  encode_json("arn", arn, f);
  encode_json("create_date", creation_date, f);
  //encode_json("assume_role_policy_document", trust_policy, f);
}

void RGWGroup::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("path", path, obj);
  JSONDecoder::decode_json("arn", arn, obj);
  JSONDecoder::decode_json("create_date", creation_date, obj);
  //JSONDecoder::decode_json("assume_role_policy_document", trust_policy, obj);
}

int RGWGroup::read_id(const string& group_name, const string& tenant, string& group_id)
{
  auto& pool = store->get_zone_params().groups_pool;
  string oid = tenant + get_names_oid_prefix() + group_name;
  bufferlist bl;
  RGWObjectCtx obj_ctx(store);

  int ret = rgw_get_system_obj(store, obj_ctx, pool, oid, bl, NULL, NULL);
  if (ret < 0) {
    return ret;
  }

  RGWNameToId nameToId;
  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(nameToId, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode group from pool: " << pool.name << ": "
                  << group_name << dendl;
    return -EIO;
  }
  group_id = nameToId.obj_id;
  return 0;
}

int RGWGroup::read_info()
{
  auto& pool = store->get_zone_params().groups_pool;
  string oid = get_info_oid_prefix() + id;
  bufferlist bl;
  RGWObjectCtx obj_ctx(store);

  int ret = rgw_get_system_obj(store, obj_ctx, pool, oid, bl, NULL, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed reading group info from pool: " << pool.name <<
                  ": " << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(*this, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode group info from pool: " << pool.name <<
                  ": " << id << dendl;
    return -EIO;
  }

  return 0;
}

int RGWGroup::read_name()
{
  auto& pool = store->get_zone_params().groups_pool;
  string oid = tenant + get_names_oid_prefix() + name;
  bufferlist bl;
  RGWObjectCtx obj_ctx(store);

  int ret = rgw_get_system_obj(store, obj_ctx, pool, oid, bl, NULL, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed reading group name from pool: " << pool.name << ": "
                  << name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  RGWNameToId nameToId;
  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(nameToId, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode group name from pool: " << pool.name << ": "
                  << name << dendl;
    return -EIO;
  }
  id = nameToId.obj_id;
  return 0;
}

bool RGWGroup::validate_input()
{
  if (name.length() > MAX_GROUP_NAME_LEN) {
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

  return true;
}

void RGWGroup::extract_name_tenant(const std::string& str)
{
  size_t pos = str.find('$');
  if (pos != std::string::npos) {
    tenant = str.substr(0, pos);
    name = str.substr(pos + 1);
  }
}

int RGWGroup::get_groups_by_path_prefix(RGWRados *store,
                                        CephContext *cct,
                                        const string& path_prefix,
                                        const string& tenant,
                                        vector<RGWGroup>& groups)
{
  auto pool = store->get_zone_params().groups_pool;
  string prefix;

  // List all roles if path prefix is empty
  if (! path_prefix.empty()) {
    prefix = tenant + group_path_oid_prefix + path_prefix;
  } else {
    prefix = tenant + group_path_oid_prefix;
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
      result.push_back(iter.substr(group_path_oid_prefix.size()));
    }
  } while (is_truncated);

  for (const auto& it : result) {
    //Find the group oid prefix from the end
    size_t pos = it.rfind(group_oid_prefix);
    if (pos == string::npos) {
        continue;
    }
    // Split the result into path and info_oid + id
    string path = it.substr(0, pos);

    /*Make sure that prefix is part of path (False results could've been returned)
      because of the role info oid + id appended to the path)*/
    if(path_prefix.empty() || path.find(path_prefix) != string::npos) {
      //Get id from info oid prefix + id
      string id = it.substr(pos + group_oid_prefix.length());

      RGWGroup group(cct, store);
      group.set_id(id);
      int ret = group.read_info();
      if (ret < 0) {
        return ret;
      }
      groups.push_back(std::move(group));
    }
  }

  return 0;
}

const string& RGWGroup::get_names_oid_prefix()
{
  return group_name_oid_prefix;
}

const string& RGWGroup::get_info_oid_prefix()
{
  return group_oid_prefix;
}

const string& RGWGroup::get_path_oid_prefix()
{
  return group_path_oid_prefix;
}
