#include <errno.h>
#include <ctime>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"
#include "rgw_rados.h"

#include "include/types.h"
#include "rgw_string.h"

#include "rgw_common.h"
#include "rgw_tools.h"
#include "rgw_role.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

const string RGWRole::role_name_oid_prefix = "role_names.";
const string RGWRole::role_oid_prefix = "roles.";

int RGWRole::store_info(bool exclusive)
{
  string oid = get_info_oid_prefix() + id;

  bufferlist bl;
  ::encode(*this, bl);
  return rgw_put_system_obj(store, store->get_zone_params().roles_pool, oid,
                bl.c_str(), bl.length(), exclusive, NULL, real_time(), NULL);
}

int RGWRole::store_name(bool exclusive)
{
  RGWNameToId nameToId;
  nameToId.obj_id = id;

  string oid = get_names_oid_prefix() + name;

  bufferlist bl;
  ::encode(nameToId, bl);
  return rgw_put_system_obj(store, store->get_zone_params().roles_pool, oid,
              bl.c_str(), bl.length(), exclusive, NULL, real_time(), NULL);
}

int RGWRole::create(bool exclusive)
{
  int ret;

  /* check to see the name is not used */
  ret = read_id(name, id);
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
  arn = "arn:aws:iam::role" + path + name;

  // Creation time
  real_clock::time_point t = real_clock::now();

  struct timeval tv;
  real_clock::to_timeval(t, tv);

  char buf[30];
  strftime(buf,30,"%Y-%m-%dT%H:%M:%S", std::gmtime(&tv.tv_sec));
  sprintf(buf + strlen(buf),".%dZ",(int)tv.tv_usec);
  creation_date.assign(buf, strlen(buf));

  auto& pool = store->get_zone_params().roles_pool;
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

  return 0;
}

int RGWRole::delete_obj()
{
  auto& pool = store->get_zone_params().roles_pool;

  int ret = read_name();
  if (ret < 0) {
    return ret;
  }

  // Delete id
  string oid = get_info_oid_prefix() + id;
  ret = rgw_delete_system_obj(store, pool, oid, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: deleting role id from pool: " << pool.name << ": "
                  << id << ": " << cpp_strerror(-ret) << dendl;
  }

  // Delete name
  oid = get_names_oid_prefix() + name;
  ret = rgw_delete_system_obj(store, pool, oid, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: deleting role name from pool: " << pool.name << ": "
                  << name << ": " << cpp_strerror(-ret) << dendl;
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

void RGWRole::dump(Formatter *f) const
{
  encode_json("id", id , f);
  encode_json("name", name , f);
  encode_json("path", path, f);
  encode_json("arn", arn, f);
  encode_json("create_date", creation_date, f);
  encode_json("assume_role_policy_document", trust_policy, f);
}

void RGWRole::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("path", path, obj);
  JSONDecoder::decode_json("arn", arn, obj);
  JSONDecoder::decode_json("create_date", creation_date, obj);
  JSONDecoder::decode_json("assume_role_policy_document", trust_policy, obj);
}

int RGWRole::read_id(const string& role_name, string& role_id)
{
  auto& pool = store->get_zone_params().roles_pool;
  string oid = get_names_oid_prefix() + role_name;
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
    ldout(cct, 0) << "ERROR: failed to decode role from pool: " << pool.name << ": "
                  << role_name << dendl;
    return -EIO;
  }
  role_id = nameToId.obj_id;
  return 0;
}

int RGWRole::read_info()
{
  auto& pool = store->get_zone_params().roles_pool;
  string oid = get_info_oid_prefix() + id;
  bufferlist bl;
  RGWObjectCtx obj_ctx(store);

  int ret = rgw_get_system_obj(store, obj_ctx, pool, oid, bl, NULL, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed reading role info from pool: " << pool.name <<
                  ": " << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(*this, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode role info from pool: " << pool.name <<
                  ": " << id << dendl;
    return -EIO;
  }

  return 0;
}

int RGWRole::read_name()
{
  auto& pool = store->get_zone_params().roles_pool;
  string oid = get_names_oid_prefix() + name;
  bufferlist bl;
  RGWObjectCtx obj_ctx(store);

  int ret = rgw_get_system_obj(store, obj_ctx, pool, oid, bl, NULL, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed reading role name from pool: " << pool.name << ": "
                  << name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  RGWNameToId nameToId;
  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(nameToId, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode role name from pool: " << pool.name << ": "
                  << name << dendl;
    return -EIO;
  }
  id = nameToId.obj_id;
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
