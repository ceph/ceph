#include <errno.h>

#include <boost/date_time/posix_time/posix_time.hpp>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "rgw_rados.h"

#include "include/types.h"
#include "rgw_string.h"

#include "rgw_common.h"
#include "rgw_tools.h"
#include "rgw_role.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

int RGWRole::store_info(bool exclusive)
{
  bufferlist bl;
  ::encode(*this, bl);
  return rgw_put_system_obj(store, store->get_zone_params().roles_pool, id,
                bl.c_str(), bl.length(), exclusive, NULL, real_time(), NULL);
}

int RGWRole::store_name(bool exclusive)
{
  RGWNameToId nameToId;
  nameToId.obj_id = id;

  bufferlist bl;
  ::encode(nameToId, bl);
  return rgw_put_system_obj(store, store->get_zone_params().roles_pool, name,
              bl.c_str(), bl.length(), exclusive, NULL, real_time(), NULL);
}

int RGWRole::create(bool exclusive)
{
  int ret;

  /* check to see the name is not used */
  ret = read_id(name, id);
  if (exclusive && ret == 0) {
    ldout(cct, 10) << "ERROR: name " << name << " already in use for role id "
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

  // Creation time
  creation_date = boost::posix_time::to_iso_extended_string(
                          boost::posix_time::microsec_clock::universal_time());

  string pool_name = store->get_zone_params().roles_pool.name;
  ret = store_info(exclusive);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR:  storing info for in pool: " << pool_name << ": "
                  << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return store_name(exclusive);
}

int RGWRole::delete_obj()
{
  string pool_name = store->get_zone_params().roles_pool.name;

  int ret = read_name();
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: finding role name in pool: " << pool_name << ": "
                  << name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  // Delete id
  ret = rgw_delete_system_obj(store, store->get_zone_params().roles_pool, id, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: deleting role id from pool: " << pool_name << ": "
                  << id << ": " << cpp_strerror(-ret) << dendl;
  }

  // Delete name
  ret = rgw_delete_system_obj(store, store->get_zone_params().roles_pool, name, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: deleting role name from pool: " << pool_name << ": "
                  << name << ": " << cpp_strerror(-ret) << dendl;
  }

  return ret;
}

int RGWRole::list()
{
  string pool_name = store->get_zone_params().roles_pool.name;

  int ret = read_name();
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: finding role name in pool: " << pool_name << ": "
                  << name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ret = read_info();
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: finding role id in pool: " << pool_name << ": "
                  << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return 0;
}

void RGWRole::dump(Formatter *f) const
{
  encode_json("id", id , f);
  encode_json("name", name , f);
  encode_json("path", path, f);
  encode_json("create_date", creation_date, f);
}

void RGWRole::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("path", path, obj);
  JSONDecoder::decode_json("create_date", creation_date, obj);
}

int RGWRole::read_id(const string& role_name, string& role_id)
{
  bufferlist bl;

  RGWObjectCtx obj_ctx(store);
  int ret = rgw_get_system_obj(store, obj_ctx, store->get_zone_params().roles_pool,
                                  role_name, bl, NULL, NULL);
  if (ret < 0) {
    return ret;
  }

  string pool_name = store->get_zone_params().roles_pool.name;
  RGWNameToId nameToId;
  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(nameToId, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode role from pool: " << pool_name << ": "
                  << role_name << dendl;
    return -EIO;
  }
  role_id = nameToId.obj_id;
  return 0;
}

int RGWRole::read_info()
{
  string pool_name = store->get_zone_params().roles_pool.name;

  bufferlist bl;
  RGWObjectCtx obj_ctx(store);
  int ret = rgw_get_system_obj(store, obj_ctx, store->get_zone_params().roles_pool,
                                id, bl, NULL, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed reading role info from pool: " << pool_name <<
                  ": " << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(*this, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode role info from pool: " << pool_name <<
                  ": " << id << dendl;
    return -EIO;
  }

  return 0;
}

int RGWRole::read_name()
{
  string pool_name = store->get_zone_params().roles_pool.name;

  bufferlist bl;
  RGWObjectCtx obj_ctx(store);
  int ret = rgw_get_system_obj(store, obj_ctx, store->get_zone_params().roles_pool,
                                name, bl, NULL, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed reading role name from pool: " << pool_name << ": "
                  << name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  RGWNameToId nameToId;
  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(nameToId, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode role name from pool: " << pool_name << ": "
                  << name << dendl;
    return -EIO;
  }
  id = nameToId.obj_id;
  return 0;
}

