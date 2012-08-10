#include <errno.h>

#include <string>
#include <map>

#include "common/errno.h"
#include "rgw_rados.h"
#include "rgw_acl.h"

#include "include/types.h"
#include "rgw_user.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;


/**
 * Get the anonymous (ie, unauthenticated) user info.
 */
void rgw_get_anon_user(RGWUserInfo& info)
{
  info.user_id = RGW_USER_ANON_ID;
  info.display_name.clear();
  info.access_keys.clear();
}

bool rgw_user_is_authenticated(RGWUserInfo& info)
{
  return (info.user_id != RGW_USER_ANON_ID);
}

/**
 * Save the given user information to storage.
 * Returns: 0 on success, -ERR# on failure.
 */
int rgw_store_user_info(RGWRados *store, RGWUserInfo& info, bool exclusive)
{
  bufferlist bl;
  info.encode(bl);
  string md5;
  int ret;
  map<string,bufferlist> attrs;

  map<string, RGWAccessKey>::iterator iter;
  for (iter = info.swift_keys.begin(); iter != info.swift_keys.end(); ++iter) {
    RGWAccessKey& k = iter->second;
    /* check if swift mapping exists */
    RGWUserInfo inf;
    int r = rgw_get_user_info_by_swift(store, k.id, inf);
    if (r >= 0 && inf.user_id.compare(info.user_id) != 0) {
      ldout(store->ctx(), 0) << "WARNING: can't store user info, swift id already mapped to another user" << dendl;
      return -EEXIST;
    }
  }

  if (info.access_keys.size()) {
    /* check if access keys already exist */
    RGWUserInfo inf;
    map<string, RGWAccessKey>::iterator iter = info.access_keys.begin();
    for (; iter != info.access_keys.end(); ++iter) {
      RGWAccessKey& k = iter->second;
      int r = rgw_get_user_info_by_access_key(store, k.id, inf);
      if (r >= 0 && inf.user_id.compare(info.user_id) != 0) {
        ldout(store->ctx(), 0) << "WARNING: can't store user info, access key already mapped to another user" << dendl;
        return -EEXIST;
      }
    }
  }

  bufferlist uid_bl;
  RGWUID ui;
  ui.user_id = info.user_id;
  ::encode(ui, uid_bl);
  ::encode(info, uid_bl);

  ret = rgw_put_obj(store, info.user_id, store->params.user_uid_pool, info.user_id, uid_bl.c_str(), uid_bl.length(), exclusive);
  if (ret < 0)
    return ret;

  if (info.user_email.size()) {
    ret = rgw_put_obj(store, info.user_id, store->params.user_email_pool, info.user_email, uid_bl.c_str(), uid_bl.length(), exclusive);
    if (ret < 0)
      return ret;
  }

  if (info.access_keys.size()) {
    map<string, RGWAccessKey>::iterator iter = info.access_keys.begin();
    for (; iter != info.access_keys.end(); ++iter) {
      RGWAccessKey& k = iter->second;
      ret = rgw_put_obj(store, k.id, store->params.user_keys_pool, k.id, uid_bl.c_str(), uid_bl.length(), exclusive);
      if (ret < 0)
        return ret;
    }
  }

  map<string, RGWAccessKey>::iterator siter;
  for (siter = info.swift_keys.begin(); siter != info.swift_keys.end(); ++siter) {
    RGWAccessKey& k = siter->second;
    ret = rgw_put_obj(store, info.user_id, store->params.user_swift_pool, k.id, uid_bl.c_str(), uid_bl.length(), exclusive);
    if (ret < 0)
      return ret;
  }

  return ret;
}

int rgw_get_user_info_from_index(RGWRados *store, string& key, rgw_bucket& bucket, RGWUserInfo& info)
{
  bufferlist bl;
  RGWUID uid;

  int ret = rgw_get_obj(store, NULL, bucket, key, bl);
  if (ret < 0)
    return ret;

  bufferlist::iterator iter = bl.begin();
  try {
    ::decode(uid, iter);
    if (!iter.end())
      info.decode(iter);
  } catch (buffer::error& err) {
    ldout(store->ctx(), 0) << "ERROR: failed to decode user info, caught buffer::error" << dendl;
    return -EIO;
  }

  return 0;
}

/**
 * Given an email, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
int rgw_get_user_info_by_uid(RGWRados *store, string& uid, RGWUserInfo& info)
{
  return rgw_get_user_info_from_index(store, uid, store->params.user_uid_pool, info);
}

/**
 * Given an email, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
int rgw_get_user_info_by_email(RGWRados *store, string& email, RGWUserInfo& info)
{
  return rgw_get_user_info_from_index(store, email, store->params.user_email_pool, info);
}

/**
 * Given an swift username, finds the user_info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_swift(RGWRados *store, string& swift_name, RGWUserInfo& info)
{
  return rgw_get_user_info_from_index(store, swift_name, store->params.user_swift_pool, info);
}

/**
 * Given an access key, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_access_key(RGWRados *store, string& access_key, RGWUserInfo& info)
{
  return rgw_get_user_info_from_index(store, access_key, store->params.user_keys_pool, info);
}

static void get_buckets_obj(string& user_id, string& buckets_obj_id)
{
    buckets_obj_id = user_id;
    buckets_obj_id += RGW_BUCKETS_OBJ_PREFIX;
}

static int rgw_read_buckets_from_attr(RGWRados *store, string& user_id, RGWUserBuckets& buckets)
{
  bufferlist bl;
  rgw_obj obj(store->params.user_uid_pool, user_id);
  int ret = store->get_attr(NULL, obj, RGW_ATTR_BUCKETS, bl);
  if (ret)
    return ret;

  bufferlist::iterator iter = bl.begin();
  try {
    buckets.decode(iter);
  } catch (buffer::error& err) {
    ldout(store->ctx(), 0) << "ERROR: failed to decode buckets info, caught buffer::error" << dendl;
    return -EIO;
  }
  return 0;
}

/**
 * Get all the buckets owned by a user and fill up an RGWUserBuckets with them.
 * Returns: 0 on success, -ERR# on failure.
 */
int rgw_read_user_buckets(RGWRados *store, string user_id, RGWUserBuckets& buckets, bool need_stats)
{
  int ret;
  buckets.clear();
  if (store->supports_omap()) {
    string buckets_obj_id;
    get_buckets_obj(user_id, buckets_obj_id);
    bufferlist bl;
    rgw_obj obj(store->params.user_uid_pool, buckets_obj_id);
    bufferlist header;
    map<string,bufferlist> m;

    ret = store->omap_get_all(obj, header, m);
    if (ret == -ENOENT)
      ret = 0;

    if (ret < 0)
      return ret;

    for (map<string,bufferlist>::iterator q = m.begin(); q != m.end(); q++) {
      bufferlist::iterator iter = q->second.begin();
      RGWBucketEnt bucket;
      ::decode(bucket, iter);
      buckets.add(bucket);
    }
  } else {
    ret = rgw_read_buckets_from_attr(store, user_id, buckets);
    switch (ret) {
    case 0:
      break;
    case -ENODATA:
      ret = 0;
      return 0;
    default:
      return ret;
    }
  }

  list<string> buckets_list;

  if (need_stats) {
   map<string, RGWBucketEnt>& m = buckets.get_buckets();
   int r = store->update_containers_stats(m);
   if (r < 0)
     ldout(store->ctx(), 0) << "ERROR: could not get stats for buckets" << dendl;

  }
  return 0;
}

/**
 * Store the set of buckets associated with a user on a n xattr
 * not used with all backends
 * This completely overwrites any previously-stored list, so be careful!
 * Returns 0 on success, -ERR# otherwise.
 */
int rgw_write_buckets_attr(RGWRados *store, string user_id, RGWUserBuckets& buckets)
{
  bufferlist bl;
  buckets.encode(bl);

  rgw_obj obj(store->params.user_uid_pool, user_id);

  int ret = store->set_attr(NULL, obj, RGW_ATTR_BUCKETS, bl);

  return ret;
}

int rgw_add_bucket(RGWRados *store, string user_id, rgw_bucket& bucket)
{
  int ret;
   string& bucket_name = bucket.name;

  if (store->supports_omap()) {
    bufferlist bl;

    RGWBucketEnt new_bucket;
    new_bucket.bucket = bucket;
    new_bucket.size = 0;
    time(&new_bucket.mtime);
    ::encode(new_bucket, bl);

    string buckets_obj_id;
    get_buckets_obj(user_id, buckets_obj_id);

    rgw_obj obj(store->params.user_uid_pool, buckets_obj_id);
    ret = store->omap_set(obj, bucket_name, bl);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: error adding bucket to directory: "
		 << cpp_strerror(-ret)<< dendl;
    }
  } else {
    RGWUserBuckets buckets;

    ret = rgw_read_user_buckets(store, user_id, buckets, false);
    RGWBucketEnt new_bucket;

    switch (ret) {
    case 0:
    case -ENOENT:
    case -ENODATA:
      new_bucket.bucket = bucket;
      new_bucket.size = 0;
      time(&new_bucket.mtime);
      buckets.add(new_bucket);
      ret = rgw_write_buckets_attr(store, user_id, buckets);
      break;
    default:
      ldout(store->ctx(), 10) << "rgw_write_buckets_attr returned " << ret << dendl;
      break;
    }
  }

  return ret;
}

int rgw_remove_user_bucket_info(RGWRados *store, string user_id, rgw_bucket& bucket)
{
  int ret;

  if (store->supports_omap()) {
    bufferlist bl;

    string buckets_obj_id;
    get_buckets_obj(user_id, buckets_obj_id);

    rgw_obj obj(store->params.user_uid_pool, buckets_obj_id);
    ret = store->omap_del(obj, bucket.name);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: error removing bucket from directory: "
		 << cpp_strerror(-ret)<< dendl;
    }
  } else {
    RGWUserBuckets buckets;

    ret = rgw_read_user_buckets(store, user_id, buckets, false);

    if (ret == 0 || ret == -ENOENT) {
      buckets.remove(bucket.name);
      ret = rgw_write_buckets_attr(store, user_id, buckets);
    }
  }

  return ret;
}

int rgw_remove_key_index(RGWRados *store, RGWAccessKey& access_key)
{
  rgw_obj obj(store->params.user_keys_pool, access_key.id);
  int ret = store->delete_obj(NULL, obj);
  return ret;
}

int rgw_remove_uid_index(RGWRados *store, string& uid)
{
  rgw_obj obj(store->params.user_uid_pool, uid);
  int ret = store->delete_obj(NULL, obj);
  return ret;
}

int rgw_remove_email_index(RGWRados *store, string& email)
{
  rgw_obj obj(store->params.user_email_pool, email);
  int ret = store->delete_obj(NULL, obj);
  return ret;
}

int rgw_remove_swift_name_index(RGWRados *store, string& swift_name)
{
  rgw_obj obj(store->params.user_swift_pool, swift_name);
  int ret = store->delete_obj(NULL, obj);
  return ret;
}

/**
 * delete a user's presence from the RGW system.
 * First remove their bucket ACLs, then delete them
 * from the user and user email pools. This leaves the pools
 * themselves alone, as well as any ACLs embedded in object xattrs.
 */
int rgw_delete_user(RGWRados *store, RGWUserInfo& info) {
  RGWUserBuckets user_buckets;
  int ret = rgw_read_user_buckets(store, info.user_id, user_buckets, false);
  if (ret < 0)
    return ret;

  map<string, RGWBucketEnt>& buckets = user_buckets.get_buckets();
  vector<rgw_bucket> buckets_vec;
  for (map<string, RGWBucketEnt>::iterator i = buckets.begin();
       i != buckets.end();
       ++i) {
    RGWBucketEnt& bucket = i->second;
    buckets_vec.push_back(bucket.bucket);
  }
  map<string, RGWAccessKey>::iterator kiter = info.access_keys.begin();
  for (; kiter != info.access_keys.end(); ++kiter) {
    ldout(store->ctx(), 10) << "removing key index: " << kiter->first << dendl;
    ret = rgw_remove_key_index(store, kiter->second);
    if (ret < 0 && ret != -ENOENT) {
      ldout(store->ctx(), 0) << "ERROR: could not remove " << kiter->first << " (access key object), should be fixed (err=" << ret << ")" << dendl;
      return ret;
    }
  }

  map<string, RGWAccessKey>::iterator siter = info.swift_keys.begin();
  for (; siter != info.swift_keys.end(); ++siter) {
    RGWAccessKey& k = siter->second;
    ldout(store->ctx(), 10) << "removing swift subuser index: " << k.id << dendl;
    /* check if swift mapping exists */
    ret = rgw_remove_swift_name_index(store, k.id);
    if (ret < 0 && ret != -ENOENT) {
      ldout(store->ctx(), 0) << "ERROR: could not remove " << k.id << " (swift name object), should be fixed (err=" << ret << ")" << dendl;
      return ret;
    }
  }

  rgw_obj email_obj(store->params.user_email_pool, info.user_email);
  ldout(store->ctx(), 10) << "removing email index: " << info.user_email << dendl;
  ret = store->delete_obj(NULL, email_obj);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 0) << "ERROR: could not remove " << info.user_id << ":" << email_obj << ", should be fixed (err=" << ret << ")" << dendl;
    return ret;
  }

  string buckets_obj_id;
  get_buckets_obj(info.user_id, buckets_obj_id);
  rgw_obj uid_bucks(store->params.user_uid_pool, buckets_obj_id);
  ldout(store->ctx(), 10) << "removing user buckets index" << dendl;
  ret = store->delete_obj(NULL, uid_bucks);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 0) << "ERROR: could not remove " << info.user_id << ":" << uid_bucks << ", should be fixed (err=" << ret << ")" << dendl;
    return ret;
  }
  
  rgw_obj uid_obj(store->params.user_uid_pool, info.user_id);
  ldout(store->ctx(), 10) << "removing user index: " << info.user_id << dendl;
  ret = store->delete_obj(NULL, uid_obj);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 0) << "ERROR: could not remove " << info.user_id << ":" << uid_obj << ", should be fixed (err=" << ret << ")" << dendl;
    return ret;
  }

  return 0;
}
