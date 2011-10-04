#include <errno.h>

#include <string>
#include <map>

#include "common/errno.h"
#include "rgw_access.h"
#include "rgw_acl.h"

#include "include/types.h"
#include "rgw_user.h"

using namespace std;

static rgw_bucket ui_key_bucket(USER_INFO_POOL_NAME);
static rgw_bucket ui_email_bucket(USER_INFO_EMAIL_POOL_NAME);
static rgw_bucket ui_swift_bucket(USER_INFO_SWIFT_POOL_NAME);
static rgw_bucket ui_uid_bucket(USER_INFO_UID_POOL_NAME);

rgw_bucket rgw_root_bucket(RGW_ROOT_BUCKET);

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
int rgw_store_user_info(RGWUserInfo& info)
{
  bufferlist bl;
  info.encode(bl);
  string md5;
  int ret;
  map<string,bufferlist> attrs;

  if (info.swift_name.size()) {
    /* check if swift mapping exists */
    RGWUserInfo inf;
    int r = rgw_get_user_info_by_swift(info.swift_name, inf);
    if (r >= 0 && inf.user_id.compare(info.user_id) != 0) {
      RGW_LOG(0) << "can't store user info, swift id already mapped to another user" << dendl;
      return -EEXIST;
    }
  }

  if (info.access_keys.size()) {
    /* check if access keys already exist */
    RGWUserInfo inf;
    map<string, RGWAccessKey>::iterator iter = info.access_keys.begin();
    for (; iter != info.access_keys.end(); ++iter) {
      RGWAccessKey& k = iter->second;
      int r = rgw_get_user_info_by_access_key(k.id, inf);
      if (r >= 0 && inf.user_id.compare(info.user_id) != 0) {
        RGW_LOG(0) << "can't store user info, access key already mapped to another user" << dendl;
        return -EEXIST;
      }
    }
  }

  bufferlist uid_bl;
  RGWUID ui;
  ui.user_id = info.user_id;
  ::encode(ui, uid_bl);
  ::encode(info, uid_bl);

  ret = rgw_put_obj(info.user_id, ui_uid_bucket, info.user_id, uid_bl.c_str(), uid_bl.length());
  if (ret < 0)
    return ret;

  if (info.user_email.size()) {
    ret = rgw_put_obj(info.user_id, ui_email_bucket, info.user_email, uid_bl.c_str(), uid_bl.length());
    if (ret < 0)
      return ret;
  }

  if (info.access_keys.size()) {
    map<string, RGWAccessKey>::iterator iter = info.access_keys.begin();
    for (; iter != info.access_keys.end(); ++iter) {
      RGWAccessKey& k = iter->second;
      ret = rgw_put_obj(k.id, ui_key_bucket, k.id, uid_bl.c_str(), uid_bl.length());
      if (ret < 0)
        return ret;
    }
  }

  if (info.swift_name.size())
    ret = rgw_put_obj(info.user_id, ui_swift_bucket, info.swift_name, uid_bl.c_str(), uid_bl.length());

  return ret;
}

int rgw_get_user_info_from_index(string& key, rgw_bucket& bucket, RGWUserInfo& info)
{
  bufferlist bl;
  RGWUID uid;

  int ret = rgw_get_obj(bucket, key, bl);
  if (ret < 0)
    return ret;

  bufferlist::iterator iter = bl.begin();
  try {
    ::decode(uid, iter);
    if (!iter.end())
      info.decode(iter);
  } catch (buffer::error& err) {
    RGW_LOG(0) << "ERROR: failed to decode user info, caught buffer::error" << dendl;
    return -EIO;
  }

  return 0;
}

/**
 * Given an email, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
int rgw_get_user_info_by_uid(string& uid, RGWUserInfo& info)
{
  return rgw_get_user_info_from_index(uid, ui_uid_bucket, info);
}

/**
 * Given an email, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
int rgw_get_user_info_by_email(string& email, RGWUserInfo& info)
{
  return rgw_get_user_info_from_index(email, ui_email_bucket, info);
}

/**
 * Given an swift username, finds the user_info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_swift(string& swift_name, RGWUserInfo& info)
{
  return rgw_get_user_info_from_index(swift_name, ui_swift_bucket, info);
}

/**
 * Given an access key, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_access_key(string& access_key, RGWUserInfo& info)
{
  return rgw_get_user_info_from_index(access_key, ui_key_bucket, info);
}

static void get_buckets_obj(string& user_id, string& buckets_obj_id)
{
    buckets_obj_id = user_id;
    buckets_obj_id += RGW_BUCKETS_OBJ_PREFIX;
}

static int rgw_read_buckets_from_attr(string& user_id, RGWUserBuckets& buckets)
{
  bufferlist bl;
  rgw_obj obj(ui_uid_bucket, user_id);
  int ret = rgwstore->get_attr(NULL, obj, RGW_ATTR_BUCKETS, bl);
  if (ret)
    return ret;

  bufferlist::iterator iter = bl.begin();
  try {
    buckets.decode(iter);
  } catch (buffer::error& err) {
    RGW_LOG(0) << "ERROR: failed to decode buckets info, caught buffer::error" << dendl;
    return -EIO;
  }
  return 0;
}

static void store_buckets(string& user_id, RGWUserBuckets& buckets)
{
  map<string, RGWBucketEnt>& m = buckets.get_buckets();
  map<string, RGWBucketEnt>::iterator iter;
  for (iter = m.begin(); iter != m.end(); ++iter) {
    RGWBucketEnt& entry = iter->second;
    int r = rgw_add_bucket(user_id, entry.bucket);
    if (r < 0)
      RGW_LOG(0) << "failed to store bucket information for user " << user_id << " bucket=" << entry.bucket << dendl;
  }
}

/**
 * Get all the buckets owned by a user and fill up an RGWUserBuckets with them.
 * Returns: 0 on success, -ERR# on failure.
 */
int rgw_read_user_buckets(string user_id, RGWUserBuckets& buckets, bool need_stats)
{
  int ret;
  buckets.clear();
  if (rgwstore->supports_tmap()) {
    string buckets_obj_id;
    get_buckets_obj(user_id, buckets_obj_id);
    bufferlist bl;
#define LARGE_ENOUGH_LEN (4096 * 1024)
    size_t len = LARGE_ENOUGH_LEN;
    rgw_obj obj(ui_uid_bucket, buckets_obj_id);

    do {
      ret = rgwstore->read(NULL, obj, 0, len, bl);
      if (ret == -ENOENT) {
        /* try to read the old format */
        ret = rgw_read_buckets_from_attr(user_id, buckets);
        if (!ret) {
          store_buckets(user_id, buckets);
          goto done;
        }

        ret = 0;
        return 0;
      }
      if (ret < 0)
        return ret;

      if ((size_t)ret != len)
        break;

      len *= 2;
    } while (1);

    bufferlist::iterator p = bl.begin();
    bufferlist header;
    map<string,bufferlist> m;
    try {
      ::decode(header, p);
      ::decode(m, p);
      for (map<string,bufferlist>::iterator q = m.begin(); q != m.end(); q++) {
        bufferlist::iterator iter = q->second.begin();
        RGWBucketEnt bucket;
        ::decode(bucket, iter);
        buckets.add(bucket);
      }
    } catch (buffer::error& err) {
      RGW_LOG(0) << "ERROR: failed to decode bucket information, caught buffer::error" << dendl;
      return -EIO;
    }
  } else {
    ret = rgw_read_buckets_from_attr(user_id, buckets);
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

done:
  list<string> buckets_list;

  if (need_stats) {
   map<string, RGWBucketEnt>& m = buckets.get_buckets();
   int r = rgwstore->update_containers_stats(m);
   if (r < 0)
     RGW_LOG(0) << "could not get stats for buckets" << dendl;

  }
  return 0;
}

/**
 * Store the set of buckets associated with a user on a n xattr
 * not used with all backends
 * This completely overwrites any previously-stored list, so be careful!
 * Returns 0 on success, -ERR# otherwise.
 */
int rgw_write_buckets_attr(string user_id, RGWUserBuckets& buckets)
{
  bufferlist bl;
  buckets.encode(bl);

  rgw_obj obj(ui_uid_bucket, user_id);

  int ret = rgwstore->set_attr(NULL, obj, RGW_ATTR_BUCKETS, bl);

  return ret;
}

int rgw_add_bucket(string user_id, rgw_bucket& bucket)
{
  int ret;
   string& bucket_name = bucket.name;

  if (rgwstore->supports_tmap()) {
    bufferlist bl;

    RGWBucketEnt new_bucket;
    new_bucket.bucket = bucket;
    new_bucket.size = 0;
    time(&new_bucket.mtime);
    ::encode(new_bucket, bl);

    string buckets_obj_id;
    get_buckets_obj(user_id, buckets_obj_id);

    rgw_obj obj(ui_uid_bucket, buckets_obj_id);
    ret = rgwstore->tmap_create(obj, bucket_name, bl);
    if (ret < 0) {
      RGW_LOG(0) << "error adding bucket to directory: "
		 << cpp_strerror(-ret)<< dendl;
    }
  } else {
    RGWUserBuckets buckets;

    ret = rgw_read_user_buckets(user_id, buckets, false);
    RGWBucketEnt new_bucket;

    switch (ret) {
    case 0:
    case -ENOENT:
    case -ENODATA:
      new_bucket.bucket = bucket;
      new_bucket.size = 0;
      time(&new_bucket.mtime);
      buckets.add(new_bucket);
      ret = rgw_write_buckets_attr(user_id, buckets);
      break;
    default:
      RGW_LOG(10) << "rgw_write_buckets_attr returned " << ret << dendl;
      break;
    }
  }

  return ret;
}

int rgw_remove_user_bucket_info(string user_id, rgw_bucket& bucket, bool purge_data)
{
  int ret;

  if (rgwstore->supports_tmap()) {
    bufferlist bl;

    string buckets_obj_id;
    get_buckets_obj(user_id, buckets_obj_id);

    rgw_obj obj(ui_uid_bucket, buckets_obj_id);
    ret = rgwstore->tmap_del(obj, bucket.name);
    if (ret < 0) {
      RGW_LOG(0) << "error removing bucket from directory: "
		 << cpp_strerror(-ret)<< dendl;
    }
  } else {
    RGWUserBuckets buckets;

    ret = rgw_read_user_buckets(user_id, buckets, false);

    if (ret == 0 || ret == -ENOENT) {
      buckets.remove(bucket.name);
      ret = rgw_write_buckets_attr(user_id, buckets);
    }
  }

  if (ret == 0 && purge_data) {
    vector<rgw_bucket> buckets_vec;
    buckets_vec.push_back(bucket);
    ret = rgwstore->purge_buckets(user_id, buckets_vec);
  }


  return ret;
}

int rgw_remove_key_index(RGWAccessKey& access_key)
{
  rgw_obj obj(ui_key_bucket, access_key.id);
  int ret = rgwstore->delete_obj(NULL, access_key.id, obj);
  return ret;
}

int rgw_remove_uid_index(string& uid)
{
  rgw_obj obj(ui_uid_bucket, uid);
  int ret = rgwstore->delete_obj(NULL, uid, obj);
  return ret;
}

int rgw_remove_email_index(string& uid, string& email)
{
  rgw_obj obj(ui_email_bucket, email);
  int ret = rgwstore->delete_obj(NULL, uid, obj);
  return ret;
}

int rgw_remove_swift_name_index(string& uid, string& swift_name)
{
  rgw_obj obj(ui_swift_bucket, swift_name);
  int ret = rgwstore->delete_obj(NULL, uid, obj);
  return ret;
}

/**
 * delete a user's presence from the RGW system.
 * First remove their bucket ACLs, then delete them
 * from the user and user email pools. This leaves the pools
 * themselves alone, as well as any ACLs embedded in object xattrs.
 */
int rgw_delete_user(RGWUserInfo& info, bool purge_data) {
  RGWUserBuckets user_buckets;
  int ret = rgw_read_user_buckets(info.user_id, user_buckets, false);
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
    RGW_LOG(0) << "removing key index: " << kiter->first << dendl;
    ret = rgw_remove_key_index(kiter->second);
    if (ret < 0 && ret != -ENOENT) {
      RGW_LOG(0) << "ERROR: could not remove " << kiter->first << " (access key object), should be fixed (err=" << ret << ")" << dendl;
      return ret;
    }
  }

  rgw_obj email_obj(ui_email_bucket, info.user_email);
  RGW_LOG(0) << "removing email index: " << info.user_email << dendl;
  ret = rgwstore->delete_obj(NULL, info.user_id, email_obj);
  if (ret < 0 && ret != -ENOENT) {
    RGW_LOG(0) << "ERROR: could not remove " << info.user_id << ":" << email_obj << ", should be fixed (err=" << ret << ")" << dendl;
    return ret;
  }

  if (purge_data) {
    RGW_LOG(0) << "purging user buckets" << dendl;
    ret = rgwstore->purge_buckets(info.user_id, buckets_vec);
    if (ret < 0 && ret != -ENOENT) {
      RGW_LOG(0) << "ERROR: delete_buckets returned " << ret << dendl;
      return ret;
    }
  }

  string buckets_obj_id;
  get_buckets_obj(info.user_id, buckets_obj_id);
  rgw_obj uid_bucks(ui_uid_bucket, buckets_obj_id);
  RGW_LOG(0) << "removing user buckets index" << dendl;
  ret = rgwstore->delete_obj(NULL, info.user_id, uid_bucks);
  if (ret < 0 && ret != -ENOENT) {
    RGW_LOG(0) << "ERROR: could not remove " << info.user_id << ":" << uid_bucks << ", should be fixed (err=" << ret << ")" << dendl;
    return ret;
  }
  
  rgw_obj uid_obj(ui_uid_bucket, info.user_id);
  RGW_LOG(0) << "removing user index: " << info.user_id << dendl;
  ret = rgwstore->delete_obj(NULL, info.user_id, uid_obj);
  if (ret < 0 && ret != -ENOENT) {
    RGW_LOG(0) << "ERROR: could not remove " << info.user_id << ":" << uid_obj << ", should be fixed (err=" << ret << ")" << dendl;
    return ret;
  }

  return 0;
}
