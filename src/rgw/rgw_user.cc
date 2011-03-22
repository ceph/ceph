#include <errno.h>

#include <string>
#include <map>

#include "rgw_access.h"
#include "rgw_acl.h"

#include "include/types.h"
#include "rgw_user.h"

using namespace std;

static string ui_bucket = USER_INFO_BUCKET_NAME;
static string ui_email_bucket = USER_INFO_EMAIL_BUCKET_NAME;
static string ui_openstack_bucket = USER_INFO_OPENSTACK_BUCKET_NAME;
static string root_bucket = ".rgw"; //keep this synced to rgw_rados.cc::ROOT_BUCKET!

/**
 * Get the info for a user out of storage.
 * Returns: 0 on success, -ERR# on failure
 */
int rgw_get_user_info(string user_id, RGWUserInfo& info)
{
  bufferlist bl;
  int ret;
  char *data;
  struct rgw_err err;
  void *handle = NULL;
  off_t ofs = 0, end = -1;
  size_t total_len;
  time_t lastmod;
  bufferlist::iterator iter;

  ret = rgwstore->prepare_get_obj(ui_bucket, user_id, ofs, &end, NULL, NULL, NULL, &lastmod, NULL, NULL, &total_len, &handle, &err);
  if (ret < 0)
    return ret;
  do {
    ret = rgwstore->get_obj(&handle, ui_bucket, user_id, &data, ofs, end);
    if (ret < 0) {
      goto done;
    }
    bl.append(data, ret);
    free(data);
    ofs += ret;
  } while (ofs <= end);

  iter = bl.begin();
  info.decode(iter); 
  ret = 0;
done:
  rgwstore->finish_get_obj(&handle);
  return ret;
}

/**
 * Get the anonymous (ie, unauthenticated) user info.
 */
void rgw_get_anon_user(RGWUserInfo& info)
{
  info.user_id = RGW_USER_ANON_ID;
  info.display_name.clear();
  info.secret_key.clear();
}

/**
 * Save the given user information to storage.
 * Returns: 0 on success, -ERR# on failure.
 */
int rgw_store_user_info(RGWUserInfo& info)
{
  bufferlist bl;
  info.encode(bl);
  const char *data = bl.c_str();
  string md5;
  int ret;
  map<string,bufferlist> attrs;

  if (info.openstack_name.size()) {
    /* check if openstack mapping exists */
    string os_uid;
    RGWUserInfo inf;
    int r = rgw_get_uid_by_openstack(info.openstack_name, os_uid, inf);
    if (r >= 0 && os_uid.compare(info.user_id) != 0) {
      RGW_LOG(0) << "can't store user info, openstack id already mapped to another user" << std::endl;
      return -EEXIST;
    }
  }

  ret = rgwstore->put_obj(info.user_id, ui_bucket, info.user_id, data, bl.length(), NULL, attrs);

  if (ret == -ENOENT) {
    ret = rgwstore->create_bucket(info.user_id, ui_bucket, attrs);
    if (ret >= 0)
      ret = rgwstore->put_obj(info.user_id, ui_bucket, info.user_id, data, bl.length(), NULL, attrs);
  }

  if (ret < 0)
    return ret;

  bufferlist uid_bl;
  RGWUID ui;
  ui.user_id = info.user_id;
  ::encode(ui, uid_bl);
  ::encode(info, uid_bl);

  if (info.user_email.size()) {
    ret = rgwstore->put_obj(info.user_id, ui_email_bucket, info.user_email, uid_bl.c_str(), uid_bl.length(), NULL, attrs);
    if (ret == -ENOENT) {
      map<string, bufferlist> attrs;
      ret = rgwstore->create_bucket(info.user_id, ui_email_bucket, attrs);
      if (ret >= 0)
        ret = rgwstore->put_obj(info.user_id, ui_email_bucket, info.user_email, uid_bl.c_str(), uid_bl.length(), NULL, attrs);
    }
  }

  if (ret < 0)
    return ret;

  if (info.openstack_name.size()) {
    ret = rgwstore->put_obj(info.user_id, ui_openstack_bucket, info.openstack_name, uid_bl.c_str(), uid_bl.length(), NULL, attrs);
    if (ret == -ENOENT) {
      map<string, bufferlist> attrs;
      ret = rgwstore->create_bucket(info.user_id, ui_openstack_bucket, attrs);
      if (ret >= 0 && ret != -EEXIST)
        ret = rgwstore->put_obj(info.user_id, ui_openstack_bucket, info.openstack_name, uid_bl.c_str(), uid_bl.length(), NULL, attrs);
    }
  }

  return ret;
}

int rgw_get_uid_from_index(string& key, string& bucket, string& user_id, RGWUserInfo& info)
{
  bufferlist bl;
  int ret;
  char *data = NULL;
  struct rgw_err err;
  RGWUID uid;
  void *handle = NULL;
  off_t ofs = 0, end = -1;
  bufferlist::iterator iter;
  size_t total_len;
  ret = rgwstore->prepare_get_obj(bucket, key, ofs, &end, NULL, NULL,
                                  NULL, NULL, NULL, NULL, &total_len, &handle, &err);
  if (ret < 0)
    return ret;

  if (total_len == 0)
    return -EACCES;

  do {
    ret = rgwstore->get_obj(&handle, bucket, key, &data, ofs, end);
    if (ret < 0)
      goto done;
    ofs += ret;
    bl.append(data, ret);
    free(data);
  } while (ofs <= end);

  iter = bl.begin();
  ::decode(uid, iter);
  user_id = uid.user_id;
  if (!iter.end()) {
    info.decode(iter);
  }
  ret = 0;
done:
  rgwstore->finish_get_obj(&handle);
  return ret;
}

/**
 * Given an email, finds the user_id associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
int rgw_get_uid_by_email(string& email, string& user_id, RGWUserInfo& info)
{
  return rgw_get_uid_from_index(email, ui_email_bucket, user_id, info);
}

/**
 * Given an openstack username, finds the user_id associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_uid_by_openstack(string& openstack_name, string& user_id, RGWUserInfo& info)
{
  return rgw_get_uid_from_index(openstack_name, ui_openstack_bucket, user_id, info);
}

static void get_buckets_obj(string& user_id, string& buckets_obj_id)
{
    buckets_obj_id = user_id;
    buckets_obj_id += RGW_BUCKETS_OBJ_PREFIX;
}

static int rgw_read_buckets_from_attr(string& user_id, RGWUserBuckets& buckets)
{
  bufferlist bl;
  int ret = rgwstore->get_attr(ui_bucket, user_id, RGW_ATTR_BUCKETS, bl);
  if (ret)
    return ret;

  bufferlist::iterator iter = bl.begin();
  buckets.decode(iter);
  return 0;
}

static void store_buckets(string& user_id, RGWUserBuckets& buckets)
{
  map<string, RGWBucketEnt>& m = buckets.get_buckets();
  map<string, RGWBucketEnt>::iterator iter;
  for (iter = m.begin(); iter != m.end(); ++iter) {
    string bucket_name = iter->first;
    int r = rgw_add_bucket(user_id, bucket_name);
    if (r < 0)
      RGW_LOG(0) << "failed to store bucket information for user " << user_id << " bucket=" << bucket_name << std::endl;
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

    do {
      ret = rgwstore->read(ui_bucket, buckets_obj_id, 0, len, bl);
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
    ::decode(header, p);
    ::decode(m, p);
    for (map<string,bufferlist>::iterator q = m.begin(); q != m.end(); q++) {
      bufferlist::iterator iter = q->second.begin();
      RGWBucketEnt bucket;
      ::decode(bucket, iter);
      buckets.add(bucket);
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
     RGW_LOG(0) << "could not get stats for buckets" << std::endl;

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

  int ret = rgwstore->set_attr(ui_bucket, user_id, RGW_ATTR_BUCKETS, bl);

  return ret;
}

int rgw_add_bucket(string user_id, string bucket_name)
{
  int ret;

  if (rgwstore->supports_tmap()) {
    bufferlist bl;

    RGWBucketEnt new_bucket;
    new_bucket.name = bucket_name;
    new_bucket.size = 0;
    time(&new_bucket.mtime);
    ::encode(new_bucket, bl);

    string buckets_obj_id;
    get_buckets_obj(user_id, buckets_obj_id);

    ret = rgwstore->tmap_set(ui_bucket, buckets_obj_id, bucket_name, bl);
    if (ret < 0) {
      RGW_LOG(0) << "error adding bucket to directory: " << strerror(-ret)<< std::endl;
    }
  } else {
    RGWUserBuckets buckets;

    ret = rgw_read_user_buckets(user_id, buckets, false);
    RGWBucketEnt new_bucket;

    switch (ret) {
    case 0:
    case -ENOENT:
    case -ENODATA:
      new_bucket.name = bucket_name;
      new_bucket.size = 0;
      time(&new_bucket.mtime);
      buckets.add(new_bucket);
      ret = rgw_write_buckets_attr(user_id, buckets);
      break;
    default:
      RGW_LOG(10) << "rgw_write_buckets_attr returned " << ret << endl;
      break;
    }
  }

  return ret;
}

int rgw_remove_bucket(string user_id, string bucket_name)
{
  int ret;

  if (rgwstore->supports_tmap()) {
    bufferlist bl;

    string buckets_obj_id;
    get_buckets_obj(user_id, buckets_obj_id);

    ret = rgwstore->tmap_del(ui_bucket, buckets_obj_id, bucket_name);
    if (ret < 0) {
      RGW_LOG(0) << "error removing bucket from directory: " << strerror(-ret)<< std::endl;
    }
  } else {
    RGWUserBuckets buckets;

    ret = rgw_read_user_buckets(user_id, buckets, false);

    if (ret == 0 || ret == -ENOENT) {
      buckets.remove(bucket_name);
      ret = rgw_write_buckets_attr(user_id, buckets);
    }
  }

  return ret;
}

/**
 * delete a user's presence from the RGW system.
 * First remove their bucket ACLs, then delete them
 * from the user and user email pools. This leaves the pools
 * themselves alone, as well as any ACLs embedded in object xattrs.
 */
int rgw_delete_user(RGWUserInfo& info) {
  RGWUserBuckets user_buckets;
  rgw_read_user_buckets(info.user_id, user_buckets, false);
  map<string, RGWBucketEnt>& buckets = user_buckets.get_buckets();
  for (map<string, RGWBucketEnt>::iterator i = buckets.begin();
       i != buckets.end();
       ++i) {
    string bucket_name = i->first;
    rgwstore->delete_obj(info.user_id, root_bucket, bucket_name);
  }
  rgwstore->delete_obj(info.user_id, ui_bucket, info.user_id);
  rgwstore->delete_obj(info.user_id, ui_email_bucket, info.user_email);
  return 0;
}
