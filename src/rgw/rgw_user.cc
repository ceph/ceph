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
  void *handle;
  off_t ofs = 0, end = -1;
  bufferlist::iterator iter;

  ret = rgwstore->prepare_get_obj(ui_bucket, user_id, ofs, &end, NULL, NULL, NULL, NULL, NULL, true, &handle, &err);
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

  ret = rgwstore->put_obj(info.user_id, ui_bucket, info.user_id, data, bl.length(), NULL, attrs);

  if (ret == -ENOENT) {
    ret = rgwstore->create_bucket(info.user_id, ui_bucket, attrs);
    if (ret >= 0)
      ret = rgwstore->put_obj(info.user_id, ui_bucket, info.user_id, data, bl.length(), NULL, attrs);
  }

  if (ret < 0)
    return ret;

  if (!info.user_email.size())
    return ret;

  RGWUID ui;
  ui.user_id = info.user_id;
  bufferlist uid_bl;
  ui.encode(uid_bl);
  ret = rgwstore->put_obj(info.user_id, ui_email_bucket, info.user_email, uid_bl.c_str(), uid_bl.length(), NULL, attrs);
  if (ret == -ENOENT) {
    map<string, bufferlist> attrs;
    ret = rgwstore->create_bucket(info.user_id, ui_email_bucket, attrs);
    if (ret >= 0)
      ret = rgwstore->put_obj(info.user_id, ui_email_bucket, info.user_email, uid_bl.c_str(), uid_bl.length(), NULL, attrs);
  }

  return ret;
}

/**
 * Given an email, finds the user_id associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
int rgw_get_uid_by_email(string& email, string& user_id)
{
  bufferlist bl;
  int ret;
  char *data;
  struct rgw_err err;
  RGWUID uid;
  void *handle;
  off_t ofs = 0, end = -1;
  bufferlist::iterator iter;

  ret = rgwstore->prepare_get_obj(ui_email_bucket, email, ofs, &end, NULL, NULL,
                                  NULL, NULL, NULL, true, &handle, &err);
  if (ret < 0)
    return ret;
  do {
    ret = rgwstore->get_obj(&handle, ui_email_bucket, email, &data, ofs, end);
    if (ret < 0)
      goto done;
    ofs += ret;
    bl.append(data, ret);
    free(data);
  } while (ofs <= end);

  iter = bl.begin();
  uid.decode(iter); 
  user_id = uid.user_id;
  ret = 0;
done:
  rgwstore->finish_get_obj(&handle);
  return ret;
}

/**
 * Get all the buckets owned by a user and fill up an RGWUserBuckets with them.
 * Returns: 0 on success, -ERR# on failure.
 */
int rgw_get_user_buckets(string user_id, RGWUserBuckets& buckets)
{
  bufferlist bl;
  int ret = rgwstore->get_attr(ui_bucket, user_id, RGW_ATTR_BUCKETS, bl);
  switch (ret) {
  case 0:
    break;
  case -ENODATA:
    return 0;
  default:
    return ret;
  }

  bufferlist::iterator iter = bl.begin();
  buckets.decode(iter);

  return 0;
}

/**
 * Store the set of buckets associated with a user.
 * This completely overwrites any previously-stored list, so be careful!
 * Returns 0 on success, -ERR# otherwise.
 */
int rgw_put_user_buckets(string user_id, RGWUserBuckets& buckets)
{
  bufferlist bl;
  buckets.encode(bl);
  int ret = rgwstore->set_attr(ui_bucket, user_id, RGW_ATTR_BUCKETS, bl);

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
  rgw_get_user_buckets(info.user_id, user_buckets);
  map<string, RGWObjEnt>& buckets = user_buckets.get_buckets();
  for (map<string, RGWObjEnt>::iterator i = buckets.begin();
       i != buckets.end();
       ++i) {
    string bucket_name = i->first;
    rgwstore->delete_obj(info.user_id, root_bucket, bucket_name);
  }
  rgwstore->delete_obj(info.user_id, ui_bucket, info.user_id);
  rgwstore->delete_obj(info.user_id, ui_email_bucket, info.user_email);
  return 0;
}
