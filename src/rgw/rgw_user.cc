// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include <string>
#include <map>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "common/RWLock.h"
#include "rgw_rados.h"
#include "rgw_acl.h"

#include "include/types.h"
#include "rgw_user.h"
#include "rgw_string.h"

// until everything is moved from rgw_common
#include "rgw_common.h"

#include "rgw_bucket.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;


static RGWMetadataHandler *user_meta_handler = NULL;


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

int rgw_user_sync_all_stats(RGWRados *store, const string& user_id)
{
  CephContext *cct = store->ctx();
  size_t max_entries = cct->_conf->rgw_list_buckets_max_chunk;
  bool done;
  string marker;
  int ret;

  do {
    RGWUserBuckets user_buckets;
    ret = rgw_read_user_buckets(store, user_id, user_buckets, marker, max_entries, false);
    if (ret < 0) {
      ldout(cct, 0) << "failed to read user buckets: ret=" << ret << dendl;
      return ret;
    }
    map<string, RGWBucketEnt>& buckets = user_buckets.get_buckets();
    for (map<string, RGWBucketEnt>::iterator i = buckets.begin();
         i != buckets.end();
         ++i) {
      marker = i->first;

      RGWBucketEnt& bucket_ent = i->second;
      ret = rgw_bucket_sync_user_stats(store, user_id, bucket_ent.bucket);
      if (ret < 0) {
        ldout(cct, 0) << "ERROR: could not sync bucket stats: ret=" << ret << dendl;
        return ret;
      }
    }
    done = (buckets.size() < max_entries);
  } while (!done);

  ret = store->complete_sync_user_stats(user_id);
  if (ret < 0) {
    cerr << "ERROR: failed to complete syncing user stats: ret=" << ret << std::endl;
    return ret;
  }

  return 0;
}

/**
 * Save the given user information to storage.
 * Returns: 0 on success, -ERR# on failure.
 */
int rgw_store_user_info(RGWRados *store, RGWUserInfo& info, RGWUserInfo *old_info,
                        RGWObjVersionTracker *objv_tracker, time_t mtime, bool exclusive)
{
  bufferlist bl;
  info.encode(bl);
  int ret;
  RGWObjVersionTracker ot;

  if (objv_tracker) {
    ot = *objv_tracker;
  }

  if (ot.write_version.tag.empty()) {
    if (ot.read_version.tag.empty()) {
      ot.generate_new_write_ver(store->ctx());
    } else {
      ot.write_version = ot.read_version;
      ot.write_version.ver++;
    }
  }

  map<string, RGWAccessKey>::iterator iter;
  for (iter = info.swift_keys.begin(); iter != info.swift_keys.end(); ++iter) {
    if (old_info && old_info->swift_keys.count(iter->first) != 0)
      continue;
    RGWAccessKey& k = iter->second;
    /* check if swift mapping exists */
    RGWUserInfo inf;
    int r = rgw_get_user_info_by_swift(store, k.id, inf);
    if (r >= 0 && inf.user_id.compare(info.user_id) != 0) {
      ldout(store->ctx(), 0) << "WARNING: can't store user info, swift id (" << k.id
        << ") already mapped to another user (" << info.user_id << ")" << dendl;
      return -EEXIST;
    }
  }

  if (!info.access_keys.empty()) {
    /* check if access keys already exist */
    RGWUserInfo inf;
    map<string, RGWAccessKey>::iterator iter = info.access_keys.begin();
    for (; iter != info.access_keys.end(); ++iter) {
      RGWAccessKey& k = iter->second;
      if (old_info && old_info->access_keys.count(iter->first) != 0)
        continue;
      int r = rgw_get_user_info_by_access_key(store, k.id, inf);
      if (r >= 0 && inf.user_id.compare(info.user_id) != 0) {
        ldout(store->ctx(), 0) << "WARNING: can't store user info, access key already mapped to another user" << dendl;
        return -EEXIST;
      }
    }
  }

  RGWUID ui;
  ui.user_id = info.user_id;

  bufferlist link_bl;
  ::encode(ui, link_bl);

  bufferlist data_bl;
  ::encode(ui, data_bl);
  ::encode(info, data_bl);

  ret = store->meta_mgr->put_entry(user_meta_handler, info.user_id, data_bl, exclusive, &ot, mtime);
  if (ret < 0)
    return ret;

  if (!info.user_email.empty()) {
    if (!old_info ||
        old_info->user_email.compare(info.user_email) != 0) { /* only if new index changed */
      ret = rgw_put_system_obj(store, store->zone.user_email_pool, info.user_email,
                               link_bl.c_str(), link_bl.length(), exclusive, NULL, 0);
      if (ret < 0)
        return ret;
    }
  }

  if (!info.access_keys.empty()) {
    map<string, RGWAccessKey>::iterator iter = info.access_keys.begin();
    for (; iter != info.access_keys.end(); ++iter) {
      RGWAccessKey& k = iter->second;
      if (old_info && old_info->access_keys.count(iter->first) != 0)
	continue;

      ret = rgw_put_system_obj(store, store->zone.user_keys_pool, k.id,
                               link_bl.c_str(), link_bl.length(), exclusive,
                               NULL, 0);
      if (ret < 0)
        return ret;
    }
  }

  map<string, RGWAccessKey>::iterator siter;
  for (siter = info.swift_keys.begin(); siter != info.swift_keys.end(); ++siter) {
    RGWAccessKey& k = siter->second;
    if (old_info && old_info->swift_keys.count(siter->first) != 0)
      continue;

    ret = rgw_put_system_obj(store, store->zone.user_swift_pool, k.id,
                             link_bl.c_str(), link_bl.length(), exclusive,
                             NULL, 0);
    if (ret < 0)
      return ret;
  }

  return ret;
}

struct user_info_entry {
  RGWUserInfo info;
  RGWObjVersionTracker objv_tracker;
  time_t mtime;
};

static RGWChainedCacheImpl<user_info_entry> uinfo_cache;

int rgw_get_user_info_from_index(RGWRados *store, string& key, rgw_bucket& bucket, RGWUserInfo& info,
                                 RGWObjVersionTracker *objv_tracker, time_t *pmtime)
{
  user_info_entry e;
  if (uinfo_cache.find(key, &e)) {
    info = e.info;
    if (objv_tracker)
      *objv_tracker = e.objv_tracker;
    if (pmtime)
      *pmtime = e.mtime;
    return 0;
  }

  bufferlist bl;
  RGWUID uid;
  RGWObjectCtx obj_ctx(store);

  int ret = rgw_get_system_obj(store, obj_ctx, bucket, key, bl, NULL, &e.mtime);
  if (ret < 0)
    return ret;

  rgw_cache_entry_info cache_info;

  bufferlist::iterator iter = bl.begin();
  try {
    ::decode(uid, iter);
    int ret = rgw_get_user_info_by_uid(store, uid.user_id, e.info, &e.objv_tracker, NULL, &cache_info);
    if (ret < 0) {
      return ret;
    }
  } catch (buffer::error& err) {
    ldout(store->ctx(), 0) << "ERROR: failed to decode user info, caught buffer::error" << dendl;
    return -EIO;
  }

  list<rgw_cache_entry_info *> cache_info_entries;
  cache_info_entries.push_back(&cache_info);

  uinfo_cache.put(store, key, &e, cache_info_entries);

  info = e.info;
  if (objv_tracker)
    *objv_tracker = e.objv_tracker;
  if (pmtime)
    *pmtime = e.mtime;

  return 0;
}

/**
 * Given a uid, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
int rgw_get_user_info_by_uid(RGWRados *store, string& uid, RGWUserInfo& info,
                             RGWObjVersionTracker *objv_tracker, time_t *pmtime,
                             rgw_cache_entry_info *cache_info)
{
  bufferlist bl;
  RGWUID user_id;

  RGWObjectCtx obj_ctx(store);
  int ret = rgw_get_system_obj(store, obj_ctx, store->zone.user_uid_pool, uid, bl, objv_tracker, pmtime, NULL, cache_info);
  if (ret < 0)
    return ret;

  bufferlist::iterator iter = bl.begin();
  try {
    ::decode(user_id, iter);
    if (user_id.user_id.compare(uid) != 0) {
      lderr(store->ctx())  << "ERROR: rgw_get_user_info_by_uid(): user id mismatch: " << user_id.user_id << " != " << uid << dendl;
      return -EIO;
    }
    if (!iter.end()) {
      ::decode(info, iter);
    }
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
int rgw_get_user_info_by_email(RGWRados *store, string& email, RGWUserInfo& info,
                               RGWObjVersionTracker *objv_tracker, time_t *pmtime)
{
  return rgw_get_user_info_from_index(store, email, store->zone.user_email_pool, info, objv_tracker, pmtime);
}

/**
 * Given an swift username, finds the user_info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_swift(RGWRados *store, string& swift_name, RGWUserInfo& info,
                                      RGWObjVersionTracker *objv_tracker, time_t *pmtime)
{
  return rgw_get_user_info_from_index(store, swift_name, store->zone.user_swift_pool, info, objv_tracker, pmtime);
}

/**
 * Given an access key, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_access_key(RGWRados *store, string& access_key, RGWUserInfo& info,
                                           RGWObjVersionTracker *objv_tracker, time_t *pmtime)
{
  return rgw_get_user_info_from_index(store, access_key, store->zone.user_keys_pool, info, objv_tracker, pmtime);
}

int rgw_remove_key_index(RGWRados *store, RGWAccessKey& access_key)
{
  rgw_obj obj(store->zone.user_keys_pool, access_key.id);
  int ret = store->delete_system_obj(obj);
  return ret;
}

int rgw_remove_uid_index(RGWRados *store, string& uid)
{
  RGWObjVersionTracker objv_tracker;
  RGWUserInfo info;
  int ret = rgw_get_user_info_by_uid(store, uid, info, &objv_tracker, NULL);
  if (ret < 0)
    return ret;

  ret = store->meta_mgr->remove_entry(user_meta_handler, uid, &objv_tracker);
  if (ret < 0)
    return ret;

  return 0;
}

int rgw_remove_email_index(RGWRados *store, string& email)
{
  rgw_obj obj(store->zone.user_email_pool, email);
  int ret = store->delete_system_obj(obj);
  return ret;
}

int rgw_remove_swift_name_index(RGWRados *store, string& swift_name)
{
  rgw_obj obj(store->zone.user_swift_pool, swift_name);
  int ret = store->delete_system_obj(obj);
  return ret;
}

/**
 * delete a user's presence from the RGW system.
 * First remove their bucket ACLs, then delete them
 * from the user and user email pools. This leaves the pools
 * themselves alone, as well as any ACLs embedded in object xattrs.
 */
int rgw_delete_user(RGWRados *store, RGWUserInfo& info, RGWObjVersionTracker& objv_tracker) {
  string marker;
  vector<rgw_bucket> buckets_vec;

  bool done;
  int ret;
  CephContext *cct = store->ctx();
  size_t max_buckets = cct->_conf->rgw_list_buckets_max_chunk;

  do {
    RGWUserBuckets user_buckets;
    ret = rgw_read_user_buckets(store, info.user_id, user_buckets, marker, max_buckets, false);
    if (ret < 0)
      return ret;

    map<string, RGWBucketEnt>& buckets = user_buckets.get_buckets();
    for (map<string, RGWBucketEnt>::iterator i = buckets.begin();
        i != buckets.end();
        ++i) {
      RGWBucketEnt& bucket = i->second;
      buckets_vec.push_back(bucket.bucket);

      marker = i->first;
    }

    done = (buckets.size() < max_buckets);
  } while (!done);

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

  rgw_obj email_obj(store->zone.user_email_pool, info.user_email);
  ldout(store->ctx(), 10) << "removing email index: " << info.user_email << dendl;
  ret = store->delete_system_obj(email_obj);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 0) << "ERROR: could not remove " << info.user_id << ":" << email_obj << ", should be fixed (err=" << ret << ")" << dendl;
    return ret;
  }

  string buckets_obj_id;
  rgw_get_buckets_obj(info.user_id, buckets_obj_id);
  rgw_obj uid_bucks(store->zone.user_uid_pool, buckets_obj_id);
  ldout(store->ctx(), 10) << "removing user buckets index" << dendl;
  ret = store->delete_system_obj(uid_bucks);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 0) << "ERROR: could not remove " << info.user_id << ":" << uid_bucks << ", should be fixed (err=" << ret << ")" << dendl;
    return ret;
  }
  
  rgw_obj uid_obj(store->zone.user_uid_pool, info.user_id);
  ldout(store->ctx(), 10) << "removing user index: " << info.user_id << dendl;
  ret = store->meta_mgr->remove_entry(user_meta_handler, info.user_id, &objv_tracker);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 0) << "ERROR: could not remove " << info.user_id << ":" << uid_obj << ", should be fixed (err=" << ret << ")" << dendl;
    return ret;
  }

  return 0;
}

static bool char_is_unreserved_url(char c)
{
  if (isalnum(c))
    return true;

  switch (c) {
  case '-':
  case '.':
  case '_':
  case '~':
    return true;
  default:
    return false;
  }
}

struct rgw_flags_desc {
  uint32_t mask;
  const char *str;
};

static struct rgw_flags_desc rgw_perms[] = {
 { RGW_PERM_FULL_CONTROL, "full-control" },
 { RGW_PERM_READ | RGW_PERM_WRITE, "read-write" },
 { RGW_PERM_READ, "read" },
 { RGW_PERM_WRITE, "write" },
 { RGW_PERM_READ_ACP, "read-acp" },
 { RGW_PERM_WRITE_ACP, "read-acp" },
 { 0, NULL }
};

void rgw_perm_to_str(uint32_t mask, char *buf, int len)
{
  const char *sep = "";
  int pos = 0;
  if (!mask) {
    snprintf(buf, len, "<none>");
    return;
  }
  while (mask) {
    uint32_t orig_mask = mask;
    for (int i = 0; rgw_perms[i].mask; i++) {
      struct rgw_flags_desc *desc = &rgw_perms[i];
      if ((mask & desc->mask) == desc->mask) {
        pos += snprintf(buf + pos, len - pos, "%s%s", sep, desc->str);
        if (pos == len)
          return;
        sep = ", ";
        mask &= ~desc->mask;
        if (!mask)
          return;
      }
    }
    if (mask == orig_mask) // no change
      break;
  }
}

uint32_t rgw_str_to_perm(const char *str)
{
  if (strcasecmp(str, "read") == 0)
    return RGW_PERM_READ;
  else if (strcasecmp(str, "write") == 0)
    return RGW_PERM_WRITE;
  else if (strcasecmp(str, "readwrite") == 0)
    return RGW_PERM_READ | RGW_PERM_WRITE;
  else if (strcasecmp(str, "full") == 0)
    return RGW_PERM_FULL_CONTROL;

  return 0; // better to return no permission
}

static bool validate_access_key(string& key)
{
  const char *p = key.c_str();
  while (*p) {
    if (!char_is_unreserved_url(*p))
      return false;
    p++;
  }
  return true;
}

static void set_err_msg(std::string *sink, std::string msg)
{
  if (sink && !msg.empty())
    *sink = msg;
}

static bool remove_old_indexes(RGWRados *store,
         RGWUserInfo& old_info, RGWUserInfo& new_info, std::string *err_msg)
{
  int ret;
  bool success = true;

  if (!old_info.user_id.empty() && old_info.user_id.compare(new_info.user_id) != 0) {
    ret = rgw_remove_uid_index(store, old_info.user_id);
    if (ret < 0 && ret != -ENOENT) {
      set_err_msg(err_msg, "ERROR: could not remove index for uid " + old_info.user_id);
      success = false;
    }
  }

  if (!old_info.user_email.empty() &&
      old_info.user_email.compare(new_info.user_email) != 0) {
    ret = rgw_remove_email_index(store, old_info.user_email);
  if (ret < 0 && ret != -ENOENT) {
      set_err_msg(err_msg, "ERROR: could not remove index for email " + old_info.user_email);
      success = false;
    }
  }

  map<string, RGWAccessKey>::iterator old_iter;
  for (old_iter = old_info.swift_keys.begin(); old_iter != old_info.swift_keys.end(); ++old_iter) {
    RGWAccessKey& swift_key = old_iter->second;
    map<string, RGWAccessKey>::iterator new_iter = new_info.swift_keys.find(swift_key.id);
    if (new_iter == new_info.swift_keys.end()) {
      ret = rgw_remove_swift_name_index(store, swift_key.id);
      if (ret < 0 && ret != -ENOENT) {
        set_err_msg(err_msg, "ERROR: could not remove index for swift_name " + swift_key.id);
        success = false;
      }
    }
  }

  return success;
}

/*
 * Dump either the full user info or a subset to a formatter.
 *
 * NOTE: It is the caller's respnsibility to ensure that the
 * formatter is flushed at the correct time.
 */

static void dump_subusers_info(Formatter *f, RGWUserInfo &info)
{
  map<string, RGWSubUser>::iterator uiter;

  f->open_array_section("subusers");
  for (uiter = info.subusers.begin(); uiter != info.subusers.end(); ++uiter) {
    RGWSubUser& u = uiter->second;
    f->open_object_section("user");
    f->dump_format("id", "%s:%s", info.user_id.c_str(), u.name.c_str());
    char buf[256];
    rgw_perm_to_str(u.perm_mask, buf, sizeof(buf));
    f->dump_string("permissions", buf);
    f->close_section();
  }
  f->close_section();
}

static void dump_access_keys_info(Formatter *f, RGWUserInfo &info)
{
  map<string, RGWAccessKey>::iterator kiter;
  f->open_array_section("keys");
  for (kiter = info.access_keys.begin(); kiter != info.access_keys.end(); ++kiter) {
    RGWAccessKey& k = kiter->second;
    const char *sep = (k.subuser.empty() ? "" : ":");
    const char *subuser = (k.subuser.empty() ? "" : k.subuser.c_str());
    f->open_object_section("key");
    f->dump_format("user", "%s%s%s", info.user_id.c_str(), sep, subuser);
    f->dump_string("access_key", k.id);
    f->dump_string("secret_key", k.key);
    f->close_section();
  }
  f->close_section();
}

static void dump_swift_keys_info(Formatter *f, RGWUserInfo &info)
{
  map<string, RGWAccessKey>::iterator kiter;
  f->open_array_section("swift_keys");
  for (kiter = info.swift_keys.begin(); kiter != info.swift_keys.end(); ++kiter) {
    RGWAccessKey& k = kiter->second;
    const char *sep = (k.subuser.empty() ? "" : ":");
    const char *subuser = (k.subuser.empty() ? "" : k.subuser.c_str());
    f->open_object_section("key");
    f->dump_format("user", "%s%s%s", info.user_id.c_str(), sep, subuser);
    f->dump_string("secret_key", k.key);
    f->close_section();
  }
  f->close_section();
}

static void dump_user_info(Formatter *f, RGWUserInfo &info,
                           RGWStorageStats *stats = NULL)
{
  f->open_object_section("user_info");

  f->dump_string("user_id", info.user_id);
  f->dump_string("display_name", info.display_name);
  f->dump_string("email", info.user_email);
  f->dump_int("suspended", (int)info.suspended);
  f->dump_int("max_buckets", (int)info.max_buckets);

  dump_subusers_info(f, info);
  dump_access_keys_info(f, info);
  dump_swift_keys_info(f, info);
  info.caps.dump(f);
  if (stats) {
    encode_json("stats", *stats, f);
  }

  f->close_section();
}


RGWAccessKeyPool::RGWAccessKeyPool(RGWUser* usr)
{
  user = usr;
  swift_keys = NULL;
  access_keys = NULL;

  if (!user) {
    keys_allowed = false;
    store = NULL;
    return;
  }

  keys_allowed = true;

  store = user->get_store();
}

RGWAccessKeyPool::~RGWAccessKeyPool()
{

}

int RGWAccessKeyPool::init(RGWUserAdminOpState& op_state)
{
  if (!op_state.is_initialized()) {
    keys_allowed = false;
    return -EINVAL;
  }

  std::string uid = op_state.get_user_id();
  if (uid.compare(RGW_USER_ANON_ID) == 0) {
    keys_allowed = false;
    return -EACCES;
  }

  swift_keys = op_state.get_swift_keys();
  access_keys = op_state.get_access_keys();

  keys_allowed = true;

  return 0;
}

/*
 * Do a fairly exhaustive search for an existing key matching the parameters
 * given. Also handles the case where no key type was specified and updates
 * the operation state if needed.
 */

bool RGWAccessKeyPool::check_existing_key(RGWUserAdminOpState& op_state)
{
  bool existing_key = false;

  int key_type = op_state.get_key_type();
  std::string kid = op_state.get_access_key();
  std::map<std::string, RGWAccessKey>::iterator kiter;
  std::string swift_kid = op_state.build_default_swift_kid();

  RGWUserInfo dup_info;

  if (kid.empty() && swift_kid.empty())
    return false;

  switch (key_type) {
  case KEY_TYPE_SWIFT:
    kiter = swift_keys->find(swift_kid);

    existing_key = (kiter != swift_keys->end());
    if (existing_key)
      op_state.set_access_key(swift_kid);

    break;
  case KEY_TYPE_S3:
    kiter = access_keys->find(kid);
    existing_key = (kiter != access_keys->end());

    break;
  default:
    kiter = access_keys->find(kid);

    existing_key = (kiter != access_keys->end());
    if (existing_key) {
      op_state.set_key_type(KEY_TYPE_S3);
      break;
    }

    kiter = swift_keys->find(kid);

    existing_key = (kiter != swift_keys->end());
    if (existing_key) {
      op_state.set_key_type(KEY_TYPE_SWIFT);
      break;
    }

    // handle the case where the access key was not provided in user:key format
    if (swift_kid.empty())
      return false;

    kiter = swift_keys->find(swift_kid);

    existing_key = (kiter != swift_keys->end());
    if (existing_key) {
      op_state.set_access_key(swift_kid);
      op_state.set_key_type(KEY_TYPE_SWIFT);
    }
  }

  op_state.set_existing_key(existing_key);

  return existing_key;
}

int RGWAccessKeyPool::check_op(RGWUserAdminOpState& op_state,
     std::string *err_msg)
{
  RGWUserInfo dup_info;

  if (!op_state.is_populated()) {
    set_err_msg(err_msg, "user info was not populated");
    return -EINVAL;
  }

  if (!keys_allowed) {
    set_err_msg(err_msg, "keys not allowed for this user");
    return -EACCES;
  }

  int32_t key_type = op_state.get_key_type();

  // if a key type wasn't specified set it to s3
  if (key_type < 0)
    key_type = KEY_TYPE_S3;

  op_state.set_key_type(key_type);

  /* see if the access key was specified */
  if (key_type == KEY_TYPE_S3 && !op_state.will_gen_access() && 
      op_state.get_access_key().empty()) {
    set_err_msg(err_msg, "empty access key");
    return -EINVAL;
  }

  // don't check for secret key because we may be doing a removal

  check_existing_key(op_state);

  return 0;
}

// Generate a new random key
int RGWAccessKeyPool::generate_key(RGWUserAdminOpState& op_state, std::string *err_msg)
{
  std::string id;
  std::string key;

  std::pair<std::string, RGWAccessKey> key_pair;
  RGWAccessKey new_key;
  RGWUserInfo duplicate_check;

  int ret = 0;
  int key_type = op_state.get_key_type();
  bool gen_access = op_state.will_gen_access();
  bool gen_secret = op_state.will_gen_secret();

  if (!keys_allowed) {
    set_err_msg(err_msg, "access keys not allowed for this user");
    return -EACCES;
  }

  if (op_state.has_existing_key()) {
    set_err_msg(err_msg, "cannot create existing key");
    return -EEXIST;
  }

  if (!gen_access) {
    id = op_state.get_access_key();
  }

  if (!id.empty()) {
    switch (key_type) {
    case KEY_TYPE_SWIFT:
      if (rgw_get_user_info_by_swift(store, id, duplicate_check) >= 0) {
        set_err_msg(err_msg, "existing swift key in RGW system:" + id);
        return -EEXIST;
      }
      break;
    case KEY_TYPE_S3:
      if (rgw_get_user_info_by_access_key(store, id, duplicate_check) >= 0) {
        set_err_msg(err_msg, "existing S3 key in RGW system:" + id);
        return -EEXIST;
      }
    }
  }

  if (op_state.has_subuser())
    new_key.subuser = op_state.get_subuser();

  if (!gen_secret) {
    key = op_state.get_secret_key();
  } else if (gen_secret) {
    char secret_key_buf[SECRET_KEY_LEN + 1];

    ret = gen_rand_alphanumeric_plain(g_ceph_context, secret_key_buf, sizeof(secret_key_buf));
    if (ret < 0) {
      set_err_msg(err_msg, "unable to generate secret key");
      return ret;
    }

    key = secret_key_buf;
  }

  // Generate the access key
  if (key_type == KEY_TYPE_S3 && gen_access) {
    char public_id_buf[PUBLIC_ID_LEN + 1];

    do {
      int id_buf_size = sizeof(public_id_buf);
      ret = gen_rand_alphanumeric_upper(g_ceph_context,
               public_id_buf, id_buf_size);

      if (ret < 0) {
        set_err_msg(err_msg, "unable to generate access key");
        return ret;
      }

      id = public_id_buf;
      if (!validate_access_key(id))
        continue;

    } while (!rgw_get_user_info_by_access_key(store, id, duplicate_check));
  }

  if (key_type == KEY_TYPE_SWIFT) {
    id = op_state.build_default_swift_kid();
    if (id.empty()) {
      set_err_msg(err_msg, "empty swift access key");
      return -EINVAL;
    }

    // check that the access key doesn't exist
    if (rgw_get_user_info_by_swift(store, id, duplicate_check) >= 0) {
      set_err_msg(err_msg, "cannot create existing swift key");
      return -EEXIST;
    }
  }

  // finally create the new key
  new_key.id = id;
  new_key.key = key;

  key_pair.first = id;
  key_pair.second = new_key;

  if (key_type == KEY_TYPE_S3) {
    access_keys->insert(key_pair);
  } else if (key_type == KEY_TYPE_SWIFT) {
    swift_keys->insert(key_pair);
  }

  return 0;
}

// modify an existing key
int RGWAccessKeyPool::modify_key(RGWUserAdminOpState& op_state, std::string *err_msg)
{
  std::string id;
  std::string key = op_state.get_secret_key();
  int key_type = op_state.get_key_type();

  RGWAccessKey modify_key;

  pair<string, RGWAccessKey> key_pair;
  map<std::string, RGWAccessKey>::iterator kiter;

  switch (key_type) {
  case KEY_TYPE_S3:
    id = op_state.get_access_key();
    if (id.empty()) {
      set_err_msg(err_msg, "no access key specified");
      return -EINVAL;
    }
    break;
  case KEY_TYPE_SWIFT:
    id = op_state.build_default_swift_kid();
    if (id.empty()) {
      set_err_msg(err_msg, "no subuser specified");
      return -EINVAL;
    }
    break;
  default:
    set_err_msg(err_msg, "invalid key type");
    return -EINVAL;
  }

  if (!op_state.has_existing_key()) {
    set_err_msg(err_msg, "key does not exist");
    return -EINVAL;
  }

  key_pair.first = id;

  if (key_type == KEY_TYPE_SWIFT) {
    modify_key.id = id;
    modify_key.subuser = op_state.get_subuser();
  } else if (key_type == KEY_TYPE_S3) {
    kiter = access_keys->find(id);
    if (kiter != access_keys->end()) {
      modify_key = kiter->second;
    }
  }

  if (op_state.will_gen_secret()) {
    char secret_key_buf[SECRET_KEY_LEN + 1];

    int ret;
    int key_buf_size = sizeof(secret_key_buf);
    ret = gen_rand_alphanumeric_plain(g_ceph_context, secret_key_buf, key_buf_size);
    if (ret < 0) {
      set_err_msg(err_msg, "unable to generate secret key");
      return ret;
    }

    key = secret_key_buf;
  }

  if (key.empty()) {
      set_err_msg(err_msg, "empty secret key");
      return  -EINVAL;
  }

  // update the access key with the new secret key
  modify_key.key = key;

  key_pair.second = modify_key;


  if (key_type == KEY_TYPE_S3) {
    (*access_keys)[id] = modify_key;
  } else if (key_type == KEY_TYPE_SWIFT) {
    (*swift_keys)[id] = modify_key;
  }

  return 0;
}

int RGWAccessKeyPool::execute_add(RGWUserAdminOpState& op_state,
         std::string *err_msg, bool defer_user_update)
{
  int ret = 0;

  std::string subprocess_msg;
  int key_op = GENERATE_KEY;

  // set the op
  if (op_state.has_existing_key())
    key_op = MODIFY_KEY;

  switch (key_op) {
  case GENERATE_KEY:
    ret = generate_key(op_state, &subprocess_msg);
    break;
  case MODIFY_KEY:
    ret = modify_key(op_state, &subprocess_msg);
    break;
  }

  if (ret < 0) {
    set_err_msg(err_msg, subprocess_msg);
    return ret;
  }

  // store the updated info
  if (!defer_user_update)
    ret = user->update(op_state, err_msg);

  if (ret < 0)
    return ret;

  return 0;
}

int RGWAccessKeyPool::add(RGWUserAdminOpState& op_state, std::string *err_msg)
{
  return add(op_state, err_msg, false);
}

int RGWAccessKeyPool::add(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_user_update)
{
  int ret; 
  std::string subprocess_msg;

  ret = check_op(op_state, &subprocess_msg);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to parse request, " + subprocess_msg);
    return ret;
  }

  ret = execute_add(op_state, &subprocess_msg, defer_user_update);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to add access key, " + subprocess_msg);
    return ret;
  }

  return 0;
}

int RGWAccessKeyPool::execute_remove(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_user_update)
{
  int ret = 0;

  int key_type = op_state.get_key_type();
  std::string id = op_state.get_access_key();
  map<std::string, RGWAccessKey>::iterator kiter;
  map<std::string, RGWAccessKey> *keys_map;

  if (!op_state.has_existing_key()) {
    set_err_msg(err_msg, "unable to find access key");
    return -EINVAL;
  }

  if (key_type == KEY_TYPE_S3) {
    keys_map = access_keys;
  } else if (key_type == KEY_TYPE_SWIFT) {
    keys_map = swift_keys;
  } else {
    keys_map = NULL;
    set_err_msg(err_msg, "invalid access key");
    return -EINVAL;
  }

  kiter = keys_map->find(id);
  if (kiter == keys_map->end()) {
    set_err_msg(err_msg, "key not found");
    return -EINVAL;
  }

  rgw_remove_key_index(store, kiter->second);
  keys_map->erase(kiter);

  if (!defer_user_update)
    ret = user->update(op_state, err_msg);

  if (ret < 0)
    return ret;

  return 0;
}

int RGWAccessKeyPool::remove(RGWUserAdminOpState& op_state, std::string *err_msg)
{
  return remove(op_state, err_msg, false);
}

int RGWAccessKeyPool::remove(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_user_update)
{
  int ret;

  std::string subprocess_msg;

  ret = check_op(op_state, &subprocess_msg);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to parse request, " + subprocess_msg);
    return ret;
  }

  ret = execute_remove(op_state, &subprocess_msg, defer_user_update);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to remove access key, " + subprocess_msg);
    return ret;
  }

  return 0;
}

RGWSubUserPool::RGWSubUserPool(RGWUser *usr)
{
  subusers_allowed = (usr != NULL);
  if (usr)
    store = usr->get_store();
  else
    store = NULL;
  user = usr;
  subuser_map = NULL;
}

RGWSubUserPool::~RGWSubUserPool()
{

}

int RGWSubUserPool::init(RGWUserAdminOpState& op_state)
{
  if (!op_state.is_initialized()) {
    subusers_allowed = false;
    return -EINVAL;
  }

  std::string uid = op_state.get_user_id();
  if (uid.compare(RGW_USER_ANON_ID) == 0) {
    subusers_allowed = false;
    return -EACCES;
  }

  subuser_map = op_state.get_subusers();
  if (subuser_map == NULL) {
    subusers_allowed = false;
    return -EINVAL;
  }

  subusers_allowed = true;

  return 0;
}

bool RGWSubUserPool::exists(std::string subuser)
{
  if (subuser.empty())
    return false;

  if (!subuser_map)
    return false;

  if (subuser_map->count(subuser))
    return true;

  return false;
}

int RGWSubUserPool::check_op(RGWUserAdminOpState& op_state,
        std::string *err_msg)
{
  bool existing = false;
  std::string subuser = op_state.get_subuser();

  if (!op_state.is_populated()) {
    set_err_msg(err_msg, "user info was not populated");
    return -EINVAL;
  }

  if (!subusers_allowed) {
    set_err_msg(err_msg, "subusers not allowed for this user");
    return -EACCES;
  }

  if (subuser.empty() && !op_state.will_gen_subuser()) {
    set_err_msg(err_msg, "empty subuser name");
    return -EINVAL;
  }

  // check if the subuser exists
  if (!subuser.empty())
    existing = exists(subuser);

  op_state.set_existing_subuser(existing);

  return 0;
}

int RGWSubUserPool::execute_add(RGWUserAdminOpState& op_state,
        std::string *err_msg, bool defer_user_update)
{
  int ret = 0;
  std::string subprocess_msg;

  RGWSubUser subuser;
  std::pair<std::string, RGWSubUser> subuser_pair;
  std::string subuser_str = op_state.get_subuser();

  subuser_pair.first = subuser_str;

  // assumes key should be created
  if (op_state.has_key_op()) {
    ret = user->keys.add(op_state, &subprocess_msg, true);
    if (ret < 0) {
      set_err_msg(err_msg, "unable to create subuser key, " + subprocess_msg);
      return ret;
    }
  }

  // create the subuser
  subuser.name = subuser_str;

  if (op_state.has_subuser_perm())
    subuser.perm_mask = op_state.get_subuser_perm();

  // insert the subuser into user info
  subuser_pair.second = subuser;
  subuser_map->insert(subuser_pair);

  // attempt to save the subuser
  if (!defer_user_update)
    ret = user->update(op_state, err_msg);

  if (ret < 0)
    return ret;

  return 0;
}

int RGWSubUserPool::add(RGWUserAdminOpState& op_state, std::string *err_msg)
{
  return add(op_state, err_msg, false);
}

int RGWSubUserPool::add(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_user_update)
{
  std::string subprocess_msg;
  int ret;

  ret = check_op(op_state, &subprocess_msg);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to parse request, " + subprocess_msg);
    return ret;
  }

  if (op_state.get_secret_key().empty()) {
    op_state.set_gen_access();
  }

  ret = execute_add(op_state, &subprocess_msg, defer_user_update);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to create subuser, " + subprocess_msg);
    return ret;
  }

  return 0;
}

int RGWSubUserPool::execute_remove(RGWUserAdminOpState& op_state,
        std::string *err_msg, bool defer_user_update)
{
  int ret = 0;
  std::string subprocess_msg;

  std::string subuser_str = op_state.get_subuser();

  map<std::string, RGWSubUser>::iterator siter;
  siter = subuser_map->find(subuser_str);

  if (!op_state.has_existing_subuser()) {
    set_err_msg(err_msg, "subuser not found: " + subuser_str);
    return -EINVAL;
  }

  if (op_state.will_purge_keys()) {
    // error would be non-existance so don't check
    user->keys.remove(op_state, &subprocess_msg, true);
  }

  //remove the subuser from the user info
  subuser_map->erase(siter);

  // attempt to save the subuser
  if (!defer_user_update)
    ret = user->update(op_state, err_msg);

  if (ret < 0)
    return ret;

  return 0;
}

int RGWSubUserPool::remove(RGWUserAdminOpState& op_state, std::string *err_msg)
{
  return remove(op_state, err_msg, false);
}

int RGWSubUserPool::remove(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_user_update)
{
  std::string subprocess_msg;
  int ret;

  ret = check_op(op_state, &subprocess_msg);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to parse request, " + subprocess_msg);
    return ret;
  }

  ret = execute_remove(op_state, &subprocess_msg, defer_user_update);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to remove subuser, " + subprocess_msg);
    return ret;
  }

  return 0;
}

int RGWSubUserPool::execute_modify(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_user_update)
{
  int ret = 0;
  std::string subprocess_msg;
  std::map<std::string, RGWSubUser>::iterator siter;
  std::pair<std::string, RGWSubUser> subuser_pair;

  std::string subuser_str = op_state.get_subuser();
  RGWSubUser subuser;

  if (!op_state.has_existing_subuser()) {
    set_err_msg(err_msg, "subuser does not exist");
    return -EINVAL;
  }

  subuser_pair.first = subuser_str;

  siter = subuser_map->find(subuser_str);
  subuser = siter->second;

  if (op_state.has_key_op()) {
    ret = user->keys.add(op_state, &subprocess_msg, true);
    if (ret < 0) {
      set_err_msg(err_msg, "unable to create subuser keys, " + subprocess_msg);
      return ret;
    }
  }

  if (op_state.has_subuser_perm())
    subuser.perm_mask = op_state.get_subuser_perm();

  subuser_pair.second = subuser;

  subuser_map->erase(siter);
  subuser_map->insert(subuser_pair);

  // attempt to save the subuser
  if (!defer_user_update)
    ret = user->update(op_state, err_msg);

  if (ret < 0)
    return ret;

  return 0;
}

int RGWSubUserPool::modify(RGWUserAdminOpState& op_state, std::string *err_msg)
{
  return RGWSubUserPool::modify(op_state, err_msg, false);
}

int RGWSubUserPool::modify(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_user_update)
{
  std::string subprocess_msg;
  int ret;

  RGWSubUser subuser;

  ret = check_op(op_state, &subprocess_msg);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to parse request, " + subprocess_msg);
    return ret;
  }

  ret = execute_modify(op_state, &subprocess_msg, defer_user_update);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to modify subuser, " + subprocess_msg);
    return ret;
  }

  return 0;
}

RGWUserCapPool::RGWUserCapPool(RGWUser *usr)
{
  user = usr;
  caps = NULL;
  caps_allowed = (user != NULL);
}

RGWUserCapPool::~RGWUserCapPool()
{

}

int RGWUserCapPool::init(RGWUserAdminOpState& op_state)
{
  if (!op_state.is_initialized()) {
    caps_allowed = false;
    return -EINVAL;
  }

  std::string uid = op_state.get_user_id();
  if (uid == RGW_USER_ANON_ID) {
    caps_allowed = false;
    return -EACCES;
  }

  caps = op_state.get_caps_obj();
  if (!caps) {
    caps_allowed = false;
    return -EINVAL;
  }

  caps_allowed = true;

  return 0;
}

int RGWUserCapPool::add(RGWUserAdminOpState& op_state, std::string *err_msg)
{
  return add(op_state, err_msg, false);
}

int RGWUserCapPool::add(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_save)
{
  int ret = 0;
  std::string caps_str = op_state.get_caps();

  if (!op_state.is_populated()) {
    set_err_msg(err_msg, "user info was not populated");
    return -EINVAL;
  }

  if (!caps_allowed) {
    set_err_msg(err_msg, "caps not allowed for this user");
    return -EACCES;
  }

  if (caps_str.empty()) {
    set_err_msg(err_msg, "empty user caps");
    return -EINVAL;
  }

  int r = caps->add_from_string(caps_str);
  if (r < 0) {
    set_err_msg(err_msg, "unable to add caps: " + caps_str);
    return r;
  }

  if (!defer_save)
    ret = user->update(op_state, err_msg);

  if (ret < 0)
    return ret;

  return 0;
}

int RGWUserCapPool::remove(RGWUserAdminOpState& op_state, std::string *err_msg)
{
  return remove(op_state, err_msg, false);
}

int RGWUserCapPool::remove(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_save)
{
  int ret = 0;

  std::string caps_str = op_state.get_caps();

  if (!op_state.is_populated()) {
    set_err_msg(err_msg, "user info was not populated");
    return -EINVAL;
  }

  if (!caps_allowed) {
    set_err_msg(err_msg, "caps not allowed for this user");
    return -EACCES;
  }

  if (caps_str.empty()) {
    set_err_msg(err_msg, "empty user caps");
    return -EINVAL;
  }

  int r = caps->remove_from_string(caps_str);
  if (r < 0) {
    set_err_msg(err_msg, "unable to remove caps: " + caps_str);
    return r;
  }

  if (!defer_save)
    ret = user->update(op_state, err_msg);

  if (ret < 0)
    return ret;

  return 0;
}

RGWUser::RGWUser() : store(NULL), info_stored(false), caps(this), keys(this), subusers(this)
{
  init_default();
}

int RGWUser::init(RGWRados *storage, RGWUserAdminOpState& op_state)
{
  init_default();
  int ret = init_storage(storage);
  if (ret < 0)
    return ret;

  ret = init(op_state);
  if (ret < 0)
    return ret;

  return 0;
}

RGWUser::~RGWUser()
{
}

void RGWUser::init_default()
{
  // use anonymous user info as a placeholder
  rgw_get_anon_user(old_info);
  user_id = RGW_USER_ANON_ID;

  clear_populated();
}

int RGWUser::init_storage(RGWRados *storage)
{
  if (!storage) {
    return -EINVAL;
  }

  store = storage;

  clear_populated();

  /* API wrappers */
  keys = RGWAccessKeyPool(this);
  caps = RGWUserCapPool(this);
  subusers = RGWSubUserPool(this);

  return 0;
}

int RGWUser::init(RGWUserAdminOpState& op_state)
{
  bool found = false;
  std::string swift_user;
  std::string uid = op_state.get_user_id();
  std::string user_email = op_state.get_user_email();
  std::string access_key = op_state.get_access_key();
  std::string subuser = op_state.get_subuser();

  int key_type = op_state.get_key_type();
  if (key_type == KEY_TYPE_SWIFT) {
    swift_user = op_state.get_access_key();
    access_key.clear();
  }

  RGWUserInfo user_info;

  clear_populated();

  if (uid.empty() && !subuser.empty()) {
    size_t pos = subuser.find(':');
    if (pos != string::npos) {
      uid = subuser.substr(0, pos);
      op_state.set_user_id(uid);
    }
  }

  if (!uid.empty() && (uid.compare(RGW_USER_ANON_ID) != 0))
    found = (rgw_get_user_info_by_uid(store, uid, user_info, &op_state.objv) >= 0);

  if (!user_email.empty() && !found)
    found = (rgw_get_user_info_by_email(store, user_email, user_info, &op_state.objv) >= 0);

  if (!swift_user.empty() && !found)
    found = (rgw_get_user_info_by_swift(store, swift_user, user_info, &op_state.objv) >= 0);

  if (!access_key.empty() && !found)
    found = (rgw_get_user_info_by_access_key(store, access_key, user_info, &op_state.objv) >= 0);

  op_state.set_existing_user(found);
  if (found) {
    op_state.set_user_info(user_info);
    op_state.set_populated();

    old_info = user_info;
    set_populated();
  }

  user_id = user_info.user_id;
  op_state.set_initialized();

  // this may have been called by a helper object
  int ret = init_members(op_state);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWUser::init_members(RGWUserAdminOpState& op_state)
{
  int ret = 0;

  ret = keys.init(op_state);
  if (ret < 0)
    return ret;

  ret = subusers.init(op_state);
  if (ret < 0)
    return ret;

  ret = caps.init(op_state);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWUser::update(RGWUserAdminOpState& op_state, std::string *err_msg)
{
  int ret;
  std::string subprocess_msg;
  RGWUserInfo user_info = op_state.get_user_info();

  if (!store) {
    set_err_msg(err_msg, "couldn't initialize storage");
    return -EINVAL;
  }

  if (is_populated()) {
    ret = rgw_store_user_info(store, user_info, &old_info, &op_state.objv, 0, false);
    if (ret < 0) {
      set_err_msg(err_msg, "unable to store user info");
      return ret;
    }

    ret = remove_old_indexes(store, old_info, user_info, &subprocess_msg);
    if (ret < 0) {
      set_err_msg(err_msg, "unable to remove old user info, " + subprocess_msg);
      return ret;
    }
  } else {
    ret = rgw_store_user_info(store, user_info, NULL, &op_state.objv, 0, false);
    if (ret < 0) {
      set_err_msg(err_msg, "unable to store user info");
      return ret;
    }
  }

  old_info = user_info;
  set_populated();

  return 0;
}

int RGWUser::check_op(RGWUserAdminOpState& op_state, std::string *err_msg)
{
  bool same_id;
  bool populated;
  //bool existing_email = false; // this check causes a fault
  std::string op_id = op_state.get_user_id();
  std::string op_email = op_state.get_user_email();

  RGWUserInfo user_info;

  same_id = (user_id.compare(op_id) == 0);
  populated = is_populated();

  if (op_id.compare(RGW_USER_ANON_ID) == 0) {
    set_err_msg(err_msg, "unable to perform operations on the anonymous user");
    return -EINVAL;
  }

  if (populated && !same_id) {
    set_err_msg(err_msg, "user id mismatch, operation id: " + op_id\
            + " does not match: " + user_id);

    return -EINVAL;
  }

  return 0;
}

int RGWUser::execute_add(RGWUserAdminOpState& op_state, std::string *err_msg)
{
  std::string subprocess_msg;
  int ret = 0;
  bool defer_user_update = true;

  RGWUserInfo user_info;

  std::string uid = op_state.get_user_id();
  std::string user_email = op_state.get_user_email();
  std::string display_name = op_state.get_display_name();

  // fail if the user exists already
  if (op_state.has_existing_user()) {
    if (!op_state.exclusive &&
        (user_email.empty() || old_info.user_email == user_email) &&
        old_info.display_name == display_name) {
      return execute_modify(op_state, err_msg);
    }

    set_err_msg(err_msg, "user: " + op_state.user_id + " exists");

    return -EEXIST;
  }

  // fail if the user_info has already been populated
  if (op_state.is_populated()) {
    set_err_msg(err_msg, "cannot overwrite already populated user");
    return -EEXIST;
  }

  // fail if the display name was not included
  if (display_name.empty()) {
    set_err_msg(err_msg, "no display name specified");
    return -EINVAL;
  }

  // fail if the user email is a duplicate
  if (op_state.has_existing_email()) {
    set_err_msg(err_msg, "duplicate email provided");
    return -EEXIST;
  }

  // set the user info
  user_id = uid;
  user_info.user_id = user_id;
  user_info.display_name = display_name;

  if (!user_email.empty())
    user_info.user_email = user_email;

  user_info.max_buckets = op_state.get_max_buckets();
  user_info.suspended = op_state.get_suspension_status();
  user_info.system = op_state.system;

  if (op_state.op_mask_specified)
    user_info.op_mask = op_state.get_op_mask();

  if (op_state.has_bucket_quota())
    user_info.bucket_quota = op_state.get_bucket_quota();

  if (op_state.temp_url_key_specified) {
    map<int, string>::iterator iter;
    for (iter = op_state.temp_url_keys.begin();
         iter != op_state.temp_url_keys.end(); ++iter) {
      user_info.temp_url_keys[iter->first] = iter->second;
    }
  }

  if (op_state.has_user_quota())
    user_info.user_quota = op_state.get_user_quota();

  // update the request
  op_state.set_user_info(user_info);
  op_state.set_populated();

  // update the helper objects
  ret = init_members(op_state);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to initialize user");
    return ret;
  }

  // see if we need to add an access key
  if (op_state.has_key_op()) {
    ret = keys.add(op_state, &subprocess_msg, defer_user_update);
    if (ret < 0) {
      set_err_msg(err_msg, "unable to create access key, " + subprocess_msg);
      return ret;
    }
  }

  // see if we need to add some caps
  if (op_state.has_caps_op()) {
    ret = caps.add(op_state, &subprocess_msg, defer_user_update);
    if (ret < 0) {
      set_err_msg(err_msg, "unable to add user capabilities, " + subprocess_msg);
      return ret;
    }
  }

  ret = update(op_state, err_msg);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWUser::add(RGWUserAdminOpState& op_state, std::string *err_msg)
{
  std::string subprocess_msg;
  int ret;

  ret = check_op(op_state, &subprocess_msg);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to parse parameters, " + subprocess_msg);
    return ret;
  }

  ret = execute_add(op_state, &subprocess_msg);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to create user, " + subprocess_msg);
    return ret;
  }

  return 0;
}

int RGWUser::execute_remove(RGWUserAdminOpState& op_state, std::string *err_msg)
{
  int ret;

  bool purge_data = op_state.will_purge_data();
  std::string uid = op_state.get_user_id();
  RGWUserInfo user_info = op_state.get_user_info();

  if (!op_state.has_existing_user()) {
    set_err_msg(err_msg, "user does not exist");
    return -EINVAL;
  }

  bool done;
  string marker;
  CephContext *cct = store->ctx();
  size_t max_buckets = cct->_conf->rgw_list_buckets_max_chunk;
  do {
    RGWUserBuckets buckets;
    ret = rgw_read_user_buckets(store, uid, buckets, marker, max_buckets, false);
    if (ret < 0) {
      set_err_msg(err_msg, "unable to read user bucket info");
      return ret;
    }

    map<std::string, RGWBucketEnt>& m = buckets.get_buckets();
    if (!m.empty() && !purge_data) {
      set_err_msg(err_msg, "must specify purge data to remove user with buckets");
      return -EEXIST; // change to code that maps to 409: conflict
    }

    std::map<std::string, RGWBucketEnt>::iterator it;
    for (it = m.begin(); it != m.end(); ++it) {
      ret = rgw_remove_bucket(store, uid, ((*it).second).bucket, true);
      if (ret < 0) {
        set_err_msg(err_msg, "unable to delete user data");
        return ret;
      }

      marker = it->first;
    }

    done = (m.size() < max_buckets);
  } while (!done);

  ret = rgw_delete_user(store, user_info, op_state.objv);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to remove user from RADOS");
    return ret;
  }

  op_state.clear_populated();
  clear_populated();

  return 0;
}

int RGWUser::remove(RGWUserAdminOpState& op_state, std::string *err_msg)
{
  std::string subprocess_msg;
  int ret;

  ret = check_op(op_state, &subprocess_msg);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to parse parameters, " + subprocess_msg);
    return ret;
  }

  ret = execute_remove(op_state, &subprocess_msg);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to remove user, " + subprocess_msg);
    return ret;
  }

  return 0;
}

int RGWUser::execute_modify(RGWUserAdminOpState& op_state, std::string *err_msg)
{
  bool populated = op_state.is_populated();
  int ret = 0;
  std::string subprocess_msg;
  std::string op_email = op_state.get_user_email();
  std::string display_name = op_state.get_display_name();

  RGWUserInfo user_info;
  RGWUserInfo duplicate_check;

  // ensure that the user info has been populated or is populate-able
  if (!op_state.has_existing_user() && !populated) {
    set_err_msg(err_msg, "user not found");
    return -ENOENT;
  }

  // if the user hasn't already been populated...attempt to
  if (!populated) {
    ret = init(op_state);
    if (ret < 0) {
      set_err_msg(err_msg, "unable to retrieve user info");
      return ret;
    }
  }

  // ensure that we can modify the user's attributes
  if (user_id == RGW_USER_ANON_ID) {
    set_err_msg(err_msg, "unable to modify anonymous user's info");
    return -EACCES;
  }

  user_info = old_info;

  std::string old_email = old_info.user_email;
  if (!op_email.empty()) {
    // make sure we are not adding a duplicate email
    if (old_email.compare(op_email) != 0) {
      ret = rgw_get_user_info_by_email(store, op_email, duplicate_check);
      if (ret >= 0 && duplicate_check.user_id != user_id) {
        set_err_msg(err_msg, "cannot add duplicate email");
        return -EEXIST;
      }
    }
    user_info.user_email = op_email;
  }


  // update the remaining user info
  if (!display_name.empty())
    user_info.display_name = display_name;

  // will be set to RGW_DEFAULT_MAX_BUCKETS by default
  uint32_t max_buckets = op_state.get_max_buckets();

  ldout(store->ctx(), 0) << "max_buckets=" << max_buckets << " specified=" << op_state.max_buckets_specified << dendl;

  if (op_state.max_buckets_specified)
    user_info.max_buckets = max_buckets;

  if (op_state.system_specified)
    user_info.system = op_state.system;

  if (op_state.temp_url_key_specified) {
    map<int, string>::iterator iter;
    for (iter = op_state.temp_url_keys.begin();
         iter != op_state.temp_url_keys.end(); ++iter) {
      user_info.temp_url_keys[iter->first] = iter->second;
    }
  }

  if (op_state.op_mask_specified)
    user_info.op_mask = op_state.get_op_mask();

  if (op_state.has_bucket_quota())
    user_info.bucket_quota = op_state.get_bucket_quota();

  if (op_state.has_user_quota())
    user_info.user_quota = op_state.get_user_quota();

  if (op_state.has_suspension_op()) {
    __u8 suspended = op_state.get_suspension_status();
    user_info.suspended = suspended;

    RGWUserBuckets buckets;

    if (user_id.empty()) {
      set_err_msg(err_msg, "empty user id passed...aborting");
      return -EINVAL;
    }

    bool done;
    string marker;
    CephContext *cct = store->ctx();
    size_t max_buckets = cct->_conf->rgw_list_buckets_max_chunk;
    do {
      ret = rgw_read_user_buckets(store, user_id, buckets, marker, max_buckets, false);
      if (ret < 0) {
        set_err_msg(err_msg, "could not get buckets for uid:  " + user_id);
        return ret;
      }

      map<string, RGWBucketEnt>& m = buckets.get_buckets();
      map<string, RGWBucketEnt>::iterator iter;

      vector<rgw_bucket> bucket_names;
      for (iter = m.begin(); iter != m.end(); ++iter) {
        RGWBucketEnt obj = iter->second;
        bucket_names.push_back(obj.bucket);

        marker = iter->first;
      }

      ret = store->set_buckets_enabled(bucket_names, !suspended);
      if (ret < 0) {
        set_err_msg(err_msg, "failed to modify bucket");
        return ret;
      }

      done = (m.size() < max_buckets);
    } while (!done);
  }
  op_state.set_user_info(user_info);

  // if we're supposed to modify keys, do so
  if (op_state.has_key_op()) {
    ret = keys.add(op_state, &subprocess_msg, true);
    if (ret < 0) {
      set_err_msg(err_msg, "unable to create or modify keys, " + subprocess_msg);
      return ret;
    }
  }

  ret = update(op_state, err_msg);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWUser::modify(RGWUserAdminOpState& op_state, std::string *err_msg)
{
  std::string subprocess_msg;
  int ret;

  ret = check_op(op_state, &subprocess_msg);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to parse parameters, " + subprocess_msg);
    return ret;
  }

  ret = execute_modify(op_state, &subprocess_msg);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to modify user, " + subprocess_msg);
    return ret;
  }

  return 0;
}

int RGWUser::info(RGWUserAdminOpState& op_state, RGWUserInfo& fetched_info, std::string *err_msg)
{
  int ret = init(op_state);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to fetch user info");
    return ret;
  }

  fetched_info = op_state.get_user_info();

  return 0;
}

int RGWUser::info(RGWUserInfo& fetched_info, std::string *err_msg)
{
  if (!is_populated()) {
    set_err_msg(err_msg, "no user info saved");
    return -EINVAL;
  }

  fetched_info = old_info;

  return 0;
}

int RGWUserAdminOp_User::info(RGWRados *store, RGWUserAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  RGWUserInfo info;
  RGWUser user;

  int ret = user.init(store, op_state);
  if (ret < 0)
    return ret;

  if (!op_state.has_existing_user())
    return -ENOENT;

  Formatter *formatter = flusher.get_formatter();

  ret = user.info(info, NULL);
  if (ret < 0)
    return ret;

  RGWStorageStats stats;
  RGWStorageStats *arg_stats = NULL;
  if (op_state.fetch_stats) {
    int ret = store->get_user_stats(info.user_id, stats);
    if (ret < 0) {
      return ret;
    }

    arg_stats = &stats;
  }

  flusher.start(0);

  dump_user_info(formatter, info, arg_stats);
  flusher.flush();

  return 0;
}

int RGWUserAdminOp_User::create(RGWRados *store, RGWUserAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  RGWUserInfo info;
  RGWUser user;
  int ret = user.init(store, op_state);
  if (ret < 0)
    return ret;

  Formatter *formatter = flusher.get_formatter();

  ret = user.add(op_state, NULL);
  if (ret < 0)
    return ret;

  ret = user.info(info, NULL);
  if (ret < 0)
    return ret;

  flusher.start(0);

  dump_user_info(formatter, info);
  flusher.flush();

  return 0;
}

int RGWUserAdminOp_User::modify(RGWRados *store, RGWUserAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  RGWUserInfo info;
  RGWUser user;
  int ret = user.init(store, op_state);
  if (ret < 0)
    return ret;
  Formatter *formatter = flusher.get_formatter();

  ret = user.modify(op_state, NULL);
  if (ret < 0)
    return ret;

  ret = user.info(info, NULL);
  if (ret < 0)
    return ret;

  flusher.start(0);

  dump_user_info(formatter, info);
  flusher.flush();

  return 0;
}

int RGWUserAdminOp_User::remove(RGWRados *store, RGWUserAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  RGWUserInfo info;
  RGWUser user;
  int ret = user.init(store, op_state);
  if (ret < 0)
    return ret;


  ret = user.remove(op_state, NULL);

  return ret;
}

int RGWUserAdminOp_Subuser::create(RGWRados *store, RGWUserAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  RGWUserInfo info;
  RGWUser user;
  int ret = user.init(store, op_state);
  if (ret < 0)
    return ret;

  Formatter *formatter = flusher.get_formatter();

  ret = user.subusers.add(op_state, NULL);
  if (ret < 0)
    return ret;

  ret = user.info(info, NULL);
  if (ret < 0)
    return ret;

  flusher.start(0);

  dump_subusers_info(formatter, info);
  flusher.flush();

  return 0;
}

int RGWUserAdminOp_Subuser::modify(RGWRados *store, RGWUserAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  RGWUserInfo info;
  RGWUser user;
  int ret = user.init(store, op_state);
  if (ret < 0)
    return ret;

  Formatter *formatter = flusher.get_formatter();

  ret = user.subusers.modify(op_state, NULL);
  if (ret < 0)
    return ret;

  ret = user.info(info, NULL);
  if (ret < 0)
    return ret;
  
  flusher.start(0);

  dump_subusers_info(formatter, info);
  flusher.flush();

  return 0;
}

int RGWUserAdminOp_Subuser::remove(RGWRados *store, RGWUserAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  RGWUserInfo info;
  RGWUser user;
  int ret = user.init(store, op_state);
  if (ret < 0)
    return ret;


  ret = user.subusers.remove(op_state, NULL);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWUserAdminOp_Key::create(RGWRados *store, RGWUserAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  RGWUserInfo info;
  RGWUser user;
  int ret = user.init(store, op_state);
  if (ret < 0)
    return ret;

  Formatter *formatter = flusher.get_formatter();

  ret = user.keys.add(op_state, NULL);
  if (ret < 0)
    return ret;

  ret = user.info(info, NULL);
  if (ret < 0)
    return ret;

  flusher.start(0);

  int key_type = op_state.get_key_type();

  if (key_type == KEY_TYPE_SWIFT)
    dump_swift_keys_info(formatter, info);

  else if (key_type == KEY_TYPE_S3)
    dump_access_keys_info(formatter, info);

  flusher.flush();

  return 0;
}

int RGWUserAdminOp_Key::remove(RGWRados *store, RGWUserAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  RGWUserInfo info;
  RGWUser user;
  int ret = user.init(store, op_state);
  if (ret < 0)
    return ret;


  ret = user.keys.remove(op_state, NULL);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWUserAdminOp_Caps::add(RGWRados *store, RGWUserAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  RGWUserInfo info;
  RGWUser user;
  int ret = user.init(store, op_state);
  if (ret < 0)
    return ret;

  Formatter *formatter = flusher.get_formatter();

  ret = user.caps.add(op_state, NULL);
  if (ret < 0)
    return ret;

  ret = user.info(info, NULL);
  if (ret < 0)
    return ret;

  flusher.start(0);

  info.caps.dump(formatter);
  flusher.flush();

  return 0;
}


int RGWUserAdminOp_Caps::remove(RGWRados *store, RGWUserAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  RGWUserInfo info;
  RGWUser user;
  int ret = user.init(store, op_state);
  if (ret < 0)
    return ret;

  Formatter *formatter = flusher.get_formatter();

  ret = user.caps.remove(op_state, NULL);
  if (ret < 0)
    return ret;

  ret = user.info(info, NULL);
  if (ret < 0)
    return ret;

  flusher.start(0);

  info.caps.dump(formatter);
  flusher.flush();

  return 0;
}

class RGWUserMetadataObject : public RGWMetadataObject {
  RGWUserInfo info;
public:
  RGWUserMetadataObject(RGWUserInfo& i, obj_version& v, time_t m) : info(i) {
    objv = v;
    mtime = m;
  }

  void dump(Formatter *f) const {
    info.dump(f);
  }
};

class RGWUserMetadataHandler : public RGWMetadataHandler {
public:
  string get_type() { return "user"; }

  int get(RGWRados *store, string& entry, RGWMetadataObject **obj) {
    RGWUserInfo info;

    RGWObjVersionTracker objv_tracker;
    time_t mtime;

    int ret = rgw_get_user_info_by_uid(store, entry, info, &objv_tracker, &mtime);
    if (ret < 0)
      return ret;

    RGWUserMetadataObject *mdo = new RGWUserMetadataObject(info, objv_tracker.read_version, mtime);

    *obj = mdo;

    return 0;
  }

  int put(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker,
          time_t mtime, JSONObj *obj, sync_type_t sync_mode) {
    RGWUserInfo info;

    decode_json_obj(info, obj);

    RGWUserInfo old_info;
    time_t orig_mtime;
    int ret = rgw_get_user_info_by_uid(store, entry, old_info, &objv_tracker, &orig_mtime);
    if (ret < 0 && ret != -ENOENT)
      return ret;

    // are we actually going to perform this put, or is it too old?
    if (ret != -ENOENT &&
        !check_versions(objv_tracker.read_version, orig_mtime,
			objv_tracker.write_version, mtime, sync_mode)) {
      return STATUS_NO_APPLY;
    }

    ret = rgw_store_user_info(store, info, &old_info, &objv_tracker, mtime, false);
    if (ret < 0)
      return ret;

    return STATUS_APPLIED;
  }

  struct list_keys_info {
    RGWRados *store;
    RGWListRawObjsCtx ctx;
  };

  int remove(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker) {
    RGWUserInfo info;
    int ret = rgw_get_user_info_by_uid(store, entry, info, &objv_tracker);
    if (ret < 0)
      return ret;

    return rgw_delete_user(store, info, objv_tracker);
  }

  void get_pool_and_oid(RGWRados *store, const string& key, rgw_bucket& bucket, string& oid) {
    oid = key;
    bucket = store->zone.user_uid_pool;
  }

  int list_keys_init(RGWRados *store, void **phandle)
  {
    list_keys_info *info = new list_keys_info;

    info->store = store;

    *phandle = (void *)info;

    return 0;
  }

  int list_keys_next(void *handle, int max, list<string>& keys, bool *truncated) {
    list_keys_info *info = static_cast<list_keys_info *>(handle);

    string no_filter;

    keys.clear();

    RGWRados *store = info->store;

    list<string> unfiltered_keys;

    int ret = store->list_raw_objects(store->zone.user_uid_pool, no_filter,
                                      max, info->ctx, unfiltered_keys, truncated);
    if (ret < 0)
      return ret;

    // now filter out the buckets entries
    list<string>::iterator iter;
    for (iter = unfiltered_keys.begin(); iter != unfiltered_keys.end(); ++iter) {
      string& k = *iter;

      if (k.find(".buckets") == string::npos) {
        keys.push_back(k);
      }
    }

    return 0;
  }

  void list_keys_complete(void *handle) {
    list_keys_info *info = static_cast<list_keys_info *>(handle);
    delete info;
  }
};

void rgw_user_init(RGWRados *store)
{
  uinfo_cache.init(store);

  user_meta_handler = new RGWUserMetadataHandler;
  store->meta_mgr->register_handler(user_meta_handler);
}
