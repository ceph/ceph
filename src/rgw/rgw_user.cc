// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include <string>
#include <map>
#include <boost/algorithm/string.hpp>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "common/RWLock.h"
#include "rgw_rados.h"
#include "rgw_zone.h"
#include "rgw_acl.h"

#include "include/types.h"
#include "rgw_user.h"
#include "rgw_string.h"

// until everything is moved from rgw_common
#include "rgw_common.h"

#include "rgw_bucket.h"
#include "rgw_quota.h"

#include "services/svc_zone.h"
#include "services/svc_sys_obj.h"
#include "services/svc_sys_obj_cache.h"

#define dout_subsys ceph_subsys_rgw



static RGWMetadataHandler *user_meta_handler = NULL;
extern void op_type_to_str(uint32_t mask, char *buf, int len);

/**
 * Get the anonymous (ie, unauthenticated) user info.
 */
void rgw_get_anon_user(RGWUserInfo& info)
{
  info.user_id = RGW_USER_ANON_ID;
  info.display_name.clear();
  info.access_keys.clear();
}

int rgw_user_sync_all_stats(RGWRados *store, const rgw_user& user_id)
{
  CephContext *cct = store->ctx();
  size_t max_entries = cct->_conf->rgw_list_buckets_max_chunk;
  bool is_truncated = false;
  string marker;
  int ret;
  RGWSysObjectCtx obj_ctx = store->svc.sysobj->init_obj_ctx();

  do {
    RGWUserBuckets user_buckets;
    ret = rgw_read_user_buckets(store, user_id, user_buckets, marker,
				string(), max_entries, false, &is_truncated);
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
      RGWBucketInfo bucket_info;

      ret = store->get_bucket_info(obj_ctx, user_id.tenant, bucket_ent.bucket.name,
                                   bucket_info, nullptr, nullptr);
      if (ret < 0) {
        ldout(cct, 0) << "ERROR: could not read bucket info: bucket=" << bucket_ent.bucket << " ret=" << ret << dendl;
        continue;
      }
      ret = rgw_bucket_sync_user_stats(store, user_id, bucket_info);
      if (ret < 0) {
        ldout(cct, 0) << "ERROR: could not sync bucket stats: ret=" << ret << dendl;
        return ret;
      }
      RGWQuotaInfo bucket_quota;
      ret = store->check_bucket_shards(bucket_info, bucket_info.bucket, bucket_quota);
      if (ret < 0) {
	ldout(cct, 0) << "ERROR in check_bucket_shards: " << cpp_strerror(-ret)<< dendl;
      }
    }
  } while (is_truncated);

  ret = store->complete_sync_user_stats(user_id);
  if (ret < 0) {
    cerr << "ERROR: failed to complete syncing user stats: ret=" << ret << std::endl;
    return ret;
  }

  return 0;
}

int rgw_user_get_all_buckets_stats(RGWRados *store, const rgw_user& user_id, map<string, cls_user_bucket_entry>&buckets_usage_map)
{
  CephContext *cct = store->ctx();
  size_t max_entries = cct->_conf->rgw_list_buckets_max_chunk;
  bool done;
  bool is_truncated;
  string marker;
  int ret;

  do {
    RGWUserBuckets user_buckets;
    ret = rgw_read_user_buckets(store, user_id, user_buckets, marker,
				string(), max_entries, false, &is_truncated);
    if (ret < 0) {
      ldout(cct, 0) << "failed to read user buckets: ret=" << ret << dendl;
      return ret;
    }
    map<string, RGWBucketEnt>& buckets = user_buckets.get_buckets();
    for (const auto& i :  buckets) {
      marker = i.first;

      const RGWBucketEnt& bucket_ent = i.second;
      cls_user_bucket_entry entry;
      ret = store->cls_user_get_bucket_stats(bucket_ent.bucket, entry);
      if (ret < 0) {
        ldout(cct, 0) << "ERROR: could not get bucket stats: ret=" << ret << dendl;
        return ret;
      }
      buckets_usage_map.emplace(bucket_ent.bucket.name, entry);
    }
    done = (buckets.size() < max_entries);
  } while (!done);

  return 0;
}

/**
 * Save the given user information to storage.
 * Returns: 0 on success, -ERR# on failure.
 */
int rgw_store_user_info(RGWRados *store,
                        RGWUserInfo& info,
                        RGWUserInfo *old_info,
                        RGWObjVersionTracker *objv_tracker,
                        real_time mtime,
                        bool exclusive,
                        map<string, bufferlist> *pattrs)
{
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
  encode(ui, link_bl);

  bufferlist data_bl;
  encode(ui, data_bl);
  encode(info, data_bl);

  string key;
  info.user_id.to_str(key);

  ret = store->meta_mgr->put_entry(user_meta_handler, key, data_bl, exclusive, &ot, mtime, pattrs);
  if (ret < 0)
    return ret;

  if (!info.user_email.empty()) {
    if (!old_info ||
        old_info->user_email.compare(info.user_email) != 0) { /* only if new index changed */
      ret = rgw_put_system_obj(store, store->svc.zone->get_zone_params().user_email_pool, info.user_email,
                               link_bl, exclusive, NULL, real_time());
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

      ret = rgw_put_system_obj(store, store->svc.zone->get_zone_params().user_keys_pool, k.id,
                               link_bl, exclusive, NULL, real_time());
      if (ret < 0)
        return ret;
    }
  }

  map<string, RGWAccessKey>::iterator siter;
  for (siter = info.swift_keys.begin(); siter != info.swift_keys.end(); ++siter) {
    RGWAccessKey& k = siter->second;
    if (old_info && old_info->swift_keys.count(siter->first) != 0)
      continue;

    ret = rgw_put_system_obj(store, store->svc.zone->get_zone_params().user_swift_pool, k.id,
                             link_bl, exclusive, NULL, real_time());
    if (ret < 0)
      return ret;
  }

  return ret;
}

struct user_info_entry {
  RGWUserInfo info;
  RGWObjVersionTracker objv_tracker;
  real_time mtime;
};

static RGWChainedCacheImpl<user_info_entry> uinfo_cache;

int rgw_get_user_info_from_index(RGWRados * const store,
                                 const string& key,
                                 const rgw_pool& pool,
                                 RGWUserInfo& info,
                                 RGWObjVersionTracker * const objv_tracker,
                                 real_time * const pmtime)
{
  if (auto e = uinfo_cache.find(key)) {
    info = e->info;
    if (objv_tracker)
      *objv_tracker = e->objv_tracker;
    if (pmtime)
      *pmtime = e->mtime;
    return 0;
  }

  user_info_entry e;
  bufferlist bl;
  RGWUID uid;
  auto obj_ctx = store->svc.sysobj->init_obj_ctx();

  int ret = rgw_get_system_obj(store, obj_ctx, pool, key, bl, NULL, &e.mtime);
  if (ret < 0)
    return ret;

  rgw_cache_entry_info cache_info;

  auto iter = bl.cbegin();
  try {
    decode(uid, iter);
    int ret = rgw_get_user_info_by_uid(store, uid.user_id, e.info, &e.objv_tracker, NULL, &cache_info);
    if (ret < 0) {
      return ret;
    }
  } catch (buffer::error& err) {
    ldout(store->ctx(), 0) << "ERROR: failed to decode user info, caught buffer::error" << dendl;
    return -EIO;
  }

  uinfo_cache.put(store->svc.cache, key, &e, { &cache_info });

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
int rgw_get_user_info_by_uid(RGWRados *store,
                             const rgw_user& uid,
                             RGWUserInfo& info,
                             RGWObjVersionTracker * const objv_tracker,
                             real_time * const pmtime,
                             rgw_cache_entry_info * const cache_info,
                             map<string, bufferlist> * const pattrs)
{
  bufferlist bl;
  RGWUID user_id;

  auto obj_ctx = store->svc.sysobj->init_obj_ctx();
  string oid = uid.to_str();
  int ret = rgw_get_system_obj(store, obj_ctx, store->svc.zone->get_zone_params().user_uid_pool, oid, bl, objv_tracker, pmtime, pattrs, cache_info);
  if (ret < 0) {
    return ret;
  }

  auto iter = bl.cbegin();
  try {
    decode(user_id, iter);
    if (user_id.user_id.compare(uid) != 0) {
      lderr(store->ctx())  << "ERROR: rgw_get_user_info_by_uid(): user id mismatch: " << user_id.user_id << " != " << uid << dendl;
      return -EIO;
    }
    if (!iter.end()) {
      decode(info, iter);
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
                               RGWObjVersionTracker *objv_tracker, real_time *pmtime)
{
  return rgw_get_user_info_from_index(store, email, store->svc.zone->get_zone_params().user_email_pool, info, objv_tracker, pmtime);
}

/**
 * Given an swift username, finds the user_info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_swift(RGWRados * const store,
                                      const string& swift_name,
                                      RGWUserInfo& info,        /* out */
                                      RGWObjVersionTracker * const objv_tracker,
                                      real_time * const pmtime)
{
  return rgw_get_user_info_from_index(store, swift_name,
                                      store->svc.zone->get_zone_params().user_swift_pool,
                                      info, objv_tracker, pmtime);
}

/**
 * Given an access key, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_access_key(RGWRados* store,
                                           const std::string& access_key,
                                           RGWUserInfo& info,
                                           RGWObjVersionTracker* objv_tracker,
                                           real_time *pmtime)
{
  return rgw_get_user_info_from_index(store, access_key,
                                      store->svc.zone->get_zone_params().user_keys_pool,
                                      info, objv_tracker, pmtime);
}

int rgw_get_user_attrs_by_uid(RGWRados *store,
                              const rgw_user& user_id,
                              map<string, bufferlist>& attrs,
                              RGWObjVersionTracker *objv_tracker)
{
  auto obj_ctx = store->svc.sysobj->init_obj_ctx();
  rgw_raw_obj obj(store->svc.zone->get_zone_params().user_uid_pool, user_id.to_str());
  auto src = obj_ctx.get_obj(obj);

  return src.rop()
            .set_attrs(&attrs)
            .set_objv_tracker(objv_tracker)
            .stat(null_yield);
}

int rgw_remove_key_index(RGWRados *store, RGWAccessKey& access_key)
{
  rgw_raw_obj obj(store->svc.zone->get_zone_params().user_keys_pool, access_key.id);
  auto obj_ctx = store->svc.sysobj->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(obj);
  return sysobj.wop().remove(null_yield);
}

int rgw_remove_uid_index(RGWRados *store, rgw_user& uid)
{
  RGWObjVersionTracker objv_tracker;
  RGWUserInfo info;
  int ret = rgw_get_user_info_by_uid(store, uid, info, &objv_tracker, NULL);
  if (ret < 0)
    return ret;

  string oid = uid.to_str();
  ret = store->meta_mgr->remove_entry(user_meta_handler, oid, &objv_tracker);
  if (ret < 0)
    return ret;

  return 0;
}

int rgw_remove_email_index(RGWRados *store, string& email)
{
  if (email.empty()) {
    return 0;
  }
  rgw_raw_obj obj(store->svc.zone->get_zone_params().user_email_pool, email);
  auto obj_ctx = store->svc.sysobj->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(obj);
  return sysobj.wop().remove(null_yield);
}

int rgw_remove_swift_name_index(RGWRados *store, string& swift_name)
{
  rgw_raw_obj obj(store->svc.zone->get_zone_params().user_swift_pool, swift_name);
  auto obj_ctx = store->svc.sysobj->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(obj);
  return sysobj.wop().remove(null_yield);
}

/**
 * delete a user's presence from the RGW system.
 * First remove their bucket ACLs, then delete them
 * from the user and user email pools. This leaves the pools
 * themselves alone, as well as any ACLs embedded in object xattrs.
 */
int rgw_delete_user(RGWRados *store, RGWUserInfo& info, RGWObjVersionTracker& objv_tracker) {
  int ret;

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

  ldout(store->ctx(), 10) << "removing email index: " << info.user_email << dendl;
  ret = rgw_remove_email_index(store, info.user_email);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 0) << "ERROR: could not remove email index object for "
        << info.user_email << ", should be fixed (err=" << ret << ")" << dendl;
    return ret;
  }

  string buckets_obj_id;
  rgw_get_buckets_obj(info.user_id, buckets_obj_id);
  rgw_raw_obj uid_bucks(store->svc.zone->get_zone_params().user_uid_pool, buckets_obj_id);
  ldout(store->ctx(), 10) << "removing user buckets index" << dendl;
  auto obj_ctx = store->svc.sysobj->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(uid_bucks);
  ret = sysobj.wop().remove(null_yield);
  if (ret < 0 && ret != -ENOENT) {
    ldout(store->ctx(), 0) << "ERROR: could not remove " << info.user_id << ":" << uid_bucks << ", should be fixed (err=" << ret << ")" << dendl;
    return ret;
  }

  string key;
  info.user_id.to_str(key);
  
  rgw_raw_obj uid_obj(store->svc.zone->get_zone_params().user_uid_pool, key);
  ldout(store->ctx(), 10) << "removing user index: " << info.user_id << dendl;
  ret = store->meta_mgr->remove_entry(user_meta_handler, key, &objv_tracker);
  if (ret < 0 && ret != -ENOENT && ret  != -ECANCELED) {
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
 { RGW_PERM_WRITE_ACP, "write-acp" },
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
  if (strcasecmp(str, "") == 0)
    return RGW_PERM_NONE;
  else if (strcasecmp(str, "read") == 0)
    return RGW_PERM_READ;
  else if (strcasecmp(str, "write") == 0)
    return RGW_PERM_WRITE;
  else if (strcasecmp(str, "readwrite") == 0)
    return RGW_PERM_READ | RGW_PERM_WRITE;
  else if (strcasecmp(str, "full") == 0)
    return RGW_PERM_FULL_CONTROL;

  return RGW_PERM_INVALID;
}

int rgw_validate_tenant_name(const string& t)
{
  struct tench {
    static bool is_good(char ch) {
      return isalnum(ch) || ch == '_';
    }
  };
  std::string::const_iterator it =
    std::find_if_not(t.begin(), t.end(), tench::is_good);
  return (it == t.end())? 0: -ERR_INVALID_TENANT_NAME;
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

  if (!old_info.user_id.empty() &&
      old_info.user_id.compare(new_info.user_id) != 0) {
    if (old_info.user_id.tenant != new_info.user_id.tenant) {
      ldout(store->ctx(), 0) << "ERROR: tenant mismatch: " << old_info.user_id.tenant << " != " << new_info.user_id.tenant << dendl;
      return false;
    }
    ret = rgw_remove_uid_index(store, old_info.user_id);
    if (ret < 0 && ret != -ENOENT) {
      set_err_msg(err_msg, "ERROR: could not remove index for uid " + old_info.user_id.to_str());
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
    string s;
    info.user_id.to_str(s);
    f->dump_format("id", "%s:%s", s.c_str(), u.name.c_str());
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
    string s;
    info.user_id.to_str(s);
    f->dump_format("user", "%s%s%s", s.c_str(), sep, subuser);
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
    string s;
    info.user_id.to_str(s);
    f->dump_format("user", "%s%s%s", s.c_str(), sep, subuser);
    f->dump_string("secret_key", k.key);
    f->close_section();
  }
  f->close_section();
}

static void dump_user_info(Formatter *f, RGWUserInfo &info,
                           RGWStorageStats *stats = NULL)
{
  f->open_object_section("user_info");
  encode_json("tenant", info.user_id.tenant, f);
  encode_json("user_id", info.user_id.id, f);
  encode_json("display_name", info.display_name, f);
  encode_json("email", info.user_email, f);
  encode_json("suspended", (int)info.suspended, f);
  encode_json("max_buckets", (int)info.max_buckets, f);

  dump_subusers_info(f, info);
  dump_access_keys_info(f, info);
  dump_swift_keys_info(f, info);

  encode_json("caps", info.caps, f);

  char buf[256];
  op_type_to_str(info.op_mask, buf, sizeof(buf));
  encode_json("op_mask", (const char *)buf, f);
  encode_json("system", (bool)info.system, f);
  encode_json("default_placement", info.default_placement.name, f);
  encode_json("default_storage_class", info.default_placement.storage_class, f);
  encode_json("placement_tags", info.placement_tags, f);
  encode_json("bucket_quota", info.bucket_quota, f);
  encode_json("user_quota", info.user_quota, f);
  encode_json("temp_url_keys", info.temp_url_keys, f);

  string user_source_type;
  switch ((RGWIdentityType)info.type) {
  case TYPE_RGW:
    user_source_type = "rgw";
    break;
  case TYPE_KEYSTONE:
    user_source_type = "keystone";
    break;
  case TYPE_LDAP:
    user_source_type = "ldap";
    break;
  case TYPE_NONE:
    user_source_type = "none";
    break;
  default:
    user_source_type = "none";
    break;
  }
  encode_json("type", user_source_type, f);
  encode_json("mfa_ids", info.mfa_ids, f);
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

  rgw_user& uid = op_state.get_user_id();
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

  // if a key type wasn't specified
  if (key_type < 0) {
      if (op_state.has_subuser()) {
        key_type = KEY_TYPE_SWIFT;
      } else {
        key_type = KEY_TYPE_S3;
      }
  }

  op_state.set_key_type(key_type);

  /* see if the access key was specified */
  if (key_type == KEY_TYPE_S3 && !op_state.will_gen_access() && 
      op_state.get_access_key().empty()) {
    set_err_msg(err_msg, "empty access key");
    return -ERR_INVALID_ACCESS_KEY;
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

  int key_type = op_state.get_key_type();
  bool gen_access = op_state.will_gen_access();
  bool gen_secret = op_state.will_gen_secret();

  if (!keys_allowed) {
    set_err_msg(err_msg, "access keys not allowed for this user");
    return -EACCES;
  }

  if (op_state.has_existing_key()) {
    set_err_msg(err_msg, "cannot create existing key");
    return -ERR_KEY_EXIST;
  }

  if (!gen_access) {
    id = op_state.get_access_key();
  }

  if (!id.empty()) {
    switch (key_type) {
    case KEY_TYPE_SWIFT:
      if (rgw_get_user_info_by_swift(store, id, duplicate_check) >= 0) {
        set_err_msg(err_msg, "existing swift key in RGW system:" + id);
        return -ERR_KEY_EXIST;
      }
      break;
    case KEY_TYPE_S3:
      if (rgw_get_user_info_by_access_key(store, id, duplicate_check) >= 0) {
        set_err_msg(err_msg, "existing S3 key in RGW system:" + id);
        return -ERR_KEY_EXIST;
      }
    }
  }

  //key's subuser
  if (op_state.has_subuser()) {
    //create user and subuser at the same time, user's s3 key should not be set this
    if (!op_state.key_type_setbycontext || (key_type == KEY_TYPE_SWIFT)) {
      new_key.subuser = op_state.get_subuser();
    }
  }

  //Secret key
  if (!gen_secret) {
    if (op_state.get_secret_key().empty()) {
      set_err_msg(err_msg, "empty secret key");
      return -ERR_INVALID_SECRET_KEY;
    }
  
    key = op_state.get_secret_key();
  } else {
    char secret_key_buf[SECRET_KEY_LEN + 1];
    gen_rand_alphanumeric_plain(g_ceph_context, secret_key_buf, sizeof(secret_key_buf));
    key = secret_key_buf;
  }

  // Generate the access key
  if (key_type == KEY_TYPE_S3 && gen_access) {
    char public_id_buf[PUBLIC_ID_LEN + 1];

    do {
      int id_buf_size = sizeof(public_id_buf);
      gen_rand_alphanumeric_upper(g_ceph_context, public_id_buf, id_buf_size);
      id = public_id_buf;
      if (!validate_access_key(id))
        continue;

    } while (!rgw_get_user_info_by_access_key(store, id, duplicate_check));
  }

  if (key_type == KEY_TYPE_SWIFT) {
    id = op_state.build_default_swift_kid();
    if (id.empty()) {
      set_err_msg(err_msg, "empty swift access key");
      return -ERR_INVALID_ACCESS_KEY;
    }

    // check that the access key doesn't exist
    if (rgw_get_user_info_by_swift(store, id, duplicate_check) >= 0) {
      set_err_msg(err_msg, "cannot create existing swift key");
      return -ERR_KEY_EXIST;
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
      return -ERR_INVALID_ACCESS_KEY;
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
    return -ERR_INVALID_KEY_TYPE;
  }

  if (!op_state.has_existing_key()) {
    set_err_msg(err_msg, "key does not exist");
    return -ERR_INVALID_ACCESS_KEY;
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
    int key_buf_size = sizeof(secret_key_buf);
    gen_rand_alphanumeric_plain(g_ceph_context, secret_key_buf, key_buf_size);
    key = secret_key_buf;
  }

  if (key.empty()) {
      set_err_msg(err_msg, "empty secret key");
      return -ERR_INVALID_SECRET_KEY;
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
    return -ERR_INVALID_ACCESS_KEY;
  }

  if (key_type == KEY_TYPE_S3) {
    keys_map = access_keys;
  } else if (key_type == KEY_TYPE_SWIFT) {
    keys_map = swift_keys;
  } else {
    keys_map = NULL;
    set_err_msg(err_msg, "invalid access key");
    return -ERR_INVALID_ACCESS_KEY;
  }

  kiter = keys_map->find(id);
  if (kiter == keys_map->end()) {
    set_err_msg(err_msg, "key not found");
    return -ERR_INVALID_ACCESS_KEY;
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

// remove all keys associated with a subuser
int RGWAccessKeyPool::remove_subuser_keys(RGWUserAdminOpState& op_state,
        std::string *err_msg, bool defer_user_update)
{
  int ret = 0;

  if (!op_state.is_populated()) {
    set_err_msg(err_msg, "user info was not populated");
    return -EINVAL;
  }

  if (!op_state.has_subuser()) {
    set_err_msg(err_msg, "no subuser specified");
    return -EINVAL;
  }

  std::string swift_kid = op_state.build_default_swift_kid();
  if (swift_kid.empty()) {
    set_err_msg(err_msg, "empty swift access key");
    return -EINVAL;
  }

  map<std::string, RGWAccessKey>::iterator kiter;
  map<std::string, RGWAccessKey> *keys_map;

  // a subuser can have at most one swift key
  keys_map = swift_keys;
  kiter = keys_map->find(swift_kid);
  if (kiter != keys_map->end()) {
    rgw_remove_key_index(store, kiter->second);
    keys_map->erase(kiter);
  }

  // a subuser may have multiple s3 key pairs
  std::string subuser_str = op_state.get_subuser();
  keys_map = access_keys;
  RGWUserInfo user_info = op_state.get_user_info();
  map<std::string, RGWAccessKey>::iterator user_kiter = user_info.access_keys.begin();
  for (; user_kiter != user_info.access_keys.end(); ++user_kiter) {
    if (user_kiter->second.subuser == subuser_str) {
      kiter = keys_map->find(user_kiter->first);
      if (kiter != keys_map->end()) {
        rgw_remove_key_index(store, kiter->second);
        keys_map->erase(kiter);
      }
    }
  }

  if (!defer_user_update)
    ret = user->update(op_state, err_msg);

  if (ret < 0)
    return ret;

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

  rgw_user& uid = op_state.get_user_id();
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

  if (op_state.get_subuser_perm() == RGW_PERM_INVALID) {
    set_err_msg(err_msg, "invaild subuser access");
    return -EINVAL;
  }

  //set key type when it not set or set by context
  if ((op_state.get_key_type() < 0) || op_state.key_type_setbycontext) {
    op_state.set_key_type(KEY_TYPE_SWIFT);
    op_state.key_type_setbycontext = true;
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
  int32_t key_type = op_state.get_key_type();

  ret = check_op(op_state, &subprocess_msg);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to parse request, " + subprocess_msg);
    return ret;
  }

  if (key_type == KEY_TYPE_S3 && op_state.get_access_key().empty()) {
    op_state.set_gen_access();
  }
  
  if (op_state.get_secret_key().empty()) {
    op_state.set_gen_secret();
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
  if (siter == subuser_map->end()){
    set_err_msg(err_msg, "subuser not found: " + subuser_str);
    return -ERR_NO_SUCH_SUBUSER;
  }
  if (!op_state.has_existing_subuser()) {
    set_err_msg(err_msg, "subuser not found: " + subuser_str);
    return -ERR_NO_SUCH_SUBUSER;
  }

  // always purge all associate keys
  user->keys.remove_subuser_keys(op_state, &subprocess_msg, true);

  // remove the subuser from the user info
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
    return -ERR_NO_SUCH_SUBUSER;
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

  rgw_user& uid = op_state.get_user_id();
  if (uid.compare(RGW_USER_ANON_ID) == 0) {
    caps_allowed = false;
    return -EACCES;
  }

  caps = op_state.get_caps_obj();
  if (!caps) {
    caps_allowed = false;
    return -ERR_INVALID_CAP;
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
    return -ERR_INVALID_CAP;
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
    return -ERR_INVALID_CAP;
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
  user_id = op_state.get_user_id();
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

  if (user_id.empty() && !subuser.empty()) {
    size_t pos = subuser.find(':');
    if (pos != string::npos) {
      user_id = subuser.substr(0, pos);
      op_state.set_user_id(user_id);
    }
  }

  if (!user_id.empty() && (user_id.compare(RGW_USER_ANON_ID) != 0)) {
    found = (rgw_get_user_info_by_uid(store, user_id, user_info, &op_state.objv) >= 0);
    op_state.found_by_uid = found;
  }
  if (!user_email.empty() && !found) {
    found = (rgw_get_user_info_by_email(store, user_email, user_info, &op_state.objv) >= 0);
    op_state.found_by_email = found;
  }
  if (!swift_user.empty() && !found) {
    found = (rgw_get_user_info_by_swift(store, swift_user, user_info, &op_state.objv) >= 0);
    op_state.found_by_key = found;
  }
  if (!access_key.empty() && !found) {
    found = (rgw_get_user_info_by_access_key(store, access_key, user_info, &op_state.objv) >= 0);
    op_state.found_by_key = found;
  }
  
  op_state.set_existing_user(found);
  if (found) {
    op_state.set_user_info(user_info);
    op_state.set_populated();

    old_info = user_info;
    set_populated();
  }

  if (user_id.empty()) {
    user_id = user_info.user_id;
  }
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
    ret = rgw_store_user_info(store, user_info, &old_info, &op_state.objv, real_time(), false);
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
    ret = rgw_store_user_info(store, user_info, NULL, &op_state.objv, real_time(), false);
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
  rgw_user& op_id = op_state.get_user_id();

  RGWUserInfo user_info;

  same_id = (user_id.compare(op_id) == 0);
  populated = is_populated();

  if (op_id.compare(RGW_USER_ANON_ID) == 0) {
    set_err_msg(err_msg, "unable to perform operations on the anonymous user");
    return -EINVAL;
  }

  if (populated && !same_id) {
    set_err_msg(err_msg, "user id mismatch, operation id: " + op_id.to_str()
            + " does not match: " + user_id.to_str());

    return -EINVAL;
  }

  int ret = rgw_validate_tenant_name(op_id.tenant);
  if (ret) {
    set_err_msg(err_msg,
		"invalid tenant only alphanumeric and _ characters are allowed");
    return ret;
  }

  //set key type when it not set or set by context
  if ((op_state.get_key_type() < 0) || op_state.key_type_setbycontext) {
    op_state.set_key_type(KEY_TYPE_S3);
    op_state.key_type_setbycontext = true;
  }

  return 0;
}

int RGWUser::execute_add(RGWUserAdminOpState& op_state, std::string *err_msg)
{
  std::string subprocess_msg;
  int ret = 0;
  bool defer_user_update = true;

  RGWUserInfo user_info;

  rgw_user& uid = op_state.get_user_id();
  std::string user_email = op_state.get_user_email();
  std::string display_name = op_state.get_display_name();

  // fail if the user exists already
  if (op_state.has_existing_user()) {
    if (!op_state.exclusive &&
        (user_email.empty() ||
	 boost::iequals(user_email, old_info.user_email)) &&
        old_info.display_name == display_name) {
      return execute_modify(op_state, err_msg);
    }

    if (op_state.found_by_email) {
      set_err_msg(err_msg, "email: " + user_email +
		  " is the email address an existing user");
      ret = -ERR_EMAIL_EXIST;
    } else if (op_state.found_by_key) {
      set_err_msg(err_msg, "duplicate key provided");
      ret = -ERR_KEY_EXIST;
    } else {
      set_err_msg(err_msg, "user: " + op_state.user_id.to_str() + " exists");
      ret = -EEXIST;
    }
    return ret;
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

		
  // set the user info
  user_id = uid;
  user_info.user_id = user_id;
  user_info.display_name = display_name;
  user_info.type = TYPE_RGW;

  if (!user_email.empty())
    user_info.user_email = user_email;

  CephContext *cct = store->ctx();
  if (op_state.max_buckets_specified) {
    user_info.max_buckets = op_state.get_max_buckets();
  } else {
    user_info.max_buckets = cct->_conf->rgw_user_max_buckets;
  }

  user_info.suspended = op_state.get_suspension_status();
  user_info.admin = op_state.admin;
  user_info.system = op_state.system;

  if (op_state.op_mask_specified)
    user_info.op_mask = op_state.get_op_mask();

  if (op_state.has_bucket_quota()) {
    user_info.bucket_quota = op_state.get_bucket_quota();
  } else {
    rgw_apply_default_bucket_quota(user_info.bucket_quota, cct->_conf);
  }

  if (op_state.temp_url_key_specified) {
    map<int, string>::iterator iter;
    for (iter = op_state.temp_url_keys.begin();
         iter != op_state.temp_url_keys.end(); ++iter) {
      user_info.temp_url_keys[iter->first] = iter->second;
    }
  }

  if (op_state.has_user_quota()) {
    user_info.user_quota = op_state.get_user_quota();
  } else {
    rgw_apply_default_user_quota(user_info.user_quota, cct->_conf);
  }

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
  rgw_user& uid = op_state.get_user_id();
  RGWUserInfo user_info = op_state.get_user_info();

  if (!op_state.has_existing_user()) {
    set_err_msg(err_msg, "user does not exist");
    return -ENOENT;
  }

  bool is_truncated = false;
  string marker;
  CephContext *cct = store->ctx();
  size_t max_buckets = cct->_conf->rgw_list_buckets_max_chunk;
  do {
    RGWUserBuckets buckets;
    ret = rgw_read_user_buckets(store, uid, buckets, marker, string(),
				max_buckets, false, &is_truncated);
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
      ret = rgw_remove_bucket(store, ((*it).second).bucket, true);
      if (ret < 0) {
        set_err_msg(err_msg, "unable to delete user data");
        return ret;
      }

      marker = it->first;
    }

  } while (is_truncated);

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
  if (user_id.compare(RGW_USER_ANON_ID) == 0) {
    set_err_msg(err_msg, "unable to modify anonymous user's info");
    return -EACCES;
  }

  user_info = old_info;

  std::string old_email = old_info.user_email;
  if (!op_email.empty()) {
    // make sure we are not adding a duplicate email
    if (old_email.compare(op_email) != 0) {
      ret = rgw_get_user_info_by_email(store, op_email, duplicate_check);
      if (ret >= 0 && duplicate_check.user_id.compare(user_id) != 0) {
        set_err_msg(err_msg, "cannot add duplicate email");
        return -ERR_EMAIL_EXIST;
      }
    }
    user_info.user_email = op_email;
  } else if (op_email.empty() && op_state.user_email_specified) {

    ldout(store->ctx(), 10) << "removing email index: " << user_info.user_email << dendl;
    ret = rgw_remove_email_index(store, user_info.user_email);
    if (ret < 0 && ret != -ENOENT) {
      ldout(store->ctx(), 0) << "ERROR: could not remove " << user_info.user_id << " index (err=" << ret << ")" << dendl;
      return ret;
    }
    user_info.user_email = "";
  }

  // update the remaining user info
  if (!display_name.empty())
    user_info.display_name = display_name;

  if (op_state.max_buckets_specified)
    user_info.max_buckets = op_state.get_max_buckets();

  if (op_state.admin_specified)
    user_info.admin = op_state.admin;

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

    bool is_truncated = false;
    string marker;
    CephContext *cct = store->ctx();
    size_t max_buckets = cct->_conf->rgw_list_buckets_max_chunk;
    do {
      ret = rgw_read_user_buckets(store, user_id, buckets, marker, string(),
				  max_buckets, false, &is_truncated);
      if (ret < 0) {
        set_err_msg(err_msg, "could not get buckets for uid:  " + user_id.to_str());
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

    } while (is_truncated);
  }

  if (op_state.mfa_ids_specified) {
    user_info.mfa_ids = op_state.mfa_ids;
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

int RGWUser::list(RGWUserAdminOpState& op_state, RGWFormatterFlusher& flusher)
{
  Formatter *formatter = flusher.get_formatter();
  void *handle = nullptr;
  std::string metadata_key = "user";
  if (op_state.max_entries > 1000) {
    op_state.max_entries = 1000;
  }

  int ret = store->meta_mgr->list_keys_init(metadata_key, op_state.marker, &handle);
  if (ret < 0) {
    return ret;
  }

  bool truncated = false;
  uint64_t count = 0;
  uint64_t left = 0;
  flusher.start(0);

  // open the result object section
  formatter->open_object_section("result");

  // open the user id list array section
  formatter->open_array_section("keys");
  do {
    std::list<std::string> keys;
    left = op_state.max_entries - count;
    ret = store->meta_mgr->list_keys_next(handle, left, keys, &truncated);
    if (ret < 0 && ret != -ENOENT) {
      return ret;
    } if (ret != -ENOENT) {
      for (std::list<std::string>::iterator iter = keys.begin(); iter != keys.end(); ++iter) {
      formatter->dump_string("key", *iter);
        ++count;
      }
    }
  } while (truncated && left > 0);
  // close user id list section
  formatter->close_section();

  formatter->dump_bool("truncated", truncated);
  formatter->dump_int("count", count);
  if (truncated) {
    formatter->dump_string("marker", store->meta_mgr->get_marker(handle));
  }

  // close result object section
  formatter->close_section();

  store->meta_mgr->list_keys_complete(handle);

  flusher.flush();
  return 0;
}

int RGWUserAdminOp_User::list(RGWRados *store, RGWUserAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  RGWUser user;

  int ret = user.init_storage(store);
  if (ret < 0)
    return ret;

  ret = user.list(op_state, flusher);
  if (ret < 0)
    return ret;

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
    return -ERR_NO_SUCH_USER;

  Formatter *formatter = flusher.get_formatter();

  ret = user.info(info, NULL);
  if (ret < 0)
    return ret;

  if (op_state.sync_stats) {
    ret = rgw_user_sync_all_stats(store, info.user_id);
    if (ret < 0) {
      return ret;
    }
  }

  RGWStorageStats stats;
  RGWStorageStats *arg_stats = NULL;
  if (op_state.fetch_stats) {
    int ret = store->get_user_stats(info.user_id, stats);
    if (ret < 0 && ret != -ENOENT) {
      return ret;
    }

    arg_stats = &stats;
  }

  if (formatter) {
    flusher.start(0);

    dump_user_info(formatter, info, arg_stats);
    flusher.flush();
  }

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
  if (ret < 0) {
    if (ret == -EEXIST)
      ret = -ERR_USER_EXIST;
    return ret;
  }

  ret = user.info(info, NULL);
  if (ret < 0)
    return ret;

  if (formatter) {
    flusher.start(0);

    dump_user_info(formatter, info);
    flusher.flush();
  }

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
  if (ret < 0) {
    if (ret == -ENOENT)
      ret = -ERR_NO_SUCH_USER;
    return ret;
  }

  ret = user.info(info, NULL);
  if (ret < 0)
    return ret;

  if (formatter) {
    flusher.start(0);

    dump_user_info(formatter, info);
    flusher.flush();
  }

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

  if (ret == -ENOENT)
    ret = -ERR_NO_SUCH_USER;
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

  if (!op_state.has_existing_user())
    return -ERR_NO_SUCH_USER;

  Formatter *formatter = flusher.get_formatter();

  ret = user.subusers.add(op_state, NULL);
  if (ret < 0)
    return ret;

  ret = user.info(info, NULL);
  if (ret < 0)
    return ret;

  if (formatter) {
    flusher.start(0);

    dump_subusers_info(formatter, info);
    flusher.flush();
  }

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

  if (!op_state.has_existing_user())
    return -ERR_NO_SUCH_USER;

  Formatter *formatter = flusher.get_formatter();

  ret = user.subusers.modify(op_state, NULL);
  if (ret < 0)
    return ret;

  ret = user.info(info, NULL);
  if (ret < 0)
    return ret;
 
  if (formatter) {
    flusher.start(0);

    dump_subusers_info(formatter, info);
    flusher.flush();
  }

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


  if (!op_state.has_existing_user())
    return -ERR_NO_SUCH_USER;

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

  if (!op_state.has_existing_user())
    return -ERR_NO_SUCH_USER;

  Formatter *formatter = flusher.get_formatter();

  ret = user.keys.add(op_state, NULL);
  if (ret < 0)
    return ret;

  ret = user.info(info, NULL);
  if (ret < 0)
    return ret;

  if (formatter) {
    flusher.start(0);

    int key_type = op_state.get_key_type();

    if (key_type == KEY_TYPE_SWIFT)
      dump_swift_keys_info(formatter, info);

    else if (key_type == KEY_TYPE_S3)
      dump_access_keys_info(formatter, info);

    flusher.flush();
  }

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

  if (!op_state.has_existing_user())
    return -ERR_NO_SUCH_USER;


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

  if (!op_state.has_existing_user())
    return -ERR_NO_SUCH_USER;

  Formatter *formatter = flusher.get_formatter();

  ret = user.caps.add(op_state, NULL);
  if (ret < 0)
    return ret;

  ret = user.info(info, NULL);
  if (ret < 0)
    return ret;

  if (formatter) {
    flusher.start(0);

    info.caps.dump(formatter);
    flusher.flush();
  }

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

  if (!op_state.has_existing_user())
    return -ERR_NO_SUCH_USER;

  Formatter *formatter = flusher.get_formatter();

  ret = user.caps.remove(op_state, NULL);
  if (ret < 0)
    return ret;

  ret = user.info(info, NULL);
  if (ret < 0)
    return ret;

  if (formatter) {
    flusher.start(0);

    info.caps.dump(formatter);
    flusher.flush();
  }

  return 0;
}

struct RGWUserCompleteInfo {
  RGWUserInfo info;
  map<string, bufferlist> attrs;
  bool has_attrs;

  RGWUserCompleteInfo()
    : has_attrs(false)
  {}

  void dump(Formatter * const f) const {
    info.dump(f);
    encode_json("attrs", attrs, f);
  }

  void decode_json(JSONObj *obj) {
    decode_json_obj(info, obj);
    has_attrs = JSONDecoder::decode_json("attrs", attrs, obj);
  }
};

class RGWUserMetadataObject : public RGWMetadataObject {
  RGWUserCompleteInfo uci;
public:
  RGWUserMetadataObject(const RGWUserCompleteInfo& _uci, obj_version& v, real_time m)
      : uci(_uci) {
    objv = v;
    mtime = m;
  }

  void dump(Formatter *f) const override {
    uci.dump(f);
  }
};

class RGWUserMetadataHandler : public RGWMetadataHandler {
public:
  string get_type() override { return "user"; }

  int get(RGWRados *store, string& entry, RGWMetadataObject **obj) override {
    RGWUserCompleteInfo uci;
    RGWObjVersionTracker objv_tracker;
    real_time mtime;

    rgw_user uid(entry);

    int ret = rgw_get_user_info_by_uid(store, uid, uci.info, &objv_tracker,
                                       &mtime, NULL, &uci.attrs);
    if (ret < 0) {
      return ret;
    }

    RGWUserMetadataObject *mdo = new RGWUserMetadataObject(uci, objv_tracker.read_version, mtime);
    *obj = mdo;

    return 0;
  }

  int put(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker,
          real_time mtime, JSONObj *obj, sync_type_t sync_mode) override {
    RGWUserCompleteInfo uci;

    try {
      decode_json_obj(uci, obj);
    } catch (JSONDecoder::err& e) {
      return -EINVAL;
    }

    map<string, bufferlist> *pattrs = NULL;
    if (uci.has_attrs) {
      pattrs = &uci.attrs;
    }

    rgw_user uid(entry);

    RGWUserInfo old_info;
    real_time orig_mtime;
    int ret = rgw_get_user_info_by_uid(store, uid, old_info, &objv_tracker, &orig_mtime);
    if (ret < 0 && ret != -ENOENT)
      return ret;

    // are we actually going to perform this put, or is it too old?
    if (ret != -ENOENT &&
        !check_versions(objv_tracker.read_version, orig_mtime,
			objv_tracker.write_version, mtime, sync_mode)) {
      return STATUS_NO_APPLY;
    }

    ret = rgw_store_user_info(store, uci.info, &old_info, &objv_tracker, mtime, false, pattrs);
    if (ret < 0) {
      return ret;
    }

    return STATUS_APPLIED;
  }

  struct list_keys_info {
    RGWRados *store;
    RGWListRawObjsCtx ctx;
  };

  int remove(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker) override {
    RGWUserInfo info;

    rgw_user uid(entry);

    int ret = rgw_get_user_info_by_uid(store, uid, info, &objv_tracker);
    if (ret < 0)
      return ret;

    return rgw_delete_user(store, info, objv_tracker);
  }

  void get_pool_and_oid(RGWRados *store, const string& key, rgw_pool& pool, string& oid) override {
    oid = key;
    pool = store->svc.zone->get_zone_params().user_uid_pool;
  }

  int list_keys_init(RGWRados *store, const string& marker, void **phandle) override
  {
    auto info = std::make_unique<list_keys_info>();

    info->store = store;

    int ret = store->list_raw_objects_init(store->svc.zone->get_zone_params().user_uid_pool, marker,
                                           &info->ctx);
    if (ret < 0) {
      return ret;
    }

    *phandle = (void *)info.release();

    return 0;
  }

  int list_keys_next(void *handle, int max, list<string>& keys, bool *truncated) override {
    list_keys_info *info = static_cast<list_keys_info *>(handle);

    string no_filter;

    keys.clear();

    RGWRados *store = info->store;

    list<string> unfiltered_keys;

    int ret = store->list_raw_objects_next(no_filter, max, info->ctx,
                                           unfiltered_keys, truncated);
    if (ret < 0 && ret != -ENOENT)
      return ret;		        
    if (ret == -ENOENT) {
      if (truncated)
        *truncated = false;
      return 0;
    }

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

  void list_keys_complete(void *handle) override {
    list_keys_info *info = static_cast<list_keys_info *>(handle);
    delete info;
  }

  string get_marker(void *handle) override {
    list_keys_info *info = static_cast<list_keys_info *>(handle);
    return info->store->list_raw_objs_get_cursor(info->ctx);
  }
};

void rgw_user_init(RGWRados *store)
{
  uinfo_cache.init(store->svc.cache);

  user_meta_handler = new RGWUserMetadataHandler;
  store->meta_mgr->register_handler(user_meta_handler);
}
