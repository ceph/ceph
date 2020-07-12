// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>

#include <string>
#include <map>
#include <boost/algorithm/string.hpp>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "common/RWLock.h"
#include "rgw_sal.h"
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
#include "services/svc_user.h"
#include "services/svc_meta.h"

#define dout_subsys ceph_subsys_rgw



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

int rgw_user_sync_all_stats(rgw::sal::RGWRadosStore *store, const rgw_user& user_id)
{
  rgw::sal::RGWBucketList user_buckets;
  rgw::sal::RGWRadosUser user(store, user_id);

  CephContext *cct = store->ctx();
  size_t max_entries = cct->_conf->rgw_list_buckets_max_chunk;
  bool is_truncated = false;
  string marker;
  int ret;

  do {
    ret = user.list_buckets(marker, string(), max_entries, false, user_buckets);
    if (ret < 0) {
      ldout(cct, 0) << "failed to read user buckets: ret=" << ret << dendl;
      return ret;
    }
    map<string, rgw::sal::RGWBucket*>& buckets = user_buckets.get_buckets();
    for (map<string, rgw::sal::RGWBucket*>::iterator i = buckets.begin();
         i != buckets.end();
         ++i) {
      marker = i->first;

      rgw::sal::RGWBucket* bucket = i->second;

      ret = bucket->get_bucket_info(null_yield);
      if (ret < 0) {
        ldout(cct, 0) << "ERROR: could not read bucket info: bucket=" << bucket << " ret=" << ret << dendl;
        continue;
      }
      ret = bucket->sync_user_stats();
      if (ret < 0) {
        ldout(cct, 0) << "ERROR: could not sync bucket stats: ret=" << ret << dendl;
        return ret;
      }
      ret = bucket->check_bucket_shards();
      if (ret < 0) {
	ldout(cct, 0) << "ERROR in check_bucket_shards: " << cpp_strerror(-ret)<< dendl;
      }
    }
  } while (is_truncated);

  ret = store->ctl()->user->complete_flush_stats(user.get_user());
  if (ret < 0) {
    cerr << "ERROR: failed to complete syncing user stats: ret=" << ret << std::endl;
    return ret;
  }

  return 0;
}

int rgw_user_get_all_buckets_stats(rgw::sal::RGWRadosStore *store, const rgw_user& user_id, map<string, cls_user_bucket_entry>& buckets_usage_map)
{
  CephContext *cct = store->ctx();
  size_t max_entries = cct->_conf->rgw_list_buckets_max_chunk;
  bool done;
  string marker;
  int ret;

  do {
    rgw::sal::RGWBucketList buckets;
    ret = rgw_read_user_buckets(store, user_id, buckets, marker,
				string(), max_entries, false);
    if (ret < 0) {
      ldout(cct, 0) << "failed to read user buckets: ret=" << ret << dendl;
      return ret;
    }
    std::map<std::string, rgw::sal::RGWBucket*>& m = buckets.get_buckets();
    for (const auto& i :  m) {
      marker = i.first;

      rgw::sal::RGWBucket* bucket_ent = i.second;
      ret = bucket_ent->read_bucket_stats(null_yield);
      if (ret < 0) {
        ldout(cct, 0) << "ERROR: could not get bucket stats: ret=" << ret << dendl;
        return ret;
      }
      cls_user_bucket_entry entry;
      bucket_ent->convert(&entry);
      buckets_usage_map.emplace(bucket_ent->get_name(), entry);
    }
    done = (buckets.count() < max_entries);
  } while (!done);

  return 0;
}

/**
 * Save the given user information to storage.
 * Returns: 0 on success, -ERR# on failure.
 */
int rgw_store_user_info(RGWUserCtl *user_ctl,
                        RGWUserInfo& info,
                        RGWUserInfo *old_info,
                        RGWObjVersionTracker *objv_tracker,
                        real_time mtime,
                        bool exclusive,
                        map<string, bufferlist> *pattrs)
{
  return user_ctl->store_info(info, null_yield,
                              RGWUserCtl::PutParams()
                              .set_old_info(old_info)
                              .set_objv_tracker(objv_tracker)
                              .set_mtime(mtime)
                              .set_exclusive(exclusive)
                              .set_attrs(pattrs));
}

/**
 * Given a uid, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
int rgw_get_user_info_by_uid(RGWUserCtl *user_ctl,
                             const rgw_user& uid,
                             RGWUserInfo& info,
                             RGWObjVersionTracker * const objv_tracker,
                             real_time * const pmtime,
                             rgw_cache_entry_info * const cache_info,
                             map<string, bufferlist> * const pattrs)
{
  return user_ctl->get_info_by_uid(uid, &info, null_yield,
                                   RGWUserCtl::GetParams()
                                   .set_objv_tracker(objv_tracker)
                                   .set_mtime(pmtime)
                                   .set_cache_info(cache_info)
                                   .set_attrs(pattrs));
}

/**
 * Given an email, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
int rgw_get_user_info_by_email(RGWUserCtl *user_ctl, string& email, RGWUserInfo& info,
                               RGWObjVersionTracker *objv_tracker, real_time *pmtime)
{
  return user_ctl->get_info_by_email(email, &info, null_yield,
                                     RGWUserCtl::GetParams()
                                     .set_objv_tracker(objv_tracker)
                                     .set_mtime(pmtime));
}

/**
 * Given an swift username, finds the user_info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_swift(RGWUserCtl *user_ctl,
                                      const string& swift_name,
                                      RGWUserInfo& info,        /* out */
                                      RGWObjVersionTracker * const objv_tracker,
                                      real_time * const pmtime)
{
  return user_ctl->get_info_by_swift(swift_name, &info, null_yield,
                                     RGWUserCtl::GetParams()
                                     .set_objv_tracker(objv_tracker)
                                     .set_mtime(pmtime));
}

/**
 * Given an access key, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_access_key(RGWUserCtl *user_ctl,
                                           const std::string& access_key,
                                           RGWUserInfo& info,
                                           RGWObjVersionTracker* objv_tracker,
                                           real_time *pmtime)
{
  return user_ctl->get_info_by_access_key(access_key, &info, null_yield,
                                          RGWUserCtl::GetParams()
                                          .set_objv_tracker(objv_tracker)
                                          .set_mtime(pmtime));
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
  encode_json("admin", (bool)info.admin, f);
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
  if (!usr) {
    return;
  }

  user = usr;

  store = user->get_store();
  user_ctl = user->get_user_ctl();
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
      if (rgw_get_user_info_by_swift(user_ctl, id, duplicate_check) >= 0) {
        set_err_msg(err_msg, "existing swift key in RGW system:" + id);
        return -ERR_KEY_EXIST;
      }
      break;
    case KEY_TYPE_S3:
      if (rgw_get_user_info_by_access_key(user_ctl, id, duplicate_check) >= 0) {
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

    } while (!rgw_get_user_info_by_access_key(user_ctl, id, duplicate_check));
  }

  if (key_type == KEY_TYPE_SWIFT) {
    id = op_state.build_default_swift_kid();
    if (id.empty()) {
      set_err_msg(err_msg, "empty swift access key");
      return -ERR_INVALID_ACCESS_KEY;
    }

    // check that the access key doesn't exist
    if (rgw_get_user_info_by_swift(user_ctl, id, duplicate_check) >= 0) {
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
    keys_map->erase(kiter);
  }

  // a subuser may have multiple s3 key pairs
  std::string subuser_str = op_state.get_subuser();
  keys_map = access_keys;
  RGWUserInfo user_info = op_state.get_user_info();
  auto user_kiter = user_info.access_keys.begin();
  for (; user_kiter != user_info.access_keys.end(); ++user_kiter) {
    if (user_kiter->second.subuser == subuser_str) {
      kiter = keys_map->find(user_kiter->first);
      if (kiter != keys_map->end()) {
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
  if (!usr) {
    return;
  }

  user = usr;

  subusers_allowed = true;
  store = user->get_store();
  user_ctl = user->get_user_ctl();
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
  if (!usr) {
    return;
  }
  user = usr;
  caps_allowed = true;
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

RGWUser::RGWUser() : caps(this), keys(this), subusers(this)
{
  init_default();
}

int RGWUser::init(rgw::sal::RGWRadosStore *storage, RGWUserAdminOpState& op_state)
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

void RGWUser::init_default()
{
  // use anonymous user info as a placeholder
  rgw_get_anon_user(old_info);
  user_id = RGW_USER_ANON_ID;

  clear_populated();
}

int RGWUser::init_storage(rgw::sal::RGWRadosStore *storage)
{
  if (!storage) {
    return -EINVAL;
  }

  store = storage;
  user_ctl = store->ctl()->user;

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
    found = (rgw_get_user_info_by_uid(user_ctl, user_id, user_info, &op_state.objv) >= 0);
    op_state.found_by_uid = found;
  }
  if (store->ctx()->_conf.get_val<bool>("rgw_user_unique_email")) {
    if (!user_email.empty() && !found) {
      found = (rgw_get_user_info_by_email(user_ctl, user_email, user_info, &op_state.objv) >= 0);
      op_state.found_by_email = found;
    }
  }
  if (!swift_user.empty() && !found) {
    found = (rgw_get_user_info_by_swift(user_ctl, swift_user, user_info, &op_state.objv) >= 0);
    op_state.found_by_key = found;
  }
  if (!access_key.empty() && !found) {
    found = (rgw_get_user_info_by_access_key(user_ctl, access_key, user_info, &op_state.objv) >= 0);
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

  RGWUserInfo *pold_info = (is_populated() ? &old_info : nullptr);

  ret = rgw_store_user_info(user_ctl, user_info, pold_info, &op_state.objv, real_time(), false);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to store user info");
    return ret;
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

// update swift_keys with new user id
static void rename_swift_keys(const rgw_user& user,
                              std::map<std::string, RGWAccessKey>& keys)
{
  std::string user_id;
  user.to_str(user_id);

  auto modify_keys = std::move(keys);
  for ([[maybe_unused]] auto& [k, key] : modify_keys) {
    std::string id = user_id + ":" + key.subuser;
    key.id = id;
    keys[id] = std::move(key);
  }
}

int RGWUser::execute_rename(RGWUserAdminOpState& op_state, std::string *err_msg)
{
  int ret;
  bool populated = op_state.is_populated();

  if (!op_state.has_existing_user() && !populated) {
    set_err_msg(err_msg, "user not found");
    return -ENOENT;
  }

  if (!populated) {
    ret = init(op_state);
    if (ret < 0) {
      set_err_msg(err_msg, "unable to retrieve user info");
      return ret;
    }
  }

  rgw::sal::RGWRadosUser old_user(store, op_state.get_user_info());
  rgw::sal::RGWRadosUser new_user(store, op_state.get_new_uid());
  if (old_user.get_tenant() != new_user.get_tenant()) {
    set_err_msg(err_msg, "users have to be under the same tenant namespace "
                + old_user.get_tenant() + " != " + new_user.get_tenant());
    return -EINVAL;
  }

  // create a stub user and write only the uid index and buckets object
  RGWUserInfo stub_user_info;
  stub_user_info.user_id = new_user.get_user();

  RGWObjVersionTracker objv;
  const bool exclusive = !op_state.get_overwrite_new_user(); // overwrite if requested

  ret = user_ctl->store_info(stub_user_info, null_yield,
                             RGWUserCtl::PutParams()
                             .set_objv_tracker(&objv)
                             .set_exclusive(exclusive));
  if (ret == -EEXIST) {
    set_err_msg(err_msg, "user name given by --new-uid already exists");
    return ret;
  }
  if (ret < 0) {
    set_err_msg(err_msg, "unable to store new user info");
    return ret;
  }

  RGWAccessControlPolicy policy_instance;
  policy_instance.create_default(new_user.get_user(), old_user.get_display_name());

  //unlink and link buckets to new user
  string marker;
  CephContext *cct = store->ctx();
  size_t max_buckets = cct->_conf->rgw_list_buckets_max_chunk;
  rgw::sal::RGWBucketList buckets;

  do {
    ret = old_user.list_buckets(marker, "", max_buckets, false, buckets);
    if (ret < 0) {
      set_err_msg(err_msg, "unable to list user buckets");
      return ret;
    }

    map<std::string, rgw::sal::RGWBucket*>& m = buckets.get_buckets();
    std::map<std::string, rgw::sal::RGWBucket*>::iterator it;

    for (it = m.begin(); it != m.end(); ++it) {
      rgw::sal::RGWBucket* bucket = it->second;
      marker = it->first;

      ret = bucket->get_bucket_info(null_yield);
      if (ret < 0) {
        set_err_msg(err_msg, "failed to fetch bucket info for bucket=" + bucket->get_name());
        return ret;
      }

      ret = bucket->set_acl(policy_instance, null_yield);
      if (ret < 0) {
        set_err_msg(err_msg, "failed to set acl on bucket " + bucket->get_name());
        return ret;
      }

      ret = bucket->link(&new_user, null_yield);
      if (ret < 0) {
        set_err_msg(err_msg, "failed to link bucket " + bucket->get_name());
        return ret;
      }

      ret = bucket->chown(&new_user, &old_user, null_yield);
      if (ret < 0) {
        set_err_msg(err_msg, "failed to run bucket chown" + cpp_strerror(-ret));
        return ret;
      }
    }

  } while (buckets.is_truncated());

  // update the 'stub user' with all of the other fields and rewrite all of the
  // associated index objects
  RGWUserInfo& user_info = op_state.get_user_info();
  user_info.user_id = new_user.get_user();
  op_state.objv = objv;

  rename_swift_keys(new_user.get_user(), user_info.swift_keys);

  return update(op_state, err_msg);
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
    user_info.max_buckets =
      cct->_conf.get_val<int64_t>("rgw_user_max_buckets");
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

  if (op_state.default_placement_specified) {
    user_info.default_placement = op_state.default_placement;
  }

  if (op_state.placement_tags_specified) {
    user_info.placement_tags = op_state.placement_tags;
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

int RGWUser::rename(RGWUserAdminOpState& op_state, std::string *err_msg)
{
  std::string subprocess_msg;
  int ret;

  ret = check_op(op_state, &subprocess_msg);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to parse parameters, " + subprocess_msg);
    return ret;
  }

  ret = execute_rename(op_state, &subprocess_msg);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to rename user, " + subprocess_msg);
    return ret;
  }

  return 0;
}

int RGWUser::execute_remove(RGWUserAdminOpState& op_state, std::string *err_msg, optional_yield y)
{
  int ret;

  bool purge_data = op_state.will_purge_data();
  rgw_user& uid = op_state.get_user_id();
  RGWUserInfo user_info = op_state.get_user_info();

  if (!op_state.has_existing_user()) {
    set_err_msg(err_msg, "user does not exist");
    return -ENOENT;
  }

  rgw::sal::RGWBucketList buckets;
  string marker;
  CephContext *cct = store->ctx();
  size_t max_buckets = cct->_conf->rgw_list_buckets_max_chunk;
  do {
    ret = rgw_read_user_buckets(store, uid, buckets, marker, string(),
				max_buckets, false);
    if (ret < 0) {
      set_err_msg(err_msg, "unable to read user bucket info");
      return ret;
    }

    std::map<std::string, rgw::sal::RGWBucket*>& m = buckets.get_buckets();
    if (!m.empty() && !purge_data) {
      set_err_msg(err_msg, "must specify purge data to remove user with buckets");
      return -EEXIST; // change to code that maps to 409: conflict
    }

    std::map<std::string, rgw::sal::RGWBucket*>::iterator it;
    for (it = m.begin(); it != m.end(); ++it) {
      ret = it->second->remove_bucket(true, y);
      if (ret < 0) {
        set_err_msg(err_msg, "unable to delete user data");
        return ret;
      }

      marker = it->first;
    }

  } while (buckets.is_truncated());

  ret = user_ctl->remove_info(user_info, y, RGWUserCtl::RemoveParams()
                                            .set_objv_tracker(&op_state.objv));
  if (ret < 0) {
    set_err_msg(err_msg, "unable to remove user from RADOS");
    return ret;
  }

  op_state.clear_populated();
  clear_populated();

  return 0;
}

int RGWUser::remove(RGWUserAdminOpState& op_state, optional_yield y, std::string *err_msg)
{
  std::string subprocess_msg;
  int ret;

  ret = check_op(op_state, &subprocess_msg);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to parse parameters, " + subprocess_msg);
    return ret;
  }

  ret = execute_remove(op_state, &subprocess_msg, y);
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
    if (old_email != op_email) {
      ret = rgw_get_user_info_by_email(user_ctl, op_email, duplicate_check);
      if (ret >= 0 && duplicate_check.user_id.compare(user_id) != 0) {
        set_err_msg(err_msg, "cannot add duplicate email");
        return -ERR_EMAIL_EXIST;
      }
    }
    user_info.user_email = op_email;
  } else if (op_email.empty() && op_state.user_email_specified) {
    ldout(store->ctx(), 10) << "removing email index: " << user_info.user_email << dendl;
    /* will be physically removed later when calling update() */
    user_info.user_email.clear();
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

    rgw::sal::RGWBucketList buckets;

    if (user_id.empty()) {
      set_err_msg(err_msg, "empty user id passed...aborting");
      return -EINVAL;
    }

    string marker;
    CephContext *cct = store->ctx();
    size_t max_buckets = cct->_conf->rgw_list_buckets_max_chunk;
    do {
      ret = rgw_read_user_buckets(store, user_id, buckets, marker, string(),
				  max_buckets, false);
      if (ret < 0) {
        set_err_msg(err_msg, "could not get buckets for uid:  " + user_id.to_str());
        return ret;
      }

      std::map<std::string, rgw::sal::RGWBucket*>& m = buckets.get_buckets();
      std::map<std::string, rgw::sal::RGWBucket*>::iterator iter;

      vector<rgw_bucket> bucket_names;
      for (iter = m.begin(); iter != m.end(); ++iter) {
	rgw::sal::RGWBucket* obj = iter->second;
        bucket_names.push_back(obj->get_bi());

        marker = iter->first;
      }

      ret = store->getRados()->set_buckets_enabled(bucket_names, !suspended);
      if (ret < 0) {
        set_err_msg(err_msg, "failed to modify bucket");
        return ret;
      }

    } while (buckets.is_truncated());
  }

  if (op_state.mfa_ids_specified) {
    user_info.mfa_ids = op_state.mfa_ids;
  }

  if (op_state.default_placement_specified) {
    user_info.default_placement = op_state.default_placement;
  }

  if (op_state.placement_tags_specified) {
    user_info.placement_tags = op_state.placement_tags;
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

  auto meta_mgr = store->ctl()->meta.mgr;

  int ret = meta_mgr->list_keys_init(metadata_key, op_state.marker, &handle);
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
    ret = meta_mgr->list_keys_next(handle, left, keys, &truncated);
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
    formatter->dump_string("marker", meta_mgr->get_marker(handle));
  }

  // close result object section
  formatter->close_section();

  meta_mgr->list_keys_complete(handle);

  flusher.flush();
  return 0;
}

int RGWUserAdminOp_User::list(rgw::sal::RGWRadosStore *store, RGWUserAdminOpState& op_state,
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

int RGWUserAdminOp_User::info(rgw::sal::RGWRadosStore *store, RGWUserAdminOpState& op_state,
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
    int ret = store->ctl()->user->read_stats(info.user_id, &stats);
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

int RGWUserAdminOp_User::create(rgw::sal::RGWRadosStore *store, RGWUserAdminOpState& op_state,
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

int RGWUserAdminOp_User::modify(rgw::sal::RGWRadosStore *store, RGWUserAdminOpState& op_state,
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

int RGWUserAdminOp_User::remove(rgw::sal::RGWRadosStore *store, RGWUserAdminOpState& op_state,
                  RGWFormatterFlusher& flusher, optional_yield y)
{
  RGWUserInfo info;
  RGWUser user;
  int ret = user.init(store, op_state);
  if (ret < 0)
    return ret;


  ret = user.remove(op_state, y, NULL);

  if (ret == -ENOENT)
    ret = -ERR_NO_SUCH_USER;
  return ret;
}

int RGWUserAdminOp_Subuser::create(rgw::sal::RGWRadosStore *store, RGWUserAdminOpState& op_state,
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

int RGWUserAdminOp_Subuser::modify(rgw::sal::RGWRadosStore *store, RGWUserAdminOpState& op_state,
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

int RGWUserAdminOp_Subuser::remove(rgw::sal::RGWRadosStore *store, RGWUserAdminOpState& op_state,
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

int RGWUserAdminOp_Key::create(rgw::sal::RGWRadosStore *store, RGWUserAdminOpState& op_state,
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

int RGWUserAdminOp_Key::remove(rgw::sal::RGWRadosStore *store, RGWUserAdminOpState& op_state,
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

int RGWUserAdminOp_Caps::add(rgw::sal::RGWRadosStore *store, RGWUserAdminOpState& op_state,
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


int RGWUserAdminOp_Caps::remove(rgw::sal::RGWRadosStore *store, RGWUserAdminOpState& op_state,
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

class RGWUserMetadataHandler : public RGWMetadataHandler_GenericMetaBE {
public:
  struct Svc {
    RGWSI_User *user{nullptr};
  } svc;

  RGWUserMetadataHandler(RGWSI_User *user_svc) {
    base_init(user_svc->ctx(), user_svc->get_be_handler());
    svc.user = user_svc;
  }

  string get_type() override { return "user"; }

  int do_get(RGWSI_MetaBackend_Handler::Op *op, string& entry, RGWMetadataObject **obj, optional_yield y) override {
    RGWUserCompleteInfo uci;
    RGWObjVersionTracker objv_tracker;
    real_time mtime;

    rgw_user user = RGWSI_User::user_from_meta_key(entry);

    int ret = svc.user->read_user_info(op->ctx(), user, &uci.info, &objv_tracker,
                                       &mtime, nullptr, &uci.attrs,
                                       y);
    if (ret < 0) {
      return ret;
    }

    RGWUserMetadataObject *mdo = new RGWUserMetadataObject(uci, objv_tracker.read_version, mtime);
    *obj = mdo;

    return 0;
  }

  RGWMetadataObject *get_meta_obj(JSONObj *jo, const obj_version& objv, const ceph::real_time& mtime) override {
    RGWUserCompleteInfo uci;

    try {
      decode_json_obj(uci, jo);
    } catch (JSONDecoder::err& e) {
      return nullptr;
    }

    return new RGWUserMetadataObject(uci, objv, mtime);
  }

  int do_put(RGWSI_MetaBackend_Handler::Op *op, string& entry,
             RGWMetadataObject *obj,
             RGWObjVersionTracker& objv_tracker,
             optional_yield y,
             RGWMDLogSyncType type) override;

  int do_remove(RGWSI_MetaBackend_Handler::Op *op, string& entry, RGWObjVersionTracker& objv_tracker,
                optional_yield y) override {
    RGWUserInfo info;

    rgw_user user = RGWSI_User::user_from_meta_key(entry);

    int ret = svc.user->read_user_info(op->ctx(), user, &info, nullptr,
                                       nullptr, nullptr, nullptr,
                                       y);
    if (ret < 0) {
      return ret;
    }

    return svc.user->remove_user_info(op->ctx(), info, &objv_tracker,
                                      y);
  }
};

class RGWMetadataHandlerPut_User : public RGWMetadataHandlerPut_SObj
{
  RGWUserMetadataHandler *uhandler;
  RGWUserMetadataObject *uobj;
public:
  RGWMetadataHandlerPut_User(RGWUserMetadataHandler *_handler,
                             RGWSI_MetaBackend_Handler::Op *op, string& entry,
                             RGWMetadataObject *obj, RGWObjVersionTracker& objv_tracker,
                             optional_yield y,
                             RGWMDLogSyncType type) : RGWMetadataHandlerPut_SObj(_handler, op, entry, obj, objv_tracker, y, type),
                                                                uhandler(_handler) {
    uobj = static_cast<RGWUserMetadataObject *>(obj);
  }

  int put_checked() override;
};

int RGWUserMetadataHandler::do_put(RGWSI_MetaBackend_Handler::Op *op, string& entry,
                                   RGWMetadataObject *obj,
                                   RGWObjVersionTracker& objv_tracker,
                                   optional_yield y,
                                   RGWMDLogSyncType type)
{
  RGWMetadataHandlerPut_User put_op(this, op, entry, obj, objv_tracker, y, type);
  return do_put_operate(&put_op);
}

int RGWMetadataHandlerPut_User::put_checked()
{
  RGWUserMetadataObject *orig_obj = static_cast<RGWUserMetadataObject *>(old_obj);
  RGWUserCompleteInfo& uci = uobj->get_uci();

  map<string, bufferlist> *pattrs{nullptr};
  if (uci.has_attrs) {
    pattrs = &uci.attrs;
  }

  RGWUserInfo *pold_info = (orig_obj ? &orig_obj->get_uci().info : nullptr);

  auto mtime = obj->get_mtime();

  int ret = uhandler->svc.user->store_user_info(op->ctx(), uci.info, pold_info,
                                               &objv_tracker, mtime,
                                               false, pattrs, y);
  if (ret < 0) {
    return ret;
  }

  return STATUS_APPLIED;
}


RGWUserCtl::RGWUserCtl(RGWSI_Zone *zone_svc,
                       RGWSI_User *user_svc,
                       RGWUserMetadataHandler *_umhandler) : umhandler(_umhandler) {
  svc.zone = zone_svc;
  svc.user = user_svc;
  be_handler = umhandler->get_be_handler();
}

template <class T>
class optional_default
{
  const std::optional<T>& opt;
  std::optional<T> def;
  const T *p;
public:
  optional_default(const std::optional<T>& _o) : opt(_o) {
    if (opt) {
      p = &(*opt);
    } else {
      def = T();
      p = &(*def);
    }
  }

  const T *operator->() {
    return p;
  }

  const T& operator*() {
    return *p;
  }
};

int RGWUserCtl::get_info_by_uid(const rgw_user& uid,
                                RGWUserInfo *info,
                                optional_yield y,
                                const GetParams& params)

{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.user->read_user_info(op->ctx(),
                                    uid,
                                    info,
                                    params.objv_tracker,
                                    params.mtime,
                                    params.cache_info,
                                    params.attrs,
                                    y);
  });
}

int RGWUserCtl::get_info_by_email(const string& email,
                                  RGWUserInfo *info,
                                  optional_yield y,
                                  const GetParams& params)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.user->get_user_info_by_email(op->ctx(), email,
                                            info,
                                            params.objv_tracker,
                                            params.mtime,
                                            y);
  });
}

int RGWUserCtl::get_info_by_swift(const string& swift_name,
                                  RGWUserInfo *info,
                                  optional_yield y,
                                  const GetParams& params)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.user->get_user_info_by_swift(op->ctx(), swift_name,
                                            info,
                                            params.objv_tracker,
                                            params.mtime,
                                            y);
  });
}

int RGWUserCtl::get_info_by_access_key(const string& access_key,
                                       RGWUserInfo *info,
                                       optional_yield y,
                                       const GetParams& params)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.user->get_user_info_by_access_key(op->ctx(), access_key,
                                                 info,
                                                 params.objv_tracker,
                                                 params.mtime,
                                                 y);
  });
}

int RGWUserCtl::get_attrs_by_uid(const rgw_user& user_id,
                                 map<string, bufferlist> *pattrs,
                                 optional_yield y,
                                 RGWObjVersionTracker *objv_tracker)
{
  RGWUserInfo user_info;

  return get_info_by_uid(user_id, &user_info, y, RGWUserCtl::GetParams()
                         .set_attrs(pattrs)
                         .set_objv_tracker(objv_tracker));
}

int RGWUserCtl::store_info(const RGWUserInfo& info, optional_yield y,
                           const PutParams& params)
{
  string key = RGWSI_User::get_meta_key(info.user_id);

  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.user->store_user_info(op->ctx(), info,
                                     params.old_info,
                                     params.objv_tracker,
                                     params.mtime,
                                     params.exclusive,
                                     params.attrs,
                                     y);
  });
}

int RGWUserCtl::remove_info(const RGWUserInfo& info, optional_yield y,
                            const RemoveParams& params)

{
  string key = RGWSI_User::get_meta_key(info.user_id);

  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.user->remove_user_info(op->ctx(), info,
                                      params.objv_tracker,
                                      y);
  });
}

int RGWUserCtl::add_bucket(const rgw_user& user,
                           const rgw_bucket& bucket,
                           ceph::real_time creation_time)

{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.user->add_bucket(op->ctx(), user, bucket, creation_time);
  });
}

int RGWUserCtl::remove_bucket(const rgw_user& user,
                              const rgw_bucket& bucket)

{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.user->remove_bucket(op->ctx(), user, bucket);
  });
}

int RGWUserCtl::list_buckets(const rgw_user& user,
                             const string& marker,
                             const string& end_marker,
                             uint64_t max,
                             bool need_stats,
                             RGWUserBuckets *buckets,
                             bool *is_truncated,
                             uint64_t default_max)
{
  if (!max) {
    max = default_max;
  }

  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    int ret = svc.user->list_buckets(op->ctx(), user, marker, end_marker,
                                     max, buckets, is_truncated);
    if (ret < 0) {
      return ret;
    }
    if (need_stats) {
      map<string, RGWBucketEnt>& m = buckets->get_buckets();
      ret = ctl.bucket->read_buckets_stats(m, null_yield);
      if (ret < 0 && ret != -ENOENT) {
        ldout(svc.user->ctx(), 0) << "ERROR: could not get stats for buckets" << dendl;
        return ret;
      }
    }
    return 0;
  });
}

int RGWUserCtl::flush_bucket_stats(const rgw_user& user,
                                   const RGWBucketEnt& ent)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.user->flush_bucket_stats(op->ctx(), user, ent);
  });
}

int RGWUserCtl::complete_flush_stats(const rgw_user& user)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.user->complete_flush_stats(op->ctx(), user);
  });
}

int RGWUserCtl::reset_stats(const rgw_user& user)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.user->reset_bucket_stats(op->ctx(), user);
  });
}

int RGWUserCtl::read_stats(const rgw_user& user, RGWStorageStats *stats,
			   ceph::real_time *last_stats_sync,
			   ceph::real_time *last_stats_update)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.user->read_stats(op->ctx(), user, stats,
				last_stats_sync, last_stats_update);
  });
}

int RGWUserCtl::read_stats_async(const rgw_user& user, RGWGetUserStats_CB *cb)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return svc.user->read_stats_async(op->ctx(), user, cb);
  });
}

RGWMetadataHandler *RGWUserMetaHandlerAllocator::alloc(RGWSI_User *user_svc) {
  return new RGWUserMetadataHandler(user_svc);
}

