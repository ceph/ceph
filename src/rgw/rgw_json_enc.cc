// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_zone.h"
#include "rgw_log.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_cache.h"
#include "rgw_bucket.h"
#include "rgw_keystone.h"
#include "rgw_basic_types.h"
#include "rgw_op.h"
#include "rgw_data_sync.h"
#include "rgw_sync.h"
#include "rgw_orphan.h"
#include "rgw_bucket_sync.h"
#include "rgw_tools.h"

#include "common/ceph_json.h"
#include "common/Formatter.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

void encode_json(const char *name, const obj_version& v, Formatter *f)
{
  f->open_object_section(name);
  f->dump_string("tag", v.tag);
  f->dump_unsigned("ver", v.ver);
  f->close_section();
}

void decode_json_obj(obj_version& v, JSONObj *obj)
{
  JSONDecoder::decode_json("tag", v.tag, obj);
  JSONDecoder::decode_json("ver", v.ver, obj);
}

void encode_json(const char *name, const RGWUserCaps& val, Formatter *f)
{
  val.dump(f, name);
}


void encode_json(const char *name, const rgw_pool& pool, Formatter *f)
{
  f->dump_string(name, pool.to_str());
}

void decode_json_obj(rgw_pool& pool, JSONObj *obj)
{
  string s;
  decode_json_obj(s, obj);
  pool = rgw_pool(s);
}

void encode_json(const char *name, const rgw_placement_rule& r, Formatter *f)
{
  encode_json(name, r.to_str(), f);
}

void decode_json_obj(rgw_placement_rule& v, JSONObj *obj)
{
  string s;
  decode_json_obj(s, obj);
  v.from_str(s);
}

void RGWOLHInfo::dump(Formatter *f) const
{
  encode_json("target", target, f);
}

void RGWOLHPendingInfo::dump(Formatter *f) const
{
  utime_t ut(time);
  encode_json("time", ut, f);
}

void RGWObjManifestPart::dump(Formatter *f) const
{
  f->open_object_section("loc");
  loc.dump(f);
  f->close_section();
  f->dump_unsigned("loc_ofs", loc_ofs);
  f->dump_unsigned("size", size);
}

void RGWObjManifestRule::dump(Formatter *f) const
{
  encode_json("start_part_num", start_part_num, f);
  encode_json("start_ofs", start_ofs, f);
  encode_json("part_size", part_size, f);
  encode_json("stripe_max_size", stripe_max_size, f);
  encode_json("override_prefix", override_prefix, f);
}

void rgw_bucket_placement::dump(Formatter *f) const
{
  encode_json("bucket", bucket, f);
  encode_json("placement_rule", placement_rule, f);
}

void rgw_obj_select::dump(Formatter *f) const
{
  f->dump_string("placement_rule", placement_rule.to_str());
  f->dump_object("obj", obj);
  f->dump_object("raw_obj", raw_obj);
  f->dump_bool("is_raw", is_raw);
}

void RGWObjManifest::obj_iterator::dump(Formatter *f) const
{
  f->dump_unsigned("part_ofs", part_ofs);
  f->dump_unsigned("stripe_ofs", stripe_ofs);
  f->dump_unsigned("ofs", ofs);
  f->dump_unsigned("stripe_size", stripe_size);
  f->dump_int("cur_part_id", cur_part_id);
  f->dump_int("cur_stripe", cur_stripe);
  f->dump_string("cur_override_prefix", cur_override_prefix);
  f->dump_object("location", location);
}

void RGWObjManifest::dump(Formatter *f) const
{
  map<uint64_t, RGWObjManifestPart>::const_iterator iter = objs.begin();
  f->open_array_section("objs");
  for (; iter != objs.end(); ++iter) {
    f->dump_unsigned("ofs", iter->first);
    f->open_object_section("part");
    iter->second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->dump_unsigned("obj_size", obj_size);
  ::encode_json("explicit_objs", explicit_objs, f);
  ::encode_json("head_size", head_size, f);
  ::encode_json("max_head_size", max_head_size, f);
  ::encode_json("prefix", prefix, f);
  ::encode_json("rules", rules, f);
  ::encode_json("tail_instance", tail_instance, f);
  ::encode_json("tail_placement", tail_placement, f);

  f->dump_object("begin_iter", begin_iter);
  f->dump_object("end_iter", end_iter);
}

void rgw_log_entry::dump(Formatter *f) const
{
  f->dump_string("object_owner", object_owner.to_str());
  f->dump_string("bucket_owner", bucket_owner.to_str());
  f->dump_string("bucket", bucket);
  f->dump_stream("time") << time;
  f->dump_string("remote_addr", remote_addr);
  f->dump_string("user", user);
  stringstream s;
  s << obj;
  f->dump_string("obj", s.str());
  f->dump_string("op", op);
  f->dump_string("uri", uri);
  f->dump_string("http_status", http_status);
  f->dump_string("error_code", error_code);
  f->dump_unsigned("bytes_sent", bytes_sent);
  f->dump_unsigned("bytes_received", bytes_received);
  f->dump_unsigned("obj_size", obj_size);
  f->dump_stream("total_time") << total_time;
  f->dump_string("user_agent", user_agent);
  f->dump_string("referrer", referrer);
  f->dump_string("bucket_id", bucket_id);
  f->dump_string("trans_id", trans_id);
}

void ACLPermission::dump(Formatter *f) const
{
  f->dump_int("flags", flags);
}

void ACLGranteeType::dump(Formatter *f) const
{
  f->dump_unsigned("type", type);
}

void ACLGrant::dump(Formatter *f) const
{
  f->open_object_section("type");
  type.dump(f);
  f->close_section();

  f->dump_string("id", id.to_str());
  f->dump_string("email", email);

  f->open_object_section("permission");
  permission.dump(f);
  f->close_section();

  f->dump_string("name", name);
  f->dump_int("group", (int)group);
  f->dump_string("url_spec", url_spec);
}

void RGWAccessControlList::dump(Formatter *f) const
{
  map<string, int>::const_iterator acl_user_iter = acl_user_map.begin();
  f->open_array_section("acl_user_map");
  for (; acl_user_iter != acl_user_map.end(); ++acl_user_iter) {
    f->open_object_section("entry");
    f->dump_string("user", acl_user_iter->first);
    f->dump_int("acl", acl_user_iter->second);
    f->close_section();
  }
  f->close_section();

  map<uint32_t, int>::const_iterator acl_group_iter = acl_group_map.begin();
  f->open_array_section("acl_group_map");
  for (; acl_group_iter != acl_group_map.end(); ++acl_group_iter) {
    f->open_object_section("entry");
    f->dump_unsigned("group", acl_group_iter->first);
    f->dump_int("acl", acl_group_iter->second);
    f->close_section();
  }
  f->close_section();

  multimap<string, ACLGrant>::const_iterator giter = grant_map.begin();
  f->open_array_section("grant_map");
  for (; giter != grant_map.end(); ++giter) {
    f->open_object_section("entry");
    f->dump_string("id", giter->first);
    f->open_object_section("grant");
    giter->second.dump(f);
    f->close_section();
    f->close_section();
  }
  f->close_section();
}

void ACLOwner::dump(Formatter *f) const
{
  encode_json("id", id.to_str(), f);
  encode_json("display_name", display_name, f);
}

void ACLOwner::decode_json(JSONObj *obj) {
  string id_str;
  JSONDecoder::decode_json("id", id_str, obj);
  id.from_str(id_str);
  JSONDecoder::decode_json("display_name", display_name, obj);
}

void RGWAccessControlPolicy::dump(Formatter *f) const
{
  encode_json("acl", acl, f);
  encode_json("owner", owner, f);
}

void ObjectMetaInfo::dump(Formatter *f) const
{
  encode_json("size", size, f);
  encode_json("mtime", utime_t(mtime), f);
}

void ObjectCacheInfo::dump(Formatter *f) const
{
  encode_json("status", status, f);
  encode_json("flags", flags, f);
  encode_json("data", data, f);
  encode_json_map("xattrs", "name", "value", "length", xattrs, f);
  encode_json_map("rm_xattrs", "name", "value", "length", rm_xattrs, f);
  encode_json("meta", meta, f);

}

void RGWCacheNotifyInfo::dump(Formatter *f) const
{
  encode_json("op", op, f);
  encode_json("obj", obj, f);
  encode_json("obj_info", obj_info, f);
  encode_json("ofs", ofs, f);
  encode_json("ns", ns, f);
}

void RGWAccessKey::dump(Formatter *f) const
{
  encode_json("access_key", id, f);
  encode_json("secret_key", key, f);
  encode_json("subuser", subuser, f);
}

void RGWAccessKey::dump_plain(Formatter *f) const
{
  encode_json("access_key", id, f);
  encode_json("secret_key", key, f);
}

void encode_json_plain(const char *name, const RGWAccessKey& val, Formatter *f)
{
  f->open_object_section(name);
  val.dump_plain(f);
  f->close_section();
}

void RGWAccessKey::dump(Formatter *f, const string& user, bool swift) const
{
  string u = user;
  if (!subuser.empty()) {
    u.append(":");
    u.append(subuser);
  }
  encode_json("user", u, f);
  if (!swift) {
    encode_json("access_key", id, f);
  }
  encode_json("secret_key", key, f);
}

void RGWAccessKey::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("access_key", id, obj, true);
  JSONDecoder::decode_json("secret_key", key, obj, true);
  if (!JSONDecoder::decode_json("subuser", subuser, obj)) {
    string user;
    JSONDecoder::decode_json("user", user, obj);
    int pos = user.find(':');
    if (pos >= 0) {
      subuser = user.substr(pos + 1);
    }
  }
}

void RGWAccessKey::decode_json(JSONObj *obj, bool swift) {
  if (!swift) {
    decode_json(obj);
    return;
  }

  if (!JSONDecoder::decode_json("subuser", subuser, obj)) {
    JSONDecoder::decode_json("user", id, obj, true);
    int pos = id.find(':');
    if (pos >= 0) {
      subuser = id.substr(pos + 1);
    }
  }
  JSONDecoder::decode_json("secret_key", key, obj, true);
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

template <class T>
static void mask_to_str(T *mask_list, uint32_t mask, char *buf, int len)
{
  const char *sep = "";
  int pos = 0;
  if (!mask) {
    snprintf(buf, len, "<none>");
    return;
  }
  while (mask) {
    uint32_t orig_mask = mask;
    for (int i = 0; mask_list[i].mask; i++) {
      T *desc = &mask_list[i];
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

static void perm_to_str(uint32_t mask, char *buf, int len)
{
  return mask_to_str(rgw_perms, mask, buf, len);
}

static struct rgw_flags_desc op_type_flags[] = {
 { RGW_OP_TYPE_READ, "read" },
 { RGW_OP_TYPE_WRITE, "write" },
 { RGW_OP_TYPE_DELETE, "delete" },
 { 0, NULL }
};

extern void op_type_to_str(uint32_t mask, char *buf, int len)
{
  return mask_to_str(op_type_flags, mask, buf, len);
}

void RGWSubUser::dump(Formatter *f) const
{
  encode_json("id", name, f);
  char buf[256];
  perm_to_str(perm_mask, buf, sizeof(buf));
  encode_json("permissions", (const char *)buf, f);
}

void RGWSubUser::dump(Formatter *f, const string& user) const
{
  string s = user;
  s.append(":");
  s.append(name);
  encode_json("id", s, f);
  char buf[256];
  perm_to_str(perm_mask, buf, sizeof(buf));
  encode_json("permissions", (const char *)buf, f);
}

static uint32_t str_to_perm(const string& s)
{
  if (s.compare("read") == 0)
    return RGW_PERM_READ;
  else if (s.compare("write") == 0)
    return RGW_PERM_WRITE;
  else if (s.compare("read-write") == 0)
    return RGW_PERM_READ | RGW_PERM_WRITE;
  else if (s.compare("full-control") == 0)
    return RGW_PERM_FULL_CONTROL;
  return 0;
}

void RGWSubUser::decode_json(JSONObj *obj)
{
  string uid;
  JSONDecoder::decode_json("id", uid, obj);
  int pos = uid.find(':');
  if (pos >= 0)
    name = uid.substr(pos + 1);
  string perm_str;
  JSONDecoder::decode_json("permissions", perm_str, obj);
  perm_mask = str_to_perm(perm_str);
}

static void user_info_dump_subuser(const char *name, const RGWSubUser& subuser, Formatter *f, void *parent)
{
  RGWUserInfo *info = static_cast<RGWUserInfo *>(parent);
  subuser.dump(f, info->user_id.to_str());
}

static void user_info_dump_key(const char *name, const RGWAccessKey& key, Formatter *f, void *parent)
{
  RGWUserInfo *info = static_cast<RGWUserInfo *>(parent);
  key.dump(f, info->user_id.to_str(), false);
}

static void user_info_dump_swift_key(const char *name, const RGWAccessKey& key, Formatter *f, void *parent)
{
  RGWUserInfo *info = static_cast<RGWUserInfo *>(parent);
  key.dump(f, info->user_id.to_str(), true);
}

void RGWUserInfo::dump(Formatter *f) const
{

  encode_json("user_id", user_id.to_str(), f);
  encode_json("display_name", display_name, f);
  encode_json("email", user_email, f);
  encode_json("suspended", (int)suspended, f);
  encode_json("max_buckets", (int)max_buckets, f);

  encode_json_map("subusers", NULL, "subuser", NULL, user_info_dump_subuser,(void *)this, subusers, f);
  encode_json_map("keys", NULL, "key", NULL, user_info_dump_key,(void *)this, access_keys, f);
  encode_json_map("swift_keys", NULL, "key", NULL, user_info_dump_swift_key,(void *)this, swift_keys, f);

  encode_json("caps", caps, f);

  char buf[256];
  op_type_to_str(op_mask, buf, sizeof(buf));
  encode_json("op_mask", (const char *)buf, f);

  if (system) { /* no need to show it for every user */
    encode_json("system", (bool)system, f);
  }
  if (admin) {
    encode_json("admin", (bool)admin, f);
  }
  encode_json("default_placement", default_placement.name, f);
  encode_json("default_storage_class", default_placement.storage_class, f);
  encode_json("placement_tags", placement_tags, f);
  encode_json("bucket_quota", bucket_quota, f);
  encode_json("user_quota", user_quota, f);
  encode_json("temp_url_keys", temp_url_keys, f);

  string user_source_type;
  switch ((RGWIdentityType)type) {
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
  encode_json("mfa_ids", mfa_ids, f);
}


static void decode_access_keys(map<string, RGWAccessKey>& m, JSONObj *o)
{
  RGWAccessKey k;
  k.decode_json(o);
  m[k.id] = k;
}

static void decode_swift_keys(map<string, RGWAccessKey>& m, JSONObj *o)
{
  RGWAccessKey k;
  k.decode_json(o, true);
  m[k.id] = k;
}

static void decode_subusers(map<string, RGWSubUser>& m, JSONObj *o)
{
  RGWSubUser u;
  u.decode_json(o);
  m[u.name] = u;
}

void RGWUserInfo::decode_json(JSONObj *obj)
{
  string uid;

  JSONDecoder::decode_json("user_id", uid, obj, true);
  user_id.from_str(uid);

  JSONDecoder::decode_json("display_name", display_name, obj);
  JSONDecoder::decode_json("email", user_email, obj);
  bool susp = false;
  JSONDecoder::decode_json("suspended", susp, obj);
  suspended = (__u8)susp;
  JSONDecoder::decode_json("max_buckets", max_buckets, obj);

  JSONDecoder::decode_json("keys", access_keys, decode_access_keys, obj);
  JSONDecoder::decode_json("swift_keys", swift_keys, decode_swift_keys, obj);
  JSONDecoder::decode_json("subusers", subusers, decode_subusers, obj);

  JSONDecoder::decode_json("caps", caps, obj);

  string mask_str;
  JSONDecoder::decode_json("op_mask", mask_str, obj);
  rgw_parse_op_type_list(mask_str, &op_mask);

  bool sys = false;
  JSONDecoder::decode_json("system", sys, obj);
  system = (__u8)sys;
  bool ad = false;
  JSONDecoder::decode_json("admin", ad, obj);
  admin = (__u8)ad;
  JSONDecoder::decode_json("default_placement", default_placement.name, obj);
  JSONDecoder::decode_json("default_storage_class", default_placement.storage_class, obj);
  JSONDecoder::decode_json("placement_tags", placement_tags, obj);
  JSONDecoder::decode_json("bucket_quota", bucket_quota, obj);
  JSONDecoder::decode_json("user_quota", user_quota, obj);
  JSONDecoder::decode_json("temp_url_keys", temp_url_keys, obj);

  string user_source_type;
  JSONDecoder::decode_json("type", user_source_type, obj);
  if (user_source_type == "rgw") {
    type = TYPE_RGW;
  } else if (user_source_type == "keystone") {
    type = TYPE_KEYSTONE;
  } else if (user_source_type == "ldap") {
    type = TYPE_LDAP;
  } else if (user_source_type == "none") {
    type = TYPE_NONE;
  }
  JSONDecoder::decode_json("mfa_ids", mfa_ids, obj);
}

void RGWQuotaInfo::dump(Formatter *f) const
{
  f->dump_bool("enabled", enabled);
  f->dump_bool("check_on_raw", check_on_raw);

  f->dump_int("max_size", max_size);
  f->dump_int("max_size_kb", rgw_rounded_kb(max_size));
  f->dump_int("max_objects", max_objects);
}

void RGWQuotaInfo::decode_json(JSONObj *obj)
{
  if (false == JSONDecoder::decode_json("max_size", max_size, obj)) {
    /* We're parsing an older version of the struct. */
    int64_t max_size_kb = 0;

    JSONDecoder::decode_json("max_size_kb", max_size_kb, obj);
    max_size = max_size_kb * 1024;
  }
  JSONDecoder::decode_json("max_objects", max_objects, obj);

  JSONDecoder::decode_json("check_on_raw", check_on_raw, obj);
  JSONDecoder::decode_json("enabled", enabled, obj);
}

void rgw_data_placement_target::dump(Formatter *f) const
{
  encode_json("data_pool", data_pool, f);
  encode_json("data_extra_pool", data_extra_pool, f);
  encode_json("index_pool", index_pool, f);
}
  
void rgw_data_placement_target::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("data_pool", data_pool, obj);
  JSONDecoder::decode_json("data_extra_pool", data_extra_pool, obj);
  JSONDecoder::decode_json("index_pool", index_pool, obj);
}

void rgw_bucket::dump(Formatter *f) const
{
  encode_json("name", name, f);
  encode_json("marker", marker, f);
  encode_json("bucket_id", bucket_id, f);
  encode_json("tenant", tenant, f);
  encode_json("explicit_placement", explicit_placement, f);
}

void rgw_bucket::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("marker", marker, obj);
  JSONDecoder::decode_json("bucket_id", bucket_id, obj);
  JSONDecoder::decode_json("tenant", tenant, obj);
  JSONDecoder::decode_json("explicit_placement", explicit_placement, obj);
  if (explicit_placement.data_pool.empty()) {
    /* decoding old format */
    JSONDecoder::decode_json("pool", explicit_placement.data_pool, obj);
    JSONDecoder::decode_json("data_extra_pool", explicit_placement.data_extra_pool, obj);
    JSONDecoder::decode_json("index_pool", explicit_placement.index_pool, obj);
  }
}

void RGWBucketEntryPoint::dump(Formatter *f) const
{
  encode_json("bucket", bucket, f);
  encode_json("owner", owner, f);
  utime_t ut(creation_time);
  encode_json("creation_time", ut, f);
  encode_json("linked", linked, f);
  encode_json("has_bucket_info", has_bucket_info, f);
  if (has_bucket_info) {
    encode_json("old_bucket_info", old_bucket_info, f);
  }
}

void RGWBucketEntryPoint::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("bucket", bucket, obj);
  JSONDecoder::decode_json("owner", owner, obj);
  utime_t ut;
  JSONDecoder::decode_json("creation_time", ut, obj);
  creation_time = ut.to_real_time();
  JSONDecoder::decode_json("linked", linked, obj);
  JSONDecoder::decode_json("has_bucket_info", has_bucket_info, obj);
  if (has_bucket_info) {
    JSONDecoder::decode_json("old_bucket_info", old_bucket_info, obj);
  }
}

void RGWStorageStats::dump(Formatter *f) const
{
  encode_json("size", size, f);
  encode_json("size_actual", size_rounded, f);
  encode_json("size_utilized", size_utilized, f);
  encode_json("size_kb", rgw_rounded_kb(size), f);
  encode_json("size_kb_actual", rgw_rounded_kb(size_rounded), f);
  encode_json("size_kb_utilized", rgw_rounded_kb(size_utilized), f);
  encode_json("num_objects", num_objects, f);
}

void RGWRedirectInfo::dump(Formatter *f) const
{
  encode_json("protocol", protocol, f);
  encode_json("hostname", hostname, f);
  encode_json("http_redirect_code", (int)http_redirect_code, f);
}

void RGWRedirectInfo::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("protocol", protocol, obj);
  JSONDecoder::decode_json("hostname", hostname, obj);
  int code;
  JSONDecoder::decode_json("http_redirect_code", code, obj);
  http_redirect_code = code;
}

void RGWBWRedirectInfo::dump(Formatter *f) const
{
  encode_json("redirect", redirect, f);
  encode_json("replace_key_prefix_with", replace_key_prefix_with, f);
  encode_json("replace_key_with", replace_key_with, f);
}

void RGWBWRedirectInfo::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("redirect", redirect, obj);
  JSONDecoder::decode_json("replace_key_prefix_with", replace_key_prefix_with, obj);
  JSONDecoder::decode_json("replace_key_with", replace_key_with, obj);
}

void RGWBWRoutingRuleCondition::dump(Formatter *f) const
{
  encode_json("key_prefix_equals", key_prefix_equals, f);
  encode_json("http_error_code_returned_equals", (int)http_error_code_returned_equals, f);
}

void RGWBWRoutingRuleCondition::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("key_prefix_equals", key_prefix_equals, obj);
  int code;
  JSONDecoder::decode_json("http_error_code_returned_equals", code, obj);
  http_error_code_returned_equals = code;
}

void RGWBWRoutingRule::dump(Formatter *f) const
{
  encode_json("condition", condition, f);
  encode_json("redirect_info", redirect_info, f);
}

void RGWBWRoutingRule::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("condition", condition, obj);
  JSONDecoder::decode_json("redirect_info", redirect_info, obj);
}

void RGWBWRoutingRules::dump(Formatter *f) const
{
  encode_json("rules", rules, f);
}

void RGWBWRoutingRules::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("rules", rules, obj);
}

void RGWBucketWebsiteConf::dump(Formatter *f) const
{
  if (!redirect_all.hostname.empty()) {
    encode_json("redirect_all", redirect_all, f);
  } else {
    encode_json("index_doc_suffix", index_doc_suffix, f);
    encode_json("error_doc", error_doc, f);
    encode_json("routing_rules", routing_rules, f);
  }
}

void RGWBucketWebsiteConf::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("redirect_all", redirect_all, obj);
  JSONDecoder::decode_json("index_doc_suffix", index_doc_suffix, obj);
  JSONDecoder::decode_json("error_doc", error_doc, obj);
  JSONDecoder::decode_json("routing_rules", routing_rules, obj);
}

void RGWBucketInfo::dump(Formatter *f) const
{
  encode_json("bucket", bucket, f);
  utime_t ut(creation_time);
  encode_json("creation_time", ut, f);
  encode_json("owner", owner.to_str(), f);
  encode_json("flags", flags, f);
  encode_json("zonegroup", zonegroup, f);
  encode_json("placement_rule", placement_rule, f);
  encode_json("has_instance_obj", has_instance_obj, f);
  encode_json("quota", quota, f);
  encode_json("num_shards", layout.current_index.layout.normal.num_shards, f);
  encode_json("bi_shard_hash_type", (uint32_t)layout.current_index.layout.normal.hash_type, f);
  encode_json("requester_pays", requester_pays, f);
  encode_json("has_website", has_website, f);
  if (has_website) {
    encode_json("website_conf", website_conf, f);
  }
  encode_json("swift_versioning", swift_versioning, f);
  encode_json("swift_ver_location", swift_ver_location, f);
  encode_json("index_type", (uint32_t)layout.current_index.layout.type, f);
  encode_json("mdsearch_config", mdsearch_config, f);
  encode_json("reshard_status", (int)reshard_status, f);
  encode_json("new_bucket_instance_id", new_bucket_instance_id, f);
  if (!empty_sync_policy()) {
    encode_json("sync_policy", *sync_policy, f);
  }
}

void RGWBucketInfo::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("bucket", bucket, obj);
  utime_t ut;
  JSONDecoder::decode_json("creation_time", ut, obj);
  creation_time = ut.to_real_time();
  JSONDecoder::decode_json("owner", owner, obj);
  JSONDecoder::decode_json("flags", flags, obj);
  JSONDecoder::decode_json("zonegroup", zonegroup, obj);
  /* backward compatability with region */
  if (zonegroup.empty()) {
    JSONDecoder::decode_json("region", zonegroup, obj);
  }
  string pr;
  JSONDecoder::decode_json("placement_rule", pr, obj);
  placement_rule.from_str(pr);
  JSONDecoder::decode_json("has_instance_obj", has_instance_obj, obj);
  JSONDecoder::decode_json("quota", quota, obj);
  JSONDecoder::decode_json("num_shards", layout.current_index.layout.normal.num_shards, obj);
  uint32_t hash_type;
  JSONDecoder::decode_json("bi_shard_hash_type", hash_type, obj);
  layout.current_index.layout.normal.hash_type = static_cast<rgw::BucketHashType>(hash_type);
  JSONDecoder::decode_json("requester_pays", requester_pays, obj);
  JSONDecoder::decode_json("has_website", has_website, obj);
  if (has_website) {
    JSONDecoder::decode_json("website_conf", website_conf, obj);
  }
  JSONDecoder::decode_json("swift_versioning", swift_versioning, obj);
  JSONDecoder::decode_json("swift_ver_location", swift_ver_location, obj);
  uint32_t it;
  JSONDecoder::decode_json("index_type", it, obj);
  layout.current_index.layout.type = (rgw::BucketIndexType)it;
  JSONDecoder::decode_json("mdsearch_config", mdsearch_config, obj);
  int rs;
  JSONDecoder::decode_json("reshard_status", rs, obj);
  reshard_status = (cls_rgw_reshard_status)rs;

  rgw_sync_policy_info sp;
  JSONDecoder::decode_json("sync_policy", sp, obj);
  if (!sp.empty()) {
    set_sync_policy(std::move(sp));
  }
}

void rgw_sync_directional_rule::dump(Formatter *f) const
{
  encode_json("source_zone", source_zone, f);
  encode_json("dest_zone", dest_zone, f);
}

void rgw_sync_directional_rule::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("source_zone", source_zone, obj);
  JSONDecoder::decode_json("dest_zone", dest_zone, obj);
}

void rgw_sync_symmetric_group::dump(Formatter *f) const
{
  encode_json("id", id, f);
  encode_json("zones", zones, f);
}

void rgw_sync_symmetric_group::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("zones", zones, obj);
}

void rgw_sync_bucket_entity::dump(Formatter *f) const
{
  encode_json("zone", zone, f);
  encode_json("bucket", bucket_key(), f);
}

void rgw_sync_bucket_entity::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("zone", zone, obj);
  string s;
  if (JSONDecoder::decode_json("bucket", s, obj)) {
    rgw_bucket b;
    int ret = rgw_bucket_parse_bucket_key(nullptr, s, &b, nullptr);
    if (ret >= 0) {
      bucket = b;
    } else {
      bucket.reset();
    }
  }
}

void rgw_sync_pipe_filter_tag::dump(Formatter *f) const
{
  encode_json("key", key, f);
  encode_json("value", value, f);
}

void rgw_sync_pipe_filter_tag::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("key", key, obj);
  JSONDecoder::decode_json("value", value, obj);
}

void rgw_sync_pipe_filter::dump(Formatter *f) const
{
  encode_json("prefix", prefix, f);
  encode_json("tags", tags, f);
}

void rgw_sync_pipe_filter::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("prefix", prefix, obj);
  JSONDecoder::decode_json("tags", tags, obj);
}

void rgw_sync_pipe_acl_translation::dump(Formatter *f) const
{
  encode_json("owner", owner, f);
}

void rgw_sync_pipe_acl_translation::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("owner", owner, obj);
}

void rgw_sync_pipe_source_params::dump(Formatter *f) const
{
  encode_json("filter", filter, f);
}

void rgw_sync_pipe_source_params::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("filter", filter, obj);
}

void rgw_sync_pipe_dest_params::dump(Formatter *f) const
{
  encode_json("acl_translation", acl_translation, f);
  encode_json("storage_class", storage_class, f);
}

void rgw_sync_pipe_dest_params::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("acl_translation", acl_translation, obj);
  JSONDecoder::decode_json("storage_class", storage_class, obj);
}

void rgw_sync_pipe_params::dump(Formatter *f) const
{
  encode_json("source", source, f);
  encode_json("dest", dest, f);
  encode_json("priority", priority, f);
  string s;
  switch (mode) {
    case MODE_SYSTEM:
      s = "system";
      break;
    default:
      s = "user";
  }
  encode_json("mode", s, f);
  encode_json("user", user, f);
}

void rgw_sync_pipe_params::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("source", source, obj);
  JSONDecoder::decode_json("dest", dest, obj);
  JSONDecoder::decode_json("priority", priority, obj);
  string s;
  JSONDecoder::decode_json("mode", s, obj);
  if (s == "system") {
    mode = MODE_SYSTEM;
  } else {
    mode = MODE_USER;
  }
  JSONDecoder::decode_json("user", user, obj);
}


void rgw_sync_bucket_entities::dump(Formatter *f) const
{
  encode_json("bucket", rgw_sync_bucket_entities::bucket_key(bucket), f);
  if (zones) {
    encode_json("zones", zones, f);
  } else if (all_zones) {
    set<string> z = { "*" };
    encode_json("zones", z, f);
  }
}

void rgw_sync_bucket_entities::decode_json(JSONObj *obj)
{
  string s;
  JSONDecoder::decode_json("bucket", s, obj);
  if (s == "*") {
    bucket.reset();
  } else {
    rgw_bucket b;
    int ret = rgw_bucket_parse_bucket_key(nullptr, s, &b, nullptr);
    if (ret < 0) {
      bucket.reset();
    } else {
      if (b.tenant == "*") {
        b.tenant.clear();
      }
      if (b.name == "*") {
        b.name.clear();
      }
      if (b.bucket_id == "*") {
        b.bucket_id.clear();
      }
      bucket = b;
    }
  }
  JSONDecoder::decode_json("zones", zones, obj);
  if (zones && zones->size() == 1) {
    auto iter = zones->begin();
    if (*iter == "*") {
      zones.reset();
      all_zones = true;
    }
  }
}

void rgw_sync_bucket_pipe::dump(Formatter *f) const
{
  encode_json("id", id, f);
  encode_json("source", source, f);
  encode_json("dest", dest, f);
  encode_json("params", params, f);
}

void rgw_sync_bucket_pipe::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("source", source, obj);
  JSONDecoder::decode_json("dest", dest, obj);
  JSONDecoder::decode_json("params", params, obj);
}

void rgw_sync_bucket_pipes::dump(Formatter *f) const
{
  encode_json("id", id, f);
  encode_json("source", source, f);
  encode_json("dest", dest, f);
  encode_json("params", params, f);
}

void rgw_sync_bucket_pipes::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("source", source, obj);
  JSONDecoder::decode_json("dest", dest, obj);
  JSONDecoder::decode_json("params", params, obj);
}

void rgw_sync_data_flow_group::dump(Formatter *f) const
{
  if (!symmetrical.empty()) {
    encode_json("symmetrical", symmetrical, f);
  }

  if (!directional.empty()) {
    encode_json("directional", directional, f);
  }
}

void rgw_sync_data_flow_group::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("symmetrical", symmetrical, obj);
  JSONDecoder::decode_json("directional", directional, obj);
}

void rgw_sync_policy_group::dump(Formatter *f) const
{
  encode_json("id", id, f);
  encode_json("data_flow", data_flow, f);
  encode_json("pipes", pipes, f);
  string s;
  switch (status) {
    case  rgw_sync_policy_group::Status::FORBIDDEN:
      s = "forbidden";
      break;
    case  rgw_sync_policy_group::Status::ALLOWED:
      s = "allowed";
      break;
    case  rgw_sync_policy_group::Status::ENABLED:
      s = "enabled";
      break;
    default:
      s = "unknown";
  }
  encode_json("status", s, f);
}

void rgw_sync_policy_group::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("data_flow", data_flow, obj);
  JSONDecoder::decode_json("pipes", pipes, obj);
  string s;
  JSONDecoder::decode_json("status", s, obj);
  set_status(s);
}

void rgw_sync_policy_info::dump(Formatter *f) const
{
  Formatter::ArraySection section(*f, "groups");
  for (auto& group : groups ) {
    encode_json("group", group.second, f);
  }
}

void rgw_sync_policy_info::decode_json(JSONObj *obj)
{
  vector<rgw_sync_policy_group> groups_vec;

  JSONDecoder::decode_json("groups", groups_vec, obj);

  for (auto& group : groups_vec) {
    groups.emplace(std::make_pair(group.id, std::move(group)));
  }
}

void rgw_obj_key::dump(Formatter *f) const
{
  encode_json("name", name, f);
  encode_json("instance", instance, f);
  encode_json("ns", ns, f);
}

void rgw_obj_key::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("instance", instance, obj);
  JSONDecoder::decode_json("ns", ns, obj);
}

void RGWBucketEnt::dump(Formatter *f) const
{
  encode_json("bucket", bucket, f);
  encode_json("size", size, f);
  encode_json("size_rounded", size_rounded, f);
  utime_t ut(creation_time);
  encode_json("mtime", ut, f); /* mtime / creation time discrepency needed for backward compatibility */
  encode_json("count", count, f);
  encode_json("placement_rule", placement_rule.to_str(), f);
}

void RGWUploadPartInfo::dump(Formatter *f) const
{
  encode_json("num", num, f);
  encode_json("size", size, f);
  encode_json("etag", etag, f);
  utime_t ut(modified);
  encode_json("modified", ut, f);
}

void rgw_raw_obj::dump(Formatter *f) const
{
  encode_json("pool", pool, f);
  encode_json("oid", oid, f);
  encode_json("loc", loc, f);
}

void rgw_raw_obj::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("pool", pool, obj);
  JSONDecoder::decode_json("oid", oid, obj);
  JSONDecoder::decode_json("loc", loc, obj);
}

void rgw_obj::dump(Formatter *f) const
{
  encode_json("bucket", bucket, f);
  encode_json("key", key, f);
}

void RGWDefaultSystemMetaObjInfo::dump(Formatter *f) const {
  encode_json("default_id", default_id, f);
}

void RGWDefaultSystemMetaObjInfo::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("default_id", default_id, obj);
}

void RGWNameToId::dump(Formatter *f) const {
  encode_json("obj_id", obj_id, f);
}

void RGWNameToId::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("obj_id", obj_id, obj);
}

void RGWSystemMetaObj::dump(Formatter *f) const
{
  encode_json("id", id , f);
  encode_json("name", name , f);
}

void RGWSystemMetaObj::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("name", name, obj);
}

void RGWPeriodLatestEpochInfo::dump(Formatter *f) const {
  encode_json("latest_epoch", epoch, f);
}

void RGWPeriodLatestEpochInfo::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("latest_epoch", epoch, obj);
}

void RGWPeriod::dump(Formatter *f) const
{
  encode_json("id", id, f);
  encode_json("epoch", epoch , f);
  encode_json("predecessor_uuid", predecessor_uuid, f);
  encode_json("sync_status", sync_status, f);
  encode_json("period_map", period_map, f);
  encode_json("master_zonegroup", master_zonegroup, f);
  encode_json("master_zone", master_zone, f);
  encode_json("period_config", period_config, f);
  encode_json("realm_id", realm_id, f);
  encode_json("realm_name", realm_name, f);
  encode_json("realm_epoch", realm_epoch, f);
}

void RGWPeriod::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("epoch", epoch, obj);
  JSONDecoder::decode_json("predecessor_uuid", predecessor_uuid, obj);
  JSONDecoder::decode_json("sync_status", sync_status, obj);
  JSONDecoder::decode_json("period_map", period_map, obj);
  JSONDecoder::decode_json("master_zonegroup", master_zonegroup, obj);
  JSONDecoder::decode_json("master_zone", master_zone, obj);
  JSONDecoder::decode_json("period_config", period_config, obj);
  JSONDecoder::decode_json("realm_id", realm_id, obj);
  JSONDecoder::decode_json("realm_name", realm_name, obj);
  JSONDecoder::decode_json("realm_epoch", realm_epoch, obj);
}

void RGWZoneParams::dump(Formatter *f) const
{
  RGWSystemMetaObj::dump(f);
  encode_json("domain_root", domain_root, f);
  encode_json("control_pool", control_pool, f);
  encode_json("gc_pool", gc_pool, f);
  encode_json("lc_pool", lc_pool, f);
  encode_json("log_pool", log_pool, f);
  encode_json("intent_log_pool", intent_log_pool, f);
  encode_json("usage_log_pool", usage_log_pool, f);
  encode_json("roles_pool", roles_pool, f);
  encode_json("reshard_pool", reshard_pool, f);
  encode_json("user_keys_pool", user_keys_pool, f);
  encode_json("user_email_pool", user_email_pool, f);
  encode_json("user_swift_pool", user_swift_pool, f);
  encode_json("user_uid_pool", user_uid_pool, f);
  encode_json("otp_pool", otp_pool, f);
  encode_json_plain("system_key", system_key, f);
  encode_json("placement_pools", placement_pools, f);
  encode_json("tier_config", tier_config, f);
  encode_json("realm_id", realm_id, f);
}

void RGWZoneStorageClass::dump(Formatter *f) const
{
  if (data_pool) {
    encode_json("data_pool", data_pool.get(), f);
  }
  if (compression_type) {
    encode_json("compression_type", compression_type.get(), f);
  }
}

void RGWZoneStorageClass::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("data_pool", data_pool, obj);
  JSONDecoder::decode_json("compression_type", compression_type, obj);
}

void RGWZoneStorageClasses::dump(Formatter *f) const
{
  for (auto& i : m) {
    encode_json(i.first.c_str(), i.second, f);
  }
}

void RGWZoneStorageClasses::decode_json(JSONObj *obj)
{
  JSONFormattable f;
  decode_json_obj(f, obj);

  for (auto& field : f.object()) {
    JSONObj *field_obj = obj->find_obj(field.first);
    assert(field_obj);

    decode_json_obj(m[field.first], field_obj);
  }
  standard_class = &m[RGW_STORAGE_CLASS_STANDARD];
}

void RGWZonePlacementInfo::dump(Formatter *f) const
{
  encode_json("index_pool", index_pool, f);
  encode_json("storage_classes", storage_classes, f);
  encode_json("data_extra_pool", data_extra_pool, f);
  encode_json("index_type", (uint32_t)index_type, f);

  /* no real need for backward compatibility of compression_type and data_pool in here,
   * rather not clutter the output */
}

void RGWZonePlacementInfo::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("index_pool", index_pool, obj);
  JSONDecoder::decode_json("storage_classes", storage_classes, obj);
  JSONDecoder::decode_json("data_extra_pool", data_extra_pool, obj);
  uint32_t it;
  JSONDecoder::decode_json("index_type", it, obj);
  index_type = (rgw::BucketIndexType)it;

  /* backward compatibility, these are now defined in storage_classes */
  string standard_compression_type;
  string *pcompression = nullptr;
  if (JSONDecoder::decode_json("compression", standard_compression_type, obj)) {
    pcompression = &standard_compression_type;
  }
  rgw_pool standard_data_pool;
  rgw_pool *ppool = nullptr;
  if (JSONDecoder::decode_json("data_pool", standard_data_pool, obj)) {
    ppool = &standard_data_pool;
  }
  if (ppool || pcompression) {
    storage_classes.set_storage_class(RGW_STORAGE_CLASS_STANDARD, ppool, pcompression);
  }
}

void RGWZoneParams::decode_json(JSONObj *obj)
{
  RGWSystemMetaObj::decode_json(obj);
  JSONDecoder::decode_json("domain_root", domain_root, obj);
  JSONDecoder::decode_json("control_pool", control_pool, obj);
  JSONDecoder::decode_json("gc_pool", gc_pool, obj);
  JSONDecoder::decode_json("lc_pool", lc_pool, obj);
  JSONDecoder::decode_json("log_pool", log_pool, obj);
  JSONDecoder::decode_json("intent_log_pool", intent_log_pool, obj);
  JSONDecoder::decode_json("roles_pool", roles_pool, obj);
  JSONDecoder::decode_json("reshard_pool", reshard_pool, obj);
  JSONDecoder::decode_json("usage_log_pool", usage_log_pool, obj);
  JSONDecoder::decode_json("user_keys_pool", user_keys_pool, obj);
  JSONDecoder::decode_json("user_email_pool", user_email_pool, obj);
  JSONDecoder::decode_json("user_swift_pool", user_swift_pool, obj);
  JSONDecoder::decode_json("user_uid_pool", user_uid_pool, obj);
  JSONDecoder::decode_json("otp_pool", otp_pool, obj);
  JSONDecoder::decode_json("system_key", system_key, obj);
  JSONDecoder::decode_json("placement_pools", placement_pools, obj);
  JSONDecoder::decode_json("tier_config", tier_config, obj);
  JSONDecoder::decode_json("realm_id", realm_id, obj);

}

void RGWZone::dump(Formatter *f) const
{
  encode_json("id", id, f);
  encode_json("name", name, f);
  encode_json("endpoints", endpoints, f);
  encode_json("log_meta", log_meta, f);
  encode_json("log_data", log_data, f);
  encode_json("bucket_index_max_shards", bucket_index_max_shards, f);
  encode_json("read_only", read_only, f);
  encode_json("tier_type", tier_type, f);
  encode_json("sync_from_all", sync_from_all, f);
  encode_json("sync_from", sync_from, f);
  encode_json("redirect_zone", redirect_zone, f);
}

void RGWZone::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("name", name, obj);
  if (id.empty()) {
    id = name;
  }
  JSONDecoder::decode_json("endpoints", endpoints, obj);
  JSONDecoder::decode_json("log_meta", log_meta, obj);
  JSONDecoder::decode_json("log_data", log_data, obj);
  JSONDecoder::decode_json("bucket_index_max_shards", bucket_index_max_shards, obj);
  JSONDecoder::decode_json("read_only", read_only, obj);
  JSONDecoder::decode_json("tier_type", tier_type, obj);
  JSONDecoder::decode_json("sync_from_all", sync_from_all, true, obj);
  JSONDecoder::decode_json("sync_from", sync_from, obj);
  JSONDecoder::decode_json("redirect_zone", redirect_zone, obj);
}

void RGWZoneGroupPlacementTarget::dump(Formatter *f) const
{
  encode_json("name", name, f);
  encode_json("tags", tags, f);
  encode_json("storage_classes", storage_classes, f);
}

void RGWZoneGroupPlacementTarget::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("tags", tags, obj);
  JSONDecoder::decode_json("storage_classes", storage_classes, obj);
  if (storage_classes.empty()) {
    storage_classes.insert(RGW_STORAGE_CLASS_STANDARD);
  }
}

void RGWZoneGroup::dump(Formatter *f) const
{
  RGWSystemMetaObj::dump(f);
  encode_json("api_name", api_name, f);
  encode_json("is_master", is_master, f);
  encode_json("endpoints", endpoints, f);
  encode_json("hostnames", hostnames, f);
  encode_json("hostnames_s3website", hostnames_s3website, f);
  encode_json("master_zone", master_zone, f);
  encode_json_map("zones", zones, f); /* more friendly representation */
  encode_json_map("placement_targets", placement_targets, f); /* more friendly representation */
  encode_json("default_placement", default_placement, f);
  encode_json("realm_id", realm_id, f);
  encode_json("sync_policy", sync_policy, f);
}

static void decode_zones(map<rgw_zone_id, RGWZone>& zones, JSONObj *o)
{
  RGWZone z;
  z.decode_json(o);
  zones[z.id] = z;
}

static void decode_placement_targets(map<string, RGWZoneGroupPlacementTarget>& targets, JSONObj *o)
{
  RGWZoneGroupPlacementTarget t;
  t.decode_json(o);
  targets[t.name] = t;
}


void RGWZoneGroup::decode_json(JSONObj *obj)
{
  RGWSystemMetaObj::decode_json(obj);
  if (id.empty()) {
    derr << "old format " << dendl;
    JSONDecoder::decode_json("name", name, obj);
    id = name;
  }
  JSONDecoder::decode_json("api_name", api_name, obj);
  JSONDecoder::decode_json("is_master", is_master, obj);
  JSONDecoder::decode_json("endpoints", endpoints, obj);
  JSONDecoder::decode_json("hostnames", hostnames, obj);
  JSONDecoder::decode_json("hostnames_s3website", hostnames_s3website, obj);
  JSONDecoder::decode_json("master_zone", master_zone, obj);
  JSONDecoder::decode_json("zones", zones, decode_zones, obj);
  JSONDecoder::decode_json("placement_targets", placement_targets, decode_placement_targets, obj);
  JSONDecoder::decode_json("default_placement", default_placement.name, obj);
  JSONDecoder::decode_json("default_storage_class", default_placement.storage_class, obj);
  JSONDecoder::decode_json("realm_id", realm_id, obj);
  JSONDecoder::decode_json("sync_policy", sync_policy, obj);
}


void RGWPeriodMap::dump(Formatter *f) const
{
  encode_json("id", id, f);
  encode_json_map("zonegroups", zonegroups, f);
  encode_json("short_zone_ids", short_zone_ids, f);
}

static void decode_zonegroups(map<string, RGWZoneGroup>& zonegroups, JSONObj *o)
{
  RGWZoneGroup zg;
  zg.decode_json(o);
  zonegroups[zg.get_id()] = zg;
}

void RGWPeriodMap::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("zonegroups", zonegroups, decode_zonegroups, obj);
  /* backward compatability with region */
  if (zonegroups.empty()) {
    JSONDecoder::decode_json("regions", zonegroups, obj);
  }
  /* backward compatability with region */
  if (master_zonegroup.empty()) {
    JSONDecoder::decode_json("master_region", master_zonegroup, obj);
  }
  JSONDecoder::decode_json("short_zone_ids", short_zone_ids, obj);
}


void RGWPeriodConfig::dump(Formatter *f) const
{
  encode_json("bucket_quota", bucket_quota, f);
  encode_json("user_quota", user_quota, f);
}

void RGWPeriodConfig::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("bucket_quota", bucket_quota, obj);
  JSONDecoder::decode_json("user_quota", user_quota, obj);
}

void RGWRegionMap::dump(Formatter *f) const
{
  encode_json("regions", regions, f);
  encode_json("master_region", master_region, f);
  encode_json("bucket_quota", bucket_quota, f);
  encode_json("user_quota", user_quota, f);
}

void RGWRegionMap::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("regions", regions, obj);
  JSONDecoder::decode_json("master_region", master_region, obj);
  JSONDecoder::decode_json("bucket_quota", bucket_quota, obj);
  JSONDecoder::decode_json("user_quota", user_quota, obj);
}

void RGWZoneGroupMap::dump(Formatter *f) const
{
  encode_json("zonegroups", zonegroups, f);
  encode_json("master_zonegroup", master_zonegroup, f);
  encode_json("bucket_quota", bucket_quota, f);
  encode_json("user_quota", user_quota, f);
}

void RGWZoneGroupMap::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("zonegroups", zonegroups, obj);
  /* backward compatability with region */
  if (zonegroups.empty()) {
    JSONDecoder::decode_json("regions", zonegroups, obj);
  }
  JSONDecoder::decode_json("master_zonegroup", master_zonegroup, obj);
  /* backward compatability with region */
  if (master_zonegroup.empty()) {
    JSONDecoder::decode_json("master_region", master_zonegroup, obj);
  }

  JSONDecoder::decode_json("bucket_quota", bucket_quota, obj);
  JSONDecoder::decode_json("user_quota", user_quota, obj);
}

void RGWMetadataLogInfo::dump(Formatter *f) const
{
  encode_json("marker", marker, f);
  utime_t ut(last_update);
  encode_json("last_update", ut, f);
}

void RGWMetadataLogInfo::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("marker", marker, obj);
  utime_t ut;
  JSONDecoder::decode_json("last_update", ut, obj);
  last_update = ut.to_real_time();
}

void RGWDataChangesLogInfo::dump(Formatter *f) const
{
  encode_json("marker", marker, f);
  utime_t ut(last_update);
  encode_json("last_update", ut, f);
}

void RGWDataChangesLogInfo::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("marker", marker, obj);
  utime_t ut;
  JSONDecoder::decode_json("last_update", ut, obj);
  last_update = ut.to_real_time();
}


void RGWRealm::dump(Formatter *f) const
{
  RGWSystemMetaObj::dump(f);
  encode_json("current_period", current_period, f);
  encode_json("epoch", epoch, f);
}


void RGWRealm::decode_json(JSONObj *obj)
{
  RGWSystemMetaObj::decode_json(obj);
  JSONDecoder::decode_json("current_period", current_period, obj);
  JSONDecoder::decode_json("epoch", epoch, obj);
}

void rgw::keystone::TokenEnvelope::Token::decode_json(JSONObj *obj)
{
  string expires_iso8601;
  struct tm t;

  JSONDecoder::decode_json("id", id, obj, true);
  JSONDecoder::decode_json("tenant", tenant_v2, obj, true);
  JSONDecoder::decode_json("expires", expires_iso8601, obj, true);

  if (parse_iso8601(expires_iso8601.c_str(), &t)) {
    expires = internal_timegm(&t);
  } else {
    expires = 0;
    throw JSONDecoder::err("Failed to parse ISO8601 expiration date from Keystone response.");
  }
}

void rgw::keystone::TokenEnvelope::Role::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("name", name, obj, true);
}

void rgw::keystone::TokenEnvelope::Domain::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj, true);
  JSONDecoder::decode_json("name", name, obj, true);
}

void rgw::keystone::TokenEnvelope::Project::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj, true);
  JSONDecoder::decode_json("name", name, obj, true);
  JSONDecoder::decode_json("domain", domain, obj);
}

void rgw::keystone::TokenEnvelope::User::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj, true);
  JSONDecoder::decode_json("name", name, obj, true);
  JSONDecoder::decode_json("domain", domain, obj);
  JSONDecoder::decode_json("roles", roles_v2, obj);
}

void rgw::keystone::TokenEnvelope::decode_v3(JSONObj* const root_obj)
{
  std::string expires_iso8601;

  JSONDecoder::decode_json("user", user, root_obj, true);
  JSONDecoder::decode_json("expires_at", expires_iso8601, root_obj, true);
  JSONDecoder::decode_json("roles", roles, root_obj, true);
  JSONDecoder::decode_json("project", project, root_obj, true);

  struct tm t;
  if (parse_iso8601(expires_iso8601.c_str(), &t)) {
    token.expires = internal_timegm(&t);
  } else {
    token.expires = 0;
    throw JSONDecoder::err("Failed to parse ISO8601 expiration date"
                           "from Keystone response.");
  }
}

void rgw::keystone::TokenEnvelope::decode_v2(JSONObj* const root_obj)
{
  JSONDecoder::decode_json("user", user, root_obj, true);
  JSONDecoder::decode_json("token", token, root_obj, true);

  roles = user.roles_v2;
  project = token.tenant_v2;
}

void rgw_slo_entry::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("path", path, obj);
  JSONDecoder::decode_json("etag", etag, obj);
  JSONDecoder::decode_json("size_bytes", size_bytes, obj);
};

void rgw_meta_sync_info::decode_json(JSONObj *obj)
{
  string s;
  JSONDecoder::decode_json("status", s, obj);
  if (s == "init") {
    state = StateInit;
  } else if (s == "building-full-sync-maps") {
    state = StateBuildingFullSyncMaps;
  } else if (s == "sync") {
    state = StateSync;
  }    
  JSONDecoder::decode_json("num_shards", num_shards, obj);
  JSONDecoder::decode_json("period", period, obj);
  JSONDecoder::decode_json("realm_epoch", realm_epoch, obj);
}

void rgw_meta_sync_info::dump(Formatter *f) const
{
  string s;
  switch ((SyncState)state) {
  case StateInit:
    s = "init";
    break;
  case StateBuildingFullSyncMaps:
    s = "building-full-sync-maps";
    break;
  case StateSync:
    s = "sync";
    break;
  default:
    s = "unknown";
    break;
  }
  encode_json("status", s, f);
  encode_json("num_shards", num_shards, f);
  encode_json("period", period, f);
  encode_json("realm_epoch", realm_epoch, f);
}

void rgw_meta_sync_marker::decode_json(JSONObj *obj)
{
  int s;
  JSONDecoder::decode_json("state", s, obj);
  state = s;
  JSONDecoder::decode_json("marker", marker, obj);
  JSONDecoder::decode_json("next_step_marker", next_step_marker, obj);
  JSONDecoder::decode_json("total_entries", total_entries, obj);
  JSONDecoder::decode_json("pos", pos, obj);
  utime_t ut;
  JSONDecoder::decode_json("timestamp", ut, obj);
  timestamp = ut.to_real_time();
  JSONDecoder::decode_json("realm_epoch", realm_epoch, obj);
}

void rgw_meta_sync_marker::dump(Formatter *f) const
{
  encode_json("state", (int)state, f);
  encode_json("marker", marker, f);
  encode_json("next_step_marker", next_step_marker, f);
  encode_json("total_entries", total_entries, f);
  encode_json("pos", pos, f);
  encode_json("timestamp", utime_t(timestamp), f);
  encode_json("realm_epoch", realm_epoch, f);
}

void rgw_meta_sync_status::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("info", sync_info, obj);
  JSONDecoder::decode_json("markers", sync_markers, obj);
}

void rgw_meta_sync_status::dump(Formatter *f) const {
  encode_json("info", sync_info, f);
  encode_json("markers", sync_markers, f);
}

void rgw_sync_error_info::dump(Formatter *f) const {
  encode_json("source_zone", source_zone, f);
  encode_json("error_code", error_code, f);
  encode_json("message", message, f);
}

void rgw_bucket_shard_full_sync_marker::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("position", position, obj);
  JSONDecoder::decode_json("count", count, obj);
}

void rgw_bucket_shard_full_sync_marker::dump(Formatter *f) const
{
  encode_json("position", position, f);
  encode_json("count", count, f);
}

void rgw_bucket_shard_inc_sync_marker::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("position", position, obj);
  JSONDecoder::decode_json("timestamp", timestamp, obj);
}

void rgw_bucket_shard_inc_sync_marker::dump(Formatter *f) const
{
  encode_json("position", position, f);
  encode_json("timestamp", timestamp, f);
}

void rgw_bucket_shard_sync_info::decode_json(JSONObj *obj)
{
  std::string s;
  JSONDecoder::decode_json("status", s, obj);
  if (s == "full-sync") {
    state = StateFullSync;
  } else if (s == "incremental-sync") {
    state = StateIncrementalSync;
  } else if (s == "stopped") {
    state = StateStopped;
  } else {
    state = StateInit;
  }
  JSONDecoder::decode_json("full_marker", full_marker, obj);
  JSONDecoder::decode_json("inc_marker", inc_marker, obj);
}

void rgw_bucket_shard_sync_info::dump(Formatter *f) const
{
  const char *s{nullptr};
  switch ((SyncState)state) {
    case StateInit:
    s = "init";
    break;
  case StateFullSync:
    s = "full-sync";
    break;
  case StateIncrementalSync:
    s = "incremental-sync";
    break;
  case StateStopped:
    s = "stopped";
    break;
  default:
    s = "unknown";
    break;
  }
  encode_json("status", s, f);
  encode_json("full_marker", full_marker, f);
  encode_json("inc_marker", inc_marker, f);
}

/* This utility function shouldn't conflict with the overload of std::to_string
 * provided by string_ref since Boost 1.54 as it's defined outside of the std
 * namespace. I hope we'll remove it soon - just after merging the Matt's PR
 * for bundled Boost. It would allow us to forget that CentOS 7 has Boost 1.53. */
static inline std::string to_string(const std::string_view& s)
{
  return std::string(s.data(), s.length());
}

void rgw::keystone::AdminTokenRequestVer2::dump(Formatter* const f) const
{
  f->open_object_section("token_request");
    f->open_object_section("auth");
      f->open_object_section("passwordCredentials");
        encode_json("username", ::to_string(conf.get_admin_user()), f);
        encode_json("password", ::to_string(conf.get_admin_password()), f);
      f->close_section();
      encode_json("tenantName", ::to_string(conf.get_admin_tenant()), f);
    f->close_section();
  f->close_section();
}

void rgw::keystone::AdminTokenRequestVer3::dump(Formatter* const f) const
{
  f->open_object_section("token_request");
    f->open_object_section("auth");
      f->open_object_section("identity");
        f->open_array_section("methods");
          f->dump_string("", "password");
        f->close_section();
        f->open_object_section("password");
          f->open_object_section("user");
            f->open_object_section("domain");
              encode_json("name", ::to_string(conf.get_admin_domain()), f);
            f->close_section();
            encode_json("name", ::to_string(conf.get_admin_user()), f);
            encode_json("password", ::to_string(conf.get_admin_password()), f);
          f->close_section();
        f->close_section();
      f->close_section();
      f->open_object_section("scope");
        f->open_object_section("project");
          if (! conf.get_admin_project().empty()) {
            encode_json("name", ::to_string(conf.get_admin_project()), f);
          } else {
            encode_json("name", ::to_string(conf.get_admin_tenant()), f);
          }
          f->open_object_section("domain");
            encode_json("name", ::to_string(conf.get_admin_domain()), f);
          f->close_section();
        f->close_section();
      f->close_section();
    f->close_section();
  f->close_section();
}


void rgw::keystone::BarbicanTokenRequestVer2::dump(Formatter* const f) const
{
  f->open_object_section("token_request");
    f->open_object_section("auth");
      f->open_object_section("passwordCredentials");
        encode_json("username", cct->_conf->rgw_keystone_barbican_user, f);
        encode_json("password", cct->_conf->rgw_keystone_barbican_password, f);
      f->close_section();
      encode_json("tenantName", cct->_conf->rgw_keystone_barbican_tenant, f);
    f->close_section();
  f->close_section();
}

void rgw::keystone::BarbicanTokenRequestVer3::dump(Formatter* const f) const
{
  f->open_object_section("token_request");
    f->open_object_section("auth");
      f->open_object_section("identity");
        f->open_array_section("methods");
          f->dump_string("", "password");
        f->close_section();
        f->open_object_section("password");
          f->open_object_section("user");
            f->open_object_section("domain");
              encode_json("name", cct->_conf->rgw_keystone_barbican_domain, f);
            f->close_section();
            encode_json("name", cct->_conf->rgw_keystone_barbican_user, f);
            encode_json("password", cct->_conf->rgw_keystone_barbican_password, f);
          f->close_section();
        f->close_section();
      f->close_section();
      f->open_object_section("scope");
        f->open_object_section("project");
          if (!cct->_conf->rgw_keystone_barbican_project.empty()) {
            encode_json("name", cct->_conf->rgw_keystone_barbican_project, f);
          } else {
            encode_json("name", cct->_conf->rgw_keystone_barbican_tenant, f);
          }
          f->open_object_section("domain");
            encode_json("name", cct->_conf->rgw_keystone_barbican_domain, f);
          f->close_section();
        f->close_section();
      f->close_section();
    f->close_section();
  f->close_section();
}

void RGWOrphanSearchStage::dump(Formatter *f) const
{
  f->open_object_section("orphan_search_stage");
  string s;
  switch(stage){
  case ORPHAN_SEARCH_STAGE_INIT:
    s = "init";
    break;
  case ORPHAN_SEARCH_STAGE_LSPOOL:
    s = "lspool";
    break;
  case ORPHAN_SEARCH_STAGE_LSBUCKETS:
    s =  "lsbuckets";
    break;
  case ORPHAN_SEARCH_STAGE_ITERATE_BI:
    s = "iterate_bucket_index";
    break;
  case ORPHAN_SEARCH_STAGE_COMPARE:
    s = "comparing";
    break;
  default:
    s = "unknown";
  }
  f->dump_string("search_stage", s);
  f->dump_int("shard",shard);
  f->dump_string("marker",marker);
  f->close_section();
}

void RGWOrphanSearchInfo::dump(Formatter *f) const
{
  f->open_object_section("orphan_search_info");
  f->dump_string("job_name", job_name);
  encode_json("pool", pool, f);
  f->dump_int("num_shards", num_shards);
  encode_json("start_time", start_time, f);
  f->close_section();
}

void RGWOrphanSearchState::dump(Formatter *f) const
{
  f->open_object_section("orphan_search_state");
  encode_json("info", info, f);
  encode_json("stage", stage, f);
  f->close_section();
}

void RGWObjTags::dump(Formatter *f) const
{
  f->open_object_section("tagset");
  for (auto& tag: tag_map){
    f->dump_string(tag.first.c_str(), tag.second);
  }
  f->close_section();
}

void lc_op::dump(Formatter *f) const
{
  f->dump_bool("status", status);
  f->dump_bool("dm_expiration", dm_expiration);

  f->dump_int("expiration", expiration);
  f->dump_int("noncur_expiration", noncur_expiration);
  f->dump_int("mp_expiration", mp_expiration);
  if (expiration_date) {
    utime_t ut(*expiration_date);
    f->dump_stream("expiration_date") << ut;
  }
  if (obj_tags) {
    f->dump_object("obj_tags", *obj_tags);
  }
}

void LCFilter::dump(Formatter *f) const
{
  f->dump_string("prefix", prefix);
  f->dump_object("obj_tags", obj_tags);
}

void LCExpiration::dump(Formatter *f) const
{
  f->dump_string("days", days);
  f->dump_string("date", date);
}

void LCRule::dump(Formatter *f) const
{
  f->dump_string("id", id);
  f->dump_string("prefix", prefix);
  f->dump_string("status", status);
  f->dump_object("expiration", expiration);
  f->dump_object("noncur_expiration", noncur_expiration);
  f->dump_object("mp_expiration", mp_expiration);
  f->dump_object("filter", filter);
  f->dump_bool("dm_expiration", dm_expiration);
}

void RGWLifecycleConfiguration::dump(Formatter *f) const
{
  f->open_object_section("prefix_map");
  for (auto& prefix : prefix_map) {
    f->dump_object(prefix.first.c_str(), prefix.second);
  }
  f->close_section();

  f->open_array_section("rule_map");
  for (auto& rule : rule_map) {
    f->open_object_section("entry");
    f->dump_string("id", rule.first);
    f->open_object_section("rule");
    rule.second.dump(f);
    f->close_section();
    f->close_section();
  }
  f->close_section();
}

void compression_block::dump(Formatter *f) const
{
  f->dump_unsigned("old_ofs", old_ofs);
  f->dump_unsigned("new_ofs", new_ofs);
  f->dump_unsigned("len", len);
}

void RGWCompressionInfo::dump(Formatter *f) const
{
  f->dump_string("compression_type", compression_type);
  f->dump_unsigned("orig_size", orig_size);
  if (compressor_message) {
    f->dump_int("compressor_message", *compressor_message);
  }
  ::encode_json("blocks", blocks, f);
}

void objexp_hint_entry::dump(Formatter *f) const
{
  f->open_object_section("objexp_hint_entry");
  encode_json("tenant", tenant, f);
  encode_json("bucket_name", bucket_name, f);
  encode_json("bucket_id", bucket_id, f);
  encode_json("rgw_obj_key", obj_key, f);
  utime_t ut(exp_time);
  encode_json("exp_time", ut, f);
  f->close_section();
}

void rgw_user::dump(Formatter *f) const
{
  ::encode_json("user", *this, f);
}

