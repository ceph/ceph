// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_log.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_cache.h"
#include "rgw_bucket.h"
#include "rgw_keystone.h"
#include "rgw_basic_types.h"

#include "common/ceph_json.h"
#include "common/Formatter.h"

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


void RGWOLHInfo::dump(Formatter *f) const
{
  encode_json("target", target, f);
}

void RGWOLHPendingInfo::dump(Formatter *f) const
{
  encode_json("time", time, f);
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
  ::encode_json("head_obj", head_obj, f);
  ::encode_json("head_size", head_size, f);
  ::encode_json("max_head_size", max_head_size, f);
  ::encode_json("prefix", prefix, f);
  ::encode_json("tail_bucket", tail_bucket, f);
  ::encode_json("rules", rules, f);
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

void RGWAccessControlPolicy::dump(Formatter *f) const
{
  encode_json("acl", acl, f);
  encode_json("owner", owner, f);
}

void ObjectMetaInfo::dump(Formatter *f) const
{
  encode_json("size", size, f);
  encode_json("mtime", mtime, f);
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
 { RGW_PERM_WRITE_ACP, "read-acp" },
 { 0, NULL }
};

static void mask_to_str(rgw_flags_desc *mask_list, uint32_t mask, char *buf, int len)
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
      struct rgw_flags_desc *desc = &mask_list[i];
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

static void op_type_to_str(uint32_t mask, char *buf, int len)
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

  encode_json("auid", auid, f);

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
  encode_json("default_placement", default_placement, f);
  encode_json("placement_tags", placement_tags, f);
  encode_json("bucket_quota", bucket_quota, f);
  encode_json("user_quota", user_quota, f);
  encode_json("temp_url_keys", temp_url_keys, f);
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
  JSONDecoder::decode_json("auid", auid, obj);

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
  JSONDecoder::decode_json("default_placement", default_placement, obj);
  JSONDecoder::decode_json("placement_tags", placement_tags, obj);
  JSONDecoder::decode_json("bucket_quota", bucket_quota, obj);
  JSONDecoder::decode_json("user_quota", user_quota, obj);
  JSONDecoder::decode_json("temp_url_keys", temp_url_keys, obj);
}

void RGWQuotaInfo::dump(Formatter *f) const
{
  f->dump_bool("enabled", enabled);
  f->dump_int("max_size_kb", max_size_kb);
  f->dump_int("max_objects", max_objects);
}

void RGWQuotaInfo::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("max_size_kb", max_size_kb, obj);
  JSONDecoder::decode_json("max_objects", max_objects, obj);
  JSONDecoder::decode_json("enabled", enabled, obj);
}

void rgw_bucket::dump(Formatter *f) const
{
  encode_json("name", name, f);
  encode_json("pool", data_pool, f);
  encode_json("data_extra_pool", data_extra_pool, f);
  encode_json("index_pool", index_pool, f);
  encode_json("marker", marker, f);
  encode_json("bucket_id", bucket_id, f);
}

void rgw_bucket::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("pool", data_pool, obj);
  JSONDecoder::decode_json("data_extra_pool", data_extra_pool, obj);
  JSONDecoder::decode_json("index_pool", index_pool, obj);
  JSONDecoder::decode_json("marker", marker, obj);
  JSONDecoder::decode_json("bucket_id", bucket_id, obj);
}

void RGWBucketEntryPoint::dump(Formatter *f) const
{
  encode_json("bucket", bucket, f);
  encode_json("owner", owner, f);
  encode_json("creation_time", creation_time, f);
  encode_json("linked", linked, f);
  encode_json("has_bucket_info", has_bucket_info, f);
  if (has_bucket_info) {
    encode_json("old_bucket_info", old_bucket_info, f);
  }
}

void RGWBucketEntryPoint::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("bucket", bucket, obj);
  JSONDecoder::decode_json("owner", owner, obj);
  JSONDecoder::decode_json("creation_time", creation_time, obj);
  JSONDecoder::decode_json("linked", linked, obj);
  JSONDecoder::decode_json("has_bucket_info", has_bucket_info, obj);
  if (has_bucket_info) {
    JSONDecoder::decode_json("old_bucket_info", old_bucket_info, obj);
  }
}

void RGWStorageStats::dump(Formatter *f) const
{
  encode_json("num_kb", num_kb, f);
  encode_json("num_kb_rounded", num_kb_rounded, f);
  encode_json("num_objects", num_objects, f);
}

void RGWBucketInfo::dump(Formatter *f) const
{
  encode_json("bucket", bucket, f);
  encode_json("creation_time", creation_time, f);
  encode_json("owner", owner.to_str(), f);
  encode_json("flags", flags, f);
  encode_json("region", region, f);
  encode_json("placement_rule", placement_rule, f);
  encode_json("has_instance_obj", has_instance_obj, f);
  encode_json("quota", quota, f);
  encode_json("num_shards", num_shards, f);
  encode_json("bi_shard_hash_type", (uint32_t)bucket_index_shard_hash_type, f);
  encode_json("requester_pays", requester_pays, f);
}

void RGWBucketInfo::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("bucket", bucket, obj);
  JSONDecoder::decode_json("creation_time", creation_time, obj);
  JSONDecoder::decode_json("owner", owner, obj);
  JSONDecoder::decode_json("flags", flags, obj);
  JSONDecoder::decode_json("region", region, obj);
  JSONDecoder::decode_json("placement_rule", placement_rule, obj);
  JSONDecoder::decode_json("has_instance_obj", has_instance_obj, obj);
  JSONDecoder::decode_json("quota", quota, obj);
  JSONDecoder::decode_json("num_shards", num_shards, obj);
  uint32_t hash_type;
  JSONDecoder::decode_json("bi_shard_hash_type", hash_type, obj);
  bucket_index_shard_hash_type = (uint8_t)hash_type;
  JSONDecoder::decode_json("requester_pays", requester_pays, obj);
}

void RGWObjEnt::dump(Formatter *f) const
{
  encode_json("name", key.name, f);
  encode_json("instance", key.instance, f);
  encode_json("namespace", ns, f);
  encode_json("owner", owner.to_str(), f);
  encode_json("owner_display_name", owner_display_name, f);
  encode_json("size", size, f);
  encode_json("mtime", mtime, f);
  encode_json("etag", etag, f);
  encode_json("content_type", content_type, f);
  encode_json("tag", tag, f);
  encode_json("flags", flags, f);
}

void RGWBucketEnt::dump(Formatter *f) const
{
  encode_json("bucket", bucket, f);
  encode_json("size", size, f);
  encode_json("size_rounded", size_rounded, f);
  encode_json("mtime", creation_time, f); /* mtime / creation time discrepency needed for backward compatibility */
  encode_json("count", count, f);
}

void RGWUploadPartInfo::dump(Formatter *f) const
{
  encode_json("num", num, f);
  encode_json("size", size, f);
  encode_json("etag", etag, f);
  encode_json("modified", modified, f);
}

void rgw_obj::dump(Formatter *f) const
{
  encode_json("bucket", bucket, f);
  encode_json("key", loc, f);
  encode_json("ns", ns, f);
  encode_json("object", object, f);
  encode_json("instance", instance, f);
  encode_json("orig_obj", orig_obj, f);
}

void RGWZoneParams::dump(Formatter *f) const
{
  encode_json("domain_root", domain_root.data_pool, f);
  encode_json("control_pool", control_pool.data_pool, f);
  encode_json("gc_pool", gc_pool.data_pool, f);
  encode_json("log_pool", log_pool.data_pool, f);
  encode_json("intent_log_pool", intent_log_pool.data_pool, f);
  encode_json("usage_log_pool", usage_log_pool.data_pool, f);
  encode_json("user_keys_pool", user_keys_pool.data_pool, f);
  encode_json("user_email_pool", user_email_pool.data_pool, f);
  encode_json("user_swift_pool", user_swift_pool.data_pool, f);
  encode_json("user_uid_pool", user_uid_pool.data_pool, f);
  encode_json_plain("system_key", system_key, f);
  encode_json("placement_pools", placement_pools, f);
}

static void decode_json(const char *field, rgw_bucket& bucket, JSONObj *obj)
{
  string pool;
  JSONDecoder::decode_json(field, pool, obj);
  if (pool[0] != '.') {
    pool = string(".") + pool;
  }
  bucket = rgw_bucket(pool.c_str());
}

void RGWZonePlacementInfo::dump(Formatter *f) const
{
  encode_json("index_pool", index_pool, f);
  encode_json("data_pool", data_pool, f);
  encode_json("data_extra_pool", data_extra_pool, f);
}

void RGWZonePlacementInfo::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("index_pool", index_pool, obj);
  JSONDecoder::decode_json("data_pool", data_pool, obj);
  JSONDecoder::decode_json("data_extra_pool", data_extra_pool, obj);
}

void RGWZoneParams::decode_json(JSONObj *obj)
{
  ::decode_json("domain_root", domain_root, obj);
  ::decode_json("control_pool", control_pool, obj);
  ::decode_json("gc_pool", gc_pool, obj);
  ::decode_json("log_pool", log_pool, obj);
  ::decode_json("intent_log_pool", intent_log_pool, obj);
  ::decode_json("usage_log_pool", usage_log_pool, obj);
  ::decode_json("user_keys_pool", user_keys_pool, obj);
  ::decode_json("user_email_pool", user_email_pool, obj);
  ::decode_json("user_uid_pool", user_uid_pool, obj);
  ::decode_json("user_swift_pool", user_swift_pool, obj);
  JSONDecoder::decode_json("system_key", system_key, obj);
  JSONDecoder::decode_json("placement_pools", placement_pools, obj);
}

void RGWZone::dump(Formatter *f) const
{
  encode_json("name", name, f);
  encode_json("endpoints", endpoints, f);
  encode_json("log_meta", log_meta, f);
  encode_json("log_data", log_data, f);
  encode_json("bucket_index_max_shards", bucket_index_max_shards, f);
}

void RGWZone::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("endpoints", endpoints, obj);
  JSONDecoder::decode_json("log_meta", log_meta, obj);
  JSONDecoder::decode_json("log_data", log_data, obj);
  JSONDecoder::decode_json("bucket_index_max_shards", bucket_index_max_shards, obj);
}

void RGWRegionPlacementTarget::dump(Formatter *f) const
{
  encode_json("name", name, f);
  encode_json("tags", tags, f);
}

void RGWRegionPlacementTarget::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("tags", tags, obj);
}

void RGWRegion::dump(Formatter *f) const
{
  encode_json("name", name, f);
  encode_json("api_name", api_name, f);
  encode_json("is_master", is_master, f);
  encode_json("endpoints", endpoints, f);
  encode_json("hostnames", hostnames, f);
  encode_json("master_zone", master_zone, f);
  encode_json_map("zones", zones, f); /* more friendly representation */
  encode_json_map("placement_targets", placement_targets, f); /* more friendly representation */
  encode_json("default_placement", default_placement, f);
}

static void decode_zones(map<string, RGWZone>& zones, JSONObj *o)
{
  RGWZone z;
  z.decode_json(o);
  zones[z.name] = z;
}

static void decode_placement_targets(map<string, RGWRegionPlacementTarget>& targets, JSONObj *o)
{
  RGWRegionPlacementTarget t;
  t.decode_json(o);
  targets[t.name] = t;
}


void RGWRegion::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("api_name", api_name, obj);
  JSONDecoder::decode_json("is_master", is_master, obj);
  JSONDecoder::decode_json("endpoints", endpoints, obj);
  JSONDecoder::decode_json("hostnames", hostnames, obj);
  JSONDecoder::decode_json("master_zone", master_zone, obj);
  JSONDecoder::decode_json("zones", zones, decode_zones, obj);
  JSONDecoder::decode_json("placement_targets", placement_targets, decode_placement_targets, obj);
  JSONDecoder::decode_json("default_placement", default_placement, obj);
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
  JSONDecoder::decode_json("user_quota", user_quota, obj);
}

void RGWMetadataLogInfo::dump(Formatter *f) const
{
  encode_json("marker", marker, f);
  encode_json("last_update", last_update, f);
}

void RGWMetadataLogInfo::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("marker", marker, obj);
  JSONDecoder::decode_json("last_update", last_update, obj);
}

void RGWDataChangesLogInfo::dump(Formatter *f) const
{
  encode_json("marker", marker, f);
  encode_json("last_update", last_update, f);
}

void RGWDataChangesLogInfo::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("marker", marker, obj);
  JSONDecoder::decode_json("last_update", last_update, obj);
}

void KeystoneToken::Metadata::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("is_admin", is_admin, obj);
}

void KeystoneToken::Service::Endpoint::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("adminURL", admin_url, obj);
  JSONDecoder::decode_json("publicURL", public_url, obj);
  JSONDecoder::decode_json("internalURL", internal_url, obj);
  JSONDecoder::decode_json("region", region, obj);
}

void KeystoneToken::Service::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("type", type, obj, true);
  JSONDecoder::decode_json("name", name, obj, true);
  JSONDecoder::decode_json("endpoints", endpoints, obj);
}

void KeystoneToken::Token::Tenant::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj, true);
  JSONDecoder::decode_json("name", name, obj, true);
  JSONDecoder::decode_json("description", description, obj);
  JSONDecoder::decode_json("enabled", enabled, obj);
}

void KeystoneToken::Token::decode_json(JSONObj *obj)
{
  string expires_iso8601;
  struct tm t;

  JSONDecoder::decode_json("id", id, obj, true);
  JSONDecoder::decode_json("tenant", tenant, obj, true);
  JSONDecoder::decode_json("expires", expires_iso8601, obj, true);

  if (parse_iso8601(expires_iso8601.c_str(), &t)) {
    expires = timegm(&t);
  } else {
    expires = 0;
    throw JSONDecoder::err("Failed to parse ISO8601 expiration date from Keystone response.");
  }
}

void KeystoneToken::User::Role::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("name", name, obj);
}

void KeystoneToken::User::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj, true);
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("username", user_name, obj, true);
  JSONDecoder::decode_json("roles", roles, obj);
}

void KeystoneToken::decode_json(JSONObj *access_obj)
{
  JSONDecoder::decode_json("metadata", metadata, access_obj);
  JSONDecoder::decode_json("token", token, access_obj, true);
  JSONDecoder::decode_json("user", user, access_obj, true);
  JSONDecoder::decode_json("serviceCatalog", service_catalog, access_obj);
}
