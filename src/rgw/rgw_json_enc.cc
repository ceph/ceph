
#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_log.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_cache.h"

#include "common/ceph_json.h"
#include "common/Formatter.h"

void RGWObjManifestPart::dump(Formatter *f) const
{
  f->open_object_section("loc");
  loc.dump(f);
  f->close_section();
  f->dump_unsigned("loc_ofs", loc_ofs);
  f->dump_unsigned("size", size);
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
}

void rgw_log_entry::dump(Formatter *f) const
{
  f->dump_string("object_owner", object_owner);
  f->dump_string("bucket_owner", bucket_owner);
  f->dump_string("bucket", bucket);
  f->dump_stream("time") << time;
  f->dump_string("remote_addr", remote_addr);
  f->dump_string("user", user);
  f->dump_string("obj", obj);
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

void rgw_intent_log_entry::dump(Formatter *f) const
{
  f->open_object_section("obj");
  obj.dump(f);
  f->close_section();
  f->dump_stream("op_time") << op_time;
  f->dump_unsigned("intent", intent);
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

  f->dump_string("id", id);
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
  f->dump_string("id", id);
  f->dump_string("display_name", display_name);
}

void RGWAccessControlPolicy::dump(Formatter *f) const
{
  f->open_object_section("acl");
  acl.dump(f);
  f->close_section();
  f->open_object_section("owner");
  owner.dump(f);
  f->close_section();
}

void ObjectMetaInfo::dump(Formatter *f) const
{
  f->dump_unsigned("size", size);
  f->dump_stream("mtime") << mtime;
}

void ObjectCacheInfo::dump(Formatter *f) const
{
  f->dump_int("status", status);
  f->dump_unsigned("flags", flags);
  f->open_object_section("data");
  f->dump_unsigned("length", data.length());
  f->close_section();

  map<string, bufferlist>::const_iterator iter = xattrs.begin();
  f->open_array_section("xattrs");
  for (; iter != xattrs.end(); ++iter) {
    f->dump_string("name", iter->first);
    f->open_object_section("value");
    f->dump_unsigned("length", iter->second.length());
    f->close_section();
  }
  f->close_section();

  f->open_array_section("rm_xattrs");
  for (iter = rm_xattrs.begin(); iter != rm_xattrs.end(); ++iter) {
    f->dump_string("name", iter->first);
    f->open_object_section("value");
    f->dump_unsigned("length", iter->second.length());
    f->close_section();
  }
  f->close_section();
  f->open_object_section("meta");
  meta.dump(f);
  f->close_section();

}

void RGWCacheNotifyInfo::dump(Formatter *f) const
{
  f->dump_unsigned("op", op);
  f->open_object_section("obj");
  obj.dump(f);
  f->close_section();
  f->open_object_section("obj_info");
  obj_info.dump(f);
  f->close_section();
  f->dump_unsigned("ofs", ofs);
  f->dump_string("ns", ns);
}

void RGWAccessKey::dump(Formatter *f) const
{
  f->open_object_section("key");
  f->dump_string("access_key", id);
  f->dump_string("secret_key", key);
  f->dump_string("subuser", subuser);
  f->close_section();
}

void RGWAccessKey::dump(Formatter *f, const string& user, bool swift) const
{
  f->open_object_section("key");
  string u = user;
  if (!subuser.empty()) {
    u.append(":");
    u.append(subuser);
  }
  f->dump_string("user", u);
  if (!swift) {
    f->dump_string("access_key", id);
  }
  f->dump_string("secret_key", key);
  f->close_section();
}

void RGWAccessKey::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("access_key", id, obj, true);
  JSONDecoder::decode_json("secret_key", key, obj, true);
  if (!JSONDecoder::decode_json("subuser", subuser, obj)) {
    string user;
    JSONDecoder::decode_json("user", user, obj, true);
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
    string user;
    JSONDecoder::decode_json("user", user, obj, true);
    int pos = user.find(':');
    if (pos >= 0) {
      subuser = user.substr(pos + 1);
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

static void perm_to_str(uint32_t mask, char *buf, int len)
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

void RGWSubUser::dump(Formatter *f) const
{
  f->open_object_section("subuser");
  f->dump_string("id", name);
  char buf[256];
  perm_to_str(perm_mask, buf, sizeof(buf));
  f->dump_string("permissions", buf);
  f->close_section();
}

void RGWSubUser::dump(Formatter *f, const string& user) const
{
  f->open_object_section("subuser");
  string s = user;
  s.append(":");
  s.append(name);
  f->dump_string("id", s);
  char buf[256];
  perm_to_str(perm_mask, buf, sizeof(buf));
  f->dump_string("permissions", buf);
  f->close_section();
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

void RGWUserInfo::dump(Formatter *f) const
{
  f->open_object_section("user_info");

  f->dump_string("user_id", user_id);
  f->dump_string("display_name", display_name);
  f->dump_string("email", user_email);
  f->dump_int("suspended", (int)suspended);
  f->dump_int("max_buckets", (int)max_buckets);

  f->dump_unsigned("auid", auid);

  map<string, RGWSubUser>::const_iterator siter = subusers.begin();
  f->open_array_section("subusers");
  for (; siter != subusers.end(); ++siter) {
    siter->second.dump(f, user_id);
  }
  f->close_section();

  map<string, RGWAccessKey>::const_iterator aiter = access_keys.begin();
  f->open_array_section("keys");
  for (; aiter != access_keys.end(); ++aiter) {
    aiter->second.dump(f, user_id, false);
  }
  f->close_section();

  aiter = swift_keys.begin();
  f->open_array_section("swift_keys");
  for (; aiter != swift_keys.end(); ++aiter) {
    aiter->second.dump(f, user_id, true);
  }
  f->close_section();

  caps.dump(f);

  f->close_section();
}


struct SwiftKeyEntry {
  RGWAccessKey key;

  void decode_json(JSONObj *obj) {
    key.decode_json(obj, true);
  }
};

void RGWUserInfo::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("user_id", user_id, obj, true);
  JSONDecoder::decode_json("display_name", display_name, obj);
  JSONDecoder::decode_json("email", user_email, obj);
  bool susp;
  JSONDecoder::decode_json("suspended", susp, obj);
  suspended = (__u8)susp;
  JSONDecoder::decode_json("max_buckets", max_buckets, obj);
  JSONDecoder::decode_json("auid", auid, obj);

  list<RGWAccessKey> akeys_list;
  JSONDecoder::decode_json("keys", akeys_list, obj);

  list<RGWAccessKey>::iterator iter;
  for (iter = akeys_list.begin(); iter != akeys_list.end(); ++iter) {
    RGWAccessKey& e = *iter;
    access_keys[e.id] = e;
  }

  list<SwiftKeyEntry> skeys_list;
  list<SwiftKeyEntry>::iterator skiter;
  JSONDecoder::decode_json("swift_keys", skeys_list, obj);

  for (skiter = skeys_list.begin(); skiter != skeys_list.end(); ++skiter) {
    SwiftKeyEntry& e = *skiter;
    swift_keys[e.key.subuser] = e.key;
  }

  list<RGWSubUser> susers_list;
  list<RGWSubUser>::iterator siter;
  JSONDecoder::decode_json("subusers", susers_list, obj);

  for (siter = susers_list.begin(); siter != susers_list.end(); ++siter) {
    RGWSubUser& e = *siter;
    subusers[e.name] = e;
  }

  JSONDecoder::decode_json("caps", caps, obj);
}

void rgw_bucket::dump(Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_string("pool", pool);
  f->dump_string("marker", marker);
  f->dump_string("bucket_id", bucket_id);
}

void RGWBucketInfo::dump(Formatter *f) const
{
  f->open_object_section("bucket");
  bucket.dump(f);
  f->close_section();
  f->dump_string("owner", owner);
  f->dump_unsigned("flags", flags);
}

void RGWBucketEnt::dump(Formatter *f) const
{
  f->open_object_section("bucket");
  bucket.dump(f);
  f->close_section();
  f->dump_unsigned("size", size);
  f->dump_unsigned("size_rounded", size_rounded);
  f->dump_stream("mtime") << mtime;
  f->dump_unsigned("count", count);
}

void RGWUploadPartInfo::dump(Formatter *f) const
{
  f->dump_unsigned("num", num);
  f->dump_unsigned("size", size);
  f->dump_string("etag", etag);
  f->dump_stream("modified") << modified;
}

void rgw_obj::dump(Formatter *f) const
{
  f->open_object_section("bucket");
  bucket.dump(f);
  f->close_section();
  f->dump_string("key", key);
  f->dump_string("ns", ns);
  f->dump_string("object", object);
}


