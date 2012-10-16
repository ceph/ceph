
#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_log.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_cache.h"

#include "common/Formatter.h"

void RGWObjManifestPart::generate_test_instances(std::list<RGWObjManifestPart*>& o)
{
  o.push_back(new RGWObjManifestPart);

  RGWObjManifestPart *p = new RGWObjManifestPart;
  rgw_bucket b("bucket", ".pool", "marker_", "12");
  p->loc = rgw_obj(b, "object");
  p->loc_ofs = 512 * 1024;
  p->size = 128 * 1024;
  o.push_back(p);
}

void RGWObjManifestPart::dump(Formatter *f) const
{
  f->open_object_section("loc");
  loc.dump(f);
  f->close_section();
  f->dump_unsigned("loc_ofs", loc_ofs);
  f->dump_unsigned("size", size);
}

void RGWObjManifest::generate_test_instances(std::list<RGWObjManifest*>& o)
{
  RGWObjManifest *m = new RGWObjManifest;
  for (int i = 0; i<10; i++) {
    RGWObjManifestPart p;
    rgw_bucket b("bucket", ".pool", "marker_", "12");
    p.loc = rgw_obj(b, "object");
    p.loc_ofs = 0;
    p.size = 512 * 1024;
    m->objs[(uint64_t)i * 512 * 1024] = p;
  }
  m->obj_size = 5 * 1024 * 1024;

  o.push_back(m);

  o.push_back(new RGWObjManifest);
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

void rgw_log_entry::generate_test_instances(list<rgw_log_entry*>& o)
{
  rgw_log_entry *e = new rgw_log_entry;
  e->object_owner = "object_owner";
  e->bucket_owner = "bucket_owner";
  e->bucket = "bucket";
  e->remote_addr = "1.2.3.4";
  e->user = "user";
  e->obj = "obj";
  e->uri = "http://uri/bucket/obj";
  e->http_status = "200";
  e->error_code = "error_code";
  e->bytes_sent = 1024;
  e->bytes_received = 512;
  e->obj_size = 2048;
  e->user_agent = "user_agent";
  e->referrer = "referrer";
  e->bucket_id = "10";
  o.push_back(e);
  o.push_back(new rgw_log_entry);
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

void rgw_intent_log_entry::generate_test_instances(list<rgw_intent_log_entry*>& o)
{
  rgw_intent_log_entry *e = new rgw_intent_log_entry;
  rgw_bucket b("bucket", "pool", "marker", "10");
  e->obj = rgw_obj(b, "object");
  e->intent = DEL_OBJ;
  o.push_back(e);
  o.push_back(new rgw_intent_log_entry);
}

void rgw_intent_log_entry::dump(Formatter *f) const
{
  f->open_object_section("obj");
  obj.dump(f);
  f->close_section();
  f->dump_stream("op_time") << op_time;
  f->dump_unsigned("intent", intent);
}


void ACLPermission::generate_test_instances(list<ACLPermission*>& o)
{
  ACLPermission *p = new ACLPermission;
  p->set_permissions(RGW_PERM_WRITE_ACP);
  o.push_back(p);
  o.push_back(new ACLPermission);
}

void ACLPermission::dump(Formatter *f) const
{
  f->dump_int("flags", flags);
}

void ACLGranteeType::generate_test_instances(list<ACLGranteeType*>& o)
{
  ACLGranteeType *t = new ACLGranteeType;
  t->set(ACL_TYPE_CANON_USER);
  o.push_back(t);
  o.push_back(new ACLGranteeType);
}

void ACLGranteeType::dump(Formatter *f) const
{
  f->dump_unsigned("type", type);
}

/* the following is copied here from rgw_acl_s3.cc, to avoid having to have excessive linking
   with everything it needs */

#define RGW_URI_ALL_USERS	"http://acs.amazonaws.com/groups/global/AllUsers"
#define RGW_URI_AUTH_USERS	"http://acs.amazonaws.com/groups/global/AuthenticatedUsers"

static string rgw_uri_all_users = RGW_URI_ALL_USERS;
static string rgw_uri_auth_users = RGW_URI_AUTH_USERS;

ACLGroupTypeEnum ACLGrant_S3::uri_to_group(string& uri)
{
  if (uri.compare(rgw_uri_all_users) == 0)
    return ACL_GROUP_ALL_USERS;
  else if (uri.compare(rgw_uri_auth_users) == 0)
    return ACL_GROUP_AUTHENTICATED_USERS;

  return ACL_GROUP_NONE;
}

void ACLGrant::generate_test_instances(list<ACLGrant*>& o)
{
  string id, name, email;
  id = "rgw";
  name = "Mr. RGW";
  email = "r@gw";

  ACLGrant *g1 = new ACLGrant;
  g1->set_canon(id, name, RGW_PERM_READ);
  g1->email = email;
  o.push_back(g1);

  ACLGrant *g2 = new ACLGrant;
  g1->set_group(ACL_GROUP_AUTHENTICATED_USERS, RGW_PERM_WRITE);
  o.push_back(g2);

  o.push_back(new ACLGrant);
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

void RGWAccessControlList::generate_test_instances(list<RGWAccessControlList*>& o)
{
  RGWAccessControlList *acl = new RGWAccessControlList(NULL);

  list<ACLGrant *> glist;
  list<ACLGrant *>::iterator iter;

  ACLGrant::generate_test_instances(glist);
  for (iter = glist.begin(); iter != glist.end(); ++iter) {
    ACLGrant *grant = *iter;
    acl->add_grant(grant);

    delete grant;
  }
  o.push_back(acl);
  o.push_back(new RGWAccessControlList(NULL));
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

void ACLOwner::generate_test_instances(list<ACLOwner*>& o)
{
  ACLOwner *owner = new ACLOwner;
  owner->id = "rgw";
  owner->display_name = "Mr. RGW";
  o.push_back(owner);
  o.push_back(new ACLOwner);
}

void ACLOwner::dump(Formatter *f) const
{
  f->dump_string("id", id);
  f->dump_string("display_name", display_name);
}

void RGWAccessControlPolicy::generate_test_instances(list<RGWAccessControlPolicy*>& o)
{
  list<RGWAccessControlList *> acl_list;
  list<RGWAccessControlList *>::iterator iter;
  for (iter = acl_list.begin(); iter != acl_list.end(); ++iter) {
    RGWAccessControlList::generate_test_instances(acl_list);
    iter = acl_list.begin();

    RGWAccessControlPolicy *p = new RGWAccessControlPolicy(NULL);
    RGWAccessControlList *l = *iter;
    p->acl = *l;

    string name = "radosgw";
    string id = "rgw";
    p->owner.set_name(name);
    p->owner.set_id(id);

    o.push_back(p);

    delete l;
  }

  o.push_back(new RGWAccessControlPolicy(NULL));
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


void ObjectMetaInfo::generate_test_instances(list<ObjectMetaInfo*>& o)
{
  ObjectMetaInfo *m = new ObjectMetaInfo;
  m->size = 1024 * 1024;
  o.push_back(m);
  o.push_back(new ObjectMetaInfo);
}

void ObjectMetaInfo::dump(Formatter *f) const
{
  f->dump_unsigned("size", size);
  f->dump_stream("mtime") << mtime;
}

void ObjectCacheInfo::generate_test_instances(list<ObjectCacheInfo*>& o)
{
  ObjectCacheInfo *i = new ObjectCacheInfo;
  i->status = 0;
  i->flags = CACHE_FLAG_MODIFY_XATTRS;
  string s = "this is a string";
  string s2 = "this is a another string";
  bufferlist data, data2;
  ::encode(s, data);
  ::encode(s2, data2);
  i->data = data;
  i->xattrs["x1"] = data;
  i->xattrs["x2"] = data2;
  i->rm_xattrs["r2"] = data2;
  i->rm_xattrs["r3"] = data;
  i->meta.size = 512 * 1024;
  o.push_back(i);
  o.push_back(new ObjectCacheInfo);
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

void RGWCacheNotifyInfo::generate_test_instances(list<RGWCacheNotifyInfo*>& o)
{
  o.push_back(new RGWCacheNotifyInfo);
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

void RGWAccessKey::generate_test_instances(list<RGWAccessKey*>& o)
{
  RGWAccessKey *k = new RGWAccessKey;
  k->id = "id";
  k->key = "key";
  k->subuser = "subuser";
  o.push_back(k);
  o.push_back(new RGWAccessKey);
}

void RGWAccessKey::dump(Formatter *f) const
{
  f->dump_string("id", id);
  f->dump_string("key", key);
  f->dump_string("subuser", subuser);
}

void RGWSubUser::generate_test_instances(list<RGWSubUser*>& o)
{
  RGWSubUser *u = new RGWSubUser;
  u->name = "name";
  u->perm_mask = 0xf;
  o.push_back(u);
  o.push_back(new RGWSubUser);
}

void RGWSubUser::dump(Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_unsigned("perm_mask", perm_mask);
}

void RGWUserInfo::generate_test_instances(list<RGWUserInfo*>& o)
{
  RGWUserInfo *i = new RGWUserInfo;
  i->auid = 1;
  i->user_id = "user_id";
  i->display_name =  "display_name";
  i->user_email = "user@email";
  RGWAccessKey k1, k2;
  k1.id = "id1";
  k1.key = "key1";
  k2.id = "id2";
  k2.subuser = "subuser";
  RGWSubUser u;
  u.name = "id2";
  u.perm_mask = 0x1;
  i->access_keys[k1.id] = k1;
  i->swift_keys[k2.id] = k2;
  i->subusers[u.name] = u;
  o.push_back(i);

  o.push_back(new RGWUserInfo);
}

void RGWUserInfo::dump(Formatter *f) const
{
  f->dump_unsigned("auid", auid);
  f->dump_string("user_id", user_id);
  f->dump_string("display_name", display_name);
  f->dump_string("user_email", user_email);

  map<string, RGWAccessKey>::const_iterator aiter = access_keys.begin();
  f->open_array_section("access_keys");
  for (; aiter != access_keys.end(); ++aiter) {
    f->open_object_section("entry");
    f->dump_string("uid", aiter->first);
    f->open_object_section("access_key");
    aiter->second.dump(f);
    f->close_section();
    f->close_section();
  }

  aiter = swift_keys.begin();
  for (; aiter != swift_keys.end(); ++aiter) {
    f->open_object_section("entry");
    f->dump_string("subuser", aiter->first);
    f->open_object_section("key");
    aiter->second.dump(f);
    f->close_section();
    f->close_section();
  }
  map<string, RGWSubUser>::const_iterator siter = subusers.begin();
  for (; siter != subusers.end(); ++siter) {
    f->open_object_section("entry");
    f->dump_string("id", siter->first);
    f->open_object_section("subuser");
    siter->second.dump(f);
    f->close_section();
    f->close_section();
  }
  f->dump_int("suspended", (int)suspended);
}

void rgw_bucket::generate_test_instances(list<rgw_bucket*>& o)
{
  rgw_bucket *b = new rgw_bucket("name", "pool", "marker", "123");
  o.push_back(b);
  o.push_back(new rgw_bucket);
}

void rgw_bucket::dump(Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_string("pool", pool);
  f->dump_string("marker", marker);
  f->dump_string("bucket_id", bucket_id);
}

void RGWBucketInfo::generate_test_instances(list<RGWBucketInfo*>& o)
{
  RGWBucketInfo *i = new RGWBucketInfo;
  i->bucket = rgw_bucket("bucket", "pool", "marker", "10");
  i->owner = "owner";
  i->flags = BUCKET_SUSPENDED;
  o.push_back(i);
  o.push_back(new RGWBucketInfo);
}

void RGWBucketInfo::dump(Formatter *f) const
{
  f->open_object_section("bucket");
  bucket.dump(f);
  f->close_section();
  f->dump_string("owner", owner);
  f->dump_unsigned("flags", flags);
}

void RGWBucketEnt::generate_test_instances(list<RGWBucketEnt*>& o)
{
  RGWBucketEnt *e = new RGWBucketEnt;
  e->bucket = rgw_bucket("bucket", "pool", "marker", "10");
  e->size = 1024;
  e->size_rounded = 4096;
  e->count = 1;
  o.push_back(e);
  o.push_back(new RGWBucketEnt);
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

void RGWUploadPartInfo::generate_test_instances(list<RGWUploadPartInfo*>& o)
{
  RGWUploadPartInfo *i = new RGWUploadPartInfo;
  i->num = 1;
  i->size = 10 * 1024 * 1024;
  i->etag = "etag";
  o.push_back(i);
  o.push_back(new RGWUploadPartInfo);
}

void RGWUploadPartInfo::dump(Formatter *f) const
{
  f->dump_unsigned("num", num);
  f->dump_unsigned("size", size);
  f->dump_string("etag", etag);
  f->dump_stream("modified") << modified;
}

void rgw_obj::generate_test_instances(list<rgw_obj*>& o)
{
  rgw_bucket b = rgw_bucket("bucket", "pool", "marker", "10");
  rgw_obj *obj = new rgw_obj(b, "object");
  o.push_back(obj);
  o.push_back(new rgw_obj);
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


