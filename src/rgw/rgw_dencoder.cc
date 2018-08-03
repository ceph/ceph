// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_zone.h"
#include "rgw_log.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_cache.h"
#include "rgw_meta_sync_status.h"
#include "rgw_data_sync.h"

#include "common/Formatter.h"

static string shadow_ns = RGW_OBJ_NS_SHADOW;

static void init_bucket(rgw_bucket *b, const char *t, const char *n, const char *dp, const char *ip, const char *m, const char *id)
{
  b->tenant = t;
  b->name = n;
  b->marker = m;
  b->bucket_id = id;
  b->explicit_placement.data_pool = rgw_pool(dp);
  b->explicit_placement.index_pool = rgw_pool(ip);
}

void RGWObjManifestPart::generate_test_instances(std::list<RGWObjManifestPart*>& o)
{
  o.push_back(new RGWObjManifestPart);

  RGWObjManifestPart *p = new RGWObjManifestPart;
  rgw_bucket b;
  init_bucket(&b, "tenant", "bucket", ".pool", ".index_pool", "marker_", "12");

  p->loc = rgw_obj(b, "object");
  p->loc_ofs = 512 * 1024;
  p->size = 128 * 1024;
  o.push_back(p);
}

void RGWObjManifest::obj_iterator::seek(uint64_t o)
{
  ofs = o;
  if (manifest->explicit_objs) {
    explicit_iter = manifest->objs.upper_bound(ofs);
    if (explicit_iter != manifest->objs.begin()) {
      --explicit_iter;
    }
    if (ofs >= manifest->obj_size) {
      ofs = manifest->obj_size;
      return;
    }
    update_explicit_pos();
    update_location();
    return;
  }
  if (o < manifest->get_head_size()) {
    rule_iter = manifest->rules.begin();
    stripe_ofs = 0;
    stripe_size = manifest->get_head_size();
    if (rule_iter != manifest->rules.end()) {
      cur_part_id = rule_iter->second.start_part_num;
      cur_override_prefix = rule_iter->second.override_prefix;
    }
    update_location();
    return;
  }

  rule_iter = manifest->rules.upper_bound(ofs);
  next_rule_iter = rule_iter;
  if (rule_iter != manifest->rules.begin()) {
    --rule_iter;
  }

  if (rule_iter == manifest->rules.end()) {
    update_location();
    return;
  }

  RGWObjManifestRule& rule = rule_iter->second;

  if (rule.part_size > 0) {
    cur_part_id = rule.start_part_num + (ofs - rule.start_ofs) / rule.part_size;
  } else {
    cur_part_id = rule.start_part_num;
  }
  part_ofs = rule.start_ofs + (cur_part_id - rule.start_part_num) * rule.part_size;

  if (rule.stripe_max_size > 0) {
    cur_stripe = (ofs - part_ofs) / rule.stripe_max_size;

    stripe_ofs = part_ofs + cur_stripe * rule.stripe_max_size;
    if (!cur_part_id && manifest->get_head_size() > 0) {
      cur_stripe++;
    }
  } else {
    cur_stripe = 0;
    stripe_ofs = part_ofs;
  }

  if (!rule.part_size) {
    stripe_size = rule.stripe_max_size;
    stripe_size = std::min(manifest->get_obj_size() - stripe_ofs, stripe_size);
  } else {
    uint64_t next = std::min(stripe_ofs + rule.stripe_max_size, part_ofs + rule.part_size);
    stripe_size = next - stripe_ofs;
  }

  cur_override_prefix = rule.override_prefix;

  update_location();
}

void RGWObjManifest::obj_iterator::update_location()
{
  if (manifest->explicit_objs) {
    location = explicit_iter->second.loc;
    return;
  }

  if (ofs < manifest->get_head_size()) {
    location = manifest->get_obj();
    location.set_placement_rule(manifest->get_head_placement_rule());
    return;
  }

  manifest->get_implicit_location(cur_part_id, cur_stripe, ofs, &cur_override_prefix, &location);
}

void RGWObjManifest::obj_iterator::update_explicit_pos()
{
  ofs = explicit_iter->first;
  stripe_ofs = ofs;

  map<uint64_t, RGWObjManifestPart>::iterator next_iter = explicit_iter;
  ++next_iter;
  if (next_iter != manifest->objs.end()) {
    stripe_size = next_iter->first - ofs;
  } else {
    stripe_size = manifest->obj_size - ofs;
  }
}

void RGWObjManifest::generate_test_instances(std::list<RGWObjManifest*>& o)
{
  RGWObjManifest *m = new RGWObjManifest;
  for (int i = 0; i<10; i++) {
    RGWObjManifestPart p;
    rgw_bucket b;
    init_bucket(&b, "tenant", "bucket", ".pool", ".index_pool", "marker_", "12");
    p.loc = rgw_obj(b, "object");
    p.loc_ofs = 0;
    p.size = 512 * 1024;
    m->objs[(uint64_t)i * 512 * 1024] = p;
  }
  m->obj_size = 5 * 1024 * 1024;

  o.push_back(m);

  o.push_back(new RGWObjManifest);
}

void RGWObjManifest::get_implicit_location(uint64_t cur_part_id, uint64_t cur_stripe, uint64_t ofs, string *override_prefix, rgw_obj_select *location)
{
  rgw_obj loc;

  string& oid = loc.key.name;
  string& ns = loc.key.ns;

  if (!override_prefix || override_prefix->empty()) {
    oid = prefix;
  } else {
    oid = *override_prefix;
  }

  if (!cur_part_id) {
    if (ofs < max_head_size) {
      location->set_placement_rule(head_placement_rule);
      *location = obj;
      return;
    } else {
      char buf[16];
      snprintf(buf, sizeof(buf), "%d", (int)cur_stripe);
      oid += buf;
      ns = shadow_ns;
    }
  } else {
    char buf[32];
    if (cur_stripe == 0) {
      snprintf(buf, sizeof(buf), ".%d", (int)cur_part_id);
      oid += buf;
      ns= RGW_OBJ_NS_MULTIPART;
    } else {
      snprintf(buf, sizeof(buf), ".%d_%d", (int)cur_part_id, (int)cur_stripe);
      oid += buf;
      ns = shadow_ns;
    }
  }

  if (!tail_placement.bucket.name.empty()) {
    loc.bucket = tail_placement.bucket;
  } else {
    loc.bucket = obj.bucket;
  }

  // Always overwrite instance with tail_instance
  // to get the right shadow object location
  loc.key.set_instance(tail_instance);

  location->set_placement_rule(tail_placement.placement_rule);
  *location = loc;
}



void rgw_log_entry::generate_test_instances(list<rgw_log_entry*>& o)
{
  rgw_log_entry *e = new rgw_log_entry;
  e->object_owner = "object_owner";
  e->bucket_owner = "bucket_owner";
  e->bucket = "bucket";
  e->remote_addr = "1.2.3.4";
  e->user = "user";
  e->obj = rgw_obj_key("obj");
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

void ACLPermission::generate_test_instances(list<ACLPermission*>& o)
{
  ACLPermission *p = new ACLPermission;
  p->set_permissions(RGW_PERM_WRITE_ACP);
  o.push_back(p);
  o.push_back(new ACLPermission);
}

void ACLGranteeType::generate_test_instances(list<ACLGranteeType*>& o)
{
  ACLGranteeType *t = new ACLGranteeType;
  t->set(ACL_TYPE_CANON_USER);
  o.push_back(t);
  o.push_back(new ACLGranteeType);
}

/* the following is copied here from rgw_acl_s3.cc, to avoid having to have excessive linking
   with everything it needs */

#define RGW_URI_ALL_USERS	"http://acs.amazonaws.com/groups/global/AllUsers"
#define RGW_URI_AUTH_USERS	"http://acs.amazonaws.com/groups/global/AuthenticatedUsers"

static string rgw_uri_all_users = RGW_URI_ALL_USERS;
static string rgw_uri_auth_users = RGW_URI_AUTH_USERS;

ACLGroupTypeEnum ACLGrant::uri_to_group(string& uri)
{
  // this is required for backward compatibility
  return ACLGrant_S3::uri_to_group(uri);
}

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
  rgw_user id("rgw");
  string name, email;
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

void ACLOwner::generate_test_instances(list<ACLOwner*>& o)
{
  ACLOwner *owner = new ACLOwner;
  owner->id = "rgw";
  owner->display_name = "Mr. RGW";
  o.push_back(owner);
  o.push_back(new ACLOwner);
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
    rgw_user id("rgw");
    p->owner.set_name(name);
    p->owner.set_id(id);

    o.push_back(p);

    delete l;
  }

  o.push_back(new RGWAccessControlPolicy(NULL));
}


void ObjectMetaInfo::generate_test_instances(list<ObjectMetaInfo*>& o)
{
  ObjectMetaInfo *m = new ObjectMetaInfo;
  m->size = 1024 * 1024;
  o.push_back(m);
  o.push_back(new ObjectMetaInfo);
}

void ObjectCacheInfo::generate_test_instances(list<ObjectCacheInfo*>& o)
{
  using ceph::encode;
  ObjectCacheInfo *i = new ObjectCacheInfo;
  i->status = 0;
  i->flags = CACHE_FLAG_MODIFY_XATTRS;
  string s = "this is a string";
  string s2 = "this is a another string";
  bufferlist data, data2;
  encode(s, data);
  encode(s2, data2);
  i->data = data;
  i->xattrs["x1"] = data;
  i->xattrs["x2"] = data2;
  i->rm_xattrs["r2"] = data2;
  i->rm_xattrs["r3"] = data;
  i->meta.size = 512 * 1024;
  o.push_back(i);
  o.push_back(new ObjectCacheInfo);
}

void RGWCacheNotifyInfo::generate_test_instances(list<RGWCacheNotifyInfo*>& o)
{
  o.push_back(new RGWCacheNotifyInfo);
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

void RGWSubUser::generate_test_instances(list<RGWSubUser*>& o)
{
  RGWSubUser *u = new RGWSubUser;
  u->name = "name";
  u->perm_mask = 0xf;
  o.push_back(u);
  o.push_back(new RGWSubUser);
}

void RGWUserInfo::generate_test_instances(list<RGWUserInfo*>& o)
{
  RGWUserInfo *i = new RGWUserInfo;
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

void rgw_bucket::generate_test_instances(list<rgw_bucket*>& o)
{
  rgw_bucket *b = new rgw_bucket;
  init_bucket(b, "tenant", "name", "pool", ".index_pool", "marker", "123");
  o.push_back(b);
  o.push_back(new rgw_bucket);
}

void RGWBucketInfo::generate_test_instances(list<RGWBucketInfo*>& o)
{
  RGWBucketInfo *i = new RGWBucketInfo;
  init_bucket(&i->bucket, "tenant", "bucket", "pool", ".index_pool", "marker", "10");
  i->owner = "owner";
  i->flags = BUCKET_SUSPENDED;
  o.push_back(i);
  o.push_back(new RGWBucketInfo);
}

void RGWZoneGroup::generate_test_instances(list<RGWZoneGroup*>& o)
{
  RGWZoneGroup *r = new RGWZoneGroup;
  o.push_back(r);
  o.push_back(new RGWZoneGroup);
}

void RGWZone::generate_test_instances(list<RGWZone*> &o)
{
  RGWZone *z = new RGWZone;
  o.push_back(z);
  o.push_back(new RGWZone);
}

void RGWRealm::generate_test_instances(list<RGWRealm*> &o)
{
  RGWRealm *z = new RGWRealm;
  o.push_back(z);
  o.push_back(new RGWRealm);
}

void RGWPeriod::generate_test_instances(list<RGWPeriod*> &o)
{
  RGWPeriod *z = new RGWPeriod;
  o.push_back(z);
  o.push_back(new RGWPeriod);
}

void RGWZoneParams::generate_test_instances(list<RGWZoneParams*> &o)
{
  o.push_back(new RGWZoneParams);
  o.push_back(new RGWZoneParams); 
}

void RGWOLHInfo::generate_test_instances(list<RGWOLHInfo*> &o)
{
  RGWOLHInfo *olh = new RGWOLHInfo;
  olh->removed = false;
  o.push_back(olh);
  o.push_back(new RGWOLHInfo);
}

void RGWBucketEnt::generate_test_instances(list<RGWBucketEnt*>& o)
{
  RGWBucketEnt *e = new RGWBucketEnt;
  init_bucket(&e->bucket, "tenant", "bucket", "pool", ".index_pool", "marker", "10");
  e->size = 1024;
  e->size_rounded = 4096;
  e->count = 1;
  o.push_back(e);
  o.push_back(new RGWBucketEnt);
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

void rgw_obj::generate_test_instances(list<rgw_obj*>& o)
{
  rgw_bucket b;
  init_bucket(&b, "tenant", "bucket", "pool", ".index_pool", "marker", "10");
  rgw_obj *obj = new rgw_obj(b, "object");
  o.push_back(obj);
  o.push_back(new rgw_obj);
}

void rgw_meta_sync_info::generate_test_instances(list<rgw_meta_sync_info*>& o)
{
  auto info = new rgw_meta_sync_info;
  info->state = rgw_meta_sync_info::StateBuildingFullSyncMaps;
  info->period = "periodid";
  info->realm_epoch = 5;
  o.push_back(info);
  o.push_back(new rgw_meta_sync_info);
}

void rgw_meta_sync_marker::generate_test_instances(list<rgw_meta_sync_marker*>& o)
{
  auto marker = new rgw_meta_sync_marker;
  marker->state = rgw_meta_sync_marker::IncrementalSync;
  marker->marker = "01234";
  marker->realm_epoch = 5;
  o.push_back(marker);
  o.push_back(new rgw_meta_sync_marker);
}

void rgw_meta_sync_status::generate_test_instances(list<rgw_meta_sync_status*>& o)
{
  o.push_back(new rgw_meta_sync_status);
}

void rgw_data_sync_info::generate_test_instances(list<rgw_data_sync_info*>& o)
{
  auto info = new rgw_data_sync_info;
  info->state = rgw_data_sync_info::StateBuildingFullSyncMaps;
  info->num_shards = 8;
  o.push_back(info);
  o.push_back(new rgw_data_sync_info);
}

void rgw_data_sync_marker::generate_test_instances(list<rgw_data_sync_marker*>& o)
{
  auto marker = new rgw_data_sync_marker;
  marker->state = rgw_data_sync_marker::IncrementalSync;
  marker->marker = "01234";
  marker->pos = 5;
  o.push_back(marker);
  o.push_back(new rgw_data_sync_marker);
}

void rgw_data_sync_status::generate_test_instances(list<rgw_data_sync_status*>& o)
{
  o.push_back(new rgw_data_sync_status);
}

void RGWBucketEntryPoint::generate_test_instances(list<RGWBucketEntryPoint*>& o)
{
  RGWBucketEntryPoint *bp = new RGWBucketEntryPoint();
  init_bucket(&bp->bucket, "tenant", "bucket", "pool", ".index.pool", "marker", "10");
  bp->owner = "owner";
  bp->creation_time = ceph::real_clock::from_ceph_timespec({{2}, {3}});

  o.push_back(bp);
  o.push_back(new RGWBucketEntryPoint);
}

void rgw_user::generate_test_instances(list<rgw_user*>& o)
{
  rgw_user *u = new rgw_user("tenant", "user");

  o.push_back(u);
  o.push_back(new rgw_user);
}

void obj_version::generate_test_instances(list<obj_version*>& o)
{
  obj_version *v = new obj_version;
  v->ver = 5;
  v->tag = "tag";

  o.push_back(v);
  o.push_back(new obj_version);
}
