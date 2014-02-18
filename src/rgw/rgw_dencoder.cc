
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
  rgw_bucket b("bucket", ".pool", ".index_pool", "marker_", "12", "region");
  p->loc = rgw_obj(b, "object");
  p->loc_ofs = 512 * 1024;
  p->size = 128 * 1024;
  o.push_back(p);
}

void RGWObjManifest::generate_test_instances(std::list<RGWObjManifest*>& o)
{
  RGWObjManifest *m = new RGWObjManifest;
  for (int i = 0; i<10; i++) {
    RGWObjManifestPart p;
    rgw_bucket b("bucket", ".pool", ".index_pool", "marker_", "12", "region");
    p.loc = rgw_obj(b, "object");
    p.loc_ofs = 0;
    p.size = 512 * 1024;
    m->objs[(uint64_t)i * 512 * 1024] = p;
  }
  m->obj_size = 5 * 1024 * 1024;

  o.push_back(m);

  o.push_back(new RGWObjManifest);
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

void rgw_intent_log_entry::generate_test_instances(list<rgw_intent_log_entry*>& o)
{
  rgw_intent_log_entry *e = new rgw_intent_log_entry;
  rgw_bucket b("bucket", "pool", ".index_pool", "marker", "10", "region");
  e->obj = rgw_obj(b, "object");
  e->intent = DEL_OBJ;
  o.push_back(e);
  o.push_back(new rgw_intent_log_entry);
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
    string id = "rgw";
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

void rgw_bucket::generate_test_instances(list<rgw_bucket*>& o)
{
  rgw_bucket *b = new rgw_bucket("name", "pool", ".index_pool", "marker", "123", "region");
  o.push_back(b);
  o.push_back(new rgw_bucket);
}

void RGWBucketInfo::generate_test_instances(list<RGWBucketInfo*>& o)
{
  RGWBucketInfo *i = new RGWBucketInfo;
  i->bucket = rgw_bucket("bucket", "pool", ".index_pool", "marker", "10", "region");
  i->owner = "owner";
  i->flags = BUCKET_SUSPENDED;
  o.push_back(i);
  o.push_back(new RGWBucketInfo);
}

void RGWBucketEnt::generate_test_instances(list<RGWBucketEnt*>& o)
{
  RGWBucketEnt *e = new RGWBucketEnt;
  e->bucket = rgw_bucket("bucket", "pool", ".index_pool", "marker", "10", "region");
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
  rgw_bucket b = rgw_bucket("bucket", "pool", ".index_pool", "marker", "10", "region");
  rgw_obj *obj = new rgw_obj(b, "object");
  o.push_back(obj);
  o.push_back(new rgw_obj);
}

