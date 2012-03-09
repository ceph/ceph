#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "common/Formatter.h"

#include "rgw_acl.h"
#include "rgw_user.h"

#include "rgw_acl_s3.h" // required for backward compatibility

#define DOUT_SUBSYS rgw

using namespace std;


void ACLPermission::generate_test_instances(list<ACLPermission*>& o)
{
  o.push_back(new ACLPermission);
}

void ACLPermission::dump(Formatter *f) const
{
  f->dump_int("flags", flags);
}

void ACLGranteeType::generate_test_instances(list<ACLGranteeType*>& o)
{
  o.push_back(new ACLGranteeType);
}

void ACLGranteeType::dump(Formatter *f) const
{
  f->dump_unsigned("type", type);
}

void ACLGrant::generate_test_instances(list<ACLGrant*>& o)
{
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
  o.push_back(new RGWAccessControlList);
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
  o.push_back(new ACLOwner);
}

void ACLOwner::dump(Formatter *f) const
{
  f->dump_string("id", id);
  f->dump_string("display_name", display_name);
}

void RGWAccessControlPolicy::generate_test_instances(list<RGWAccessControlPolicy*>& o)
{
  o.push_back(new RGWAccessControlPolicy);
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

void RGWAccessControlList::_add_grant(ACLGrant *grant)
{
  ACLPermission& perm = grant->get_permission();
  ACLGranteeType& type = grant->get_type();
  switch (type.get_type()) {
  case ACL_TYPE_GROUP:
    acl_group_map[grant->get_group()] |= perm.get_permissions();
    break;
  default:
    {
      string id;
      if (!grant->get_id(id)) {
        dout(0) << "ERROR: grant->get_id() failed" << dendl;
      }
      acl_user_map[id] |= perm.get_permissions();
    }
  }
}

void RGWAccessControlList::add_grant(ACLGrant *grant)
{
  string id;
  grant->get_id(id); // not that this will return false for groups, but that's ok, we won't search groups
  grant_map.insert(pair<string, ACLGrant>(id, *grant));
  _add_grant(grant);
}

int RGWAccessControlList::get_perm(string& id, int perm_mask) {
  dout(5) << "Searching permissions for uid=" << id << " mask=" << perm_mask << dendl;
  map<string, int>::iterator iter = acl_user_map.find(id);
  if (iter != acl_user_map.end()) {
    dout(5) << "Found permission: " << iter->second << dendl;
    return iter->second & perm_mask;
  }
  dout(5) << "Permissions for user not found" << dendl;
  return 0;
}

int RGWAccessControlList::get_group_perm(ACLGroupTypeEnum group, int perm_mask) {
  dout(5) << "Searching permissions for group=" << (int)group << " mask=" << perm_mask << dendl;
  map<uint32_t, int>::iterator iter = acl_group_map.find((uint32_t)group);
  if (iter != acl_group_map.end()) {
    dout(5) << "Found permission: " << iter->second << dendl;
    return iter->second & perm_mask;
  }
  dout(5) << "Permissions for group not found" << dendl;
  return 0;
}

int RGWAccessControlPolicy::get_perm(string& id, int perm_mask) {
  int perm = acl.get_perm(id, perm_mask);

  if (id.compare(owner.get_id()) == 0) {
    perm |= perm_mask & (RGW_PERM_READ_ACP | RGW_PERM_WRITE_ACP);
  }

  if (perm == perm_mask)
    return perm;

  /* should we continue looking up? */
  if ((perm & perm_mask) != perm_mask) {
    perm |= acl.get_group_perm(ACL_GROUP_ALL_USERS, perm_mask);

    if (compare_group_name(id, ACL_GROUP_ALL_USERS) != 0) {
      /* this is not the anonymous user */
      perm |= acl.get_group_perm(ACL_GROUP_AUTHENTICATED_USERS, perm_mask);
    }
  }

  dout(5) << "Getting permissions id=" << id << " owner=" << owner.get_id() << " perm=" << perm << dendl;

  return perm;
}

bool RGWAccessControlPolicy::verify_permission(string& uid, int user_perm_mask, int perm)
{
  int test_perm = perm;

  int policy_perm = get_perm(uid, test_perm);

  /* the swift WRITE_OBJS perm is equivalent to the WRITE obj, just
     convert those bits. Note that these bits will only be set on
     buckets, so the swift READ permission on bucket will allow listing
     the bucket content */
  if (policy_perm & RGW_PERM_WRITE_OBJS) {
    policy_perm |= (RGW_PERM_WRITE | RGW_PERM_WRITE_ACP);
  }
  if (policy_perm & RGW_PERM_READ_OBJS) {
    policy_perm |= (RGW_PERM_READ | RGW_PERM_READ_ACP);
  }
   
  int acl_perm = policy_perm & user_perm_mask;

  dout(10) << " uid=" << uid << " requested perm (type)=" << perm << ", policy perm=" << policy_perm << ", user_perm_mask=" << user_perm_mask << ", acl perm=" << acl_perm << dendl;

  return (perm == acl_perm);
}


ACLGroupTypeEnum ACLGrant::uri_to_group(string& uri)
{
  // this is required for backward compatibility
  return ACLGrant_S3::uri_to_group(uri);
}
