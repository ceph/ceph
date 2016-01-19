// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "common/Formatter.h"

#include "rgw_acl.h"
#include "rgw_user.h"

#include "rgw_acl_s3.h" // required for backward compatibility

#define dout_subsys ceph_subsys_rgw

using namespace std;

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
      rgw_user id;
      if (!grant->get_id(id)) {
        ldout(cct, 0) << "ERROR: grant->get_id() failed" << dendl;
      }
      acl_user_map[id.to_str()] |= perm.get_permissions();
    }
  }
}

void RGWAccessControlList::add_grant(ACLGrant *grant)
{
  rgw_user id;
  grant->get_id(id); // not that this will return false for groups, but that's ok, we won't search groups
  grant_map.insert(pair<string, ACLGrant>(id.to_str(), *grant));
  _add_grant(grant);
}

int RGWAccessControlList::get_perm(rgw_user& id, int perm_mask) {
  ldout(cct, 5) << "Searching permissions for uid=" << id << " mask=" << perm_mask << dendl;
  map<string, int>::iterator iter = acl_user_map.find(id.to_str());
  if (iter != acl_user_map.end()) {
    ldout(cct, 5) << "Found permission: " << iter->second << dendl;
    return iter->second & perm_mask;
  }
  ldout(cct, 5) << "Permissions for user not found" << dendl;
  return 0;
}

int RGWAccessControlList::get_group_perm(ACLGroupTypeEnum group, int perm_mask) {
  ldout(cct, 5) << "Searching permissions for group=" << (int)group << " mask=" << perm_mask << dendl;
  map<uint32_t, int>::iterator iter = acl_group_map.find((uint32_t)group);
  if (iter != acl_group_map.end()) {
    ldout(cct, 5) << "Found permission: " << iter->second << dendl;
    return iter->second & perm_mask;
  }
  ldout(cct, 5) << "Permissions for group not found" << dendl;
  return 0;
}

int RGWAccessControlPolicy::get_perm(rgw_user& id, int perm_mask) {
  int perm = acl.get_perm(id, perm_mask);

  if (id.compare(owner.get_id()) == 0) {
    perm |= perm_mask & (RGW_PERM_READ_ACP | RGW_PERM_WRITE_ACP);
  }

  if (perm == perm_mask)
    return perm;

  /* should we continue looking up? */
  if ((perm & perm_mask) != perm_mask) {
    perm |= acl.get_group_perm(ACL_GROUP_ALL_USERS, perm_mask);

    if (!compare_group_name(id.id, ACL_GROUP_ALL_USERS)) {
      /* this is not the anonymous user */
      perm |= acl.get_group_perm(ACL_GROUP_AUTHENTICATED_USERS, perm_mask);
    }
  }

  ldout(cct, 5) << "Getting permissions id=" << id << " owner=" << owner.get_id() << " perm=" << perm << dendl;

  return perm;
}

bool RGWAccessControlPolicy::verify_permission(rgw_user& uid, int user_perm_mask, int perm)
{
  int test_perm = perm | RGW_PERM_READ_OBJS | RGW_PERM_WRITE_OBJS;

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
   
  int acl_perm = policy_perm & perm & user_perm_mask;

  ldout(cct, 10) << " uid=" << uid << " requested perm (type)=" << perm << ", policy perm=" << policy_perm << ", user_perm_mask=" << user_perm_mask << ", acl perm=" << acl_perm << dendl;

  return (perm == acl_perm);
}


