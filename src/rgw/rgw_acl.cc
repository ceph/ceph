// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "common/Formatter.h"

#include "rgw_acl.h"
#include "rgw_user.h"

#define dout_subsys ceph_subsys_rgw


void RGWAccessControlList::_add_grant(ACLGrant *grant)
{
  ACLPermission& perm = grant->get_permission();
  ACLGranteeType& type = grant->get_type();
  switch (type.get_type()) {
  case ACL_TYPE_REFERER:
    referer_list.emplace_back(grant->get_referer(), perm.get_permissions());

    /* We're specially handling the Swift's .r:* as the S3 API has a similar
     * concept and thus we can have a small portion of compatibility here. */
     if (grant->get_referer() == RGW_REFERER_WILDCARD) {
       acl_group_map[ACL_GROUP_ALL_USERS] |= perm.get_permissions();
     }
    break;
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

uint32_t RGWAccessControlList::get_perm(const rgw::auth::Identity& auth_identity,
                                        const uint32_t perm_mask)
{
  ldout(cct, 5) << "Searching permissions for identity=" << auth_identity
                << " mask=" << perm_mask << dendl;

  return perm_mask & auth_identity.get_perms_from_aclspec(acl_user_map);
}

uint32_t RGWAccessControlList::get_group_perm(ACLGroupTypeEnum group,
                                              const uint32_t perm_mask)
{
  ldout(cct, 5) << "Searching permissions for group=" << (int)group
                << " mask=" << perm_mask << dendl;

  const auto iter = acl_group_map.find((uint32_t)group);
  if (iter != acl_group_map.end()) {
    ldout(cct, 5) << "Found permission: " << iter->second << dendl;
    return iter->second & perm_mask;
  }
  ldout(cct, 5) << "Permissions for group not found" << dendl;
  return 0;
}

uint32_t RGWAccessControlList::get_referer_perm(const uint32_t current_perm,
                                                const std::string http_referer,
                                                const uint32_t perm_mask)
{
  ldout(cct, 5) << "Searching permissions for referer=" << http_referer
                << " mask=" << perm_mask << dendl;

  /* This function is basically a transformation from current perm to
   * a new one that takes into consideration the Swift's HTTP referer-
   * based ACLs. We need to go through all items to respect negative
   * grants. */
  uint32_t referer_perm = current_perm;
  for (const auto& r : referer_list) {
    if (r.is_match(http_referer)) {
       referer_perm = r.perm;
    }
  }

  ldout(cct, 5) << "Found referer permission=" << referer_perm << dendl;
  return referer_perm & perm_mask;
}

uint32_t RGWAccessControlPolicy::get_perm(const rgw::auth::Identity& auth_identity,
                                          const uint32_t perm_mask,
                                          const char * const http_referer)
{
  ldout(cct, 20) << "-- Getting permissions begin with perm_mask=" << perm_mask
                 << dendl;

  uint32_t perm = acl.get_perm(auth_identity, perm_mask);

  if (auth_identity.is_owner_of(owner.get_id())) {
    perm |= perm_mask & (RGW_PERM_READ_ACP | RGW_PERM_WRITE_ACP);
  }

  if (perm == perm_mask) {
    return perm;
  }

  /* should we continue looking up? */
  if ((perm & perm_mask) != perm_mask) {
    perm |= acl.get_group_perm(ACL_GROUP_ALL_USERS, perm_mask);

    if (false == auth_identity.is_owner_of(rgw_user(RGW_USER_ANON_ID))) {
      /* this is not the anonymous user */
      perm |= acl.get_group_perm(ACL_GROUP_AUTHENTICATED_USERS, perm_mask);
    }
  }

  /* Should we continue looking up even deeper? */
  if (nullptr != http_referer && (perm & perm_mask) != perm_mask) {
    perm = acl.get_referer_perm(perm, http_referer, perm_mask);
  }

  ldout(cct, 5) << "-- Getting permissions done for identity=" << auth_identity
                << ", owner=" << owner.get_id()
                << ", perm=" << perm << dendl;

  return perm;
}

bool RGWAccessControlPolicy::verify_permission(const rgw::auth::Identity& auth_identity,
                                               const uint32_t user_perm_mask,
                                               const uint32_t perm,
                                               const char * const http_referer)
{
  uint32_t test_perm = perm | RGW_PERM_READ_OBJS | RGW_PERM_WRITE_OBJS;

  uint32_t policy_perm = get_perm(auth_identity, test_perm, http_referer);

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
   
  uint32_t acl_perm = policy_perm & perm & user_perm_mask;

  ldout(cct, 10) << " identity=" << auth_identity
                 << " requested perm (type)=" << perm
                 << ", policy perm=" << policy_perm
                 << ", user_perm_mask=" << user_perm_mask
                 << ", acl perm=" << acl_perm << dendl;

  return (perm == acl_perm);
}


