#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "rgw_acl.h"
#include "rgw_user.h"

#define DOUT_SUBSYS rgw

using namespace std;


void RGWAccessControlList::add_grant(ACLGrant *grant)
{
  ACLPermission& perm = grant->get_permission();
  ACLGranteeType& type = grant->get_type();
  switch (type.get_type()) {
  case ACL_TYPE_GROUP:
    acl_group_map[grant->get_group()] |= perm.get_permissions();
  default:
    acl_user_map[grant->get_id()] |= perm.get_permissions();
  }
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

  if (perm == perm_mask)
    return perm;

  if (perm_mask & (RGW_PERM_READ_ACP | RGW_PERM_WRITE_ACP)) {
    /* this is the owner, it has implicit permissions */
    if (id.compare(owner.get_id()) == 0) {
      perm |= RGW_PERM_READ_ACP | RGW_PERM_WRITE_ACP;
      perm &= perm_mask;
    }
  }

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

