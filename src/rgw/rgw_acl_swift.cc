// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>

#include <list>

#include "rgw_common.h"
#include "rgw_user.h"
#include "rgw_acl_swift.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

#define SWIFT_PERM_READ  RGW_PERM_READ_OBJS
#define SWIFT_PERM_WRITE RGW_PERM_WRITE_OBJS

#define SWIFT_GROUP_ALL_USERS ".r:*"

static int parse_list(string& uid_list, list<string>& uids)
{
  char *s = strdup(uid_list.c_str());
  if (!s)
    return -ENOMEM;

  const char *p = strtok(s, " ,");
  while (p) {
    if (*p) {
      string acl = p;
      uids.push_back(acl);
    }
    p = strtok(NULL, " ,");
  }
  free(s);
  return 0;
}

static bool uid_is_public(string& uid)
{
  if (uid[0] != '.' || uid[1] != 'r')
    return false;

  int pos = uid.find(':');
  if (pos < 0 || pos == (int)uid.size())
    return false;

  string sub = uid.substr(0, pos);
  string after = uid.substr(pos + 1);

  if (after.compare("*") != 0)
    return false;

  return sub.compare(".r") == 0 ||
         sub.compare(".referer") == 0 ||
         sub.compare(".referrer") == 0;
}

void RGWAccessControlPolicy_SWIFT::add_grants(RGWRados *store, list<string>& uids, int perm)
{
  list<string>::iterator iter;
  for (iter = uids.begin(); iter != uids.end(); ++iter ) {
    ACLGrant grant;
    RGWUserInfo grant_user;
    string& uid = *iter;
    if (uid_is_public(uid)) {
      grant.set_group(ACL_GROUP_ALL_USERS, perm);
      acl.add_grant(&grant);
    } else  {
      rgw_user user(uid);
      if (rgw_get_user_info_by_uid(store, user, grant_user) < 0) {
        ldout(cct, 10) << "grant user does not exist:" << uid << dendl;
        /* skipping silently */
      } else {
        grant.set_canon(user, grant_user.display_name, perm);
        acl.add_grant(&grant);
      }
    }
  }
}

bool RGWAccessControlPolicy_SWIFT::create(RGWRados *store, rgw_user& id, string& name, string& read_list, string& write_list)
{
  acl.create_default(id, name);
  owner.set_id(id);
  owner.set_name(name);

  if (read_list.size()) {
    list<string> uids;
    int r = parse_list(read_list, uids);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: parse_list returned r=" << r << dendl;
      return false;
    }

    add_grants(store, uids, SWIFT_PERM_READ);
  }
  if (write_list.size()) {
    list<string> uids;
    int r = parse_list(write_list, uids);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: parse_list returned r=" << r << dendl;
      return false;
    }

    add_grants(store, uids, SWIFT_PERM_WRITE);
  }
  return true;
}

void RGWAccessControlPolicy_SWIFT::to_str(string& read, string& write)
{
  multimap<string, ACLGrant>& m = acl.get_grant_map();
  multimap<string, ACLGrant>::iterator iter;

  for (iter = m.begin(); iter != m.end(); ++iter) {
    ACLGrant& grant = iter->second;
    int perm = grant.get_permission().get_permissions();
    rgw_user id;
    if (!grant.get_id(id)) {
      if (grant.get_group() != ACL_GROUP_ALL_USERS)
        continue;
      id = SWIFT_GROUP_ALL_USERS;
    }
    if (perm & SWIFT_PERM_READ) {
      if (!read.empty())
        read.append(", ");
      read.append(id.to_str());
    } else if (perm & SWIFT_PERM_WRITE) {
      if (!write.empty())
        write.append(", ");
      write.append(id.to_str());
    }
  }
}

