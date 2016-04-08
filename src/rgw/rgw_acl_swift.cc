// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>

#include <list>

#include "common/ceph_json.h"
#include "rgw_common.h"
#include "rgw_user.h"
#include "rgw_acl_swift.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

#define SWIFT_PERM_READ  RGW_PERM_READ_OBJS
#define SWIFT_PERM_WRITE RGW_PERM_WRITE_OBJS
/* FIXME: do we really need separate RW? */
#define SWIFT_PERM_RWRT  (SWIFT_PERM_READ | SWIFT_PERM_WRITE)
#define SWIFT_PERM_ADMIN RGW_PERM_FULL_CONTROL

#define SWIFT_GROUP_ALL_USERS ".r:*"

static int parse_list(const string& uid_list, list<string>& uids)
{
  char *s = strdup(uid_list.c_str());
  if (!s) {
    return -ENOMEM;
  }

  char *tokctx;
  const char *p = strtok_r(s, " ,", &tokctx);
  while (p) {
    if (*p) {
      string acl = p;
      uids.push_back(acl);
    }
    p = strtok_r(NULL, " ,", &tokctx);
  }
  free(s);
  return 0;
}

static bool uid_is_public(const string& uid)
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

void RGWAccessControlPolicy_SWIFT::add_grants(RGWRados * const store,
                                              const list<string>& uids,
                                              const int perm)
{
  for (const auto& uid : uids) {
    ACLGrant grant;
    RGWUserInfo grant_user;
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

bool RGWAccessControlPolicy_SWIFT::create(RGWRados * const store,
                                          const rgw_user& id,
                                          const std::string& name,
                                          const std::string& read_list,
                                          const std::string& write_list)
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

void RGWAccessControlPolicy_SWIFTAcct::add_grants(RGWRados * const store,
                                                  const list<string>& uids,
                                                  const int perm)
{
  for (const auto uid : uids) {
    ACLGrant grant;
    RGWUserInfo grant_user;

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

bool RGWAccessControlPolicy_SWIFTAcct::create(RGWRados * const store,
                                              const rgw_user& id,
                                              const std::string& name,
                                              const std::string& acl_str)
{
  acl.create_default(id, name);
  owner.set_id(id);
  owner.set_name(name);

  JSONParser parser;

  if (!parser.parse(acl_str.c_str(), acl_str.length())) {
    ldout(cct, 0) << "ERROR: JSONParser::parse returned error=" << dendl;
    return false;
  }

  JSONObjIter iter = parser.find_first("admin");
  if (!iter.end() && (*iter)->is_array()) {
    list<string> admin;
    decode_json_obj(admin, *iter);
    ldout(cct, 0) << "admins: " << admin << dendl;

    add_grants(store, admin, SWIFT_PERM_ADMIN);
  }

  iter = parser.find_first("read-write");
  if (!iter.end() && (*iter)->is_array()) {
    list<string> readwrite;
    decode_json_obj(readwrite, *iter);
    ldout(cct, 0) << "read-write: " << readwrite << dendl;

    add_grants(store, readwrite, SWIFT_PERM_RWRT);
  }

  iter = parser.find_first("read-only");
  if (!iter.end() && (*iter)->is_array()) {
    list<string> readonly;
    decode_json_obj(readonly, *iter);
    ldout(cct, 0) << "read-only: " << readonly << dendl;

    add_grants(store, readonly, SWIFT_PERM_READ);
  }

  return true;
}

void RGWAccessControlPolicy_SWIFTAcct::to_str(std::string& acl_str) const
{
  list<string> admin;
  list<string> readwrite;
  list<string> readonly;

  /* Parition the grant map into three not-overlapping groups. */
  for (const auto item : get_acl().get_grant_map()) {
    const ACLGrant& grant = item.second;
    const int perm = grant.get_permission().get_permissions();

    rgw_user id;
    if (!grant.get_id(id)) {
      if (grant.get_group() != ACL_GROUP_ALL_USERS) {
        continue;
      }
      id = SWIFT_GROUP_ALL_USERS;
    }

    if (SWIFT_PERM_ADMIN == (perm & SWIFT_PERM_ADMIN)) {
      admin.insert(admin.end(), id.to_str());
    } else if (SWIFT_PERM_RWRT == (perm & SWIFT_PERM_RWRT)) {
      readwrite.insert(readwrite.end(), id.to_str());
    } else if (SWIFT_PERM_READ == (perm & SWIFT_PERM_READ)) {
      readonly.insert(readonly.end(), id.to_str());
    } else {
      // FIXME: print a warning
    }
  }

  /* Serialize the groups. */
  JSONFormatter formatter;

  formatter.open_object_section("acl");
  if (!readonly.empty()) {
    encode_json("read-only", readonly, &formatter);
  }
  if (!readwrite.empty()) {
    encode_json("read-write", readwrite, &formatter);
  }
  if (!admin.empty()) {
    encode_json("admin", admin, &formatter);
  }
  formatter.close_section();

  std::ostringstream oss;
  formatter.flush(oss);

  acl_str = oss.str();
}
