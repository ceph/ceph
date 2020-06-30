// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <string.h>

#include <vector>

#include <boost/algorithm/string/predicate.hpp>

#include "common/ceph_json.h"
#include "rgw_common.h"
#include "rgw_user.h"
#include "rgw_acl_swift.h"

#define dout_subsys ceph_subsys_rgw


#define SWIFT_PERM_READ  RGW_PERM_READ_OBJS
#define SWIFT_PERM_WRITE RGW_PERM_WRITE_OBJS
/* FIXME: do we really need separate RW? */
#define SWIFT_PERM_RWRT  (SWIFT_PERM_READ | SWIFT_PERM_WRITE)
#define SWIFT_PERM_ADMIN RGW_PERM_FULL_CONTROL

#define SWIFT_GROUP_ALL_USERS ".r:*"

static int parse_list(const char* uid_list,
                      std::vector<std::string>& uids)           /* out */
{
  char *s = strdup(uid_list);
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

static bool is_referrer(const std::string& designator)
{
  return designator.compare(".r") == 0 ||
         designator.compare(".ref") == 0 ||
         designator.compare(".referer") == 0 ||
         designator.compare(".referrer") == 0;
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

  return is_referrer(sub);
}

static boost::optional<ACLGrant> referrer_to_grant(std::string url_spec,
                                                   const uint32_t perm)
{
  /* This function takes url_spec as non-ref std::string because of the trim
   * operation that is essential to preserve compliance with Swift. It can't
   * be easily accomplished with std::string_view. */
  try {
    bool is_negative;
    ACLGrant grant;

    if ('-' == url_spec[0]) {
      url_spec = url_spec.substr(1);
      boost::algorithm::trim(url_spec);

      is_negative = true;
    } else {
      is_negative = false;
    }

    if (url_spec != RGW_REFERER_WILDCARD) {
      if ('*' == url_spec[0]) {
        url_spec = url_spec.substr(1);
        boost::algorithm::trim(url_spec);
      }

      if (url_spec.empty() || url_spec == ".") {
        return boost::none;
      }
    } else {
      /* Please be aware we're specially handling the .r:* in _add_grant()
       * of RGWAccessControlList as the S3 API has a similar concept, and
       * thus we can have a small portion of compatibility. */
    }

    grant.set_referer(url_spec, is_negative ? 0 : perm);
    return grant;
  } catch (const std::out_of_range&) {
    return boost::none;
  }
}

static ACLGrant user_to_grant(CephContext* const cct,
                              RGWUserCtl* const user_ctl,
                              const std::string& uid,
                              const uint32_t perm)
{
  rgw_user user(uid);
  RGWUserInfo grant_user;
  ACLGrant grant;

  if (user_ctl->get_info_by_uid(user, &grant_user, null_yield) < 0) {
    ldout(cct, 10) << "grant user does not exist: " << uid << dendl;
    /* skipping silently */
    grant.set_canon(user, std::string(), perm);
  } else {
    grant.set_canon(user, grant_user.display_name, perm);
  }

  return grant;
}

int RGWAccessControlPolicy_SWIFT::add_grants(RGWUserCtl* const user_ctl,
                                             const std::vector<std::string>& uids,
                                             const uint32_t perm)
{
  for (const auto& uid : uids) {
    boost::optional<ACLGrant> grant;
    ldout(cct, 20) << "trying to add grant for ACL uid=" << uid << dendl;

    /* Let's check whether the item has a separator potentially indicating
     * a special meaning (like an HTTP referral-based grant). */
    const size_t pos = uid.find(':');
    if (std::string::npos == pos) {
      /* No, it don't have -- we've got just a regular user identifier. */
      grant = user_to_grant(cct, user_ctl, uid, perm);
    } else {
      /* Yes, *potentially* an HTTP referral. */
      auto designator = uid.substr(0, pos);
      auto designatee = uid.substr(pos + 1);

      /* Swift strips whitespaces at both beginning and end. */
      boost::algorithm::trim(designator);
      boost::algorithm::trim(designatee);

      if (! boost::algorithm::starts_with(designator, ".")) {
        grant = user_to_grant(cct, user_ctl, uid, perm);
      } else if ((perm & SWIFT_PERM_WRITE) == 0 && is_referrer(designator)) {
        /* HTTP referrer-based ACLs aren't acceptable for writes. */
        grant = referrer_to_grant(designatee, perm);
      }
    }

    if (grant) {
      acl.add_grant(&*grant);
    } else {
      return -EINVAL;
    }
  }

  return 0;
}


int RGWAccessControlPolicy_SWIFT::create(RGWUserCtl* const user_ctl,
                                         const rgw_user& id,
                                         const std::string& name,
                                         const char* read_list,
                                         const char* write_list,
                                         uint32_t& rw_mask)
{
  acl.create_default(id, name);
  owner.set_id(id);
  owner.set_name(name);
  rw_mask = 0;

  if (read_list) {
    std::vector<std::string> uids;
    int r = parse_list(read_list, uids);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: parse_list for read returned r="
                    << r << dendl;
      return r;
    }

    r = add_grants(user_ctl, uids, SWIFT_PERM_READ);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: add_grants for read returned r="
                    << r << dendl;
      return r;
    }
    rw_mask |= SWIFT_PERM_READ;
  }
  if (write_list) {
    std::vector<std::string> uids;
    int r = parse_list(write_list, uids);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: parse_list for write returned r="
                    << r << dendl;
      return r;
    }

    r = add_grants(user_ctl, uids, SWIFT_PERM_WRITE);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: add_grants for write returned r="
                    << r << dendl;
      return r;
    }
    rw_mask |= SWIFT_PERM_WRITE;
  }
  return 0;
}

void RGWAccessControlPolicy_SWIFT::filter_merge(uint32_t rw_mask,
                                                RGWAccessControlPolicy_SWIFT *old)
{
  /* rw_mask&SWIFT_PERM_READ => setting read acl,
   * rw_mask&SWIFT_PERM_WRITE => setting write acl
   * when bit is cleared, copy matching elements from old.
   */
  if (rw_mask == (SWIFT_PERM_READ|SWIFT_PERM_WRITE)) {
    return;
  }
  rw_mask ^= (SWIFT_PERM_READ|SWIFT_PERM_WRITE);
  for (auto &iter: old->acl.get_grant_map()) {
    ACLGrant& grant = iter.second;
    uint32_t perm = grant.get_permission().get_permissions();
    rgw_user id;
    string url_spec;
    if (!grant.get_id(id)) {
      if (grant.get_group() != ACL_GROUP_ALL_USERS) {
        url_spec = grant.get_referer();
        if (url_spec.empty()) {
          continue;
        }
        if (perm == 0) {
          /* We need to carry also negative, HTTP referrer-based ACLs. */
          perm = SWIFT_PERM_READ;
        }
      }
    }
    if (perm & rw_mask) {
      acl.add_grant(&grant);
    }
  }
}

void RGWAccessControlPolicy_SWIFT::to_str(string& read, string& write)
{
  multimap<string, ACLGrant>& m = acl.get_grant_map();
  multimap<string, ACLGrant>::iterator iter;

  for (iter = m.begin(); iter != m.end(); ++iter) {
    ACLGrant& grant = iter->second;
    const uint32_t perm = grant.get_permission().get_permissions();
    rgw_user id;
    string url_spec;
    if (!grant.get_id(id)) {
      if (grant.get_group() == ACL_GROUP_ALL_USERS) {
        id = SWIFT_GROUP_ALL_USERS;
      } else {
        url_spec = grant.get_referer();
        if (url_spec.empty()) {
          continue;
        }
        id = (perm != 0) ? ".r:" + url_spec : ".r:-" + url_spec;
      }
    }
    if (perm & SWIFT_PERM_READ) {
      if (!read.empty()) {
        read.append(",");
      }
      read.append(id.to_str());
    } else if (perm & SWIFT_PERM_WRITE) {
      if (!write.empty()) {
        write.append(",");
      }
      write.append(id.to_str());
    } else if (perm == 0 && !url_spec.empty()) {
      /* only X-Container-Read headers support referers */
      if (!read.empty()) {
        read.append(",");
      }
      read.append(id.to_str());
    }
  }
}

void RGWAccessControlPolicy_SWIFTAcct::add_grants(RGWUserCtl * const user_ctl,
                                                  const std::vector<std::string>& uids,
                                                  const uint32_t perm)
{
  for (const auto& uid : uids) {
    ACLGrant grant;
    RGWUserInfo grant_user;

    if (uid_is_public(uid)) {
      grant.set_group(ACL_GROUP_ALL_USERS, perm);
      acl.add_grant(&grant);
    } else  {
      rgw_user user(uid);

      if (user_ctl->get_info_by_uid(user, &grant_user, null_yield) < 0) {
        ldout(cct, 10) << "grant user does not exist:" << uid << dendl;
        /* skipping silently */
        grant.set_canon(user, std::string(), perm);
        acl.add_grant(&grant);
      } else {
        grant.set_canon(user, grant_user.display_name, perm);
        acl.add_grant(&grant);
      }
    }
  }
}

bool RGWAccessControlPolicy_SWIFTAcct::create(RGWUserCtl * const user_ctl,
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
    std::vector<std::string> admin;
    decode_json_obj(admin, *iter);
    ldout(cct, 0) << "admins: " << admin << dendl;

    add_grants(user_ctl, admin, SWIFT_PERM_ADMIN);
  }

  iter = parser.find_first("read-write");
  if (!iter.end() && (*iter)->is_array()) {
    std::vector<std::string> readwrite;
    decode_json_obj(readwrite, *iter);
    ldout(cct, 0) << "read-write: " << readwrite << dendl;

    add_grants(user_ctl, readwrite, SWIFT_PERM_RWRT);
  }

  iter = parser.find_first("read-only");
  if (!iter.end() && (*iter)->is_array()) {
    std::vector<std::string> readonly;
    decode_json_obj(readonly, *iter);
    ldout(cct, 0) << "read-only: " << readonly << dendl;

    add_grants(user_ctl, readonly, SWIFT_PERM_READ);
  }

  return true;
}

boost::optional<std::string> RGWAccessControlPolicy_SWIFTAcct::to_str() const
{
  std::vector<std::string> admin;
  std::vector<std::string> readwrite;
  std::vector<std::string> readonly;

  /* Parition the grant map into three not-overlapping groups. */
  for (const auto& item : get_acl().get_grant_map()) {
    const ACLGrant& grant = item.second;
    const uint32_t perm = grant.get_permission().get_permissions();

    rgw_user id;
    if (!grant.get_id(id)) {
      if (grant.get_group() != ACL_GROUP_ALL_USERS) {
        continue;
      }
      id = SWIFT_GROUP_ALL_USERS;
    } else if (owner.get_id() == id) {
      continue;
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

  /* If there is no grant to serialize, let's exit earlier to not return
   * an empty JSON object which brakes the functional tests of Swift. */
  if (admin.empty() && readwrite.empty() && readonly.empty()) {
    return boost::none;
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

  return oss.str();
}
