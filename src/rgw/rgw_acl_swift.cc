// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <string.h>

#include <optional>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>

#include "common/ceph_json.h"
#include "common/split.h"
#include "rgw_common.h"
#include "rgw_user.h"
#include "rgw_acl_swift.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw


#define SWIFT_PERM_READ  RGW_PERM_READ_OBJS
#define SWIFT_PERM_WRITE RGW_PERM_WRITE_OBJS
/* FIXME: do we really need separate RW? */
#define SWIFT_PERM_RWRT  (SWIFT_PERM_READ | SWIFT_PERM_WRITE)
#define SWIFT_PERM_ADMIN RGW_PERM_FULL_CONTROL

#define SWIFT_GROUP_ALL_USERS ".r:*"

using namespace std;

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

static std::optional<ACLGrant> referrer_to_grant(std::string url_spec,
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
        return std::nullopt;
      }
    } else {
      /* Please be aware we're specially handling the .r:* in _add_grant()
       * of RGWAccessControlList as the S3 API has a similar concept, and
       * thus we can have a small portion of compatibility. */
    }

    grant.set_referer(url_spec, is_negative ? 0 : perm);
    return grant;
  } catch (const std::out_of_range&) {
    return std::nullopt;
  }
}

static ACLGrant user_to_grant(const DoutPrefixProvider *dpp,
                              rgw::sal::Driver* driver,
                              const std::string& uid,
                              const uint32_t perm)
{
  ACLGrant grant;

  std::unique_ptr<rgw::sal::User> user = driver->get_user(rgw_user(uid));
  if (user->load_user(dpp, null_yield) < 0) {
    ldpp_dout(dpp, 10) << "grant user does not exist: " << uid << dendl;
    /* skipping silently */
    grant.set_canon(user->get_id(), std::string(), perm);
  } else {
    grant.set_canon(user->get_id(), user->get_display_name(), perm);
  }

  return grant;
}

// parse a container acl grant in 'V1' format
// https://docs.openstack.org/swift/latest/overview_acl.html#container-acls
static auto parse_grant(const DoutPrefixProvider* dpp,
                        rgw::sal::Driver* driver,
                        const std::string& uid,
                        const uint32_t perm)
  -> std::optional<ACLGrant>
{
  ldpp_dout(dpp, 20) << "trying to add grant for ACL uid=" << uid << dendl;

  /* Let's check whether the item has a separator potentially indicating
   * a special meaning (like an HTTP referral-based grant). */
  const size_t pos = uid.find(':');
  if (std::string::npos == pos) {
    /* No, it don't have -- we've got just a regular user identifier. */
    return user_to_grant(dpp, driver, uid, perm);
  }

  /* Yes, *potentially* an HTTP referral. */
  auto designator = uid.substr(0, pos);
  auto designatee = uid.substr(pos + 1);

  /* Swift strips whitespaces at both beginning and end. */
  boost::algorithm::trim(designator);
  boost::algorithm::trim(designatee);

  if (! boost::algorithm::starts_with(designator, ".")) {
    return user_to_grant(dpp, driver, uid, perm);
  }
  if ((perm & SWIFT_PERM_WRITE) == 0 && is_referrer(designator)) {
    /* HTTP referrer-based ACLs aren't acceptable for writes. */
    return referrer_to_grant(designatee, perm);
  }

  return std::nullopt;
}

static void add_grants(const DoutPrefixProvider* dpp,
                       rgw::sal::Driver* driver,
                       const std::vector<std::string>& uids,
                       uint32_t perm, RGWAccessControlList& acl)
{
  for (const auto& uid : uids) {
    ACLGrant grant;
    if (uid_is_public(uid)) {
      grant.set_group(ACL_GROUP_ALL_USERS, perm);
    } else  {
      grant = user_to_grant(dpp, driver, uid, perm);
    }
    acl.add_grant(grant);
  }
}

namespace rgw::swift {

int create_container_policy(const DoutPrefixProvider *dpp,
                            rgw::sal::Driver* driver,
                            const rgw_user& id,
                            const std::string& name,
                            const char* read_list,
                            const char* write_list,
                            uint32_t& rw_mask,
                            RGWAccessControlPolicy& policy)
{
  policy.create_default(id, name);
  auto& acl = policy.get_acl();

  if (read_list) {
    for (std::string_view uid : ceph::split(read_list, " ,")) {
      auto grant = parse_grant(dpp, driver, std::string{uid}, SWIFT_PERM_READ);
      if (!grant) {
        ldpp_dout(dpp, 4) << "ERROR: failed to parse read acl grant "
            << uid << dendl;
        return -EINVAL;
      }
      acl.add_grant(*grant);
    }
    rw_mask |= SWIFT_PERM_READ;
  }
  if (write_list) {
    for (std::string_view uid : ceph::split(write_list, " ,")) {
      auto grant = parse_grant(dpp, driver, std::string{uid}, SWIFT_PERM_WRITE);
      if (!grant) {
        ldpp_dout(dpp, 4) << "ERROR: failed to parse write acl grant "
            << uid << dendl;
        return -EINVAL;
      }
      acl.add_grant(*grant);
    }
    rw_mask |= SWIFT_PERM_WRITE;
  }
  return 0;
}

void merge_policy(uint32_t rw_mask, const RGWAccessControlPolicy& src,
                  RGWAccessControlPolicy& dest)
{
  /* rw_mask&SWIFT_PERM_READ => setting read acl,
   * rw_mask&SWIFT_PERM_WRITE => setting write acl
   * when bit is cleared, copy matching elements from old.
   */
  if (rw_mask == (SWIFT_PERM_READ|SWIFT_PERM_WRITE)) {
    return;
  }
  rw_mask ^= (SWIFT_PERM_READ|SWIFT_PERM_WRITE);
  for (const auto &iter: src.get_acl().get_grant_map()) {
    const ACLGrant& grant = iter.second;
    uint32_t perm = grant.get_permission().get_permissions();
    if (const auto* referer = grant.get_referer(); referer) {
      if (referer->url_spec.empty()) {
        continue;
      }
      if (perm == 0) {
        /* We need to carry also negative, HTTP referrer-based ACLs. */
        perm = SWIFT_PERM_READ;
      }
    }
    if (perm & rw_mask) {
      dest.get_acl().add_grant(grant);
    }
  }
}

void format_container_acls(const RGWAccessControlPolicy& policy,
                           std::string& read, std::string& write)
{
  for (const auto& [k, grant] : policy.get_acl().get_grant_map()) {
    const uint32_t perm = grant.get_permission().get_permissions();
    std::string id;
    std::string url_spec;
    if (const auto user = grant.get_user(); user) {
      id = user->id.to_str();
    } else if (const auto group = grant.get_group(); group) {
      if (group->type == ACL_GROUP_ALL_USERS) {
        id = SWIFT_GROUP_ALL_USERS;
      }
    } else if (const auto referer = grant.get_referer(); referer) {
      url_spec = referer->url_spec;
      if (url_spec.empty()) {
        continue;
      }
      id = (perm != 0) ? ".r:" + url_spec : ".r:-" + url_spec;
    }
    if (perm & SWIFT_PERM_READ) {
      if (!read.empty()) {
        read.append(",");
      }
      read.append(id);
    } else if (perm & SWIFT_PERM_WRITE) {
      if (!write.empty()) {
        write.append(",");
      }
      write.append(id);
    } else if (perm == 0 && !url_spec.empty()) {
      /* only X-Container-Read headers support referers */
      if (!read.empty()) {
        read.append(",");
      }
      read.append(id);
    }
  }
}

int create_account_policy(const DoutPrefixProvider* dpp,
                          rgw::sal::Driver* driver,
                          const rgw_user& id,
                          const std::string& name,
                          const std::string& acl_str,
                          RGWAccessControlPolicy& policy)
{
  policy.create_default(id, name);
  auto& acl = policy.get_acl();

  JSONParser parser;
  if (!parser.parse(acl_str.c_str(), acl_str.length())) {
    ldpp_dout(dpp, 0) << "ERROR: JSONParser::parse returned error=" << dendl;
    return -EINVAL;
  }

  JSONObjIter iter = parser.find_first("admin");
  if (!iter.end() && (*iter)->is_array()) {
    std::vector<std::string> admin;
    decode_json_obj(admin, *iter);
    ldpp_dout(dpp, 0) << "admins: " << admin << dendl;

    add_grants(dpp, driver, admin, SWIFT_PERM_ADMIN, acl);
  }

  iter = parser.find_first("read-write");
  if (!iter.end() && (*iter)->is_array()) {
    std::vector<std::string> readwrite;
    decode_json_obj(readwrite, *iter);
    ldpp_dout(dpp, 0) << "read-write: " << readwrite << dendl;

    add_grants(dpp, driver, readwrite, SWIFT_PERM_RWRT, acl);
  }

  iter = parser.find_first("read-only");
  if (!iter.end() && (*iter)->is_array()) {
    std::vector<std::string> readonly;
    decode_json_obj(readonly, *iter);
    ldpp_dout(dpp, 0) << "read-only: " << readonly << dendl;

    add_grants(dpp, driver, readonly, SWIFT_PERM_READ, acl);
  }

  return 0;
}

auto format_account_acl(const RGWAccessControlPolicy& policy)
  -> std::optional<std::string>
{
  const ACLOwner& owner = policy.get_owner();

  std::vector<std::string> admin;
  std::vector<std::string> readwrite;
  std::vector<std::string> readonly;

  /* Partition the grant map into three not-overlapping groups. */
  for (const auto& item : policy.get_acl().get_grant_map()) {
    const ACLGrant& grant = item.second;
    const uint32_t perm = grant.get_permission().get_permissions();

    std::string id;
    if (const auto user = grant.get_user(); user) {
      if (owner.id == user->id) {
        continue;
      }
      id = user->id.to_str();
    } else if (const auto group = grant.get_group(); group) {
      if (group->type != ACL_GROUP_ALL_USERS) {
        continue;
      }
      id = SWIFT_GROUP_ALL_USERS;
    } else {
      continue;
    }

    if (SWIFT_PERM_ADMIN == (perm & SWIFT_PERM_ADMIN)) {
      admin.insert(admin.end(), id);
    } else if (SWIFT_PERM_RWRT == (perm & SWIFT_PERM_RWRT)) {
      readwrite.insert(readwrite.end(), id);
    } else if (SWIFT_PERM_READ == (perm & SWIFT_PERM_READ)) {
      readonly.insert(readonly.end(), id);
    } else {
      // FIXME: print a warning
    }
  }

  /* If there is no grant to serialize, let's exit earlier to not return
   * an empty JSON object which brakes the functional tests of Swift. */
  if (admin.empty() && readwrite.empty() && readonly.empty()) {
    return std::nullopt;
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

} // namespace rgw::swift
