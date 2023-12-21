// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "common/Formatter.h"

#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_user.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

bool operator==(const ACLPermission& lhs, const ACLPermission& rhs) {
  return lhs.flags == rhs.flags;
}
bool operator!=(const ACLPermission& lhs, const ACLPermission& rhs) {
  return !(lhs == rhs);
}

bool operator==(const ACLGranteeType& lhs, const ACLGranteeType& rhs) {
  return lhs.type == rhs.type;
}
bool operator!=(const ACLGranteeType& lhs, const ACLGranteeType& rhs) {
  return lhs.type != rhs.type;
}

bool operator==(const ACLGrant& lhs, const ACLGrant& rhs) {
  return lhs.grantee == rhs.grantee && lhs.permission == rhs.permission;
}
bool operator!=(const ACLGrant& lhs, const ACLGrant& rhs) {
  return !(lhs == rhs);
}

bool operator==(const ACLReferer& lhs, const ACLReferer& rhs) {
  return lhs.url_spec == rhs.url_spec && lhs.perm == rhs.perm;
}
bool operator!=(const ACLReferer& lhs, const ACLReferer& rhs) {
  return !(lhs == rhs);
}

bool operator==(const RGWAccessControlList& lhs,
                const RGWAccessControlList& rhs) {
  return lhs.acl_user_map == rhs.acl_user_map
      && lhs.acl_group_map == rhs.acl_group_map
      && lhs.referer_list == rhs.referer_list
      && lhs.grant_map == rhs.grant_map;
}
bool operator!=(const RGWAccessControlList& lhs,
                const RGWAccessControlList& rhs) {
  return !(lhs == rhs);
}

bool operator==(const RGWAccessControlPolicy& lhs,
                const RGWAccessControlPolicy& rhs) {
  return lhs.acl == rhs.acl && lhs.owner == rhs.owner;
}
bool operator!=(const RGWAccessControlPolicy& lhs,
                const RGWAccessControlPolicy& rhs) {
  return !(lhs == rhs);
}

void RGWAccessControlList::register_grant(const ACLGrant& grant)
{
  ACLPermission perm = grant.get_permission();

  if (const auto* user = grant.get_user(); user) {
    acl_user_map[user->id.to_str()] |= perm.get_permissions();
  } else if (const auto* email = grant.get_email(); email) {
    acl_user_map[email->address] |= perm.get_permissions();
  } else if (const auto* group = grant.get_group(); group) {
    acl_group_map[group->type] |= perm.get_permissions();
  } else if (const auto* referer = grant.get_referer(); referer) {
    referer_list.emplace_back(referer->url_spec, perm.get_permissions());

    /* We're specially handling the Swift's .r:* as the S3 API has a similar
     * concept and thus we can have a small portion of compatibility here. */
     if (referer->url_spec == RGW_REFERER_WILDCARD) {
       acl_group_map[ACL_GROUP_ALL_USERS] |= perm.get_permissions();
     }
  }
}

void RGWAccessControlList::add_grant(const ACLGrant& grant)
{
  std::string id;
  if (const auto* user = grant.get_user(); user) {
    id = user->id.to_str();
  } else if (const auto* email = grant.get_email(); email) {
    id = email->address;
  } // other types share the empty key in the grant multimap
  grant_map.emplace(id, grant);
  register_grant(grant);
}

void RGWAccessControlList::remove_canon_user_grant(const rgw_user& user_id)
{
  const std::string& key = user_id.to_str();
  grant_map.erase(key);
  acl_user_map.erase(key);
}

uint32_t RGWAccessControlList::get_perm(const DoutPrefixProvider* dpp, 
                                        const rgw::auth::Identity& auth_identity,
                                        const uint32_t perm_mask) const
{
  ldpp_dout(dpp, 5) << "Searching permissions for identity=" << auth_identity
                << " mask=" << perm_mask << dendl;

  return perm_mask & auth_identity.get_perms_from_aclspec(dpp, acl_user_map);
}

uint32_t RGWAccessControlList::get_group_perm(const DoutPrefixProvider *dpp, 
                                              ACLGroupTypeEnum group,
                                              const uint32_t perm_mask) const
{
  ldpp_dout(dpp, 5) << "Searching permissions for group=" << (int)group
                << " mask=" << perm_mask << dendl;

  const auto iter = acl_group_map.find((uint32_t)group);
  if (iter != acl_group_map.end()) {
    ldpp_dout(dpp, 5) << "Found permission: " << iter->second << dendl;
    return iter->second & perm_mask;
  }
  ldpp_dout(dpp, 5) << "Permissions for group not found" << dendl;
  return 0;
}

uint32_t RGWAccessControlList::get_referer_perm(const DoutPrefixProvider *dpp,
                                                const uint32_t current_perm,
                                                const std::string http_referer,
                                                const uint32_t perm_mask) const
{
  ldpp_dout(dpp, 5) << "Searching permissions for referer=" << http_referer
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

  ldpp_dout(dpp, 5) << "Found referer permission=" << referer_perm << dendl;
  return referer_perm & perm_mask;
}

uint32_t RGWAccessControlPolicy::get_perm(const DoutPrefixProvider* dpp,
                                          const rgw::auth::Identity& auth_identity,
                                          const uint32_t perm_mask,
                                          const char * const http_referer,
                                          bool ignore_public_acls) const
{
  ldpp_dout(dpp, 20) << "-- Getting permissions begin with perm_mask=" << perm_mask
                 << dendl;

  uint32_t perm = acl.get_perm(dpp, auth_identity, perm_mask);

  if (auth_identity.is_owner_of(owner.id)) {
    perm |= perm_mask & (RGW_PERM_READ_ACP | RGW_PERM_WRITE_ACP);
  }

  if (perm == perm_mask) {
    return perm;
  }

  /* should we continue looking up? */
  if (!ignore_public_acls && ((perm & perm_mask) != perm_mask)) {
    perm |= acl.get_group_perm(dpp, ACL_GROUP_ALL_USERS, perm_mask);

    if (false == auth_identity.is_owner_of(rgw_user(RGW_USER_ANON_ID))) {
      /* this is not the anonymous user */
      perm |= acl.get_group_perm(dpp, ACL_GROUP_AUTHENTICATED_USERS, perm_mask);
    }
  }

  /* Should we continue looking up even deeper? */
  if (nullptr != http_referer && (perm & perm_mask) != perm_mask) {
    perm = acl.get_referer_perm(dpp, perm, http_referer, perm_mask);
  }

  ldpp_dout(dpp, 5) << "-- Getting permissions done for identity=" << auth_identity
                << ", owner=" << owner.id
                << ", perm=" << perm << dendl;

  return perm;
}

bool RGWAccessControlPolicy::verify_permission(const DoutPrefixProvider* dpp,
                                               const rgw::auth::Identity& auth_identity,
                                               const uint32_t user_perm_mask,
                                               const uint32_t perm,
                                               const char * const http_referer,
                                               bool ignore_public_acls) const
{
  uint32_t test_perm = perm | RGW_PERM_READ_OBJS | RGW_PERM_WRITE_OBJS;

  uint32_t policy_perm = get_perm(dpp, auth_identity, test_perm, http_referer, ignore_public_acls);

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

  ldpp_dout(dpp, 10) << " identity=" << auth_identity
                 << " requested perm (type)=" << perm
                 << ", policy perm=" << policy_perm
                 << ", user_perm_mask=" << user_perm_mask
                 << ", acl perm=" << acl_perm << dendl;

  return (perm == acl_perm);
}


bool RGWAccessControlPolicy::is_public(const DoutPrefixProvider *dpp) const
{

  static constexpr auto public_groups = {ACL_GROUP_ALL_USERS,
					 ACL_GROUP_AUTHENTICATED_USERS};
  return std::any_of(public_groups.begin(), public_groups.end(),
                         [&, dpp](ACLGroupTypeEnum g) {
                           auto p = acl.get_group_perm(dpp, g, RGW_PERM_FULL_CONTROL);
                           return (p != RGW_PERM_NONE) && (p != RGW_PERM_INVALID);
                         }
                         );

}

void ACLPermission::generate_test_instances(list<ACLPermission*>& o)
{
  ACLPermission *p = new ACLPermission;
  p->set_permissions(RGW_PERM_WRITE_ACP);
  o.push_back(p);
  o.push_back(new ACLPermission);
}

void ACLPermission::dump(Formatter *f) const
{
  f->dump_int("flags", flags);
}

void ACLGranteeType::dump(Formatter *f) const
{
  f->dump_unsigned("type", type);
}

void ACLGrant::dump(Formatter *f) const
{
  f->open_object_section("type");
  get_type().dump(f);
  f->close_section();

  struct dump_visitor {
    Formatter* f;

    void operator()(const ACLGranteeCanonicalUser& user) {
      encode_json("id", user.id, f);
      encode_json("name", user.name, f);
    }
    void operator()(const ACLGranteeEmailUser& email) {
      encode_json("email", email.address, f);
    }
    void operator()(const ACLGranteeGroup& group) {
      encode_json("group", static_cast<int>(group.type), f);
    }
    void operator()(const ACLGranteeUnknown&) {}
    void operator()(const ACLGranteeReferer& r) {
      encode_json("url_spec", r.url_spec, f);
    }
  };
  std::visit(dump_visitor{f}, grantee);

  encode_json("permission", permission, f);
}

void ACLGrant::generate_test_instances(list<ACLGrant*>& o)
{
  ACLGrant *g1 = new ACLGrant;
  g1->set_canon(rgw_user{"rgw"}, "Mr. RGW", RGW_PERM_READ);
  o.push_back(g1);

  ACLGrant *g2 = new ACLGrant;
  g1->set_group(ACL_GROUP_AUTHENTICATED_USERS, RGW_PERM_WRITE);
  o.push_back(g2);

  o.push_back(new ACLGrant);
}

void ACLGranteeType::generate_test_instances(list<ACLGranteeType*>& o)
{
  o.push_back(new ACLGranteeType(ACL_TYPE_CANON_USER));
  o.push_back(new ACLGranteeType);
}

void RGWAccessControlList::generate_test_instances(list<RGWAccessControlList*>& o)
{
  RGWAccessControlList *acl = new RGWAccessControlList;

  list<ACLGrant *> grants;
  ACLGrant::generate_test_instances(grants);
  for (ACLGrant* grant : grants) {
    acl->add_grant(*grant);
    delete grant;
  }
  o.push_back(acl);
  o.push_back(new RGWAccessControlList);
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

    RGWAccessControlPolicy *p = new RGWAccessControlPolicy;
    RGWAccessControlList *l = *iter;
    p->acl = *l;

    p->owner.id.id = "rgw";
    p->owner.display_name = "radosgw";

    o.push_back(p);

    delete l;
  }

  o.push_back(new RGWAccessControlPolicy);
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

void ACLOwner::dump(Formatter *f) const
{
  encode_json("id", id.to_str(), f);
  encode_json("display_name", display_name, f);
}

void ACLOwner::decode_json(JSONObj *obj) {
  string id_str;
  JSONDecoder::decode_json("id", id_str, obj);
  id.from_str(id_str);
  JSONDecoder::decode_json("display_name", display_name, obj);
}

void RGWAccessControlPolicy::dump(Formatter *f) const
{
  encode_json("acl", acl, f);
  encode_json("owner", owner, f);
}

ACLGroupTypeEnum ACLGrant::uri_to_group(std::string_view uri)
{
  // this is required for backward compatibility
  return rgw::s3::acl_uri_to_group(uri);
}

