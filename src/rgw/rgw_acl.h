// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <map>
#include <string>
#include <string_view>
#include <variant>
#include <include/types.h>

#include <boost/optional.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include "common/debug.h"

#include "rgw_basic_types.h" //includes rgw_acl_types.h

// acl grantee types
struct ACLGranteeCanonicalUser {
  rgw_user id;
  std::string name;

  friend auto operator<=>(const ACLGranteeCanonicalUser&,
                          const ACLGranteeCanonicalUser&) = default;
};
struct ACLGranteeEmailUser {
  std::string address;

  friend auto operator<=>(const ACLGranteeEmailUser&,
                          const ACLGranteeEmailUser&) = default;
};
struct ACLGranteeGroup {
  ACLGroupTypeEnum type = ACL_GROUP_NONE;

  friend auto operator<=>(const ACLGranteeGroup&,
                          const ACLGranteeGroup&) = default;
};
struct ACLGranteeUnknown {
  friend auto operator<=>(const ACLGranteeUnknown&,
                          const ACLGranteeUnknown&) = default;
};
struct ACLGranteeReferer {
  std::string url_spec;

  friend auto operator<=>(const ACLGranteeReferer&,
                          const ACLGranteeReferer&) = default;
};

class ACLGrant
{
protected:
  // acl grantee variant, where variant index matches ACLGranteeTypeEnum
  using ACLGrantee = std::variant<
    ACLGranteeCanonicalUser,
    ACLGranteeEmailUser,
    ACLGranteeGroup,
    ACLGranteeUnknown,
    ACLGranteeReferer>;

  ACLGrantee grantee;
  ACLPermission permission;

public:
  ACLGranteeType get_type() const {
    return static_cast<ACLGranteeTypeEnum>(grantee.index());
  }
  ACLPermission get_permission() const { return permission; }

  // return the user grantee, or nullptr
  const ACLGranteeCanonicalUser* get_user() const {
    return std::get_if<ACLGranteeCanonicalUser>(&grantee);
  }
  // return the email grantee, or nullptr
  const ACLGranteeEmailUser* get_email() const {
    return std::get_if<ACLGranteeEmailUser>(&grantee);
  }
  // return the group grantee, or nullptr
  const ACLGranteeGroup* get_group() const {
    return std::get_if<ACLGranteeGroup>(&grantee);
  }
  // return the referer grantee, or nullptr
  const ACLGranteeReferer* get_referer() const {
    return std::get_if<ACLGranteeReferer>(&grantee);
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(5, 3, bl);
    ACLGranteeType type = get_type();
    encode(type, bl);

    if (const ACLGranteeCanonicalUser* user = get_user(); user) {
      encode(user->id.to_str(), bl);
    } else {
      encode(std::string{}, bl); // encode empty id
    }

    std::string uri; // always empty, v2 converted to 'ACLGroupTypeEnum g' below
    encode(uri, bl);

    if (const ACLGranteeEmailUser* email = get_email(); email) {
      encode(email->address, bl);
    } else {
      encode(std::string{}, bl); // encode empty email address
    }
    encode(permission, bl);
    if (const ACLGranteeCanonicalUser* user = get_user(); user) {
      encode(user->name, bl);
    } else {
      encode(std::string{}, bl); // encode empty name
    }

    __u32 g;
    if (const ACLGranteeGroup* group = get_group(); group) {
      g = static_cast<__u32>(group->type);
    } else {
      g = static_cast<__u32>(ACL_GROUP_NONE);
    }
    encode(g, bl);

    if (const ACLGranteeReferer* referer = get_referer(); referer) {
      encode(referer->url_spec, bl);
    } else {
      encode(std::string{}, bl); // encode empty referer
    }
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(5, 3, 3, bl);
    ACLGranteeType type;
    decode(type, bl);

    ACLGranteeCanonicalUser user;
    std::string s;
    decode(s, bl);
    user.id.from_str(s);

    std::string uri;
    decode(uri, bl);

    ACLGranteeEmailUser email;
    decode(email.address, bl);

    decode(permission, bl);
    decode(user.name, bl);

    ACLGranteeGroup group;
    __u32 g;
    decode(g, bl);
    group.type = static_cast<ACLGroupTypeEnum>(g);

    ACLGranteeReferer referer;
    if (struct_v >= 5) {
      decode(referer.url_spec, bl);
    }

    // construct the grantee type
    switch (type) {
      case ACL_TYPE_CANON_USER:
        grantee = std::move(user);
        break;
      case ACL_TYPE_EMAIL_USER:
        grantee = std::move(email);
        break;
      case ACL_TYPE_GROUP:
        grantee = std::move(group);
        break;
      case ACL_TYPE_REFERER:
        grantee = std::move(referer);
        break;
      case ACL_TYPE_UNKNOWN:
      default:
        grantee = ACLGranteeUnknown{};
        break;
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<ACLGrant*>& o);

  static ACLGroupTypeEnum uri_to_group(std::string_view uri);

  void set_canon(const rgw_user& id, const std::string& name, uint32_t perm) {
    grantee = ACLGranteeCanonicalUser{id, name};
    permission.set_permissions(perm);
  }
  void set_group(ACLGroupTypeEnum group, uint32_t perm) {
    grantee = ACLGranteeGroup{group};
    permission.set_permissions(perm);
  }
  void set_referer(const std::string& url_spec, uint32_t perm) {
    grantee = ACLGranteeReferer{url_spec};
    permission.set_permissions(perm);
  }

  friend bool operator==(const ACLGrant& lhs, const ACLGrant& rhs);
  friend bool operator!=(const ACLGrant& lhs, const ACLGrant& rhs);
};
WRITE_CLASS_ENCODER(ACLGrant)

struct ACLReferer {
  std::string url_spec;
  uint32_t perm;

  ACLReferer() : perm(0) {}
  ACLReferer(const std::string& url_spec,
             const uint32_t perm)
    : url_spec(url_spec),
      perm(perm) {
  }

  bool is_match(std::string_view http_referer) const {
    const auto http_host = get_http_host(http_referer);
    if (!http_host || http_host->length() < url_spec.length()) {
      return false;
    }

    if ("*" == url_spec) {
      return true;
    }

    if (http_host->compare(url_spec) == 0) {
      return true;
    }

    if ('.' == url_spec[0]) {
      /* Wildcard support: a referer matches the spec when its last char are
       * perfectly equal to spec. */
      return boost::algorithm::ends_with(http_host.value(), url_spec);
    }

    return false;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(url_spec, bl);
    encode(perm, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    decode(url_spec, bl);
    decode(perm, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;

  friend bool operator==(const ACLReferer& lhs, const ACLReferer& rhs);
  friend bool operator!=(const ACLReferer& lhs, const ACLReferer& rhs);

private:
  boost::optional<std::string_view> get_http_host(const std::string_view url) const {
    size_t pos = url.find("://");
    if (pos == std::string_view::npos || boost::algorithm::starts_with(url, "://") ||
        boost::algorithm::ends_with(url, "://") || boost::algorithm::ends_with(url, "@")) {
      return boost::none;
    }
    std::string_view url_sub = url.substr(pos + strlen("://"));
    pos = url_sub.find('@');
    if (pos != std::string_view::npos) {
      url_sub = url_sub.substr(pos + 1);
    }
    pos = url_sub.find_first_of("/:");
    if (pos == std::string_view::npos) {
      /* no port or path exists */
      return url_sub;
    }
    return url_sub.substr(0, pos);
  }
};
WRITE_CLASS_ENCODER(ACLReferer)

namespace rgw {
namespace auth {
  class Identity;
}
}

using ACLGrantMap = std::multimap<std::string, ACLGrant>;

class RGWAccessControlList
{
protected:
  /* FIXME: in the feature we should consider switching to uint32_t also
   * in data structures. */
  std::map<std::string, int> acl_user_map;
  std::map<uint32_t, int> acl_group_map;
  std::list<ACLReferer> referer_list;
  ACLGrantMap grant_map;
  // register a grant in the correspoding acl_user/group_map
  void register_grant(const ACLGrant& grant);
public:
  uint32_t get_perm(const DoutPrefixProvider* dpp,
                    const rgw::auth::Identity& auth_identity,
                    uint32_t perm_mask) const;
  uint32_t get_group_perm(const DoutPrefixProvider *dpp, ACLGroupTypeEnum group, uint32_t perm_mask) const;
  uint32_t get_referer_perm(const DoutPrefixProvider *dpp, uint32_t current_perm,
                            std::string http_referer,
                            uint32_t perm_mask) const;
  void encode(bufferlist& bl) const {
    ENCODE_START(4, 3, bl);
    bool maps_initialized = true;
    encode(maps_initialized, bl);
    encode(acl_user_map, bl);
    encode(grant_map, bl);
    encode(acl_group_map, bl);
    encode(referer_list, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(4, 3, 3, bl);
    bool maps_initialized;
    decode(maps_initialized, bl);
    decode(acl_user_map, bl);
    decode(grant_map, bl);
    if (struct_v >= 2) {
      decode(acl_group_map, bl);
    } else if (!maps_initialized) {
      // register everything in the grant_map
      for (const auto& [id, grant] : grant_map) {
        register_grant(grant);
      }
    }
    if (struct_v >= 4) {
      decode(referer_list, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<RGWAccessControlList*>& o);

  void add_grant(const ACLGrant& grant);
  void remove_canon_user_grant(const rgw_user& user_id);

  ACLGrantMap& get_grant_map() { return grant_map; }
  const ACLGrantMap& get_grant_map() const { return grant_map; }

  void create_default(const rgw_user& id, const std::string& name) {
    acl_user_map.clear();
    acl_group_map.clear();
    referer_list.clear();

    ACLGrant grant;
    grant.set_canon(id, name, RGW_PERM_FULL_CONTROL);
    add_grant(grant);
  }

  friend bool operator==(const RGWAccessControlList& lhs, const RGWAccessControlList& rhs);
  friend bool operator!=(const RGWAccessControlList& lhs, const RGWAccessControlList& rhs);
};
WRITE_CLASS_ENCODER(RGWAccessControlList)

struct ACLOwner {
  rgw_user id;
  std::string display_name;

  void encode(bufferlist& bl) const {
    ENCODE_START(3, 2, bl);
    std::string s;
    id.to_str(s);
    encode(s, bl);
    encode(display_name, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
    std::string s;
    decode(s, bl);
    id.from_str(s);
    decode(display_name, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<ACLOwner*>& o);

  auto operator<=>(const ACLOwner&) const = default;
};
WRITE_CLASS_ENCODER(ACLOwner)

class RGWAccessControlPolicy
{
protected:
  RGWAccessControlList acl;
  ACLOwner owner;

public:
  uint32_t get_perm(const DoutPrefixProvider* dpp,
                    const rgw::auth::Identity& auth_identity,
                    uint32_t perm_mask,
                    const char * http_referer,
                    bool ignore_public_acls=false) const;
  bool verify_permission(const DoutPrefixProvider* dpp,
                         const rgw::auth::Identity& auth_identity,
                         uint32_t user_perm_mask,
                         uint32_t perm,
                         const char * http_referer = nullptr,
                         bool ignore_public_acls=false) const;

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    encode(owner, bl);
    encode(acl, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    decode(owner, bl);
    decode(acl, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<RGWAccessControlPolicy*>& o);
  void decode_owner(bufferlist::const_iterator& bl) { // sometimes we only need that, should be faster
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    decode(owner, bl);
    DECODE_FINISH(bl);
  }

  void set_owner(const ACLOwner& o) { owner = o; }
  const ACLOwner& get_owner() const { return owner; }
  ACLOwner& get_owner() { return owner; }

  void create_default(const rgw_user& id, const std::string& name) {
    acl.create_default(id, name);
    owner.id = id;
    owner.display_name = name;
  }
  RGWAccessControlList& get_acl() {
    return acl;
  }
  const RGWAccessControlList& get_acl() const {
    return acl;
  }

  bool is_public(const DoutPrefixProvider *dpp) const;

  friend bool operator==(const RGWAccessControlPolicy& lhs, const RGWAccessControlPolicy& rhs);
  friend bool operator!=(const RGWAccessControlPolicy& lhs, const RGWAccessControlPolicy& rhs);
};
WRITE_CLASS_ENCODER(RGWAccessControlPolicy)
