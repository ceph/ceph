// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_BASIC_TYPES_H
#define CEPH_RGW_BASIC_TYPES_H

#include <string>

#include "include/types.h"

struct rgw_user {
  std::string tenant;
  std::string id;

  rgw_user() {}
  // cppcheck-suppress noExplicitConstructor
  rgw_user(const std::string& s) {
    from_str(s);
  }
  rgw_user(const std::string& tenant, const std::string& id)
    : tenant(tenant),
      id(id) {
  }
  rgw_user(std::string&& tenant, std::string&& id)
    : tenant(std::move(tenant)),
      id(std::move(id)) {
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(tenant, bl);
    encode(id, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(tenant, bl);
    decode(id, bl);
    DECODE_FINISH(bl);
  }

  void to_str(std::string& str) const {
    if (!tenant.empty()) {
      str = tenant + '$' + id;
    } else {
      str = id;
    }
  }

  void clear() {
    tenant.clear();
    id.clear();
  }

  bool empty() const {
    return id.empty();
  }

  string to_str() const {
    string s;
    to_str(s);
    return s;
  }

  void from_str(const std::string& str) {
    size_t pos = str.find('$');
    if (pos != std::string::npos) {
      tenant = str.substr(0, pos);
      id = str.substr(pos + 1);
    } else {
      tenant.clear();
      id = str;
    }
  }

  rgw_user& operator=(const string& str) {
    from_str(str);
    return *this;
  }

  int compare(const rgw_user& u) const {
    int r = tenant.compare(u.tenant);
    if (r != 0)
      return r;

    return id.compare(u.id);
  }
  int compare(const string& str) const {
    rgw_user u(str);
    return compare(u);
  }

  bool operator!=(const rgw_user& rhs) const {
    return (compare(rhs) != 0);
  }
  bool operator==(const rgw_user& rhs) const {
    return (compare(rhs) == 0);
  }
  bool operator<(const rgw_user& rhs) const {
    if (tenant < rhs.tenant) {
      return true;
    } else if (tenant > rhs.tenant) {
      return false;
    }
    return (id < rhs.id);
  }
};
WRITE_CLASS_ENCODER(rgw_user)

// Represents an identity. This is more wide-ranging than a
// 'User'. Its purposes is to be matched against by an
// IdentityApplier. The internal representation will doubtless change as
// more types are added. We may want to expose the type enum and make
// the member public so people can switch/case on it.

namespace rgw {
namespace auth {
class Principal {
  enum types { User, Role, Tenant, Wildcard, OidcProvider };
  types t;
  rgw_user u;
  string idp_url;

  explicit Principal(types t)
    : t(t) {}

  Principal(types t, std::string&& n, std::string i)
    : t(t), u(std::move(n), std::move(i)) {}

  Principal(string&& idp_url)
    : t(OidcProvider), idp_url(std::move(idp_url)) {}

public:

  static Principal wildcard() {
    return Principal(Wildcard);
  }

  static Principal user(std::string&& t, std::string&& u) {
    return Principal(User, std::move(t), std::move(u));
  }

  static Principal role(std::string&& t, std::string&& u) {
    return Principal(Role, std::move(t), std::move(u));
  }

  static Principal tenant(std::string&& t) {
    return Principal(Tenant, std::move(t), {});
  }

  static Principal oidc_provider(string&& idp_url) {
    return Principal(std::move(idp_url));
  }

  bool is_wildcard() const {
    return t == Wildcard;
  }

  bool is_user() const {
    return t == User;
  }

  bool is_role() const {
    return t == Role;
  }

  bool is_tenant() const {
    return t == Tenant;
  }

  bool is_oidc_provider() const {
    return t == OidcProvider;
  }

  const std::string& get_tenant() const {
    return u.tenant;
  }

  const std::string& get_id() const {
    return u.id;
  }

  const string& get_idp_url() const {
    return idp_url;
  }

  bool operator ==(const Principal& o) const {
    return (t == o.t) && (u == o.u);
  }

  bool operator <(const Principal& o) const {
    return (t < o.t) || ((t == o.t) && (u < o.u));
  }
};

std::ostream& operator <<(std::ostream& m, const Principal& p);
}
}

class JSONObj;

void decode_json_obj(rgw_user& val, JSONObj *obj);
void encode_json(const char *name, const rgw_user& val, Formatter *f);

inline ostream& operator<<(ostream& out, const rgw_user &u) {
  string s;
  u.to_str(s);
  return out << s;
}


#endif
