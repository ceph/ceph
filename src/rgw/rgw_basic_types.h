// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_BASIC_TYPES_H
#define CEPH_RGW_BASIC_TYPES_H

#include <string>

#include "include/types.h"

class JSONObj;
class cls_user_bucket;

struct rgw_user {
  std::string tenant;
  std::string id;

  rgw_user() {}
  explicit rgw_user(const std::string& s) {
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

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(tenant, bl);
    encode(id, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
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

  std::string to_str() const {
    std::string s;
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

  rgw_user& operator=(const std::string& str) {
    from_str(str);
    return *this;
  }

  int compare(const rgw_user& u) const {
    int r = tenant.compare(u.tenant);
    if (r != 0)
      return r;

    return id.compare(u.id);
  }
  int compare(const std::string& str) const {
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
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<rgw_user*>& o);
};
WRITE_CLASS_ENCODER(rgw_user)

struct rgw_pool {
  std::string name;
  std::string ns;

  rgw_pool() = default;
  rgw_pool(const rgw_pool& _p) : name(_p.name), ns(_p.ns) {}
  rgw_pool(rgw_pool&&) = default;
  rgw_pool(const std::string& _s) {
    from_str(_s);
  }
  rgw_pool(const std::string& _name, const std::string& _ns) : name(_name), ns(_ns) {}

  std::string to_str() const;
  void from_str(const std::string& s);

  void init(const std::string& _s) {
    from_str(_s);
  }

  bool empty() const {
    return name.empty();
  }

  int compare(const rgw_pool& p) const {
    int r = name.compare(p.name);
    if (r != 0) {
      return r;
    }
    return ns.compare(p.ns);
  }

  void encode(ceph::buffer::list& bl) const {
     ENCODE_START(10, 10, bl);
    encode(name, bl);
    encode(ns, bl);
    ENCODE_FINISH(bl);
  }

  void decode_from_bucket(ceph::buffer::list::const_iterator& bl);

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(10, 3, 3, bl);

    decode(name, bl);

    if (struct_v < 10) {

    /*
     * note that rgw_pool can be used where rgw_bucket was used before
     * therefore we inherit rgw_bucket's old versions. However, we only
     * need the first field from rgw_bucket. unless we add more fields
     * in which case we'll need to look at struct_v, and check the actual
     * version. Anything older than 10 needs to be treated as old rgw_bucket
     */

    } else {
      decode(ns, bl);
    }

    DECODE_FINISH(bl);
  }

  rgw_pool& operator=(const rgw_pool&) = default;

  bool operator==(const rgw_pool& p) const {
    return (compare(p) == 0);
  }
  bool operator!=(const rgw_pool& p) const {
    return !(*this == p);
  }
  bool operator<(const rgw_pool& p) const {
    int r = name.compare(p.name);
    if (r == 0) {
      return (ns.compare(p.ns) < 0);
    }
    return (r < 0);
  }
};
WRITE_CLASS_ENCODER(rgw_pool)

inline std::ostream& operator<<(std::ostream& out, const rgw_pool& p) {
  out << p.to_str();
  return out;
}

struct rgw_data_placement_target {
  rgw_pool data_pool;
  rgw_pool data_extra_pool;
  rgw_pool index_pool;

  rgw_data_placement_target() = default;
  rgw_data_placement_target(const rgw_data_placement_target&) = default;
  rgw_data_placement_target(rgw_data_placement_target&&) = default;

  rgw_data_placement_target(const rgw_pool& data_pool,
                            const rgw_pool& data_extra_pool,
                            const rgw_pool& index_pool)
    : data_pool(data_pool),
      data_extra_pool(data_extra_pool),
      index_pool(index_pool) {
  }

  rgw_data_placement_target&
  operator=(const rgw_data_placement_target&) = default;

  const rgw_pool& get_data_extra_pool() const {
    if (data_extra_pool.empty()) {
      return data_pool;
    }
    return data_extra_pool;
  }

  int compare(const rgw_data_placement_target& t) {
    int c = data_pool.compare(t.data_pool);
    if (c != 0) {
      return c;
    }
    c = data_extra_pool.compare(t.data_extra_pool);
    if (c != 0) {
      return c;
    }
    return index_pool.compare(t.index_pool);
  };

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
};

struct rgw_bucket_key {
  std::string tenant;
  std::string name;
  std::string bucket_id;

  rgw_bucket_key(const std::string& _tenant,
                 const std::string& _name,
                 const std::string& _bucket_id) : tenant(_tenant),
                                                  name(_name),
                                                  bucket_id(_bucket_id) {}
  rgw_bucket_key(const std::string& _tenant,
                 const std::string& _name) : tenant(_tenant),
                                             name(_name) {}
}; 

struct rgw_bucket {
  std::string tenant;
  std::string name;
  std::string marker;
  std::string bucket_id;
  rgw_data_placement_target explicit_placement;

  rgw_bucket() { }
  // cppcheck-suppress noExplicitConstructor
  explicit rgw_bucket(const rgw_user& u, const cls_user_bucket& b);

  rgw_bucket(const rgw_bucket_key& bk) : tenant(bk.tenant),
                                         name(bk.name),
                                         bucket_id(bk.bucket_id) {}
  rgw_bucket(const rgw_bucket&) = default;
  rgw_bucket(rgw_bucket&&) = default;

  bool match(const rgw_bucket& b) const {
    return (tenant == b.tenant &&
	    name == b.name &&
	    (bucket_id == b.bucket_id ||
	     bucket_id.empty() ||
	     b.bucket_id.empty()));
  }

  void convert(cls_user_bucket *b) const;

  void encode(ceph::buffer::list& bl) const {
     ENCODE_START(10, 10, bl);
    encode(name, bl);
    encode(marker, bl);
    encode(bucket_id, bl);
    encode(tenant, bl);
    bool encode_explicit = !explicit_placement.data_pool.empty();
    encode(encode_explicit, bl);
    if (encode_explicit) {
      encode(explicit_placement.data_pool, bl);
      encode(explicit_placement.data_extra_pool, bl);
      encode(explicit_placement.index_pool, bl);
    }
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(10, 3, 3, bl);
    decode(name, bl);
    if (struct_v < 10) {
      decode(explicit_placement.data_pool.name, bl);
    }
    if (struct_v >= 2) {
      decode(marker, bl);
      if (struct_v <= 3) {
        uint64_t id;
        decode(id, bl);
        char buf[16];
        snprintf(buf, sizeof(buf), "%" PRIu64, id);
        bucket_id = buf;
      } else {
        decode(bucket_id, bl);
      }
    }
    if (struct_v < 10) {
      if (struct_v >= 5) {
        decode(explicit_placement.index_pool.name, bl);
      } else {
        explicit_placement.index_pool = explicit_placement.data_pool;
      }
      if (struct_v >= 7) {
        decode(explicit_placement.data_extra_pool.name, bl);
      }
    }
    if (struct_v >= 8) {
      decode(tenant, bl);
    }
    if (struct_v >= 10) {
      bool decode_explicit = !explicit_placement.data_pool.empty();
      decode(decode_explicit, bl);
      if (decode_explicit) {
        decode(explicit_placement.data_pool, bl);
        decode(explicit_placement.data_extra_pool, bl);
        decode(explicit_placement.index_pool, bl);
      }
    }
    DECODE_FINISH(bl);
  }

  void update_bucket_id(const std::string& new_bucket_id) {
    bucket_id = new_bucket_id;
  }

  // format a key for the bucket/instance. pass delim=0 to skip a field
  std::string get_key(char tenant_delim = '/',
                      char id_delim = ':',
                      size_t reserve = 0) const;

  const rgw_pool& get_data_extra_pool() const {
    return explicit_placement.get_data_extra_pool();
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<rgw_bucket*>& o);

  rgw_bucket& operator=(const rgw_bucket&) = default;

  bool operator<(const rgw_bucket& b) const {
    if (name < b.name) {
      return true;
    } else if (name > b.name) {
      return false;
    }

    if (bucket_id < b.bucket_id) {
      return true;
    } else if (bucket_id > b.bucket_id) {
      return false;
    }

    return (tenant < b.tenant);
  }

  bool operator==(const rgw_bucket& b) const {
    return (tenant == b.tenant) && (name == b.name) && \
           (bucket_id == b.bucket_id);
  }
  bool operator!=(const rgw_bucket& b) const {
    return (tenant != b.tenant) || (name != b.name) ||
           (bucket_id != b.bucket_id);
  }
};
WRITE_CLASS_ENCODER(rgw_bucket)

inline std::ostream& operator<<(std::ostream& out, const rgw_bucket &b) {
  out << b.tenant << ":" << b.name << "[" << b.bucket_id << "])";
  return out;
}

struct rgw_bucket_shard {
  rgw_bucket bucket;
  int shard_id;

  rgw_bucket_shard() : shard_id(-1) {}
  rgw_bucket_shard(const rgw_bucket& _b, int _sid) : bucket(_b), shard_id(_sid) {}

  std::string get_key(char tenant_delim = '/', char id_delim = ':',
                      char shard_delim = ':') const;

  bool operator<(const rgw_bucket_shard& b) const {
    if (bucket < b.bucket) {
      return true;
    }
    if (b.bucket < bucket) {
      return false;
    }
    return shard_id < b.shard_id;
  }

  bool operator==(const rgw_bucket_shard& b) const {
    return (bucket == b.bucket &&
            shard_id == b.shard_id);
  }
};

inline std::ostream& operator<<(std::ostream& out, const rgw_bucket_shard& bs) {
  if (bs.shard_id <= 0) {
    return out << bs.bucket;
  }

  return out << bs.bucket << ":" << bs.shard_id;
}


struct rgw_zone_id {
  std::string id;

  rgw_zone_id() {}
  rgw_zone_id(const std::string& _id) : id(_id) {}
  rgw_zone_id(std::string&& _id) : id(std::move(_id)) {}

  void encode(ceph::buffer::list& bl) const {
    /* backward compatiblity, not using ENCODE_{START,END} macros */
    ceph::encode(id, bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    /* backward compatiblity, not using DECODE_{START,END} macros */
    ceph::decode(id, bl);
  }

  void clear() {
    id.clear();
  }

  bool operator==(const std::string& _id) const {
    return (id == _id);
  }
  bool operator==(const rgw_zone_id& zid) const {
    return (id == zid.id);
  }
  bool operator!=(const rgw_zone_id& zid) const {
    return (id != zid.id);
  }
  bool operator<(const rgw_zone_id& zid) const {
    return (id < zid.id);
  }
  bool operator>(const rgw_zone_id& zid) const {
    return (id > zid.id);
  }

  bool empty() const {
    return id.empty();
  }
};
WRITE_CLASS_ENCODER(rgw_zone_id)

inline std::ostream& operator<<(std::ostream& os, const rgw_zone_id& zid) {
  os << zid.id;
  return os;
}

void encode_json_impl(const char *name, const rgw_zone_id& zid, ceph::Formatter *f);
void decode_json_obj(rgw_zone_id& zid, JSONObj *obj);


// Represents an identity. This is more wide-ranging than a
// 'User'. Its purposes is to be matched against by an
// IdentityApplier. The internal representation will doubtless change as
// more types are added. We may want to expose the type enum and make
// the member public so people can switch/case on it.

namespace rgw {
namespace auth {
class Principal {
  enum types { User, Role, Tenant, Wildcard, OidcProvider, AssumedRole };
  types t;
  rgw_user u;
  std::string idp_url;

  explicit Principal(types t)
    : t(t) {}

  Principal(types t, std::string&& n, std::string i)
    : t(t), u(std::move(n), std::move(i)) {}

  Principal(std::string&& idp_url)
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

  static Principal oidc_provider(std::string&& idp_url) {
    return Principal(std::move(idp_url));
  }

  static Principal assumed_role(std::string&& t, std::string&& u) {
    return Principal(AssumedRole, std::move(t), std::move(u));
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

  bool is_assumed_role() const {
    return t == AssumedRole;
  }

  const std::string& get_tenant() const {
    return u.tenant;
  }

  const std::string& get_id() const {
    return u.id;
  }

  const std::string& get_idp_url() const {
    return idp_url;
  }

  const string& get_role_session() const {
    return u.id;
  }

  const string& get_role() const {
    return u.id;
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
void encode_json(const char *name, const rgw_user& val, ceph::Formatter *f);
void encode_xml(const char *name, const rgw_user& val, ceph::Formatter *f);

inline std::ostream& operator<<(std::ostream& out, const rgw_user &u) {
  std::string s;
  u.to_str(s);
  return out << s;
}


#endif
