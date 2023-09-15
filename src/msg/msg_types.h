// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MSG_TYPES_H
#define CEPH_MSG_TYPES_H

#include <sstream>

#include <netinet/in.h>
#include "common/fmt_common.h"
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

#include "include/ceph_features.h"
#include "include/types.h"
#include "include/blobhash.h"
#include "include/encoding.h"

#define MAX_PORT_NUMBER 65535

#ifdef _WIN32
// ceph_sockaddr_storage matches the Linux format.
#define AF_INET6_LINUX 10
#endif

namespace ceph {
  class Formatter;
}

std::ostream& operator<<(std::ostream& out, const sockaddr_storage &ss);
std::ostream& operator<<(std::ostream& out, const sockaddr *sa);

typedef uint8_t entity_type_t;

class entity_name_t {
public:
  entity_type_t _type;
  int64_t _num;

public:
  static const int TYPE_MON = CEPH_ENTITY_TYPE_MON;
  static const int TYPE_MDS = CEPH_ENTITY_TYPE_MDS;
  static const int TYPE_OSD = CEPH_ENTITY_TYPE_OSD;
  static const int TYPE_CLIENT = CEPH_ENTITY_TYPE_CLIENT;
  static const int TYPE_MGR = CEPH_ENTITY_TYPE_MGR;

  static const int64_t NEW = -1;

  // cons
  entity_name_t() : _type(0), _num(0) { }
  entity_name_t(int t, int64_t n) : _type(t), _num(n) { }
  explicit entity_name_t(const ceph_entity_name &n) :
    _type(n.type), _num(n.num) { }

  // static cons
  static entity_name_t MON(int64_t i=NEW) { return entity_name_t(TYPE_MON, i); }
  static entity_name_t MDS(int64_t i=NEW) { return entity_name_t(TYPE_MDS, i); }
  static entity_name_t OSD(int64_t i=NEW) { return entity_name_t(TYPE_OSD, i); }
  static entity_name_t CLIENT(int64_t i=NEW) { return entity_name_t(TYPE_CLIENT, i); }
  static entity_name_t MGR(int64_t i=NEW) { return entity_name_t(TYPE_MGR, i); }

  int64_t num() const { return _num; }
  int type() const { return _type; }
  const char *type_str() const {
    return ceph_entity_type_name(type());
  }

  bool is_new() const { return num() < 0; }

  bool is_client() const { return type() == TYPE_CLIENT; }
  bool is_mds() const { return type() == TYPE_MDS; }
  bool is_osd() const { return type() == TYPE_OSD; }
  bool is_mon() const { return type() == TYPE_MON; }
  bool is_mgr() const { return type() == TYPE_MGR; }

  operator ceph_entity_name() const {
    ceph_entity_name n = { _type, ceph_le64(_num) };
    return n;
  }

  bool parse(std::string_view s);

  DENC(entity_name_t, v, p) {
    denc(v._type, p);
    denc(v._num, p);
  }
  void dump(ceph::Formatter *f) const;

  static void generate_test_instances(std::list<entity_name_t*>& o);
};
WRITE_CLASS_DENC(entity_name_t)

inline bool operator== (const entity_name_t& l, const entity_name_t& r) {
  return (l.type() == r.type()) && (l.num() == r.num()); }
inline bool operator!= (const entity_name_t& l, const entity_name_t& r) {
  return (l.type() != r.type()) || (l.num() != r.num()); }
inline bool operator< (const entity_name_t& l, const entity_name_t& r) {
  return (l.type() < r.type()) || (l.type() == r.type() && l.num() < r.num()); }

inline std::ostream& operator<<(std::ostream& out, const entity_name_t& addr) {
  //if (addr.is_namer()) return out << "namer";
  if (addr.is_new() || addr.num() < 0)
    return out << addr.type_str() << ".?";
  else
    return out << addr.type_str() << '.' << addr.num();
}
inline std::ostream& operator<<(std::ostream& out, const ceph_entity_name& addr) {
  return out << entity_name_t{addr.type, static_cast<int64_t>(addr.num)};
}

namespace std {
  template<> struct hash< entity_name_t >
  {
    size_t operator()( const entity_name_t &m ) const
    {
      return rjhash32(m.type() ^ m.num());
    }
  };
} // namespace std

// define a wire format for sockaddr that matches Linux's.
struct ceph_sockaddr_storage {
  ceph_le16 ss_family;
  __u8 __ss_padding[128 - sizeof(ceph_le16)];

  void encode(ceph::buffer::list& bl) const {
    struct ceph_sockaddr_storage ss = *this;
    ss.ss_family = htons(ss.ss_family);
    ceph::encode_raw(ss, bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    struct ceph_sockaddr_storage ss;
    ceph::decode_raw(ss, bl);
    ss.ss_family = ntohs(ss.ss_family);
    *this = ss;
  }
} __attribute__ ((__packed__));
WRITE_CLASS_ENCODER(ceph_sockaddr_storage)

/*
 * encode sockaddr.ss_family as network byte order
 */
static inline void encode(const sockaddr_storage& a, ceph::buffer::list& bl) {
#if defined(__linux__)
  struct sockaddr_storage ss = a;
  ss.ss_family = htons(ss.ss_family);
  ceph::encode_raw(ss, bl);
#elif defined(__FreeBSD__) || defined(__APPLE__)
  ceph_sockaddr_storage ss{};
  auto src = (unsigned char const *)&a;
  auto dst = (unsigned char *)&ss;
  src += sizeof(a.ss_len);
  ss.ss_family = a.ss_family;
  src += sizeof(a.ss_family);
  dst += sizeof(ss.ss_family);
  const auto copy_size = std::min((unsigned char*)(&a + 1) - src,
				  (unsigned char*)(&ss + 1) - dst);
  ::memcpy(dst, src, copy_size);
  encode(ss, bl);
#elif defined(_WIN32)
  ceph_sockaddr_storage ss{};
  ::memcpy(&ss, &a, std::min(sizeof(ss), sizeof(a)));
  // The Windows AF_INET6 definition doesn't match the Linux one.
  if (a.ss_family == AF_INET6) {
    ss.ss_family = AF_INET6_LINUX;
  }
  encode(ss, bl);
#else
  ceph_sockaddr_storage ss;
  ::memset(&ss, '\0', sizeof(ss));
  ::memcpy(&ss, &a, std::min(sizeof(ss), sizeof(a)));
  encode(ss, bl);
#endif
}
static inline void decode(sockaddr_storage& a,
			  ceph::buffer::list::const_iterator& bl) {
#if defined(__linux__)
  ceph::decode_raw(a, bl);
  a.ss_family = ntohs(a.ss_family);
#elif defined(__FreeBSD__) || defined(__APPLE__)
  ceph_sockaddr_storage ss{};
  decode(ss, bl);
  auto src = (unsigned char const *)&ss;
  auto dst = (unsigned char *)&a;
  a.ss_len = 0;
  dst += sizeof(a.ss_len);
  a.ss_family = ss.ss_family;
  src += sizeof(ss.ss_family);
  dst += sizeof(a.ss_family);
  auto const copy_size = std::min((unsigned char*)(&ss + 1) - src,
				  (unsigned char*)(&a + 1) - dst);
  ::memcpy(dst, src, copy_size);
#elif defined(_WIN32)
  ceph_sockaddr_storage ss{};
  decode(ss, bl);
  ::memcpy(&a, &ss, std::min(sizeof(ss), sizeof(a)));
  if (a.ss_family == AF_INET6_LINUX) {
    a.ss_family = AF_INET6;
  }
#else
  ceph_sockaddr_storage ss{};
  decode(ss, bl);
  ::memcpy(&a, &ss, std::min(sizeof(ss), sizeof(a)));
#endif
}

/*
 * an entity's network address.
 * includes a random value that prevents it from being reused.
 * thus identifies a particular process instance.
 *
 * This also happens to work to support cidr ranges, in which
 * case the nonce contains the netmask. It's great!
 */
struct entity_addr_t {
  typedef enum {
    TYPE_NONE = 0,
    TYPE_LEGACY = 1,  ///< legacy msgr1 protocol (ceph jewel and older)
    TYPE_MSGR2 = 2,   ///< msgr2 protocol (new in ceph kraken)
    TYPE_ANY = 3,  ///< ambiguous
    TYPE_CIDR = 4,
  } type_t;
  static const type_t TYPE_DEFAULT = TYPE_MSGR2;
  static std::string_view get_type_name(int t) {
    switch (t) {
    case TYPE_NONE: return "none";
    case TYPE_LEGACY: return "v1";
    case TYPE_MSGR2: return "v2";
    case TYPE_ANY: return "any";
    case TYPE_CIDR: return "cidr";
    default: return "???";
    }
  };

  __u32 type;
  __u32 nonce;
  union {
    sockaddr sa;
    sockaddr_in sin;
    sockaddr_in6 sin6;
  } u;

  entity_addr_t() : type(0), nonce(0) {
    memset(&u, 0, sizeof(u));
  }
  entity_addr_t(__u32 _type, __u32 _nonce) : type(_type), nonce(_nonce) {
    memset(&u, 0, sizeof(u));
  }
  explicit entity_addr_t(const ceph_entity_addr &o) {
    type = o.type;
    nonce = o.nonce;
    memcpy(&u, &o.in_addr, sizeof(u));
#if !defined(__FreeBSD__)
    u.sa.sa_family = ntohs(u.sa.sa_family);
#endif
  }

  uint32_t get_type() const { return type; }
  void set_type(uint32_t t) { type = t; }
  bool is_legacy() const { return type == TYPE_LEGACY; }
  bool is_msgr2() const { return type == TYPE_MSGR2; }
  bool is_any() const { return type == TYPE_ANY; }
  // this isn't a guarantee; some client addrs will match it
  bool maybe_cidr() const { return get_port() == 0 && nonce != 0; }

  __u32 get_nonce() const { return nonce; }
  void set_nonce(__u32 n) { nonce = n; }

  int get_family() const {
    return u.sa.sa_family;
  }
  void set_family(int f) {
    u.sa.sa_family = f;
  }

  bool is_ipv4() const {
    return u.sa.sa_family == AF_INET;
  }
  bool is_ipv6() const {
    return u.sa.sa_family == AF_INET6;
  }

  sockaddr_in &in4_addr() {
    return u.sin;
  }
  const sockaddr_in &in4_addr() const{
    return u.sin;
  }
  sockaddr_in6 &in6_addr(){
    return u.sin6;
  }
  const sockaddr_in6 &in6_addr() const{
    return u.sin6;
  }
  const sockaddr *get_sockaddr() const {
    return &u.sa;
  }
  size_t get_sockaddr_len() const {
    switch (u.sa.sa_family) {
    case AF_INET:
      return sizeof(u.sin);
    case AF_INET6:
      return sizeof(u.sin6);
    }
    return sizeof(u);
  }
  bool set_sockaddr(const struct sockaddr *sa)
  {
    switch (sa->sa_family) {
    case AF_INET:
      // pre-zero, since we're only copying a portion of the source
      memset(&u, 0, sizeof(u));
      memcpy(&u.sin, sa, sizeof(u.sin));
      break;
    case AF_INET6:
      // pre-zero, since we're only copying a portion of the source
      memset(&u, 0, sizeof(u));
      memcpy(&u.sin6, sa, sizeof(u.sin6));
      break;
    case AF_UNSPEC:
      memset(&u, 0, sizeof(u));
      break;
    default:
      return false;
    }
    return true;
  }

  sockaddr_storage get_sockaddr_storage() const {
    sockaddr_storage ss;
    memcpy(&ss, &u, sizeof(u));
    memset((char*)&ss + sizeof(u), 0, sizeof(ss) - sizeof(u));
    return ss;
  }

  void set_in4_quad(int pos, int val) {
    u.sin.sin_family = AF_INET;
    unsigned char *ipq = (unsigned char*)&u.sin.sin_addr.s_addr;
    ipq[pos] = val;
  }
  void set_port(int port) {
    switch (u.sa.sa_family) {
    case AF_INET:
      u.sin.sin_port = htons(port);
      break;
    case AF_INET6:
      u.sin6.sin6_port = htons(port);
      break;
    default:
      ceph_abort();
    }
  }
  int get_port() const {
    switch (u.sa.sa_family) {
    case AF_INET:
      return ntohs(u.sin.sin_port);
    case AF_INET6:
      return ntohs(u.sin6.sin6_port);
    }
    return 0;
  }

  operator ceph_entity_addr() const {
    ceph_entity_addr a;
    a.type = 0;
    a.nonce = nonce;
    a.in_addr = get_sockaddr_storage();
#if !defined(__FreeBSD__)
    a.in_addr.ss_family = htons(a.in_addr.ss_family);
#endif
    return a;
  }

  bool probably_equals(const entity_addr_t &o) const {
    if (get_port() != o.get_port())
      return false;
    if (get_nonce() != o.get_nonce())
      return false;
    if (is_blank_ip() || o.is_blank_ip())
      return true;
    if (memcmp(&u, &o.u, sizeof(u)) == 0)
      return true;
    return false;
  }

  bool is_same_host(const entity_addr_t &o) const {
    if (u.sa.sa_family != o.u.sa.sa_family)
      return false;
    if (u.sa.sa_family == AF_INET)
      return u.sin.sin_addr.s_addr == o.u.sin.sin_addr.s_addr;
    if (u.sa.sa_family == AF_INET6)
      return memcmp(u.sin6.sin6_addr.s6_addr,
		    o.u.sin6.sin6_addr.s6_addr,
		    sizeof(u.sin6.sin6_addr.s6_addr)) == 0;
    return false;
  }

  bool is_blank_ip() const {
    switch (u.sa.sa_family) {
    case AF_INET:
      return u.sin.sin_addr.s_addr == INADDR_ANY;
    case AF_INET6:
      return memcmp(&u.sin6.sin6_addr, &in6addr_any, sizeof(in6addr_any)) == 0;
    default:
      return true;
    }
  }

  bool is_ip() const {
    switch (u.sa.sa_family) {
    case AF_INET:
    case AF_INET6:
      return true;
    default:
      return false;
    }
  }

  std::string ip_only_to_str() const;
  std::string ip_n_port_to_str() const;

  std::string get_legacy_str() const {
    std::ostringstream ss;
    ss << get_sockaddr() << "/" << get_nonce();
    return ss.str();
  }

  bool parse(const std::string_view s, int default_type=TYPE_DEFAULT);
  bool parse(const char *s, const char **end = 0, int default_type=TYPE_DEFAULT);

  void decode_legacy_addr_after_marker(ceph::buffer::list::const_iterator& bl)
  {
    using ceph::decode;
    __u8 marker;
    __u16 rest;
    decode(marker, bl);
    decode(rest, bl);
    decode(nonce, bl);
    sockaddr_storage ss;
    decode(ss, bl);
    set_sockaddr((sockaddr*)&ss);
    if (get_family() == AF_UNSPEC) {
      type = TYPE_NONE;
    } else {
      type = TYPE_LEGACY;
    }
  }

  // Right now, these only deal with sockaddr_storage that have only family and content.
  // Apparently on BSD there is also an ss_len that we need to handle; this requires
  // broader study

  void encode(ceph::buffer::list& bl, uint64_t features) const {
    using ceph::encode;
    if ((features & CEPH_FEATURE_MSG_ADDR2) == 0) {
      encode((__u32)0, bl);
      encode(nonce, bl);
      sockaddr_storage ss = get_sockaddr_storage();
      encode(ss, bl);
      return;
    }
    encode((__u8)1, bl);
    ENCODE_START(1, 1, bl);
    if (HAVE_FEATURE(features, SERVER_NAUTILUS)) {
      encode(type, bl);
    } else {
      // map any -> legacy for old clients.  this is primary for the benefit
      // of OSDMap's blocklist, but is reasonable in general since any: is
      // meaningless for pre-nautilus clients or daemons.
      auto t = type;
      if (t == TYPE_ANY) {
	t = TYPE_LEGACY;
      }
      encode(t, bl);
    }
    encode(nonce, bl);
    __u32 elen = get_sockaddr_len();
#if (__FreeBSD__) || defined(__APPLE__)
      elen -= sizeof(u.sa.sa_len);
#endif
    encode(elen, bl);
    if (elen) {
      uint16_t ss_family = u.sa.sa_family;
#if defined(_WIN32)
      if (ss_family == AF_INET6) {
        ss_family = AF_INET6_LINUX;
      }
#endif
      encode(ss_family, bl);
      elen -= sizeof(u.sa.sa_family);
      bl.append(u.sa.sa_data, elen);
    }
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    __u8 marker;
    decode(marker, bl);
    if (marker == 0) {
      decode_legacy_addr_after_marker(bl);
      return;
    }
    if (marker != 1)
      throw ceph::buffer::malformed_input("entity_addr_t marker != 1");
    DECODE_START(1, bl);
    decode(type, bl);
    decode(nonce, bl);
    __u32 elen;
    decode(elen, bl);
    if (elen) {
#if defined(__FreeBSD__) || defined(__APPLE__)
      u.sa.sa_len = 0;
#endif
      uint16_t ss_family;
      if (elen < sizeof(ss_family)) {
	throw ceph::buffer::malformed_input("elen smaller than family len");
      }
      decode(ss_family, bl);
#if defined(_WIN32)
      if (ss_family == AF_INET6_LINUX) {
        ss_family = AF_INET6;
      }
#endif
      u.sa.sa_family = ss_family;
      elen -= sizeof(ss_family);
      if (elen > get_sockaddr_len() - sizeof(u.sa.sa_family)) {
	throw ceph::buffer::malformed_input("elen exceeds sockaddr len");
      }
      bl.copy(elen, u.sa.sa_data);
    }
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  std::string fmt_print() const; ///< used by the default fmt formatter

  static void generate_test_instances(std::list<entity_addr_t*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(entity_addr_t)

std::ostream& operator<<(std::ostream& out, const entity_addr_t &addr);

inline bool operator==(const entity_addr_t& a, const entity_addr_t& b) { return memcmp(&a, &b, sizeof(a)) == 0; }
inline bool operator!=(const entity_addr_t& a, const entity_addr_t& b) { return memcmp(&a, &b, sizeof(a)) != 0; }
inline bool operator<(const entity_addr_t& a, const entity_addr_t& b) { return memcmp(&a, &b, sizeof(a)) < 0; }
inline bool operator<=(const entity_addr_t& a, const entity_addr_t& b) { return memcmp(&a, &b, sizeof(a)) <= 0; }
inline bool operator>(const entity_addr_t& a, const entity_addr_t& b) { return memcmp(&a, &b, sizeof(a)) > 0; }
inline bool operator>=(const entity_addr_t& a, const entity_addr_t& b) { return memcmp(&a, &b, sizeof(a)) >= 0; }

namespace std {
template<> struct hash<entity_addr_t> {
  size_t operator()( const entity_addr_t& x ) const {
    static blobhash H;
    return H(&x, sizeof(x));
  }
};
} // namespace std

struct entity_addrvec_t {
  std::vector<entity_addr_t> v;

  entity_addrvec_t() {}
  explicit entity_addrvec_t(const entity_addr_t& a) : v({ a }) {}

  unsigned size() const { return v.size(); }
  bool empty() const { return v.empty(); }

  entity_addr_t legacy_addr() const {
    return addr_of_type(entity_addr_t::TYPE_LEGACY);
  }
  entity_addr_t as_legacy_addr() const {
    for (auto& a : v) {
      if (a.is_legacy()) {
	return a;
      }
      if (a.is_any()) {
	auto b = a;
	b.set_type(entity_addr_t::TYPE_LEGACY);
	return b;
      }
    }
    // hrm... lie!
    auto a = front();
    a.set_type(entity_addr_t::TYPE_LEGACY);
    return a;
  }
  entity_addr_t front() const {
    if (!v.empty()) {
      return v.front();
    }
    return entity_addr_t();
  }
  entity_addr_t legacy_or_front_addr() const {
    for (auto& a : v) {
      if (a.type == entity_addr_t::TYPE_LEGACY) {
	return a;
      }
    }
    return front();
  }
  std::string get_legacy_str() const {
    return legacy_or_front_addr().get_legacy_str();
  }

  entity_addr_t msgr2_addr() const {
    return addr_of_type(entity_addr_t::TYPE_MSGR2);
  }
  bool has_msgr2() const {
    for (auto& a : v) {
      if (a.is_msgr2()) {
	return true;
      }
    }
    return false;
  }

  entity_addr_t pick_addr(uint32_t type) const {
    entity_addr_t picked_addr;
    switch (type) {
    case entity_addr_t::TYPE_LEGACY:
      [[fallthrough]];
    case entity_addr_t::TYPE_MSGR2:
      picked_addr = addr_of_type(type);
      break;
    case entity_addr_t::TYPE_ANY:
      return front();
    default:
      return {};
    }
    if (!picked_addr.is_blank_ip()) {
      return picked_addr;
    } else {
      return addr_of_type(entity_addr_t::TYPE_ANY);
    }
  }

  entity_addr_t addr_of_type(uint32_t type) const {
    for (auto &a : v) {
      if (a.type == type) {
        return a;
      }
    }
    return entity_addr_t();
  }

  bool parse(const char *s, const char **end = 0);

  void get_ports(std::set<int> *ports) const {
    for (auto& a : v) {
      ports->insert(a.get_port());
    }
  }
  std::set<int> get_ports() const {
    std::set<int> r;
    get_ports(&r);
    return r;
  }

  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<entity_addrvec_t*>& ls);

  bool legacy_equals(const entity_addrvec_t& o) const {
    if (v == o.v) {
      return true;
    }
    if (v.size() == 1 &&
	front().is_legacy() &&
	front() == o.legacy_addr()) {
      return true;
    }
    if (o.v.size() == 1 &&
	o.front().is_legacy() &&
	o.front() == legacy_addr()) {
      return true;
    }
    return false;
  }

  bool probably_equals(const entity_addrvec_t& o) const {
    for (unsigned i = 0; i < v.size(); ++i) {
      if (!v[i].probably_equals(o.v[i])) {
	return false;
      }
    }
    return true;
  }
  bool contains(const entity_addr_t& a) const {
    for (auto& i : v) {
      if (a == i) {
	return true;
      }
    }
    return false;
  }
  bool is_same_host(const entity_addr_t& a) const {
    for (auto& i : v) {
      if (i.is_same_host(a)) {
	return true;
      }
    }
    return false;
  }

  friend std::ostream& operator<<(std::ostream& out, const entity_addrvec_t& av) {
    if (av.v.empty()) {
      return out;
    } else if (av.v.size() == 1) {
      return out << av.v[0];
    } else {
      return out << av.v;
    }
  }

  friend bool operator==(const entity_addrvec_t& l, const entity_addrvec_t& r) {
    return l.v == r.v;
  }
  friend bool operator!=(const entity_addrvec_t& l, const entity_addrvec_t& r) {
    return l.v != r.v;
  }
  friend bool operator<(const entity_addrvec_t& l, const entity_addrvec_t& r) {
    return l.v < r.v;  // see lexicographical_compare()
  }
  friend bool operator>(const entity_addrvec_t& l, const entity_addrvec_t& r) {
    return l.v > r.v;  // see lexicographical_compare()
  }
};
WRITE_CLASS_ENCODER_FEATURES(entity_addrvec_t);
#if FMT_VERSION >= 90000
template <> struct fmt::formatter<entity_addrvec_t> : fmt::ostream_formatter {};
#endif

namespace std {
template<> struct hash<entity_addrvec_t> {
  size_t operator()( const entity_addrvec_t& x) const {
    static blobhash H;
    size_t r = 0;
    for (auto& i : x.v) {
      r += H((const char*)&i, sizeof(i));
    }
    return r;
  }
};
} // namespace std

/*
 * a particular entity instance
 */
struct entity_inst_t {
  entity_name_t name;
  entity_addr_t addr;
  entity_inst_t() {}
  entity_inst_t(entity_name_t n, const entity_addr_t& a) : name(n), addr(a) {}
  // cppcheck-suppress noExplicitConstructor
  entity_inst_t(const ceph_entity_inst& i) : name(i.name), addr(i.addr) { }
  entity_inst_t(const ceph_entity_name& n, const ceph_entity_addr &a) : name(n), addr(a) {}
  operator ceph_entity_inst() {
    ceph_entity_inst i = {name, addr};
    return i;
  }

  void encode(ceph::buffer::list& bl, uint64_t features) const {
    using ceph::encode;
    encode(name, bl);
    encode(addr, bl, features);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    decode(name, bl);
    decode(addr, bl);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<entity_inst_t*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(entity_inst_t)


inline bool operator==(const entity_inst_t& a, const entity_inst_t& b) {
  return a.name == b.name && a.addr == b.addr;
}
inline bool operator!=(const entity_inst_t& a, const entity_inst_t& b) {
  return a.name != b.name || a.addr != b.addr;
}
inline bool operator<(const entity_inst_t& a, const entity_inst_t& b) {
  return a.name < b.name || (a.name == b.name && a.addr < b.addr);
}
inline bool operator<=(const entity_inst_t& a, const entity_inst_t& b) {
  return a.name < b.name || (a.name == b.name && a.addr <= b.addr);
}
inline bool operator>(const entity_inst_t& a, const entity_inst_t& b) { return b < a; }
inline bool operator>=(const entity_inst_t& a, const entity_inst_t& b) { return b <= a; }

namespace std {
  template<> struct hash< entity_inst_t >
  {
    size_t operator()( const entity_inst_t& x ) const
    {
      static hash< entity_name_t > H;
      static hash< entity_addr_t > I;
      return H(x.name) ^ I(x.addr);
    }
  };
} // namespace std


inline std::ostream& operator<<(std::ostream& out, const entity_inst_t &i)
{
  return out << i.name << " " << i.addr;
}
inline std::ostream& operator<<(std::ostream& out, const ceph_entity_inst &i)
{
  entity_inst_t n = i;
  return out << n;
}

#endif
