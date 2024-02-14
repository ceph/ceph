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
#ifndef CEPH_TYPES_H
#define CEPH_TYPES_H

// this is needed for ceph_fs to compile in userland
#include "int_types.h"
#include "byteorder.h"

#include "uuid.h"

#include <netinet/in.h>
#include <fcntl.h>
#include <string.h>

#include "ceph_fs.h"
#include "ceph_frag.h"
#include "rbd_types.h"

#ifdef __cplusplus
#ifndef _BACKWARD_BACKWARD_WARNING_H
#define _BACKWARD_BACKWARD_WARNING_H   // make gcc 4.3 shut up about hash_*
#endif
#endif

extern "C" {
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "statlite.h"
}

#include <string>
#include <list>
#include <set>
#include <boost/container/flat_set.hpp>
#include <boost/container/flat_map.hpp>
#include <map>
#include <vector>
#include <optional>
#include <ostream>
#include <iomanip>


#include "include/unordered_map.h"

#include "object.h"
#include "intarith.h"

#include "acconfig.h"

#include "assert.h"

// DARWIN compatibility
#ifdef __APPLE__
typedef long long loff_t;
typedef long long off64_t;
#define O_DIRECT 00040000
#endif

// FreeBSD compatibility
#ifdef __FreeBSD__
typedef off_t loff_t;
typedef off_t off64_t;
#endif

#if defined(__sun) || defined(_AIX)
typedef off_t loff_t;
#endif


// -- io helpers --

// Forward declare all the I/O helpers so strict ADL can find them in
// the case of containers of containers. I'm tempted to abstract this
// stuff using template templates like I did for denc.

namespace std {
template<class A, class B>
inline std::ostream& operator<<(std::ostream&out, const std::pair<A,B>& v);
template<class A, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::vector<A,Alloc>& v);
template<class A, std::size_t N, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const boost::container::small_vector<A,N,Alloc>& v);
template<class A, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::deque<A,Alloc>& v);
template<typename... Ts>
inline std::ostream& operator<<(std::ostream& out, const std::tuple<Ts...> &t);
template<typename T>
inline std::ostream& operator<<(std::ostream& out, const std::optional<T> &t);
template<class A, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::list<A,Alloc>& ilist);
template<class A, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::set<A, Comp, Alloc>& iset);
template<class A, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::multiset<A,Comp,Alloc>& iset);
template<class A, class B, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::map<A,B,Comp,Alloc>& m);
template<class A, class B, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::multimap<A,B,Comp,Alloc>& m);
}

namespace boost {
template<typename... Ts>
inline std::ostream& operator<<(std::ostream& out, const boost::tuple<Ts...> &t);

namespace container {
template<class A, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const boost::container::flat_set<A, Comp, Alloc>& iset);
template<class A, class B, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const boost::container::flat_map<A, B, Comp, Alloc>& iset);
}
}

namespace std {
template<class A, class B>
inline std::ostream& operator<<(std::ostream& out, const std::pair<A,B>& v) {
  return out << v.first << "," << v.second;
}

template<class A, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::vector<A,Alloc>& v) {
  bool first = true;
  out << "[";
  for (const auto& p : v) {
    if (!first) out << ",";
    out << p;
    first = false;
  }
  out << "]";
  return out;
}

template<class A, std::size_t N, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const boost::container::small_vector<A,N,Alloc>& v) {
  bool first = true;
  out << "[";
  for (const auto& p : v) {
    if (!first) out << ",";
    out << p;
    first = false;
  }
  out << "]";
  return out;
}

template<class A, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::deque<A,Alloc>& v) {
  out << "<";
  for (auto p = v.begin(); p != v.end(); ++p) {
    if (p != v.begin()) out << ",";
    out << *p;
  }
  out << ">";
  return out;
}

template<typename... Ts>
inline std::ostream& operator<<(std::ostream& out, const std::tuple<Ts...> &t) {
  auto f = [n = sizeof...(Ts), i = 0U, &out](const auto& e) mutable {
    out << e;
    if (++i != n)
      out << ",";
  };
  ceph::for_each(t, f);
  return out;
}

// Mimics boost::optional
template<typename T>
inline std::ostream& operator<<(std::ostream& out, const std::optional<T> &t) {
  if (!t)
    out << "--" ;
  else
    out << ' ' << *t ;
  return out;
}

template<class A, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::list<A,Alloc>& ilist) {
  for (auto it = ilist.begin();
       it != ilist.end();
       ++it) {
    if (it != ilist.begin()) out << ",";
    out << *it;
  }
  return out;
}

template<class A, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::set<A, Comp, Alloc>& iset) {
  for (auto it = iset.begin();
       it != iset.end();
       ++it) {
    if (it != iset.begin()) out << ",";
    out << *it;
  }
  return out;
}

template<class A, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::multiset<A,Comp,Alloc>& iset) {
  for (auto it = iset.begin();
       it != iset.end();
       ++it) {
    if (it != iset.begin()) out << ",";
    out << *it;
  }
  return out;
}

template<class A, class B, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::map<A,B,Comp,Alloc>& m)
{
  out << "{";
  for (auto it = m.begin();
       it != m.end();
       ++it) {
    if (it != m.begin()) out << ",";
    out << it->first << "=" << it->second;
  }
  out << "}";
  return out;
}

template<class A, class B, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::multimap<A,B,Comp,Alloc>& m)
{
  out << "{{";
  for (auto it = m.begin();
       it != m.end();
       ++it) {
    if (it != m.begin()) out << ",";
    out << it->first << "=" << it->second;
  }
  out << "}}";
  return out;
}

} // namespace std

namespace boost {
namespace tuples {
template<typename A, typename B, typename C>
inline std::ostream& operator<<(std::ostream& out, const boost::tuples::tuple<A, B, C> &t) {
  return out << boost::get<0>(t) << ","
	     << boost::get<1>(t) << ","
	     << boost::get<2>(t);
}
}
namespace container {
template<class A, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const boost::container::flat_set<A, Comp, Alloc>& iset) {
  for (auto it = iset.begin();
       it != iset.end();
       ++it) {
    if (it != iset.begin()) out << ",";
    out << *it;
  }
  return out;
}

template<class A, class B, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const boost::container::flat_map<A, B, Comp, Alloc>& m) {
  for (auto it = m.begin();
       it != m.end();
       ++it) {
    if (it != m.begin()) out << ",";
    out << it->first << "=" << it->second;
  }
  return out;
}
}
} // namespace boost



/*
 * comparators for stl containers
 */
// for ceph::unordered_map:
//   ceph::unordered_map<const char*, long, hash<const char*>, eqstr> vals;
struct eqstr
{
  bool operator()(const char* s1, const char* s2) const
  {
    return strcmp(s1, s2) == 0;
  }
};

// for set, map
struct ltstr
{
  bool operator()(const char* s1, const char* s2) const
  {
    return strcmp(s1, s2) < 0;
  }
};


namespace ceph {
  class Formatter;
}

#include "encoding.h"

WRITE_RAW_ENCODER(ceph_fsid)
WRITE_RAW_ENCODER(ceph_file_layout)
WRITE_RAW_ENCODER(ceph_dir_layout)
WRITE_RAW_ENCODER(ceph_mds_session_head)
WRITE_RAW_ENCODER(ceph_mds_request_head_legacy)
WRITE_RAW_ENCODER(ceph_mds_request_release)
WRITE_RAW_ENCODER(ceph_filelock)
WRITE_RAW_ENCODER(ceph_mds_caps_head)
WRITE_RAW_ENCODER(ceph_mds_caps_export_body)
WRITE_RAW_ENCODER(ceph_mds_caps_non_export_body)
WRITE_RAW_ENCODER(ceph_mds_cap_peer)
WRITE_RAW_ENCODER(ceph_mds_cap_release)
WRITE_RAW_ENCODER(ceph_mds_cap_item)
WRITE_RAW_ENCODER(ceph_mds_lease)
WRITE_RAW_ENCODER(ceph_mds_snap_head)
WRITE_RAW_ENCODER(ceph_mds_snap_realm)
WRITE_RAW_ENCODER(ceph_mds_reply_head)
WRITE_RAW_ENCODER(ceph_mds_reply_cap)
WRITE_RAW_ENCODER(ceph_mds_cap_reconnect)
WRITE_RAW_ENCODER(ceph_mds_snaprealm_reconnect)
WRITE_RAW_ENCODER(ceph_frag_tree_split)
WRITE_RAW_ENCODER(ceph_osd_reply_head)
WRITE_RAW_ENCODER(ceph_osd_op)
WRITE_RAW_ENCODER(ceph_msg_header)
WRITE_RAW_ENCODER(ceph_msg_footer)
WRITE_RAW_ENCODER(ceph_msg_footer_old)
WRITE_RAW_ENCODER(ceph_mon_subscribe_item)

WRITE_RAW_ENCODER(ceph_mon_statfs)
WRITE_RAW_ENCODER(ceph_mon_statfs_reply)

// ----------------------
// some basic types

// NOTE: these must match ceph_fs.h typedefs
typedef uint64_t ceph_tid_t; // transaction id
typedef uint64_t version_t;
typedef __u32 epoch_t;       // map epoch  (32bits -> 13 epochs/second for 10 years)

// --------------------------------------
// identify individual mount clients by 64bit value

struct client_t {
  int64_t v;

  // cppcheck-suppress noExplicitConstructor
  client_t(int64_t _v = -2) : v(_v) {}

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    encode(v, bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    decode(v, bl);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_int("id", v);
  }
  static void generate_test_instances(std::list<client_t*>& ls) {
    ls.push_back(new client_t);
    ls.push_back(new client_t(1));
    ls.push_back(new client_t(123));
  }
};
WRITE_CLASS_ENCODER(client_t)

static inline bool operator==(const client_t& l, const client_t& r) { return l.v == r.v; }
static inline bool operator!=(const client_t& l, const client_t& r) { return l.v != r.v; }
static inline bool operator<(const client_t& l, const client_t& r) { return l.v < r.v; }
static inline bool operator<=(const client_t& l, const client_t& r) { return l.v <= r.v; }
static inline bool operator>(const client_t& l, const client_t& r) { return l.v > r.v; }
static inline bool operator>=(const client_t& l, const client_t& r) { return l.v >= r.v; }

static inline bool operator>=(const client_t& l, int64_t o) { return l.v >= o; }
static inline bool operator<(const client_t& l, int64_t o) { return l.v < o; }

inline std::ostream& operator<<(std::ostream& out, const client_t& c) {
  return out << c.v;
}



// --

namespace {
inline std::ostream& format_u(std::ostream& out, const uint64_t v, const uint64_t n,
      const int index, const uint64_t mult, const char* u)
  {
    char buffer[32];

    if (index == 0) {
      (void) snprintf(buffer, sizeof(buffer), "%" PRId64 "%s", n, u);
    } else if ((v % mult) == 0) {
      // If this is an even multiple of the base, always display
      // without any decimal fraction.
      (void) snprintf(buffer, sizeof(buffer), "%" PRId64 "%s", n, u);
    } else {
      // We want to choose a precision that reflects the best choice
      // for fitting in 5 characters.  This can get rather tricky when
      // we have numbers that are very close to an order of magnitude.
      // For example, when displaying 10239 (which is really 9.999K),
      // we want only a single place of precision for 10.0K.  We could
      // develop some complex heuristics for this, but it's much
      // easier just to try each combination in turn.
      int i;
      for (i = 2; i >= 0; i--) {
        if (snprintf(buffer, sizeof(buffer), "%.*f%s", i,
          static_cast<double>(v) / mult, u) <= 7)
          break;
      }
    }

    return out << buffer;
  }
}

/*
 * Use this struct to pretty print values that should be formatted with a
 * decimal unit prefix (the classic SI units). No actual unit will be added.
 */
struct si_u_t {
  uint64_t v;
  explicit si_u_t(uint64_t _v) : v(_v) {};
};

inline std::ostream& operator<<(std::ostream& out, const si_u_t& b)
{
  uint64_t n = b.v;
  int index = 0;
  uint64_t mult = 1;
  const char* u[] = {"", "k", "M", "G", "T", "P", "E"};

  while (n >= 1000 && index < 7) {
    n /= 1000;
    index++;
    mult *= 1000;
  }

  return format_u(out, b.v, n, index, mult, u[index]);
}

/*
 * Use this struct to pretty print values that should be formatted with a
 * binary unit prefix (IEC units). Since binary unit prefixes are to be used for
 * "multiples of units in data processing, data transmission, and digital
 * information" (so bits and bytes) and so far bits are not printed, the unit
 * "B" for "byte" is added besides the multiplier.
 */
struct byte_u_t {
  uint64_t v;
  explicit byte_u_t(uint64_t _v) : v(_v) {};
};

inline std::ostream& operator<<(std::ostream& out, const byte_u_t& b)
{
  uint64_t n = b.v;
  int index = 0;
  const char* u[] = {" B", " KiB", " MiB", " GiB", " TiB", " PiB", " EiB"};

  while (n >= 1024 && index < 7) {
    n /= 1024;
    index++;
  }

  return format_u(out, b.v, n, index, 1ULL << (10 * index), u[index]);
}

inline std::ostream& operator<<(std::ostream& out, const ceph_mon_subscribe_item& i)
{
  return out << (long)i.start
	     << ((i.flags & CEPH_SUBSCRIBE_ONETIME) ? "" : "+");
}

struct weightf_t {
  float v;
  // cppcheck-suppress noExplicitConstructor
  weightf_t(float _v) : v(_v) {}
};

inline std::ostream& operator<<(std::ostream& out, const weightf_t& w)
{
  if (w.v < -0.01F) {
    return out << "-";
  } else if (w.v < 0.000001F) {
    return out << "0";
  } else {
    std::streamsize p = out.precision();
    return out << std::fixed << std::setprecision(5) << w.v << std::setprecision(p);
  }
}

struct shard_id_t {
  int8_t id;

  shard_id_t() : id(0) {}
  constexpr explicit shard_id_t(int8_t _id) : id(_id) {}

  operator int8_t() const { return id; }

  const static shard_id_t NO_SHARD;

  void encode(ceph::buffer::list &bl) const {
    using ceph::encode;
    encode(id, bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    using ceph::decode;
    decode(id, bl);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_int("id", id);
  }
  static void generate_test_instances(std::list<shard_id_t*>& ls) {
    ls.push_back(new shard_id_t(1));
    ls.push_back(new shard_id_t(2));
  }
  bool operator==(const shard_id_t&) const = default;
  auto operator<=>(const shard_id_t&) const = default;
};
WRITE_CLASS_ENCODER(shard_id_t)
std::ostream &operator<<(std::ostream &lhs, const shard_id_t &rhs);

#if defined(__sun) || defined(_AIX) || defined(__APPLE__) || \
    defined(__FreeBSD__) || defined(_WIN32)
extern "C" {
__s32  ceph_to_hostos_errno(__s32 e);
__s32  hostos_to_ceph_errno(__s32 e);
}
#else
#define  ceph_to_hostos_errno(e) (e)
#define  hostos_to_ceph_errno(e) (e)
#endif

struct errorcode32_t {
  int32_t code;

  errorcode32_t() : code(0) {}
  // cppcheck-suppress noExplicitConstructor
  explicit errorcode32_t(int32_t i) : code(i) {}

  operator int() const  { return code; }
  int* operator&()      { return &code; }
  errorcode32_t& operator=(int32_t i) {
    code = i;
    return *this;
  }
  bool operator==(const errorcode32_t&) const = default;
  auto operator<=>(const errorcode32_t&) const = default;

  void encode(ceph::buffer::list &bl) const {
    using ceph::encode;
    __s32 newcode = hostos_to_ceph_errno(code);
    encode(newcode, bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    using ceph::decode;
    decode(code, bl);
    code = ceph_to_hostos_errno(code);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_int("code", code);
  }
  static void generate_test_instances(std::list<errorcode32_t*>& ls) {
    ls.push_back(new errorcode32_t(1));
    ls.push_back(new errorcode32_t(2));
  }
};
WRITE_CLASS_ENCODER(errorcode32_t)

template <uint8_t S>
struct sha_digest_t {
  constexpr static uint32_t SIZE = S;
  // TODO: we might consider std::array in the future. Avoiding it for now
  // as sha_digest_t is a part of our public API.
  unsigned char v[S] = {0};

  std::string to_str() const {
    char str[S * 2 + 1] = {0};
    str[0] = '\0';
    for (size_t i = 0; i < S; i++) {
      ::sprintf(&str[i * 2], "%02x", static_cast<int>(v[i]));
    }
    return std::string(str);
  }
  sha_digest_t(const unsigned char *_v) { memcpy(v, _v, SIZE); };
  sha_digest_t() {}

  bool operator==(const sha_digest_t& r) const {
    return ::memcmp(v, r.v, SIZE) == 0;
  }
  bool operator!=(const sha_digest_t& r) const {
    return ::memcmp(v, r.v, SIZE) != 0;
  }

  void encode(ceph::buffer::list &bl) const {
    // copy to avoid reinterpret_cast, is_pod and other nasty things
    using ceph::encode;
    std::array<unsigned char, SIZE> tmparr;
    memcpy(tmparr.data(), v, SIZE);
    encode(tmparr, bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    using ceph::decode;
    std::array<unsigned char, SIZE> tmparr;
    decode(tmparr, bl);
    memcpy(v, tmparr.data(), SIZE);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_string("sha1", to_str());
  }
  static void generate_test_instances(std::list<sha_digest_t*>& ls) {
    ls.push_back(new sha_digest_t);
    ls.push_back(new sha_digest_t);
    ls.back()->v[0] = 1;
    ls.push_back(new sha_digest_t);
    ls.back()->v[0] = 2;
  }
};

template<uint8_t S>
inline std::ostream &operator<<(std::ostream &out, const sha_digest_t<S> &b) {
  std::string str = b.to_str();
  return out << str;
}

#if FMT_VERSION >= 90000
template <uint8_t S> struct fmt::formatter<sha_digest_t<S>> : fmt::ostream_formatter {};
#endif

using sha1_digest_t = sha_digest_t<20>;
WRITE_CLASS_ENCODER(sha1_digest_t)

using sha256_digest_t = sha_digest_t<32>;
WRITE_CLASS_ENCODER(sha256_digest_t)

using sha512_digest_t = sha_digest_t<64>;

using md5_digest_t = sha_digest_t<16>;
WRITE_CLASS_ENCODER(md5_digest_t)


#endif
