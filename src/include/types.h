// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

#include <fmt/core.h> // for FMT_VERSION
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

// this is needed for ceph_fs to compile in userland
#include "int_types.h"
#include "platform_errno.h"

#include <string.h>

#include "ceph_fs.h"

extern "C" {
#include <stdint.h>
}

#include <deque>
#include <list>
#include <set>
#include <span>
#include <boost/container/flat_set.hpp>
#include <boost/container/flat_map.hpp>
#include "boost/tuple/tuple.hpp"
#include <map>
#include <vector>
#include <optional>
#include <iomanip>
#include <iosfwd>
#include <unordered_map>
#include <unordered_set>

#include "common/convenience.h" // for ceph::for_each()

#include "acconfig.h"

#include <fmt/core.h> // for FMT_VERSION
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

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
template<class T, std::size_t Extent>
inline std::ostream& operator<<(std::ostream& out, const std::span<T, Extent>& s);
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
inline std::ostream& operator<<(std::ostream& out, const std::unordered_set<A, Comp, Alloc>& iset);
template<class A, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::multiset<A,Comp,Alloc>& iset);
template<class A, class B, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::map<A,B,Comp,Alloc>& m);
template<class A, class B, class Hash, class KeyEqual>
inline std::ostream& operator<<(std::ostream& out, const std::unordered_map<A,B,Hash, KeyEqual>& m);
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

template<class T, std::size_t Extent>
inline std::ostream& operator<<(std::ostream& out, const std::span<T, Extent>& s) {
  bool first = true;
  out << "[";
  for (const auto& p : s) {
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
inline std::ostream& operator<<(std::ostream& out, const std::unordered_set<A, Comp, Alloc>& iset) {
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

template <class A, class B, class Hash, class KeyEqual>
inline std::ostream&
operator<<(std::ostream& out,
	   const std::unordered_map<A, B, Hash, KeyEqual>& m)
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
// for std::unordered_map:
//   std::unordered_map<const char*, long, hash<const char*>, eqstr> vals;
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
  void dump(ceph::Formatter *f) const;
  static std::list<client_t> generate_test_instances();
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

std::ostream& operator<<(std::ostream& out, const client_t& c);

// --

/*
 * Use this struct to pretty print values that should be formatted with a
 * decimal unit prefix (the classic SI units). No actual unit will be added.
 */
struct si_u_t {
  uint64_t v;
  explicit si_u_t(uint64_t _v) : v(_v) {};
};

std::ostream& operator<<(std::ostream& out, const si_u_t& b);

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

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<byte_u_t> : fmt::ostream_formatter {};
#endif

std::ostream& operator<<(std::ostream& out, const byte_u_t& b);
std::ostream& operator<<(std::ostream& out, const ceph_mon_subscribe_item& i);

struct weightf_t {
  float v;
  // cppcheck-suppress noExplicitConstructor
  weightf_t(float _v) : v(_v) {}
};

std::ostream& operator<<(std::ostream& out, const weightf_t& w);

struct shard_id_t {
  int8_t id;

  shard_id_t() : id(0) {}
  explicit constexpr shard_id_t(int8_t _id) : id(_id) {}

  explicit constexpr operator int8_t() const { return id; }
  explicit constexpr operator int64_t() const { return id; }
  explicit constexpr operator int() const { return id; }
  explicit constexpr operator unsigned() const { return id; }

  const static shard_id_t NO_SHARD;

  void encode(ceph::buffer::list &bl) const {
    using ceph::encode;
    encode(id, bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    using ceph::decode;
    decode(id, bl);
  }
  void dump(ceph::Formatter *f) const;
  static std::list<shard_id_t> generate_test_instances();
  shard_id_t& operator++() { ++id; return *this; }
  friend constexpr std::strong_ordering operator<=>(const shard_id_t &lhs,
                                                    const shard_id_t &rhs) {
    return lhs.id <=> rhs.id;
  }

  friend constexpr std::strong_ordering operator<=>(int lhs,
                                                    const shard_id_t &rhs) {
    return lhs <=> rhs.id;
  }
  friend constexpr std::strong_ordering operator<=>(const shard_id_t &lhs,
                                                    int rhs) {
    return lhs.id <=> rhs;
  }

  shard_id_t& operator=(int other) { id = other; return *this; }
  bool operator==(const shard_id_t &other) const { return id == other.id; }

  shard_id_t operator+(int other) const { return shard_id_t(id + other); }
  shard_id_t operator-(int other) const { return shard_id_t(id - other); }
};
WRITE_CLASS_ENCODER(shard_id_t)
std::ostream &operator<<(std::ostream &lhs, const shard_id_t &rhs);

#endif
