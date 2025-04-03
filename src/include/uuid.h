// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#ifndef _CEPH_UUID_H
#define _CEPH_UUID_H

/*
 * Thin C++ wrapper around libuuid.
 */

#include "encoding.h"

#include <ostream>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/nil_generator.hpp>
#include <boost/version.hpp>

#include <fmt/core.h> // for FMT_VERSION
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

namespace ceph {
  class Formatter;
}

struct uuid_d {
  boost::uuids::uuid uuid;

  uuid_d() {
    boost::uuids::nil_generator gen;
    uuid = gen();
  }

  bool is_zero() const {
    return uuid.is_nil();
  }

  void generate_random();
  
  bool parse(const char *s);
  void print(char *s) const;

  std::string to_string() const;

  const char *bytes() const {
#if BOOST_VERSION >= 108600
    return (const char*)uuid.data();
#else
    return (const char*)uuid.data;
#endif
  }

  void encode(::ceph::buffer::list::contiguous_appender& p) const {
    p.append(reinterpret_cast<const char *>(&uuid), sizeof(uuid));
  }

  void bound_encode(size_t& p) const {
    p += sizeof(uuid);
  }

  void decode(::ceph::buffer::ptr::const_iterator& p) {
    assert((p.get_end() - p.get_pos()) >= (int)sizeof(*this));
    memcpy((char *)this, p.get_pos_add(sizeof(*this)), sizeof(*this));
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<uuid_d*>& o);
};
WRITE_CLASS_DENC_BOUNDED(uuid_d)

inline std::ostream& operator<<(std::ostream& out, const uuid_d& u) {
  char b[37];
  u.print(b);
  return out << b;
}

inline bool operator==(const uuid_d& l, const uuid_d& r) {
  return l.uuid == r.uuid;
}
inline bool operator!=(const uuid_d& l, const uuid_d& r) {
  return l.uuid != r.uuid;
}
inline bool operator<(const uuid_d& l, const uuid_d& r) {
  return l.to_string() < r.to_string();
}
inline bool operator>(const uuid_d& l, const uuid_d& r) {
  return l.to_string() > r.to_string();
}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<uuid_d> : fmt::ostream_formatter {};
#endif

#endif
