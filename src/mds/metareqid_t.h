// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_METAREQID_T_H
#define CEPH_METAREQID_T_H

#include <iosfwd>
#include <string_view>
#include <stdexcept>

#include "include/encoding.h"
#include "include/types.h" // for ceph_tid_t
#include "msg/msg_types.h" // for entity_name_t

namespace ceph { class Formatter; }

// requests
struct metareqid_t {
  metareqid_t() {}
  metareqid_t(entity_name_t n, ceph_tid_t t) : name(n), tid(t) {}
  metareqid_t(std::string_view sv) {
    auto p = sv.find(':');
    if (p == std::string::npos) {
      throw std::invalid_argument("invalid format: expected colon");
    }
    if (!name.parse(sv.substr(0, p))) {
      throw std::invalid_argument("invalid format: invalid entity name");
    }
    try {
      tid = std::stoul(std::string(sv.substr(p+1)), nullptr, 0);
    } catch (const std::invalid_argument& e) {
      throw std::invalid_argument("invalid format: tid is not a number");
    } catch (const std::out_of_range& e) {
      throw std::invalid_argument("invalid format: tid is out of range");
    }
  }
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator &p);
  void dump(ceph::Formatter *f) const;
  void print(std::ostream& out) const;
  static std::list<metareqid_t> generate_test_instances();
  entity_name_t name;
  uint64_t tid = 0;
};
WRITE_CLASS_ENCODER(metareqid_t)

inline bool operator==(const metareqid_t& l, const metareqid_t& r) {
  return (l.name == r.name) && (l.tid == r.tid);
}
inline bool operator!=(const metareqid_t& l, const metareqid_t& r) {
  return (l.name != r.name) || (l.tid != r.tid);
}
inline bool operator<(const metareqid_t& l, const metareqid_t& r) {
  return (l.name < r.name) || 
    (l.name == r.name && l.tid < r.tid);
}
inline bool operator<=(const metareqid_t& l, const metareqid_t& r) {
  return (l.name < r.name) ||
    (l.name == r.name && l.tid <= r.tid);
}
inline bool operator>(const metareqid_t& l, const metareqid_t& r) { return !(l <= r); }
inline bool operator>=(const metareqid_t& l, const metareqid_t& r) { return !(l < r); }

namespace std {
  template<> struct hash<metareqid_t> {
    size_t operator()(const metareqid_t &r) const { 
      hash<uint64_t> H;
      return H(r.name.num()) ^ H(r.name.type()) ^ H(r.tid);
    }
  };
} // namespace std

#endif
