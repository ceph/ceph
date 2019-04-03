// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <ostream>
#include <string>

#include "include/encoding.h"

// health_status_t
enum health_status_t {
  HEALTH_ERR = 0,
  HEALTH_WARN = 1,
  HEALTH_OK = 2,
};

inline void encode(health_status_t hs, ceph::buffer::list& bl) {
  using ceph::encode;
  uint8_t v = hs;
  encode(v, bl);
}
inline void decode(health_status_t& hs, ceph::buffer::list::const_iterator& p) {
  using ceph::decode;
  uint8_t v;
  decode(v, p);
  hs = health_status_t(v);
}
template<>
struct denc_traits<health_status_t> {
  static constexpr bool supported = true;
  static constexpr bool featured = false;
  static constexpr bool bounded = true;
  static constexpr bool need_contiguous = false;
  static void bound_encode(const ceph::buffer::ptr& v, size_t& p, uint64_t f=0) {
    p++;
  }
  static void encode(const health_status_t& v,
		     ceph::buffer::list::contiguous_appender& p,
		     uint64_t f=0) {
    ::denc((uint8_t)v, p);
  }
  static void decode(health_status_t& v, ceph::buffer::ptr::const_iterator& p,
		     uint64_t f=0) {
    uint8_t tmp;
    ::denc(tmp, p);
    v = health_status_t(tmp);
  }
  static void decode(health_status_t& v, ceph::buffer::list::const_iterator& p,
		     uint64_t f=0) {
    uint8_t tmp;
    ::denc(tmp, p);
    v = health_status_t(tmp);
  }
};

inline std::ostream& operator<<(std::ostream &oss, const health_status_t status) {
  switch (status) {
    case HEALTH_ERR:
      oss << "HEALTH_ERR";
      break;
    case HEALTH_WARN:
      oss << "HEALTH_WARN";
      break;
    case HEALTH_OK:
      oss << "HEALTH_OK";
      break;
  }
  return oss;
}
