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

#ifndef CEPH_SNAPID_H
#define CEPH_SNAPID_H

#include <cstdint>
#include <iosfwd>

#include <fmt/format.h>

#include "encoding.h"

struct snapid_t {
  uint64_t val;
  // cppcheck-suppress noExplicitConstructor
  constexpr snapid_t(uint64_t v=0) : val(v) {}
  snapid_t operator+=(snapid_t o) { val += o.val; return *this; }
  snapid_t operator++() { ++val; return *this; }
  constexpr operator uint64_t() const { return val; }
};

inline void encode(snapid_t i, ceph::buffer::list &bl) {
  using ceph::encode;
  encode(i.val, bl);
}
inline void decode(snapid_t &i, ceph::buffer::list::const_iterator &p) {
  using ceph::decode;
  decode(i.val, p);
}

template<>
struct denc_traits<snapid_t> {
  static constexpr bool supported = true;
  static constexpr bool featured = false;
  static constexpr bool bounded = true;
  static constexpr bool need_contiguous = true;
  static void bound_encode(const snapid_t& o, size_t& p) {
    denc(o.val, p);
  }
  static void encode(const snapid_t &o, ceph::buffer::list::contiguous_appender& p) {
    denc(o.val, p);
  }
  static void decode(snapid_t& o, ceph::buffer::ptr::const_iterator &p) {
    denc(o.val, p);
  }
};

std::ostream& operator<<(std::ostream& out, const snapid_t& s);

namespace fmt {
template <>
struct formatter<snapid_t> {

  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  auto format(const snapid_t& snp, format_context& ctx) const -> format_context::iterator;
};
} // namespace fmt

#endif
