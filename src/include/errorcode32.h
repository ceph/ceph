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
#ifndef CEPH_ERRORCODE32_H
#define CEPH_ERRORCODE32_H

#include "encoding.h"
#include "int_types.h"

namespace ceph {
  class Formatter;
}

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
  using code_t = __s32;
  code_t code;

  errorcode32_t() : code(0) {}
  // cppcheck-suppress noExplicitConstructor
  explicit errorcode32_t(code_t i) : code(i) {}

  operator code_t() const  { return code; }
  code_t* operator&()      { return &code; }
  errorcode32_t& operator=(code_t i) {
    code = i;
    return *this;
  }
  bool operator==(const errorcode32_t&) const = default;
  auto operator<=>(const errorcode32_t&) const = default;

  inline code_t get_host_to_wire() const {
    return hostos_to_ceph_errno(code);
  }

  inline void set_wire_to_host(code_t host_code) {
    code = ceph_to_hostos_errno(host_code);
  }

  void encode(ceph::buffer::list &bl) const {
    using ceph::encode;
    auto new_code = get_host_to_wire();
    encode(new_code, bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    using ceph::decode;
    code_t newcode;
    decode(newcode, bl);
    set_wire_to_host(newcode);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<errorcode32_t*>& ls);
};
WRITE_CLASS_ENCODER(errorcode32_t)

#endif
