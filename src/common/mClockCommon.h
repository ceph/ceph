// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 SK Telecom
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#pragma once

#include "dmclock/src/dmclock_recs.h"

// the following is done to unclobber _ASSERT_H so it returns to the
// way ceph likes it
#include "include/ceph_assert.h"
#include "include/denc.h"


namespace dmc = crimson::dmclock;

template<>
struct denc_traits<dmc::ReqParams> {
  static constexpr bool supported = true;
  static constexpr bool featured = false;
  static constexpr bool bounded = true;
  static constexpr bool need_contiguous = true;

  static void bound_encode(
    const dmc::ReqParams& rp,
    size_t& p) {
    denc(rp.delta, p);
    denc(rp.rho, p);
  }
  static void encode(
    const dmc::ReqParams& rp,
    ceph::buffer::list::contiguous_appender& p) {
    denc(rp.delta, p);
    denc(rp.rho, p);
  }
  static void decode(
    dmc::ReqParams& rp,
    ceph::buffer::ptr::const_iterator& p) {
    denc(rp.delta, p);
    denc(rp.rho, p);
  }
};

template<>
struct denc_traits<dmc::PhaseType> {
  static constexpr bool supported = true;
  static constexpr bool featured = false;
  static constexpr bool bounded = true;
  static constexpr bool need_contiguous = false;

  static void bound_encode(
    const dmc::PhaseType& t,
    size_t& p,
    uint64_t f=0) {
    p += sizeof(dmc::PhaseType);
  }
  template<class It>
  requires (!is_const_iterator<It>)
  static void encode(
    const dmc::PhaseType& t,
    It& p,
    uint64_t f=0) {
    get_pos_add<dmc::PhaseType>(p) = t;
  }
  template<is_const_iterator It>
  static void decode(
    dmc::PhaseType& t,
    It& p,
    uint64_t f=0) {
    t = get_pos_add<dmc::PhaseType>(p);
  }
  static void decode(
    dmc::PhaseType& t,
    ceph::buffer::list::const_iterator& p) {
    p.copy(sizeof(dmc::PhaseType), reinterpret_cast<char*>(&t));
  }
};
