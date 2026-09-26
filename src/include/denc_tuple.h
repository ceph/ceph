// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Allen Samuels <allen.samuels@sandisk.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <tuple>

#include "denc.h"
#include "common/convenience.h" // for ceph::for_each()

template<typename... Ts>
struct denc_traits<
  std::tuple<Ts...>,
  std::enable_if_t<(denc_traits<Ts>::supported && ...)>> {

private:
  static_assert(sizeof...(Ts) > 0,
		"Zero-length tuples are not supported.");
  using container = std::tuple<Ts...>;

public:

  static constexpr bool supported = true;
  static constexpr bool featured = (denc_traits<Ts>::featured || ...);
  static constexpr bool bounded = (denc_traits<Ts>::bounded && ...);
  static constexpr bool need_contiguous =
      (denc_traits<Ts>::need_contiguous || ...);

  template<typename U = container>
  static std::enable_if_t<denc_traits<U>::featured>
  bound_encode(const container& s, size_t& p, uint64_t f) {
    ceph::for_each(s, [&p, f] (const auto& e) {
	if constexpr (denc_traits<std::decay_t<decltype(e)>>::featured) {
	  denc(e, p, f);
	} else {
	  denc(e, p);
	}
      });
  }
  template<typename U = container>
  static std::enable_if_t<!denc_traits<U>::featured>
  bound_encode(const container& s, size_t& p) {
    ceph::for_each(s, [&p] (const auto& e) {
	denc(e, p);
      });
  }

  template<typename U = container>
  static std::enable_if_t<denc_traits<U>::featured>
  encode(const container& s, ceph::buffer::list::contiguous_appender& p,
	 uint64_t f) {
    ceph::for_each(s, [&p, f] (const auto& e) {
	if constexpr (denc_traits<std::decay_t<decltype(e)>>::featured) {
	  denc(e, p, f);
	} else {
	  denc(e, p);
	}
      });
  }
  template<typename U = container>
  static std::enable_if_t<!denc_traits<U>::featured>
  encode(const container& s, ceph::buffer::list::contiguous_appender& p) {
    ceph::for_each(s, [&p] (const auto& e) {
	denc(e, p);
      });
  }

  static void decode(container& s, ceph::buffer::ptr::const_iterator& p,
		     uint64_t f = 0) {
    ceph::for_each(s, [&p] (auto& e) {
	denc(e, p);
      });
  }

  template<typename U = container>
  static std::enable_if_t<!denc_traits<U>::need_contiguous>
  decode(container& s, ceph::buffer::list::const_iterator& p, uint64_t f = 0) {
    ceph::for_each(s, [&p] (auto& e) {
	denc(e, p);
      });
  }
};
