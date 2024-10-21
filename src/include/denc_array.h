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

#include <array>

#include "denc.h"

template<typename T, size_t N>
struct denc_traits<
  std::array<T, N>,
  std::enable_if_t<denc_traits<T>::supported>> {
private:
  using container = std::array<T, N>;
public:
  using traits = denc_traits<T>;

  static constexpr bool supported = true;
  static constexpr bool featured = traits::featured;
  static constexpr bool bounded = traits::bounded;
  static constexpr bool need_contiguous = traits::need_contiguous;

  static void bound_encode(const container& s, size_t& p, uint64_t f = 0) {
    if constexpr (traits::bounded) {
      if constexpr (traits::featured) {
        if (!s.empty()) {
          size_t elem_size = 0;
          denc(*s.begin(), elem_size, f);
          p += elem_size * s.size();
        }
      } else {
        size_t elem_size = 0;
        denc(*s.begin(), elem_size);
        p += elem_size * N;
      }
    } else {
      for (const auto& e : s) {
        if constexpr (traits::featured) {
          denc(e, p, f);
        } else {
          denc(e, p);
        }
      }
    }
  }

  static void encode(const container& s, ceph::buffer::list::contiguous_appender& p,
		     uint64_t f = 0) {
    for (const auto& e : s) {
      if constexpr (traits::featured) {
        denc(e, p, f);
      } else {
        denc(e, p);
      }
    }
  }
  static void decode(container& s, ceph::buffer::ptr::const_iterator& p,
		     uint64_t f = 0) {
    for (auto& e : s)
      denc(e, p, f);
  }
  template<typename U=T>
  static std::enable_if_t<!!sizeof(U) &&
			  !need_contiguous>
  decode(container& s, ceph::buffer::list::const_iterator& p) {
    for (auto& e : s) {
      denc(e, p);
    }
  }
};
