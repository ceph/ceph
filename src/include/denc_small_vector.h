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

#include <boost/container/small_vector.hpp>

#include "denc.h"

template<typename T, std::size_t N, typename ...Ts>
struct denc_traits<
  boost::container::small_vector<T, N, Ts...>,
  typename std::enable_if_t<denc_traits<T>::supported>> {
private:
  using container = boost::container::small_vector<T, N, Ts...>;
public:
  using traits = denc_traits<T>;

  static constexpr bool supported = true;
  static constexpr bool featured = traits::featured;
  static constexpr bool bounded = false;
  static constexpr bool need_contiguous = traits::need_contiguous;

  template<typename U=T>
  static void bound_encode(const container& s, size_t& p, uint64_t f = 0) {
    p += sizeof(uint32_t);
    if constexpr (traits::bounded) {
      if (!s.empty()) {
	const auto elem_num = s.size();
	size_t elem_size = 0;
	if constexpr (traits::featured) {
	  denc(*s.begin(), elem_size, f);
        } else {
          denc(*s.begin(), elem_size);
        }
        p += elem_size * elem_num;
      }
    } else {
      for (const T& e : s) {
	if constexpr (traits::featured) {
	  denc(e, p, f);
	} else {
	  denc(e, p);
	}
      }
    }
  }

  template<typename U=T>
  static void encode(const container& s,
		     ceph::buffer::list::contiguous_appender& p,
		     uint64_t f = 0) {
    denc((uint32_t)s.size(), p);
    if constexpr (traits::featured) {
      encode_nohead(s, p, f);
    } else {
      encode_nohead(s, p);
    }
  }
  static void decode(container& s, ceph::buffer::ptr::const_iterator& p,
		     uint64_t f = 0) {
    uint32_t num;
    denc(num, p);
    decode_nohead(num, s, p, f);
  }
  template<typename U=T>
  static std::enable_if_t<!!sizeof(U) && !need_contiguous>
  decode(container& s, ceph::buffer::list::const_iterator& p) {
    uint32_t num;
    denc(num, p);
    decode_nohead(num, s, p);
  }

  // nohead
  static void encode_nohead(const container& s, ceph::buffer::list::contiguous_appender& p,
			    uint64_t f = 0) {
    for (const T& e : s) {
      if constexpr (traits::featured) {
        denc(e, p, f);
      } else {
        denc(e, p);
      }
    }
  }
  static void decode_nohead(size_t num, container& s,
			    ceph::buffer::ptr::const_iterator& p,
			    uint64_t f=0) {
    s.clear();
    s.reserve(num);
    while (num--) {
      T t;
      denc(t, p, f);
      s.push_back(std::move(t));
    }
  }
  template<typename U=T>
  static std::enable_if_t<!!sizeof(U) && !need_contiguous>
  decode_nohead(size_t num, container& s,
		ceph::buffer::list::const_iterator& p) {
    s.clear();
    s.reserve(num);
    while (num--) {
      T t;
      denc(t, p);
      s.push_back(std::move(t));
    }
  }
};
