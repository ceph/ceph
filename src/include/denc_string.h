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

#include <string>

#include "denc.h"

template<typename A>
struct denc_traits<std::basic_string<char,std::char_traits<char>,A>> {
private:
  using value_type = std::basic_string<char,std::char_traits<char>,A>;

public:
  static constexpr bool supported = true;
  static constexpr bool featured = false;
  static constexpr bool bounded = false;
  static constexpr bool need_contiguous = false;

  static void bound_encode(const value_type& s, size_t& p, uint64_t f=0) {
    p += sizeof(uint32_t) + s.size();
  }
  template<class It>
  static void encode(const value_type& s,
		     It& p,
                     uint64_t f=0) {
    denc((uint32_t)s.size(), p);
    memcpy(p.get_pos_add(s.size()), s.data(), s.size());
  }
  template<class It>
  static void decode(value_type& s,
		     It& p,
		     uint64_t f=0) {
    uint32_t len;
    denc(len, p);
    decode_nohead(len, s, p);
  }
  static void decode(value_type& s, ceph::buffer::list::const_iterator& p)
  {
    uint32_t len;
    denc(len, p);
    decode_nohead(len, s, p);
  }
  template<class It>
  static void decode_nohead(size_t len, value_type& s, It& p) {
    s.clear();
    if (len) {
      s.append(p.get_pos_add(len), len);
    }
  }
  static void decode_nohead(size_t len, value_type& s,
                            ceph::buffer::list::const_iterator& p) {
    if (len) {
      if constexpr (std::is_same_v<value_type, std::string>) {
        s.clear();
        p.copy(len, s);
      } else {
        s.resize(len);
        p.copy(len, s.data());
      }
    } else {
      s.clear();
    }
  }
  template<class It>
  requires (!is_const_iterator<It>)
  static void
  encode_nohead(const value_type& s, It& p) {
    auto len = s.length();
    maybe_inline_memcpy(p.get_pos_add(len), s.data(), len, 16);
  }
};
