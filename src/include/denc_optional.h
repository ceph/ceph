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

#include <optional>

#include "denc.h"

template<typename T>
struct denc_traits<
  std::optional<T>,
  std::enable_if_t<denc_traits<T>::supported>> {
  using traits = denc_traits<T>;

  static constexpr bool supported = true;
  static constexpr bool featured = traits::featured;
  static constexpr bool bounded = false;
  static constexpr bool need_contiguous = traits::need_contiguous;

  static void bound_encode(const std::optional<T>& v, size_t& p,
			   uint64_t f = 0) {
    p += sizeof(bool);
    if (v) {
      if constexpr (featured) {
        denc(*v, p, f);
      } else {
        denc(*v, p);
      }
    }
  }

  static void encode(const std::optional<T>& v,
		     ceph::buffer::list::contiguous_appender& p,
		     uint64_t f = 0) {
    denc((bool)v, p);
    if (v) {
      if constexpr (featured) {
        denc(*v, p, f);
      } else {
        denc(*v, p);
      }
    }
  }

  static void decode(std::optional<T>& v, ceph::buffer::ptr::const_iterator& p,
		     uint64_t f = 0) {
    bool x;
    denc(x, p, f);
    if (x) {
      v = T{};
      denc(*v, p, f);
    } else {
      v = std::nullopt;
    }
  }

  template<typename U = T>
  static std::enable_if_t<!!sizeof(U) && !need_contiguous>
  decode(std::optional<T>& v, ceph::buffer::list::const_iterator& p) {
    bool x;
    denc(x, p);
    if (x) {
      v = T{};
      denc(*v, p);
    } else {
      v = std::nullopt;
    }
  }

  static void encode_nohead(const std::optional<T>& v,
			    ceph::buffer::list::contiguous_appender& p,
			    uint64_t f = 0) {
    if (v) {
      if constexpr (featured) {
        denc(*v, p, f);
      } else {
        denc(*v, p);
      }
    }
  }

  static void decode_nohead(bool num, std::optional<T>& v,
			    ceph::buffer::ptr::const_iterator& p, uint64_t f = 0) {
    if (num) {
      v = T();
      denc(*v, p, f);
    } else {
      v = std::nullopt;
    }
  }
};

template<>
struct denc_traits<std::nullopt_t> {
  static constexpr bool supported = true;
  static constexpr bool featured = false;
  static constexpr bool bounded = true;
  static constexpr bool need_contiguous = false;

  static void bound_encode(const std::nullopt_t& v, size_t& p) {
    p += sizeof(bool);
  }

  static void encode(const std::nullopt_t& v,
		     ceph::buffer::list::contiguous_appender& p) {
    denc(false, p);
  }
};
