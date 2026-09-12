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
  std::enable_if_t<denc_traits<T>::supported>>
  : public _denc::optional_base<std::optional<T>, T> {};

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
