// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <boost/system/error_code.hpp>

#include "include/rados.h"

const boost::system::error_category& osdc_category() noexcept;

enum class osdc_errc {
  pool_dne = 1,
  pool_exists,
  // Come the revolution, we'll just kill your program. Maybe.
  precondition_violated,
  not_supported,
  snapshot_exists,
  snapshot_dne,
  timed_out,
  pool_eio,
  handler_failed
};

namespace boost::system {
template<>
struct is_error_code_enum<::osdc_errc> {
  static const bool value = true;
};

template<>
struct is_error_condition_enum<::osdc_errc> {
  static const bool value = false;
};
}

//  implicit conversion:
inline boost::system::error_code make_error_code(osdc_errc e) noexcept {
  return { static_cast<int>(e), osdc_category() };
}

// explicit conversion:
inline boost::system::error_condition make_error_condition(osdc_errc e) noexcept {
  return { static_cast<int>(e), osdc_category() };
}
