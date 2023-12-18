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

#include "include/err.h"

const boost::system::error_category& osd_category() noexcept;

// Since the OSD mostly uses POSIX error codes plus a couple
// additions, this will be a degenerate error category for now that
// mostly forwards to POSIX.

enum class osd_errc {
  old_snapc = 85,  /* ORDERSNAP flag set; writer has old snapc*/
  blocklisted = 108, /* blocklisted */
  cmpext_mismatch = MAX_ERRNO /* cmpext failed */
};

namespace boost::system {
template<>
struct is_error_code_enum<::osd_errc> {
  static const bool value = true;
};

template<>
struct is_error_condition_enum<::osd_errc> {
  static const bool value = false;
};
}

//  implicit conversion:
inline boost::system::error_code make_error_code(osd_errc e) noexcept {
  return { static_cast<int>(e), osd_category() };
}

// explicit conversion:
inline boost::system::error_condition make_error_condition(osd_errc e) noexcept {
  return { static_cast<int>(e), osd_category() };
}
