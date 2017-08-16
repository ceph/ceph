// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc. <contact@redhat.com>
 *
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version
 * 2.1, as published by the Free Software Foundation.  See file
 * COPYING.
 */

#ifndef COMMON_CEPH_ERROR_CODE
#define COMMON_CEPH_ERROR_CODE

#include <netdb.h>

#include <boost/system/error_code.hpp>
#include <boost/asio.hpp>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wnon-virtual-dtor"

namespace ceph {

// This is for error categories we define, so we can specify the
// equivalent integral value at the point of definition.
class converting_category : public boost::system::error_category {
public:
  virtual int from_code(int code) const noexcept = 0;
};

const boost::system::error_category& ceph_category() noexcept;

enum class errc {
  not_in_map = 1, // The requested item was not found in the map
  does_not_exist, // Item does not exist
  failure, // An internal fault or inconsistency
  exists, // Already exists
  limit_exceeded, // Attempting to use too much of something
  auth, // May not be an auth failure. It could be that the
	// preconditions to attempt auth failed.
  conflict, // Conflict or precondition failure
};
}

namespace boost::system {
template<>
struct is_error_condition_enum<::ceph::errc> {
  static const bool value = true;
};
template<>
struct is_error_code_enum<::ceph::errc> {
  static const bool value = false;
};
}

namespace ceph {
//  explicit conversion:
inline boost::system::error_code make_error_code(errc e) noexcept {
  return { static_cast<int>(e), ceph_category() };
}

// implicit conversion:
inline boost::system::error_condition make_error_condition(errc e) noexcept {
  return { static_cast<int>(e), ceph_category() };
}

boost::system::error_code to_error_code(int ret) noexcept;
int from_error_code(boost::system::error_code e) noexcept;
}
#pragma GCC diagnostic pop
#pragma clang diagnostic pop

#endif // COMMON_CEPH_ERROR_CODE
