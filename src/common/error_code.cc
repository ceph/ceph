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

#include <exception>

#include <boost/asio/error.hpp>
#include "common/error_code.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wnon-virtual-dtor"

using boost::system::error_category;
using boost::system::error_condition;
using boost::system::generic_category;
using boost::system::system_category;

namespace ceph {

// A category for error conditions particular to Ceph

class ceph_error_category : public converting_category {
public:
  ceph_error_category(){}
  const char* name() const noexcept override;
  using converting_category::message;
  std::string message(int ev) const override;
  const char* message(int ev, char*, std::size_t) const noexcept override;
  using converting_category::equivalent;
  bool equivalent(const boost::system::error_code& c,
		  int ev) const noexcept override;
  int from_code(int ev) const noexcept override;
};

const char* ceph_error_category::name() const noexcept {
  return "ceph";
}

const char* ceph_error_category::message(int ev, char*,
					 std::size_t) const noexcept {
  if (ev == 0)
    return "No error";

  switch (static_cast<errc>(ev)) {

  case errc::not_in_map:
    return "Map does not contain requested entry.";
  case errc::does_not_exist:
    return "Item does not exist";
  case errc::failure:
    return "An internal fault or inconsistency occurred";
  case errc::exists:
    return "Already exists";
  case errc::limit_exceeded:
    return "Attempt to use too much";
  case errc::auth:
    return "Authentication error";
  case errc::conflict:
    return "Conflict detected or precondition failed";
  }

  return "Unknown error.";
}

std::string ceph_error_category::message(int ev) const {
  return message(ev, nullptr, 0);
}

bool ceph_error_category::equivalent(const boost::system::error_code& c,
				     int ev) const noexcept {
  if (c.category() == system_category()) {
    if (c.value() == boost::system::errc::no_such_file_or_directory) {
      if (ev == static_cast<int>(errc::not_in_map) ||
	  ev == static_cast<int>(errc::does_not_exist)) {
	// Blargh. A bunch of stuff returns ENOENT now, so just to be safe.
	return true;
      }
    }
    if (c.value() == boost::system::errc::io_error) {
      if (ev == static_cast<int>(errc::failure)) {
	return true;
      }
    }
    if (c.value() == boost::system::errc::file_exists) {
      if (ev == static_cast<int>(errc::exists)) {
	return true;
      }
    }
    if (c.value() == boost::system::errc::no_space_on_device ||
	c.value() == boost::system::errc::invalid_argument) {
      if (ev == static_cast<int>(errc::limit_exceeded)) {
	return true;
      }
    }
    if (c.value() == boost::system::errc::operation_not_permitted) {
      if (ev == static_cast<int>(ceph::errc::conflict)) {
	return true;
      }
    }
  }
  return false;
}

int ceph_error_category::from_code(int ev) const noexcept {
  if (ev == 0)
    return 0;

  switch (static_cast<errc>(ev)) {
  case errc::not_in_map:
  case errc::does_not_exist:
    // What we use now.
    return -ENOENT;
  case errc::failure:
    return -EIO;
  case errc::exists:
    return -EEXIST;
  case errc::limit_exceeded:
    return -EIO;
  case errc::auth:
    return -EACCES;
  case errc::conflict:
    return -EINVAL;
  }
  return -EDOM;
}

const error_category& ceph_category() noexcept {
  static const ceph_error_category c;
  return c;
}


// This is part of the glue for hooking new code to old. Since
// Context* and other things give us integer codes from errno, wrap
// them in an error_code.
[[nodiscard]] boost::system::error_code to_error_code(int ret) noexcept
{
  if (ret == 0)
    return {};
  return { std::abs(ret), boost::system::system_category() };
}

// This is more complicated. For the case of categories defined
// elsewhere, we have to convert everything here.
[[nodiscard]] int from_error_code(boost::system::error_code e) noexcept
{
  if (!e)
    return 0;

  auto c = dynamic_cast<const converting_category*>(&e.category());
  // For categories we define
  if (c)
    return c->from_code(e.value());

  // For categories matching values of errno
  if (e.category() == boost::system::system_category() ||
      e.category() == boost::system::generic_category() ||
      // ASIO uses the system category for these and matches system
      // error values.
      e.category() == boost::asio::error::get_netdb_category() ||
      e.category() == boost::asio::error::get_addrinfo_category())
    return -e.value();

  if (e.category() == boost::asio::error::get_misc_category()) {
    // These values are specific to asio
    switch (e.value()) {
    case boost::asio::error::already_open:
      return -EIO;
    case boost::asio::error::eof:
      return -EIO;
    case boost::asio::error::not_found:
      return -ENOENT;
    case boost::asio::error::fd_set_failure:
      return -EINVAL;
    }
  }
  // Add any other categories we use here.

  // Marcus likes this as a sentinel for 'Error code? What error code?'
  return -EDOM;
}
}
#pragma GCC diagnostic pop
#pragma clang diagnostic pop
