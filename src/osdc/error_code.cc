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

#include <string>

#include "common/error_code.h"
#include "error_code.h"

namespace bs = boost::system;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wnon-virtual-dtor"
class osdc_error_category : public ceph::converting_category {
public:
  osdc_error_category(){}
  const char* name() const noexcept override;
  const char* message(int ev, char*, std::size_t) const noexcept override;
  std::string message(int ev) const override;
  bs::error_condition default_error_condition(int ev) const noexcept
    override;
  bool equivalent(int ev, const bs::error_condition& c) const
    noexcept override;
  using ceph::converting_category::equivalent;
  int from_code(int ev) const noexcept override;
};
#pragma GCC diagnostic pop
#pragma clang diagnostic pop

const char* osdc_error_category::name() const noexcept {
  return "osdc";
}

const char* osdc_error_category::message(int ev, char*,
					 std::size_t) const noexcept {
  if (ev == 0)
    return "No error";

  switch (static_cast<osdc_errc>(ev)) {
  case osdc_errc::pool_dne:
    return "Pool does not exist";

  case osdc_errc::pool_exists:
    return "Pool already exists";

  case osdc_errc::precondition_violated:
    return "Precondition for operation not satisfied";

  case osdc_errc::not_supported:
    return "Operation not supported";

  case osdc_errc::snapshot_exists:
    return "Snapshot already exists";

  case osdc_errc::snapshot_dne:
    return "Snapshot does not exist";

  case osdc_errc::timed_out:
    return "Operation timed out";

  case osdc_errc::pool_eio:
    return "Pool EIO flag set";

  case osdc_errc::handler_failed:
    return "Handler function threw unknown exception";
  }

  return "Unknown error";
}

std::string osdc_error_category::message(int ev) const {
  return message(ev, nullptr, 0);
}

bs::error_condition
osdc_error_category::default_error_condition(int ev) const noexcept {
  switch (static_cast<osdc_errc>(ev)) {
  case osdc_errc::pool_dne:
    return ceph::errc::does_not_exist;
  case osdc_errc::pool_exists:
    return ceph::errc::exists;
  case osdc_errc::precondition_violated:
    return bs::errc::invalid_argument;
  case osdc_errc::not_supported:
    return bs::errc::operation_not_supported;
  case osdc_errc::snapshot_exists:
    return ceph::errc::exists;
  case osdc_errc::snapshot_dne:
    return ceph::errc::does_not_exist;
  case osdc_errc::timed_out:
    return bs::errc::timed_out;
  case osdc_errc::pool_eio:
    return bs::errc::io_error;
  case osdc_errc::handler_failed:
    return bs::errc::io_error;
  }

  return { ev, *this };
}

bool osdc_error_category::equivalent(int ev,
                                     const bs::error_condition& c) const noexcept {
  if (static_cast<osdc_errc>(ev) == osdc_errc::pool_dne) {
    if (c == bs::errc::no_such_file_or_directory) {
      return true;
    }
    if (c == ceph::errc::not_in_map) {
      return true;
    }
  }
  if (static_cast<osdc_errc>(ev) == osdc_errc::pool_exists) {
    if (c == bs::errc::file_exists) {
      return true;
    }
  }
  if (static_cast<osdc_errc>(ev) == osdc_errc::snapshot_exists) {
    if (c == bs::errc::file_exists) {
      return true;
    }
  }
  if (static_cast<osdc_errc>(ev) == osdc_errc::snapshot_dne) {
    if (c == bs::errc::no_such_file_or_directory) {
      return true;
    }
    if (c == ceph::errc::not_in_map) {
      return true;
    }
  }

  return default_error_condition(ev) == c;
}

int osdc_error_category::from_code(int ev) const noexcept {
  switch (static_cast<osdc_errc>(ev)) {
  case osdc_errc::pool_dne:
    return -ENOENT;
  case osdc_errc::pool_exists:
    return -EEXIST;
  case osdc_errc::precondition_violated:
    return -EINVAL;
  case osdc_errc::not_supported:
    return -EOPNOTSUPP;
  case osdc_errc::snapshot_exists:
    return -EEXIST;
  case osdc_errc::snapshot_dne:
    return -ENOENT;
  case osdc_errc::timed_out:
    return -ETIMEDOUT;
  case osdc_errc::pool_eio:
    return -EIO;
  case osdc_errc::handler_failed:
    return -EIO;
  }
  return -EDOM;
}

const bs::error_category& osdc_category() noexcept {
  static const osdc_error_category c;
  return c;
}
