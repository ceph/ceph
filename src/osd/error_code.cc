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
#include "common/errno.h"
#include "error_code.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wnon-virtual-dtor"
class osd_error_category : public ceph::converting_category {
public:
  osd_error_category(){}
  const char* name() const noexcept override;
  const char* message(int ev, char*, std::size_t) const noexcept override;
  std::string message(int ev) const override;
  boost::system::error_condition default_error_condition(int ev) const noexcept
    override;
  bool equivalent(int ev, const boost::system::error_condition& c) const
    noexcept override;
  using ceph::converting_category::equivalent;
  int from_code(int ev) const noexcept override;
};
#pragma GCC diagnostic pop
#pragma clang diagnostic pop

const char* osd_error_category::name() const noexcept {
  return "osd";
}

const char* osd_error_category::message(int ev, char* buf,
					std::size_t len) const noexcept {
  if (ev == 0)
    return "No error";

  switch (static_cast<osd_errc>(ev)) {
  case osd_errc::old_snapc:
    return "ORDERSNAP flag set; writer has old snapc";
  case osd_errc::blocklisted:
    return "Blocklisted";
  case osd_errc::cmpext_mismatch:
    return "CmpExt mismatch";
  }

  if (len) {
    auto s = cpp_strerror(ev);
    auto n = s.copy(buf, len - 1);
    *(buf + n) = '\0';
  }
  return buf;
}

std::string osd_error_category::message(int ev) const {
  if (ev == 0)
    return "No error";

  switch (static_cast<osd_errc>(ev)) {
  case osd_errc::old_snapc:
    return "ORDERSNAP flag set; writer has old snapc";
  case osd_errc::blocklisted:
    return "Blocklisted";
  case osd_errc::cmpext_mismatch:
    return "CmpExt mismatch";
  }

  return cpp_strerror(ev);
}

boost::system::error_condition osd_error_category::default_error_condition(int ev) const noexcept {
  if (ev == static_cast<int>(osd_errc::old_snapc) ||
      ev == static_cast<int>(osd_errc::blocklisted) ||
      ev == static_cast<int>(osd_errc::cmpext_mismatch))
    return { ev, *this };
  else
    return { ev, boost::system::generic_category() };
}

bool osd_error_category::equivalent(int ev, const boost::system::error_condition& c) const noexcept {
  switch (static_cast<osd_errc>(ev)) {
  case osd_errc::old_snapc:
      return c == boost::system::errc::invalid_argument;
  case osd_errc::blocklisted:
      return c == boost::system::errc::operation_not_permitted;
  case osd_errc::cmpext_mismatch:
      return c == boost::system::errc::operation_canceled;
  }
  return default_error_condition(ev) == c;
}

int osd_error_category::from_code(int ev) const noexcept {
  return -ev;
}

const boost::system::error_category& osd_category() noexcept {
  static const osd_error_category c;
  return c;
}
