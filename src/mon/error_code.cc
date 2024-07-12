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

namespace bs = boost::system;

class mon_error_category : public ceph::converting_category {
public:
  mon_error_category(){}
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

const char* mon_error_category::name() const noexcept {
  return "mon";
}

const char* mon_error_category::message(int ev, char* buf,
					std::size_t len) const noexcept {
  if (ev == 0)
    return "No error";

  if (len) {
    auto s = cpp_strerror(ev);
    auto n = s.copy(buf, len - 1);
    *(buf + n) = '\0';
  }
  return buf;
}

std::string mon_error_category::message(int ev) const {
  if (ev == 0)
    return "No error";

  return cpp_strerror(ev);
}

bs::error_condition
mon_error_category::default_error_condition(int ev) const noexcept {
  return { ev, bs::generic_category() };
}

bool mon_error_category::equivalent(int ev,const bs::error_condition& c) const noexcept {
  return default_error_condition(ev) == c;
}

int mon_error_category::from_code(int ev) const noexcept {
  return -ev;
}

const bs::error_category& mon_category() noexcept {
  static const mon_error_category c;
  return c;
}
#pragma GCC diagnostic pop
#pragma clang diagnostic pop
