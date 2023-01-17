// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "error.h"

namespace rgw::dbstore::sqlite {

const std::error_category& error_category()
{
  struct category : std::error_category {
    const char* name() const noexcept override {
      return "dbstore:sqlite";
    }
    std::string message(int ev) const override {
      return ::sqlite3_errstr(ev);
    }
    std::error_condition default_error_condition(int code) const noexcept override {
      return {code & 0xFF, category()};
    }
  };
  static category instance;
  return instance;
}

} // namespace rgw::dbstore::sqlite
