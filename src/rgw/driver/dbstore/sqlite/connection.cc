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

#include "common/dout.h"
#include "connection.h"
#include "error.h"

namespace rgw::dbstore::sqlite {

db_ptr open_database(const char* filename, int flags)
{
  sqlite3* db = nullptr;
  const int result = ::sqlite3_open_v2(filename, &db, flags, nullptr);
  if (result != SQLITE_OK) {
    throw std::system_error(result, sqlite::error_category());
  }
  // request extended result codes
  (void) ::sqlite3_extended_result_codes(db, 1);
  return db_ptr{db};
}

} // namespace rgw::dbstore::sqlite
