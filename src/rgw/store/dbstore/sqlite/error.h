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

#pragma once

#include <system_error>
#include <sqlite3.h>

namespace rgw::dbstore::sqlite {

// error category for sqlite extended result codes:
//   https://www.sqlite.org/rescode.html
const std::error_category& error_category();


// sqlite exception type that carries the extended error code and message
class error : public std::runtime_error {
  std::error_code ec;
 public:
  error(const char* errmsg, std::error_code ec)
      : runtime_error(errmsg), ec(ec) {}
  error(sqlite3* db, std::error_code ec) : error(::sqlite3_errmsg(db), ec) {}
  error(sqlite3* db, int result) : error(db, {result, error_category()}) {}
  error(sqlite3* db) : error(db, ::sqlite3_extended_errcode(db)) {}
  std::error_code code() const { return ec; }
};


// sqlite error conditions for primary and extended result codes
//
// 'primary' error_conditions will match 'primary' error_codes as well as any
// 'extended' error_codes whose lowest 8 bits match that primary code. for
// example, the error_condition for SQLITE_CONSTRAINT will match the error_codes
// SQLITE_CONSTRAINT and SQLITE_CONSTRAINT_*
enum class errc {
  // primary result codes
  ok = SQLITE_OK,
  busy = SQLITE_BUSY,
  constraint = SQLITE_CONSTRAINT,
  row = SQLITE_ROW,
  done = SQLITE_DONE,

  // extended result codes
  primary_key_constraint = SQLITE_CONSTRAINT_PRIMARYKEY,
  foreign_key_constraint = SQLITE_CONSTRAINT_FOREIGNKEY,
  unique_constraint = SQLITE_CONSTRAINT_UNIQUE,

  // ..add conditions as needed
};

inline std::error_code make_error_code(errc e)
{
  return {static_cast<int>(e), error_category()};
}

inline std::error_condition make_error_condition(errc e)
{
  return {static_cast<int>(e), error_category()};
}

} // namespace rgw::dbstore::sqlite

namespace std {

// enable implicit conversions from sqlite::errc to std::error_condition
template<> struct is_error_condition_enum<
    rgw::dbstore::sqlite::errc> : public true_type {};

} // namespace std
