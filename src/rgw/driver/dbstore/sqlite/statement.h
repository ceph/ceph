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

#include <memory>
#include <span>
#include <string>

#include <sqlite3.h>

class DoutPrefixProvider;

namespace rgw::dbstore::sqlite {

// owning sqlite3_stmt pointer
struct stmt_deleter {
  void operator()(sqlite3_stmt* p) const { ::sqlite3_finalize(p); }
};
using stmt_ptr = std::unique_ptr<sqlite3_stmt, stmt_deleter>;

// non-owning sqlite3_stmt pointer that clears binding state on destruction
struct stmt_binding_deleter {
  void operator()(sqlite3_stmt* p) const { ::sqlite3_clear_bindings(p); }
};
using stmt_binding = std::unique_ptr<sqlite3_stmt, stmt_binding_deleter>;

// non-owning sqlite3_stmt pointer that clears execution state on destruction
struct stmt_execution_deleter {
  void operator()(sqlite3_stmt* p) const { ::sqlite3_reset(p); }
};
using stmt_execution = std::unique_ptr<sqlite3_stmt, stmt_execution_deleter>;


// prepare the sql statement or throw on error
stmt_ptr prepare_statement(const DoutPrefixProvider* dpp,
                           sqlite3* db, std::string_view sql);

// bind a NULL input for the given parameter name
void bind_null(const DoutPrefixProvider* dpp, const stmt_binding& stmt,
               const char* name);

// bind an input string for the given parameter name
void bind_text(const DoutPrefixProvider* dpp, const stmt_binding& stmt,
               const char* name, std::string_view value);

// bind an input integer for the given parameter name
void bind_int(const DoutPrefixProvider* dpp, const stmt_binding& stmt,
              const char* name, int value);

// evaluate a prepared statement, expecting no result rows
void eval0(const DoutPrefixProvider* dpp, const stmt_execution& stmt);

// evaluate a prepared statement, expecting a single result row
void eval1(const DoutPrefixProvider* dpp, const stmt_execution& stmt);

// return the given column as an integer
int column_int(const stmt_execution& stmt, int column);

// return the given column as text, or an empty string on NULL
std::string column_text(const stmt_execution& stmt, int column);

// read the text column from each result row into the given entries, and return
// the sub-span of entries that contain results
auto read_text_rows(const DoutPrefixProvider* dpp,
                    const stmt_execution& stmt,
                    std::span<std::string> entries)
  -> std::span<std::string>;

// execute a raw query without preparing a statement. the optional callback
// can be used to read results
void execute(const DoutPrefixProvider* dpp, sqlite3* db, const char* query,
             sqlite3_callback callback, void* arg);

} // namespace rgw::dbstore::sqlite
