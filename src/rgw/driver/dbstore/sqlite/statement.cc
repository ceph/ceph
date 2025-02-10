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
#include "error.h"
#include "statement.h"

#define dout_subsys ceph_subsys_rgw_dbstore

namespace rgw::dbstore::sqlite {

// owning pointer to arbitrary memory allocated and returned by sqlite3
struct sqlite_deleter {
  template <typename T>
  void operator()(T* p) { ::sqlite3_free(p); }
};
template <typename T>
using sqlite_ptr = std::unique_ptr<T, sqlite_deleter>;


stmt_ptr prepare_statement(const DoutPrefixProvider* dpp,
                           sqlite3* db, std::string_view sql)
{
  sqlite3_stmt* stmt = nullptr;
  int result = ::sqlite3_prepare_v2(db, sql.data(), sql.size(), &stmt, nullptr);
  auto ec = std::error_code{result, sqlite::error_category()};
  if (ec != sqlite::errc::ok) {
    const char* errmsg = ::sqlite3_errmsg(db);
    ldpp_dout(dpp, 1) << "preparation failed: " << errmsg
        << " (" << ec << ")\nstatement: " << sql << dendl;
    throw sqlite::error(errmsg, ec);
  }
  return stmt_ptr{stmt};
}

static int bind_index(const DoutPrefixProvider* dpp,
                      const stmt_binding& stmt, const char* name)
{
  const int index = ::sqlite3_bind_parameter_index(stmt.get(), name);
  if (index <= 0) {
    ldpp_dout(dpp, 1) << "binding failed on parameter name="
        << name << dendl;
    sqlite3* db = ::sqlite3_db_handle(stmt.get());
    throw sqlite::error(db);
  }
  return index;
}

void bind_null(const DoutPrefixProvider* dpp, const stmt_binding& stmt,
               const char* name)
{
  const int index = bind_index(dpp, stmt, name);

  int result = ::sqlite3_bind_null(stmt.get(), index);
  auto ec = std::error_code{result, sqlite::error_category()};
  if (ec != sqlite::errc::ok) {
    ldpp_dout(dpp, 1) << "binding failed on parameter name="
        << name << dendl;
    sqlite3* db = ::sqlite3_db_handle(stmt.get());
    throw sqlite::error(db, ec);
  }
}

void bind_text(const DoutPrefixProvider* dpp, const stmt_binding& stmt,
               const char* name, std::string_view value)
{
  const int index = bind_index(dpp, stmt, name);

  int result = ::sqlite3_bind_text(stmt.get(), index, value.data(),
                                   value.size(), SQLITE_STATIC);
  auto ec = std::error_code{result, sqlite::error_category()};
  if (ec != sqlite::errc::ok) {
    ldpp_dout(dpp, 1) << "binding failed on parameter name="
        << name << " value=" << value << dendl;
    sqlite3* db = ::sqlite3_db_handle(stmt.get());
    throw sqlite::error(db, ec);
  }
}

void bind_int(const DoutPrefixProvider* dpp, const stmt_binding& stmt,
              const char* name, int value)
{
  const int index = bind_index(dpp, stmt, name);

  int result = ::sqlite3_bind_int(stmt.get(), index, value);
  auto ec = std::error_code{result, sqlite::error_category()};
  if (ec != sqlite::errc::ok) {
    ldpp_dout(dpp, 1) << "binding failed on parameter name="
        << name << " value=" << value << dendl;
    sqlite3* db = ::sqlite3_db_handle(stmt.get());
    throw sqlite::error(db, ec);
  }
}

void eval0(const DoutPrefixProvider* dpp, const stmt_execution& stmt)
{
  sqlite_ptr<char> sql;
  if (dpp->get_cct()->_conf->subsys.should_gather<dout_subsys, 20>()) {
    sql.reset(::sqlite3_expanded_sql(stmt.get()));
  }

  const int result = ::sqlite3_step(stmt.get());
  auto ec = std::error_code{result, sqlite::error_category()};
  sqlite3* db = ::sqlite3_db_handle(stmt.get());

  if (ec != sqlite::errc::done) {
    const char* errmsg = ::sqlite3_errmsg(db);
    ldpp_dout(dpp, 20) << "evaluation failed: " << errmsg
        << " (" << ec << ")\nstatement: " << (sql ? sql.get() : "") << dendl;
    throw sqlite::error(errmsg, ec);
  }
  ldpp_dout(dpp, 20) << "evaluation succeeded: " << (sql ? sql.get() : "") << dendl;
}

void eval1(const DoutPrefixProvider* dpp, const stmt_execution& stmt)
{
  sqlite_ptr<char> sql;
  if (dpp->get_cct()->_conf->subsys.should_gather<dout_subsys, 20>()) {
    sql.reset(::sqlite3_expanded_sql(stmt.get()));
  }

  const int result = ::sqlite3_step(stmt.get());
  auto ec = std::error_code{result, sqlite::error_category()};
  if (ec != sqlite::errc::row) {
    sqlite3* db = ::sqlite3_db_handle(stmt.get());
    const char* errmsg = ::sqlite3_errmsg(db);
    ldpp_dout(dpp, 1) << "evaluation failed: " << errmsg << " (" << ec
        << ")\nstatement: " << (sql ? sql.get() : "") << dendl;
    throw sqlite::error(errmsg, ec);
  }
  ldpp_dout(dpp, 20) << "evaluation succeeded: " << (sql ? sql.get() : "") << dendl;
}

int column_int(const stmt_execution& stmt, int column)
{
  return ::sqlite3_column_int(stmt.get(), column);
}

std::string column_text(const stmt_execution& stmt, int column)
{
  const unsigned char* text = ::sqlite3_column_text(stmt.get(), column);
  // may be NULL
  if (text) {
    const std::size_t size = ::sqlite3_column_bytes(stmt.get(), column);
    return {reinterpret_cast<const char*>(text), size};
  } else {
    return {};
  }
}

auto read_text_rows(const DoutPrefixProvider* dpp,
                    const stmt_execution& stmt,
                    std::span<std::string> entries)
  -> std::span<std::string>
{
  sqlite_ptr<char> sql;
  if (dpp->get_cct()->_conf->subsys.should_gather<dout_subsys, 20>()) {
    sql.reset(::sqlite3_expanded_sql(stmt.get()));
  }

  std::size_t count = 0;
  while (count < entries.size()) {
    const int result = ::sqlite3_step(stmt.get());
    auto ec = std::error_code{result, sqlite::error_category()};
    if (ec == sqlite::errc::done) {
      break;
    }
    if (ec != sqlite::errc::row) {
      sqlite3* db = ::sqlite3_db_handle(stmt.get());
      const char* errmsg = ::sqlite3_errmsg(db);
      ldpp_dout(dpp, 1) << "evaluation failed: " << errmsg << " (" << ec
          << ")\nstatement: " << (sql ? sql.get() : "") << dendl;
      throw sqlite::error(errmsg, ec);
    }
    entries[count] = column_text(stmt, 0);
    ++count;
  }
  ldpp_dout(dpp, 20) << "statement evaluation produced " << count
      << " results: " << (sql ? sql.get() : "") << dendl;

  return entries.first(count);
}

void execute(const DoutPrefixProvider* dpp, sqlite3* db, const char* query,
             sqlite3_callback callback, void* arg)
{
  char* errmsg = nullptr;
  const int result = ::sqlite3_exec(db, query, callback, arg, &errmsg);
  auto ec = std::error_code{result, sqlite::error_category()};
  auto ptr = sqlite_ptr<char>{errmsg}; // free on destruction
  if (ec != sqlite::errc::ok) {
    ldpp_dout(dpp, 1) << "query execution failed: " << errmsg << " (" << ec
        << ")\nquery: " << query << dendl;
    throw sqlite::error(errmsg, ec);
  }
  ldpp_dout(dpp, 20) << "query execution succeeded: " << query << dendl;
}

} // namespace rgw::dbstore::sqlite
