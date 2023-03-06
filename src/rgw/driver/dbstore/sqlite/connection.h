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
#include <sqlite3.h>

#include <fmt/format.h>

#include "sqlite/statement.h"

class DoutPrefixProvider;

namespace rgw::dbstore::sqlite {

// owning sqlite3 pointer
struct db_deleter {
  void operator()(sqlite3* p) const { ::sqlite3_close(p); }
};
using db_ptr = std::unique_ptr<sqlite3, db_deleter>;


// open the database file or throw on error
db_ptr open_database(const char* filename, int flags);


struct Connection {
  db_ptr db;
  // map of statements, prepared on first use
  std::map<std::string_view, stmt_ptr> statements;

  explicit Connection(db_ptr db) : db(std::move(db)) {}
};

// sqlite connection factory for ConnectionPool
class ConnectionFactory {
  std::string uri;
  int flags;
 public:
  ConnectionFactory(std::string uri, int flags)
      : uri(std::move(uri)), flags(flags) {}

  auto operator()(const DoutPrefixProvider* dpp)
    -> std::unique_ptr<Connection>
  {
    auto db = open_database(uri.c_str(), flags);
    return std::make_unique<Connection>(std::move(db));
  }
};

} // namespace rgw::dbstore::sqlite
