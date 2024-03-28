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

#include <charconv>
#include <initializer_list>
#include <map>

#include <fmt/format.h>

#include <sqlite3.h>

#include "include/buffer.h"
#include "include/encoding.h"
#include "common/dout.h"
#include "common/random_string.h"

#include "driver/rados/rgw_zone.h" // FIXME: subclass dependency

#include "common/connection_pool.h"
#include "sqlite/connection.h"
#include "sqlite/error.h"
#include "sqlite/statement.h"
#include "sqlite_schema.h"
#include "sqlite.h"

#define dout_subsys ceph_subsys_rgw_dbstore

namespace rgw::dbstore::config {

struct Prefix : DoutPrefixPipe {
  std::string_view prefix;
  Prefix(const DoutPrefixProvider& dpp, std::string_view prefix)
      : DoutPrefixPipe(dpp), prefix(prefix) {}
  unsigned get_subsys() const override { return dout_subsys; }
  void add_prefix(std::ostream& out) const override {
    out << prefix;
  }
};

namespace {

// parameter names for prepared statement bindings
static constexpr const char* P1 = ":1";
static constexpr const char* P2 = ":2";
static constexpr const char* P3 = ":3";
static constexpr const char* P4 = ":4";
static constexpr const char* P5 = ":5";
static constexpr const char* P6 = ":6";

// bind as text unless value is empty
void bind_text_or_null(const DoutPrefixProvider* dpp,
                       const sqlite::stmt_binding& stmt,
                       const char* name, std::string_view value)
{
  if (value.empty()) {
    sqlite::bind_null(dpp, stmt, name);
  } else {
    sqlite::bind_text(dpp, stmt, name, value);
  }
}

void read_text_rows(const DoutPrefixProvider* dpp,
                    const sqlite::stmt_execution& stmt,
                    std::span<std::string> entries,
                    sal::ListResult<std::string>& result)
{
  result.entries = sqlite::read_text_rows(dpp, stmt, entries);
  if (result.entries.size() < entries.size()) { // end of listing
    result.next.clear();
  } else {
    result.next = result.entries.back();
  }
}

struct RealmRow {
  RGWRealm info;
  int ver;
  std::string tag;
};

void read_realm_row(const sqlite::stmt_execution& stmt, RealmRow& row)
{
  row.info.id = sqlite::column_text(stmt, 0);
  row.info.name = sqlite::column_text(stmt, 1);
  row.info.current_period = sqlite::column_text(stmt, 2);
  row.info.epoch = sqlite::column_int(stmt, 3);
  row.ver = sqlite::column_int(stmt, 4);
  row.tag = sqlite::column_text(stmt, 5);
}

void read_period_row(const sqlite::stmt_execution& stmt, RGWPeriod& row)
{
  // just read the Data column and decode everything else from that
  std::string data = sqlite::column_text(stmt, 3);

  bufferlist bl = bufferlist::static_from_string(data);
  auto p = bl.cbegin();
  decode(row, p);
}

struct ZoneGroupRow {
  RGWZoneGroup info;
  int ver;
  std::string tag;
};

void read_zonegroup_row(const sqlite::stmt_execution& stmt, ZoneGroupRow& row)
{
  std::string data = sqlite::column_text(stmt, 3);
  row.ver = sqlite::column_int(stmt, 4);
  row.tag = sqlite::column_text(stmt, 5);

  bufferlist bl = bufferlist::static_from_string(data);
  auto p = bl.cbegin();
  decode(row.info, p);
}

struct ZoneRow {
  RGWZoneParams info;
  int ver;
  std::string tag;
};

void read_zone_row(const sqlite::stmt_execution& stmt, ZoneRow& row)
{
  std::string data = sqlite::column_text(stmt, 3);
  row.ver = sqlite::column_int(stmt, 4);
  row.tag = sqlite::column_text(stmt, 5);

  bufferlist bl = bufferlist::static_from_string(data);
  auto p = bl.cbegin();
  decode(row.info, p);
}

std::string generate_version_tag(CephContext* cct)
{
  static constexpr auto TAG_LEN = 24;
  return gen_rand_alphanumeric(cct, TAG_LEN);
}

using SQLiteConnectionHandle = ConnectionHandle<sqlite::Connection>;

using SQLiteConnectionPool = ConnectionPool<
    sqlite::Connection, sqlite::ConnectionFactory>;

} // anonymous namespace

class SQLiteImpl : public SQLiteConnectionPool {
 public:
  using SQLiteConnectionPool::SQLiteConnectionPool;
};


SQLiteConfigStore::SQLiteConfigStore(std::unique_ptr<SQLiteImpl> impl)
  : impl(std::move(impl))
{
}

SQLiteConfigStore::~SQLiteConfigStore() = default;


// Realm

class SQLiteRealmWriter : public sal::RealmWriter {
  SQLiteImpl* impl;
  int ver;
  std::string tag;
  std::string realm_id;
  std::string realm_name;
 public:
  SQLiteRealmWriter(SQLiteImpl* impl, int ver, std::string tag,
                    std::string_view realm_id, std::string_view realm_name)
    : impl(impl), ver(ver), tag(std::move(tag)),
      realm_id(realm_id), realm_name(realm_name)
  {}

  int write(const DoutPrefixProvider* dpp, optional_yield y,
            const RGWRealm& info) override
  {
    Prefix prefix{*dpp, "dbconfig:sqlite:realm_write "}; dpp = &prefix;

    if (!impl) {
      return -EINVAL; // can't write after a conflict or delete
    }
    if (realm_id != info.id || realm_name != info.name) {
      return -EINVAL; // can't modify realm id or name directly
    }

    try {
      auto conn = impl->get(dpp);
      auto& stmt = conn->statements["realm_upd"];
      if (!stmt) {
        const std::string sql = fmt::format(schema::realm_update5,
                                            P1, P2, P3, P4, P5);
        stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
      auto binding = sqlite::stmt_binding{stmt.get()};
      sqlite::bind_text(dpp, binding, P1, info.id);
      sqlite::bind_text(dpp, binding, P2, info.current_period);
      sqlite::bind_int(dpp, binding, P3, info.epoch);
      sqlite::bind_int(dpp, binding, P4, ver);
      sqlite::bind_text(dpp, binding, P5, tag);

      auto reset = sqlite::stmt_execution{stmt.get()};
      sqlite::eval0(dpp, reset);

      if (!::sqlite3_changes(conn->db.get())) { // VersionNumber/Tag mismatch
        // our version is no longer consistent, so later writes would fail too
        impl = nullptr;
        return -ECANCELED;
      }
    } catch (const sqlite::error& e) {
      ldpp_dout(dpp, 20) << "realm update failed: " << e.what() << dendl;
      if (e.code() == sqlite::errc::foreign_key_constraint) {
        return -EINVAL; // refers to nonexistent CurrentPeriod
      } else if (e.code() == sqlite::errc::busy) {
        return -EBUSY;
      }
      return -EIO;
    }
    ++ver;
    return 0;
  }

  int rename(const DoutPrefixProvider* dpp, optional_yield y,
             RGWRealm& info, std::string_view new_name) override
  {
    Prefix prefix{*dpp, "dbconfig:sqlite:realm_rename "}; dpp = &prefix;

    if (!impl) {
      return -EINVAL; // can't write after conflict or delete
    }
    if (realm_id != info.id || realm_name != info.name) {
      return -EINVAL; // can't modify realm id or name directly
    }
    if (new_name.empty()) {
      ldpp_dout(dpp, 0) << "realm cannot have an empty name" << dendl;
      return -EINVAL;
    }

    try {
      auto conn = impl->get(dpp);
      auto& stmt = conn->statements["realm_rename"];
      if (!stmt) {
        const std::string sql = fmt::format(schema::realm_rename4,
                                            P1, P2, P3, P4);
        stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
      auto binding = sqlite::stmt_binding{stmt.get()};
      sqlite::bind_text(dpp, binding, P1, realm_id);
      sqlite::bind_text(dpp, binding, P2, new_name);
      sqlite::bind_int(dpp, binding, P3, ver);
      sqlite::bind_text(dpp, binding, P4, tag);

      auto reset = sqlite::stmt_execution{stmt.get()};
      sqlite::eval0(dpp, reset);

      if (!::sqlite3_changes(conn->db.get())) { // VersionNumber/Tag mismatch
        impl = nullptr;
        return -ECANCELED;
      }
    } catch (const sqlite::error& e) {
      ldpp_dout(dpp, 20) << "realm rename failed: " << e.what() << dendl;
      if (e.code() == sqlite::errc::unique_constraint) {
        return -EEXIST; // Name already taken
      } else if (e.code() == sqlite::errc::busy) {
        return -EBUSY;
      }
      return -EIO;
    }
    info.name = std::string{new_name};
    ++ver;
    return 0;
  }

  int remove(const DoutPrefixProvider* dpp, optional_yield y) override
  {
    Prefix prefix{*dpp, "dbconfig:sqlite:realm_remove "}; dpp = &prefix;

    if (!impl) {
      return -EINVAL; // can't write after conflict or delete
    }
    try {
      auto conn = impl->get(dpp);
      auto& stmt = conn->statements["realm_del"];
      if (!stmt) {
        const std::string sql = fmt::format(schema::realm_delete3, P1, P2, P3);
        stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
      auto binding = sqlite::stmt_binding{stmt.get()};
      sqlite::bind_text(dpp, binding, P1, realm_id);
      sqlite::bind_int(dpp, binding, P2, ver);
      sqlite::bind_text(dpp, binding, P3, tag);

      auto reset = sqlite::stmt_execution{stmt.get()};
      sqlite::eval0(dpp, reset);

      impl = nullptr; // prevent any further writes after delete
      if (!::sqlite3_changes(conn->db.get())) {
        return -ECANCELED; // VersionNumber/Tag mismatch
      }
    } catch (const sqlite::error& e) {
      ldpp_dout(dpp, 20) << "realm delete failed: " << e.what() << dendl;
      if (e.code() == sqlite::errc::busy) {
        return -EBUSY;
      }
      return -EIO;
    }
    return 0;
  }
}; // SQLiteRealmWriter


int SQLiteConfigStore::write_default_realm_id(const DoutPrefixProvider* dpp,
                                              optional_yield y, bool exclusive,
                                              std::string_view realm_id,
                                              std::string_view realm_name)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:write_default_realm_id "}; dpp = &prefix;

  if (realm_id.empty()) {
    ldpp_dout(dpp, 0) << "requires a realm id" << dendl;
    return -EINVAL;
  }

  try {
    auto conn = impl->get(dpp);
    sqlite::stmt_ptr* stmt = nullptr;
    if (exclusive) {
      stmt = &conn->statements["def_realm_ins"];
      if (!*stmt) {
        const std::string sql = fmt::format(schema::default_realm_insert1, P1, P2);
        *stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
    } else {
      stmt = &conn->statements["def_realm_ups"];
      if (!*stmt) {
        const std::string sql = fmt::format(schema::default_realm_upsert1, P1, P2);
        *stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
    }
    auto binding = sqlite::stmt_binding{stmt->get()};
    sqlite::bind_text(dpp, binding, P1, realm_id);
    sqlite::bind_text(dpp, binding, P2, realm_name);

    auto reset = sqlite::stmt_execution{stmt->get()};
    sqlite::eval0(dpp, reset);
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "default realm insert failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::primary_key_constraint) {
      return -EEXIST;
    } else if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }
  return 0;
}

int SQLiteConfigStore::read_default_realm_id(const DoutPrefixProvider* dpp,
                                             optional_yield y,
                                             std::string& realm_id,
                                             std::string& realm_name)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:read_default_realm_id "}; dpp = &prefix;

  try {
    auto conn = impl->get(dpp);
    auto& stmt = conn->statements["def_realm_sel"];
    if (!stmt) {
      static constexpr std::string_view sql = schema::default_realm_select0;
      stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
    }
    auto reset = sqlite::stmt_execution{stmt.get()};
    sqlite::eval1(dpp, reset);

    realm_id = sqlite::column_text(reset, 0);
    realm_name = sqlite::column_text(reset, 1);
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "default realm select failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }
  return 0;
}

int SQLiteConfigStore::delete_default_realm_id(const DoutPrefixProvider* dpp,
                                               optional_yield y)

{
  Prefix prefix{*dpp, "dbconfig:sqlite:delete_default_realm_id "}; dpp = &prefix;

  try {
    auto conn = impl->get(dpp);
    auto& stmt = conn->statements["def_realm_del"];
    if (!stmt) {
      static constexpr std::string_view sql = schema::default_realm_delete0;
      stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
    }
    auto reset = sqlite::stmt_execution{stmt.get()};
    sqlite::eval0(dpp, reset);

    if (!::sqlite3_changes(conn->db.get())) {
      return -ENOENT;
    }
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "default realm delete failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }
  return 0;
}


int SQLiteConfigStore::create_realm(const DoutPrefixProvider* dpp,
                                    optional_yield y, bool exclusive,
                                    const RGWRealm& info,
                                    std::unique_ptr<sal::RealmWriter>* writer)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:create_realm "}; dpp = &prefix;

  if (info.id.empty()) {
    ldpp_dout(dpp, 0) << "realm cannot have an empty id" << dendl;
    return -EINVAL;
  }
  if (info.name.empty()) {
    ldpp_dout(dpp, 0) << "realm cannot have an empty name" << dendl;
    return -EINVAL;
  }

  int ver = 1;
  auto tag = generate_version_tag(dpp->get_cct());

  try {
    auto conn = impl->get(dpp);
    sqlite::stmt_ptr* stmt = nullptr;
    if (exclusive) {
      stmt = &conn->statements["realm_ins"];
      if (!*stmt) {
        const std::string sql = fmt::format(schema::realm_insert4,
                                            P1, P2, P3, P4);
        *stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
    } else {
      stmt = &conn->statements["realm_ups"];
      if (!*stmt) {
        const std::string sql = fmt::format(schema::realm_upsert4,
                                            P1, P2, P3, P4);
        *stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
    }
    auto binding = sqlite::stmt_binding{stmt->get()};
    sqlite::bind_text(dpp, binding, P1, info.id);
    sqlite::bind_text(dpp, binding, P2, info.name);
    sqlite::bind_int(dpp, binding, P3, ver);
    sqlite::bind_text(dpp, binding, P4, tag);

    auto reset = sqlite::stmt_execution{stmt->get()};
    sqlite::eval0(dpp, reset);
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "realm insert failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::primary_key_constraint) {
      return -EEXIST; // ID already taken
    } else if (e.code() == sqlite::errc::unique_constraint) {
      return -EEXIST; // Name already taken
    } else if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }

  if (writer) {
    *writer = std::make_unique<SQLiteRealmWriter>(
        impl.get(), ver, std::move(tag), info.id, info.name);
  }
  return 0;
}

int SQLiteConfigStore::read_realm_by_id(const DoutPrefixProvider* dpp,
                                        optional_yield y,
                                        std::string_view realm_id,
                                        RGWRealm& info,
                                        std::unique_ptr<sal::RealmWriter>* writer)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:read_realm_by_id "}; dpp = &prefix;

  if (realm_id.empty()) {
    ldpp_dout(dpp, 0) << "requires a realm id" << dendl;
    return -EINVAL;
  }

  RealmRow row;
  try {
    auto conn = impl->get(dpp);
    auto& stmt = conn->statements["realm_sel_id"];
    if (!stmt) {
      const std::string sql = fmt::format(schema::realm_select_id1, P1);
      stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
    }
    auto binding = sqlite::stmt_binding{stmt.get()};
    sqlite::bind_text(dpp, binding, P1, realm_id);

    auto reset = sqlite::stmt_execution{stmt.get()};
    sqlite::eval1(dpp, reset);

    read_realm_row(reset, row);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "realm decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "realm select failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::done) {
      return -ENOENT;
    } else if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }

  info = std::move(row.info);
  if (writer) {
    *writer = std::make_unique<SQLiteRealmWriter>(
        impl.get(), row.ver, std::move(row.tag), info.id, info.name);
  }
  return 0;
}

static void realm_select_by_name(const DoutPrefixProvider* dpp,
                                 sqlite::Connection& conn,
                                 std::string_view realm_name,
                                 RealmRow& row)
{
  auto& stmt = conn.statements["realm_sel_name"];
  if (!stmt) {
    const std::string sql = fmt::format(schema::realm_select_name1, P1);
    stmt = sqlite::prepare_statement(dpp, conn.db.get(), sql);
  }
  auto binding = sqlite::stmt_binding{stmt.get()};
  sqlite::bind_text(dpp, binding, P1, realm_name);

  auto reset = sqlite::stmt_execution{stmt.get()};
  sqlite::eval1(dpp, reset);

  read_realm_row(reset, row);
}

int SQLiteConfigStore::read_realm_by_name(const DoutPrefixProvider* dpp,
                                          optional_yield y,
                                          std::string_view realm_name,
                                          RGWRealm& info,
                                          std::unique_ptr<sal::RealmWriter>* writer)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:read_realm_by_name "}; dpp = &prefix;

  if (realm_name.empty()) {
    ldpp_dout(dpp, 0) << "requires a realm name" << dendl;
    return -EINVAL;
  }

  RealmRow row;
  try {
    auto conn = impl->get(dpp);
    realm_select_by_name(dpp, *conn, realm_name, row);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "realm decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "realm select failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::done) {
      return -ENOENT;
    } else if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }

  info = std::move(row.info);
  if (writer) {
    *writer = std::make_unique<SQLiteRealmWriter>(
        impl.get(), row.ver, std::move(row.tag), info.id, info.name);
  }
  return 0;
}

int SQLiteConfigStore::read_default_realm(const DoutPrefixProvider* dpp,
                                          optional_yield y,
                                          RGWRealm& info,
                                          std::unique_ptr<sal::RealmWriter>* writer)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:read_default_realm "}; dpp = &prefix;

  RealmRow row;
  try {
    auto conn = impl->get(dpp);
    auto& stmt = conn->statements["realm_sel_def"];
    if (!stmt) {
      static constexpr std::string_view sql = schema::realm_select_default0;
      stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
    }
    auto reset = sqlite::stmt_execution{stmt.get()};
    sqlite::eval1(dpp, reset);

    read_realm_row(reset, row);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "realm decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "realm select failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::done) {
      return -ENOENT;
    } else if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }

  info = std::move(row.info);
  if (writer) {
    *writer = std::make_unique<SQLiteRealmWriter>(
        impl.get(), row.ver, std::move(row.tag), info.id, info.name);
  }
  return 0;
}

int SQLiteConfigStore::read_realm_id(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view realm_name,
                                     std::string& realm_id)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:read_realm_id "}; dpp = &prefix;

  if (realm_name.empty()) {
    ldpp_dout(dpp, 0) << "requires a realm name" << dendl;
    return -EINVAL;
  }

  try {
    auto conn = impl->get(dpp);

    RealmRow row;
    realm_select_by_name(dpp, *conn, realm_name, row);

    realm_id = std::move(row.info.id);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "realm decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "realm select failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::done) {
      return -ENOENT;
    } else if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }

  return 0;
}

int SQLiteConfigStore::realm_notify_new_period(const DoutPrefixProvider* dpp,
                                               optional_yield y,
                                               const RGWPeriod& period)
{
  return -ENOTSUP;
}

int SQLiteConfigStore::list_realm_names(const DoutPrefixProvider* dpp,
                                        optional_yield y, const std::string& marker,
                                        std::span<std::string> entries,
                                        sal::ListResult<std::string>& result)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:list_realm_names "}; dpp = &prefix;

  try {
    auto conn = impl->get(dpp);
    auto& stmt = conn->statements["realm_sel_names"];
    if (!stmt) {
      const std::string sql = fmt::format(schema::realm_select_names2, P1, P2);
      stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
    }
    auto binding = sqlite::stmt_binding{stmt.get()};
    sqlite::bind_text(dpp, binding, P1, marker);
    sqlite::bind_int(dpp, binding, P2, entries.size());

    auto reset = sqlite::stmt_execution{stmt.get()};
    read_text_rows(dpp, reset, entries, result);
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "realm select failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }
  return 0;
}


// Period

int SQLiteConfigStore::create_period(const DoutPrefixProvider* dpp,
                                     optional_yield y, bool exclusive,
                                     const RGWPeriod& info)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:create_period "}; dpp = &prefix;

  if (info.id.empty()) {
    ldpp_dout(dpp, 0) << "period cannot have an empty id" << dendl;
    return -EINVAL;
  }

  bufferlist bl;
  encode(info, bl);
  const auto data = std::string_view{bl.c_str(), bl.length()};

  try {
    auto conn = impl->get(dpp);
    sqlite::stmt_ptr* stmt = nullptr;
    if (exclusive) {
      stmt = &conn->statements["period_ins"];
      if (!*stmt) {
        const std::string sql = fmt::format(schema::period_insert4,
                                            P1, P2, P3, P4);
        *stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
    } else {
      stmt = &conn->statements["period_ups"];
      if (!*stmt) {
        const std::string sql = fmt::format(schema::period_upsert4,
                                            P1, P2, P3, P4);
        *stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
    }
    auto binding = sqlite::stmt_binding{stmt->get()};
    sqlite::bind_text(dpp, binding, P1, info.id);
    sqlite::bind_int(dpp, binding, P2, info.epoch);
    sqlite::bind_text(dpp, binding, P3, info.realm_id);
    sqlite::bind_text(dpp, binding, P4, data);

    auto reset = sqlite::stmt_execution{stmt->get()};
    sqlite::eval0(dpp, reset);
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "period insert failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::foreign_key_constraint) {
      return -EINVAL; // refers to nonexistent RealmID
    } else if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }
  return 0;
}

static void period_select_epoch(const DoutPrefixProvider* dpp,
                                sqlite::Connection& conn,
                                std::string_view id, uint32_t epoch,
                                RGWPeriod& row)
{
  auto& stmt = conn.statements["period_sel_epoch"];
  if (!stmt) {
    const std::string sql = fmt::format(schema::period_select_epoch2, P1, P2);
    stmt = sqlite::prepare_statement(dpp, conn.db.get(), sql);
  }
  auto binding = sqlite::stmt_binding{stmt.get()};
  sqlite::bind_text(dpp, binding, P1, id);
  sqlite::bind_int(dpp, binding, P2, epoch);

  auto reset = sqlite::stmt_execution{stmt.get()};
  sqlite::eval1(dpp, reset);

  read_period_row(reset, row);
}

static void period_select_latest(const DoutPrefixProvider* dpp,
                                 sqlite::Connection& conn,
                                 std::string_view id, RGWPeriod& row)
{
  auto& stmt = conn.statements["period_sel_latest"];
  if (!stmt) {
    const std::string sql = fmt::format(schema::period_select_latest1, P1);
    stmt = sqlite::prepare_statement(dpp, conn.db.get(), sql);
  }
  auto binding = sqlite::stmt_binding{stmt.get()};
  sqlite::bind_text(dpp, binding, P1, id);

  auto reset = sqlite::stmt_execution{stmt.get()};
  sqlite::eval1(dpp, reset);

  read_period_row(reset, row);
}

int SQLiteConfigStore::read_period(const DoutPrefixProvider* dpp,
                                   optional_yield y,
                                   std::string_view period_id,
                                   std::optional<uint32_t> epoch,
                                   RGWPeriod& info)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:read_period "}; dpp = &prefix;

  if (period_id.empty()) {
    ldpp_dout(dpp, 0) << "requires a period id" << dendl;
    return -EINVAL;
  }

  try {
    auto conn = impl->get(dpp);
    if (epoch) {
      period_select_epoch(dpp, *conn, period_id, *epoch, info);
    } else {
      period_select_latest(dpp, *conn, period_id, info);
    }
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "period decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "period select failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::done) {
      return -ENOENT;
    } else if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }
  return 0;
}

int SQLiteConfigStore::delete_period(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view period_id)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:delete_period "}; dpp = &prefix;

  if (period_id.empty()) {
    ldpp_dout(dpp, 0) << "requires a period id" << dendl;
    return -EINVAL;
  }

  try {
    auto conn = impl->get(dpp);
    auto& stmt = conn->statements["period_del"];
    if (!stmt) {
      const std::string sql = fmt::format(schema::period_delete1, P1);
      stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
    }
    auto binding = sqlite::stmt_binding{stmt.get()};
    sqlite::bind_text(dpp, binding, P1, period_id);

    auto reset = sqlite::stmt_execution{stmt.get()};
    sqlite::eval0(dpp, reset);

    if (!::sqlite3_changes(conn->db.get())) {
      return -ENOENT;
    }
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "period delete failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }
  return 0;
}

int SQLiteConfigStore::list_period_ids(const DoutPrefixProvider* dpp,
                                       optional_yield y,
                                       const std::string& marker,
                                       std::span<std::string> entries,
                                       sal::ListResult<std::string>& result)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:list_period_ids "}; dpp = &prefix;

  try {
    auto conn = impl->get(dpp);
    auto& stmt = conn->statements["period_sel_ids"];
    if (!stmt) {
      const std::string sql = fmt::format(schema::period_select_ids2, P1, P2);
      stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
    }
    auto binding = sqlite::stmt_binding{stmt.get()};
    sqlite::bind_text(dpp, binding, P1, marker);
    sqlite::bind_int(dpp, binding, P2, entries.size());

    auto reset = sqlite::stmt_execution{stmt.get()};
    read_text_rows(dpp, reset, entries, result);
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "period select failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }
  return 0;
}


// ZoneGroup

class SQLiteZoneGroupWriter : public sal::ZoneGroupWriter {
  SQLiteImpl* impl;
  int ver;
  std::string tag;
  std::string zonegroup_id;
  std::string zonegroup_name;
 public:
  SQLiteZoneGroupWriter(SQLiteImpl* impl, int ver, std::string tag,
                        std::string_view zonegroup_id,
                        std::string_view zonegroup_name)
    : impl(impl), ver(ver), tag(std::move(tag)),
      zonegroup_id(zonegroup_id), zonegroup_name(zonegroup_name)
  {}

  int write(const DoutPrefixProvider* dpp, optional_yield y,
            const RGWZoneGroup& info) override
  {
    Prefix prefix{*dpp, "dbconfig:sqlite:zonegroup_write "}; dpp = &prefix;

    if (!impl) {
      return -EINVAL; // can't write after conflict or delete
    }
    if (zonegroup_id != info.id || zonegroup_name != info.name) {
      return -EINVAL; // can't modify zonegroup id or name directly
    }

    bufferlist bl;
    encode(info, bl);
    const auto data = std::string_view{bl.c_str(), bl.length()};

    try {
      auto conn = impl->get(dpp);
      auto& stmt = conn->statements["zonegroup_upd"];
      if (!stmt) {
        const std::string sql = fmt::format(schema::zonegroup_update5,
                                            P1, P2, P3, P4, P5);
        stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
      auto binding = sqlite::stmt_binding{stmt.get()};
      sqlite::bind_text(dpp, binding, P1, info.id);
      bind_text_or_null(dpp, binding, P2, info.realm_id);
      sqlite::bind_text(dpp, binding, P3, data);
      sqlite::bind_int(dpp, binding, P4, ver);
      sqlite::bind_text(dpp, binding, P5, tag);

      auto reset = sqlite::stmt_execution{stmt.get()};
      sqlite::eval0(dpp, reset);

      if (!::sqlite3_changes(conn->db.get())) { // VersionNumber/Tag mismatch
        impl = nullptr;
        return -ECANCELED;
      }
    } catch (const sqlite::error& e) {
      ldpp_dout(dpp, 20) << "zonegroup update failed: " << e.what() << dendl;
      if (e.code() == sqlite::errc::foreign_key_constraint) {
        return -EINVAL; // refers to nonexistent RealmID
      } else if (e.code() == sqlite::errc::busy) {
        return -EBUSY;
      }
      return -EIO;
    }
    return 0;
  }

  int rename(const DoutPrefixProvider* dpp, optional_yield y,
             RGWZoneGroup& info, std::string_view new_name) override
  {
    Prefix prefix{*dpp, "dbconfig:sqlite:zonegroup_rename "}; dpp = &prefix;

    if (!impl) {
      return -EINVAL; // can't write after conflict or delete
    }
    if (zonegroup_id != info.get_id() || zonegroup_name != info.get_name()) {
      return -EINVAL; // can't modify zonegroup id or name directly
    }
    if (new_name.empty()) {
      ldpp_dout(dpp, 0) << "zonegroup cannot have an empty name" << dendl;
      return -EINVAL;
    }

    try {
      auto conn = impl->get(dpp);
      auto& stmt = conn->statements["zonegroup_rename"];
      if (!stmt) {
        const std::string sql = fmt::format(schema::zonegroup_rename4,
                                            P1, P2, P3, P4);
        stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
      auto binding = sqlite::stmt_binding{stmt.get()};
      sqlite::bind_text(dpp, binding, P1, info.id);
      sqlite::bind_text(dpp, binding, P2, new_name);
      sqlite::bind_int(dpp, binding, P3, ver);
      sqlite::bind_text(dpp, binding, P4, tag);

      auto reset = sqlite::stmt_execution{stmt.get()};
      sqlite::eval0(dpp, reset);

      if (!::sqlite3_changes(conn->db.get())) { // VersionNumber/Tag mismatch
        impl = nullptr;
        return -ECANCELED;
      }
    } catch (const sqlite::error& e) {
      ldpp_dout(dpp, 20) << "zonegroup rename failed: " << e.what() << dendl;
      if (e.code() == sqlite::errc::unique_constraint) {
        return -EEXIST; // Name already taken
      } else if (e.code() == sqlite::errc::busy) {
        return -EBUSY;
      }
      return -EIO;
    }
    info.name = std::string{new_name};
    return 0;
  }

  int remove(const DoutPrefixProvider* dpp, optional_yield y) override
  {
    Prefix prefix{*dpp, "dbconfig:sqlite:zonegroup_remove "}; dpp = &prefix;

    if (!impl) {
      return -EINVAL; // can't write after conflict or delete
    }
    try {
      auto conn = impl->get(dpp);
      auto& stmt = conn->statements["zonegroup_del"];
      if (!stmt) {
        const std::string sql = fmt::format(schema::zonegroup_delete3,
                                            P1, P2, P3);
        stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
      auto binding = sqlite::stmt_binding{stmt.get()};
      sqlite::bind_text(dpp, binding, P1, zonegroup_id);
      sqlite::bind_int(dpp, binding, P2, ver);
      sqlite::bind_text(dpp, binding, P3, tag);

      auto reset = sqlite::stmt_execution{stmt.get()};
      sqlite::eval0(dpp, reset);

      impl = nullptr;
      if (!::sqlite3_changes(conn->db.get())) { // VersionNumber/Tag mismatch
        return -ECANCELED;
      }
    } catch (const sqlite::error& e) {
      ldpp_dout(dpp, 20) << "zonegroup delete failed: " << e.what() << dendl;
      if (e.code() == sqlite::errc::busy) {
        return -EBUSY;
      }
      return -EIO;
    }
    return 0;
  }
}; // SQLiteZoneGroupWriter


int SQLiteConfigStore::write_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                                  optional_yield y, bool exclusive,
                                                  std::string_view realm_id,
                                                  std::string_view zonegroup_id)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:write_default_zonegroup_id "}; dpp = &prefix;

  try {
    auto conn = impl->get(dpp);
    sqlite::stmt_ptr* stmt = nullptr;
    if (exclusive) {
      stmt = &conn->statements["def_zonegroup_ins"];
      if (!*stmt) {
        const std::string sql = fmt::format(schema::default_zonegroup_insert2,
                                            P1, P2);
        *stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
    } else {
      stmt = &conn->statements["def_zonegroup_ups"];
      if (!*stmt) {
        const std::string sql = fmt::format(schema::default_zonegroup_upsert2,
                                            P1, P2);
        *stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
    }
    auto binding = sqlite::stmt_binding{stmt->get()};
    bind_text_or_null(dpp, binding, P1, realm_id);
    sqlite::bind_text(dpp, binding, P2, zonegroup_id);

    auto reset = sqlite::stmt_execution{stmt->get()};
    sqlite::eval0(dpp, reset);
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "default zonegroup insert failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }
  return 0;
}

int SQLiteConfigStore::read_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                                 optional_yield y,
                                                 std::string_view realm_id,
                                                 std::string& zonegroup_id)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:read_default_zonegroup_id "}; dpp = &prefix;

  try {
    auto conn = impl->get(dpp);
    auto& stmt = conn->statements["def_zonegroup_sel"];
    if (!stmt) {
      const std::string sql = fmt::format(schema::default_zonegroup_select1, P1);
      stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
    }
    auto binding = sqlite::stmt_binding{stmt.get()};
    bind_text_or_null(dpp, binding, P1, realm_id);

    auto reset = sqlite::stmt_execution{stmt.get()};
    sqlite::eval1(dpp, reset);

    zonegroup_id = sqlite::column_text(reset, 0);
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "default zonegroup select failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::done) {
      return -ENOENT;
    } else if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }
  return 0;
}

int SQLiteConfigStore::delete_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                                   optional_yield y,
                                                   std::string_view realm_id)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:delete_default_zonegroup_id "}; dpp = &prefix;

  try {
    auto conn = impl->get(dpp);
    auto& stmt = conn->statements["def_zonegroup_del"];
    if (!stmt) {
      const std::string sql = fmt::format(schema::default_zonegroup_delete1, P1);
      stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
    }
    auto binding = sqlite::stmt_binding{stmt.get()};
    bind_text_or_null(dpp, binding, P1, realm_id);

    auto reset = sqlite::stmt_execution{stmt.get()};
    sqlite::eval0(dpp, reset);

    if (!::sqlite3_changes(conn->db.get())) {
      return -ENOENT;
    }
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "default zonegroup delete failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }
  return 0;
}


int SQLiteConfigStore::create_zonegroup(const DoutPrefixProvider* dpp,
                                        optional_yield y, bool exclusive,
                                        const RGWZoneGroup& info,
                                        std::unique_ptr<sal::ZoneGroupWriter>* writer)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:create_zonegroup "}; dpp = &prefix;

  if (info.id.empty()) {
    ldpp_dout(dpp, 0) << "zonegroup cannot have an empty id" << dendl;
    return -EINVAL;
  }
  if (info.name.empty()) {
    ldpp_dout(dpp, 0) << "zonegroup cannot have an empty name" << dendl;
    return -EINVAL;
  }

  int ver = 1;
  auto tag = generate_version_tag(dpp->get_cct());

  bufferlist bl;
  encode(info, bl);
  const auto data = std::string_view{bl.c_str(), bl.length()};

  try {
    auto conn = impl->get(dpp);
    sqlite::stmt_ptr* stmt = nullptr;
    if (exclusive) {
      stmt = &conn->statements["zonegroup_ins"];
      if (!*stmt) {
        const std::string sql = fmt::format(schema::zonegroup_insert6,
                                            P1, P2, P3, P4, P5, P6);
        *stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
    } else {
      stmt = &conn->statements["zonegroup_ups"];
      if (!*stmt) {
        const std::string sql = fmt::format(schema::zonegroup_upsert6,
                                            P1, P2, P3, P4, P5, P6);
        *stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
    }
    auto binding = sqlite::stmt_binding{stmt->get()};
    sqlite::bind_text(dpp, binding, P1, info.id);
    sqlite::bind_text(dpp, binding, P2, info.name);
    bind_text_or_null(dpp, binding, P3, info.realm_id);
    sqlite::bind_text(dpp, binding, P4, data);
    sqlite::bind_int(dpp, binding, P5, ver);
    sqlite::bind_text(dpp, binding, P6, tag);

    auto reset = sqlite::stmt_execution{stmt->get()};
    sqlite::eval0(dpp, reset);
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "zonegroup insert failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::foreign_key_constraint) {
      return -EINVAL; // refers to nonexistent RealmID
    } else if (e.code() == sqlite::errc::primary_key_constraint) {
      return -EEXIST; // ID already taken
    } else if (e.code() == sqlite::errc::unique_constraint) {
      return -EEXIST; // Name already taken
    } else if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }

  if (writer) {
    *writer = std::make_unique<SQLiteZoneGroupWriter>(
        impl.get(), ver, std::move(tag), info.id, info.name);
  }
  return 0;
}

int SQLiteConfigStore::read_zonegroup_by_id(const DoutPrefixProvider* dpp,
                                            optional_yield y,
                                            std::string_view zonegroup_id,
                                            RGWZoneGroup& info,
                                            std::unique_ptr<sal::ZoneGroupWriter>* writer)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:read_zonegroup_by_id "}; dpp = &prefix;

  if (zonegroup_id.empty()) {
    ldpp_dout(dpp, 0) << "requires a zonegroup id" << dendl;
    return -EINVAL;
  }

  ZoneGroupRow row;
  try {
    auto conn = impl->get(dpp);
    auto& stmt = conn->statements["zonegroup_sel_id"];
    if (!stmt) {
      const std::string sql = fmt::format(schema::zonegroup_select_id1, P1);
      stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
    }
    auto binding = sqlite::stmt_binding{stmt.get()};
    sqlite::bind_text(dpp, binding, P1, zonegroup_id);

    auto reset = sqlite::stmt_execution{stmt.get()};
    sqlite::eval1(dpp, reset);

    read_zonegroup_row(reset, row);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "zonegroup decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "zonegroup select failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::done) {
      return -ENOENT;
    } else if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }

  info = std::move(row.info);
  if (writer) {
    *writer = std::make_unique<SQLiteZoneGroupWriter>(
        impl.get(), row.ver, std::move(row.tag), info.id, info.name);
  }
  return 0;
}

int SQLiteConfigStore::read_zonegroup_by_name(const DoutPrefixProvider* dpp,
                                              optional_yield y,
                                              std::string_view zonegroup_name,
                                              RGWZoneGroup& info,
                                              std::unique_ptr<sal::ZoneGroupWriter>* writer)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:read_zonegroup_by_name "}; dpp = &prefix;

  if (zonegroup_name.empty()) {
    ldpp_dout(dpp, 0) << "requires a zonegroup name" << dendl;
    return -EINVAL;
  }

  ZoneGroupRow row;
  try {
    auto conn = impl->get(dpp);
    auto& stmt = conn->statements["zonegroup_sel_name"];
    if (!stmt) {
      const std::string sql = fmt::format(schema::zonegroup_select_name1, P1);
      stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
    }
    auto binding = sqlite::stmt_binding{stmt.get()};
    sqlite::bind_text(dpp, binding, P1, zonegroup_name);

    auto reset = sqlite::stmt_execution{stmt.get()};
    sqlite::eval1(dpp, reset);

    read_zonegroup_row(reset, row);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "zonegroup decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "zonegroup select failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::done) {
      return -ENOENT;
    } else if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }

  info = std::move(row.info);
  if (writer) {
    *writer = std::make_unique<SQLiteZoneGroupWriter>(
        impl.get(), row.ver, std::move(row.tag), info.id, info.name);
  }
  return 0;
}

int SQLiteConfigStore::read_default_zonegroup(const DoutPrefixProvider* dpp,
                                              optional_yield y,
                                              std::string_view realm_id,
                                              RGWZoneGroup& info,
                                              std::unique_ptr<sal::ZoneGroupWriter>* writer)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:read_default_zonegroup "}; dpp = &prefix;

  ZoneGroupRow row;
  try {
    auto conn = impl->get(dpp);
    auto& stmt = conn->statements["zonegroup_sel_def"];
    if (!stmt) {
      static constexpr std::string_view sql = schema::zonegroup_select_default0;
      stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
    }
    auto reset = sqlite::stmt_execution{stmt.get()};
    sqlite::eval1(dpp, reset);

    read_zonegroup_row(reset, row);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "zonegroup decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "zonegroup select failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::done) {
      return -ENOENT;
    } else if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }

  info = std::move(row.info);
  if (writer) {
    *writer = std::make_unique<SQLiteZoneGroupWriter>(
        impl.get(), row.ver, std::move(row.tag), info.id, info.name);
  }
  return 0;
}

int SQLiteConfigStore::list_zonegroup_names(const DoutPrefixProvider* dpp,
                                            optional_yield y,
                                            const std::string& marker,
                                            std::span<std::string> entries,
                                            sal::ListResult<std::string>& result)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:list_zonegroup_names "}; dpp = &prefix;

  try {
    auto conn = impl->get(dpp);
    auto& stmt = conn->statements["zonegroup_sel_names"];
    if (!stmt) {
      const std::string sql = fmt::format(schema::zonegroup_select_names2, P1, P2);
      stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
    }
    auto binding = sqlite::stmt_binding{stmt.get()};
    auto reset = sqlite::stmt_execution{stmt.get()};

    sqlite::bind_text(dpp, binding, P1, marker);
    sqlite::bind_int(dpp, binding, P2, entries.size());

    read_text_rows(dpp, reset, entries, result);
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "zonegroup select failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }
  return 0;
}


// Zone

class SQLiteZoneWriter : public sal::ZoneWriter {
  SQLiteImpl* impl;
  int ver;
  std::string tag;
  std::string zone_id;
  std::string zone_name;
 public:
  SQLiteZoneWriter(SQLiteImpl* impl, int ver, std::string tag,
                   std::string_view zone_id, std::string_view zone_name)
    : impl(impl), ver(ver), tag(std::move(tag)),
      zone_id(zone_id), zone_name(zone_name)
  {}

  int write(const DoutPrefixProvider* dpp, optional_yield y,
            const RGWZoneParams& info) override
  {
    Prefix prefix{*dpp, "dbconfig:sqlite:zone_write "}; dpp = &prefix;

    if (!impl) {
      return -EINVAL; // can't write after conflict or delete
    }
    if (zone_id != info.id || zone_name != info.name) {
      return -EINVAL; // can't modify zone id or name directly
    }

    bufferlist bl;
    encode(info, bl);
    const auto data = std::string_view{bl.c_str(), bl.length()};

    try {
      auto conn = impl->get(dpp);
      auto& stmt = conn->statements["zone_upd"];
      if (!stmt) {
        const std::string sql = fmt::format(schema::zone_update5,
                                            P1, P2, P3, P4, P5);
        stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
      auto binding = sqlite::stmt_binding{stmt.get()};
      sqlite::bind_text(dpp, binding, P1, info.id);
      bind_text_or_null(dpp, binding, P2, info.realm_id);
      sqlite::bind_text(dpp, binding, P3, data);
      sqlite::bind_int(dpp, binding, P4, ver);
      sqlite::bind_text(dpp, binding, P5, tag);

      auto reset = sqlite::stmt_execution{stmt.get()};
      sqlite::eval0(dpp, reset);

      if (!::sqlite3_changes(conn->db.get())) { // VersionNumber/Tag mismatch
        impl = nullptr;
        return -ECANCELED;
      }
    } catch (const sqlite::error& e) {
      ldpp_dout(dpp, 20) << "zone update failed: " << e.what() << dendl;
      if (e.code() == sqlite::errc::foreign_key_constraint) {
        return -EINVAL; // refers to nonexistent RealmID
      } else if (e.code() == sqlite::errc::busy) {
        return -EBUSY;
      }
      return -EIO;
    }
    ++ver;
    return 0;
  }

  int rename(const DoutPrefixProvider* dpp, optional_yield y,
             RGWZoneParams& info, std::string_view new_name) override
  {
    Prefix prefix{*dpp, "dbconfig:sqlite:zone_rename "}; dpp = &prefix;

    if (!impl) {
      return -EINVAL; // can't write after conflict or delete
    }
    if (zone_id != info.id || zone_name != info.name) {
      return -EINVAL; // can't modify zone id or name directly
    }
    if (new_name.empty()) {
      ldpp_dout(dpp, 0) << "zonegroup cannot have an empty name" << dendl;
      return -EINVAL;
    }

    try {
      auto conn = impl->get(dpp);
      auto& stmt = conn->statements["zone_rename"];
      if (!stmt) {
        const std::string sql = fmt::format(schema::zone_rename4, P1, P2, P2, P3);
        stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
      auto binding = sqlite::stmt_binding{stmt.get()};
      sqlite::bind_text(dpp, binding, P1, info.id);
      sqlite::bind_text(dpp, binding, P2, new_name);
      sqlite::bind_int(dpp, binding, P3, ver);
      sqlite::bind_text(dpp, binding, P4, tag);

      auto reset = sqlite::stmt_execution{stmt.get()};
      sqlite::eval0(dpp, reset);

      if (!::sqlite3_changes(conn->db.get())) { // VersionNumber/Tag mismatch
        impl = nullptr;
        return -ECANCELED;
      }
    } catch (const sqlite::error& e) {
      ldpp_dout(dpp, 20) << "zone rename failed: " << e.what() << dendl;
      if (e.code() == sqlite::errc::unique_constraint) {
        return -EEXIST; // Name already taken
      } else if (e.code() == sqlite::errc::busy) {
        return -EBUSY;
      }
      return -EIO;
    }
    info.name = std::string{new_name};
    ++ver;
    return 0;
  }

  int remove(const DoutPrefixProvider* dpp, optional_yield y) override
  {
    Prefix prefix{*dpp, "dbconfig:sqlite:zone_remove "}; dpp = &prefix;

    if (!impl) {
      return -EINVAL; // can't write after conflict or delete
    }
    try {
      auto conn = impl->get(dpp);
      auto& stmt = conn->statements["zone_del"];
      if (!stmt) {
        const std::string sql = fmt::format(schema::zone_delete3, P1, P2, P3);
        stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
      auto binding = sqlite::stmt_binding{stmt.get()};
      sqlite::bind_text(dpp, binding, P1, zone_id);
      sqlite::bind_int(dpp, binding, P2, ver);
      sqlite::bind_text(dpp, binding, P3, tag);

      auto reset = sqlite::stmt_execution{stmt.get()};
      sqlite::eval0(dpp, reset);

      impl = nullptr;
      if (!::sqlite3_changes(conn->db.get())) { // VersionNumber/Tag mismatch
        return -ECANCELED;
      }
    } catch (const sqlite::error& e) {
      ldpp_dout(dpp, 20) << "zone delete failed: " << e.what() << dendl;
      if (e.code() == sqlite::errc::busy) {
        return -EBUSY;
      }
      return -EIO;
    }
    return 0;
  }
}; // SQLiteZoneWriter


int SQLiteConfigStore::write_default_zone_id(const DoutPrefixProvider* dpp,
                                             optional_yield y, bool exclusive,
                                             std::string_view realm_id,
                                             std::string_view zone_id)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:write_default_zone_id "}; dpp = &prefix;

  if (zone_id.empty()) {
    ldpp_dout(dpp, 0) << "requires a zone id" << dendl;
    return -EINVAL;
  }

  try {
    auto conn = impl->get(dpp);
    sqlite::stmt_ptr* stmt = nullptr;
    if (exclusive) {
      stmt = &conn->statements["def_zone_ins"];
      if (!*stmt) {
        const std::string sql = fmt::format(schema::default_zone_insert2, P1, P2);
        *stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
    } else {
      stmt = &conn->statements["def_zone_ups"];
      if (!*stmt) {
        const std::string sql = fmt::format(schema::default_zone_upsert2, P1, P2);
        *stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
    }
    auto binding = sqlite::stmt_binding{stmt->get()};
    bind_text_or_null(dpp, binding, P1, realm_id);
    sqlite::bind_text(dpp, binding, P2, zone_id);

    auto reset = sqlite::stmt_execution{stmt->get()};
    sqlite::eval0(dpp, reset);
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "default zone insert failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }
  return 0;
}

int SQLiteConfigStore::read_default_zone_id(const DoutPrefixProvider* dpp,
                                            optional_yield y,
                                            std::string_view realm_id,
                                            std::string& zone_id)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:read_default_zone_id "}; dpp = &prefix;

  try {
    auto conn = impl->get(dpp);
    auto& stmt = conn->statements["def_zone_sel"];
    if (!stmt) {
      const std::string sql = fmt::format(schema::default_zone_select1, P1);
      stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
    }
    auto binding = sqlite::stmt_binding{stmt.get()};
    bind_text_or_null(dpp, binding, P1, realm_id);

    auto reset = sqlite::stmt_execution{stmt.get()};
    sqlite::eval1(dpp, reset);

    zone_id = sqlite::column_text(reset, 0);
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "default zone select failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::done) {
      return -ENOENT;
    } else if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }
  return 0;
}

int SQLiteConfigStore::delete_default_zone_id(const DoutPrefixProvider* dpp,
                                              optional_yield y,
                                              std::string_view realm_id)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:delete_default_zone_id "}; dpp = &prefix;

  try {
    auto conn = impl->get(dpp);
    auto& stmt = conn->statements["def_zone_del"];
    if (!stmt) {
      const std::string sql = fmt::format(schema::default_zone_delete1, P1);
      stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
    }
    auto binding = sqlite::stmt_binding{stmt.get()};
    bind_text_or_null(dpp, binding, P1, realm_id);

    auto reset = sqlite::stmt_execution{stmt.get()};
    sqlite::eval0(dpp, reset);

    if (!::sqlite3_changes(conn->db.get())) {
      return -ENOENT;
    }
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "default zone delete failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }
  return 0;
}


int SQLiteConfigStore::create_zone(const DoutPrefixProvider* dpp,
                                   optional_yield y, bool exclusive,
                                   const RGWZoneParams& info,
                                   std::unique_ptr<sal::ZoneWriter>* writer)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:create_zone "}; dpp = &prefix;

  if (info.id.empty()) {
    ldpp_dout(dpp, 0) << "zone cannot have an empty id" << dendl;
    return -EINVAL;
  }
  if (info.name.empty()) {
    ldpp_dout(dpp, 0) << "zone cannot have an empty name" << dendl;
    return -EINVAL;
  }

  int ver = 1;
  auto tag = generate_version_tag(dpp->get_cct());

  bufferlist bl;
  encode(info, bl);
  const auto data = std::string_view{bl.c_str(), bl.length()};

  try {
    auto conn = impl->get(dpp);
    sqlite::stmt_ptr* stmt = nullptr;
    if (exclusive) {
      stmt = &conn->statements["zone_ins"];
      if (!*stmt) {
        const std::string sql = fmt::format(schema::zone_insert6,
                                            P1, P2, P3, P4, P5, P6);
        *stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
    } else {
      stmt = &conn->statements["zone_ups"];
      if (!*stmt) {
        const std::string sql = fmt::format(schema::zone_upsert6,
                                            P1, P2, P3, P4, P5, P6);
        *stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
    }
    auto binding = sqlite::stmt_binding{stmt->get()};
    sqlite::bind_text(dpp, binding, P1, info.id);
    sqlite::bind_text(dpp, binding, P2, info.name);
    bind_text_or_null(dpp, binding, P3, info.realm_id);
    sqlite::bind_text(dpp, binding, P4, data);
    sqlite::bind_int(dpp, binding, P5, ver);
    sqlite::bind_text(dpp, binding, P6, tag);

    auto reset = sqlite::stmt_execution{stmt->get()};
    sqlite::eval0(dpp, reset);
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "zone insert failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::foreign_key_constraint) {
      return -EINVAL; // refers to nonexistent RealmID
    } else if (e.code() == sqlite::errc::primary_key_constraint) {
      return -EEXIST; // ID already taken
    } else if (e.code() == sqlite::errc::unique_constraint) {
      return -EEXIST; // Name already taken
    } else if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }

  if (writer) {
    *writer = std::make_unique<SQLiteZoneWriter>(
        impl.get(), ver, std::move(tag), info.id, info.name);
  }
  return 0;
}

int SQLiteConfigStore::read_zone_by_id(const DoutPrefixProvider* dpp,
                                       optional_yield y,
                                       std::string_view zone_id,
                                       RGWZoneParams& info,
                                       std::unique_ptr<sal::ZoneWriter>* writer)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:read_zone_by_id "}; dpp = &prefix;

  if (zone_id.empty()) {
    ldpp_dout(dpp, 0) << "requires a zone id" << dendl;
    return -EINVAL;
  }

  ZoneRow row;
  try {
    auto conn = impl->get(dpp);
    auto& stmt = conn->statements["zone_sel_id"];
    if (!stmt) {
      const std::string sql = fmt::format(schema::zone_select_id1, P1);
      stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
    }
    auto binding = sqlite::stmt_binding{stmt.get()};
    sqlite::bind_text(dpp, binding, P1, zone_id);

    auto reset = sqlite::stmt_execution{stmt.get()};
    sqlite::eval1(dpp, reset);

    read_zone_row(reset, row);
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "zone select failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::done) {
      return -ENOENT;
    } else if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }

  info = std::move(row.info);
  if (writer) {
    *writer = std::make_unique<SQLiteZoneWriter>(
        impl.get(), row.ver, std::move(row.tag), info.id, info.name);
  }
  return 0;
}

int SQLiteConfigStore::read_zone_by_name(const DoutPrefixProvider* dpp,
                                         optional_yield y,
                                         std::string_view zone_name,
                                         RGWZoneParams& info,
                                         std::unique_ptr<sal::ZoneWriter>* writer)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:read_zone_by_name "}; dpp = &prefix;

  if (zone_name.empty()) {
    ldpp_dout(dpp, 0) << "requires a zone name" << dendl;
    return -EINVAL;
  }

  ZoneRow row;
  try {
    auto conn = impl->get(dpp);
    auto& stmt = conn->statements["zone_sel_name"];
    if (!stmt) {
      const std::string sql = fmt::format(schema::zone_select_name1, P1);
      stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
    }
    auto binding = sqlite::stmt_binding{stmt.get()};
    sqlite::bind_text(dpp, binding, P1, zone_name);

    auto reset = sqlite::stmt_execution{stmt.get()};
    sqlite::eval1(dpp, reset);

    read_zone_row(reset, row);
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "zone select failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::done) {
      return -ENOENT;
    } else if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }

  info = std::move(row.info);
  if (writer) {
    *writer = std::make_unique<SQLiteZoneWriter>(
        impl.get(), row.ver, std::move(row.tag), info.id, info.name);
  }
  return 0;
}

int SQLiteConfigStore::read_default_zone(const DoutPrefixProvider* dpp,
                                         optional_yield y,
                                         std::string_view realm_id,
                                         RGWZoneParams& info,
                                         std::unique_ptr<sal::ZoneWriter>* writer)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:read_default_zone "}; dpp = &prefix;

  ZoneRow row;
  try {
    auto conn = impl->get(dpp);
    auto& stmt = conn->statements["zone_sel_def"];
    if (!stmt) {
      static constexpr std::string_view sql = schema::zone_select_default0;
      stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
    }
    auto reset = sqlite::stmt_execution{stmt.get()};
    sqlite::eval1(dpp, reset);

    read_zone_row(reset, row);
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "zone select failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::done) {
      return -ENOENT;
    } else if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }

  info = std::move(row.info);
  if (writer) {
    *writer = std::make_unique<SQLiteZoneWriter>(
        impl.get(), row.ver, std::move(row.tag), info.id, info.name);
  }
  return 0;
}

int SQLiteConfigStore::list_zone_names(const DoutPrefixProvider* dpp,
                                       optional_yield y,
                                       const std::string& marker,
                                       std::span<std::string> entries,
                                       sal::ListResult<std::string>& result)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:list_zone_names "}; dpp = &prefix;

  try {
    auto conn = impl->get(dpp);
    auto& stmt = conn->statements["zone_sel_names"];
    if (!stmt) {
      const std::string sql = fmt::format(schema::zone_select_names2, P1, P2);
      stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
    }
    auto binding = sqlite::stmt_binding{stmt.get()};
    sqlite::bind_text(dpp, binding, P1, marker);
    sqlite::bind_int(dpp, binding, P2, entries.size());

    auto reset = sqlite::stmt_execution{stmt.get()};
    read_text_rows(dpp, reset, entries, result);
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "zone select failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }
  return 0;
}


// PeriodConfig

int SQLiteConfigStore::read_period_config(const DoutPrefixProvider* dpp,
                                          optional_yield y,
                                          std::string_view realm_id,
                                          RGWPeriodConfig& info)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:read_period_config "}; dpp = &prefix;

  try {
    auto conn = impl->get(dpp);
    auto& stmt = conn->statements["period_conf_sel"];
    if (!stmt) {
      const std::string sql = fmt::format(schema::period_config_select1, P1);
      stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
    }
    auto binding = sqlite::stmt_binding{stmt.get()};
    sqlite::bind_text(dpp, binding, P1, realm_id);

    auto reset = sqlite::stmt_execution{stmt.get()};
    sqlite::eval1(dpp, reset);

    std::string data = sqlite::column_text(reset, 0);
    bufferlist bl = bufferlist::static_from_string(data);
    auto p = bl.cbegin();
    decode(info, p);

  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "period config decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "period config select failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::done) {
      return -ENOENT;
    } else if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }
  return 0;
}

int SQLiteConfigStore::write_period_config(const DoutPrefixProvider* dpp,
                                           optional_yield y, bool exclusive,
                                           std::string_view realm_id,
                                           const RGWPeriodConfig& info)
{
  Prefix prefix{*dpp, "dbconfig:sqlite:write_period_config "}; dpp = &prefix;

  bufferlist bl;
  encode(info, bl);
  const auto data = std::string_view{bl.c_str(), bl.length()};

  try {
    auto conn = impl->get(dpp);
    sqlite::stmt_ptr* stmt = nullptr;
    if (exclusive) {
      stmt = &conn->statements["period_conf_ins"];
      if (!*stmt) {
        const std::string sql = fmt::format(schema::period_config_insert2, P1, P2);
        *stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
    } else {
      stmt = &conn->statements["period_conf_ups"];
      if (!*stmt) {
        const std::string sql = fmt::format(schema::period_config_upsert2, P1, P2);
        *stmt = sqlite::prepare_statement(dpp, conn->db.get(), sql);
      }
    }
    auto binding = sqlite::stmt_binding{stmt->get()};
    sqlite::bind_text(dpp, binding, P1, realm_id);
    sqlite::bind_text(dpp, binding, P2, data);

    auto reset = sqlite::stmt_execution{stmt->get()};
    sqlite::eval0(dpp, reset);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 20) << "period config decode failed: " << e.what() << dendl;
    return -EIO;
  } catch (const sqlite::error& e) {
    ldpp_dout(dpp, 20) << "period config insert failed: " << e.what() << dendl;
    if (e.code() == sqlite::errc::primary_key_constraint) {
      return -EEXIST;
    } else if (e.code() == sqlite::errc::busy) {
      return -EBUSY;
    }
    return -EIO;
  }
  return 0;
}

namespace {

int version_cb(void* user, int count, char** values, char** names)
{
  if (count != 1) {
    return EINVAL;
  }
  std::string_view name = names[0];
  if (name != "user_version") {
    return EINVAL;
  }
  std::string_view value = values[0];
  auto result = std::from_chars(value.begin(), value.end(),
                                *reinterpret_cast<uint32_t*>(user));
  if (result.ec != std::errc{}) {
    return static_cast<int>(result.ec);
  }
  return 0;
}

void apply_schema_migrations(const DoutPrefixProvider* dpp, sqlite3* db)
{
  sqlite::execute(dpp, db, "PRAGMA foreign_keys = ON", nullptr, nullptr);

  // initiate a transaction and read the current schema version
  uint32_t version = 0;
  sqlite::execute(dpp, db, "BEGIN; PRAGMA user_version", version_cb, &version);

  const uint32_t initial_version = version;
  ldpp_dout(dpp, 4) << "current schema version " << version << dendl;

  // use the version as an index into schema::migrations
  auto m = std::next(schema::migrations.begin(), version);

  for (; m != schema::migrations.end(); ++m, ++version) {
    try {
      sqlite::execute(dpp, db, m->up, nullptr, nullptr);
    } catch (const sqlite::error&) {
      ldpp_dout(dpp, -1) << "ERROR: schema migration failed on v" << version
          << ": " << m->description << dendl;
      throw;
    }
  }

  if (version > initial_version) {
    // update the user_version and commit the transaction
    const auto commit = fmt::format("PRAGMA user_version = {}; COMMIT", version);
    sqlite::execute(dpp, db, commit.c_str(), nullptr, nullptr);

    ldpp_dout(dpp, 4) << "upgraded database schema to version " << version << dendl;
  } else {
    // nothing to commit
    sqlite::execute(dpp, db, "ROLLBACK", nullptr, nullptr);
  }
}

} // anonymous namespace


auto create_sqlite_store(const DoutPrefixProvider* dpp, const std::string& uri)
  -> std::unique_ptr<config::SQLiteConfigStore>
{
  Prefix prefix{*dpp, "dbconfig:sqlite:create_sqlite_store "}; dpp = &prefix;

  // build the connection pool
  int flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_URI | SQLITE_OPEN_READWRITE |
      SQLITE_OPEN_NOMUTEX;
  auto factory = sqlite::ConnectionFactory{uri, flags};

  // sqlite does not support concurrent writers. we enforce this limitation by
  // using a connection pool of size=1
  static constexpr size_t max_connections = 1;
  auto impl = std::make_unique<SQLiteImpl>(std::move(factory), max_connections);

  // open a connection to apply schema migrations
  auto conn = impl->get(dpp);
  apply_schema_migrations(dpp, conn->db.get());

  return std::make_unique<SQLiteConfigStore>(std::move(impl));
}

} // namespace rgw::dbstore::config
