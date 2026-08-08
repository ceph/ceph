// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License version 2.1, as published by
 * the Free Software Foundation.  See file COPYING.
 *
 */

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>

#include <stdlib.h>
#include <string.h>

#include <sqlite3.h>
#include <fmt/format.h>
#include "gtest/gtest.h"

#include "include/uuid.h"
#include "include/rados/librados.hpp"
#include "include/libcephsqlite.h"
#include "SimpleRADOSStriper.h"

#include "common/ceph_argparse.h"
#include "common/ceph_crypto.h"
#include "common/ceph_time.h"
#include "common/common_init.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_client
#undef dout_prefix
#define dout_prefix *_dout << "unittest_libcephsqlite: "

#define sqlcatchcode(S, code) \
do {\
    rc = S;\
    if (rc != code) {\
        std::cout << "[" << __FILE__ << ":" << __LINE__ << "]"\
                  << " sqlite3 error: " << rc << " `" << sqlite3_errstr(rc)\
                  << "': " << sqlite3_errmsg(db) << std::endl;\
        sqlite3_finalize(stmt);\
        stmt = NULL;\
        goto out;\
    }\
} while (0)

#define sqlcatch(S) sqlcatchcode(S, SQLITE_OK)

static boost::intrusive_ptr<CephContext> cct;

class CephSQLiteTest : public ::testing::Test {
public:
  inline static const std::string pool = "cephsqlite";

  static void SetUpTestSuite() {
    librados::Rados cluster;
    ASSERT_LE(0, cluster.init_with_context(cct.get()));
    ASSERT_LE(0, cluster.connect());
    if (int rc = cluster.pool_create(pool.c_str()); rc < 0 && rc != -EEXIST) {
      ASSERT_EQ(0, rc);
    }
    cluster.shutdown();
    sleep(5);
  }
  void SetUp() override {
    uuid.generate_random();
    ASSERT_LE(0, cluster.init_with_context(cct.get()));
    ASSERT_LE(0, cluster.connect());
    ASSERT_LE(0, cluster.wait_for_latest_osdmap());
    ASSERT_EQ(0, db_open());
  }
  void TearDown() override {
    ASSERT_EQ(SQLITE_OK, sqlite3_close(db));
    db = nullptr;
    cluster.shutdown();
    /* Leave database behind for inspection. */
  }

protected:
  int db_open()
  {
    static const char SQL[] =
      "PRAGMA journal_mode = PERSIST;"
      "PRAGMA page_size = 65536;"
      "PRAGMA cache_size = 32768;"
      "PRAGMA temp_store = memory;"
      "CREATE TEMPORARY TABLE perf (i INTEGER PRIMARY KEY, v TEXT);"
      "INSERT INTO perf (v)"
      "    VALUES (ceph_perf());"
      ;

    sqlite3_stmt *stmt = NULL;
    const char *current = SQL;
    int rc;

    auto&& name = get_uri();
    sqlcatch(sqlite3_open_v2(name.c_str(), &db, SQLITE_OPEN_CREATE|SQLITE_OPEN_READWRITE|SQLITE_OPEN_URI, "ceph"));
    std::cout << "using database: " << name << std::endl;

    std::cout << SQL << std::endl;
    sqlcatch(sqlite3_exec(db, current, NULL, NULL, NULL));

    rc = 0;
out:
    sqlite3_finalize(stmt);
    return rc;
  }

  virtual std::string get_uri() const {
    auto uri = fmt::format("file:{}:/{}?vfs=ceph", pool, get_name());
    return uri;
  }
  virtual std::string get_name() const {
    auto name = fmt::format("{}.db", uuid.to_string());
    return name;
  }

  sqlite3* db = nullptr;
  uuid_d uuid;
  librados::Rados cluster;
};

TEST_F(CephSQLiteTest, Create) {
  static const char SQL[] =
    "CREATE TABLE foo (a INT);"
    ;

  sqlite3_stmt *stmt = NULL;
  const char *current = SQL;
  int rc;

  std::cout << SQL << std::endl;
  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  rc = 0;

out:
  sqlite3_finalize(stmt);
  ASSERT_EQ(0, rc);
}

TEST_F(CephSQLiteTest, InsertBulk4096) {
  static const char SQL[] =
    "PRAGMA page_size = 4096;"
    "CREATE TABLE foo (a INT);"
    "WITH RECURSIVE c(x) AS"
    "  ("
    "   VALUES(1)"
    "  UNION ALL"
    "   SELECT x+1"
    "   FROM c"
    "  )"
    "INSERT INTO foo (a)"
    "  SELECT RANDOM()"
    "  FROM c"
    "  LIMIT 1000000;"
    "PRAGMA page_size;"
    ;

    int rc;
    const char *current = SQL;
    sqlite3_stmt *stmt = NULL;

    std::cout << SQL << std::endl;
    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
    ASSERT_EQ(sqlite3_column_int64(stmt, 0), 4096);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    rc = 0;
out:
    sqlite3_finalize(stmt);
    ASSERT_EQ(0, rc);
}

TEST_F(CephSQLiteTest, InsertBulk) {
  static const char SQL[] =
    "CREATE TABLE foo (a INT);"
    "WITH RECURSIVE c(x) AS"
    "  ("
    "   VALUES(1)"
    "  UNION ALL"
    "   SELECT x+1"
    "   FROM c"
    "  )"
    "INSERT INTO foo (a)"
    "  SELECT RANDOM()"
    "  FROM c"
    "  LIMIT 1000000;"
    ;

    int rc;
    const char *current = SQL;
    sqlite3_stmt *stmt = NULL;

    std::cout << SQL << std::endl;
    sqlcatch(sqlite3_exec(db, current, NULL, NULL, NULL));
    rc = 0;
out:
    sqlite3_finalize(stmt);
    ASSERT_EQ(0, rc);
}

TEST_F(CephSQLiteTest, UpdateBulk) {
  static const char SQL[] =
    "CREATE TABLE foo (a INT);"
    "WITH RECURSIVE c(x) AS"
    "  ("
    "   VALUES(1)"
    "  UNION ALL"
    "   SELECT x+1"
    "   FROM c"
    "  )"
    "INSERT INTO foo (a)"
    "  SELECT x"
    "  FROM c"
    "  LIMIT 1000000;"
    "SELECT SUM(a) FROM foo;"
    "UPDATE foo"
    "  SET a = a+a;"
    "SELECT SUM(a) FROM foo;"
    ;

    int rc;
    const char *current = SQL;
    sqlite3_stmt *stmt = NULL;
    uint64_t sum, sum2;

    std::cout << SQL << std::endl;
    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
    sum = sqlite3_column_int64(stmt, 0);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
    sum2 = sqlite3_column_int64(stmt, 0);
    ASSERT_EQ(sum*2, sum2);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    rc = 0;
out:
    sqlite3_finalize(stmt);
    ASSERT_EQ(0, rc);
}

TEST_F(CephSQLiteTest, InsertRate) {
  using clock = ceph::coarse_mono_clock;
  using time = ceph::coarse_mono_time;

  static const char SQL[] =
    "CREATE TABLE foo (a INT);"
    "INSERT INTO foo (a) VALUES (RANDOM());"
    ;

  int rc;
  const char *current = SQL;
  sqlite3_stmt *stmt = NULL;
  time t1, t2;
  int count = 100;

  std::cout << SQL << std::endl;
  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  t1 = clock::now();
  for (int i = 0; i < count; ++i) {
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  }
  t2 = clock::now();
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  {
    auto diff = std::chrono::duration<double>(t2-t1);
    std::cout << "transactions per second: " << count/diff.count() << std::endl;
  }

  rc = 0;
out:
  sqlite3_finalize(stmt);
  ASSERT_EQ(0, rc);
}

TEST_F(CephSQLiteTest, DatabaseShrink) {
  static const char SQL[] =
    "CREATE TABLE foo (a INT);"
    "WITH RECURSIVE c(x) AS"
    "  ("
    "   VALUES(1)"
    "  UNION ALL"
    "   SELECT x+1"
    "   FROM c"
    "  )"
    "INSERT INTO foo (a)"
    "  SELECT x"
    "  FROM c"
    "  LIMIT 1000000;"
    "DELETE FROM foo"
    "  WHERE RANDOM()%4 < 3;"
    "VACUUM;"
    ;

  int rc;
  const char *current = SQL;
  sqlite3_stmt *stmt = NULL;
  librados::IoCtx ioctx;
  std::unique_ptr<SimpleRADOSStriper> rs;
  uint64_t size1, size2;

  std::cout << SQL << std::endl;

  ASSERT_EQ(0, cluster.ioctx_create(pool.c_str(), ioctx));
  rs = std::make_unique<SimpleRADOSStriper>(ioctx, get_name());

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  ASSERT_EQ(0, rs->open());
  ASSERT_EQ(0, rs->lock(SimpleRADOSStriper::LockLevel::Exclusive));
  ASSERT_EQ(0, rs->stat(&size1));
  ASSERT_EQ(0, rs->unlock(SimpleRADOSStriper::LockLevel::None));
  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
  ASSERT_EQ(0, rs->lock(SimpleRADOSStriper::LockLevel::Exclusive));
  ASSERT_EQ(0, rs->stat(&size2));
  ASSERT_EQ(0, rs->unlock(SimpleRADOSStriper::LockLevel::None));
  ASSERT_LT(size2, size1/2);

  rc = 0;
out:
  sqlite3_finalize(stmt);
  ASSERT_EQ(0, rc);
}

TEST_F(CephSQLiteTest, InsertExclusiveRate) {
  using clock = ceph::coarse_mono_clock;
  using time = ceph::coarse_mono_time;

  static const char SQL[] =
    "PRAGMA locking_mode=EXCLUSIVE;"
    "CREATE TABLE foo (a INT);"
    "INSERT INTO foo (a) VALUES (RANDOM());"
    ;

  int rc;
  const char *current = SQL;
  sqlite3_stmt *stmt = NULL;
  time t1, t2;
  int count = 100;

  std::cout << SQL << std::endl;

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  t1 = clock::now();
  for (int i = 0; i < count; ++i) {
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  }
  t2 = clock::now();
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  {
    auto diff = std::chrono::duration<double>(t2-t1);
    std::cout << "transactions per second: " << count/diff.count() << std::endl;
  }

  rc = 0;
out:
  sqlite3_finalize(stmt);
  ASSERT_EQ(0, rc);
}

TEST_F(CephSQLiteTest, InsertExclusiveWALRate) {
  using clock = ceph::coarse_mono_clock;
  using time = ceph::coarse_mono_time;

  static const char SQL[] =
    "PRAGMA locking_mode=EXCLUSIVE;"
    "PRAGMA journal_mode=WAL;"
    "CREATE TABLE foo (a INT);"
    "INSERT INTO foo (a) VALUES (RANDOM());"
    ;

  int rc;
  const char *current = SQL;
  sqlite3_stmt *stmt = NULL;
  time t1, t2;
  int count = 100;

  std::cout << SQL << std::endl;

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  t1 = clock::now();
  for (int i = 0; i < count; ++i) {
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  }
  t2 = clock::now();
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  {
    auto diff = std::chrono::duration<double>(t2-t1);
    std::cout << "transactions per second: " << count/diff.count() << std::endl;
  }

  rc = 0;
out:
  sqlite3_finalize(stmt);
  ASSERT_EQ(0, rc);
}

TEST_F(CephSQLiteTest, WALTransactionSync) {
  static const char SQL[] =
    "PRAGMA locking_mode=EXCLUSIVE;"
    "PRAGMA journal_mode=WAL;"
    "CREATE TABLE foo (a INT);" /* sets up the -wal journal */
    "INSERT INTO perf (v)"
    "    VALUES (ceph_perf());"
    "BEGIN TRANSACTION;"
    "INSERT INTO foo (a) VALUES (RANDOM());"
    "END TRANSACTION;"
    "INSERT INTO perf (v)"
    "    VALUES (ceph_perf());"
    "SELECT json_extract(a.v, '$.libcephsqlite_vfs.opf_sync.avgcount') - "
    "       json_extract(b.v, '$.libcephsqlite_vfs.opf_sync.avgcount') "
    "    FROM perf AS a, perf AS b"
    "    WHERE a.i = ? AND b.i = ?;"
    ;

  int rc;
  const char *current = SQL;
  sqlite3_stmt *stmt = NULL;
  uint64_t id;

  std::cout << SQL << std::endl;

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  for (int i = 0; i < 10; i++) {
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  }
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  id = sqlite3_last_insert_rowid(db);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatch(sqlite3_bind_int64(stmt, 1, id));
  sqlcatch(sqlite3_bind_int64(stmt, 2, id-1));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
  ASSERT_EQ(sqlite3_column_int64(stmt, 0), 1);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  rc = 0;
out:
  sqlite3_finalize(stmt);
  ASSERT_EQ(0, rc);
}

TEST_F(CephSQLiteTest, PersistTransactionSync) {
  static const char SQL[] =
    "BEGIN TRANSACTION;"
    "CREATE TABLE foo (a INT);"
    "INSERT INTO foo (a) VALUES (RANDOM());"
    "END TRANSACTION;"
    "INSERT INTO perf (v)"
    "    VALUES (ceph_perf());"
    "SELECT json_extract(a.v, '$.libcephsqlite_vfs.opf_sync.avgcount') - "
    "       json_extract(b.v, '$.libcephsqlite_vfs.opf_sync.avgcount') "
    "    FROM perf AS a, perf AS b"
    "    WHERE a.i = ? AND b.i = ?;"
    ;

  int rc;
  const char *current = SQL;
  sqlite3_stmt *stmt = NULL;
  uint64_t id;

  std::cout << SQL << std::endl;

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  id = sqlite3_last_insert_rowid(db);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatch(sqlite3_bind_int64(stmt, 1, id));
  sqlcatch(sqlite3_bind_int64(stmt, 2, id-1));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
  ASSERT_EQ(sqlite3_column_int64(stmt, 0), 3); /* journal, db, journal header (PERIST) */
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  rc = 0;
out:
  sqlite3_finalize(stmt);
  ASSERT_EQ(0, rc);
}

TEST_F(CephSQLiteTest, InsertExclusiveLock) {
  static const char SQL[] =
    "PRAGMA locking_mode=EXCLUSIVE;"
    "CREATE TABLE foo (a INT);"
    "INSERT INTO foo (a) VALUES (RANDOM());"
    "INSERT INTO perf (v)"
    "    VALUES (ceph_perf());"
    "SELECT json_extract(a.v, '$.libcephsqlite_vfs.opf_lock.avgcount'), "
    "       json_extract(b.v, '$.libcephsqlite_vfs.opf_lock.avgcount'), "
    "       json_extract(a.v, '$.libcephsqlite_vfs.opf_lock.avgcount') - "
    "       json_extract(b.v, '$.libcephsqlite_vfs.opf_lock.avgcount') "
    "    FROM perf AS a, perf AS b"
    "    WHERE a.i = ? AND b.i = ?;"
    "SELECT json_extract(a.v, '$.libcephsqlite_striper.lock'), "
    "       json_extract(b.v, '$.libcephsqlite_striper.lock'), "
    "       json_extract(a.v, '$.libcephsqlite_striper.lock') - "
    "       json_extract(b.v, '$.libcephsqlite_striper.lock') "
    "    FROM perf AS a, perf AS b"
    "    WHERE a.i = ? AND b.i = ?;"
    ;

  int rc;
  const char *current = SQL;
  sqlite3_stmt *stmt = NULL;
  uint64_t id;

  std::cout << SQL << std::endl;

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  id = sqlite3_last_insert_rowid(db);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatch(sqlite3_bind_int64(stmt, 1, id));
  sqlcatch(sqlite3_bind_int64(stmt, 2, id-1));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
  ASSERT_GT(sqlite3_column_int64(stmt, 0), 0);
  ASSERT_GT(sqlite3_column_int64(stmt, 1), 0);
  ASSERT_EQ(sqlite3_column_int64(stmt, 2), 3); /* NONE -> SHARED; SHARED -> RESERVED; RESERVED -> EXCLUSIVE */
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatch(sqlite3_bind_int64(stmt, 1, id));
  sqlcatch(sqlite3_bind_int64(stmt, 2, id-1));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
  ASSERT_GT(sqlite3_column_int64(stmt, 0), 0);
  ASSERT_GT(sqlite3_column_int64(stmt, 1), 0);
  ASSERT_EQ(sqlite3_column_int64(stmt, 2), 3); /* NONE -> SHARED -> RESERVED -> EXCLUSIVE */
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  rc = 0;
out:
  sqlite3_finalize(stmt);
  ASSERT_EQ(0, rc);
}

TEST_F(CephSQLiteTest, TransactionSizeUpdate) {
  static const char SQL[] =
    "BEGIN TRANSACTION;"
    "CREATE TABLE foo (a INT);"
    "INSERT INTO foo (a) VALUES (RANDOM());"
    "END TRANSACTION;"
    "INSERT INTO perf (v)"
    "    VALUES (ceph_perf());"
    "SELECT json_extract(a.v, '$.libcephsqlite_striper.update_size'), "
    "       json_extract(b.v, '$.libcephsqlite_striper.update_size'), "
    "       json_extract(a.v, '$.libcephsqlite_striper.update_size') - "
    "       json_extract(b.v, '$.libcephsqlite_striper.update_size') "
    "    FROM perf AS a, perf AS b"
    "    WHERE a.i = ? AND b.i = ?;"
    ;

  int rc;
  const char *current = SQL;
  sqlite3_stmt *stmt = NULL;
  uint64_t id;

  std::cout << SQL << std::endl;

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  id = sqlite3_last_insert_rowid(db);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatch(sqlite3_bind_int64(stmt, 1, id));
  sqlcatch(sqlite3_bind_int64(stmt, 2, id-1));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
  ASSERT_GT(sqlite3_column_int64(stmt, 0), 0);
  ASSERT_GT(sqlite3_column_int64(stmt, 1), 0);
  ASSERT_EQ(sqlite3_column_int64(stmt, 2), 2); /* once for journal write and db write (but not journal header clear!) */
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  rc = 0;
out:
  sqlite3_finalize(stmt);
  ASSERT_EQ(0, rc);
}

TEST_F(CephSQLiteTest, AllocatedGrowth) {
  static const char SQL[] =
    "CREATE TABLE foo (a BLOB);"
    "WITH RECURSIVE c(x) AS"
    "  ("
    "   VALUES(1)"
    "  UNION ALL"
    "   SELECT x+1"
    "   FROM c"
    "  )"
    "INSERT INTO foo (a)"
    "  SELECT RANDOMBLOB(1<<20)"
    "  FROM c"
    "  LIMIT 1024;"
    "INSERT INTO perf (v)"
    "    VALUES (ceph_perf());"
    "SELECT json_extract(a.v, '$.libcephsqlite_striper.update_allocated'), "
    "       json_extract(b.v, '$.libcephsqlite_striper.update_allocated'), "
    "       json_extract(a.v, '$.libcephsqlite_striper.update_allocated') - "
    "       json_extract(b.v, '$.libcephsqlite_striper.update_allocated') "
    "    FROM perf AS a, perf AS b"
    "    WHERE a.i = ? AND b.i = ?;"
    ;

  int rc;
  const char *current = SQL;
  sqlite3_stmt *stmt = NULL;
  uint64_t id;

  std::cout << SQL << std::endl;

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  id = sqlite3_last_insert_rowid(db);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatch(sqlite3_bind_int64(stmt, 1, id));
  sqlcatch(sqlite3_bind_int64(stmt, 2, id-1));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
  ASSERT_GT(sqlite3_column_int64(stmt, 2), 8); /* max_growth = 128MB, 1024MB of data */
  ASSERT_LT(sqlite3_column_int64(stmt, 2), 12);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  rc = 0;
out:
  sqlite3_finalize(stmt);
  ASSERT_EQ(0, rc);
}


TEST_F(CephSQLiteTest, DeleteBulk) {
  static const char SQL[] =
    "CREATE TABLE foo (a INT);"
    "WITH RECURSIVE c(x) AS"
    "  ("
    "   VALUES(1)"
    "  UNION ALL"
    "   SELECT x+1"
    "   FROM c"
    "  )"
    "INSERT INTO foo (a)"
    "  SELECT x"
    "  FROM c"
    "  LIMIT 1000000;"
    "DELETE FROM foo"
    "  WHERE RANDOM()%2 == 0;"
    ;

    int rc;
    const char *current = SQL;
    sqlite3_stmt *stmt = NULL;

    std::cout << SQL << std::endl;
    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    rc = 0;
out:
    sqlite3_finalize(stmt);
    ASSERT_EQ(0, rc);
}

TEST_F(CephSQLiteTest, DropMassive) {
  static const char SQL[] =
    "CREATE TABLE foo (a BLOB);"
    "WITH RECURSIVE c(x) AS"
    "  ("
    "   VALUES(1)"
    "  UNION ALL"
    "   SELECT x+1"
    "   FROM c"
    "  )"
    "INSERT INTO foo (a)"
    "  SELECT RANDOMBLOB(1<<20)"
    "  FROM c"
    "  LIMIT 1024;"
    "DROP TABLE foo;"
    "VACUUM;"
    "INSERT INTO perf (v)"
    "    VALUES (ceph_perf());"
    "SELECT json_extract(a.v, '$.libcephsqlite_striper.shrink'), "
    "       json_extract(b.v, '$.libcephsqlite_striper.shrink') "
    "    FROM perf AS a, perf AS b"
    "    WHERE a.i = ? AND b.i = ?;"
    "SELECT json_extract(a.v, '$.libcephsqlite_striper.shrink_bytes') - "
    "       json_extract(b.v, '$.libcephsqlite_striper.shrink_bytes') "
    "    FROM perf AS a, perf AS b"
    "    WHERE a.i = ? AND b.i = ?;"
    ;

    int rc;
    const char *current = SQL;
    sqlite3_stmt *stmt = NULL;
    uint64_t id;

    std::cout << SQL << std::endl;
    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
    id = sqlite3_last_insert_rowid(db);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatch(sqlite3_bind_int64(stmt, 1, id));
    sqlcatch(sqlite3_bind_int64(stmt, 2, id-1));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
    ASSERT_GT(sqlite3_column_int64(stmt, 0), sqlite3_column_int64(stmt, 1));
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatch(sqlite3_bind_int64(stmt, 1, id));
    sqlcatch(sqlite3_bind_int64(stmt, 2, id-1));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
    ASSERT_LT(512*(1<<20), sqlite3_column_int64(stmt, 0));
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    rc = 0;
out:
    sqlite3_finalize(stmt);
    ASSERT_EQ(0, rc);
}

TEST_F(CephSQLiteTest, InsertMassiveVerify) {
  static const char SQL[] =
    "CREATE TABLE foo (a BLOB);"
    "CREATE TEMPORARY TABLE bar (a BLOB);"
    "WITH RECURSIVE c(x) AS"
    "  ("
    "   VALUES(1)"
    "  UNION ALL"
    "   SELECT x+1"
    "   FROM c"
    "  )"
    "INSERT INTO bar (a)"
    "  SELECT RANDOMBLOB(1<<20)"
    "  FROM c"
    "  LIMIT 1024;"
    "SELECT a FROM bar;"
    "INSERT INTO foo (a)"
    "  SELECT a FROM bar;"
    "SELECT a FROM foo;"
    ;

    int rc;
    const char *current = SQL;
    sqlite3_stmt *stmt = NULL;
    std::vector<std::string> hashes1, hashes2;

    std::cout << SQL << std::endl;

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
      const void* blob = sqlite3_column_blob(stmt, 0);
      ceph::bufferlist bl;
      bl.append(std::string_view((const char*)blob, (size_t)sqlite3_column_bytes(stmt, 0)));
      auto digest = ceph::crypto::digest<ceph::crypto::SHA1>(bl);
      hashes1.emplace_back(digest.to_str());
    }
    sqlcatchcode(rc, SQLITE_DONE);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
      const void* blob = sqlite3_column_blob(stmt, 0);
      ceph::bufferlist bl;
      bl.append(std::string_view((const char*)blob, (size_t)sqlite3_column_bytes(stmt, 0)));
      auto digest = ceph::crypto::digest<ceph::crypto::SHA1>(bl);
      hashes2.emplace_back(digest.to_str());
    }
    sqlcatchcode(rc, SQLITE_DONE);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    ASSERT_EQ(hashes1, hashes2);

    rc = 0;
out:
    sqlite3_finalize(stmt);
    ASSERT_EQ(0, rc);
}

TEST_F(CephSQLiteTest, PerfValid) {
  static const char SQL[] =
    "SELECT json_valid(ceph_perf());"
    ;

    int rc;
    const char *current = SQL;
    sqlite3_stmt *stmt = NULL;

    std::cout << SQL << std::endl;
    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
    ASSERT_EQ(sqlite3_column_int64(stmt, 0), 1);
    sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

    rc = 0;
out:
    sqlite3_finalize(stmt);
    ASSERT_EQ(0, rc);
}

TEST_F(CephSQLiteTest, StatusValid) {
  static const char SQL[] =
    "SELECT json_valid(ceph_status());"
    ;

  int rc;
  const char *current = SQL;
  sqlite3_stmt *stmt = NULL;

  std::cout << SQL << std::endl;
  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
  ASSERT_EQ(sqlite3_column_int64(stmt, 0), 1);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  rc = 0;
out:
  sqlite3_finalize(stmt);
  ASSERT_EQ(0, rc);
}

TEST_F(CephSQLiteTest, CurrentTime) {
  static const char SQL[] =
    "SELECT strftime('%s', 'now');"
    ;

  int rc;
  const char *current = SQL;
  sqlite3_stmt *stmt = NULL;

  std::cout << SQL << std::endl;
  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
  {
    time_t now = time(0);
    auto t = sqlite3_column_int64(stmt, 0);
    ASSERT_LT(abs(now-t), 5);
  }
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  rc = 0;
out:
  sqlite3_finalize(stmt);
  ASSERT_EQ(0, rc);
}


TEST_F(CephSQLiteTest, StatusFields) {
  static const char SQL[] =
    "SELECT json_extract(ceph_status(), '$.addr');"
    "SELECT json_extract(ceph_status(), '$.id');"
    ;

  int rc;
  const char *current = SQL;
  sqlite3_stmt *stmt = NULL;

  std::cout << SQL << std::endl;
  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
  {
    auto addr = sqlite3_column_text(stmt, 0);
    std::cout << addr << std::endl;
  }
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
  {
    auto id = sqlite3_column_int64(stmt, 0);
    std::cout << id << std::endl;
    ASSERT_GT(id, 0);
  }
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

  rc = 0;
out:
  sqlite3_finalize(stmt);
  ASSERT_EQ(0, rc);
}

TEST_F(CephSQLiteTest, ConcurrentReaders) {
  static const char SETUP_SQL[] = "CREATE TABLE foo (a INT); INSERT INTO foo (a) VALUES (1);";
  ASSERT_EQ(SQLITE_OK, sqlite3_exec(db, SETUP_SQL, NULL, NULL, NULL));

  sqlite3* db2 = nullptr;
  ASSERT_EQ(SQLITE_OK, sqlite3_open_v2(get_uri().c_str(), &db2, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, "ceph"));

  sqlite3_stmt *stmt1 = NULL;
  sqlite3_stmt *stmt2 = NULL;
  const char *sql = "SELECT * FROM foo;";

  ASSERT_EQ(SQLITE_OK, sqlite3_prepare_v2(db, sql, -1, &stmt1, NULL));
  ASSERT_EQ(SQLITE_OK, sqlite3_prepare_v2(db2, sql, -1, &stmt2, NULL));

  // Step both statements so they hold SHARED locks simultaneously without blocking
  ASSERT_EQ(SQLITE_ROW, sqlite3_step(stmt1));
  ASSERT_EQ(SQLITE_ROW, sqlite3_step(stmt2));

  // Verify that both connections actually hold a SHARED lock
  int lock_state1 = SQLITE_LOCK_NONE;
  int lock_state2 = SQLITE_LOCK_NONE;
  ASSERT_EQ(SQLITE_OK, sqlite3_file_control(db, "main", SQLITE_FCNTL_LOCKSTATE, &lock_state1));
  ASSERT_EQ(SQLITE_OK, sqlite3_file_control(db2, "main", SQLITE_FCNTL_LOCKSTATE, &lock_state2));
  ASSERT_EQ(SQLITE_LOCK_SHARED, lock_state1);
  ASSERT_EQ(SQLITE_LOCK_SHARED, lock_state2);

  // Cleanup
  ASSERT_EQ(SQLITE_OK, sqlite3_finalize(stmt1));
  ASSERT_EQ(SQLITE_OK, sqlite3_finalize(stmt2));
  sqlite3_close(db2);
}

TEST_F(CephSQLiteTest, ReservedLockConcurrency) {
  static const char SETUP_SQL[] = "CREATE TABLE foo (a INT); INSERT INTO foo (a) VALUES (1);";
  ASSERT_EQ(SQLITE_OK, sqlite3_exec(db, SETUP_SQL, NULL, NULL, NULL));

  sqlite3* db2 = nullptr;
  ASSERT_EQ(SQLITE_OK, sqlite3_open_v2(get_uri().c_str(), &db2, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, "ceph"));

  // Set timeout to 0 so blocked locks fail immediately with SQLITE_BUSY
  sqlite3_busy_timeout(db2, 0);

  // Explicitly tell the VFS to use a 0ms internal lock timeout
  int vfs_timeout = 0;
  ASSERT_EQ(SQLITE_OK, sqlite3_file_control(db2, "main", SQLITE_FCNTL_LOCK_TIMEOUT, &vfs_timeout));

  // Connection 1 begins immediate transaction (acquires RESERVED lock)
  ASSERT_EQ(SQLITE_OK, sqlite3_exec(db, "BEGIN IMMEDIATE TRANSACTION;", NULL, NULL, NULL));

  int lock_state1 = SQLITE_LOCK_NONE;
  ASSERT_EQ(SQLITE_OK, sqlite3_file_control(db, "main", SQLITE_FCNTL_LOCKSTATE, &lock_state1));
  ASSERT_EQ(SQLITE_LOCK_RESERVED, lock_state1);

  // Connection 2 can still read (acquires SHARED lock without conflicting with RESERVED)
  sqlite3_stmt *stmt = NULL;
  ASSERT_EQ(SQLITE_OK, sqlite3_prepare_v2(db2, "SELECT * FROM foo;", -1, &stmt, NULL));
  ASSERT_EQ(SQLITE_ROW, sqlite3_step(stmt));

  int lock_state2 = SQLITE_LOCK_NONE;
  ASSERT_EQ(SQLITE_OK, sqlite3_file_control(db2, "main", SQLITE_FCNTL_LOCKSTATE, &lock_state2));
  ASSERT_EQ(SQLITE_LOCK_SHARED, lock_state2);

  ASSERT_EQ(SQLITE_OK, sqlite3_finalize(stmt));

  // Connection 2 cannot begin immediate transaction (blocked by Connection 1's RESERVED lock)
  int rc = sqlite3_exec(db2, "BEGIN IMMEDIATE TRANSACTION;", NULL, NULL, NULL);
  ASSERT_EQ(SQLITE_BUSY, rc);

  // Connection 1 commits (upgrades to EXCLUSIVE to write, then releases all locks)
  ASSERT_EQ(SQLITE_OK, sqlite3_exec(db, "INSERT INTO foo (a) VALUES (2);", NULL, NULL, NULL));
  ASSERT_EQ(SQLITE_OK, sqlite3_file_control(db, "main", SQLITE_FCNTL_LOCKSTATE, &lock_state1));
  ASSERT_EQ(SQLITE_LOCK_RESERVED, lock_state1);

  ASSERT_EQ(SQLITE_OK, sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL));
  ASSERT_EQ(SQLITE_OK, sqlite3_file_control(db, "main", SQLITE_FCNTL_LOCKSTATE, &lock_state1));
  ASSERT_EQ(SQLITE_LOCK_NONE, lock_state1);

  // Now Connection 2 can successfully begin an immediate transaction
  ASSERT_EQ(SQLITE_OK, sqlite3_exec(db2, "BEGIN IMMEDIATE TRANSACTION;", NULL, NULL, NULL));
  ASSERT_EQ(SQLITE_OK, sqlite3_file_control(db2, "main", SQLITE_FCNTL_LOCKSTATE, &lock_state2));
  ASSERT_EQ(SQLITE_LOCK_RESERVED, lock_state2);

  ASSERT_EQ(SQLITE_OK, sqlite3_exec(db2, "COMMIT;", NULL, NULL, NULL));

  sqlite3_close(db2);
}

TEST_F(CephSQLiteTest, LockDowngrade) {
  static const char SETUP_SQL[] = "CREATE TABLE foo (a INT); CREATE TABLE bar (b INT); INSERT INTO foo (a) VALUES (1);";
  ASSERT_EQ(SQLITE_OK, sqlite3_exec(db, SETUP_SQL, NULL, NULL, NULL));

  sqlite3_stmt *stmt = NULL;
  // Step 1: Start a read query on foo to hold a SHARED lock
  ASSERT_EQ(SQLITE_OK, sqlite3_prepare_v2(db, "SELECT * FROM foo;", -1, &stmt, NULL));
  ASSERT_EQ(SQLITE_ROW, sqlite3_step(stmt));

  int lock_state = SQLITE_LOCK_NONE;
  ASSERT_EQ(SQLITE_OK, sqlite3_file_control(db, "main", SQLITE_FCNTL_LOCKSTATE, &lock_state));
  ASSERT_EQ(SQLITE_LOCK_SHARED, lock_state);

  // Step 2: Write to bar. This forces an upgrade to EXCLUSIVE, then an auto-commit.
  // Because stmt is still open, the lock will automatically downgrade to SHARED, not NONE.
  ASSERT_EQ(SQLITE_OK, sqlite3_exec(db, "INSERT INTO bar (b) VALUES (2);", NULL, NULL, NULL));

  ASSERT_EQ(SQLITE_OK, sqlite3_file_control(db, "main", SQLITE_FCNTL_LOCKSTATE, &lock_state));
  ASSERT_EQ(SQLITE_LOCK_SHARED, lock_state);

  // Step 3: Finalize the read cursor to release the SHARED lock completely.
  ASSERT_EQ(SQLITE_OK, sqlite3_finalize(stmt));

  ASSERT_EQ(SQLITE_OK, sqlite3_file_control(db, "main", SQLITE_FCNTL_LOCKSTATE, &lock_state));
  ASSERT_EQ(SQLITE_LOCK_NONE, lock_state);
}

TEST_F(CephSQLiteTest, ExclusiveLockConcurrency) {
  static const char SETUP_SQL[] = "CREATE TABLE foo (a INT); INSERT INTO foo (a) VALUES (1);";
  ASSERT_EQ(SQLITE_OK, sqlite3_exec(db, SETUP_SQL, NULL, NULL, NULL));

  sqlite3* db2 = nullptr;
  ASSERT_EQ(SQLITE_OK, sqlite3_open_v2(get_uri().c_str(), &db2, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, "ceph"));
  sqlite3_busy_timeout(db2, 0);

  // Explicitly tell the VFS to use a 0ms internal lock timeout
  int vfs_timeout = 0;
  ASSERT_EQ(SQLITE_OK, sqlite3_file_control(db2, "main", SQLITE_FCNTL_LOCK_TIMEOUT, &vfs_timeout));

  // Connection 1 acquires EXCLUSIVE lock directly
  ASSERT_EQ(SQLITE_OK, sqlite3_exec(db, "BEGIN EXCLUSIVE TRANSACTION;", NULL, NULL, NULL));
  ASSERT_EQ(SQLITE_OK, sqlite3_exec(db, "INSERT INTO foo (a) VALUES (2);", NULL, NULL, NULL));

  int lock_state1 = SQLITE_LOCK_NONE;
  ASSERT_EQ(SQLITE_OK, sqlite3_file_control(db, "main", SQLITE_FCNTL_LOCKSTATE, &lock_state1));
  ASSERT_EQ(SQLITE_LOCK_EXCLUSIVE, lock_state1);

  // Connection 2 cannot read (fails to acquire SHARED lock because of EXCLUSIVE lock)
  int rc = sqlite3_exec(db2, "SELECT * FROM foo;", NULL, NULL, NULL);
  ASSERT_EQ(SQLITE_BUSY, rc);

  // Connection 1 commits and releases the lock
  ASSERT_EQ(SQLITE_OK, sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL));

  // Connection 2 can now read successfully
  ASSERT_EQ(SQLITE_OK, sqlite3_exec(db2, "SELECT * FROM foo;", NULL, NULL, NULL));

  sqlite3_close(db2);
}


TEST_F(CephSQLiteTest, DeadClientRecovery) {
  static const char SETUP_SQL[] = "CREATE TABLE foo (a INT); INSERT INTO foo (a) VALUES (1);";
  ASSERT_EQ(SQLITE_OK, sqlite3_exec(db, SETUP_SQL, NULL, NULL, NULL));

  // 1. Manually inject a "dead client" by setting XATTR_EXCL.
  // This simulates a client that crashed while holding a RESERVED/EXCLUSIVE lock,
  // leaving the XATTR populated but the ephemeral RADOS locks released.
  librados::IoCtx ioctx;
  ASSERT_EQ(0, cluster.ioctx_create(pool.c_str(), ioctx));
  std::string obj_name = fmt::format("{}.0000000000000000", get_name());

  ceph::bufferlist bl;
  bl.append("[v2:192.0.2.1:0/0,v1:192.0.2.1:0/0]");
  ASSERT_EQ(0, ioctx.setxattr(obj_name, "striper.excl", bl));

  // 2. Attempt to write. The VFS will attempt to upgrade to RESERVED,
  // see the leftover XATTR_EXCL, fail the cmpxattr (-ECANCELED), and trigger recover_lock().
  // recover_lock() will blocklist the mock IP and overwrite XATTR_EXCL to safely take ownership.
  ASSERT_EQ(SQLITE_OK, sqlite3_exec(db, "BEGIN IMMEDIATE TRANSACTION;", NULL, NULL, NULL));

  // Verify that the lock was stolen/recovered and striper.excl is updated to OUR address.
  ceph::bufferlist current_bl;
  ASSERT_LT(0, ioctx.getxattr(obj_name, "striper.excl", current_bl));
  ASSERT_NE(bl.to_str(), current_bl.to_str());
  ASSERT_FALSE(current_bl.to_str().empty());

  ASSERT_EQ(SQLITE_OK, sqlite3_exec(db, "INSERT INTO foo (a) VALUES (2);", NULL, NULL, NULL));
  ASSERT_EQ(SQLITE_OK, sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL));

  // 3. Verify the write succeeded and the lock state was restored.
  sqlite3_stmt *stmt = NULL;
  ASSERT_EQ(SQLITE_OK, sqlite3_prepare_v2(db, "SELECT SUM(a) FROM foo;", -1, &stmt, NULL));
  ASSERT_EQ(SQLITE_ROW, sqlite3_step(stmt));
  ASSERT_EQ(3, sqlite3_column_int64(stmt, 0));
  ASSERT_EQ(SQLITE_OK, sqlite3_finalize(stmt));

  int lock_state = SQLITE_LOCK_NONE;
  ASSERT_EQ(SQLITE_OK, sqlite3_file_control(db, "main", SQLITE_FCNTL_LOCKSTATE, &lock_state));
  ASSERT_EQ(SQLITE_LOCK_NONE, lock_state);
}

TEST_F(CephSQLiteTest, ContentionWaitLoop) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, cluster.ioctx_create(pool.c_str(), ioctx));
  std::string db_name = fmt::format("{}.wait_test", get_name());

  SimpleRADOSStriper rs1(ioctx, db_name);
  ASSERT_EQ(0, rs1.create());
  ASSERT_EQ(0, rs1.open());

  SimpleRADOSStriper rs2(ioctx, db_name);
  ASSERT_EQ(0, rs2.open());
  rs2.set_acquire_timeout(std::chrono::milliseconds(2000));

  // Client 1 acquires an Exclusive lock
  ASSERT_EQ(0, rs1.lock(SimpleRADOSStriper::LockLevel::Exclusive));

  std::atomic<int> rs2_rc = -1;
  auto start = std::chrono::steady_clock::now();

  // Client 2 attempts to acquire a Shared lock concurrently.
  // This should trigger the `-EBUSY` wait loop instead of failing immediately.
  std::thread t([&]() {
    rs2_rc = rs2.lock(SimpleRADOSStriper::LockLevel::Shared);
  });

  // Client 1 holds the lock for ~300ms then releases
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  ASSERT_EQ(0, rs1.unlock(SimpleRADOSStriper::LockLevel::None));

  t.join();

  auto end = std::chrono::steady_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

  // Client 2 should have eventually succeeded and waited at least 300ms
  ASSERT_EQ(0, rs2_rc.load());
  ASSERT_GE(elapsed, 300);

  ASSERT_EQ(0, rs2.unlock(SimpleRADOSStriper::LockLevel::None));
  ASSERT_EQ(0, rs1.remove());
}

TEST_F(CephSQLiteTest, LockRenewal) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, cluster.ioctx_create(pool.c_str(), ioctx));
  std::string db_name = fmt::format("{}.renewal_test", get_name());

  SimpleRADOSStriper rs1(ioctx, db_name);
  ASSERT_EQ(0, rs1.create());
  ASSERT_EQ(0, rs1.open());

  // Use very short timeouts to quickly test the lock keeper thread
  rs1.set_lock_interval(std::chrono::milliseconds(200));
  rs1.set_lock_timeout(std::chrono::milliseconds(500));

  // --- 1. Test Shared lock renewal ---
  ASSERT_EQ(0, rs1.lock(SimpleRADOSStriper::LockLevel::Shared));

  // Sleep past the lock timeout (500ms). If the thread wasn't renewing, the lock would drop on the MDS.
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  SimpleRADOSStriper rs2(ioctx, db_name);
  ASSERT_EQ(0, rs2.open());
  rs2.set_acquire_timeout(std::chrono::milliseconds(0));

  // rs2 should fail to grab Exclusive because rs1's lock_keeper successfully kept the Shared lock alive
  ASSERT_EQ(-EBUSY, rs2.lock(SimpleRADOSStriper::LockLevel::Exclusive));

  // Release Shared lock
  ASSERT_EQ(0, rs1.unlock(SimpleRADOSStriper::LockLevel::None));

  // --- 2. Test Exclusive lock renewal ---
  ASSERT_EQ(0, rs1.lock(SimpleRADOSStriper::LockLevel::Exclusive));

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  // rs2 should fail to grab Shared because rs1's lock_keeper successfully kept the Exclusive lock alive
  ASSERT_EQ(-EBUSY, rs2.lock(SimpleRADOSStriper::LockLevel::Shared));

  ASSERT_EQ(0, rs1.unlock(SimpleRADOSStriper::LockLevel::None));
  ASSERT_EQ(0, rs1.remove());
}

int main(int argc, char **argv) {
  auto args = argv_to_vec(argc, argv);

  std::string conf_file_list;
  std::string cluster;
  CephInitParameters iparams = ceph_argparse_early_args(args, CEPH_ENTITY_TYPE_CLIENT, &cluster, &conf_file_list);
  cct = boost::intrusive_ptr<CephContext>(common_preinit(iparams, CODE_ENVIRONMENT_UTILITY, 0), false);
  cct->_conf.parse_config_files(conf_file_list.empty() ? nullptr : conf_file_list.c_str(), &std::cerr, 0);
  cct->_conf.parse_env(cct->get_module_type()); // environment variables override
  cct->_conf.parse_argv(args);
  cct->_conf.apply_changes(nullptr);
  common_init_finish(cct.get());

  ldout(cct, 1) << "sqlite3 version: " << sqlite3_libversion() << dendl;
  if (int rc = sqlite3_config(SQLITE_CONFIG_URI, 1); rc) {
    lderr(cct) << "sqlite3 config failed: " << rc << dendl;
    exit(EXIT_FAILURE);
  }

  sqlite3_auto_extension((void (*)())sqlite3_cephsqlite_init);
  sqlite3* db = nullptr;
  if (int rc = sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE, nullptr); rc == SQLITE_OK) {
    sqlite3_close(db);
  } else {
    lderr(cct) << "could not open sqlite3: " << rc << dendl;
    exit(EXIT_FAILURE);
  }
  if (int rc = cephsqlite_setcct(cct.get(), nullptr); rc < 0) {
    lderr(cct) << "could not set cct: " << rc << dendl;
    exit(EXIT_FAILURE);
  }

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
