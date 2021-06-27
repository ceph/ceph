// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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
      "CREATE TEMPORARY VIEW p AS"
      "    SELECT perf.i, J.*"
      "    FROM perf, json_tree(perf.v) AS J;"
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

  ASSERT_EQ(0, rs->lock(1000));
  ASSERT_EQ(0, rs->stat(&size1));
  ASSERT_EQ(0, rs->unlock());
  sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
  sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
  sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
  ASSERT_EQ(0, rs->lock(1000));
  ASSERT_EQ(0, rs->stat(&size2));
  ASSERT_EQ(0, rs->unlock());
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
    "SELECT a.atom-b.atom"
    "    FROM p AS a, p AS b"
    "    WHERE a.i = ? AND"
    "          b.i = ? AND"
    "          a.fullkey = '$.libcephsqlite_vfs.opf_sync.avgcount' AND"
    "          b.fullkey = '$.libcephsqlite_vfs.opf_sync.avgcount';"
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
    "SELECT a.atom-b.atom"
    "    FROM p AS a, p AS b"
    "    WHERE a.i = ? AND"
    "          b.i = ? AND"
    "          a.fullkey = '$.libcephsqlite_vfs.opf_sync.avgcount' AND"
    "          b.fullkey = '$.libcephsqlite_vfs.opf_sync.avgcount';"
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
    "SELECT a.atom, b.atom, a.atom-b.atom"
    "    FROM p AS a, p AS b"
    "    WHERE a.i = ? AND"
    "          b.i = ? AND"
    "          a.fullkey = '$.libcephsqlite_vfs.opf_lock.avgcount' AND"
    "          b.fullkey = '$.libcephsqlite_vfs.opf_lock.avgcount';"
    "SELECT a.atom, b.atom, a.atom-b.atom"
    "    FROM p AS a, p AS b"
    "    WHERE a.i = ? AND"
    "          b.i = ? AND"
    "          a.fullkey = '$.libcephsqlite_striper.lock' AND"
    "          b.fullkey = '$.libcephsqlite_striper.lock';"
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
  ASSERT_EQ(sqlite3_column_int64(stmt, 2), 1); /* one actual lock on the striper */
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
    "SELECT a.atom, b.atom, a.atom-b.atom"
    "    FROM p AS a, p AS b"
    "    WHERE a.i = ? AND"
    "          b.i = ? AND"
    "          a.fullkey = '$.libcephsqlite_striper.update_size' AND"
    "          b.fullkey = '$.libcephsqlite_striper.update_size';"
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
    "SELECT a.atom, b.atom, a.atom-b.atom"
    "    FROM p AS a, p AS b"
    "    WHERE a.i = ? AND"
    "          b.i = ? AND"
    "          a.fullkey = '$.libcephsqlite_striper.update_allocated' AND"
    "          b.fullkey = '$.libcephsqlite_striper.update_allocated';"
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
    "SELECT a.atom, b.atom"
    "    FROM p AS a, p AS b"
    "    WHERE a.i = ? AND"
    "          b.i = ? AND"
    "          a.fullkey = '$.libcephsqlite_striper.shrink' AND"
    "          b.fullkey = '$.libcephsqlite_striper.shrink';"
    "SELECT a.atom-b.atom"
    "    FROM p AS a, p AS b"
    "    WHERE a.i = ? AND"
    "          b.i = ? AND"
    "          a.fullkey = '$.libcephsqlite_striper.shrink_bytes' AND"
    "          b.fullkey = '$.libcephsqlite_striper.shrink_bytes';"
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


int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

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
