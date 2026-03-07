// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <iostream>
#include <errno.h>

#include "include/types.h"
#include "msg/msg_types.h"
#include "include/rados/librados.hpp"

#include "test/librados/test_cxx.h"
#include "gtest/gtest.h"

#include "cls/timeindex/cls_timeindex_client.h"

using namespace librados;

static void add_entry(IoCtx &ioctx,
                      const std::string &oid,
                      const utime_t &key_ts,
                      const std::string &key_ext,
                      const bufferlist &value) {
  ObjectWriteOperation op;

  cls_timeindex_add(op, key_ts, key_ext, value);
  ASSERT_EQ(0, ioctx.operate(oid, &op));
}

static void list_entries(IoCtx &ioctx,
                         const std::string &oid,
                         const utime_t &start_ts,
                         const utime_t &end_ts,
                         std::list<cls_timeindex_entry> *entries) {
  std::string out_marker;
  bool truncated = true;
  bufferlist bl;
  ObjectReadOperation op;

  entries->clear();
  cls_timeindex_list(op, start_ts, end_ts, "", 10000, *entries,
                     &out_marker, &truncated);
  ASSERT_EQ(0, ioctx.operate(oid, &op, &bl));
  ASSERT_FALSE(truncated);
}

static void trim_entries(IoCtx &ioctx,
                         const std::string &oid,
                         const utime_t &start_ts,
                         const utime_t &end_ts) {
  ObjectWriteOperation op;

  cls_timeindex_trim(op, start_ts, end_ts, "", "");
  ASSERT_EQ(0, ioctx.operate(oid, &op));
}

static void check_entry(const cls_timeindex_entry &e,
                        const utime_t &key_ts,
                        const std::string &key_ext,
                        const bufferlist &value) {
  ASSERT_EQ(key_ts, e.key_ts);
  ASSERT_EQ(key_ext, e.key_ext);
  ASSERT_EQ(value, e.value);
}

class TestClsTimeindex : public ::testing::Test {
public:

  static void SetUpTestCase() {
    _pool_name = get_temp_pool_name();
    ASSERT_EQ("", create_one_pool_pp(_pool_name, _rados));
  }

  static void TearDownTestCase() {
    ASSERT_EQ(0, destroy_one_pool_pp(_pool_name, _rados));
  }

  static std::string _pool_name;
  static Rados _rados;
};

std::string TestClsTimeindex::_pool_name;
Rados TestClsTimeindex::_rados;


TEST_F(TestClsTimeindex, Basic) {
  IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  const std::string oid = "foo";
  ASSERT_EQ(0, ioctx.create(oid, false));

  bufferlist bl;

  // add
  add_entry(ioctx, oid, utime_t(100, 0), "key1", bufferlist());
  add_entry(ioctx, oid, utime_t(200, 0), "key2", bl);

  // list
  std::list<cls_timeindex_entry> entries;

  list_entries(ioctx, oid, utime_t(0, 0), utime_t(1000, 0), &entries);
  ASSERT_EQ(2U, entries.size());
  check_entry(*(entries.begin()), utime_t(100, 0), "key1", bufferlist());
  entries.pop_front();
  check_entry(*(entries.begin()), utime_t(200, 0), "key2", bl);

  // trim
  trim_entries(ioctx, oid, utime_t(0, 0), utime_t(150, 0));

  list_entries(ioctx, oid, utime_t(0, 0), utime_t(1000, 0), &entries);
  ASSERT_EQ(1U, entries.size());
  check_entry(*(entries.begin()), utime_t(200, 0), "key2", bl);

  // replace
  bl.append("bar");
  add_entry(ioctx, oid, utime_t(300, 0), "key2", bl);

  list_entries(ioctx, oid, utime_t(0, 0), utime_t(1000, 0), &entries);
  ASSERT_EQ(1U, entries.size());
  check_entry(*(entries.begin()), utime_t(300, 0), "key2", bl);

  trim_entries(ioctx, oid, utime_t(0, 0), utime_t(1000, 0));

  list_entries(ioctx, oid, utime_t(0, 0), utime_t(1000, 0), &entries);
  ASSERT_EQ(0U, entries.size());
}
