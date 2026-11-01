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

static void get_omap_keys(IoCtx &ioctx,
                          const std::string &oid,
                          std::set<std::string> *keys) {
  keys->clear();
  ASSERT_EQ(0, ioctx.omap_get_keys2(oid, "", 10000, keys, nullptr));
}

static std::string get_index(const utime_t &key_ts,
                             const std::string &key_ext) {
  char buf[32];
  snprintf(buf, sizeof(buf), "1_%010ld.%06ld_", (long)key_ts.sec(),
           (long)key_ts.usec());
  return buf + key_ext;
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

  std::set<std::string> keys;
  get_omap_keys(ioctx, oid, &keys);
  ASSERT_TRUE(keys.empty());
}

TEST_F(TestClsTimeindex, LegacyEntries) {
  IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  const std::string oid = "legacy";
  ASSERT_EQ(0, ioctx.create(oid, false));

  // an entry added before the ext index was introduced (e.g. before upgrade)
  bufferlist bl1;
  bl1.append("v1");
  std::map<std::string, bufferlist> legacy = {
    {get_index(utime_t(100, 0), "key"), bl1}};
  ASSERT_EQ(0, ioctx.omap_set(oid, legacy));

  // the legacy entry is not known to the ext index so it is not replaced
  bufferlist bl2;
  bl2.append("v2");
  add_entry(ioctx, oid, utime_t(200, 0), "key", bl2);

  std::list<cls_timeindex_entry> entries;
  list_entries(ioctx, oid, utime_t(0, 0), utime_t(1000, 0), &entries);
  ASSERT_EQ(2U, entries.size());
  check_entry(entries.front(), utime_t(100, 0), "key", bl1);
  check_entry(entries.back(), utime_t(200, 0), "key", bl2);

  // trimming the legacy entry must not remove the ext index for the new one
  trim_entries(ioctx, oid, utime_t(0, 0), utime_t(150, 0));

  std::set<std::string> keys;
  get_omap_keys(ioctx, oid, &keys);
  ASSERT_EQ((std::set<std::string>{get_index(utime_t(200, 0), "key"),
                                   "2_key"}), keys);

  // so the new entry is still replaced
  bufferlist bl3;
  bl3.append("v3");
  add_entry(ioctx, oid, utime_t(300, 0), "key", bl3);

  list_entries(ioctx, oid, utime_t(0, 0), utime_t(1000, 0), &entries);
  ASSERT_EQ(1U, entries.size());
  check_entry(entries.front(), utime_t(300, 0), "key", bl3);

  trim_entries(ioctx, oid, utime_t(0, 0), utime_t(1000, 0));
  get_omap_keys(ioctx, oid, &keys);
  ASSERT_TRUE(keys.empty());
}

TEST_F(TestClsTimeindex, LongKeyExt) {
  IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  const std::string oid = "long_key_ext";
  ASSERT_EQ(0, ioctx.create(oid, false));

  // rgw key_ext is "tenant:bucket:bucket_id:object:instance", and an
  // object name may be up to 1024 bytes long and contain spaces
  const std::string key1 = "tenant:bucket:id:" + std::string(1000, 'x') + ":";
  const std::string key2 = "tenant:bucket:id:dir/file with spaces_1.txt:";
  bufferlist bl;
  bl.append("foo");

  add_entry(ioctx, oid, utime_t(100, 0), key1, bl);
  add_entry(ioctx, oid, utime_t(100, 0), key2, bl);
  add_entry(ioctx, oid, utime_t(200, 0), key1, bl);
  add_entry(ioctx, oid, utime_t(300, 0), key2, bl);

  std::list<cls_timeindex_entry> entries;
  list_entries(ioctx, oid, utime_t(0, 0), utime_t(1000, 0), &entries);
  ASSERT_EQ(2U, entries.size());
  check_entry(entries.front(), utime_t(200, 0), key1, bl);
  check_entry(entries.back(), utime_t(300, 0), key2, bl);

  // no ext index is left behind
  trim_entries(ioctx, oid, utime_t(0, 0), utime_t(1000, 0));
  std::set<std::string> keys;
  get_omap_keys(ioctx, oid, &keys);
  ASSERT_TRUE(keys.empty());
}

TEST_F(TestClsTimeindex, SameTimestamp) {
  IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  const std::string oid = "same_timestamp";
  ASSERT_EQ(0, ioctx.create(oid, false));

  bufferlist bl1;
  bl1.append("v1");
  bufferlist bl2;
  bl2.append("v2");

  add_entry(ioctx, oid, utime_t(100, 0), "key1", bl1);
  add_entry(ioctx, oid, utime_t(100, 0), "key2", bl1);
  // re-adding the same entry updates its value
  add_entry(ioctx, oid, utime_t(100, 0), "key1", bl2);

  std::list<cls_timeindex_entry> entries;
  list_entries(ioctx, oid, utime_t(0, 0), utime_t(1000, 0), &entries);
  ASSERT_EQ(2U, entries.size());
  check_entry(entries.front(), utime_t(100, 0), "key1", bl2);
  check_entry(entries.back(), utime_t(100, 0), "key2", bl1);

  trim_entries(ioctx, oid, utime_t(0, 0), utime_t(1000, 0));
  std::set<std::string> keys;
  get_omap_keys(ioctx, oid, &keys);
  ASSERT_TRUE(keys.empty());
}

TEST_F(TestClsTimeindex, SameKeyExtInOneOp) {
  IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  const std::string oid = "same_key_ext_in_one_op";
  ASSERT_EQ(0, ioctx.create(oid, false));

  bufferlist bl1;
  bl1.append("v1");
  bufferlist bl2;
  bl2.append("v2");
  bufferlist bl3;
  bl3.append("v3");

  add_entry(ioctx, oid, utime_t(50, 0), "key1", bl1);

  std::list<cls_timeindex_entry> add_entries(3);
  auto it = add_entries.begin();
  cls_timeindex_add_prepare_entry(*it++, utime_t(100, 0), "key1", bl1);
  cls_timeindex_add_prepare_entry(*it++, utime_t(200, 0), "key1", bl2);
  cls_timeindex_add_prepare_entry(*it++, utime_t(150, 0), "key2", bl3);
  ObjectWriteOperation op;
  cls_timeindex_add(op, add_entries);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  std::list<cls_timeindex_entry> entries;
  list_entries(ioctx, oid, utime_t(0, 0), utime_t(1000, 0), &entries);
  ASSERT_EQ(2U, entries.size());
  check_entry(entries.front(), utime_t(150, 0), "key2", bl3);
  check_entry(entries.back(), utime_t(200, 0), "key1", bl2);

  trim_entries(ioctx, oid, utime_t(0, 0), utime_t(1000, 0));
  std::set<std::string> keys;
  get_omap_keys(ioctx, oid, &keys);
  ASSERT_TRUE(keys.empty());
}
