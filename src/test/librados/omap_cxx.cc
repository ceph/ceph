// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <common/perf_counters_collection.h>

#include "test/librados/test_cxx.h"
#include "test/librados/test_pool_types.h"
#include "crimson_utils.h"
#include "cls/fifo/cls_fifo_ops.h"
#include "cls/version/cls_version_ops.h"
#include "common/json/OSDStructures.h"
#include "librados/librados_asio.h"

#include <boost/asio/io_context.hpp>

#include <algorithm>
#include <climits>
#include <thread>
#include <chrono>

using namespace std;
using namespace librados;
using ceph::test::PoolType;
using ceph::test::PoolTypeTestFixture;

class OmapTest : public PoolTypeTestFixture {
protected:
  static std::string pool_name_prefix();
  void SetUp() override;
  void TearDown() override;
  void check_omap_read(
      std::string oid,
      std::string first_omap_key,
      std::string first_omap_value,
      int expected_size,
      int expected_err);

  void create_test_object_with_omap(
      const std::string& oid,
      const std::map<std::string, bufferlist>& omap_map,
      const bufferlist& omap_header);

  // Helper to get standard test omap data
  std::map<std::string, bufferlist> get_test_omap_data();

  uint64_t create_snapshot_and_set_write_ctx(std::vector<uint64_t>& snaps);

  // Helper to trigger clone creation by writing to object
  void trigger_clone(const std::string& oid);

  static void verify_omap_header(
      IoCtx& ctx,
      const std::string& oid,
      const std::string& expected_header);

  static void verify_omap_keys(
      IoCtx& ctx,
      const std::string& oid,
      const std::set<std::string>& expected_keys);

  static void verify_omap_values(
      IoCtx& ctx,
      const std::string& oid,
      const std::map<std::string, bufferlist>& expected_vals);

  static void verify_omap_state(
      IoCtx& ctx,
      const std::string& oid,
      const std::string& expected_header,
      const std::map<std::string, bufferlist>& expected_omap);

  void verify_omap_header(
      const std::string& oid,
      const std::string& expected_header) {
    verify_omap_header(ioctx, oid, expected_header);
  }

  void verify_omap_keys(
      const std::string& oid,
      const std::set<std::string>& expected_keys) {
    verify_omap_keys(ioctx, oid, expected_keys);
  }

  void verify_omap_values(
      const std::string& oid,
      const std::map<std::string, bufferlist>& expected_vals) {
    verify_omap_values(ioctx, oid, expected_vals);
  }

  void verify_omap_state(
      const std::string& oid,
      const std::string& expected_header,
      const std::map<std::string, bufferlist>& expected_omap) {
    verify_omap_state(ioctx, oid, expected_header, expected_omap);
  }

  class SnapshotContext {
    IoCtx& ioctx;
    std::vector<uint64_t> snaps;
    IoCtx snap_ioctx;
    librados::Rados& rados;
    std::string pool_name;
    std::string nspace;

  public:
    SnapshotContext(IoCtx& ctx, librados::Rados& r,
                    const std::string& pool, const std::string& ns);

    uint64_t get_snap_id() const { return snaps[0]; }
    IoCtx& get_snap_ioctx() { return snap_ioctx; }

    ~SnapshotContext();
  };
};

std::string OmapTest::pool_name_prefix()
{
  return "omap_test_";
}

void OmapTest::SetUp()
{
  SKIP_IF_CRIMSON();

  PoolTypeTestFixture::SetUp();
  balancing_disabled = false;
}

void OmapTest::TearDown()
{
  SKIP_IF_CRIMSON();

  if (balancing_disabled) {
    turn_balancing_on();
  }

  PoolTypeTestFixture::TearDown();
}

void OmapTest::check_omap_read(
    std::string oid,
    std::string first_omap_key,
    std::string first_omap_value,
    int expected_size,
    int expected_err)
{
  ObjectReadOperation read;
  int err = 0;
  std::map<std::string,bufferlist> vals_read{{"_", {}}};
  read.omap_get_vals2("", LONG_MAX, &vals_read, nullptr, &err);
  int ret = ioctx.operate(oid, &read, nullptr);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(err, expected_err);
  EXPECT_EQ(vals_read.size(), expected_size);
  if (expected_size > 0) {
    EXPECT_EQ(vals_read.begin()->first, first_omap_key);
    std::string val;
    decode(val, vals_read.begin()->second);
    EXPECT_EQ(val, first_omap_value);
  }
}

void OmapTest::create_test_object_with_omap(
    const std::string& oid,
    const std::map<std::string, bufferlist>& omap_map,
    const bufferlist& omap_header) {
  bufferlist bl_write;
  bl_write.append("ceph");
  ObjectWriteOperation write_op;
  write_op.write(0, bl_write);
  write_op.omap_set_header(omap_header);
  write_op.omap_set(omap_map);
  int ret = ioctx.operate(oid, &write_op);
  ASSERT_EQ(ret, 0);
}

std::map<std::string, bufferlist> OmapTest::get_test_omap_data() {
  const std::string omap_value = "omap_value_1_horse";
  bufferlist omap_val_bl;
  encode(omap_value, omap_val_bl);
  
  return {
    {"omap_key_1_palomino", omap_val_bl},
    {"omap_key_2_chestnut", omap_val_bl},
    {"omap_key_3_bay", omap_val_bl},
    {"omap_key_4_dun", omap_val_bl},
    {"omap_key_5_black", omap_val_bl},
    {"omap_key_6_grey", omap_val_bl}
  };
}

uint64_t OmapTest::create_snapshot_and_set_write_ctx(std::vector<uint64_t>& snaps) {
  snaps.push_back(-2);
  EXPECT_EQ(0, ioctx.selfmanaged_snap_create(&snaps.back()));
  ::std::reverse(snaps.begin(), snaps.end());
  EXPECT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(snaps[0], snaps));
  ::std::reverse(snaps.begin(), snaps.end());
  return snaps.back();
}

void OmapTest::trigger_clone(const std::string& oid) {
  bufferlist write_bl;
  write_bl.append("trigger_clone");
  EXPECT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));
}

void OmapTest::verify_omap_header(
    IoCtx& ctx,
    const std::string& oid,
    const std::string& expected_header) {
  bufferlist header_bl;
  int err = 0;
  ObjectReadOperation read;
  read.omap_get_header(&header_bl, &err);
  ASSERT_EQ(0, ctx.operate(oid, &read, nullptr));
  ASSERT_EQ(0, err);

  std::string header_str;
  if (header_bl.length() > 0) {
    decode(header_str, header_bl);
  }
  ASSERT_EQ(expected_header, header_str);
}

void OmapTest::verify_omap_keys(
    IoCtx& ctx,
    const std::string& oid,
    const std::set<std::string>& expected_keys) {
  std::set<std::string> keys;
  int err = 0;
  ObjectReadOperation read;
  read.omap_get_keys2("", LONG_MAX, &keys, nullptr, &err);
  ASSERT_EQ(0, ctx.operate(oid, &read, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(expected_keys.size(), keys.size());
  for (const auto& key : expected_keys) {
    ASSERT_EQ(1u, keys.count(key));
  }
}

void OmapTest::verify_omap_values(
    IoCtx& ctx,
    const std::string& oid,
    const std::map<std::string, bufferlist>& expected_vals) {
  std::map<std::string, bufferlist> vals;
  int err = 0;
  ObjectReadOperation read;
  read.omap_get_vals2("", LONG_MAX, &vals, nullptr, &err);
  ASSERT_EQ(0, ctx.operate(oid, &read, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(expected_vals.size(), vals.size());
  for (const auto& kv : expected_vals) {
    ASSERT_EQ(1u, vals.count(kv.first));
    ASSERT_TRUE(kv.second.contents_equal(vals[kv.first]));
  }
}

void OmapTest::verify_omap_state(
    IoCtx& ctx,
    const std::string& oid,
    const std::string& expected_header,
    const std::map<std::string, bufferlist>& expected_omap) {
  verify_omap_header(ctx, oid, expected_header);
  verify_omap_values(ctx, oid, expected_omap);
}

OmapTest::SnapshotContext::SnapshotContext(
    IoCtx& ctx, librados::Rados& r,
    const std::string& pool, const std::string& ns)
  : ioctx(ctx), rados(r), pool_name(pool), nspace(ns) {
  snaps.push_back(-2);
  EXPECT_EQ(0, ioctx.selfmanaged_snap_create(&snaps.back()));
  ::std::reverse(snaps.begin(), snaps.end());
  EXPECT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(snaps[0], snaps));
  ::std::reverse(snaps.begin(), snaps.end());

  EXPECT_EQ(0, rados.ioctx_create(pool_name.c_str(), snap_ioctx));
  snap_ioctx.set_namespace(nspace);
  snap_ioctx.snap_set_read(snaps[0]);
}

OmapTest::SnapshotContext::~SnapshotContext() {
  for (auto snap : snaps) {
    ioctx.selfmanaged_snap_remove(snap);
  }
  snap_ioctx.close();
}

TEST_P(OmapTest, OmapSetAndWrite) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_set";
  const std::string omap_header = "my_omap_header";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  
  // Test that we can write an object with omap data
  create_test_object_with_omap(oid, omap_map, omap_header_bl);
  
  // Verify the object data was written
  bufferlist bl_read;
  ObjectReadOperation read;
  read.read(0, 4, &bl_read, nullptr);
  int ret = ioctx.operate(oid, &read, nullptr);
  EXPECT_EQ(ret, 0);
  ASSERT_EQ(0, memcmp(bl_read.c_str(), "ceph", 4));
}

TEST_P(OmapTest, OmapGetVals) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_get_vals";
  const std::string omap_header = "my_omap_header";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  create_test_object_with_omap(oid, omap_map, omap_header_bl);
  
  // Test omap_get_vals2 with start_after and max_return
  int err = 0;
  std::map<std::string, bufferlist> returned_vals_1;
  std::map<std::string, bufferlist> returned_vals_2;
  
  ObjectReadOperation read;
  read.omap_get_vals2("omap_key_1_palomino", 1, &returned_vals_1, nullptr, &err);
  read.omap_get_vals2("omap", 4, &returned_vals_2, nullptr, &err);
  
  int ret = ioctx.operate(oid, &read, nullptr);
  EXPECT_EQ(ret, 0);
  ASSERT_EQ(0, err);
  ASSERT_EQ(1u, returned_vals_1.size());
  ASSERT_EQ(4u, returned_vals_2.size());
}

TEST_P(OmapTest, OmapGetKeys) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_get_keys";
  const std::string omap_header = "my_omap_header";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  create_test_object_with_omap(oid, omap_map, omap_header_bl);
  
  // Test omap_get_keys2
  int err = 0;
  std::set<std::string> returned_keys;
  
  ObjectReadOperation read;
  read.omap_get_keys2("", LONG_MAX, &returned_keys, nullptr, &err);
  
  int ret = ioctx.operate(oid, &read, nullptr);
  EXPECT_EQ(ret, 0);
  ASSERT_EQ(0, err);
  ASSERT_EQ(6u, returned_keys.size());
}

TEST_P(OmapTest, OmapGetHeader) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_get_header";
  const std::string omap_header = "my_omap_header";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  create_test_object_with_omap(oid, omap_map, omap_header_bl);
  
  // Test omap_get_header
  verify_omap_header(oid, omap_header);
}

TEST_P(OmapTest, OmapGetValsByKeys) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_get_vals_by_keys";
  const std::string omap_header = "my_omap_header";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  create_test_object_with_omap(oid, omap_map, omap_header_bl);
  
  // Test omap_get_vals_by_keys
  int err = 0;
  std::set<std::string> key_filter = { "omap_key_1_palomino", "omap_key_3_bay" };
  std::map<std::string, bufferlist> returned_vals_by_keys;
  
  ObjectReadOperation read;
  read.omap_get_vals_by_keys(key_filter, &returned_vals_by_keys, &err);
  
  int ret = ioctx.operate(oid, &read, nullptr);
  EXPECT_EQ(ret, 0);
  ASSERT_EQ(0, err);
  ASSERT_EQ(2u, returned_vals_by_keys.size());
}

TEST_P(OmapTest, OmapCmp) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_cmp";
  const std::string omap_header = "my_omap_header";
  const std::string omap_value = "omap_value_1_horse";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  create_test_object_with_omap(oid, omap_map, omap_header_bl);
  
  // Test omap_cmp
  int err = 0;
  map<string, pair<bufferlist, int>> cmp_results;
  bufferlist cmp_val_bl;
  encode(omap_value, cmp_val_bl);
  
  cmp_results["omap_key_1_palomino"] = pair<bufferlist, int>(cmp_val_bl, CEPH_OSD_CMPXATTR_OP_EQ);
  
  ObjectReadOperation read;
  read.omap_cmp(cmp_results, &err);
  
  int ret = ioctx.operate(oid, &read, nullptr);
  EXPECT_EQ(ret, 0);
  ASSERT_EQ(0, err);
}

TEST_P(OmapTest, OmapRmKeys) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_rm_keys";
  const std::string omap_header = "my_omap_header";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  create_test_object_with_omap(oid, omap_map, omap_header_bl);
  
  // Test omap_rm_keys
  std::set<std::string> keys_to_remove;
  keys_to_remove.insert("omap_key_2_chestnut");
  
  ObjectWriteOperation write_op;
  write_op.omap_rm_keys(keys_to_remove);
  int ret = ioctx.operate(oid, &write_op);
  EXPECT_EQ(ret, 0);
  
  // Verify key was removed
  int err = 0;
  std::set<std::string> returned_keys;
  ObjectReadOperation read;
  read.omap_get_keys2("", LONG_MAX, &returned_keys, nullptr, &err);
  ret = ioctx.operate(oid, &read, nullptr);
  EXPECT_EQ(ret, 0);
  ASSERT_EQ(0, err);
  ASSERT_EQ(5u, returned_keys.size());
  ASSERT_EQ(0u, returned_keys.count("omap_key_2_chestnut"));
}

TEST_P(OmapTest, OmapRmRange) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_rm_range";
  const std::string omap_header = "my_omap_header";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  create_test_object_with_omap(oid, omap_map, omap_header_bl);
  
  // Test omap_rm_range - removes keys in range [start, end)
  ObjectWriteOperation write_op;
  write_op.omap_rm_range("omap_key_3_bay", "omap_key_5_black");
  int ret = ioctx.operate(oid, &write_op);
  EXPECT_EQ(ret, 0);
  
  // Verify keys in range were removed
  int err = 0;
  std::set<std::string> returned_keys;
  ObjectReadOperation read;
  read.omap_get_keys2("", LONG_MAX, &returned_keys, nullptr, &err);
  ret = ioctx.operate(oid, &read, nullptr);
  EXPECT_EQ(ret, 0);
  ASSERT_EQ(0, err);
  ASSERT_EQ(4u, returned_keys.size());
  // Keys 3 and 4 should be removed, keys 1, 2, 5, 6 should remain
  ASSERT_EQ(0u, returned_keys.count("omap_key_3_bay"));
  ASSERT_EQ(0u, returned_keys.count("omap_key_4_dun"));
  ASSERT_EQ(1u, returned_keys.count("omap_key_1_palomino"));
  ASSERT_EQ(1u, returned_keys.count("omap_key_2_chestnut"));
}

TEST_P(OmapTest, OmapClear) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_clear";
  const std::string omap_header = "my_omap_header";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  create_test_object_with_omap(oid, omap_map, omap_header_bl);
  
  // Test omap_clear
  ObjectWriteOperation write_op;
  write_op.omap_clear();
  int ret = ioctx.operate(oid, &write_op);
  EXPECT_EQ(ret, 0);
  
  // Verify all omap data was cleared
  int err = 0;
  std::set<std::string> returned_keys;
  ObjectReadOperation read;
  read.omap_get_keys2("", LONG_MAX, &returned_keys, nullptr, &err);
  ret = ioctx.operate(oid, &read, nullptr);
  EXPECT_EQ(ret, 0);
  ASSERT_EQ(0, err);
  ASSERT_TRUE(returned_keys.empty());
}

TEST_P(OmapTest, OmapPreservedAfterTruncateToZero) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_truncate_to_zero";
  const std::string omap_header = "test_header_for_truncate";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);

  auto omap_map = get_test_omap_data();
  create_test_object_with_omap(oid, omap_map, omap_header_bl);
  
  // Verify initial state - object has data
  bufferlist read_bl;
  int ret = ioctx.read(oid, read_bl, 0, 0);
  EXPECT_GT(read_bl.length(), 0);
  
  // Truncate object to zero
  ObjectWriteOperation write_op;
  write_op.truncate(0);
  ret = ioctx.operate(oid, &write_op);
  EXPECT_EQ(ret, 0);
  
  // Verify object data is truncated to zero
  read_bl.clear();
  ret = ioctx.read(oid, read_bl, 0, 0);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(read_bl.length(), 0);
  
  // Verify omap state is preserved after truncate
  verify_omap_state(oid, omap_header, omap_map);
}


TEST_P(OmapTest, OmapRecovery) {
  SKIP_IF_CRIMSON();
  turn_balancing_off();
  bufferlist bl_write, omap_val_bl, xattr_val_bl;
  const std::string omap_key_1 = "key_a";
  const std::string omap_key_2 = "key_b";
  const std::string omap_key_3 = "key_c";
  const std::string omap_value = "val_12345";
  encode(omap_value, omap_val_bl);
  std::map<std::string, bufferlist> omap_map = {
    {omap_key_1, omap_val_bl},
    {omap_key_2, omap_val_bl},
    {omap_key_3, omap_val_bl}
  };
  const std::string header = "upmap_header_z";
  bufferlist header_bl;
  encode(header, header_bl);
  bl_write.append("ceph");
  
  // 1. Write data to omap
  ObjectWriteOperation write1;
  write1.write(0, bl_write);
  write1.omap_set(omap_map);
  write1.omap_set_header(header_bl);
  int ret = ioctx.operate("change_upmap_oid", &write1);
  EXPECT_EQ(ret, 0);

  // 2-5. Setup and trigger recovery via upmap
  int new_primary;
  setup_and_trigger_recovery("change_upmap_oid", new_primary);
  
  // 6. Read omap
  check_omap_read("change_upmap_oid", omap_key_1, omap_value, 3, 0);
}

TEST_P(OmapTest, NoOmapRecovery) {
  SKIP_IF_CRIMSON();
  turn_balancing_off();
  bufferlist bl_write;
  bl_write.append("ceph");

  // Write data without omap
  ObjectWriteOperation write1;
  write1.write(0, bl_write);
  int ret = ioctx.operate("no_omap_oid", &write1);
  EXPECT_EQ(ret, 0);

  // Setup and trigger recovery via upmap
  int new_primary;
  setup_and_trigger_recovery("no_omap_oid", new_primary);

  // Read data
  bufferlist bl_read;
  ObjectReadOperation read;
  read.read(0, bl_write.length(), &bl_read, nullptr);
  ret = ioctx.operate("no_omap_oid", &read, nullptr);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(0, memcmp(bl_read.c_str(), "ceph", 4));
}

TEST_P(OmapTest, NoOmapRecoveryZeroSized) {
  SKIP_IF_CRIMSON();
  turn_balancing_off();
  bufferlist bl_write;

  // Write zero-sized object using write_full
  ObjectWriteOperation write1;
  write1.write_full(bl_write);
  int ret = ioctx.operate("no_omap_oid_zero", &write1);
  EXPECT_EQ(ret, 0);

  // Verify object exists (even if zero-sized)
  uint64_t size;
  time_t mtime;
  ret = ioctx.stat("no_omap_oid_zero", &size, &mtime);
  ASSERT_EQ(ret, 0);
  EXPECT_EQ(size, 0);

  // Setup and trigger recovery via upmap
  int new_primary;
  setup_and_trigger_recovery("no_omap_oid_zero", new_primary);

  // Read data (should be empty for zero-sized object)
  bufferlist bl_read;
  ObjectReadOperation read;
  read.read(0, 0, &bl_read, nullptr);
  ret = ioctx.operate("no_omap_oid_zero", &read, nullptr);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(bl_read.length(), 0);
}

TEST_P(OmapTest, LargeOmapRecovery) {
  SKIP_IF_CRIMSON();
  turn_balancing_off();

  bufferlist bl_write, header_bl;
  const std::string huge_val(1024, 'x');
  const std::string header = "large_header";
  encode(header, header_bl);
  bl_write.append("ceph");

  // Write data to omap in 100 chunks of 100 keys
  int total_keys = 10000;
  int chunk_size = 100;
  int num_chunks = total_keys / chunk_size;
  int current_key_index = 0;

  for (int chunk = 0; chunk < num_chunks; ++chunk) {
    std::map<std::string, bufferlist> omap_map_chunk;

    for (int i = 0; i < chunk_size; ++i) {
      char key_buf[32];
      snprintf(key_buf, sizeof(key_buf), "key_%06d", current_key_index);
      bufferlist omap_val_bl;
      encode(huge_val, omap_val_bl);
      omap_map_chunk[std::string(key_buf)] = omap_val_bl;
      current_key_index++;
    }

    ObjectWriteOperation write_op;

    // Only write the object payload and omap header on the first transaction
    if (chunk == 0) {
      write_op.write(0, bl_write);
      write_op.omap_set_header(header_bl);
    }

    write_op.omap_set(omap_map_chunk);
    int ret = ioctx.operate("large_oid", &write_op);
    EXPECT_EQ(ret, 0);
  }

  // Setup and trigger recovery via upmap
  int new_primary;
  setup_and_trigger_recovery("large_oid", new_primary);

  // Read omap
  check_omap_read("large_oid", "key_000000", huge_val, 1024, 0);
}

TEST_P(OmapTest, OmapAfterDelete) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_after_delete";
  const std::string omap_header = "my_omap_header";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  create_test_object_with_omap(oid, omap_map, omap_header_bl);
  
  // Verify omap data exists
  std::set<std::string> expected_keys = {
    "omap_key_1_palomino", "omap_key_2_chestnut", "omap_key_3_bay",
    "omap_key_4_dun", "omap_key_5_black", "omap_key_6_grey"
  };
  verify_omap_keys(oid, expected_keys);

  // Delete the object
  int ret = ioctx.remove(oid);
  EXPECT_EQ(ret, 0);

  // Try to read omap keys - should fail with -ENOENT
  int err = 0;
  std::set<std::string> returned_keys;
  ObjectReadOperation read2;
  read2.omap_get_keys2("", LONG_MAX, &returned_keys, nullptr, &err);
  ret = ioctx.operate(oid, &read2, nullptr);
  EXPECT_EQ(ret, -ENOENT);
  EXPECT_EQ(err, -EIO);
  
  // Try to read omap values - should fail with -ENOENT
  err = 0;
  std::map<std::string, bufferlist> returned_vals;
  ObjectReadOperation read3;
  read3.omap_get_vals2("", LONG_MAX, &returned_vals, nullptr, &err);
  ret = ioctx.operate(oid, &read3, nullptr);
  EXPECT_EQ(ret, -ENOENT);
  EXPECT_EQ(err, -EIO);
  
  // Try to read omap header - should fail with -ENOENT
  err = 0;
  bufferlist returned_header_bl;
  ObjectReadOperation read4;
  read4.omap_get_header(&returned_header_bl, &err);
  ret = ioctx.operate(oid, &read4, nullptr);
  EXPECT_EQ(ret, -ENOENT);
  EXPECT_EQ(err, 0);
  
  // Try to read omap values by keys - should fail with -ENOENT
  err = 0;
  std::set<std::string> key_filter = { "omap_key_1_palomino", "omap_key_3_bay" };
  std::map<std::string, bufferlist> returned_vals_by_keys;
  ObjectReadOperation read5;
  read5.omap_get_vals_by_keys(key_filter, &returned_vals_by_keys, &err);
  ret = ioctx.operate(oid, &read5, nullptr);
  EXPECT_EQ(ret, -ENOENT);
  EXPECT_EQ(err, -EIO);
  
  // Write omap data to deleted object - should succeed (creates new object)
  bufferlist new_omap_val_bl;
  const std::string new_omap_value = "new_value";
  encode(new_omap_value, new_omap_val_bl);
  std::map<std::string, bufferlist> new_omap_map = {
    {"new_key", new_omap_val_bl}
  };
  ObjectWriteOperation write_op;
  write_op.omap_set(new_omap_map);
  ret = ioctx.operate(oid, &write_op);
  EXPECT_EQ(ret, 0);
  
  // Verify new omap data exists (object was recreated)
  std::set<std::string> new_expected_keys = {"new_key"};
  verify_omap_keys(oid, new_expected_keys);
}

TEST_P(OmapTest, CloneBasic) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_clone_basic";
  const std::string original_header = "original_header";
  const std::string original_value = "original_value";
  
  // Create object with OMAP data
  bufferlist header_bl;
  encode(original_header, header_bl);
  
  bufferlist val_bl;
  encode(original_value, val_bl);
  std::map<std::string, bufferlist> omap_map = {
    {"key1", val_bl},
    {"key2", val_bl},
    {"key3", val_bl}
  };
  
  create_test_object_with_omap(oid, omap_map, header_bl);
  
  // Create snapshot and trigger clone creation
  SnapshotContext snap_ctx(ioctx, rados, pool_name, nspace);
  trigger_clone(oid);
  
  // Modify head object OMAP
  const std::string new_header = "modified_header";
  const std::string new_value = "modified_value";
  bufferlist new_header_bl;
  encode(new_header, new_header_bl);
  bufferlist new_val_bl;
  encode(new_value, new_val_bl);
  
  ObjectWriteOperation write_op;
  write_op.omap_set_header(new_header_bl);
  std::map<std::string, bufferlist> new_omap = {{"key4", new_val_bl}};
  write_op.omap_set(new_omap);
  ASSERT_EQ(0, ioctx.operate(oid, &write_op));
  
  // Verify clone has original OMAP data
  IoCtx& snap_ioctx = snap_ctx.get_snap_ioctx();
  
  // Check clone header
  bufferlist clone_header_bl;
  int err = 0;
  ObjectReadOperation read_clone;
  read_clone.omap_get_header(&clone_header_bl, &err);
  ASSERT_EQ(0, snap_ioctx.operate(oid, &read_clone, nullptr));
  ASSERT_EQ(0, err);
  
  std::string clone_header;
  decode(clone_header, clone_header_bl);
  ASSERT_EQ(original_header, clone_header);
  
  // Check clone keys
  std::set<std::string> clone_keys;
  ObjectReadOperation read_clone_keys;
  err = 0;
  read_clone_keys.omap_get_keys2("", LONG_MAX, &clone_keys, nullptr, &err);
  ASSERT_EQ(0, snap_ioctx.operate(oid, &read_clone_keys, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(3u, clone_keys.size());
  ASSERT_EQ(1u, clone_keys.count("key1"));
  ASSERT_EQ(1u, clone_keys.count("key2"));
  ASSERT_EQ(1u, clone_keys.count("key3"));
  ASSERT_EQ(0u, clone_keys.count("key4"));
  
  // Verify head has modified OMAP data
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
  verify_omap_header(oid, new_header);
  
  // Check head keys
  std::set<std::string> head_keys;
  ObjectReadOperation read_head_keys;
  err = 0;
  read_head_keys.omap_get_keys2("", LONG_MAX, &head_keys, nullptr, &err);
  ASSERT_EQ(0, ioctx.operate(oid, &read_head_keys, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(4u, head_keys.size());
  ASSERT_EQ(1u, head_keys.count("key4"));
}

TEST_P(OmapTest, CloneHeader) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_clone_header";
  const std::string header1 = "header_version_1";
  const std::string header2 = "header_version_2";
  
  // Create object with OMAP header
  bufferlist header1_bl;
  encode(header1, header1_bl);
  std::map<std::string, bufferlist> omap_map;
  create_test_object_with_omap(oid, omap_map, header1_bl);
  
  // Create snapshot and trigger clone creation
  SnapshotContext snap_ctx(ioctx, rados, pool_name, nspace);
  trigger_clone(oid);
  
  // Change header on head
  bufferlist header2_bl;
  encode(header2, header2_bl);
  ObjectWriteOperation write_op;
  write_op.omap_set_header(header2_bl);
  ASSERT_EQ(0, ioctx.operate(oid, &write_op));
  
  // Verify clone has original header
  IoCtx& snap_ioctx = snap_ctx.get_snap_ioctx();
  
  bufferlist clone_header_bl;
  int err = 0;
  ObjectReadOperation read_clone;
  read_clone.omap_get_header(&clone_header_bl, &err);
  ASSERT_EQ(0, snap_ioctx.operate(oid, &read_clone, nullptr));
  ASSERT_EQ(0, err);
  
  std::string clone_header;
  decode(clone_header, clone_header_bl);
  ASSERT_EQ(header1, clone_header);
  
  // Verify head has new header
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
  verify_omap_header(oid, header2);
}

TEST_P(OmapTest, CloneKeyModifications) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_clone_keys";
  const std::string value = "test_value";
  
  // Create object with OMAP keys
  bufferlist val_bl;
  encode(value, val_bl);
  std::map<std::string, bufferlist> omap_map = {
    {"original_key1", val_bl},
    {"original_key2", val_bl},
    {"to_be_removed", val_bl}
  };
  bufferlist header_bl;
  create_test_object_with_omap(oid, omap_map, header_bl);
  
  // Create snapshot and trigger clone creation
  std::vector<uint64_t> my_snaps;
  create_snapshot_and_set_write_ctx(my_snaps);
  trigger_clone(oid);
  
  // Modify keys on head: add, remove, and modify
  ObjectWriteOperation write_op;
  std::map<std::string, bufferlist> new_keys = {{"new_key", val_bl}};
  write_op.omap_set(new_keys);
  std::set<std::string> remove_keys = {"to_be_removed"};
  write_op.omap_rm_keys(remove_keys);
  ASSERT_EQ(0, ioctx.operate(oid, &write_op));
  
  // Verify clone has original keys
  IoCtx snap_ioctx;
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), snap_ioctx));
  snap_ioctx.set_namespace(nspace);
  snap_ioctx.snap_set_read(my_snaps[0]);
  
  std::set<std::string> clone_expected_keys = {
    "original_key1", "original_key2", "to_be_removed"
  };
  verify_omap_keys(snap_ioctx, oid, clone_expected_keys);
  
  // Verify head has modified keys
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
  std::set<std::string> head_expected_keys = {
    "original_key1", "original_key2", "new_key"
  };
  verify_omap_keys(oid, head_expected_keys);
  
  // Cleanup
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  snap_ioctx.close();
}

TEST_P(OmapTest, CloneMultiple) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_clone_multiple";
  const std::string value = "value";
  
  // Create object with initial OMAP data
  bufferlist val_bl;
  encode(value, val_bl);
  std::map<std::string, bufferlist> omap_map1 = {{"key1", val_bl}};
  bufferlist header_bl;
  create_test_object_with_omap(oid, omap_map1, header_bl);
  
  // Create first snapshot and trigger clone
  std::vector<uint64_t> my_snaps;
  create_snapshot_and_set_write_ctx(my_snaps);
  trigger_clone(oid);
  
  // Modify OMAP
  ObjectWriteOperation write_op1;
  std::map<std::string, bufferlist> omap_map2 = {{"key2", val_bl}};
  write_op1.omap_set(omap_map2);
  ASSERT_EQ(0, ioctx.operate(oid, &write_op1));
  
  // Create second snapshot and trigger clone
  create_snapshot_and_set_write_ctx(my_snaps);
  trigger_clone(oid);
  
  // Modify OMAP again
  ObjectWriteOperation write_op2;
  std::map<std::string, bufferlist> omap_map3 = {{"key3", val_bl}};
  write_op2.omap_set(omap_map3);
  ASSERT_EQ(0, ioctx.operate(oid, &write_op2));
  
  // Verify first clone has only key1
  IoCtx snap_ioctx;
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), snap_ioctx));
  snap_ioctx.set_namespace(nspace);
  snap_ioctx.snap_set_read(my_snaps[0]);
  
  std::set<std::string> clone1_keys;
  int err = 0;
  ObjectReadOperation read_clone1;
  read_clone1.omap_get_keys2("", LONG_MAX, &clone1_keys, nullptr, &err);
  ASSERT_EQ(0, snap_ioctx.operate(oid, &read_clone1, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(1u, clone1_keys.size());
  ASSERT_EQ(1u, clone1_keys.count("key1"));
  
  // Verify second clone has key1 and key2
  snap_ioctx.snap_set_read(my_snaps[1]);
  std::set<std::string> clone2_keys;
  ObjectReadOperation read_clone2;
  err = 0;
  read_clone2.omap_get_keys2("", LONG_MAX, &clone2_keys, nullptr, &err);
  ASSERT_EQ(0, snap_ioctx.operate(oid, &read_clone2, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(2u, clone2_keys.size());
  ASSERT_EQ(1u, clone2_keys.count("key1"));
  ASSERT_EQ(1u, clone2_keys.count("key2"));
  
  // Verify head has all three keys
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
  std::set<std::string> head_keys;
  ObjectReadOperation read_head;
  err = 0;
  read_head.omap_get_keys2("", LONG_MAX, &head_keys, nullptr, &err);
  ASSERT_EQ(0, ioctx.operate(oid, &read_head, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(3u, head_keys.size());
  ASSERT_EQ(1u, head_keys.count("key1"));
  ASSERT_EQ(1u, head_keys.count("key2"));
  ASSERT_EQ(1u, head_keys.count("key3"));
  
  // Cleanup
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps[1]));
  snap_ioctx.close();
}

TEST_P(OmapTest, CloneAfterClear) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_clone_after_clear";
  const std::string value = "value";
  
  // Create object with OMAP data
  bufferlist val_bl;
  encode(value, val_bl);
  std::map<std::string, bufferlist> omap_map = {
    {"key1", val_bl},
    {"key2", val_bl}
  };
  bufferlist header_bl;
  encode("header", header_bl);
  create_test_object_with_omap(oid, omap_map, header_bl);
  
  // Clear OMAP
  ObjectWriteOperation clear_op;
  clear_op.omap_clear();
  ASSERT_EQ(0, ioctx.operate(oid, &clear_op));
  
  // Create snapshot
  std::vector<uint64_t> my_snaps;
  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  
  // Write to object to trigger clone creation
  bufferlist write_bl;
  write_bl.append("trigger_clone");
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));
  
  // Add new OMAP data to head
  ObjectWriteOperation write_op;
  std::map<std::string, bufferlist> new_omap = {{"new_key", val_bl}};
  write_op.omap_set(new_omap);
  ASSERT_EQ(0, ioctx.operate(oid, &write_op));
  
  // Verify clone has no OMAP data
  IoCtx snap_ioctx;
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), snap_ioctx));
  snap_ioctx.set_namespace(nspace);
  snap_ioctx.snap_set_read(my_snaps[0]);
  
  std::set<std::string> clone_keys;
  int err = 0;
  ObjectReadOperation read_clone;
  read_clone.omap_get_keys2("", LONG_MAX, &clone_keys, nullptr, &err);
  ASSERT_EQ(0, snap_ioctx.operate(oid, &read_clone, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_TRUE(clone_keys.empty());
  
  // Verify head has new OMAP data
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
  std::set<std::string> head_keys;
  ObjectReadOperation read_head;
  err = 0;
  read_head.omap_get_keys2("", LONG_MAX, &head_keys, nullptr, &err);
  ASSERT_EQ(0, ioctx.operate(oid, &read_head, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(1u, head_keys.size());
  ASSERT_EQ(1u, head_keys.count("new_key"));
  
  // Cleanup
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  snap_ioctx.close();
}

TEST_P(OmapTest, CloneMixedOperations) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_clone_mixed";
  const std::string header1 = "header1";
  const std::string header2 = "header2";
  const std::string value = "value";
  
  // Create object with OMAP data
  bufferlist header1_bl;
  encode(header1, header1_bl);
  bufferlist val_bl;
  encode(value, val_bl);
  std::map<std::string, bufferlist> omap_map = {
    {"keep_key", val_bl},
    {"remove_key", val_bl},
    {"modify_key", val_bl}
  };
  create_test_object_with_omap(oid, omap_map, header1_bl);
  
  // Create snapshot and trigger clone creation
  std::vector<uint64_t> my_snaps;
  create_snapshot_and_set_write_ctx(my_snaps);
  trigger_clone(oid);
  
  // Perform mixed operations on head
  bufferlist header2_bl;
  encode(header2, header2_bl);
  bufferlist new_val_bl;
  encode("new_value", new_val_bl);
  
  ObjectWriteOperation write_op;
  write_op.omap_set_header(header2_bl);
  std::map<std::string, bufferlist> add_keys = {
    {"add_key", val_bl},
    {"modify_key", new_val_bl}
  };
  write_op.omap_set(add_keys);
  std::set<std::string> remove_keys = {"remove_key"};
  write_op.omap_rm_keys(remove_keys);
  ASSERT_EQ(0, ioctx.operate(oid, &write_op));
  
  // Verify clone has original state
  IoCtx snap_ioctx;
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), snap_ioctx));
  snap_ioctx.set_namespace(nspace);
  snap_ioctx.snap_set_read(my_snaps[0]);
  
  // Check clone header
  bufferlist clone_header_bl;
  int err = 0;
  ObjectReadOperation read_clone;
  read_clone.omap_get_header(&clone_header_bl, &err);
  ASSERT_EQ(0, snap_ioctx.operate(oid, &read_clone, nullptr));
  ASSERT_EQ(0, err);
  
  std::string clone_header;
  decode(clone_header, clone_header_bl);
  ASSERT_EQ(header1, clone_header);
  
  // Check clone keys
  std::set<std::string> clone_keys;
  ObjectReadOperation read_clone_keys;
  err = 0;
  read_clone_keys.omap_get_keys2("", LONG_MAX, &clone_keys, nullptr, &err);
  ASSERT_EQ(0, snap_ioctx.operate(oid, &read_clone_keys, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(3u, clone_keys.size());
  ASSERT_EQ(1u, clone_keys.count("keep_key"));
  ASSERT_EQ(1u, clone_keys.count("remove_key"));
  ASSERT_EQ(1u, clone_keys.count("modify_key"));
  ASSERT_EQ(0u, clone_keys.count("add_key"));
  
  // Check clone value for modify_key
  std::map<std::string, bufferlist> clone_vals;
  ObjectReadOperation read_clone_vals;
  err = 0;
  read_clone_vals.omap_get_vals2("", LONG_MAX, &clone_vals, nullptr, &err);
  ASSERT_EQ(0, snap_ioctx.operate(oid, &read_clone_vals, nullptr));
  ASSERT_EQ(0, err);
  std::string clone_modify_val;
  decode(clone_modify_val, clone_vals["modify_key"]);
  ASSERT_EQ(value, clone_modify_val);
  
  // Verify head has modified state
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
  
  // Check head header
  bufferlist head_header_bl;
  ObjectReadOperation read_head;
  err = 0;
  read_head.omap_get_header(&head_header_bl, &err);
  ASSERT_EQ(0, ioctx.operate(oid, &read_head, nullptr));
  ASSERT_EQ(0, err);
  
  std::string head_header;
  decode(head_header, head_header_bl);
  ASSERT_EQ(header2, head_header);
  
  // Check head keys
  std::set<std::string> head_keys;
  ObjectReadOperation read_head_keys;
  err = 0;
  read_head_keys.omap_get_keys2("", LONG_MAX, &head_keys, nullptr, &err);
  ASSERT_EQ(0, ioctx.operate(oid, &read_head_keys, nullptr));
  ASSERT_EQ(0, err);
  ASSERT_EQ(3u, head_keys.size());
  ASSERT_EQ(1u, head_keys.count("keep_key"));
  ASSERT_EQ(0u, head_keys.count("remove_key"));
  ASSERT_EQ(1u, head_keys.count("modify_key"));
  ASSERT_EQ(1u, head_keys.count("add_key"));
  
  // Check head value for modify_key
  std::map<std::string, bufferlist> head_vals;
  ObjectReadOperation read_head_vals;
  err = 0;
  read_head_vals.omap_get_vals2("", LONG_MAX, &head_vals, nullptr, &err);
  ASSERT_EQ(0, ioctx.operate(oid, &read_head_vals, nullptr));
  ASSERT_EQ(0, err);
  std::string head_modify_val;
  decode(head_modify_val, head_vals["modify_key"]);
  ASSERT_EQ("new_value", head_modify_val);
  
  // Cleanup
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  snap_ioctx.close();
}

TEST_P(OmapTest, CloneRollback) {
  SKIP_IF_CRIMSON();
  const std::string oid = "test_omap_rollback_basic";
  const std::string initial_header = "initial_header";
  const std::string initial_value = "initial_value";
  
  // Create object with initial OMAP data
  bufferlist header_bl;
  encode(initial_header, header_bl);
  
  bufferlist val_bl;
  encode(initial_value, val_bl);
  std::map<std::string, bufferlist> initial_omap = {
    {"key1", val_bl},
    {"key2", val_bl}
  };
  
  create_test_object_with_omap(oid, initial_omap, header_bl);
  
  // Create snapshot and trigger clone creation
  std::vector<uint64_t> my_snaps;
  create_snapshot_and_set_write_ctx(my_snaps);
  trigger_clone(oid);
  
  // Modify head object OMAP
  const std::string modified_header = "modified_header";
  const std::string modified_value = "modified_value";
  bufferlist mod_header_bl;
  encode(modified_header, mod_header_bl);
  bufferlist mod_val_bl;
  encode(modified_value, mod_val_bl);
  
  ObjectWriteOperation write_op;
  write_op.omap_set_header(mod_header_bl);
  std::map<std::string, bufferlist> modified_omap = {
    {"key3", mod_val_bl}
  };
  write_op.omap_set(modified_omap);
  ASSERT_EQ(0, ioctx.operate(oid, &write_op));
  
  // Verify head has modified OMAP data before rollback
  verify_omap_header(oid, modified_header);
  std::set<std::string> head_expected_keys = {"key1", "key2", "key3"};
  verify_omap_keys(oid, head_expected_keys);
  
  // Perform rollback to snapshot
  ASSERT_EQ(0, ioctx.selfmanaged_snap_rollback(oid, my_snaps[0]));
  
  // Verify head now has initial OMAP state after rollback
  verify_omap_header(oid, initial_header);
  std::set<std::string> rolled_expected_keys = {"key1", "key2"};
  verify_omap_keys(oid, rolled_expected_keys);
  verify_omap_values(oid, initial_omap);
  
  // Cleanup
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
}

TEST_P(OmapTest, OmapCopyFrom) {
  SKIP_IF_CRIMSON();
  const std::string src_oid = "test_omap_copy_from_src";
  const std::string dst_oid = "test_omap_copy_from_dst";
  const std::string dst_oid2 = "test_omap_copy_from_dst2";
  
  // Create source object with OMAP data
  const std::string omap_header = "test_omap_header_for_copy";
  bufferlist omap_header_bl;
  encode(omap_header, omap_header_bl);
  
  auto omap_map = get_test_omap_data();
  create_test_object_with_omap(src_oid, omap_map, omap_header_bl);
  
  // Get the version of the source object
  version_t src_version = ioctx.get_last_version();
  
  // Test 1: Copy with explicit version
  {
    ObjectWriteOperation write_op;
    write_op.copy_from(src_oid, ioctx, src_version, 0);
    int ret = ioctx.operate(dst_oid, &write_op);
    ASSERT_EQ(0, ret);
    
    // Verify OMAP
    verify_omap_state(dst_oid, omap_header, omap_map);
    
    // Verify object data was also copied
    bufferlist data_read;
    ObjectReadOperation read_data;
    read_data.read(0, 4, &data_read, nullptr);
    ret = ioctx.operate(dst_oid, &read_data, nullptr);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, memcmp(data_read.c_str(), "ceph", 4));
  }
  
  // Test 2: Copy with version=0 (any version)
  {
    ObjectWriteOperation write_op;
    write_op.copy_from(src_oid, ioctx, 0, 0);
    int ret = ioctx.operate(dst_oid2, &write_op);
    ASSERT_EQ(0, ret);
    
    // Verify OMAP was copied using helpers
    verify_omap_state(dst_oid2, omap_header, omap_map);
  }
}

TEST_P(OmapTest, OmapCopyFromOverwritesTarget) {
  SKIP_IF_CRIMSON();
  const std::string src_oid = "test_omap_copy_from_src_overwrite";
  const std::string dst_oid = "test_omap_copy_from_dst_overwrite";
  
  // Create source object with OMAP data
  const std::string src_header = "source_header";
  bufferlist src_header_bl;
  encode(src_header, src_header_bl);
  
  std::map<std::string, bufferlist> src_omap_map;
  bufferlist src_val_bl;
  std::string src_value = "source_value";
  encode(src_value, src_val_bl);
  src_omap_map["src_key1"] = src_val_bl;
  src_omap_map["src_key2"] = src_val_bl;
  
  create_test_object_with_omap(src_oid, src_omap_map, src_header_bl);
  
  // Capture source version immediately after creation
  version_t src_version = ioctx.get_last_version();
  
  // Create destination object with DIFFERENT OMAP data
  const std::string dst_header = "destination_header";
  bufferlist dst_header_bl;
  encode(dst_header, dst_header_bl);
  
  std::map<std::string, bufferlist> dst_omap_map;
  bufferlist dst_val_bl;
  std::string dst_value = "destination_value";
  encode(dst_value, dst_val_bl);
  dst_omap_map["dst_key1"] = dst_val_bl;
  dst_omap_map["dst_key2"] = dst_val_bl;
  dst_omap_map["dst_key3"] = dst_val_bl;
  
  create_test_object_with_omap(dst_oid, dst_omap_map, dst_header_bl);
  
  // For EC pools, add another OMAP update to destination to ensure it's in the journal
  if (pool_type == PoolType::FAST_EC) {
    bufferlist updated_dst_header_bl;
    std::string updated_dst_header = "updated_destination_header_in_journal";
    encode(updated_dst_header, updated_dst_header_bl);
    
    ObjectWriteOperation update_op;
    update_op.omap_set_header(updated_dst_header_bl);
    
    // Add a new key that should NOT appear after copy_from
    std::map<std::string, bufferlist> new_dst_keys;
    bufferlist new_dst_val_bl;
    std::string new_dst_value = "new_destination_value_in_journal";
    encode(new_dst_value, new_dst_val_bl);
    new_dst_keys["dst_key_journal_only"] = new_dst_val_bl;
    update_op.omap_set(new_dst_keys);
    
    int ret = ioctx.operate(dst_oid, &update_op);
    ASSERT_EQ(0, ret);
  }
  
  // Perform copy_from to overwrite destination with source
  ObjectWriteOperation write_op;
  write_op.copy_from(src_oid, ioctx, src_version, 0);
  int ret = ioctx.operate(dst_oid, &write_op);
  ASSERT_EQ(0, ret);
  
  // Verify destination now has SOURCE OMAP, not destination OMAP
  verify_omap_state(dst_oid, src_header, src_omap_map);

  // Verify destination keys are NOT present
  std::set<std::string> keys_read;
  int err = 0;
  ObjectReadOperation read_keys;
  read_keys.omap_get_keys2("", LONG_MAX, &keys_read, nullptr, &err);
  ret = ioctx.operate(dst_oid, &read_keys, nullptr);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(0, err);

  ASSERT_EQ(0u, keys_read.count("dst_key1"));
  ASSERT_EQ(0u, keys_read.count("dst_key2"));
  ASSERT_EQ(0u, keys_read.count("dst_key3"));

  // For EC pools, verify journal-only key is NOT present
  if (pool_type == PoolType::FAST_EC) {
    ASSERT_EQ(0u, keys_read.count("dst_key_journal_only"));
  }
}

TEST_P(OmapTest, GenerationalObjectRecovery) {
  SKIP_IF_CRIMSON();
  turn_balancing_off();
  
  const std::string oid = "gen_recovery_test_oid";
  
  // 1. Create object with omap (generation 0/HEAD)
  std::map<std::string, bufferlist> omap_v1;
  bufferlist val1;
  const std::string original_value = "original_generation_value";
  encode(original_value, val1);
  omap_v1["test_key"] = val1;
  
  bufferlist header1;
  const std::string original_header = "original_generation_header";
  encode(original_header, header1);
  
  ObjectWriteOperation write1;
  write1.create(true);  // exclusive create
  write1.omap_set(omap_v1);
  write1.omap_set_header(header1);
  int ret = ioctx.operate(oid, &write1);
  EXPECT_EQ(ret, 0);

  // 2. Delete object (creates a generational tombstone)
  ObjectWriteOperation del_op;
  del_op.remove();
  ret = ioctx.operate(oid, &del_op);
  EXPECT_EQ(ret, 0);

  // 3. Recreate object with DIFFERENT omap (new HEAD, old gen still exists)
  std::map<std::string, bufferlist> omap_v2;
  bufferlist val2;
  const std::string new_head_value = "new_head_generation_value";
  encode(new_head_value, val2);
  omap_v2["test_key"] = val2;
  
  bufferlist header2;
  const std::string new_head_header = "new_head_generation_header";
  encode(new_head_header, header2);
  
  ObjectWriteOperation write2;
  write2.create(true);
  write2.omap_set(omap_v2);
  write2.omap_set_header(header2);
  ret = ioctx.operate(oid, &write2);
  EXPECT_EQ(ret, 0);

  // 4. Get current OSD mapping
  ceph::messaging::osd::OSDMapReply reply;
  int res = request_osd_map(oid, &reply);
  EXPECT_TRUE(res == 0);
  
  std::vector<int> prev_up_osds = reply.up;
  std::string pgid = reply.pgid;
  print_osd_map("Previous up osds: ", prev_up_osds);
  
  // 5. Trigger recovery via upmap IMMEDIATELY (before delete fully completes)
  std::vector<int> new_up_osds = prev_up_osds;
  std::swap(new_up_osds[0], new_up_osds[new_up_osds.size() - 1]);
  int new_primary = new_up_osds[0];
  
  std::cout << "Previous primary osd: " << prev_up_osds[0] << std::endl;
  std::cout << "New primary osd: " << new_primary << std::endl;
  print_osd_map("Desired up osds: ", new_up_osds);
  
  int rc = set_osd_upmap(pgid, new_up_osds);
  EXPECT_TRUE(rc == 0);

  // 6. Wait for recovery to complete
  int res2 = wait_for_upmap(oid, new_primary, 60s);
  EXPECT_TRUE(res2 == 0);

  // 7. Read omap - verify we got the NEW head value, not old generation
  // If the bug exists, recovery might have retrieved data from wrong generation
  std::map<std::string, bufferlist> result_omap;
  std::set<std::string> keys{"test_key"};
  ObjectReadOperation read_op;
  int err = 0;
  read_op.omap_get_vals_by_keys(keys, &result_omap, &err);
  ret = ioctx.operate(oid, &read_op, nullptr);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(err, 0);
  
  ASSERT_EQ(1u, result_omap.size());
  ASSERT_TRUE(result_omap.count("test_key") > 0);
  
  std::string result_val;
  decode(result_val, result_omap["test_key"]);

  // This assertion will FAIL if the bug exists and wrong generation was recovered
  EXPECT_EQ(result_val, new_head_value)
    << "ERROR: Recovered wrong generation! Got '" << result_val
    << "' but expected '" << new_head_value << "'";
  
  // 8. Also verify omap header
  bufferlist result_header;
  ObjectReadOperation read_header_op;
  err = 0;
  read_header_op.omap_get_header(&result_header, &err);
  ret = ioctx.operate(oid, &read_header_op, nullptr);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(err, 0);
  
  std::string result_header_str;
  decode(result_header_str, result_header);

  // This assertion will FAIL if the bug exists and wrong generation header was recovered
  EXPECT_EQ(result_header_str, new_head_header)
    << "ERROR: Recovered wrong generation header! Got '" << result_header_str
    << "' but expected '" << new_head_header << "'";
}

// Instantiate tests for both pool types
INSTANTIATE_TEST_SUITE_P(, OmapTest,
  ::testing::Values(
    PoolType::REPLICATED,
    PoolType::FAST_EC
  ),
  [](const ::testing::TestParamInfo<PoolType>& info) {
    return pool_type_name(info.param);
  }
);
