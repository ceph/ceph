#include <common/perf_counters_collection.h>

#include "test/librados/test_cxx.h"
#include "test/librados/testcase_cxx.h"
#include "crimson_utils.h"
#include "cls/fifo/cls_fifo_ops.h"
#include "cls/version/cls_version_ops.h"
#include <fmt/format.h>
#include <json_spirit/json_spirit.h>

#include "common/ceph_json.h"
#include "common/JSONFormatter.h"
#include "common/json/OSDStructures.h"
#include "librados/librados_asio.h"

#include <boost/asio/io_context.hpp>

#include <algorithm>
#include <climits>
#include <thread>
#include <chrono>

using namespace std;
using namespace librados;

typedef RadosTestPP LibRadosOmapPP;
typedef RadosTestECPP LibRadosOmapECPP;

TEST_P(LibRadosOmapECPP, OmapReads) {
  SKIP_IF_CRIMSON();
  bufferlist bl_write, omap_val_bl, omap_header_bl;
  const std::string omap_key_1 = "omap_key_1_palomino";
  const std::string omap_key_2 = "omap_key_2_chestnut";
  const std::string omap_key_3 = "omap_key_3_bay";
  const std::string omap_key_4 = "omap_key_4_dun";
  const std::string omap_key_5 = "omap_key_5_black";
  const std::string omap_key_6 = "omap_key_6_grey";
  const std::string omap_value = "omap_value_1_horse";
  const std::string omap_header = "my_omap_header";

  encode(omap_value, omap_val_bl);
  encode(omap_header, omap_header_bl);

  std::map<std::string, bufferlist> omap_map = {
    {omap_key_1.c_str(), omap_val_bl},
    {omap_key_2.c_str(), omap_val_bl},
    {omap_key_3.c_str(), omap_val_bl},
    {omap_key_4.c_str(), omap_val_bl},
    {omap_key_5.c_str(), omap_val_bl},
    {omap_key_6.c_str(), omap_val_bl}
  };

  bl_write.append("ceph");
  ObjectWriteOperation write1;
  write1.write(0, bl_write);
  write1.omap_set_header(omap_header_bl);
  write1.omap_set(omap_map);

  std::cout << "Writing object with OMap data..." << std::endl;
  int ret = ioctx.operate("my_object", &write1);
  EXPECT_EQ(ret, 0);

  int err = 0;
  bufferlist bl_read;
  ObjectReadOperation read;

  // OMAP GET VALS TESTING

  read.read(0, bl_write.length(), &bl_read, NULL);

  std::map<std::string, bufferlist> returned_vals_1;
  std::map<std::string, bufferlist> returned_vals_2;

  read.omap_get_vals2(omap_key_1, 1, &returned_vals_1, nullptr, &err);
  read.omap_get_vals2("omap", 4, &returned_vals_2, nullptr, &err);

  std::cout << "Getting OMap values..." << std::endl;
  ret = ioctx.operate("my_object", &read, nullptr);
  EXPECT_EQ(ret, 0);

  ASSERT_EQ(0, memcmp(bl_read.c_str(), "ceph", 4));
  ASSERT_EQ(0, err);
  ASSERT_EQ(returned_vals_1.size(), (unsigned)1);
  ASSERT_EQ(returned_vals_2.size(), (unsigned)4);

  std::cout << "--- OMap Vals testing passed ---" << std::endl;


  // OMAP GET KEYS TESTING

  std::set<std::string> returned_keys;

  read.omap_get_keys2("", LONG_MAX, &returned_keys, nullptr, &err);

  ret = ioctx.operate("my_object", &read, nullptr);
  EXPECT_EQ(ret, 0);

  ASSERT_EQ(0, err);
  ASSERT_EQ(returned_keys.size(), (unsigned)6);

  std::cout << "--- OMap Keys testing passed ---" << std::endl;

  // OMAP GET HEADER TESTING

  bufferlist returned_header_bl;

  read.omap_get_header(&returned_header_bl, &err);

  ret = ioctx.operate("my_object", &read, nullptr);
  EXPECT_EQ(ret, 0);

  std::string returned_header_str;
  decode(returned_header_str, returned_header_bl);

  ASSERT_EQ(0, err);
  ASSERT_EQ(returned_header_str, omap_header);

  std::cout << "--- OMap Header testing passed ---" << std::endl;

  // OMAP GET VALS BY KEYS TESTING

  std::set<std::string> key_filter = { omap_key_1, omap_key_3 };
  std::map<std::string, bufferlist> returned_vals_by_keys;

  read.omap_get_vals_by_keys(key_filter, &returned_vals_by_keys, &err);

  ret = ioctx.operate("my_object", &read, nullptr);
  EXPECT_EQ(ret, 0);

  ASSERT_EQ(0, err);
  ASSERT_EQ(returned_vals_by_keys.size(), (unsigned)2);

  std::cout << "--- OMap Vals by Keys testing passed ---" << std::endl;

  // OMAP CMP TESTING

  map<string, pair<bufferlist, int>> cmp_results;
  bufferlist cmp_val_bl;

  encode(omap_value, cmp_val_bl);

  cmp_results["omap_key_1_palomino"] = pair<bufferlist, int>(cmp_val_bl, CEPH_OSD_CMPXATTR_OP_EQ);

  read.omap_cmp(cmp_results, &err);

  ret = ioctx.operate("my_object", &read, nullptr);
  EXPECT_EQ(ret, 0);

  ASSERT_EQ(0, err);

  std::cout << "--- OMap Cmp testing passed ---" << std::endl;

  // OMAP REMOVE KEYS TESTING

  std::set<std::string> keys_to_remove;
  std::set<std::string> returned_keys_with_removed;

  keys_to_remove.insert("omap_key_2_chestnut");

  write1.omap_rm_keys(keys_to_remove);

  std::cout << "Removing key: omap_key_2_chestnut" << std::endl;
  ret = ioctx.operate("my_object", &write1);
  EXPECT_EQ(ret, 0);

  read.omap_get_keys2("", LONG_MAX, &returned_keys_with_removed, nullptr, &err);

  std::cout << "Getting all keys after removal..." << std::endl;
  ret = ioctx.operate("my_object", &read, nullptr);
  EXPECT_EQ(ret, 0);

  ASSERT_EQ(0, err);
  ASSERT_EQ(returned_keys_with_removed.size(), (unsigned)5);

  std::cout << "--- OMap Remove Keys testing passed ---" << std::endl;

  // OMAP REMOVE RANGE TESTING

  std::set<std::string> returned_keys_with_removed_range;

  write1.omap_rm_range("omap_key_3_bay", "omap_key_5_black");

  ret = ioctx.operate("my_object", &write1);
  EXPECT_EQ(ret, 0);

  read.omap_get_keys2("", 10, &returned_keys_with_removed_range, nullptr, &err);

  ret = ioctx.operate("my_object", &read, nullptr);
  EXPECT_EQ(ret, 0);

  ASSERT_EQ(0, err);
  ASSERT_EQ(returned_keys_with_removed_range.size(), (unsigned)3);

  std::cout << "--- OMap Remove Range testing passed ---" << std::endl;

  // OMAP CLEAR TESTING

  write1.omap_clear();

  ret = ioctx.operate("my_object", &write1);
  EXPECT_EQ(ret, 0);

  read.omap_get_keys2("", LONG_MAX, &returned_keys, nullptr, &err);

  ret = ioctx.operate("my_object", &read, nullptr);
  EXPECT_EQ(ret, 0);

  ASSERT_EQ(0, err);
  ASSERT_EQ(returned_keys.size(), (unsigned)0);

  std::cout << "--- OMap Clear testing passed ---" << std::endl;
}

TEST_P(LibRadosOmapECPP, ErrorInject) {
  SKIP_IF_CRIMSON();
  bufferlist bl_write, omap_val_bl, xattr_val_bl;
  const std::string omap_key_1 = "key_a";
  const std::string omap_key_2 = "key_b";
  const std::string omap_value = "val_c";
  encode(omap_value, omap_val_bl);
  std::map<std::string, bufferlist> omap_map = {
    {omap_key_1, omap_val_bl},
    {omap_key_2, omap_val_bl}
  };
  const std::string xattr_key = "xattr_key_1";
  const std::string xattr_value = "xattr_value_2";
  encode(xattr_value, xattr_val_bl);
  bl_write.append("ceph");
  
  // 1a. Write data to omap
  ObjectWriteOperation write1;
  write1.write(0, bl_write);
  write1.omap_set(omap_map);
  int ret = ioctx.operate("error_inject_oid", &write1);
  EXPECT_EQ(ret, 0);

  // 1b. Write data to xattrs
  write1.setxattr(xattr_key.c_str(), xattr_val_bl);
  ret = ioctx.operate("error_inject_oid", &write1);
  EXPECT_EQ(ret, 0);

  // 2. Set osd_debug_reject_backfill_probability to 1.0
  CephContext* cct = static_cast<CephContext*>(cluster.cct());
  cct->_conf->osd_debug_reject_backfill_probability = 1.0;

  // 3. Read xattrs before switching primary osd
  check_xattr_read("error_inject_oid", xattr_key, xattr_value, 1, 1, 0);

  // 4. Find up osds
  ceph::messaging::osd::OSDMapReply reply;
  int res = request_osd_map(pool_name, "error_inject_oid", nspace, &reply);
  EXPECT_TRUE(res == 0);
  std::vector<int> prev_up_osds = reply.up;
  std::string pgid = reply.pgid;
  print_osd_map("Previous up osds: ", prev_up_osds);
  
  // 5. Find unused osd to be new primary
  int prev_primary = prev_up_osds[0];
  int new_primary = 0;
  while (true) {
    auto it = std::find(prev_up_osds.begin(), prev_up_osds.end(), new_primary);
    if (it == prev_up_osds.end()) {
      break;
    }
    new_primary++;
  }
  std::vector<int> new_up_osds = prev_up_osds;
  new_up_osds[0] = new_primary;
  std::cout << "Previous primary osd: " << prev_primary << std::endl;
  std::cout << "New primary osd: " << new_primary << std::endl;
  print_osd_map("Desired up osds: ", new_up_osds);

  // 6. Set new up map
  int rc = set_osd_upmap(pgid, new_up_osds);
  EXPECT_TRUE(rc == 0);

  // 7. Wait for new upmap to appear as acting set of osds
  int res2 = wait_for_upmap(pool_name, "error_inject_oid", nspace, new_primary, 30s);
  EXPECT_TRUE(res2 == 0);
  
  // 8a. Read omap
  check_omap_read("error_inject_oid", omap_key_1, omap_value, 2, 0);

  // 8b. Read xattrs after switching primary osd
  check_xattr_read("error_inject_oid", xattr_key, xattr_value, 1, 1, 0);

  // 9. Set osd_debug_reject_backfill_probability to 0.0
  cct->_conf->osd_debug_reject_backfill_probability = 0.0;

  // 10. Reset up map to previous values
  int rc2 = set_osd_upmap(pgid, prev_up_osds);
  EXPECT_TRUE(rc2 == 0);

  // 11. Wait for prev upmap to appear as acting set of osds
  int res3 = wait_for_upmap(pool_name, "error_inject_oid", nspace, prev_primary, 30s);
  EXPECT_TRUE(res3 == 0);
}


TEST_P(LibRadosOmapECPP, RemoveOneRange) {
  // 1. Freeze journal
  freeze_omap_journal();

  // 2. Insert keys to omap
  std::set<std::string> expected_keys;
  write_omap_keys("remove_one_range", 1, 100, expected_keys);
  
  // 3. Remove range from omap
  ObjectWriteOperation write;
  write.omap_rm_range("key_020", "key_041");
  int ret = ioctx.operate("remove_one_range", &write);
  EXPECT_EQ(ret, 0);

  auto it_start = expected_keys.lower_bound("key_020");
  auto it_end = expected_keys.upper_bound("key_040");
  expected_keys.erase(it_start, it_end);

  // 4. Read keys from omap
  std::set<std::string> returned_keys;
  read_omap_keys("remove_one_range", returned_keys);

  // 5. Check returned keys
  EXPECT_EQ(returned_keys, expected_keys);

  // 6. Unfreeze journal
  unfreeze_omap_journal();
}

TEST_P(LibRadosOmapECPP, RemoveTwoRanges) {
  // 1. Freeze journal
  freeze_omap_journal();

  // 2. Insert keys to omap
  std::set<std::string> expected_keys;
  write_omap_keys("remove_two_ranges", 1, 100, expected_keys);

  // 3. Remove ranges from omap
  ObjectWriteOperation write;
  write.omap_rm_range("key_020", "key_041");
  write.omap_rm_range("key_060", "key_081");
  int ret = ioctx.operate("remove_two_ranges", &write);
  EXPECT_EQ(ret, 0);

  auto it_start = expected_keys.lower_bound("key_020");
  auto it_end = expected_keys.upper_bound("key_040");
  expected_keys.erase(it_start, it_end);
  it_start = expected_keys.lower_bound("key_060");
  it_end = expected_keys.upper_bound("key_080");
  expected_keys.erase(it_start, it_end);

  // 4. Read keys from omap
  std::set<std::string> returned_keys;
  read_omap_keys("remove_two_ranges", returned_keys);

  // 5. Check returned keys
  EXPECT_EQ(returned_keys, expected_keys);

  // 6. Unfreeze journal
  unfreeze_omap_journal();
}

TEST_P(LibRadosOmapECPP, RightOverlappingRanges) {
  // 1. Freeze journal
  freeze_omap_journal();

  // 2. Insert keys to omap
  std::set<std::string> expected_keys;
  write_omap_keys("right_overlap", 1, 100, expected_keys);

  // 3. Remove ranges from omap
  ObjectWriteOperation write;
  write.omap_rm_range("key_010", "key_021");
  write.omap_rm_range("key_015", "key_031");
  int ret = ioctx.operate("right_overlap", &write);
  EXPECT_EQ(ret, 0);

  auto it_start = expected_keys.lower_bound("key_010");
  auto it_end = expected_keys.upper_bound("key_030");
  expected_keys.erase(it_start, it_end);
  
  // 4. Read keys from omap
  std::set<std::string> returned_keys;
  read_omap_keys("right_overlap", returned_keys);

  // 5. Check returned keys
  EXPECT_EQ(returned_keys, expected_keys);

  // 6. Unfreeze journal
  unfreeze_omap_journal();
}

TEST_P(LibRadosOmapECPP, LeftOverlappingRanges) {
  // 1. Freeze journal
  freeze_omap_journal();

  // 2. Insert keys to omap
  std::set<std::string> expected_keys;
  write_omap_keys("left_overlap", 1, 100, expected_keys);

  // 3. Remove ranges from omap
  ObjectWriteOperation write;
  write.omap_rm_range("key_015", "key_031");
  write.omap_rm_range("key_010", "key_021");
  int ret = ioctx.operate("left_overlap", &write);
  EXPECT_EQ(ret, 0);

  auto it_start = expected_keys.lower_bound("key_010");
  auto it_end = expected_keys.upper_bound("key_030");
  expected_keys.erase(it_start, it_end);

  // 4. Read keys from omap
  std::set<std::string> returned_keys;
  read_omap_keys("left_overlap", returned_keys);

  // 5. Check returned keys
  EXPECT_EQ(returned_keys, expected_keys);

  // 6. Unfreeze journal
  unfreeze_omap_journal();
}

TEST_P(LibRadosOmapECPP, FullyOverlappingRanges) {
  // 1. Freeze journal
  freeze_omap_journal();

  // 2. Insert keys to omap
  std::set<std::string> expected_keys;
  write_omap_keys("full_overlap", 1, 100, expected_keys);

  // 3. Remove ranges from omap
  ObjectWriteOperation write;
  write.omap_rm_range("key_015", "key_026");
  write.omap_rm_range("key_010", "key_031");
  int ret = ioctx.operate("full_overlap", &write);
  EXPECT_EQ(ret, 0);

  auto it_start = expected_keys.lower_bound("key_010");
  auto it_end = expected_keys.upper_bound("key_030");
  expected_keys.erase(it_start, it_end);

  // 4. Read keys from omap
  std::set<std::string> returned_keys;
  read_omap_keys("full_overlap", returned_keys);

  // 5. Check returned keys
  EXPECT_EQ(returned_keys, expected_keys);
  
  // 6. Unfreeze journal
  unfreeze_omap_journal();
}
TEST_P(LibRadosOmapECPP, FullyOverlappingRanges2) {
  // 1. Freeze journal
  freeze_omap_journal();

  // 2. Insert keys to omap
  std::set<std::string> expected_keys;
  write_omap_keys("full_overlap_2", 1, 100, expected_keys);

  // 3. Remove ranges from omap
  ObjectWriteOperation write;
  write.omap_rm_range("key_010", "key_031");
  write.omap_rm_range("key_015", "key_026");
  int ret = ioctx.operate("full_overlap_2", &write);
  EXPECT_EQ(ret, 0);

  auto it_start = expected_keys.lower_bound("key_010");
  auto it_end = expected_keys.upper_bound("key_030");
  expected_keys.erase(it_start, it_end);

  // 4. Read keys from omap
  std::set<std::string> returned_keys;
  read_omap_keys("full_overlap_2", returned_keys);

  // 5. Check returned keys
  EXPECT_EQ(returned_keys, expected_keys);
  
  // 6. Unfreeze journal
  unfreeze_omap_journal();
}

TEST_P(LibRadosOmapECPP, RemoveWithNullStart) {
  // 1. Freeze journal
  freeze_omap_journal();

  // 2. Insert keys to omap
  std::set<std::string> expected_keys;
  write_omap_keys("remove_null_start", 1, 100, expected_keys);

  // 3. Remove range from omap
  ObjectWriteOperation write;
  write.omap_rm_range("", "key_021");
  int ret = ioctx.operate("remove_null_start", &write);
  EXPECT_EQ(ret, 0);

  auto it_start = expected_keys.begin();
  auto it_end = expected_keys.upper_bound("key_020");
  expected_keys.erase(it_start, it_end);

  // 4. Read keys from omap
  std::set<std::string> returned_keys;
  read_omap_keys("remove_null_start", returned_keys);

  // 5. Check returned keys
  EXPECT_EQ(returned_keys, expected_keys);
  
  // 6. Unfreeze journal
  unfreeze_omap_journal();
}
  
TEST_P(LibRadosOmapECPP, OverlapWithNullStart) {
  // 1. Freeze journal
  freeze_omap_journal();

  // 2. Insert keys to omap
  std::set<std::string> expected_keys;
  write_omap_keys("overlap_null_start", 1, 100, expected_keys);

  // 3. Remove ranges from omap
  ObjectWriteOperation write;
  write.omap_rm_range("", "key_031");
  write.omap_rm_range("key_020", "key_051");
  int ret = ioctx.operate("overlap_null_start", &write);
  EXPECT_EQ(ret, 0);

  auto it_start = expected_keys.begin();
  auto it_end = expected_keys.upper_bound("key_050");
  expected_keys.erase(it_start, it_end);
  
  // 4. Read keys from omap
  std::set<std::string> returned_keys;
  read_omap_keys("overlap_null_start", returned_keys);

  // 5. Check returned keys
  EXPECT_EQ(returned_keys, expected_keys);

  // 6. Unfreeze journal
  unfreeze_omap_journal();
}

TEST_P(LibRadosOmapECPP, RemoveRangeThenInsert) {

  // 1. Freeze journal
  freeze_omap_journal();

  // 2. Insert keys to omap
  std::set<std::string> expected_keys;
  write_omap_keys("remove_then_insert", 1, 100, expected_keys);

  // 3. Remove range from omap
  ObjectWriteOperation write;
  write.omap_rm_range("key_030", "key_061");
  int ret = ioctx.operate("remove_then_insert", &write);
  EXPECT_EQ(ret, 0);

  auto it_start = expected_keys.lower_bound("key_030");
  auto it_end = expected_keys.upper_bound("key_060");
  expected_keys.erase(it_start, it_end);
  
  // 4. Insert key into omap
  write_omap_keys("remove_then_insert", 45, 45, expected_keys);

  // 5. Read keys from omap
  std::set<std::string> returned_keys;
  read_omap_keys("remove_then_insert", returned_keys);

  // 6. Check returned keys
  EXPECT_EQ(returned_keys, expected_keys);

  // 7. Unfreeze journal
  unfreeze_omap_journal();
}

TEST_P(LibRadosOmapECPP, RemoveRangeThenInsertThenRemoveRange) {

  // 1. Freeze journal
  freeze_omap_journal();

  // 2. Insert keys to omap
  std::set<std::string> expected_keys;
  write_omap_keys("remove_insert_remove", 1, 100, expected_keys);

  // 3. Remove range from omap
  ObjectWriteOperation write;
  write.omap_rm_range("key_030", "key_061");
  int ret = ioctx.operate("remove_insert_remove", &write);
  EXPECT_EQ(ret, 0);

  auto it_start = expected_keys.lower_bound("key_030");
  auto it_end = expected_keys.upper_bound("key_060");
  expected_keys.erase(it_start, it_end);

  // 4. Insert key into omap
  write_omap_keys("remove_insert_remove", 45, 45, expected_keys);

  // 5. Remove range from omap
  write.omap_rm_range("key_030", "key_061");
  ret = ioctx.operate("remove_insert_remove", &write);
  EXPECT_EQ(ret, 0);

  it_start = expected_keys.lower_bound("key_030");
  it_end = expected_keys.upper_bound("key_060");
  expected_keys.erase(it_start, it_end);

  // 6. Read keys from omap
  std::set<std::string> returned_keys;
  read_omap_keys("remove_insert_remove", returned_keys);

  // 7. Check returned keys
  EXPECT_EQ(returned_keys, expected_keys);

  // 8. Unfreeze journal
  unfreeze_omap_journal();
}

TEST_P(LibRadosOmapECPP, FrozenJournalReads) {

  // 1. Freeze journal
  freeze_omap_journal();

  // 2. Insert keys to omap
  std::set<std::string> expected_keys;
  std::set<std::string> returned_keys;

  write_omap_keys("frozen_journal_reads", 1, 50, expected_keys);

  read_omap_keys("frozen_journal_reads", returned_keys);

  EXPECT_EQ(returned_keys, expected_keys);
  std::cout << "Size of Returned Keys: " << returned_keys.size() << std::endl;
  
  // 3. Remove specific keys and remove a range
  ObjectWriteOperation write;
  std::set<std::string> keys_to_remove;

  for (int i = 11; i <= 20; i++) {
    std::stringstream key_ss;
    key_ss << "key_" << std::setw(3) << std::setfill('0') << i;
    keys_to_remove.insert(key_ss.str());
  }

  write.omap_rm_keys(keys_to_remove);
  write.omap_rm_range("key_031", "key_041");

  auto it_start = expected_keys.lower_bound("key_011");
  auto it_end = expected_keys.upper_bound("key_020");
  expected_keys.erase(it_start, it_end);

  it_start = expected_keys.lower_bound("key_031");
  it_end = expected_keys.upper_bound("key_040");
  expected_keys.erase(it_start, it_end);

  int ret = ioctx.operate("frozen_journal_reads", &write);
  EXPECT_EQ(ret, 0);
  
  // 4. Read keys from omap
  returned_keys.clear();
  read_omap_keys("frozen_journal_reads", returned_keys);

  EXPECT_EQ(returned_keys, expected_keys);

  // 5. Unfreeze journal
  unfreeze_omap_journal();
}

TEST_P(LibRadosOmapECPP, MixedBluestoreJournalReads) {

  // 1. Insert keys and allow journal to trim
  std::set<std::string> expected_keys;
  std::set<std::string> returned_keys;

  write_omap_keys("mixed_bluestore_journal_reads", 1, 25, expected_keys);

  read_omap_keys("mixed_bluestore_journal_reads", returned_keys);

  EXPECT_EQ(returned_keys, expected_keys);

  // 2. Wait for Journal to trim then Freeze journal
  sleep(2);
  freeze_omap_journal();
  
  // 3. Add some more keys (now some exist in Bluestore, some in journal)
  write_omap_keys("mixed_bluestore_journal_reads", 26, 50, expected_keys);

  returned_keys.clear();

  read_omap_keys("mixed_bluestore_journal_reads", returned_keys);

  EXPECT_EQ(returned_keys, expected_keys);

  // 4. Remove keys that exist in Bluestore
  std::set<std::string> keys_to_remove;

  ObjectWriteOperation write;
  write.omap_rm_range("key_011", "key_016");

  int ret = ioctx.operate("mixed_bluestore_journal_reads", &write);
  EXPECT_EQ(ret, 0);

  auto it_start = expected_keys.lower_bound("key_011");
  auto it_end = expected_keys.upper_bound("key_015");
  expected_keys.erase(it_start, it_end);

  // 5. Read keys from omap
  returned_keys.clear();

  read_omap_keys("mixed_bluestore_journal_reads", returned_keys);

  EXPECT_EQ(returned_keys, expected_keys);

  // 6. Unfreeze journal
  unfreeze_omap_journal();
}

INSTANTIATE_TEST_SUITE_P_EC(LibRadosOmapECPP);
