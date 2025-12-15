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

  int ret = ioctx.operate("my_object", &write1);
  EXPECT_EQ(ret, 0);

  int err = 0;
  bufferlist bl_read;
  ObjectReadOperation read;

  // OMAP GET VALS TESTING

  read.read(0, bl_write.length(), &bl_read, nullptr);

  std::map<std::string, bufferlist> returned_vals_1;
  std::map<std::string, bufferlist> returned_vals_2;

  read.omap_get_vals2(omap_key_1, 1, &returned_vals_1, nullptr, &err);
  read.omap_get_vals2("omap", 4, &returned_vals_2, nullptr, &err);

  ret = ioctx.operate("my_object", &read, nullptr);
  EXPECT_EQ(ret, 0);

  ASSERT_EQ(0, memcmp(bl_read.c_str(), "ceph", 4));
  ASSERT_EQ(0, err);
  ASSERT_EQ(1u, returned_vals_1.size());
  ASSERT_EQ(4u, returned_vals_2.size());

  // OMAP GET KEYS TESTING

  std::set<std::string> returned_keys;

  read.omap_get_keys2("", LONG_MAX, &returned_keys, nullptr, &err);

  ret = ioctx.operate("my_object", &read, nullptr);
  EXPECT_EQ(ret, 0);

  ASSERT_EQ(0, err);
  ASSERT_EQ(6u, returned_keys.size());

  // OMAP GET HEADER TESTING

  bufferlist returned_header_bl;

  read.omap_get_header(&returned_header_bl, &err);

  ret = ioctx.operate("my_object", &read, nullptr);
  EXPECT_EQ(ret, 0);

  std::string returned_header_str;
  decode(returned_header_str, returned_header_bl);

  ASSERT_EQ(0, err);
  ASSERT_EQ(returned_header_str, omap_header);

  // OMAP GET VALS BY KEYS TESTING

  std::set<std::string> key_filter = { omap_key_1, omap_key_3 };
  std::map<std::string, bufferlist> returned_vals_by_keys;

  read.omap_get_vals_by_keys(key_filter, &returned_vals_by_keys, &err);

  ret = ioctx.operate("my_object", &read, nullptr);
  EXPECT_EQ(ret, 0);

  ASSERT_EQ(0, err);
  ASSERT_EQ(2u, returned_vals_by_keys.size());

  // OMAP CMP TESTING

  map<string, pair<bufferlist, int>> cmp_results;
  bufferlist cmp_val_bl;

  encode(omap_value, cmp_val_bl);

  cmp_results["omap_key_1_palomino"] = pair<bufferlist, int>(cmp_val_bl, CEPH_OSD_CMPXATTR_OP_EQ);

  read.omap_cmp(cmp_results, &err);

  ret = ioctx.operate("my_object", &read, nullptr);
  EXPECT_EQ(ret, 0);

  ASSERT_EQ(0, err);

  // OMAP REMOVE KEYS TESTING

  std::set<std::string> keys_to_remove;
  std::set<std::string> returned_keys_with_removed;

  keys_to_remove.insert("omap_key_2_chestnut");

  write1.omap_rm_keys(keys_to_remove);

  ret = ioctx.operate("my_object", &write1);
  EXPECT_EQ(ret, 0);

  read.omap_get_keys2("", LONG_MAX, &returned_keys_with_removed, nullptr, &err);

  ret = ioctx.operate("my_object", &read, nullptr);
  EXPECT_EQ(ret, 0);

  ASSERT_EQ(0, err);
  ASSERT_EQ(5u, returned_keys_with_removed.size());

  // OMAP REMOVE RANGE TESTING

  std::set<std::string> returned_keys_with_removed_range;

  write1.omap_rm_range("omap_key_3_bay", "omap_key_5_black");

  ret = ioctx.operate("my_object", &write1);
  EXPECT_EQ(ret, 0);

  read.omap_get_keys2("", 10, &returned_keys_with_removed_range, nullptr, &err);

  ret = ioctx.operate("my_object", &read, nullptr);
  EXPECT_EQ(ret, 0);

  ASSERT_EQ(0, err);
  ASSERT_EQ(3u, returned_keys_with_removed_range.size());

  // OMAP CLEAR TESTING

  write1.omap_clear();

  ret = ioctx.operate("my_object", &write1);
  EXPECT_EQ(ret, 0);

  read.omap_get_keys2("", LONG_MAX, &returned_keys, nullptr, &err);

  ret = ioctx.operate("my_object", &read, nullptr);
  EXPECT_EQ(ret, 0);

  ASSERT_EQ(0, err);
  ASSERT_TRUE(returned_keys.empty());

}

// This test is destructive to other tests running in parallel
// Do not merge this into the main branch
TEST_P(LibRadosOmapECPP, OmapRecovery) {
  SKIP_IF_CRIMSON();
  turn_balancing_off();
  bufferlist bl_write, omap_val_bl;
  const std::string omap_key_1 = "key_a";
  const std::string omap_key_2 = "key_b";
  const std::string omap_value = "val_c";
  encode(omap_value, omap_val_bl);
  std::map<std::string, bufferlist> omap_map = {
  {omap_key_1, omap_val_bl},
  {omap_key_2, omap_val_bl}
  };
  bl_write.append("ceph");

  // 1. Write to OMAP
  ObjectWriteOperation write1;
  write1.write(0, bl_write);
  write1.omap_set(omap_map);
  int ret = ioctx.operate("omap_recovery_oid", &write1);
  EXPECT_EQ(ret, 0);

  // 2. Find Primary OSD
  ceph::messaging::osd::OSDMapReply reply;
  int res = request_osd_map(pool_name, "omap_recovery_oid", nspace, &reply);
  EXPECT_EQ(res, 0);
  int victim_osd = reply.acting_primary;
  std::string pgid = reply.pgid;

  std::cout << "Target Object in PG " << pgid << " on Primary OSD." << victim_osd << std::endl;

  // 3. Mark Primary OSD as Down and Out
  std::cout << "Marking OSD." << victim_osd << " down and out..." << std::endl;
  bufferlist inbl, outbl;
  std::ostringstream cmd_down, cmd_out;
  cmd_out << "{\"prefix\": \"osd out\", \"ids\": [\"" << victim_osd << "\"]}";
  ret = cluster.mon_command(cmd_out.str(), std::move(inbl), &outbl, nullptr);
  EXPECT_EQ(ret, 0);
  cmd_down << "{\"prefix\": \"osd down\", \"ids\": [\"" << victim_osd << "\"]}";
  ret = cluster.mon_command(cmd_down.str(), std::move(inbl), &outbl, nullptr);
  EXPECT_EQ(ret, 0);

  // 4. Wait for PG to be Active + Clean
  std::cout << "Waiting for PG " << pgid << " to recover (active+clean)..." << std::endl;

  bool recovered = false;
  for (int i = 0; i < 120; ++i) {
    std::string state;
    bufferlist inbl2, outbl2;
    std::string cmd = "{\"prefix\": \"pg dump\", \"pgid\": \"" + pgid + "\"}";
    cluster.mon_command(std::move(cmd), std::move(inbl2), &outbl2, nullptr);
    std::string out_str = outbl2.to_str();
    if (out_str.find("active+clean") != std::string::npos) {
      recovered = true;
      break;
    }
    sleep(1);
  }
  EXPECT_TRUE(recovered) << "Timed out waiting for recovery on " << pgid;

  // 5. Read OMAP
  check_omap_read("omap_recovery_oid", omap_key_1, omap_value, 2, 0);

  // 6. Deep Scrub the PG
  std::cout << "Forcing Deep Scrub to verify OMAP integrity..." << std::endl;
  std::ostringstream cmd_scrub;
  cmd_scrub << "{\"prefix\": \"pg deep-scrub\", \"pgid\": \"" << pgid << "\"}";
  cluster.mon_command(cmd_scrub.str(), bufferlist(), &outbl, nullptr);

  // 7. Wait for deep scrub to finish
  sleep(5);
  bool clean_after_scrub = false;
  for (int i = 0; i < 60; ++i) {
    bufferlist in, out;
    std::string cmd = "{\"prefix\": \"pg dump\", \"pgid\": \"" + pgid + "\"}";
    cluster.mon_command(std::move(cmd), std::move(in), &out, nullptr);
    if (out.to_str().find("active+clean") != std::string::npos) {
      clean_after_scrub = true;
      break;
    }
    sleep(1);
  }
  EXPECT_TRUE(clean_after_scrub);

  // 8. Check for inconsistency
  bufferlist q_in, q_out;
  std::string q_cmd = "{\"prefix\": \"pg query\", \"pgid\": \"" + pgid + "\"}";
  cluster.mon_command(std::move(q_cmd), std::move(q_in), &q_out, nullptr);
  std::string query_res = q_out.to_str();
  if (query_res.find("inconsistent") != std::string::npos) {
    ADD_FAILURE() << "PG " << pgid << " is inconsistent after OMAP recovery! Scrub failed.";
  }

  // 7. Bring Old Primary OSD Back In
  std::ostringstream cmd_in;
  bufferlist inbl3;
  cmd_in << "{\"prefix\": \"osd in\", \"ids\": [\"" << victim_osd << "\"]}";
  ret = cluster.mon_command(cmd_in.str(), std::move(inbl3), &outbl, nullptr);
  EXPECT_EQ(ret, 0);

  turn_balancing_on();
}

TEST_P(LibRadosOmapECPP, ChangeUpmap) {
  SKIP_IF_CRIMSON();
  turn_balancing_off();
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
  
  // 1. Write data to omap
  ObjectWriteOperation write1;
  write1.write(0, bl_write);
  write1.omap_set(omap_map);
  int ret = ioctx.operate("change_upmap_oid", &write1);
  EXPECT_EQ(ret, 0);

  // 2. Find up osds
  ceph::messaging::osd::OSDMapReply reply;
  int res = request_osd_map(pool_name, "change_upmap_oid", nspace, &reply);
  EXPECT_TRUE(res == 0);
  std::vector<int> prev_up_osds = reply.up;
  std::string pgid = reply.pgid;
  print_osd_map("Previous up osds: ", prev_up_osds);
  
  // 3. Find unused osd to be new primary
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

  // 4. Set new up map
  int rc = set_osd_upmap(pgid, new_up_osds);
  EXPECT_TRUE(rc == 0);

  // 5. Wait for new upmap to appear as acting set of osds
  int res2 = wait_for_upmap(pool_name, "change_upmap_oid", nspace, new_primary, 30s);
  EXPECT_TRUE(res2 == 0);
  
  // 6. Read omap
  check_omap_read("change_upmap_oid", omap_key_1, omap_value, 2, 0);

  turn_balancing_on();
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

  // 2. Wait for Journal to trim then Freeze journal (Write should force a trim and guarantee other shards have the data)
  bufferlist bl_write;

  bl_write.append("ceph");
  ObjectWriteOperation write;
  write.write(0, bl_write);
  int ret = ioctx.operate("mixed_bluestore_journal_reads", &write);
  EXPECT_EQ(ret, 0);

  freeze_omap_journal();
  
  // 3. Add some more keys (now some exist in Bluestore, some in journal)
  write_omap_keys("mixed_bluestore_journal_reads", 26, 50, expected_keys);

  returned_keys.clear();

  read_omap_keys("mixed_bluestore_journal_reads", returned_keys);

  EXPECT_EQ(returned_keys, expected_keys);

  // 4. Remove keys that exist in Bluestore
  std::set<std::string> keys_to_remove;

  write.omap_rm_range("key_011", "key_016");

  ret = ioctx.operate("mixed_bluestore_journal_reads", &write);
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

TEST_P(LibRadosOmapECPP, IteratorStopsButKeysStillInJounral)
{
  // 1. Insert keys and allow journal to trim
  std::set<std::string> expected_keys;
  std::set<std::string> returned_keys;

  write_omap_keys("Iterator-stop", 1, 25, expected_keys);

  read_omap_keys("Iterator-stop", returned_keys);

  EXPECT_EQ(returned_keys, expected_keys);

  // 2. Wait for Journal to trim then Freeze journal (Write should force a trim and guarantee other shards have the data)
  bufferlist bl_write;

  bl_write.append("ceph");
  ObjectWriteOperation write;
  write.write(0, bl_write);
  int ret = ioctx.operate("Iterator-stop", &write);
  EXPECT_EQ(ret, 0);

  freeze_omap_journal();

  // 3. Add some more keys (now some exist in Bluestore, some in journal)

  write_omap_keys("Iterator-stop", 26, 50, expected_keys);

  returned_keys.clear();

  read_omap_keys("Iterator-stop", returned_keys, 20);

  EXPECT_EQ(20u, returned_keys.size());

  // 4. Unfreeze journal
  unfreeze_omap_journal();
}

INSTANTIATE_TEST_SUITE_P_EC(LibRadosOmapECPP);
