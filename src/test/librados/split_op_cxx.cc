#include <common/perf_counters_collection.h>
#include "common/perf_counters_key.h"

#include <fmt/format.h>
#include "test/librados/test_cxx.h"
#include "test/librados/testcase_cxx.h"
#include "crimson_utils.h"
#include "cls/fifo/cls_fifo_ops.h"
#include "cls/version/cls_version_ops.h"
#include "erasure-code/consistency/RadosCommands.h"
#include "common/ceph_json.h"

using namespace std;
using namespace librados;
using namespace cls;
using namespace rados::cls;

namespace {
// Misordered reassembly of split reads is invisible with all-zero data,
// so tests that verify read contents need a patterned buffer.
bufferlist make_pattern_bl(uint64_t len) {
  bufferlist bl;
  std::string s;
  s.reserve(len);
  for (uint64_t i = 0; i < len; i++) {
    s.push_back(static_cast<char>((i * 131) % 251));
  }
  bl.append(s);
  return bl;
}
} // namespace

class LibRadosSplitOpPP : public RadosTestPP {
public:
  static void SetUpTestCase() {
    auto pool_prefix = fmt::format("{}_", ::testing::UnitTest::GetInstance()->current_test_case()->name());
    pool_name_default = get_temp_pool_name(pool_prefix);
    std::map<std::string, std::string> config = {{"rados_replica_read_policy", "default"}};
    ASSERT_EQ("", connect_cluster_pp(s_cluster, config));
    ASSERT_EQ("", create_pool_pp(pool_name_default, s_cluster, 3));
    s_cluster.wait_for_latest_osdmap();
  }

protected:
  // Write a second object in the same PG; that flushes the commit of the
  // preceding write, which split reads need. Uses ASSERT_*.
  void stabilize_pg_log(const bufferlist &bl) {
    uint32_t hash_position;
    ASSERT_EQ(0, ioctx.get_object_pg_hash_position2("foo", &hash_position));

    std::string other_object = "other";
    while (true) {
      uint32_t hash_position2;
      ASSERT_EQ(0, ioctx.get_object_pg_hash_position2(other_object, &hash_position2));
      if (hash_position == hash_position2) {
        break;
      }
      other_object += ".";
    }
    ObjectWriteOperation write2;
    write2.write(0, bl);
    ASSERT_TRUE(AssertOperateWithoutSplitOp(0, other_object, &write2));
  }

  // Write num_slices * osd_min_split_replica_read_size + extra_bytes of
  // patterned data to "foo". Uses ASSERT_*.
  void write_pattern_object(uint64_t num_slices, uint64_t extra_bytes,
                            bufferlist *written) {
    std::string min_split_size_str;
    ASSERT_EQ(0, cluster.conf_get("osd_min_split_replica_read_size", min_split_size_str));
    uint64_t min_split_size = std::stoull(min_split_size_str);
    // With split ops disabled the option is 0; keep the data non-empty so
    // the content checks are meaningful in the non-split variant too.
    if (min_split_size == 0) {
      min_split_size = 262144;
    }

    bufferlist bl = make_pattern_bl(num_slices * min_split_size + extra_bytes);
    ObjectWriteOperation write1;
    write1.write(0, bl);
    ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));
    ASSERT_NO_FATAL_FAILURE(stabilize_pg_log(bl));

    *written = std::move(bl);
  }
};

typedef RadosTestECPP LibRadosSplitOpECPP;

TEST_P(LibRadosSplitOpPP, BigRead) {
  SKIP_IF_CRIMSON();
  std::string min_split_size_str;
  ASSERT_EQ(0, cluster.conf_get("osd_min_split_replica_read_size", min_split_size_str));
  uint64_t min_split_size = std::stoull(min_split_size_str);
  bufferlist bl;
  bl.append_zero(3 * min_split_size);
  ObjectWriteOperation write1;
  write1.write(0, bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));
  ASSERT_NO_FATAL_FAILURE(stabilize_pg_log(bl));

  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);
  ASSERT_TRUE(AssertOperateWithSplitOp(0, 3, "foo", &read, &bl, balanced_read_flags));
}

TEST_P(LibRadosSplitOpPP, ReadTwoShards) {
  SKIP_IF_CRIMSON();
  // Read the osd_min_split_replica_read_size config value
  std::string min_split_size_str;
  ASSERT_EQ(0, cluster.conf_get("osd_min_split_replica_read_size", min_split_size_str));
  uint64_t min_split_size = std::stoull(min_split_size_str);
  
  // Write data large enough to cover multiple shards
  bufferlist bl;
  bl.append_zero(min_split_size * 3);
  ObjectWriteOperation write1;
  write1.write(0, bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));
  ASSERT_NO_FATAL_FAILURE(stabilize_pg_log(bl));

  // Test 1: Read exactly osd_min_split_replica_read_size - should NOT split
  {
    ObjectReadOperation read;
    bufferlist read_bl;
    read.read(0, min_split_size, NULL, NULL);
    ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &read, &read_bl, balanced_read_flags));
  }

  // Test 2: Read osd_min_split_replica_read_size * 2 - 1 - should NOT split
  {
    ObjectReadOperation read;
    bufferlist read_bl;
    read.read(0, min_split_size * 2 - 1, NULL, NULL);
    ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &read, &read_bl, balanced_read_flags));
  }

  // Test 3: Read exactly osd_min_split_replica_read_size * 2 - should split into 2
  {
    ObjectReadOperation read;
    bufferlist read_bl;
    read.read(0, min_split_size * 2, NULL, NULL);
    ASSERT_TRUE(AssertOperateWithSplitOp(0, 2, "foo", &read, &read_bl, balanced_read_flags));
  }

  // Test 4: Read osd_min_split_replica_read_size * 3 - 1 - should split into 2
  {
    ObjectReadOperation read;
    bufferlist read_bl;
    read.read(0, min_split_size * 3 - 1, NULL, NULL);
    ASSERT_TRUE(AssertOperateWithSplitOp(0, 2, "foo", &read, &read_bl, balanced_read_flags));
  }

  // Test 5: Read osd_min_split_replica_read_size * 3 - should split into 3
  {
    ObjectReadOperation read;
    bufferlist read_bl;
    read.read(0, min_split_size * 3, NULL, NULL);
    ASSERT_TRUE(AssertOperateWithSplitOp(0, 3, "foo", &read, &read_bl, balanced_read_flags));
  }
}

TEST_P(LibRadosSplitOpPP, StatBeforeRead) {
  SKIP_IF_CRIMSON();
  // Read the osd_min_split_replica_read_size config value
  std::string min_split_size_str;
  ASSERT_EQ(0, cluster.conf_get("osd_min_split_replica_read_size", min_split_size_str));
  uint64_t min_split_size = std::stoull(min_split_size_str);
  
  // Use buffer at least 2x min_split_size to force splitting across replicas
  bufferlist bl;
  bl.append_zero(min_split_size * 2);
  ObjectWriteOperation write1;
  write1.write(0, bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));
  ASSERT_NO_FATAL_FAILURE(stabilize_pg_log(bl));

  // This test verifies the bug fix: STAT operation comes BEFORE READ
  // In ReplicaSplitOp::init(), when processing ops in order:
  // 1. STAT is processed first (ops_index=0)
  // 2. It goes to default case which tries to access reference_sub_read
  // 3. But reference_sub_read must be set by init_reference_sub_read() first
  // 4. The fix ensures init_reference_sub_read() is called before processing any ops
  
  ObjectReadOperation read;
  uint64_t size;
  timespec time;
  time.tv_nsec = 0;
  time.tv_sec = 0;
  int stat_rval;
  bufferlist read_bl;
  int read_rval;
  
  read.stat2(&size, &time, &stat_rval);  // STAT comes FIRST
  read.read(0, bl.length(), &read_bl, &read_rval);  // READ comes SECOND (spans multiple replicas)

  // This operation should succeed with the bug fix
  bufferlist result_bl;
  ASSERT_TRUE(AssertOperateWithSplitOp(0, 2, "foo", &read, &result_bl, balanced_read_flags));
  ASSERT_EQ(0, stat_rval);
  ASSERT_EQ(0, read_rval);
  ASSERT_EQ(min_split_size * 2, size);
  ASSERT_EQ(min_split_size * 2, read_bl.length());
}

TEST_P(LibRadosSplitOpPP, GetXattrBeforeRead) {
  SKIP_IF_CRIMSON();
  // Read the osd_min_split_replica_read_size config value
  std::string min_split_size_str;
  ASSERT_EQ(0, cluster.conf_get("osd_min_split_replica_read_size", min_split_size_str));
  uint64_t min_split_size = std::stoull(min_split_size_str);
  
  // Use buffer at least 2x min_split_size to force splitting across replicas
  bufferlist bl, attr_bl, attr_read_bl;
  std::string attr_key = "my_key";
  std::string attr_value = "my_attr";

  bl.append_zero(min_split_size * 2);
  ObjectWriteOperation write1;
  write1.write(0, bl);
  encode(attr_value, attr_bl);
  write1.setxattr(attr_key.c_str(), attr_bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));
  ASSERT_NO_FATAL_FAILURE(stabilize_pg_log(bl));

  // Another variant of the bug: GETXATTR before READ
  // This verifies that init_reference_sub_read() is called before processing GETXATTR
  ObjectReadOperation read;
  int getxattr_rval;
  bufferlist read_bl;
  int read_rval;
  
  read.getxattr(attr_key.c_str(), &attr_read_bl, &getxattr_rval);  // GETXATTR FIRST
  read.read(0, bl.length(), &read_bl, &read_rval);  // READ SECOND (spans multiple replicas)

  bufferlist result_bl;
  ASSERT_TRUE(AssertOperateWithSplitOp(0, 2, "foo", &read, &result_bl, balanced_read_flags));
  ASSERT_EQ(0, getxattr_rval);
  ASSERT_EQ(0, read_rval);
  ASSERT_EQ(min_split_size * 2, read_bl.length());
}

TEST_P(LibRadosSplitOpPP, BigReadPattern) {
  SKIP_IF_CRIMSON();
  bufferlist bl;
  ASSERT_NO_FATAL_FAILURE(write_pattern_object(3, 0, &bl));

  // The chunk window starts at a random acting index; repeat so that a
  // wrapped window is exercised.
  for (int i = 0; i < 8; i++) {
    ObjectReadOperation read;
    bufferlist read_bl;
    read.read(0, bl.length(), NULL, NULL);
    ASSERT_TRUE(AssertOperateWithSplitOp(0, 3, "foo", &read, &read_bl, balanced_read_flags));
    ASSERT_EQ(bl.length(), read_bl.length());
    ASSERT_EQ(0, memcmp(bl.c_str(), read_bl.c_str(), bl.length()));
  }
}

TEST_P(LibRadosSplitOpPP, UnalignedBigRead) {
  SKIP_IF_CRIMSON();
  // One byte past three slices: a rounded-down chunk size produces a
  // fourth chunk, which used to wrap the window fully around and attach
  // two reads with a shared buffer to one sub-read.
  bufferlist bl;
  ASSERT_NO_FATAL_FAILURE(write_pattern_object(3, 1, &bl));

  ObjectReadOperation read;
  bufferlist read_bl;
  read.read(0, bl.length(), NULL, NULL);
  ASSERT_TRUE(AssertOperateWithSplitOp(0, 3, "foo", &read, &read_bl, balanced_read_flags));
  ASSERT_EQ(bl.length(), read_bl.length());
  ASSERT_EQ(0, memcmp(bl.c_str(), read_bl.c_str(), bl.length()));
}

TEST_P(LibRadosSplitOpPP, ShortReadInMultiOp) {
  SKIP_IF_CRIMSON();
  bufferlist bl;
  ASSERT_NO_FATAL_FAILURE(write_pattern_object(2, 0, &bl));

  // A read shorter than the minimum split size in the same request used
  // to crash on a division by zero in init_read(), then on
  // std::out_of_range in buffer assembly.
  ObjectReadOperation read;
  bufferlist big_bl, small_bl;
  int big_rval, small_rval;
  read.read(0, bl.length(), &big_bl, &big_rval);
  read.read(0, 16, &small_bl, &small_rval);

  bufferlist result_bl;
  ASSERT_TRUE(AssertOperateWithSplitOp(0, 2, "foo", &read, &result_bl, balanced_read_flags));
  ASSERT_EQ(0, big_rval);
  ASSERT_EQ(0, small_rval);
  ASSERT_EQ(bl.length(), big_bl.length());
  ASSERT_EQ(0, memcmp(bl.c_str(), big_bl.c_str(), bl.length()));
  ASSERT_EQ(16u, small_bl.length());
  ASSERT_EQ(0, memcmp(bl.c_str(), small_bl.c_str(), 16));
}

TEST_P(LibRadosSplitOpPP, ShortSparseReadInMultiOp) {
  SKIP_IF_CRIMSON();
  bufferlist bl;
  ASSERT_NO_FATAL_FAILURE(write_pattern_object(2, 0, &bl));

  // Sparse variant of ShortReadInMultiOp: the short op only uses the
  // reference sub-read, so sparse assembly must skip the others.
  ObjectReadOperation read;
  bufferlist big_bl, sparse_bl;
  int big_rval, sparse_rval;
  std::map<uint64_t, uint64_t> extents;
  read.read(0, bl.length(), &big_bl, &big_rval);
  read.sparse_read(0, 16, &extents, &sparse_bl, &sparse_rval);

  bufferlist result_bl;
  ASSERT_TRUE(AssertOperateWithSplitOp(0, 2, "foo", &read, &result_bl, balanced_read_flags));
  ASSERT_EQ(0, big_rval);
  ASSERT_EQ(0, sparse_rval);
  ASSERT_EQ(bl.length(), big_bl.length());
  ASSERT_EQ(0, memcmp(bl.c_str(), big_bl.c_str(), bl.length()));
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(16u, sparse_bl.length());
  ASSERT_EQ(0, memcmp(bl.c_str(), sparse_bl.c_str(), 16));
}

TEST_P(LibRadosSplitOpECPP, ReadWithVersion) {
  SKIP_IF_CRIMSON();
  bufferlist bl;
  bl.append("ceph");
  ObjectWriteOperation write1;
  write1.write(0, bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));

  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);

  bufferlist exec_inbl, exec_outbl;
  int exec_rval;
  read.exec(version::method::read, exec_inbl, &exec_outbl, &exec_rval);
  ASSERT_TRUE(AssertOperateWithSplitOp(0, "foo", &read, &bl, balanced_read_flags));
  ASSERT_EQ(0, memcmp(bl.c_str(), "ceph", 4));
  ASSERT_EQ(0, exec_rval);
  cls_version_read_ret exec_version;
  auto iter = exec_outbl.cbegin();
  decode(exec_version, iter);
  ASSERT_EQ(0, exec_version.objv.ver);
  ASSERT_EQ("", exec_version.objv.tag);
}

TEST_P(LibRadosSplitOpECPP, SmallRead) {
  SKIP_IF_CRIMSON();
  bufferlist bl;
  bl.append("ceph");
  ObjectWriteOperation write1;
  write1.write(0, bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));

  ioctx.set_no_version_on_read(true);
  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);
  ASSERT_TRUE(AssertOperateWithSplitOp(0, "foo", &read, &bl, balanced_read_flags));
  ioctx.set_no_version_on_read(false);
}

TEST_P(LibRadosSplitOpECPP, ReadTwoShards) {
  SKIP_IF_CRIMSON();
  bufferlist bl;
  bl.append_zero(2 * get_ec_stripe_unit());
  ObjectWriteOperation write1;
  write1.write(0, bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));
  
  ensure_log_committed("foo", 0, bl.length());

  ioctx.set_no_version_on_read(true);
  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);
  ASSERT_TRUE(AssertOperateWithSplitOp(0, 2, "foo", &read, &bl, balanced_read_flags));
  ioctx.set_no_version_on_read(false);
}

TEST_P(LibRadosSplitOpECPP, ReadSecondShard) {
  SKIP_IF_CRIMSON();
  bufferlist bl;
  bl.append_zero(2 * get_ec_stripe_unit());
  ObjectWriteOperation write1;
  write1.write(0, bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));
  
  ensure_log_committed("foo", 0, bl.length());

  ioctx.set_no_version_on_read(true);
  ObjectReadOperation read;
  read.read(get_ec_stripe_unit(), get_ec_stripe_unit(), NULL, NULL);
  ASSERT_TRUE(AssertOperateWithSplitOp(0, "foo", &read, &bl, balanced_read_flags));
  ioctx.set_no_version_on_read(false);
}

TEST_P(LibRadosSplitOpECPP, ReadSecondShardWithVersion) {
  SKIP_IF_CRIMSON();
  bufferlist bl;
  bl.append_zero(2 * get_ec_stripe_unit());
  ObjectWriteOperation write1;
  write1.write(0, bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));

  ensure_log_committed("foo", 0, bl.length());

  ObjectReadOperation read;
  read.read(get_ec_stripe_unit(), get_ec_stripe_unit(), NULL, NULL);
  ASSERT_TRUE(AssertOperateWithSplitOp(0, 2, "foo", &read, &bl, balanced_read_flags));
}

TEST_P(LibRadosSplitOpECPP, XattrReads) {
  SKIP_IF_CRIMSON();
  bufferlist bl, attr_bl, attr_read_bl;
  std::string attr_key = "my_key";
  std::string attr_value = "my_attr";

  bl.append("ceph");
  ObjectWriteOperation write1;
  write1.write(0, bl);
  encode(attr_value, attr_bl);
  write1.setxattr(attr_key.c_str(), attr_bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));

  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);

  int getxattr_rval, getxattrs_rval;
  read.getxattr(attr_key.c_str(), &attr_read_bl, &getxattr_rval);
  std::map<string, bufferlist> pattrs{ {"", {}}, {attr_key, {}}};
  read.getxattrs(&pattrs, &getxattrs_rval);
  read.cmpxattr(attr_key.c_str(), CEPH_OSD_CMPXATTR_OP_EQ, attr_bl);

  ASSERT_TRUE(AssertOperateWithSplitOp(1, "foo", &read, &bl, balanced_read_flags));
  ASSERT_EQ(0, memcmp(bl.c_str(), "ceph", 4));
  ASSERT_EQ(0, getxattr_rval);
  ASSERT_EQ(0, getxattrs_rval);
}

TEST_P(LibRadosSplitOpECPP, Stat) {
  SKIP_IF_CRIMSON();
  bufferlist bl, attr_bl, attr_read_bl;
  std::string attr_key = "my_key";
  std::string attr_value = "my_attr";

  bl.append("ceph");
  ObjectWriteOperation write1;
  write1.write(0, bl);
  encode(attr_value, attr_bl);
  write1.setxattr(attr_key.c_str(), attr_bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));

  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);

  uint64_t size;
  timespec time;
  time.tv_nsec = 0;
  time.tv_sec = 0;
  int stat_rval;
  read.stat2(&size, &time, &stat_rval);

  ASSERT_TRUE(AssertOperateWithSplitOp(0, "foo", &read, &bl, balanced_read_flags));
  ASSERT_EQ(0, memcmp(bl.c_str(), "ceph", 4));
  ASSERT_EQ(0, stat_rval);
  ASSERT_EQ(4, size);
  ASSERT_NE(0, time.tv_nsec);
  ASSERT_NE(0, time.tv_sec);
}

TEST_P(LibRadosSplitOpECPP, StatBeforeRead) {
  SKIP_IF_CRIMSON();
  // Read spans multiple EC chunks and triggers split op
  bufferlist bl;
  bl.append_zero(2 * get_ec_stripe_unit());
  ObjectWriteOperation write1;
  write1.write(0, bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));

  ensure_log_committed("foo", 0, bl.length());

  // This test demonstrates the bug: STAT operation comes BEFORE READ
  // In SplitOp::init(), when processing ops in order:
  // 1. STAT is processed first (ops_index=0)
  // 2. It goes to default case at line 603 in SplitOp.cc
  // 3. Line 603 tries: sub_reads.at(reference_sub_read).details[ops_index]
  // 4. But reference_sub_read is still -1 (not set yet)
  // 5. init_read() for READ hasn't been called yet
  // This should trigger an assertion failure or undefined behavior
  
  ObjectReadOperation read;
  uint64_t size;
  timespec time;
  time.tv_nsec = 0;
  time.tv_sec = 0;
  int stat_rval;
  bufferlist read_bl;
  int read_rval;
  
  read.stat2(&size, &time, &stat_rval);  // STAT comes FIRST
  read.read(0, bl.length(), &read_bl, &read_rval);  // READ comes SECOND (spans multiple chunks)

  // This operation should fail or demonstrate the bug
  bufferlist result_bl;
  ASSERT_TRUE(AssertOperateWithSplitOp(0, 2, "foo", &read, &result_bl, balanced_read_flags));
  ASSERT_EQ(0, stat_rval);
  ASSERT_EQ(0, read_rval);
  ASSERT_EQ(2 * get_ec_stripe_unit(), size);
  ASSERT_EQ(2 * get_ec_stripe_unit(), read_bl.length());
}

TEST_P(LibRadosSplitOpECPP, GetXattrBeforeRead) {
  SKIP_IF_CRIMSON();
  // Read spans multiple EC chunks and triggers split op
  bufferlist bl, attr_bl, attr_read_bl;
  std::string attr_key = "my_key";
  std::string attr_value = "my_attr";

  bl.append_zero(2 * get_ec_stripe_unit());
  ObjectWriteOperation write1;
  write1.write(0, bl);
  encode(attr_value, attr_bl);
  write1.setxattr(attr_key.c_str(), attr_bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));

  ensure_log_committed("foo", 0, bl.length());

  // Another variant of the bug: GETXATTR before READ
  ObjectReadOperation read;
  int getxattr_rval;
  bufferlist read_bl;
  int read_rval;
  
  read.getxattr(attr_key.c_str(), &attr_read_bl, &getxattr_rval);  // GETXATTR FIRST
  read.read(0, bl.length(), &read_bl, &read_rval); // READ SECOND (spans multiple chunks)

  bufferlist result_bl;
  ASSERT_TRUE(AssertOperateWithSplitOp(0, 2, "foo", &read, &result_bl, balanced_read_flags));
  ASSERT_EQ(0, getxattr_rval);
  ASSERT_EQ(0, read_rval);
  ASSERT_EQ(2 * get_ec_stripe_unit(), read_bl.length());
}

TEST_P(LibRadosSplitOpPP, CancelReplica)
{
  SKIP_IF_CRIMSON();
  if (!split_ops) {
    GTEST_SKIP() << "Inject requires split_ops!";
  }
  bufferlist bl, attr_bl, attr_read_bl;
  uint64_t length = 512 * 1024;
  const std::string oid = "foo";

  bl.append_zero(length);
  ObjectWriteOperation write1;
  write1.write(0, bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, oid, &write1));

  int ret = 0;
  auto c = std::unique_ptr<AioCompletion>{Rados::aio_create_completion()};
  ObjectReadOperation op;
  int osd_ret;
  bufferlist outval;
  op.read(0, length, &outval, &osd_ret);
  ioctx.aio_operate(oid, c.get(), &op, balanced_read_flags, nullptr);

  EXPECT_EQ(0, c->cancel());
  {
    TestAlarm alarm;
    EXPECT_EQ(0, c->wait_for_complete());
  }
  ret = c->get_return_value();

  EXPECT_EQ(-ECANCELED, ret);
}

// ===========================================================================
// Zone-aware split-read tests
//
// These tests need a CRUSH map with two zones, buckets of the type that
// osd_pool_default_zone_failure_domain names (datacenter by default), and
// skip themselves on other clusters. The stretch pool that RadosTestECPP
// creates there is a 2+1 pool over the two zones.
//
// A PG's zones are numbered by its acting set, not by CRUSH bucket: shards 0
// to k+m-1 are its zone 0 wherever CRUSH places them. So the tests take the
// client's zone, and the shards to poison, from the CRUSH map, and count the
// reads that each zone's OSDs serve.
// ===========================================================================

class LibRadosSplitOpZoneBase : public RadosTestECPP {
public:
  ZoneTopology topo;
  int k_per_zone = 0;
  int m_per_zone = 0;

  void SetUp() override {
    SKIP_IF_CRIMSON();
    RadosTestECPP::SetUp();

    if (!has_two_zone_topology()) {
      GTEST_SKIP() << "Zone-aware split-read tests require a CRUSH map with two "
                      "zones: buckets of the type that "
                      "osd_pool_default_zone_failure_domain names";
    }
    topo = get_zone_topology(s_cluster);

    // Switch ioctx to the stretch pool for zone tests
    ioctx.close();
    ASSERT_EQ(0, s_cluster.ioctx_create(pool_name_stretch.c_str(), ioctx));
    pool_name = pool_name_stretch;
    nspace = get_temp_pool_name();
    ioctx.set_namespace(nspace);
    // The stretch pool's required_alignment is the full stripe width. Every
    // zone holds the whole stripe, so the chunk size is stripe_width /
    // k_per_zone: a read of `alignment` bytes hits exactly one EC shard.
    uint64_t stripe_width = 0;
    ASSERT_EQ(0, ioctx.pool_required_alignment2(&stripe_width));
    ASSERT_NE(0U, stripe_width);
    ceph::consistency::RadosCommands ec_cmds(s_cluster);
    ceph::ErasureCodeProfile prof = ec_cmds.get_ec_profile_for_pool(pool_name);
    k_per_zone = std::stoi(prof["k"]);
    m_per_zone = std::stoi(prof["m"]);
    alignment = stripe_width / k_per_zone;
    ASSERT_NE(0U, alignment);
    ec_k = k_per_zone;
    ec_m = m_per_zone;
    ec_num_zones = 2;
  }

  // The zone that holds a shard of oid
  std::string shard_zone(const std::string &oid, int shard) {
    ceph::consistency::RadosCommands ec_cmds(s_cluster);
    return topo.osd_zone.at(
      ec_cmds.get_shard_osd(pool_name, oid, shard_id_t(shard), nspace));
  }

  std::string other_zone(const std::string &zone) {
    return topo.zones[0] == zone ? topo.zones[1] : topo.zones[0];
  }

  // Puts the client in a zone. An empty crush_location would not take it out
  // again: the client keeps its last location.
  void set_client_zone(const std::string &zone) {
    const std::string location = topo.type + "=" + zone;
    ASSERT_EQ(0, s_cluster.conf_set("crush_location", location.c_str()));
  }

  // Poisons the shards of oid outside a zone, and returns them
  std::vector<int> poison_shards_outside(const std::string &oid,
                                         const std::string &zone) {
    std::vector<int> poisoned;
    for (int s = 0; s < ec_num_zones * (k_per_zone + m_per_zone); ++s) {
      if (shard_zone(oid, s) != zone) {
        inject_ec_read_error_on_shard(oid, shard_id_t(s));
        poisoned.push_back(s);
      }
    }
    return poisoned;
  }

  // The client reads that an OSD has served
  int64_t osd_reads(int osd) {
    bufferlist out;
    EXPECT_EQ(0, s_cluster.osd_command(osd, "{\"prefix\": \"perf dump\"}",
                                       {}, &out, nullptr));
    JSONParser p;
    int64_t op_r = 0;
    if (p.parse(out.c_str(), out.length()) && p.find_obj("osd")) {
      JSONDecoder::decode_json("op_r", op_r, p.find_obj("osd"));
    }
    return op_r;
  }

  // The client reads that the OSDs of a zone have served
  int64_t zone_reads(const std::string &zone) {
    int64_t reads = 0;
    for (const auto &[osd, z] : topo.osd_zone) {
      if (z == zone) {
        reads += osd_reads(osd);
      }
    }
    return reads;
  }

  int primary_osd(const std::string &oid) {
    ceph::consistency::RadosCommands ec_cmds(s_cluster);
    return ec_cmds.get_primary_osd(pool_name, oid, nspace);
  }

  // Localized reads of the whole object from the zone of shard `shard`: the data
  // sub-reads stay in that zone; a multi-chunk read also checks the version with
  // the primary, which can be in the other zone.
  void localize_reads_stay_in_zone(int shard, const std::string &oid)
  {
    int zone_size = k_per_zone + m_per_zone;
    bufferlist wbl;
    uint64_t write_len = static_cast<uint64_t>(2 * zone_size) * alignment;
    wbl.append_zero(write_len);
    {
      ObjectWriteOperation write;
      write.write(0, wbl);
      ASSERT_TRUE(AssertOperateWithoutSplitOp(0, oid, &write));
    }
    wait_for_stable_acting_set(oid);

    const std::string local = shard_zone(oid, shard);
    const std::string other = other_zone(local);
    const bool primary_remote = topo.osd_zone.at(primary_osd(oid)) != local;
    set_client_zone(local);

    int64_t before_local = zone_reads(local);
    int64_t before_other = zone_reads(other);

    constexpr int reads = 10;
    for (int i = 0; i < reads; ++i) {
      bufferlist rbl;
      ObjectReadOperation read;
      read.read(0, write_len, &rbl, nullptr);
      ASSERT_EQ(0, ioctx.operate(oid, &read, &rbl,
                                    librados::OPERATION_LOCALIZE_READS));
    }

    int64_t delta_local = zone_reads(local) - before_local;
    int64_t delta_other = zone_reads(other) - before_other;
    EXPECT_GE(delta_local, reads * k_per_zone)
        << "Expected at least " << (reads * k_per_zone)
        << " sub-reads in the client's zone";
    EXPECT_LE(delta_other, primary_remote ? reads : 0)
        << "Only the version checks with a primary in the other zone may go there";

    ioctx.remove(oid);
  }
};

class LibRadosSplitOpZone : public LibRadosSplitOpZoneBase {};

// ---------------------------------------------------------------------------
// Scenario 1: client in the zone of the PG's zone 0, the other zone's shards
// poisoned — local read succeeds.
//
// A single-chunk read (offset=0, length=stripe_unit) maps to shard 0, which
// lies in the client's zone.  After injecting EIO on every shard of the other
// zone the split read must still succeed because it never contacts those shards.
// ---------------------------------------------------------------------------
TEST_P(LibRadosSplitOpZone, Zone0LocalReadSucceedsWhenZone1Poisoned)
{
  SKIP_IF_CRIMSON();
  if (!split_ops) GTEST_SKIP() << "Requires split_ops";

  const std::string oid = "zone0-local";
  // Write one alignment-unit worth of data.
  bufferlist wbl;
  wbl.append_zero(alignment);
  {
    ObjectWriteOperation write;
    write.write(0, wbl);
    ASSERT_TRUE(AssertOperateWithoutSplitOp(0, oid, &write));
  }

  wait_for_stable_acting_set(oid);

  const std::string local = shard_zone(oid, 0);
  const std::vector<int> poisoned = poison_shards_outside(oid, local);
  ASSERT_EQ(k_per_zone + m_per_zone, static_cast<int>(poisoned.size()));
  set_client_zone(local);

  // A read of exactly one alignment unit at offset 0 stays within the zone.
  bufferlist rbl;
  ObjectReadOperation read;
  read.read(0, alignment, &rbl, nullptr);
  ASSERT_TRUE(AssertOperateWithSplitOp(0, oid, &read, &rbl, balanced_read_flags));
  ASSERT_EQ(alignment, rbl.length());

  // Cleanup
  for (int s : poisoned) {
    clear_ec_read_error_on_shard(oid, shard_id_t(s));
  }
  ioctx.remove(oid);
}

// ---------------------------------------------------------------------------
// Scenario 2: client in the zone of the PG's zone 1, the other zone's shards
// poisoned — local read succeeds.
// ---------------------------------------------------------------------------
TEST_P(LibRadosSplitOpZone, Zone1LocalReadSucceedsWhenZone0Poisoned)
{
  SKIP_IF_CRIMSON();
  if (!split_ops) GTEST_SKIP() << "Requires split_ops";

  const std::string oid = "zone1-local";
  const int zone_size = k_per_zone + m_per_zone;

  // Write more than a stripe; the chunk read below is served from the copy in
  // the PG's zone 1, which is in the client's zone.
  uint64_t zone1_offset = static_cast<uint64_t>(zone_size) * alignment;
  bufferlist wbl;
  wbl.append_zero(zone1_offset + alignment);
  {
    ObjectWriteOperation write;
    write.write(0, wbl);
    ASSERT_TRUE(AssertOperateWithoutSplitOp(0, oid, &write));
  }

  wait_for_stable_acting_set(oid);

  const std::string local = shard_zone(oid, zone_size);
  const std::vector<int> poisoned = poison_shards_outside(oid, local);
  ASSERT_EQ(zone_size, static_cast<int>(poisoned.size()));
  set_client_zone(local);
  // The primary, shard 0, is in the PG's zone 0, so the read also checks the
  // version with it there: two sub-reads.
  ASSERT_NE(local, topo.osd_zone.at(primary_osd(oid)));

  // Read exactly one chunk from the PG's zone 1.
  bufferlist rbl;
  ObjectReadOperation read;
  read.read(zone1_offset, alignment, &rbl, nullptr);
  ASSERT_TRUE(AssertOperateWithSplitOp(0, 2, oid, &read, &rbl,
                                       balanced_read_flags));
  ASSERT_EQ(alignment, rbl.length());

  // Cleanup
  for (int s : poisoned) {
    clear_ec_read_error_on_shard(oid, shard_id_t(s));
  }
  ioctx.remove(oid);
}

// ---------------------------------------------------------------------------
// Scenario 4: a balanced read is not filtered by zone.
// ---------------------------------------------------------------------------
TEST_P(LibRadosSplitOpZone, BalancedReadIgnoresCrushLocation)
{
  SKIP_IF_CRIMSON();
  if (!split_ops) GTEST_SKIP() << "Requires split_ops";

  const std::string oid = "no-location";
  bufferlist wbl;
  wbl.append_zero(alignment);
  {
    ObjectWriteOperation write;
    write.write(0, wbl);
    ASSERT_TRUE(AssertOperateWithoutSplitOp(0, oid, &write));
  }

  // Whatever zone an earlier test put the client in, a balanced read can use
  // the shards of either zone.
  bufferlist rbl;
  ObjectReadOperation read;
  read.read(0, alignment, &rbl, nullptr);
  ASSERT_TRUE(AssertOperateWithSplitOp(0, oid, &read, &rbl, balanced_read_flags));
  ASSERT_EQ(alignment, rbl.length());

  ioctx.remove(oid);
}

// ===========================================================================
// LibRadosSplitOpZoneStats — check where zone-aware split reads go, by the
// client reads (op_r) that the OSDs of each zone serve.
// ===========================================================================

class LibRadosSplitOpZoneStats : public LibRadosSplitOpZoneBase {};

TEST_P(LibRadosSplitOpZoneStats, BalanceReadsSpreadsAcrossZones)
{
  SKIP_IF_CRIMSON();
  if (!split_ops) GTEST_SKIP() << "Requires split_ops";

  int zone_size = k_per_zone + m_per_zone;
  const std::string oid = "balance-spread";
  bufferlist wbl;
  uint64_t write_len = static_cast<uint64_t>(2 * zone_size) * alignment;
  wbl.append_zero(write_len);
  {
    ObjectWriteOperation write;
    write.write(0, wbl);
    ASSERT_TRUE(AssertOperateWithoutSplitOp(0, oid, &write));
  }
  wait_for_stable_acting_set(oid);

  // BALANCE_READS with the current single-zone-per-read implementation picks
  // the nearest zone (same as LOCALIZE_READS): the reads reach the client's.
  const std::string local = topo.zones[0];
  set_client_zone(local);

  int64_t before = zone_reads(local);

  for (int i = 0; i < 10; ++i) {
    bufferlist rbl;
    ObjectReadOperation read;
    read.read(0, write_len, &rbl, nullptr);
    ASSERT_EQ(0, ioctx.operate(oid, &read, &rbl,
                                librados::OPERATION_BALANCE_READS));
  }

  ASSERT_GT(zone_reads(local) - before, 0)
      << "Expected reads in the client's zone for BALANCE_READS";

  ioctx.remove(oid);
}

TEST_P(LibRadosSplitOpZoneStats, LocalizeReadsStaysInZone0)
{
  SKIP_IF_CRIMSON();
  if (!split_ops) GTEST_SKIP() << "Requires split_ops";
  localize_reads_stay_in_zone(0, "localize-z0");
}

TEST_P(LibRadosSplitOpZoneStats, LocalizeReadsStaysInZone1)
{
  SKIP_IF_CRIMSON();
  if (!split_ops) GTEST_SKIP() << "Requires split_ops";
  localize_reads_stay_in_zone(k_per_zone + m_per_zone, "localize-z1");
}

TEST_P(LibRadosSplitOpZoneStats, LocalizeReadsFallsBackToPrimaryWhenZoneShardsUnavailable)
{
  SKIP_IF_CRIMSON();
  if (!split_ops) GTEST_SKIP() << "Requires split_ops";

  int zone_size = k_per_zone + m_per_zone;
  const std::string oid = "localize-fallback";
  bufferlist wbl;
  uint64_t write_len = static_cast<uint64_t>(2 * zone_size) * alignment;
  wbl.append_zero(write_len);
  {
    ObjectWriteOperation write;
    write.write(0, wbl);
    ASSERT_TRUE(AssertOperateWithoutSplitOp(0, oid, &write));
  }
  wait_for_stable_acting_set(oid);

  set_client_zone(shard_zone(oid, 0));

  // Inject EIO on the data shards of the PG's zone 0, in the client's zone.
  for (int s = 0; s < k_per_zone; ++s) {
    inject_ec_read_error_on_shard(oid, shard_id_t(s));
  }

  const int primary = primary_osd(oid);
  int64_t before_primary = osd_reads(primary);

  // Issue one read with LOCALIZE_READS.
  bufferlist rbl;
  ObjectReadOperation read;
  read.read(0, alignment, &rbl, nullptr);
  int rc = ioctx.operate(oid, &read, &rbl,
                          librados::OPERATION_LOCALIZE_READS);
  ASSERT_EQ(0, rc) << "Read should succeed via primary fallback";
  ASSERT_EQ(alignment, rbl.length());
  EXPECT_GE(osd_reads(primary) - before_primary, 1)
      << "the primary should serve the read";

  // Clean up injected errors.
  for (int s = 0; s < k_per_zone; ++s) {
    clear_ec_read_error_on_shard(oid, shard_id_t(s));
  }
  ioctx.remove(oid);
}

// ---------------------------------------------------------------------------
// Client in the zone of the PG's zone 1, multi-chunk localized read: when
// primary_required is true (count > 1), init_read() also creates a
// version-check sub-read to the primary, which may be in the other zone.
// Every data sub-read stays in the client's zone.
// ---------------------------------------------------------------------------
TEST_P(LibRadosSplitOpZoneStats, Zone1MultiChunkDataSubReadsStayInZone1)
{
  SKIP_IF_CRIMSON();
  if (!split_ops) GTEST_SKIP() << "Requires split_ops";

  // A multi-chunk read forces primary_required=true and therefore adds the
  // reference sub-read.
  // We need at least 2 chunks → read k_per_zone * alignment bytes so that
  // count = k_per_zone > 1.
  if (k_per_zone < 2) GTEST_SKIP() << "Requires k_per_zone >= 2 to make count > 1";

  int zone_size = k_per_zone + m_per_zone;
  const std::string oid = "bug2-ref-subread";
  // Write enough to fill both zones so all shards are populated.
  uint64_t write_len = static_cast<uint64_t>(2 * zone_size) * alignment;
  bufferlist wbl;
  wbl.append_zero(write_len);
  {
    ObjectWriteOperation write;
    write.write(0, wbl);
    ASSERT_TRUE(AssertOperateWithoutSplitOp(0, oid, &write));
  }
  wait_for_stable_acting_set(oid);

  const std::string local = shard_zone(oid, zone_size);
  const std::string other = other_zone(local);
  set_client_zone(local);

  int64_t before_local = zone_reads(local);
  int64_t before_other = zone_reads(other);

  // Read exactly k_per_zone chunks (count = k_per_zone > 1 → primary_required).
  // All chunks start at offset 0 and span k_per_zone * alignment bytes.
  uint64_t read_len = static_cast<uint64_t>(k_per_zone) * alignment;
  bufferlist rbl;
  ObjectReadOperation read;
  read.read(0, read_len, &rbl, nullptr);
  ASSERT_EQ(0, ioctx.operate(oid, &read, &rbl,
                              librados::OPERATION_LOCALIZE_READS));
  ASSERT_EQ(read_len, rbl.length());

  int64_t delta_local = zone_reads(local) - before_local;
  int64_t delta_other = zone_reads(other) - before_other;

  // Only the version-check sub-read to the primary may go to the other zone.
  EXPECT_LE(delta_other, 1)
    << "a data sub-read was dispatched to the other zone (delta=" << delta_other
    << ") even though the client is in " << local;
  EXPECT_GE(delta_local, k_per_zone)
    << "Expected at least " << k_per_zone << " sub-reads in the client's zone";

  ioctx.remove(oid);
}

INSTANTIATE_TEST_SUITE_P_REPLICA(LibRadosSplitOpPP);
INSTANTIATE_TEST_SUITE_P_EC(LibRadosSplitOpECPP);
INSTANTIATE_TEST_SUITE_P_EC(LibRadosSplitOpZone);
INSTANTIATE_TEST_SUITE_P_EC(LibRadosSplitOpZoneStats);
