#include <common/perf_counters_collection.h>

#include "test/librados/test_cxx.h"
#include "test/librados/testcase_cxx.h"
#include "crimson_utils.h"
#include "cls/fifo/cls_fifo_ops.h"
#include "cls/version/cls_version_ops.h"

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

// After a write is committed, it isn't necessarily true that the log is
// committed. We do a read of the written area, which allows us to be
// sure that the shards have all received the message that the log can be
// committed, allowing us to test split ops with certainty that it won't be
// bounced due to unstability.
void RadosTestPPBase::ensure_log_committed(const char* oid, uint64_t offset, uint64_t length) {
  ObjectReadOperation read;
  read.read(offset, length, NULL, NULL);

  bufferlist bl;
  int rc = ioctx.operate(oid, &read, &bl);
  ASSERT_EQ(0, rc);
}

TEST_P(LibRadosSplitOpPP, BigRead) {
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
  bl.append_zero(8*1024);
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
  bl.append_zero(8*1024);
  ObjectWriteOperation write1;
  write1.write(0, bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));
  
  ensure_log_committed("foo", 0, bl.length());

  ioctx.set_no_version_on_read(true);
  ObjectReadOperation read;
  read.read(4*1024, 4*1024, NULL, NULL);
  ASSERT_TRUE(AssertOperateWithSplitOp(0, "foo", &read, &bl, balanced_read_flags));
  ioctx.set_no_version_on_read(false);
}

TEST_P(LibRadosSplitOpECPP, ReadSecondShardWithVersion) {
  SKIP_IF_CRIMSON();
  bufferlist bl;
  bl.append_zero(8*1024);
  ObjectWriteOperation write1;
  write1.write(0, bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));

  ensure_log_committed("foo", 0, bl.length());

  ObjectReadOperation read;
  read.read(4*1024, 4*1024, NULL, NULL);
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
  // Use 8KB buffer to ensure the read spans multiple EC chunks and triggers split op
  bufferlist bl;
  bl.append_zero(8*1024);
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
  read.read(0, bl.length(), &read_bl, &read_rval);  // READ comes SECOND (8KB, spans multiple chunks)

  // This operation should fail or demonstrate the bug
  bufferlist result_bl;
  ASSERT_TRUE(AssertOperateWithSplitOp(0, 2, "foo", &read, &result_bl, balanced_read_flags));
  ASSERT_EQ(0, stat_rval);
  ASSERT_EQ(0, read_rval);
  ASSERT_EQ(8*1024, size);
  ASSERT_EQ(8*1024, read_bl.length());
}

TEST_P(LibRadosSplitOpECPP, GetXattrBeforeRead) {
  SKIP_IF_CRIMSON();
  // Use 8KB buffer to ensure the read spans multiple EC chunks and triggers split op
  bufferlist bl, attr_bl, attr_read_bl;
  std::string attr_key = "my_key";
  std::string attr_value = "my_attr";

  bl.append_zero(8*1024);
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
  read.read(0, bl.length(), &read_bl, &read_rval);  // READ SECOND (8KB, spans multiple chunks)

  bufferlist result_bl;
  ASSERT_TRUE(AssertOperateWithSplitOp(0, 2, "foo", &read, &result_bl, balanced_read_flags));
  ASSERT_EQ(0, getxattr_rval);
  ASSERT_EQ(0, read_rval);
  ASSERT_EQ(8*1024, read_bl.length());
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

INSTANTIATE_TEST_SUITE_P_REPLICA(LibRadosSplitOpPP);
INSTANTIATE_TEST_SUITE_P_EC(LibRadosSplitOpECPP);
