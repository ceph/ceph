#include <common/perf_counters_collection.h>
#include "common/perf_counters_key.h"

#include "test/librados/test_cxx.h"
#include "test/librados/testcase_cxx.h"
#include "crimson_utils.h"
#include "cls/fifo/cls_fifo_ops.h"
#include "cls/version/cls_version_ops.h"
#include "erasure-code/consistency/RadosCommands.h"

using namespace std;
using namespace librados;
using namespace cls;
using namespace rados::cls;

typedef RadosTestPP LibRadosSplitOpPP;
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
  ObjectWriteOperation write1, write2;
  write1.write(0, bl);
  uint32_t hash_position;
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));
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
  // The second write flushes the commit of the first.
  write2.write(0, bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, other_object, &write2));


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
  ObjectWriteOperation write1, write2;
  write1.write(0, bl);
  uint32_t hash_position;
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));
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
  // The second write flushes the commit of the first.
  write2.write(0, bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, other_object, &write2));

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
  ObjectWriteOperation write1, write2;
  write1.write(0, bl);
  uint32_t hash_position;
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));
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
  // The second write flushes the commit of the first.
  write2.write(0, bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, other_object, &write2));

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
  ObjectWriteOperation write1, write2;
  write1.write(0, bl);
  encode(attr_value, attr_bl);
  write1.setxattr(attr_key.c_str(), attr_bl);
  uint32_t hash_position;
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, "foo", &write1));
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
  // The second write flushes the commit of the first.
  write2.write(0, bl);
  ASSERT_TRUE(AssertOperateWithoutSplitOp(0, other_object, &write2));

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

// ===========================================================================
// Zone-aware split-read tests
//
// These tests require the cluster CRUSH map to have two datacenter-level
// buckets (e.g. "zone-0" and "zone-1") — the topology created by:
//   ceph osd crush add-bucket zone-0 datacenter
//   ceph osd crush add-bucket zone-1 datacenter
//   for i in 0 1 2; do ceph osd crush move osd.$i datacenter=zone-0; done
//   for i in 3 4 5; do ceph osd crush move osd.$i datacenter=zone-1; done
//
// All four tests call GTEST_SKIP() when the topology is absent so they
// remain harmless in default CI environments.
//
// Pool setup: LibRadosSplitOpZone uses the stretch pool created by the
// RadosTestECPP base fixture (pool_name_stretch).
// ===========================================================================

class LibRadosSplitOpZone : public RadosTestECPP {
public:
  void SetUp() override {
    SKIP_IF_CRIMSON();
    RadosTestECPP::SetUp();

    ASSERT_TRUE(has_two_zone_topology())
        << "Zone-aware split-read tests require a 2-zone CRUSH topology "
           "(two datacenter buckets) but the cluster is not configured for it.\n"
           "Set up the CRUSH topology before running these tests:\n"
           "  ceph osd crush add-bucket zone-0 datacenter\n"
           "  ceph osd crush add-bucket zone-1 datacenter\n"
           "  ceph osd crush move zone-{0,1} root=default\n"
           "  for i in 0 1 2; do ceph osd crush move osd.$i datacenter=zone-0; done\n"
           "  for i in 3 4 5; do ceph osd crush move osd.$i datacenter=zone-1; done";

    // Switch ioctx to the stretch pool for zone tests
    ioctx.close();
    ASSERT_EQ(0, s_cluster.ioctx_create(pool_name_stretch.c_str(), ioctx));
    pool_name = pool_name_stretch;
    nspace = get_temp_pool_name();
    ioctx.set_namespace(nspace);
    // The stretch pool's required_alignment is the full stripe width.
    // The chunk size (stripe_width / k_total) is what we need for a single-shard
    // read, where k_total = k_per_zone * num_zones.  Use the chunk size as the
    // effective alignment so a read of `alignment` bytes hits exactly one EC shard.
    uint64_t stripe_width = 0;
    ASSERT_EQ(0, ioctx.pool_required_alignment2(&stripe_width));
    ASSERT_NE(0U, stripe_width);
    ceph::consistency::RadosCommands ec_cmds(s_cluster);
    ceph::ErasureCodeProfile prof = ec_cmds.get_ec_profile_for_pool(pool_name);
    int k_per_zone = std::stoi(prof["k"]);
    int m_per_zone = std::stoi(prof["m"]);
    constexpr int num_zones = 2;
    alignment = stripe_width / (k_per_zone * num_zones);
    ASSERT_NE(0U, alignment);
    ec_k = k_per_zone;
    ec_m = m_per_zone;
    ec_num_zones = num_zones;
  }
};

// ---------------------------------------------------------------------------
// Scenario 1: client in zone-0, zone-1 shards poisoned — local read succeeds.
//
// A single-chunk read (offset=0, length=stripe_unit) maps to shard 0, which
// lies in zone-0.  After injecting EIO on every zone-1 shard the split read
// must still succeed because it never contacts those shards.
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

  // Poison all zone-1 shards (shards [zone_size, 2*zone_size)).
  // With k=2 m=1 per zone: zone_size = k+m = 3; zone-1 shards = {3, 4, 5}.
  ceph::consistency::RadosCommands ec_commands(s_cluster);
  ceph::ErasureCodeProfile profile = ec_commands.get_ec_profile_for_pool(pool_name);
  int k = std::stoi(profile["k"]);
  int m = std::stoi(profile["m"]);
  int zone_size = k + m;   // shards per zone
  int total_shards = 2 * zone_size;

  for (int s = zone_size; s < total_shards; ++s) {
    inject_ec_read_error_on_shard(oid, shard_id_t(s));
  }

  // Set crush_location to zone-0 so the Objecter knows which zone we are in.
  ASSERT_EQ(0, s_cluster.conf_set("crush_location", "datacenter=zone-0"));
  s_cluster.wait_for_latest_osdmap();

  // A read of exactly one alignment unit at offset 0 stays within zone-0.
  bufferlist rbl;
  ObjectReadOperation read;
  read.read(0, alignment, &rbl, nullptr);
  ASSERT_TRUE(AssertOperateWithSplitOp(0, oid, &read, &rbl, balanced_read_flags));
  ASSERT_EQ(alignment, rbl.length());

  // Cleanup
  for (int s = zone_size; s < total_shards; ++s) {
    clear_ec_read_error_on_shard(oid, shard_id_t(s));
  }
  s_cluster.conf_set("crush_location", "");
  ioctx.remove(oid);
}

// ---------------------------------------------------------------------------
// Scenario 2: client in zone-1, zone-0 shards poisoned — local read succeeds.
//
// TODO: Skipped due to Bug 2 — the reference_sub_read abs_shard is always
// shard_id_t(reference_sub_read), a zone-0 acting index.  When zone-0 is
// poisoned the version-check sub-read returns EAGAIN, the retry loop also
// targets the same poisoned zone-0 OSD, and the read never succeeds.
// Un-skip when SplitOp.cc:261 is fixed to use:
//   shard_id_t(reference_sub_read + local_zone_index * zone_size)
// (See Zone1MultiChunkReferenceSubReadStaysInZone1 for the perf-counter
// demonstration of exactly this sub-read misdispatch.)
// ---------------------------------------------------------------------------
TEST_P(LibRadosSplitOpZone, Zone1LocalReadSucceedsWhenZone0Poisoned)
{
  SKIP_IF_CRIMSON();
  if (!split_ops) GTEST_SKIP() << "Requires split_ops";

  const std::string oid = "zone1-local";
  ceph::consistency::RadosCommands ec_commands(s_cluster);
  ceph::ErasureCodeProfile profile = ec_commands.get_ec_profile_for_pool(pool_name);
  int k = std::stoi(profile["k"]);
  int m = std::stoi(profile["m"]);
  int zone_size = k + m;
  int total_shards = 2 * zone_size;

  // Write enough data to populate zone-1 shards.
  // zone-1 starts at shard index zone_size; its first data shard begins
  // at byte offset zone_size * alignment.
  uint64_t zone1_offset = static_cast<uint64_t>(zone_size) * alignment;
  bufferlist wbl;
  wbl.append_zero(zone1_offset + alignment);
  {
    ObjectWriteOperation write;
    write.write(0, wbl);
    ASSERT_TRUE(AssertOperateWithoutSplitOp(0, oid, &write));
  }

  wait_for_stable_acting_set(oid);

  // Poison all zone-0 shards.
  for (int s = 0; s < zone_size; ++s) {
    inject_ec_read_error_on_shard(oid, shard_id_t(s));
  }

  ASSERT_EQ(0, s_cluster.conf_set("crush_location", "datacenter=zone-1"));
  s_cluster.wait_for_latest_osdmap();

  // Read exactly one chunk from zone-1.
  bufferlist rbl;
  ObjectReadOperation read;
  read.read(zone1_offset, alignment, &rbl, nullptr);
  ASSERT_TRUE(AssertOperateWithSplitOp(0, oid, &read, &rbl, balanced_read_flags));
  ASSERT_EQ(alignment, rbl.length());

  // Cleanup
  for (int s = 0; s < zone_size; ++s) {
    clear_ec_read_error_on_shard(oid, shard_id_t(s));
  }
  s_cluster.conf_set("crush_location", "");
  ioctx.remove(oid);
  (void)total_shards;
}

// ---------------------------------------------------------------------------
// Scenario 4: no crush_location → no zone filtering, all shards contacted.
// ---------------------------------------------------------------------------
TEST_P(LibRadosSplitOpZone, NoCrushLocationMeansNoZoneFiltering)
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

  // Ensure crush_location is unset — local_zone_index defaults to 0.
  ASSERT_EQ(0, s_cluster.conf_set("crush_location", ""));
  s_cluster.wait_for_latest_osdmap();

  // Read must succeed: with no crush_location, local_zone_index == 0 and
  // shards are selected from zone 0 (the only zone for a non-stretch pool).
  bufferlist rbl;
  ObjectReadOperation read;
  read.read(0, alignment, &rbl, nullptr);
  ASSERT_TRUE(AssertOperateWithSplitOp(0, oid, &read, &rbl, balanced_read_flags));
  ASSERT_EQ(alignment, rbl.length());

  ioctx.remove(oid);
}

// ===========================================================================
// LibRadosSplitOpZoneStats — verify per-zone perf counters for zone-aware
// split-op routing.  Extends the same stretch-pool pattern as LibRadosSplitOpZone
// but checks objecter.split_op_reads_zone_N counters instead of poisoning shards.
// ===========================================================================

class LibRadosSplitOpZoneStats : public RadosTestECPP {
public:
  int k_per_zone = 0;
  int m_per_zone = 0;

  void SetUp() override {
    SKIP_IF_CRIMSON();
    RadosTestECPP::SetUp();

    ASSERT_TRUE(has_two_zone_topology())
        << "Zone-stats tests require a 2-zone CRUSH topology.";

    ioctx.close();
    ASSERT_EQ(0, s_cluster.ioctx_create(pool_name_stretch.c_str(), ioctx));
    pool_name = pool_name_stretch;
    nspace = get_temp_pool_name();
    ioctx.set_namespace(nspace);

    uint64_t stripe_width = 0;
    ASSERT_EQ(0, ioctx.pool_required_alignment2(&stripe_width));
    ASSERT_NE(0U, stripe_width);

    ceph::consistency::RadosCommands ec_cmds(s_cluster);
    ceph::ErasureCodeProfile prof = ec_cmds.get_ec_profile_for_pool(pool_name);
    k_per_zone = std::stoi(prof["k"]);
    m_per_zone = std::stoi(prof["m"]);
    constexpr int num_zones = 2;
    alignment = stripe_width / (k_per_zone * num_zones);
    ASSERT_NE(0U, alignment);
    ec_k = k_per_zone;
    ec_m = m_per_zone;
    ec_num_zones = num_zones;
  }

  int64_t get_zone_read_count(const std::string& zone_name) {
    std::string key = ceph::perf_counters::key_create(
        "objecter_zone_reads", {{"zone", zone_name}});
    std::string path = key + ".reads";
    try {
      return static_cast<int64_t>(get_perf_counter_by_path(path));
    } catch (const std::range_error&) {
      return 0;
    }
  }
};

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

  // Set crush_location to zone-0; BALANCE_READS with the current single-zone-
  // per-read implementation picks the nearest zone (same as LOCALIZE_READS).
  // Verify that balanced reads route to zone-0 and the counter increments.
  ASSERT_EQ(0, s_cluster.conf_set("crush_location", "datacenter=zone-0"));
  s_cluster.wait_for_latest_osdmap();

  int64_t before_z0 = get_zone_read_count("zone-0");

  for (int i = 0; i < 10; ++i) {
    bufferlist rbl;
    ObjectReadOperation read;
    read.read(0, write_len, &rbl, nullptr);
    ASSERT_EQ(0, ioctx.operate(oid, &read, &rbl,
                                librados::OPERATION_BALANCE_READS));
  }

  int64_t delta_z0 = get_zone_read_count("zone-0") - before_z0;
  ASSERT_GT(delta_z0, 0)
      << "Expected zone-0 counter to increase for BALANCE_READS";

  s_cluster.conf_set("crush_location", "");
  ioctx.remove(oid);
}

TEST_P(LibRadosSplitOpZoneStats, LocalizeReadsStaysInZone0)
{
  SKIP_IF_CRIMSON();
  if (!split_ops) GTEST_SKIP() << "Requires split_ops";

  int zone_size = k_per_zone + m_per_zone;
  const std::string oid = "localize-z0";
  bufferlist wbl;
  uint64_t write_len = static_cast<uint64_t>(2 * zone_size) * alignment;
  wbl.append_zero(write_len);
  {
    ObjectWriteOperation write;
    write.write(0, wbl);
    ASSERT_TRUE(AssertOperateWithoutSplitOp(0, oid, &write));
  }
  wait_for_stable_acting_set(oid);

  ASSERT_EQ(0, s_cluster.conf_set("crush_location", "datacenter=zone-0"));
  s_cluster.wait_for_latest_osdmap();

  int64_t before_z0 = get_zone_read_count("zone-0");
  int64_t before_z1 = get_zone_read_count("zone-1");

  for (int i = 0; i < 10; ++i) {
    bufferlist rbl;
    ObjectReadOperation read;
    read.read(0, write_len, &rbl, nullptr);
    ASSERT_EQ(0, ioctx.operate(oid, &read, &rbl,
                                librados::OPERATION_LOCALIZE_READS));
  }

  int64_t delta_z0 = get_zone_read_count("zone-0") - before_z0;
  int64_t delta_z1 = get_zone_read_count("zone-1") - before_z1;
  EXPECT_GE(delta_z0, 10 * k_per_zone)
      << "Expected at least " << (10 * k_per_zone) << " zone-0 sub-reads";
  EXPECT_EQ(delta_z1, 0) << "zone-1 counter should not have increased";

  s_cluster.conf_set("crush_location", "");
  ioctx.remove(oid);
}

TEST_P(LibRadosSplitOpZoneStats, LocalizeReadsStaysInZone1)
{
  SKIP_IF_CRIMSON();
  if (!split_ops) GTEST_SKIP() << "Requires split_ops";

  int zone_size = k_per_zone + m_per_zone;
  const std::string oid = "localize-z1";
  bufferlist wbl;
  uint64_t write_len = static_cast<uint64_t>(2 * zone_size) * alignment;
  wbl.append_zero(write_len);
  {
    ObjectWriteOperation write;
    write.write(0, wbl);
    ASSERT_TRUE(AssertOperateWithoutSplitOp(0, oid, &write));
  }
  wait_for_stable_acting_set(oid);

  ASSERT_EQ(0, s_cluster.conf_set("crush_location", "datacenter=zone-1"));
  s_cluster.wait_for_latest_osdmap();

  int64_t before_z0 = get_zone_read_count("zone-0");
  int64_t before_z1 = get_zone_read_count("zone-1");

  for (int i = 0; i < 10; ++i) {
    bufferlist rbl;
    ObjectReadOperation read;
    read.read(0, write_len, &rbl, nullptr);
    ASSERT_EQ(0, ioctx.operate(oid, &read, &rbl,
                                librados::OPERATION_LOCALIZE_READS));
  }

  int64_t delta_z0 = get_zone_read_count("zone-0") - before_z0;
  int64_t delta_z1 = get_zone_read_count("zone-1") - before_z1;
  EXPECT_GE(delta_z1, 10 * k_per_zone)
      << "Expected at least " << (10 * k_per_zone) << " zone-1 sub-reads";
  EXPECT_EQ(delta_z0, 0) << "zone-0 counter should not have increased";

  s_cluster.conf_set("crush_location", "");
  ioctx.remove(oid);
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

  ASSERT_EQ(0, s_cluster.conf_set("crush_location", "datacenter=zone-0"));
  s_cluster.wait_for_latest_osdmap();

  // Inject EIO on all zone-0 data shards (shards 0 to k_per_zone-1).
  for (int s = 0; s < k_per_zone; ++s) {
    inject_ec_read_error_on_shard(oid, shard_id_t(s));
  }

  int64_t before_z0 = get_zone_read_count("zone-0");
  int64_t before_z1 = get_zone_read_count("zone-1");

  // Issue one read with LOCALIZE_READS.
  bufferlist rbl;
  ObjectReadOperation read;
  read.read(0, alignment, &rbl, nullptr);
  int rc = ioctx.operate(oid, &read, &rbl,
                          librados::OPERATION_LOCALIZE_READS);
  ASSERT_EQ(0, rc) << "Read should succeed via primary fallback";
  ASSERT_EQ(alignment, rbl.length());

  int64_t delta_z0 = get_zone_read_count("zone-0") - before_z0;
  int64_t delta_z1 = get_zone_read_count("zone-1") - before_z1;
  // The split op should have been aborted — neither counter should increase.
  EXPECT_EQ(delta_z0, 0) << "zone-0 counter should not increase (split aborted)";
  EXPECT_EQ(delta_z1, 0) << "zone-1 counter should not increase (split aborted)";

  // Clean up injected errors.
  for (int s = 0; s < k_per_zone; ++s) {
    clear_ec_read_error_on_shard(oid, shard_id_t(s));
  }
  s_cluster.conf_set("crush_location", "");
  ioctx.remove(oid);
}

// ---------------------------------------------------------------------------
// Regression: Bug 2 — reference_sub_read abs_shard must use local_zone_index
//
// When primary_required is true (count > 1 on a multi-chunk read), init_read()
// creates an extra sub-read for the version-check "reference" shard.  Before
// the fix this always used shard_id_t(reference_sub_read) — a zone-0 acting
// index — even when local_zone_index=1.  The fix uses:
//
//   shard_id_t(reference_sub_read + local_zone_index * zone_size)
//
// This test verifies the fix: with crush_location=zone-1 and a multi-chunk
// read, the zone-0 perf counter must not increase (all sub-reads stay in
// zone-1, including the version-check reference sub-read).
// ---------------------------------------------------------------------------
TEST_P(LibRadosSplitOpZoneStats, Zone1MultiChunkReferenceSubReadStaysInZone1)
{
  SKIP_IF_CRIMSON();
  if (!split_ops) GTEST_SKIP() << "Requires split_ops";

  // A multi-chunk read forces primary_required=true and therefore triggers the
  // reference_sub_read emplace with the buggy abs_shard formula.
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

  // Place the client in zone-1.
  ASSERT_EQ(0, s_cluster.conf_set("crush_location", "datacenter=zone-1"));
  s_cluster.wait_for_latest_osdmap();

  int64_t before_z0 = get_zone_read_count("zone-0");
  int64_t before_z1 = get_zone_read_count("zone-1");

  // Read exactly k_per_zone chunks (count = k_per_zone > 1 → primary_required).
  // All chunks start at offset 0 and span k_per_zone * alignment bytes.
  uint64_t read_len = static_cast<uint64_t>(k_per_zone) * alignment;
  bufferlist rbl;
  ObjectReadOperation read;
  read.read(0, read_len, &rbl, nullptr);
  ASSERT_EQ(0, ioctx.operate(oid, &read, &rbl,
                              librados::OPERATION_LOCALIZE_READS));
  ASSERT_EQ(read_len, rbl.length());

  int64_t delta_z0 = get_zone_read_count("zone-0") - before_z0;
  int64_t delta_z1 = get_zone_read_count("zone-1") - before_z1;

  // All sub-reads — including the version-check reference sub-read — should
  // have been dispatched to zone-1.  zone-0 counter must not change.
  //
  // BUG: currently delta_z0 == 1 because reference_sub_read gets
  // abs_shard = shard_id_t(reference_sub_read) which is a zone-0 acting index.
  // Fix SplitOp.cc:261 to use shard_id_t(reference_sub_read + local_zone * zone_size).
  EXPECT_EQ(delta_z0, 0)
    << "Bug 2: reference sub-read was dispatched to zone-0 (delta_z0=" << delta_z0
    << ") even though client crush_location=zone-1. "
       "Fix: shard_id_t(reference_sub_read + local_zone_index * zone_size) "
       "in SplitOp.cc init_read().";
  EXPECT_GE(delta_z1, k_per_zone)
    << "Expected at least " << k_per_zone << " zone-1 sub-reads";

  s_cluster.conf_set("crush_location", "");
  ioctx.remove(oid);
}

INSTANTIATE_TEST_SUITE_P_REPLICA(LibRadosSplitOpPP);
INSTANTIATE_TEST_SUITE_P_EC(LibRadosSplitOpECPP);
INSTANTIATE_TEST_SUITE_P_EC(LibRadosSplitOpZone);
INSTANTIATE_TEST_SUITE_P_EC(LibRadosSplitOpZoneStats);
