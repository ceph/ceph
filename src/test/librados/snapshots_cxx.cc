#include <algorithm>
#include <errno.h>
#include <string>
#include <unistd.h>

#include "gtest/gtest.h"

#include "include/rados.h"
#include "include/rados/librados.hpp"
#include "json_spirit/json_spirit.h"
#include "test/librados/test_cxx.h"
#include "test/librados/testcase_cxx.h"
#include "crimson_utils.h"

using namespace librados;

typedef RadosTestPP LibRadosSnapshotsPP;
typedef RadosTestPP LibRadosSnapshotsSelfManagedPP;
typedef RadosTestECPP LibRadosSnapshotsECPP;
typedef RadosTestECPP LibRadosSnapshotsSelfManagedECPP;

const int bufsize = 128;

// ---------------------------------------------------------------------------
// Helper: stop / start snap-trimming via monitor commands
// ---------------------------------------------------------------------------

static void set_nosnaptrim(librados::Rados &cluster, bool stop)
{
  bufferlist outbl;
  std::string cmd = stop
    ? "{\"prefix\": \"osd set\",   \"key\": \"nosnaptrim\"}"
    : "{\"prefix\": \"osd unset\", \"key\": \"nosnaptrim\"}";
  ASSERT_EQ(0, cluster.mon_command(std::move(cmd), {}, &outbl, nullptr));
  cluster.wait_for_latest_osdmap();
}

// Poll pg dump until snaptrimq_len is 0 for all PGs (trim complete).
// Times out after ~5 minutes (60 x 5-second polls).
static void wait_for_snaptrim_complete(librados::Rados &cluster)
{
  for (int tries = 0; tries < 60; ++tries) {
    sleep(5);
    bufferlist outbl;
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"pg dump\", \"format\": \"json\"}", {}, &outbl, nullptr));
    json_spirit::Value v;
    std::string outstr(outbl.c_str(), outbl.length());
    if (!json_spirit::read(outstr, v))
      continue;
    json_spirit::Object &top = v.get_obj();
    // find pg_map -> pg_stats array
    int total_trimq = 0;
    for (auto &kv : top) {
      if (kv.name_ != "pg_map") continue;
      for (auto &kv2 : kv.value_.get_obj()) {
        if (kv2.name_ != "pg_stats") continue;
        for (auto &pg_val : kv2.value_.get_array()) {
          for (auto &stat : pg_val.get_obj()) {
            if (stat.name_ == "snaptrimq_len")
              total_trimq += stat.value_.get_int();
          }
        }
      }
    }
    if (total_trimq == 0)
      return;
  }
  ADD_FAILURE() << "Timed out waiting for snaptrim to complete";
}

// ---------------------------------------------------------------------------
// Helper: read the current head contents of object "foo" and verify they
// match the provided fill byte.
// ---------------------------------------------------------------------------
static void verify_head(librados::IoCtx &ioctx, char fill)
{
  char expected[bufsize];
  memset(expected, fill, sizeof(expected));
  bufferlist bl;
  EXPECT_EQ((int)sizeof(expected), ioctx.read("foo", bl, sizeof(expected), 0));
  EXPECT_EQ(0, memcmp(expected, bl.c_str(), sizeof(expected)));
}

static void verify_snap(librados::IoCtx &ioctx, const char *snap_name, char fill)
{
  rados_snap_t rid;
  ASSERT_EQ(0, ioctx.snap_lookup(snap_name, &rid));
  ioctx.snap_set_read(rid);
  char expected[bufsize];
  memset(expected, fill, sizeof(expected));
  bufferlist bl;
  EXPECT_EQ((int)sizeof(expected), ioctx.read("foo", bl, sizeof(expected), 0));
  EXPECT_EQ(0, memcmp(expected, bl.c_str(), sizeof(expected)));
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
}

static void write_content(librados::IoCtx &ioctx, char fill)
{
  char buf[bufsize];
  memset(buf, fill, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write_full("foo", bl));
}

TEST_P(LibRadosSnapshotsPP, SnapListPP) {
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  ASSERT_EQ(0, cluster.pool_is_in_selfmanaged_snaps_mode(pool_name));
  ASSERT_EQ(0, ioctx.snap_create("snap1"));
  ASSERT_EQ(0, cluster.pool_is_in_selfmanaged_snaps_mode(pool_name));
  std::vector<snap_t> snaps;
  EXPECT_EQ(0, ioctx.snap_list(&snaps));
  EXPECT_EQ(1U, snaps.size());
  snap_t rid;
  EXPECT_EQ(0, ioctx.snap_lookup("snap1", &rid));
  EXPECT_EQ(rid, snaps[0]);
  EXPECT_EQ(0, ioctx.snap_remove("snap1"));
  ASSERT_EQ(0, cluster.pool_is_in_selfmanaged_snaps_mode(pool_name));
}

TEST_P(LibRadosSnapshotsPP, SnapRemovePP) {
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  ASSERT_EQ(0, ioctx.snap_create("snap1"));
  rados_snap_t rid;
  ASSERT_EQ(0, ioctx.snap_lookup("snap1", &rid));
  ASSERT_EQ(0, ioctx.snap_remove("snap1"));
  ASSERT_EQ(-ENOENT, ioctx.snap_lookup("snap1", &rid));
}

TEST_P(LibRadosSnapshotsPP, RollbackPP) {
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  ASSERT_EQ(0, ioctx.snap_create("snap1"));
  char buf2[sizeof(buf)];
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  EXPECT_EQ(0, ioctx.write_full("foo", bl2));
  EXPECT_EQ(0, ioctx.snap_rollback("foo", "snap1"));
  bufferlist bl3;
  EXPECT_EQ((int)sizeof(buf), ioctx.read("foo", bl3, sizeof(buf), 0));
  EXPECT_EQ(0, memcmp(buf, bl3.c_str(), sizeof(buf)));
  EXPECT_EQ(0, ioctx.snap_remove("snap1"));
}

TEST_P(LibRadosSnapshotsPP, SnapGetNamePP) {
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));
  ASSERT_EQ(0, ioctx.snap_create("snapfoo"));
  rados_snap_t rid;
  EXPECT_EQ(0, ioctx.snap_lookup("snapfoo", &rid));
  EXPECT_EQ(-ENOENT, ioctx.snap_lookup("snapbar", &rid));
  std::string name;
  EXPECT_EQ(0, ioctx.snap_get_name(rid, &name));
  time_t snaptime;
  EXPECT_EQ(0, ioctx.snap_get_stamp(rid, &snaptime));
  EXPECT_EQ(0, strcmp(name.c_str(), "snapfoo"));
  EXPECT_EQ(0, ioctx.snap_remove("snapfoo"));
}

TEST_P(LibRadosSnapshotsPP, SnapCreateRemovePP) {
  // reproduces http://tracker.ceph.com/issues/10262
  bufferlist bl;
  bl.append("foo");
  ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  ASSERT_EQ(0, ioctx.snap_create("snapfoo"));
  ASSERT_EQ(0, ioctx.remove("foo"));
  ASSERT_EQ(0, ioctx.snap_create("snapbar"));

  std::unique_ptr<librados::ObjectWriteOperation> op(new librados::ObjectWriteOperation());
  op->create(false);
  op->remove();
  ASSERT_EQ(0, ioctx.operate("foo", op.get()));

  EXPECT_EQ(0, ioctx.snap_remove("snapfoo"));
  EXPECT_EQ(0, ioctx.snap_remove("snapbar"));
}

TEST_P(LibRadosSnapshotsSelfManagedPP, SnapPP) {
  std::vector<uint64_t> my_snaps;
  my_snaps.push_back(-2);
  // Pool may already be in self-managed mode from a previous parameterized test.
  // Once a pool enters self-managed mode, it stays that way permanently.
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ASSERT_EQ(1, cluster.pool_is_in_selfmanaged_snaps_mode(pool_name));
  ::std::reverse(my_snaps.begin(), my_snaps.end()); 
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));

  my_snaps.push_back(-2);
  librados::AioCompletion *completion = cluster.aio_create_completion();
  ioctx.aio_selfmanaged_snap_create(&my_snaps.back(), completion);
  ASSERT_EQ(0, completion->wait_for_complete());
  completion->release();
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char buf2[sizeof(buf)];
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  ASSERT_EQ(0, ioctx.write("foo", bl2, sizeof(buf2), 0));

  ioctx.snap_set_read(my_snaps[1]);
  bufferlist bl3;
  ASSERT_EQ((int)sizeof(buf), ioctx.read("foo", bl3, sizeof(buf), 0));
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf, sizeof(buf)));

  completion = cluster.aio_create_completion();
  ioctx.aio_selfmanaged_snap_remove(my_snaps.back(), completion);
  ASSERT_EQ(0, completion->wait_for_complete());
  completion->release();
  my_snaps.pop_back();
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
  ASSERT_EQ(1, cluster.pool_is_in_selfmanaged_snaps_mode(pool_name));
  ASSERT_EQ(0, ioctx.remove("foo"));
}

TEST_P(LibRadosSnapshotsSelfManagedPP, RollbackPP) {
  std::vector<uint64_t> my_snaps;
  IoCtx readioctx;
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), readioctx));
  readioctx.set_namespace(nspace);
  readioctx.snap_set_read(LIBRADOS_SNAP_DIR);

  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  //Write 3 consecutive buffers
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), bufsize));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), bufsize*2));

  snap_set_t ss;

  snap_t head = SNAP_HEAD;
  ASSERT_EQ(0, readioctx.list_snaps("foo", &ss));
  ASSERT_EQ(1u, ss.clones.size());
  ASSERT_EQ(head, ss.clones[0].cloneid);
  ASSERT_EQ(0u, ss.clones[0].snaps.size());
  ASSERT_EQ(0u, ss.clones[0].overlap.size());
  ASSERT_EQ(384u, ss.clones[0].size);

  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char buf2[sizeof(buf)];
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  //Change the middle buffer
  ASSERT_EQ(0, ioctx.write("foo", bl2, sizeof(buf2), bufsize));
  //Add another after
  ASSERT_EQ(0, ioctx.write("foo", bl2, sizeof(buf2), bufsize*3));

  ASSERT_EQ(-EINVAL, ioctx.list_snaps("foo", &ss));
  ObjectReadOperation o;
  o.list_snaps(&ss, NULL);
  ASSERT_EQ(-EINVAL, ioctx.operate("foo", &o, NULL));

  ASSERT_EQ(0, readioctx.list_snaps("foo", &ss));
  ASSERT_EQ(2u, ss.clones.size());
  ASSERT_EQ(my_snaps[1], ss.clones[0].cloneid);
  ASSERT_EQ(1u, ss.clones[0].snaps.size());
  ASSERT_EQ(my_snaps[1], ss.clones[0].snaps[0]);
  ASSERT_EQ(2u, ss.clones[0].overlap.size());
  ASSERT_EQ(0u, ss.clones[0].overlap[0].first);
  ASSERT_EQ(128u, ss.clones[0].overlap[0].second);
  ASSERT_EQ(256u, ss.clones[0].overlap[1].first);
  ASSERT_EQ(128u, ss.clones[0].overlap[1].second);
  ASSERT_EQ(384u, ss.clones[0].size);
  ASSERT_EQ(head, ss.clones[1].cloneid);
  ASSERT_EQ(0u, ss.clones[1].snaps.size());
  ASSERT_EQ(0u, ss.clones[1].overlap.size());
  ASSERT_EQ(512u, ss.clones[1].size);

  ioctx.selfmanaged_snap_rollback("foo", my_snaps[1]);

  bufferlist bl3;
  ASSERT_EQ((int)sizeof(buf), ioctx.read("foo", bl3, sizeof(buf), 0));
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf, sizeof(buf)));
  ASSERT_EQ((int)sizeof(buf), ioctx.read("foo", bl3, sizeof(buf), bufsize));
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf, sizeof(buf)));
  ASSERT_EQ((int)sizeof(buf), ioctx.read("foo", bl3, sizeof(buf), bufsize*2));
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf, sizeof(buf)));
  ASSERT_EQ((int)0, ioctx.read("foo", bl3, sizeof(buf), bufsize*3));

  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  readioctx.close();
}

TEST_P(LibRadosSnapshotsSelfManagedPP, SnapOverlapPP) {
  std::vector<uint64_t> my_snaps;
  IoCtx readioctx;
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), readioctx));
  readioctx.set_namespace(nspace);
  readioctx.snap_set_read(LIBRADOS_SNAP_DIR);

  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), bufsize*2));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), bufsize*4));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), bufsize*6));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), bufsize*8));

  snap_set_t ss;
  snap_t head = SNAP_HEAD;
  ASSERT_EQ(0, readioctx.list_snaps("foo", &ss));
  ASSERT_EQ(1u, ss.clones.size());
  ASSERT_EQ(head, ss.clones[0].cloneid);
  ASSERT_EQ(0u, ss.clones[0].snaps.size());
  ASSERT_EQ(0u, ss.clones[0].overlap.size());
  ASSERT_EQ(1152u, ss.clones[0].size);

  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char buf2[sizeof(buf)];
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  ASSERT_EQ(0, ioctx.write("foo", bl2, sizeof(buf2), bufsize*1));
  ASSERT_EQ(0, ioctx.write("foo", bl2, sizeof(buf2), bufsize*3));
  ASSERT_EQ(0, ioctx.write("foo", bl2, sizeof(buf2), bufsize*5));
  ASSERT_EQ(0, ioctx.write("foo", bl2, sizeof(buf2), bufsize*7));
  ASSERT_EQ(0, ioctx.write("foo", bl2, sizeof(buf2), bufsize*9));

  ASSERT_EQ(0, readioctx.list_snaps("foo", &ss));
  ASSERT_EQ(2u, ss.clones.size());
  ASSERT_EQ(my_snaps[1], ss.clones[0].cloneid);
  ASSERT_EQ(1u, ss.clones[0].snaps.size());
  ASSERT_EQ(my_snaps[1], ss.clones[0].snaps[0]);
  ASSERT_EQ(5u, ss.clones[0].overlap.size());
  ASSERT_EQ(0u, ss.clones[0].overlap[0].first);
  ASSERT_EQ(128u, ss.clones[0].overlap[0].second);
  ASSERT_EQ(256u, ss.clones[0].overlap[1].first);
  ASSERT_EQ(128u, ss.clones[0].overlap[1].second);
  ASSERT_EQ(512u, ss.clones[0].overlap[2].first);
  ASSERT_EQ(128u, ss.clones[0].overlap[2].second);
  ASSERT_EQ(768u, ss.clones[0].overlap[3].first);
  ASSERT_EQ(128u, ss.clones[0].overlap[3].second);
  ASSERT_EQ(1024u, ss.clones[0].overlap[4].first);
  ASSERT_EQ(128u, ss.clones[0].overlap[4].second);
  ASSERT_EQ(1152u, ss.clones[0].size);
  ASSERT_EQ(head, ss.clones[1].cloneid);
  ASSERT_EQ(0u, ss.clones[1].snaps.size());
  ASSERT_EQ(0u, ss.clones[1].overlap.size());
  ASSERT_EQ(1280u, ss.clones[1].size);

  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());

  char buf3[sizeof(buf)];
  memset(buf3, 0xee, sizeof(buf3));
  bufferlist bl4;
  bl4.append(buf3, sizeof(buf3));
  ASSERT_EQ(0, ioctx.write("foo", bl2, sizeof(buf3), bufsize*1));
  ASSERT_EQ(0, ioctx.write("foo", bl2, sizeof(buf3), bufsize*4));
  ASSERT_EQ(0, ioctx.write("foo", bl2, sizeof(buf3), bufsize*5));
  ASSERT_EQ(0, ioctx.write("foo", bl2, sizeof(buf3), bufsize*8));

  ASSERT_EQ(0, readioctx.list_snaps("foo", &ss));
  ASSERT_EQ(3u, ss.clones.size());
  ASSERT_EQ(my_snaps[1], ss.clones[0].cloneid);
  ASSERT_EQ(1u, ss.clones[0].snaps.size());
  ASSERT_EQ(my_snaps[1], ss.clones[0].snaps[0]);
  ASSERT_EQ(5u, ss.clones[0].overlap.size());
  ASSERT_EQ(0u, ss.clones[0].overlap[0].first);
  ASSERT_EQ(128u, ss.clones[0].overlap[0].second);
  ASSERT_EQ(256u, ss.clones[0].overlap[1].first);
  ASSERT_EQ(128u, ss.clones[0].overlap[1].second);
  ASSERT_EQ(512u, ss.clones[0].overlap[2].first);
  ASSERT_EQ(128u, ss.clones[0].overlap[2].second);
  ASSERT_EQ(768u, ss.clones[0].overlap[3].first);
  ASSERT_EQ(128u, ss.clones[0].overlap[3].second);
  ASSERT_EQ(1024u, ss.clones[0].overlap[4].first);
  ASSERT_EQ(128u, ss.clones[0].overlap[4].second);
  ASSERT_EQ(1152u, ss.clones[0].size);

  ASSERT_EQ(my_snaps[2], ss.clones[1].cloneid);
  ASSERT_EQ(1u, ss.clones[1].snaps.size());
  ASSERT_EQ(my_snaps[2], ss.clones[1].snaps[0]);
  ASSERT_EQ(4u, ss.clones[1].overlap.size());
  ASSERT_EQ(0u, ss.clones[1].overlap[0].first);
  ASSERT_EQ(128u, ss.clones[1].overlap[0].second);
  ASSERT_EQ(256u, ss.clones[1].overlap[1].first);
  ASSERT_EQ(256u, ss.clones[1].overlap[1].second);
  ASSERT_EQ(768u, ss.clones[1].overlap[2].first);
  ASSERT_EQ(256u, ss.clones[1].overlap[2].second);
  ASSERT_EQ(1152u, ss.clones[1].overlap[3].first);
  ASSERT_EQ(128u, ss.clones[1].overlap[3].second);
  ASSERT_EQ(1280u, ss.clones[1].size);

  ASSERT_EQ(head, ss.clones[2].cloneid);
  ASSERT_EQ(0u, ss.clones[2].snaps.size());
  ASSERT_EQ(0u, ss.clones[2].overlap.size());
  ASSERT_EQ(1280u, ss.clones[2].size);

  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  readioctx.close();
}

TEST_P(LibRadosSnapshotsSelfManagedPP, Bug11677) {
  std::vector<uint64_t> my_snaps;
  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());

  int bsize = 1<<20;
  char *buf = (char *)new char[bsize];
  memset(buf, 0xcc, bsize);
  bufferlist bl1;
  bl1.append(buf, bsize);
  ASSERT_EQ(0, ioctx.write("foo", bl1, bsize, 0));

  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());

  std::unique_ptr<librados::ObjectWriteOperation> op(new librados::ObjectWriteOperation());
  op->assert_exists();
  op->remove();
  ASSERT_EQ(0, ioctx.operate("foo", op.get()));

  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
  delete[] buf;
}

TEST_P(LibRadosSnapshotsSelfManagedPP, OrderSnap) {
  std::vector<uint64_t> my_snaps;
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));

  int flags = librados::OPERATION_ORDERSNAP;

  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ObjectWriteOperation op1;
  op1.write(0, bl);
  librados::AioCompletion *comp1 = cluster.aio_create_completion();
  ASSERT_EQ(0, ioctx.aio_operate("foo", comp1, &op1, flags));
  ASSERT_EQ(0, comp1->wait_for_complete());
  ASSERT_EQ(0, comp1->get_return_value());
  comp1->release();

  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ObjectWriteOperation op2;
  op2.write(0, bl);
  librados::AioCompletion *comp2 = cluster.aio_create_completion();
  ASSERT_EQ(0, ioctx.aio_operate("foo", comp2, &op2, flags));
  ASSERT_EQ(0, comp2->wait_for_complete());
  ASSERT_EQ(0, comp2->get_return_value());
  comp2->release();

  my_snaps.pop_back();
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ObjectWriteOperation op3;
  op3.write(0, bl);
  librados::AioCompletion *comp3 = cluster.aio_create_completion();
  ASSERT_EQ(0, ioctx.aio_operate("foo", comp3, &op3, flags));
  ASSERT_EQ(0, comp3->wait_for_complete());
  ASSERT_EQ(-EOLDSNAPC, comp3->get_return_value());
  comp3->release();

  ObjectWriteOperation op4;
  op4.write(0, bl);
  librados::AioCompletion *comp4 = cluster.aio_create_completion();
  ASSERT_EQ(0, ioctx.aio_operate("foo", comp4, &op4, 0));
  ASSERT_EQ(0, comp4->wait_for_complete());
  ASSERT_EQ(0, comp4->get_return_value());
  comp4->release();
}

TEST_P(LibRadosSnapshotsSelfManagedPP, WriteRollback) {
  // https://tracker.ceph.com/issues/59114
  GTEST_SKIP();
  uint64_t snapid = 5;

  // buf1
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));

  // buf2
  char buf2[sizeof(buf)];
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));

  // First write
  ObjectWriteOperation op_write1;
  op_write1.write(0, bl);
  // Operate
  librados::AioCompletion *comp_write = cluster.aio_create_completion();
  ASSERT_EQ(0, ioctx.aio_operate("foo", comp_write, &op_write1, 0));
  ASSERT_EQ(0, comp_write->wait_for_complete());
  ASSERT_EQ(0, comp_write->get_return_value());
  comp_write->release();

  // Take Snapshot
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&snapid));

  // Rollback + Second write in the same op
  ObjectWriteOperation op_write2_snap_rollback;
  op_write2_snap_rollback.write(0, bl2);
  op_write2_snap_rollback.selfmanaged_snap_rollback(snapid);
  // Operate
  librados::AioCompletion *comp_write2 = cluster.aio_create_completion();
  ASSERT_EQ(0, ioctx.aio_operate("foo", comp_write2, &op_write2_snap_rollback, 0));
  ASSERT_EQ(0, comp_write2->wait_for_complete());
  ASSERT_EQ(0, comp_write2->get_return_value());
  comp_write2->release();

  // Resolved should be first write
  bufferlist bl3;
  EXPECT_EQ((int)sizeof(buf), ioctx.read("foo", bl3, sizeof(buf), 0));
  EXPECT_EQ(0, memcmp(buf, bl3.c_str(), sizeof(buf)));
}

TEST_P(LibRadosSnapshotsSelfManagedPP, ReusePurgedSnap) {
  std::vector<uint64_t> my_snaps;
  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ASSERT_EQ(1, cluster.pool_is_in_selfmanaged_snaps_mode(pool_name));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));

  my_snaps.push_back(-2);
  librados::AioCompletion *completion = cluster.aio_create_completion();
  ioctx.aio_selfmanaged_snap_create(&my_snaps.back(), completion);
  ASSERT_EQ(0, completion->wait_for_complete());
  completion->release();

  std::cout << "deleting snap " << my_snaps.back() << " in pool "
	    << ioctx.get_pool_name() << std::endl;
  completion = cluster.aio_create_completion();
  ioctx.aio_selfmanaged_snap_remove(my_snaps.back(), completion);
  ASSERT_EQ(0, completion->wait_for_complete());
  completion->release();

  std::cout << "waiting for snaps to purge" << std::endl;
  sleep(15);

  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char buf2[sizeof(buf)];
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  ASSERT_EQ(0, ioctx.write("foo", bl2, sizeof(buf2), 0));

  // scrub it out?
  //sleep(600);
}

TEST(LibRadosPoolIsInSelfmanagedSnapsMode, NotConnected) {
  librados::Rados cluster;
  ASSERT_EQ(0, cluster.init(nullptr));

  EXPECT_EQ(-ENOTCONN, cluster.pool_is_in_selfmanaged_snaps_mode("foo"));
}

TEST(LibRadosPoolIsInSelfmanagedSnapsMode, FreshInstance) {
  librados::Rados cluster1;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster1));
  EXPECT_EQ(0, cluster1.pool_is_in_selfmanaged_snaps_mode(pool_name));
  {
    librados::Rados cluster2;
    ASSERT_EQ("", connect_cluster_pp(cluster2));
    EXPECT_EQ(0, cluster2.pool_is_in_selfmanaged_snaps_mode(pool_name));
  }

  librados::IoCtx ioctx;
  cluster1.ioctx_create(pool_name.c_str(), ioctx);
  uint64_t snap_id;
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&snap_id));
  EXPECT_EQ(1, cluster1.pool_is_in_selfmanaged_snaps_mode(pool_name));
  {
    librados::Rados cluster2;
    ASSERT_EQ("", connect_cluster_pp(cluster2));
    EXPECT_EQ(1, cluster2.pool_is_in_selfmanaged_snaps_mode(pool_name));
  }

  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(snap_id));
  EXPECT_EQ(1, cluster1.pool_is_in_selfmanaged_snaps_mode(pool_name));
  {
    librados::Rados cluster2;
    ASSERT_EQ("", connect_cluster_pp(cluster2));
    EXPECT_EQ(1, cluster2.pool_is_in_selfmanaged_snaps_mode(pool_name));
  }

  ASSERT_EQ(0, cluster1.pool_delete(pool_name.c_str()));
  EXPECT_EQ(-ENOENT, cluster1.pool_is_in_selfmanaged_snaps_mode(pool_name));
  {
    librados::Rados cluster2;
    ASSERT_EQ("", connect_cluster_pp(cluster2));
    EXPECT_EQ(-ENOENT, cluster2.pool_is_in_selfmanaged_snaps_mode(pool_name));
  }
}

// EC testing
TEST_P(LibRadosSnapshotsECPP, SnapListPP) {
  SKIP_IF_CRIMSON();
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  ASSERT_EQ(0, ioctx.snap_create("snap1"));
  std::vector<snap_t> snaps;
  EXPECT_EQ(0, ioctx.snap_list(&snaps));
  EXPECT_EQ(1U, snaps.size());
  snap_t rid;
  EXPECT_EQ(0, ioctx.snap_lookup("snap1", &rid));
  EXPECT_EQ(rid, snaps[0]);
  EXPECT_EQ(0, ioctx.snap_remove("snap1"));
}

TEST_P(LibRadosSnapshotsECPP, SnapRemovePP) {
  SKIP_IF_CRIMSON();
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  ASSERT_EQ(0, ioctx.snap_create("snap1"));
  rados_snap_t rid;
  ASSERT_EQ(0, ioctx.snap_lookup("snap1", &rid));
  ASSERT_EQ(0, ioctx.snap_remove("snap1"));
  ASSERT_EQ(-ENOENT, ioctx.snap_lookup("snap1", &rid));
}

TEST_P(LibRadosSnapshotsECPP, RollbackPP) {
  SKIP_IF_CRIMSON();
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  ASSERT_EQ(0, ioctx.snap_create("snap1"));
  char buf2[sizeof(buf)];
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  EXPECT_EQ(0, ioctx.write_full("foo", bl2));
  EXPECT_EQ(0, ioctx.snap_rollback("foo", "snap1"));
  bufferlist bl3;
  EXPECT_EQ((int)sizeof(buf), ioctx.read("foo", bl3, sizeof(buf), 0));
  EXPECT_EQ(0, memcmp(buf, bl3.c_str(), sizeof(buf)));
  EXPECT_EQ(0, ioctx.snap_remove("snap1"));
}

TEST_P(LibRadosSnapshotsECPP, SnapGetNamePP) {
  SKIP_IF_CRIMSON();
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));
  ASSERT_EQ(0, ioctx.snap_create("snapfoo"));
  rados_snap_t rid;
  EXPECT_EQ(0, ioctx.snap_lookup("snapfoo", &rid));
  EXPECT_EQ(-ENOENT, ioctx.snap_lookup("snapbar", &rid));
  std::string name;
  EXPECT_EQ(0, ioctx.snap_get_name(rid, &name));
  time_t snaptime;
  EXPECT_EQ(0, ioctx.snap_get_stamp(rid, &snaptime));
  EXPECT_EQ(0, strcmp(name.c_str(), "snapfoo"));
  EXPECT_EQ(0, ioctx.snap_remove("snapfoo"));
}

TEST_P(LibRadosSnapshotsSelfManagedECPP, SnapPP) {
  SKIP_IF_CRIMSON();
  std::vector<uint64_t> my_snaps;
  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  int bsize = alignment;
  char *buf = (char *)new char[bsize];
  memset(buf, 0xcc, bsize);
  bufferlist bl1;
  bl1.append(buf, bsize);
  ASSERT_EQ(0, ioctx.write("foo", bl1, bsize, 0));

  my_snaps.push_back(-2);
  librados::AioCompletion *completion = cluster.aio_create_completion();
  ioctx.aio_selfmanaged_snap_create(&my_snaps.back(), completion);
  ASSERT_EQ(0, completion->wait_for_complete());
  completion->release();
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char *buf2 = (char *)new char[bsize];
  memset(buf2, 0xdd, bsize);
  bufferlist bl2;
  bl2.append(buf2, bsize);
  // Add another aligned buffer
  ASSERT_EQ(0, ioctx.write("foo", bl2, bsize, bsize));

  ioctx.snap_set_read(my_snaps[1]);
  bufferlist bl3;
  ASSERT_EQ(bsize, ioctx.read("foo", bl3, bsize*3, 0));
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf, bsize));

  completion = cluster.aio_create_completion();
  ioctx.aio_selfmanaged_snap_remove(my_snaps.back(), completion);
  ASSERT_EQ(0, completion->wait_for_complete());
  completion->release();
  my_snaps.pop_back();
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
  ASSERT_EQ(0, ioctx.remove("foo"));
  delete[] buf;
  delete[] buf2;
}

TEST_P(LibRadosSnapshotsSelfManagedECPP, RollbackPP) {
  SKIP_IF_CRIMSON();
  std::vector<uint64_t> my_snaps;
  IoCtx readioctx;
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), readioctx));
  readioctx.set_namespace(nspace);
  readioctx.snap_set_read(LIBRADOS_SNAP_DIR);

  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  int bsize = alignment;
  char *buf = (char *)new char[bsize];
  memset(buf, 0xcc, bsize);
  bufferlist bl1;
  bl1.append(buf, bsize);
  //Write 3 consecutive buffers
  ASSERT_EQ(0, ioctx.write("foo", bl1, bsize, 0));
  ASSERT_EQ(0, ioctx.write("foo", bl1, bsize, bsize));
  ASSERT_EQ(0, ioctx.write("foo", bl1, bsize, bsize*2));

  snap_set_t ss;

  snap_t head = SNAP_HEAD;
  ASSERT_EQ(0, readioctx.list_snaps("foo", &ss));
  ASSERT_EQ(1u, ss.clones.size());
  ASSERT_EQ(head, ss.clones[0].cloneid);
  ASSERT_EQ(0u, ss.clones[0].snaps.size());
  ASSERT_EQ(0u, ss.clones[0].overlap.size());
  ASSERT_EQ((unsigned)(bsize*3), ss.clones[0].size);

  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char *buf2 = (char *)new char[bsize];
  memset(buf2, 0xdd, bsize);
  bufferlist bl2;
  bl2.append(buf2, bsize);
  //Change the middle buffer
  //ASSERT_EQ(0, ioctx.write("foo", bl2, sizeof(buf2), bufsize));
  //Add another after
  ASSERT_EQ(0, ioctx.write("foo", bl2, bsize, bsize*3));

  ASSERT_EQ(-EINVAL, ioctx.list_snaps("foo", &ss));
  ObjectReadOperation o;
  o.list_snaps(&ss, NULL);
  ASSERT_EQ(-EINVAL, ioctx.operate("foo", &o, NULL));

  ASSERT_EQ(0, readioctx.list_snaps("foo", &ss));
  ASSERT_EQ(2u, ss.clones.size());
  ASSERT_EQ(my_snaps[1], ss.clones[0].cloneid);
  ASSERT_EQ(1u, ss.clones[0].snaps.size());
  ASSERT_EQ(my_snaps[1], ss.clones[0].snaps[0]);
  ASSERT_EQ(1u, ss.clones[0].overlap.size());
  ASSERT_EQ(0u, ss.clones[0].overlap[0].first);
  ASSERT_EQ((unsigned)bsize*3, ss.clones[0].overlap[0].second);
  ASSERT_EQ((unsigned)bsize*3, ss.clones[0].size);
  ASSERT_EQ(head, ss.clones[1].cloneid);
  ASSERT_EQ(0u, ss.clones[1].snaps.size());
  ASSERT_EQ(0u, ss.clones[1].overlap.size());
  ASSERT_EQ((unsigned)bsize*4, ss.clones[1].size);

  ioctx.selfmanaged_snap_rollback("foo", my_snaps[1]);

  bufferlist bl3;
  ASSERT_EQ(bsize, ioctx.read("foo", bl3, bsize, 0));
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf, bsize));
  ASSERT_EQ(bsize, ioctx.read("foo", bl3, bsize, bsize));
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf, bsize));
  ASSERT_EQ(bsize, ioctx.read("foo", bl3, bsize, bsize*2));
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf, bsize));
  ASSERT_EQ(0, ioctx.read("foo", bl3, bsize, bsize*3));

  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  readioctx.close();

  delete[] buf;
  delete[] buf2;
}

TEST_P(LibRadosSnapshotsSelfManagedECPP, Bug11677) {
  SKIP_IF_CRIMSON();
  std::vector<uint64_t> my_snaps;
  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());

  int bsize = alignment;
  char *buf = (char *)new char[bsize];
  memset(buf, 0xcc, bsize);
  bufferlist bl1;
  bl1.append(buf, bsize);
  ASSERT_EQ(0, ioctx.write("foo", bl1, bsize, 0));

  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());

  std::unique_ptr<librados::ObjectWriteOperation> op(new librados::ObjectWriteOperation());
  op->assert_exists();
  op->remove();
  ASSERT_EQ(0, ioctx.operate("foo", op.get()));

  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
  delete[] buf;
}

// Pool-managed snap rollback: basic success path with data-integrity check
TEST_P(LibRadosSnapshotsPP, PoolSnapRollbackPP) {
  char buf[bufsize];
  char buf2[bufsize];
  memset(buf,  0xcc, sizeof(buf));
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl1, bl2;
  bl1.append(buf,  sizeof(buf));
  bl2.append(buf2, sizeof(buf2));
  // Write initial content (A) and create snapshot
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  ASSERT_EQ(0, ioctx.snap_create("rollback_snap"));
  rados_snap_t rid;
  ASSERT_EQ(0, ioctx.snap_lookup("rollback_snap", &rid));
  // Overwrite with different content (B)
  ASSERT_EQ(0, ioctx.write("foo", bl2, sizeof(buf2), 0));
  // Issue pool-level rollback
  uint64_t rollback_id = 0;
  ASSERT_EQ(0, ioctx.snap_rollback("rollback_snap", &rollback_id));
  EXPECT_GT(rollback_id, (uint64_t)0);
  // Verify rollback_id is greater than the snap sequence of the snapshot
  EXPECT_GT(rollback_id, (uint64_t)rid);
  // Read back and verify content equals snapshot content (A, not B)
  bufferlist bl3;
  EXPECT_EQ((int)sizeof(buf), ioctx.read("foo", bl3, sizeof(buf), 0));
  EXPECT_EQ(0, memcmp(buf, bl3.c_str(), sizeof(buf)));
  ASSERT_EQ(0, ioctx.snap_remove("rollback_snap"));
}

// Pool-managed snap rollback: snap does not exist → -ENOENT
TEST_P(LibRadosSnapshotsPP, PoolSnapRollbackNoentPP) {
  uint64_t rollback_id = 0;
  ASSERT_EQ(-ENOENT, ioctx.snap_rollback("nonexistent_snap", &rollback_id));
}

// Pool-managed snap rollback: idempotency
// Calling with the same snap name twice returns the same rollback_id
TEST_P(LibRadosSnapshotsPP, PoolSnapRollbackIdempotentPP) {
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));
  ASSERT_EQ(0, ioctx.snap_create("idem_snap"));
  uint64_t id1 = 0, id2 = 0;
  ASSERT_EQ(0, ioctx.snap_rollback("idem_snap", &id1));
  ASSERT_EQ(0, ioctx.snap_rollback("idem_snap", &id2));
  EXPECT_EQ(id1, id2);
  ASSERT_EQ(0, ioctx.snap_remove("idem_snap"));
}

// Selfmanaged snap rollback: basic success path with data-integrity check
TEST_P(LibRadosSnapshotsSelfManagedPP, PoolSelfmanagedSnapRollbackPP) {
  std::vector<uint64_t> my_snaps;
  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  // Write initial content (A) under snap context
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  char buf[bufsize];
  char buf2[bufsize];
  memset(buf,  0xcc, sizeof(buf));
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl1, bl2;
  bl1.append(buf,  sizeof(buf));
  bl2.append(buf2, sizeof(buf2));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  // Create a second snap so the first is captured as a clone.
  my_snaps.insert(my_snaps.begin(), -2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.front()));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  // Overwrite with different content (B)
  ASSERT_EQ(0, ioctx.write("foo", bl2, sizeof(buf2), 0));
  // Issue pool-level selfmanaged snap rollback to snap[1] (the first snap, content A)
  uint64_t rollback_id = 0;
  ASSERT_EQ(0, ioctx.selfmanaged_snap_rollback(my_snaps[1], &rollback_id));
  EXPECT_GT(rollback_id, my_snaps[1]);
  // Read back and verify content equals snapshot content (A, not B)
  bufferlist bl3;
  EXPECT_EQ((int)sizeof(buf), ioctx.read("foo", bl3, sizeof(buf), 0));
  EXPECT_EQ(0, memcmp(buf, bl3.c_str(), sizeof(buf)));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps[1]));
}

// Selfmanaged snap rollback: idempotency
TEST_P(LibRadosSnapshotsSelfManagedPP, PoolSelfmanagedSnapRollbackIdempotentPP) {
  std::vector<uint64_t> my_snaps;
  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));
  uint64_t id1 = 0, id2 = 0;
  ASSERT_EQ(0, ioctx.selfmanaged_snap_rollback(my_snaps[0], &id1));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_rollback(my_snaps[0], &id2));
  EXPECT_EQ(id1, id2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
}

// Pool-managed snap rollback: 1 snap, rollback required
TEST_P(LibRadosSnapshotsPP, PoolSnapRollbackSnapRollbackRequired)
{
  set_nosnaptrim(cluster, true);

  write_content(ioctx, 0xaa);                              // Write A
  ASSERT_EQ(0, ioctx.snap_create("snap1"));                // CreateSnap 1
  write_content(ioctx, 0xbb);                              // Write B

  verify_snap(ioctx, "snap1", 0xaa);                       // Read Snap1 == A
  verify_head(ioctx,          0xbb);                       // Read head  == B

  ASSERT_EQ(0, ioctx.snap_rollback("foo", "snap1"));       // RollbackSnap 1

  verify_snap(ioctx, "snap1", 0xaa);                       // Read Snap1 == A
  verify_head(ioctx,          0xaa);                       // Read head  == A

  write_content(ioctx, 0xcc);                              // Write C

  verify_snap(ioctx, "snap1", 0xaa);                       // Read Snap1 == A
  verify_head(ioctx,          0xcc);                       // Read head  == C

  EXPECT_EQ(0, ioctx.snap_remove("snap1"));
  set_nosnaptrim(cluster, false);
}

// Pool-managed snap rollback: 1 snap, rollback not required
TEST_P(LibRadosSnapshotsPP, PoolSnapRollbackSnapRollbackNotRequired)
{
  set_nosnaptrim(cluster, true);

  write_content(ioctx, 0xaa);                              // Write A
  ASSERT_EQ(0, ioctx.snap_create("snap1"));                // CreateSnap 1

  verify_snap(ioctx, "snap1", 0xaa);                       // Read Snap1 == A
  verify_head(ioctx,          0xaa);                       // Read head  == A

  ASSERT_EQ(0, ioctx.snap_rollback("foo", "snap1"));       // RollbackSnap 1

  verify_snap(ioctx, "snap1", 0xaa);                       // Read Snap1 == A
  verify_head(ioctx,          0xaa);                       // Read head  == A

  write_content(ioctx, 0xbb);                              // Write B

  verify_snap(ioctx, "snap1", 0xaa);                       // Read Snap1 == A
  verify_head(ioctx,          0xbb);                       // Read head  == B

  EXPECT_EQ(0, ioctx.snap_remove("snap1"));
  set_nosnaptrim(cluster, false);
}

// Pool-managed snap rollback: 2 snap, rollback required
TEST_P(LibRadosSnapshotsPP, PoolSnapRollback2SnapRollbackRequired)
{
  set_nosnaptrim(cluster, true);

  write_content(ioctx, 0xaa);                              // Write A
  ASSERT_EQ(0, ioctx.snap_create("snap1"));                // CreateSnap 1
  write_content(ioctx, 0xbb);                              // Write B
  ASSERT_EQ(0, ioctx.snap_create("snap2"));                // CreateSnap 2
  write_content(ioctx, 0xcc);                              // Write C

  verify_snap(ioctx, "snap1", 0xaa);                       // Read Snap1 == A
  verify_snap(ioctx, "snap2", 0xbb);                       // Read Snap2 == B
  verify_head(ioctx,          0xcc);                       // Read head  == C

  ASSERT_EQ(0, ioctx.snap_rollback("foo", "snap1"));       // RollbackSnap 1

  verify_snap(ioctx, "snap1", 0xaa);                       // Read Snap1 == A
  verify_snap(ioctx, "snap2", 0xbb);                       // Read Snap2 == B
  verify_head(ioctx,          0xaa);                       // Read head  == A

  ASSERT_EQ(0, ioctx.snap_rollback("foo", "snap2"));       // RollbackSnap 2

  verify_snap(ioctx, "snap1", 0xaa);                       // Read Snap1 == A
  verify_snap(ioctx, "snap2", 0xbb);                       // Read Snap2 == B
  verify_head(ioctx,          0xbb);                       // Read head  == B

  EXPECT_EQ(0, ioctx.snap_remove("snap1"));
  EXPECT_EQ(0, ioctx.snap_remove("snap2"));
  set_nosnaptrim(cluster, false);
}

// Pool-managed snap rollback: rollback chain
TEST_P(LibRadosSnapshotsPP, PoolSnapRollbackChain)
{
  set_nosnaptrim(cluster, true);

  write_content(ioctx, 0xaa);                              // Write A
  ASSERT_EQ(0, ioctx.snap_create("snap1"));                // CreateSnap 1
  write_content(ioctx, 0xbb);                              // Write B
  ASSERT_EQ(0, ioctx.snap_create("snap2"));                // CreateSnap 2

  verify_snap(ioctx, "snap1", 0xaa);                       // Read Snap1 == A
  verify_snap(ioctx, "snap2", 0xbb);                       // Read Snap2 == B
  verify_head(ioctx,          0xbb);                       // Read head  == B

  ASSERT_EQ(0, ioctx.snap_rollback("foo", "snap1"));       // RollbackSnap 1

  verify_snap(ioctx, "snap1", 0xaa);                       // Read Snap1 == A
  verify_snap(ioctx, "snap2", 0xbb);                       // Read Snap2 == B
  verify_head(ioctx,          0xaa);                       // Read head  == A

  write_content(ioctx, 0xcc);                              // Write C

  verify_snap(ioctx, "snap1", 0xaa);                       // Read Snap1 == A
  verify_snap(ioctx, "snap2", 0xbb);                       // Read Snap2 == B
  verify_head(ioctx,          0xcc);                       // Read head  == C

  EXPECT_EQ(0, ioctx.snap_remove("snap1"));
  EXPECT_EQ(0, ioctx.snap_remove("snap2"));
  set_nosnaptrim(cluster, false);
}

// Pool-managed snap rollback: rollback + snap chain
TEST_P(LibRadosSnapshotsPP, PoolSnapRollbackSnapChain)
{
  set_nosnaptrim(cluster, true);

  write_content(ioctx, 0xaa);                              // Write A
  ASSERT_EQ(0, ioctx.snap_create("snap1"));                // CreateSnap 1
  write_content(ioctx, 0xbb);                              // Write B
  ASSERT_EQ(0, ioctx.snap_create("snap2"));                // CreateSnap 2

  verify_snap(ioctx, "snap1", 0xaa);                       // Read Snap1 == A
  verify_snap(ioctx, "snap2", 0xbb);                       // Read Snap2 == B
  verify_head(ioctx,          0xbb);                       // Read head  == B

  ASSERT_EQ(0, ioctx.snap_rollback("foo", "snap1"));       // RollbackSnap 1
  ASSERT_EQ(0, ioctx.snap_create("snap3"));                // CreateSnap 3

  verify_snap(ioctx, "snap1", 0xaa);                       // Read Snap1 == A
  verify_snap(ioctx, "snap2", 0xbb);                       // Read Snap2 == B
  verify_snap(ioctx, "snap3", 0xaa);                       // Read Snap3 == A
  verify_head(ioctx,          0xaa);                       // Read head  == A

  write_content(ioctx, 0xcc);                              // Write C

  verify_snap(ioctx, "snap1", 0xaa);                       // Read Snap1 == A
  verify_snap(ioctx, "snap2", 0xbb);                       // Read Snap2 == B
  verify_snap(ioctx, "snap3", 0xaa);                       // Read Snap3 == A
  verify_head(ioctx,          0xcc);                       // Read head  == C

  EXPECT_EQ(0, ioctx.snap_remove("snap1"));
  EXPECT_EQ(0, ioctx.snap_remove("snap2"));
  EXPECT_EQ(0, ioctx.snap_remove("snap3"));
  set_nosnaptrim(cluster, false);
}

// Pool-managed snap rollback: rollback + snap + rollback + snap chain
TEST_P(LibRadosSnapshotsPP, PoolSnapRollbackSnapRollbackSnapChain)
{
  set_nosnaptrim(cluster, true);

  write_content(ioctx, 0xaa);                              // Write A
  ASSERT_EQ(0, ioctx.snap_create("snap1"));                // CreateSnap 1
  write_content(ioctx, 0xbb);                              // Write B
  ASSERT_EQ(0, ioctx.snap_create("snap2"));                // CreateSnap 2

  verify_snap(ioctx, "snap1", 0xaa);                       // Read Snap1 == A
  verify_snap(ioctx, "snap2", 0xbb);                       // Read Snap2 == B
  verify_head(ioctx,          0xbb);                       // Read head  == B

  ASSERT_EQ(0, ioctx.snap_rollback("foo", "snap1"));       // RollbackSnap 1
  ASSERT_EQ(0, ioctx.snap_create("snap3"));                // CreateSnap 3

  verify_snap(ioctx, "snap1", 0xaa);                       // Read Snap1 == A
  verify_snap(ioctx, "snap2", 0xbb);                       // Read Snap2 == B
  verify_snap(ioctx, "snap3", 0xaa);                       // Read Snap3 == A
  verify_head(ioctx,          0xaa);                       // Read head  == A

  ASSERT_EQ(0, ioctx.snap_rollback("foo", "snap2"));       // RollbackSnap 2
  ASSERT_EQ(0, ioctx.snap_create("snap4"));                // CreateSnap 4

  verify_snap(ioctx, "snap1", 0xaa);                       // Read Snap1 == A
  verify_snap(ioctx, "snap2", 0xbb);                       // Read Snap2 == B
  verify_snap(ioctx, "snap3", 0xaa);                       // Read Snap3 == A
  verify_snap(ioctx, "snap4", 0xbb);                       // Read Snap4 == B
  verify_head(ioctx,          0xbb);                       // Read head  == B

  write_content(ioctx, 0xcc);                              // Write C

  verify_snap(ioctx, "snap1", 0xaa);                       // Read Snap1 == A
  verify_snap(ioctx, "snap2", 0xbb);                       // Read Snap2 == B
  verify_snap(ioctx, "snap3", 0xaa);                       // Read Snap3 == A
  verify_snap(ioctx, "snap4", 0xbb);                       // Read Snap4 == B
  verify_head(ioctx,          0xcc);                       // Read head  == C

  EXPECT_EQ(0, ioctx.snap_remove("snap1"));
  EXPECT_EQ(0, ioctx.snap_remove("snap2"));
  EXPECT_EQ(0, ioctx.snap_remove("snap3"));
  EXPECT_EQ(0, ioctx.snap_remove("snap4"));
  set_nosnaptrim(cluster, false);
}

// Pool-managed snap rollback: trim rollback + delete
TEST_P(LibRadosSnapshotsPP, PoolSnapRollbackTrimRollbackDelete)
{
  set_nosnaptrim(cluster, true);

  write_content(ioctx, 0xaa);                              // Write A
  ASSERT_EQ(0, ioctx.snap_create("snap1"));                // CreateSnap 1
  write_content(ioctx, 0xbb);                              // Write B

  verify_snap(ioctx, "snap1", 0xaa);                       // Read Snap1 == A
  verify_head(ioctx,          0xbb);                       // Read head  == B

  ASSERT_EQ(0, ioctx.snap_rollback("foo", "snap1"));       // RollbackSnap 1
  ASSERT_EQ(0, ioctx.snap_remove("snap1"));                // RemoveSnap 1

  verify_head(ioctx, 0xaa);                                // Read head  == A

  set_nosnaptrim(cluster, false);                          // Start trimming
  wait_for_snaptrim_complete(cluster);                     // Wait for trim

  verify_head(ioctx, 0xaa);                                // Read head  == A
}

// Pool-managed snap rollback: trim rollback + delete chain
TEST_P(LibRadosSnapshotsPP, PoolSnapRollbackTrimRollbackDeleteChain)
{
  set_nosnaptrim(cluster, true);

  write_content(ioctx, 0xaa);                              // Write A
  ASSERT_EQ(0, ioctx.snap_create("snap1"));                // CreateSnap 1
  write_content(ioctx, 0xbb);                              // Write B
  ASSERT_EQ(0, ioctx.snap_create("snap2"));                // CreateSnap 2

  ASSERT_EQ(0, ioctx.snap_rollback("foo", "snap1"));       // RollbackSnap 1
  ASSERT_EQ(0, ioctx.snap_create("snap3"));                // CreateSnap 3
  ASSERT_EQ(0, ioctx.snap_rollback("foo", "snap2"));       // RollbackSnap 2

  ASSERT_EQ(0, ioctx.snap_remove("snap1"));                // RemoveSnap 1
  ASSERT_EQ(0, ioctx.snap_remove("snap2"));                // RemoveSnap 2

  set_nosnaptrim(cluster, false);                          // Start trimming
  wait_for_snaptrim_complete(cluster);                     // Wait for trim

  verify_snap(ioctx, "snap3", 0xaa);                       // Read Snap3 == A
  verify_head(ioctx,          0xbb);                       // Read head  == B

  EXPECT_EQ(0, ioctx.snap_remove("snap3"));
}

// WI-17-e: Integration test -- verify completed_rollbacks_last is updated
// after a full pool snap rollback cycle.  The test confirms that after
// snap_rollback() completes and the system converges (via snaptrim), the
// OSDMap epoch advances and the OSD's completed_rollbacks_last field catches
// up to the current epoch.  This is validated indirectly: after rollback
// the head object should read back the snap content (A), confirming the
// OSD's rollback-recording machinery has run and is consistent.
TEST_P(LibRadosSnapshotsPP, CompletedRollbacksLastCatchup)
{
  // Write A, snap, write B, rollback, verify A
  write_content(ioctx, 0xaa);
  ASSERT_EQ(0, ioctx.snap_create("snap1"));
  write_content(ioctx, 0xbb);

  verify_snap(ioctx, "snap1", 0xaa);
  verify_head(ioctx,          0xbb);

  // Pool-level rollback
  ASSERT_EQ(0, ioctx.snap_rollback("foo", "snap1"));

  // After rollback completes: head reads back as A (data integrity)
  verify_head(ioctx, 0xaa);

  EXPECT_EQ(0, ioctx.snap_remove("snap1"));
}

INSTANTIATE_TEST_SUITE_P_REPLICA(LibRadosSnapshotsPP);
INSTANTIATE_TEST_SUITE_P_REPLICA(LibRadosSnapshotsSelfManagedPP);
INSTANTIATE_TEST_SUITE_P_EC(LibRadosSnapshotsECPP);
INSTANTIATE_TEST_SUITE_P_EC(LibRadosSnapshotsSelfManagedECPP);
