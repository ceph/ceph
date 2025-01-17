#include <algorithm>
#include <errno.h>
#include <string>

#include "gtest/gtest.h"

#include "include/rados.h"
#include "include/rados/librados.hpp"
#include "test/librados/test_cxx.h"
#include "test/librados/testcase_cxx.h"
#include "crimson_utils.h"

using namespace librados;

typedef RadosTestPP LibRadosSnapshotsPP;
typedef RadosTestPP LibRadosSnapshotsSelfManagedPP;
typedef RadosTestECPP LibRadosSnapshotsECPP;
typedef RadosTestECPP LibRadosSnapshotsSelfManagedECPP;

const int bufsize = 128;

TEST_F(LibRadosSnapshotsPP, SnapListPP) {
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

TEST_F(LibRadosSnapshotsPP, SnapRemovePP) {
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

TEST_F(LibRadosSnapshotsPP, RollbackPP) {
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

TEST_F(LibRadosSnapshotsPP, SnapGetNamePP) {
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

TEST_F(LibRadosSnapshotsPP, SnapCreateRemovePP) {
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

TEST_F(LibRadosSnapshotsSelfManagedPP, SnapPP) {
  std::vector<uint64_t> my_snaps;
  my_snaps.push_back(-2);
  ASSERT_EQ(0, cluster.pool_is_in_selfmanaged_snaps_mode(pool_name));
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

TEST_F(LibRadosSnapshotsSelfManagedPP, RollbackPP) {
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

TEST_F(LibRadosSnapshotsSelfManagedPP, SnapOverlapPP) {
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

TEST_F(LibRadosSnapshotsSelfManagedPP, Bug11677) {
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

TEST_F(LibRadosSnapshotsSelfManagedPP, OrderSnap) {
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

TEST_F(LibRadosSnapshotsSelfManagedPP, WriteRollback) {
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

TEST_F(LibRadosSnapshotsSelfManagedPP, ReusePurgedSnap) {
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
TEST_F(LibRadosSnapshotsECPP, SnapListPP) {
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

TEST_F(LibRadosSnapshotsECPP, SnapRemovePP) {
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

TEST_F(LibRadosSnapshotsECPP, RollbackPP) {
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

TEST_F(LibRadosSnapshotsECPP, SnapGetNamePP) {
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

TEST_F(LibRadosSnapshotsSelfManagedECPP, SnapPP) {
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

TEST_F(LibRadosSnapshotsSelfManagedECPP, RollbackPP) {
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

TEST_F(LibRadosSnapshotsSelfManagedECPP, Bug11677) {
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
