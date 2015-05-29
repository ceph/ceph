#include "include/rados/librados.hpp"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"

#include <algorithm>
#include <errno.h>
#include "gtest/gtest.h"
#include <string>

using namespace librados;
using std::string;

typedef RadosTest LibRadosSnapshots;
typedef RadosTest LibRadosSnapshotsSelfManaged;
typedef RadosTestPP LibRadosSnapshotsPP;
typedef RadosTestPP LibRadosSnapshotsSelfManagedPP;
typedef RadosTestEC LibRadosSnapshotsEC;
typedef RadosTestEC LibRadosSnapshotsSelfManagedEC;
typedef RadosTestECPP LibRadosSnapshotsECPP;
typedef RadosTestECPP LibRadosSnapshotsSelfManagedECPP;

const int bufsize = 128;

TEST_F(LibRadosSnapshots, SnapList) {
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_ioctx_snap_create(ioctx, "snap1"));
  rados_snap_t snaps[10];
  EXPECT_EQ(1, rados_ioctx_snap_list(ioctx, snaps,
				     sizeof(snaps) / sizeof(snaps[0])));
  rados_snap_t rid;
  EXPECT_EQ(0, rados_ioctx_snap_lookup(ioctx, "snap1", &rid));
  EXPECT_EQ(rid, snaps[0]);
  EXPECT_EQ(0, rados_ioctx_snap_remove(ioctx, "snap1"));
}

TEST_F(LibRadosSnapshotsPP, SnapListPP) {
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

TEST_F(LibRadosSnapshots, SnapRemove) {
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_ioctx_snap_create(ioctx, "snap1"));
  rados_snap_t rid;
  ASSERT_EQ(0, rados_ioctx_snap_lookup(ioctx, "snap1", &rid));
  ASSERT_EQ(-EEXIST, rados_ioctx_snap_create(ioctx, "snap1"));
  ASSERT_EQ(0, rados_ioctx_snap_remove(ioctx, "snap1"));
  ASSERT_EQ(-ENOENT, rados_ioctx_snap_lookup(ioctx, "snap1", &rid));
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

TEST_F(LibRadosSnapshots, Rollback) {
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_ioctx_snap_create(ioctx, "snap1"));
  char buf2[sizeof(buf)];
  memset(buf2, 0xdd, sizeof(buf2));
  EXPECT_EQ(0, rados_write_full(ioctx, "foo", buf2, sizeof(buf2)));
  EXPECT_EQ(0, rados_ioctx_snap_rollback(ioctx, "foo", "snap1"));
  char buf3[sizeof(buf)];
  EXPECT_EQ((int)sizeof(buf3), rados_read(ioctx, "foo", buf3, sizeof(buf3), 0));
  EXPECT_EQ(0, memcmp(buf, buf3, sizeof(buf)));
  EXPECT_EQ(0, rados_ioctx_snap_remove(ioctx, "snap1"));
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

TEST_F(LibRadosSnapshots, SnapGetName) {
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_ioctx_snap_create(ioctx, "snapfoo"));
  rados_snap_t rid;
  EXPECT_EQ(0, rados_ioctx_snap_lookup(ioctx, "snapfoo", &rid));
  EXPECT_EQ(-ENOENT, rados_ioctx_snap_lookup(ioctx, "snapbar", &rid));
  char name[128];
  memset(name, 0, sizeof(name));
  EXPECT_EQ(0, rados_ioctx_snap_get_name(ioctx, rid, name, sizeof(name)));
  time_t snaptime;
  EXPECT_EQ(0, rados_ioctx_snap_get_stamp(ioctx, rid, &snaptime));
  EXPECT_EQ(0, strcmp(name, "snapfoo"));
  EXPECT_EQ(0, rados_ioctx_snap_remove(ioctx, "snapfoo"));
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

  librados::ObjectWriteOperation *op = new librados::ObjectWriteOperation();
  op->create(false);
  op->remove();
  ASSERT_EQ(0, ioctx.operate("foo", op));

  EXPECT_EQ(0, ioctx.snap_remove("snapfoo"));
  EXPECT_EQ(0, ioctx.snap_remove("snapbar"));

  delete op;
}

TEST_F(LibRadosSnapshotsSelfManaged, Snap) {
  std::vector<uint64_t> my_snaps;
  my_snaps.push_back(-2);
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_create(ioctx, &my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_set_write_ctx(ioctx, my_snaps[0],
					&my_snaps[0], my_snaps.size()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));

  my_snaps.push_back(-2);
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_create(ioctx, &my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_set_write_ctx(ioctx, my_snaps[0],
					&my_snaps[0], my_snaps.size()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char buf2[sizeof(buf)];
  memset(buf2, 0xdd, sizeof(buf2));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf2, sizeof(buf2), 0));
  rados_ioctx_snap_set_read(ioctx, my_snaps[1]-1);
  char buf3[sizeof(buf)];
  ASSERT_EQ(-ENOENT, rados_read(ioctx, "foo", buf3, sizeof(buf3), 0));

  rados_ioctx_snap_set_read(ioctx, my_snaps[1]);
  ASSERT_EQ((int)sizeof(buf3), rados_read(ioctx, "foo", buf3, sizeof(buf3), 0));
  ASSERT_EQ(0, memcmp(buf3, buf, sizeof(buf)));

  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_remove(ioctx, my_snaps.back()));
  my_snaps.pop_back();
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_remove(ioctx, my_snaps.back()));
  my_snaps.pop_back();
  rados_ioctx_snap_set_read(ioctx, LIBRADOS_SNAP_HEAD);
  ASSERT_EQ(0, rados_remove(ioctx, "foo"));
}

TEST_F(LibRadosSnapshotsSelfManaged, Rollback) {
  std::vector<uint64_t> my_snaps;
  my_snaps.push_back(-2);
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_create(ioctx, &my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_set_write_ctx(ioctx, my_snaps[0],
					&my_snaps[0], my_snaps.size()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));

  my_snaps.push_back(-2);
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_create(ioctx, &my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_set_write_ctx(ioctx, my_snaps[0],
					&my_snaps[0], my_snaps.size()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char buf2[sizeof(buf)];
  memset(buf2, 0xdd, sizeof(buf2));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf2, sizeof(buf2), 0));
  rados_ioctx_selfmanaged_snap_rollback(ioctx, "foo", my_snaps[1]);
  char buf3[sizeof(buf)];
  ASSERT_EQ((int)sizeof(buf3), rados_read(ioctx, "foo", buf3, sizeof(buf3), 0));
  ASSERT_EQ(0, memcmp(buf3, buf, sizeof(buf)));

  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_remove(ioctx, my_snaps.back()));
  my_snaps.pop_back();
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_remove(ioctx, my_snaps.back()));
  my_snaps.pop_back();
  ASSERT_EQ(0, rados_remove(ioctx, "foo"));
}

TEST_F(LibRadosSnapshotsSelfManagedPP, SnapPP) {
  std::vector<uint64_t> my_snaps;
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

  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
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

  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
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

  librados::ObjectWriteOperation *op = new librados::ObjectWriteOperation();
  op->assert_exists();
  op->remove();
  ASSERT_EQ(0, ioctx.operate("foo", op));

  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
  delete[] buf;
}

// EC testing
TEST_F(LibRadosSnapshotsEC, SnapList) {
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_ioctx_snap_create(ioctx, "snap1"));
  rados_snap_t snaps[10];
  EXPECT_EQ(1, rados_ioctx_snap_list(ioctx, snaps,
				     sizeof(snaps) / sizeof(snaps[0])));
  rados_snap_t rid;
  EXPECT_EQ(0, rados_ioctx_snap_lookup(ioctx, "snap1", &rid));
  EXPECT_EQ(rid, snaps[0]);
  EXPECT_EQ(0, rados_ioctx_snap_remove(ioctx, "snap1"));
}

TEST_F(LibRadosSnapshotsECPP, SnapListPP) {
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

TEST_F(LibRadosSnapshotsEC, SnapRemove) {
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_ioctx_snap_create(ioctx, "snap1"));
  rados_snap_t rid;
  ASSERT_EQ(0, rados_ioctx_snap_lookup(ioctx, "snap1", &rid));
  ASSERT_EQ(-EEXIST, rados_ioctx_snap_create(ioctx, "snap1"));
  ASSERT_EQ(0, rados_ioctx_snap_remove(ioctx, "snap1"));
  ASSERT_EQ(-ENOENT, rados_ioctx_snap_lookup(ioctx, "snap1", &rid));
}

TEST_F(LibRadosSnapshotsECPP, SnapRemovePP) {
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

TEST_F(LibRadosSnapshotsEC, Rollback) {
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_ioctx_snap_create(ioctx, "snap1"));
  char buf2[sizeof(buf)];
  memset(buf2, 0xdd, sizeof(buf2));
  EXPECT_EQ(0, rados_write_full(ioctx, "foo", buf2, sizeof(buf2)));
  EXPECT_EQ(0, rados_ioctx_snap_rollback(ioctx, "foo", "snap1"));
  char buf3[sizeof(buf)];
  EXPECT_EQ((int)sizeof(buf3), rados_read(ioctx, "foo", buf3, sizeof(buf3), 0));
  EXPECT_EQ(0, memcmp(buf, buf3, sizeof(buf)));
  EXPECT_EQ(0, rados_ioctx_snap_remove(ioctx, "snap1"));
}

TEST_F(LibRadosSnapshotsECPP, RollbackPP) {
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

TEST_F(LibRadosSnapshotsEC, SnapGetName) {
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_ioctx_snap_create(ioctx, "snapfoo"));
  rados_snap_t rid;
  EXPECT_EQ(0, rados_ioctx_snap_lookup(ioctx, "snapfoo", &rid));
  EXPECT_EQ(-ENOENT, rados_ioctx_snap_lookup(ioctx, "snapbar", &rid));
  char name[128];
  memset(name, 0, sizeof(name));
  EXPECT_EQ(0, rados_ioctx_snap_get_name(ioctx, rid, name, sizeof(name)));
  time_t snaptime;
  EXPECT_EQ(0, rados_ioctx_snap_get_stamp(ioctx, rid, &snaptime));
  EXPECT_EQ(0, strcmp(name, "snapfoo"));
  EXPECT_EQ(0, rados_ioctx_snap_remove(ioctx, "snapfoo"));
}

TEST_F(LibRadosSnapshotsECPP, SnapGetNamePP) {
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

TEST_F(LibRadosSnapshotsSelfManagedEC, Snap) {
  std::vector<uint64_t> my_snaps;
  my_snaps.push_back(-2);
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_create(ioctx, &my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_set_write_ctx(ioctx, my_snaps[0],
					&my_snaps[0], my_snaps.size()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  int bsize = alignment;
  char *buf = (char *)new char[bsize];
  memset(buf, 0xcc, bsize);
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, bsize, 0));

  my_snaps.push_back(-2);
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_create(ioctx, &my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_set_write_ctx(ioctx, my_snaps[0],
					&my_snaps[0], my_snaps.size()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char *buf2 = (char *)new char[bsize];
  memset(buf2, 0xdd, bsize);
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf2, bsize, bsize));
  rados_ioctx_snap_set_read(ioctx, my_snaps[1]-1);
  char *buf3 = (char *)new char[bsize*2];
  ASSERT_EQ(-ENOENT, rados_read(ioctx, "foo", buf3, bsize*2, 0));

  rados_ioctx_snap_set_read(ioctx, my_snaps[1]);
  ASSERT_EQ(bsize, rados_read(ioctx, "foo", buf3, bsize*2, 0));
  ASSERT_EQ(0, memcmp(buf3, buf, bsize));

  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_remove(ioctx, my_snaps.back()));
  my_snaps.pop_back();
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_remove(ioctx, my_snaps.back()));
  my_snaps.pop_back();
  rados_ioctx_snap_set_read(ioctx, LIBRADOS_SNAP_HEAD);
  ASSERT_EQ(0, rados_remove(ioctx, "foo"));
  delete[] buf;
  delete[] buf2;
  delete[] buf3;
}

TEST_F(LibRadosSnapshotsSelfManagedEC, Rollback) {
  std::vector<uint64_t> my_snaps;
  my_snaps.push_back(-2);
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_create(ioctx, &my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_set_write_ctx(ioctx, my_snaps[0],
					&my_snaps[0], my_snaps.size()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  int bsize = alignment;
  char *buf = (char *)new char[bsize];
  memset(buf, 0xcc, bsize);
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, bsize, 0));

  my_snaps.push_back(-2);
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_create(ioctx, &my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_set_write_ctx(ioctx, my_snaps[0],
					&my_snaps[0], my_snaps.size()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char *buf2 = (char *)new char[bsize];
  memset(buf2, 0xdd, bsize);

  ASSERT_EQ(0, rados_write(ioctx, "foo", buf2, bsize, bsize));
  rados_ioctx_selfmanaged_snap_rollback(ioctx, "foo", my_snaps[1]);
  char *buf3 = (char *)new char[bsize*2];
  ASSERT_EQ(bsize, rados_read(ioctx, "foo", buf3, bsize*2, 0));
  ASSERT_EQ(0, memcmp(buf3, buf, bsize));

  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_remove(ioctx, my_snaps.back()));
  my_snaps.pop_back();
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_remove(ioctx, my_snaps.back()));
  my_snaps.pop_back();
  ASSERT_EQ(0, rados_remove(ioctx, "foo"));
  delete[] buf;
  delete[] buf2;
  delete[] buf3;
}

TEST_F(LibRadosSnapshotsSelfManagedECPP, SnapPP) {
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

  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
  ASSERT_EQ(0, ioctx.remove("foo"));
  delete[] buf;
  delete[] buf2;
}

TEST_F(LibRadosSnapshotsSelfManagedECPP, RollbackPP) {
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

  librados::ObjectWriteOperation *op = new librados::ObjectWriteOperation();
  op->assert_exists();
  op->remove();
  ASSERT_EQ(0, ioctx.operate("foo", op));

  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ioctx.snap_set_read(LIBRADOS_SNAP_HEAD);
  delete[] buf;
}

