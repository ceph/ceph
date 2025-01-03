#include "include/rados.h"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"
#include "crimson_utils.h"

#include <algorithm>
#include <errno.h>
#include "gtest/gtest.h"
#include <string>

using std::string;

typedef RadosTest LibRadosSnapshots;
typedef RadosTest LibRadosSnapshotsSelfManaged;
typedef RadosTestEC LibRadosSnapshotsEC;
typedef RadosTestEC LibRadosSnapshotsSelfManagedEC;

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
  rados_completion_t completion;
  ASSERT_EQ(0, rados_aio_create_completion2(nullptr, nullptr,
                                           &completion));
  rados_aio_ioctx_selfmanaged_snap_create(ioctx, &my_snaps.back(), completion);
  ASSERT_EQ(0, rados_aio_wait_for_complete(completion));
  rados_aio_release(completion);
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

  ASSERT_EQ(0, rados_aio_create_completion2(nullptr, nullptr,
                                           &completion));
  rados_aio_ioctx_selfmanaged_snap_remove(ioctx, my_snaps.back(), completion);
  ASSERT_EQ(0, rados_aio_wait_for_complete(completion));
  rados_aio_release(completion);
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
  // First write
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));

  my_snaps.push_back(-2);
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_create(ioctx, &my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_set_write_ctx(ioctx, my_snaps[0],
					&my_snaps[0], my_snaps.size()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char buf2[sizeof(buf)];
  memset(buf2, 0xdd, sizeof(buf2));
  // Second write
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf2, sizeof(buf2), 0));
  // Rollback to my_snaps[1] - Object is expeceted to conatin the first write
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

TEST_F(LibRadosSnapshotsSelfManaged, FutureSnapRollback) {
  std::vector<uint64_t> my_snaps;
  // Snapshot 1
  my_snaps.push_back(-2);
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_create(ioctx, &my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_set_write_ctx(ioctx, my_snaps[0],
					&my_snaps[0], my_snaps.size()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char buf[bufsize];
  memset(buf, 0xcc, sizeof(buf));
  // First write
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));

  // Snapshot 2
  my_snaps.push_back(-2);
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_create(ioctx, &my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_set_write_ctx(ioctx, my_snaps[0],
					&my_snaps[0], my_snaps.size()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char buf2[sizeof(buf)];
  memset(buf2, 0xdd, sizeof(buf2));
  // Second write
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf2, sizeof(buf2), 0));
  // Snapshot 3
  my_snaps.push_back(-2);
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_create(ioctx, &my_snaps.back()));

  // Rollback to the last snap id - Object is expected to conatin
  // latest write (head object)
  rados_ioctx_selfmanaged_snap_rollback(ioctx, "foo", my_snaps[2]);
  char buf3[sizeof(buf)];
  ASSERT_EQ((int)sizeof(buf3), rados_read(ioctx, "foo", buf3, sizeof(buf3), 0));
  ASSERT_EQ(0, memcmp(buf3, buf2, sizeof(buf)));

  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_remove(ioctx, my_snaps.back()));
  my_snaps.pop_back();
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_remove(ioctx, my_snaps.back()));
  my_snaps.pop_back();
  ASSERT_EQ(0, rados_remove(ioctx, "foo"));
}



// EC testing
TEST_F(LibRadosSnapshotsEC, SnapList) {
  SKIP_IF_CRIMSON();
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

TEST_F(LibRadosSnapshotsEC, SnapRemove) {
  SKIP_IF_CRIMSON();
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

TEST_F(LibRadosSnapshotsEC, Rollback) {
  SKIP_IF_CRIMSON();
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

TEST_F(LibRadosSnapshotsEC, SnapGetName) {
  SKIP_IF_CRIMSON();
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

TEST_F(LibRadosSnapshotsSelfManagedEC, Snap) {
  SKIP_IF_CRIMSON();
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
  rados_completion_t completion;
  ASSERT_EQ(0, rados_aio_create_completion2(nullptr, nullptr,
                                           &completion));
  rados_aio_ioctx_selfmanaged_snap_create(ioctx, &my_snaps.back(), completion);
  ASSERT_EQ(0, rados_aio_wait_for_complete(completion));
  rados_aio_release(completion);
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

  ASSERT_EQ(0, rados_aio_create_completion2(nullptr, nullptr,
                                           &completion));
  rados_aio_ioctx_selfmanaged_snap_remove(ioctx, my_snaps.back(), completion);
  ASSERT_EQ(0, rados_aio_wait_for_complete(completion));
  rados_aio_release(completion);
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
  SKIP_IF_CRIMSON();
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
