#include "include/rados/librados.h"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"

#include "common/ceph_time.h"

#include <algorithm>
#include <errno.h>
#include "gtest/gtest.h"
#include "crimson_utils.h"

typedef RadosTest LibRadosStat;
typedef RadosTestEC LibRadosStatEC;

TEST_F(LibRadosStat, Stat) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  uint64_t size = 0;
  time_t mtime = 0;
  ASSERT_EQ(0, rados_stat(ioctx, "foo", &size, &mtime));
  ASSERT_EQ(sizeof(buf), size);
  ASSERT_EQ(-ENOENT, rados_stat(ioctx, "nonexistent", &size, &mtime));
}

TEST_F(LibRadosStat, Stat2) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  rados_write_op_t op = rados_create_write_op();
  rados_write_op_write(op, buf, sizeof(buf), 0);
  struct timespec ts;
  ts.tv_sec = 1457129052;
  ts.tv_nsec = 123456789;
  ASSERT_EQ(0, rados_write_op_operate2(op, ioctx, "foo", &ts, 0));
  rados_release_write_op(op);

  uint64_t size = 0;
  time_t mtime = 0;
  ASSERT_EQ(0, rados_stat(ioctx, "foo", &size, &mtime));
  ASSERT_EQ(sizeof(buf), size);
  ASSERT_EQ(mtime, ts.tv_sec);

  struct timespec ts2 = {};
  ASSERT_EQ(0, rados_stat2(ioctx, "foo", &size, &ts2));
  ASSERT_EQ(sizeof(buf), size);
  ASSERT_EQ(ts2.tv_sec, ts.tv_sec);
  ASSERT_EQ(ts2.tv_nsec, ts.tv_nsec);

  ASSERT_EQ(-ENOENT, rados_stat2(ioctx, "nonexistent", &size, &ts2));
}

TEST_F(LibRadosStat, StatNS) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  rados_ioctx_set_namespace(ioctx, "");
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_write(ioctx, "foo2", buf, sizeof(buf), 0));

  char buf2[64];
  memset(buf2, 0xcc, sizeof(buf2));
  rados_ioctx_set_namespace(ioctx, "nspace");
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf2, sizeof(buf2), 0));

  uint64_t size = 0;
  time_t mtime = 0;
  rados_ioctx_set_namespace(ioctx, "");
  ASSERT_EQ(0, rados_stat(ioctx, "foo", &size, &mtime));
  ASSERT_EQ(sizeof(buf), size);
  ASSERT_EQ(-ENOENT, rados_stat(ioctx, "nonexistent", &size, &mtime));

  rados_ioctx_set_namespace(ioctx, "nspace");
  ASSERT_EQ(0, rados_stat(ioctx, "foo", &size, &mtime));
  ASSERT_EQ(sizeof(buf2), size);
  ASSERT_EQ(-ENOENT, rados_stat(ioctx, "nonexistent", &size, &mtime));
  ASSERT_EQ(-ENOENT, rados_stat(ioctx, "foo2", &size, &mtime));
}

TEST_F(LibRadosStat, ClusterStat) {
  struct rados_cluster_stat_t result;
  ASSERT_EQ(0, rados_cluster_stat(cluster, &result));
}

TEST_F(LibRadosStat, PoolStat) {
  char buf[128];
  char actual_pool_name[80];
  unsigned l = rados_ioctx_get_pool_name(ioctx, actual_pool_name, sizeof(actual_pool_name));
  ASSERT_EQ(strlen(actual_pool_name), l);
  ASSERT_EQ(0, strcmp(actual_pool_name, pool_name.c_str()));
  memset(buf, 0xff, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  struct rados_pool_stat_t stats;
  memset(&stats, 0, sizeof(stats));
  ASSERT_EQ(0, rados_ioctx_pool_stat(ioctx, &stats));
}

TEST_F(LibRadosStatEC, Stat) {
  SKIP_IF_CRIMSON();
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  uint64_t size = 0;
  time_t mtime = 0;
  ASSERT_EQ(0, rados_stat(ioctx, "foo", &size, &mtime));
  ASSERT_EQ(sizeof(buf), size);
  ASSERT_EQ(-ENOENT, rados_stat(ioctx, "nonexistent", &size, &mtime));
}

TEST_F(LibRadosStatEC, StatNS) {
  SKIP_IF_CRIMSON();
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  rados_ioctx_set_namespace(ioctx, "");
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_write(ioctx, "foo2", buf, sizeof(buf), 0));

  char buf2[64];
  memset(buf2, 0xcc, sizeof(buf2));
  rados_ioctx_set_namespace(ioctx, "nspace");
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf2, sizeof(buf2), 0));

  uint64_t size = 0;
  time_t mtime = 0;
  rados_ioctx_set_namespace(ioctx, "");
  ASSERT_EQ(0, rados_stat(ioctx, "foo", &size, &mtime));
  ASSERT_EQ(sizeof(buf), size);
  ASSERT_EQ(-ENOENT, rados_stat(ioctx, "nonexistent", &size, &mtime));

  rados_ioctx_set_namespace(ioctx, "nspace");
  ASSERT_EQ(0, rados_stat(ioctx, "foo", &size, &mtime));
  ASSERT_EQ(sizeof(buf2), size);
  ASSERT_EQ(-ENOENT, rados_stat(ioctx, "nonexistent", &size, &mtime));
  ASSERT_EQ(-ENOENT, rados_stat(ioctx, "foo2", &size, &mtime));
}

TEST_F(LibRadosStatEC, ClusterStat) {
  SKIP_IF_CRIMSON();
  struct rados_cluster_stat_t result;
  ASSERT_EQ(0, rados_cluster_stat(cluster, &result));
}

TEST_F(LibRadosStatEC, PoolStat) {
  SKIP_IF_CRIMSON();
  char buf[128];
  char actual_pool_name[80];
  unsigned l = rados_ioctx_get_pool_name(ioctx, actual_pool_name, sizeof(actual_pool_name));
  ASSERT_EQ(strlen(actual_pool_name), l);
  ASSERT_EQ(0, strcmp(actual_pool_name, pool_name.c_str()));
  memset(buf, 0xff, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  struct rados_pool_stat_t stats;
  memset(&stats, 0, sizeof(stats));
  ASSERT_EQ(0, rados_ioctx_pool_stat(ioctx, &stats));
}
