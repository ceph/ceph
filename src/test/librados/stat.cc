#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"

#include "common/ceph_time.h"

#include <algorithm>
#include <errno.h>
#include "gtest/gtest.h"

using namespace librados;

typedef RadosTest LibRadosStat;
typedef RadosTestPP LibRadosStatPP;
typedef RadosTestEC LibRadosStatEC;
typedef RadosTestECPP LibRadosStatECPP;

TEST_F(LibRadosStat, Stat) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  uint64_t size;
  time_t mtime;
  ASSERT_EQ(0, rados_stat(ioctx, "foo", &size, &mtime));
  ASSERT_EQ(sizeof(buf), size);
  ASSERT_EQ(-ENOENT, rados_stat(ioctx, "nonexistent", &size, &mtime));
}

TEST_F(LibRadosStatPP, StatPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));
  uint64_t size;
  time_t mtime;
  ASSERT_EQ(0, ioctx.stat("foo", &size, &mtime));
  ASSERT_EQ(sizeof(buf), size);
  ASSERT_EQ(-ENOENT, ioctx.stat("nonexistent", &size, &mtime));
}

TEST_F(LibRadosStatPP, Stat2Mtime2PP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  librados::ObjectWriteOperation op;
  struct timespec ts;
  ts.tv_sec = 1457129052;
  ts.tv_nsec = 123456789;
  op.mtime2(&ts);
  op.write(0, bl);
  ASSERT_EQ(0, ioctx.operate("foo", &op));

  uint64_t size;
  time_t mtime;
  ASSERT_EQ(0, ioctx.stat("foo", &size, &mtime));
  ASSERT_EQ(sizeof(buf), size);
  ASSERT_EQ(mtime, ts.tv_sec);

  struct timespec ts2;
  ASSERT_EQ(0, ioctx.stat2("foo", &size, &ts2));
  ASSERT_EQ(sizeof(buf), size);
  ASSERT_EQ(ts2.tv_sec, ts.tv_sec);
  ASSERT_EQ(ts2.tv_nsec, ts.tv_nsec);

  real_time rt;
  ASSERT_EQ(0, ioctx.stat2("foo", &size, (ceph_real_time_t *)&rt));
  ASSERT_EQ(sizeof(buf), size);

  struct timespec ts3 = ceph::real_clock::to_timespec(rt);
  ASSERT_EQ(ts3.tv_sec, ts.tv_sec);
  ASSERT_EQ(ts3.tv_nsec, ts.tv_nsec);

  ASSERT_EQ(-ENOENT, ioctx.stat2("nonexistent", &size, (ceph_real_time_t *)&rt));
}

TEST_F(LibRadosStatPP, Stat2Mtime2RealTimePP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  librados::ObjectWriteOperation op;
  struct timespec ts;
  ts.tv_sec = 1457129052;
  ts.tv_nsec = 123456789;
  real_time rt = real_clock::from_timespec(ts);
  op.mtime2((ceph_real_time_t *)&rt);
  op.write(0, bl);
  ASSERT_EQ(0, ioctx.operate("foo", &op));

  uint64_t size;
  time_t mtime;
  ASSERT_EQ(0, ioctx.stat("foo", &size, &mtime));
  ASSERT_EQ(sizeof(buf), size);
  ASSERT_EQ(mtime, ts.tv_sec);

  struct timespec ts2;
  ASSERT_EQ(0, ioctx.stat2("foo", &size, &ts2));
  ASSERT_EQ(sizeof(buf), size);
  ASSERT_EQ(ts2.tv_sec, ts.tv_sec);
  ASSERT_EQ(ts2.tv_nsec, ts.tv_nsec);

  real_time rt2;
  ASSERT_EQ(0, ioctx.stat2("foo", &size, (ceph_real_time_t *)&rt2));
  ASSERT_EQ(sizeof(buf), size);

  ASSERT_EQ(rt, rt2);

  ASSERT_EQ(-ENOENT, ioctx.stat2("nonexistent", &size, (ceph_real_time_t *)&rt));
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

  uint64_t size;
  time_t mtime;
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

TEST_F(LibRadosStatPP, StatPPNS) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ioctx.set_namespace("");
  ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));
  ASSERT_EQ(0, ioctx.write("foo2", bl, sizeof(buf), 0));

  char buf2[64];
  memset(buf2, 0xbb, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  ioctx.set_namespace("nspace");
  ASSERT_EQ(0, ioctx.write("foo", bl2, sizeof(buf2), 0));

  uint64_t size;
  time_t mtime;
  ioctx.set_namespace("");
  ASSERT_EQ(0, ioctx.stat("foo", &size, &mtime));
  ASSERT_EQ(sizeof(buf), size);
  ASSERT_EQ(-ENOENT, ioctx.stat("nonexistent", &size, &mtime));

  ioctx.set_namespace("nspace");
  ASSERT_EQ(0, ioctx.stat("foo", &size, &mtime));
  ASSERT_EQ(sizeof(buf2), size);
  ASSERT_EQ(-ENOENT, ioctx.stat("nonexistent", &size, &mtime));
  ASSERT_EQ(-ENOENT, ioctx.stat("foo2", &size, &mtime));
}

TEST_F(LibRadosStat, ClusterStat) {
  struct rados_cluster_stat_t result;
  ASSERT_EQ(0, rados_cluster_stat(cluster, &result));
}

TEST_F(LibRadosStatPP, ClusterStatPP) {
  cluster_stat_t cstat;
  ASSERT_EQ(0, cluster.cluster_stat(cstat));
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

TEST_F(LibRadosStatPP, PoolStatPP) {
  std::string n = ioctx.get_pool_name();
  ASSERT_EQ(n, pool_name);
  char buf[128];
  memset(buf, 0xff, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  std::list<std::string> v;
  std::map<std::string,stats_map> stats;
  ASSERT_EQ(0, cluster.get_pool_stats(v, stats));
}

TEST_F(LibRadosStatEC, Stat) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  uint64_t size;
  time_t mtime;
  ASSERT_EQ(0, rados_stat(ioctx, "foo", &size, &mtime));
  ASSERT_EQ(sizeof(buf), size);
  ASSERT_EQ(-ENOENT, rados_stat(ioctx, "nonexistent", &size, &mtime));
}

TEST_F(LibRadosStatECPP, StatPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));
  uint64_t size;
  time_t mtime;
  ASSERT_EQ(0, ioctx.stat("foo", &size, &mtime));
  ASSERT_EQ(sizeof(buf), size);
  ASSERT_EQ(-ENOENT, ioctx.stat("nonexistent", &size, &mtime));
}

TEST_F(LibRadosStatEC, StatNS) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  rados_ioctx_set_namespace(ioctx, "");
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_write(ioctx, "foo2", buf, sizeof(buf), 0));

  char buf2[64];
  memset(buf2, 0xcc, sizeof(buf2));
  rados_ioctx_set_namespace(ioctx, "nspace");
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf2, sizeof(buf2), 0));

  uint64_t size;
  time_t mtime;
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

TEST_F(LibRadosStatECPP, StatPPNS) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ioctx.set_namespace("");
  ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));
  ASSERT_EQ(0, ioctx.write("foo2", bl, sizeof(buf), 0));

  char buf2[64];
  memset(buf2, 0xbb, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  ioctx.set_namespace("nspace");
  ASSERT_EQ(0, ioctx.write("foo", bl2, sizeof(buf2), 0));

  uint64_t size;
  time_t mtime;
  ioctx.set_namespace("");
  ASSERT_EQ(0, ioctx.stat("foo", &size, &mtime));
  ASSERT_EQ(sizeof(buf), size);
  ASSERT_EQ(-ENOENT, ioctx.stat("nonexistent", &size, &mtime));

  ioctx.set_namespace("nspace");
  ASSERT_EQ(0, ioctx.stat("foo", &size, &mtime));
  ASSERT_EQ(sizeof(buf2), size);
  ASSERT_EQ(-ENOENT, ioctx.stat("nonexistent", &size, &mtime));
  ASSERT_EQ(-ENOENT, ioctx.stat("foo2", &size, &mtime));
}

TEST_F(LibRadosStatEC, ClusterStat) {
  struct rados_cluster_stat_t result;
  ASSERT_EQ(0, rados_cluster_stat(cluster, &result));
}

TEST_F(LibRadosStatECPP, ClusterStatPP) {
  cluster_stat_t cstat;
  ASSERT_EQ(0, cluster.cluster_stat(cstat));
}

TEST_F(LibRadosStatEC, PoolStat) {
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

TEST_F(LibRadosStatECPP, PoolStatPP) {
  std::string n = ioctx.get_pool_name();
  ASSERT_EQ(n, pool_name);
  char buf[128];
  memset(buf, 0xff, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  std::list<std::string> v;
  std::map<std::string,stats_map> stats;
  ASSERT_EQ(0, cluster.get_pool_stats(v, stats));
}
