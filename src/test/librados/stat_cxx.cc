#include "gtest/gtest.h"

#include "include/rados/librados.hpp"

#include "test/librados/test_cxx.h"
#include "test/librados/testcase_cxx.h"
#include "crimson_utils.h"

using namespace librados;

typedef RadosTestPP LibRadosStatPP;
typedef RadosTestECPP LibRadosStatECPP;

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

  /* XXX time comparison asserts could spuriously fail */

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

  ASSERT_EQ(-ENOENT, ioctx.stat2("nonexistent", &size, &ts2));
}

TEST_F(LibRadosStatPP, ClusterStatPP) {
  cluster_stat_t cstat;
  ASSERT_EQ(0, cluster.cluster_stat(cstat));
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

TEST_F(LibRadosStatECPP, StatPP) {
  SKIP_IF_CRIMSON();
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

TEST_F(LibRadosStatECPP, ClusterStatPP) {
  SKIP_IF_CRIMSON();
  cluster_stat_t cstat;
  ASSERT_EQ(0, cluster.cluster_stat(cstat));
}

TEST_F(LibRadosStatECPP, PoolStatPP) {
  SKIP_IF_CRIMSON();
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

TEST_F(LibRadosStatECPP, StatPPNS) {
  SKIP_IF_CRIMSON();
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
