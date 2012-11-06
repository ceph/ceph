#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "test/librados/test.h"

#include <algorithm>
#include <errno.h>
#include "gtest/gtest.h"

using namespace librados;

TEST(LibRadosStat, Stat) {
  char buf[128];
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  uint64_t size;
  time_t mtime;
  ASSERT_EQ(0, rados_stat(ioctx, "foo", &size, &mtime));
  ASSERT_EQ(sizeof(buf), size);
  ASSERT_EQ(-ENOENT, rados_stat(ioctx, "nonexistent", &size, &mtime));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosStat, StatPP) {
  char buf[128];
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), ioctx.write("foo", bl, sizeof(buf), 0));
  uint64_t size;
  time_t mtime;
  ASSERT_EQ(0, ioctx.stat("foo", &size, &mtime));
  ASSERT_EQ(sizeof(buf), size);
  ASSERT_EQ(-ENOENT, ioctx.stat("nonexistent", &size, &mtime));
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(LibRadosStat, ClusterStat) {
  rados_t cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  struct rados_cluster_stat_t result;
  ASSERT_EQ(0, rados_cluster_stat(cluster, &result));
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosStat, ClusterStatPP) {
  Rados cluster;
  cluster_stat_t cstat;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  ASSERT_EQ(0, cluster.cluster_stat(cstat));
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(LibRadosStat, PoolStat) {
  char buf[128];
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
  char actual_pool_name[80];
  unsigned l = rados_ioctx_get_pool_name(ioctx, actual_pool_name, sizeof(actual_pool_name));
  ASSERT_EQ(strlen(actual_pool_name), l);
  ASSERT_EQ(0, strcmp(actual_pool_name, pool_name.c_str()));
  memset(buf, 0xff, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  struct rados_pool_stat_t stats;
  memset(&stats, 0, sizeof(stats));
  ASSERT_EQ(0, rados_ioctx_pool_stat(ioctx, &stats));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosStat, PoolStatPP) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  std::string n = ioctx.get_pool_name();
  ASSERT_EQ(n, pool_name);
  char buf[128];
  memset(buf, 0xff, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), ioctx.write("foo", bl1, sizeof(buf), 0));
  std::list<std::string> v;
  std::map<std::string,stats_map> stats;
  ASSERT_EQ(0, cluster.get_pool_stats(v, stats));
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
