#include "include/rados/librados.h"
#include "test/rados-api/test.h"

#include <errno.h>
#include "gtest/gtest.h"

/* cluster info */
//int rados_cluster_stat(rados_t cluster, struct rados_cluster_stat_t *result);
//
//int rados_ioctx_pool_stat(rados_ioctx_t io, struct rados_pool_stat_t *stats);

TEST(LibRadosIo, Stat) {
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
