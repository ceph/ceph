#include "include/rados/librados.h"
#include "test/rados-api/test.h"

#include "gtest/gtest.h"

TEST(LibRadosIo, SimpleWrite) {
  char buf[128];
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ(0, create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(sizeof(buf), rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

