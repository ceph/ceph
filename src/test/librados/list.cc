#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "test/librados/test.h"

#include "gtest/gtest.h"
#include <errno.h>
#include <string>

using namespace librados;

TEST(LibRadosList, ListObjects) {
  char buf[128];
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  rados_list_ctx_t ctx;
  ASSERT_EQ(0, rados_objects_list_open(ioctx, &ctx));
  const char *entry;
  ASSERT_EQ(0, rados_objects_list_next(ctx, &entry, NULL));
  ASSERT_EQ(std::string(entry), "foo");
  ASSERT_EQ(-ENOENT, rados_objects_list_next(ctx, &entry, NULL));
  rados_objects_list_close(ctx);
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosList, ListObjectsPP) {
  std::string pool_name = get_temp_pool_name();
  Rados cluster;
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), ioctx.write("foo", bl1, sizeof(buf), 0));
  ObjectIterator iter(ioctx.objects_begin());
  ASSERT_EQ((iter == ioctx.objects_end()), false);
  ASSERT_EQ((*iter).first, "foo");
  ++iter;
  ASSERT_EQ(true, (iter == ioctx.objects_end()));
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
