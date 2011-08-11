#include "include/rados/librados.h"
#include "test/rados-api/test.h"

#include <errno.h>
#include "gtest/gtest.h"

TEST(LibRadosMisc, Version) {
  int major, minor, extra;
  rados_version(&major, &minor, &extra);
}

//TEST(LibRadosMisc, Exec) {
//  char buf[128];
//  char buf2[sizeof(buf)];
//  char buf3[sizeof(buf)];
//  rados_t cluster;
//  rados_ioctx_t ioctx;
//  std::string pool_name = get_temp_pool_name();
//  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
//  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
//  memset(buf, 0xcc, sizeof(buf));
//  ASSERT_EQ((int)sizeof(buf), rados_write(ioctx, "foo", buf, sizeof(buf), 0));
//  strncpy(buf2, "abracadabra", sizeof(buf2));
//  memset(buf3, 0, sizeof(buf3));
//  ASSERT_EQ(0, rados_exec(ioctx, "foo", "crypto", "md5",
//	  buf2, strlen(buf2) + 1, buf3, sizeof(buf3)));
//  ASSERT_EQ(string("06fce6115b1efc638e0cc2026f69ec43"), string(buf3));
//  rados_ioctx_destroy(ioctx);
//  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
//}

//int rados_tmap_update(rados_ioctx_t io, const char *o, const char *cmdbuf, size_t cmdbuflen);

//TEST(LibRadosMisc, CloneRange) {
//  char buf[128];
//  rados_t cluster;
//  rados_ioctx_t ioctx;
//  std::string pool_name = get_temp_pool_name();
//  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
//  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
//  memset(buf, 0xcc, sizeof(buf));
//  ASSERT_EQ((int)sizeof(buf), rados_write(ioctx, "src", buf, sizeof(buf), 0));
//  rados_ioctx_locator_set_key(ioctx, "src");
//  ASSERT_EQ(0, rados_clone_range(ioctx, "dst", 0, "src", 0, sizeof(buf)));
//  rados_ioctx_locator_set_key(ioctx, NULL);
//  char buf2[sizeof(buf)];
//  memset(buf2, 0, sizeof(buf2));
//  ASSERT_EQ((int)sizeof(buf2), rados_read(ioctx, "dst", buf2, sizeof(buf2), 0));
//  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
//  rados_ioctx_destroy(ioctx);
//  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
//}
