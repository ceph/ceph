#include "include/rados/librados.h"
#include "test/librados/test.h"

#include "gtest/gtest.h"
#include <errno.h>
#include <vector>

#define POOL_LIST_BUF_SZ 32768

TEST(LibRadosPools, PoolList) {
  std::vector<char> pool_list_buf(POOL_LIST_BUF_SZ, '\0');
  char *buf = &pool_list_buf[0];
  rados_t cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  ASSERT_LT(rados_pool_list(cluster, buf, POOL_LIST_BUF_SZ), POOL_LIST_BUF_SZ);

  bool found_pool = false;
  while (buf[0] != '\0') {
    if ((found_pool == false) && (strcmp(buf, pool_name.c_str()) == 0)) {
      found_pool = true;
    }
    buf += strlen(buf) + 1;
  }
  ASSERT_EQ(found_pool, true);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

int64_t rados_pool_lookup(rados_t cluster, const char *pool_name);

TEST(LibRadosPools, PoolLookup) {
  rados_t cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  ASSERT_LT(0, rados_pool_lookup(cluster, pool_name.c_str()));
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosPools, PoolLookup2) {
  rados_t cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  int64_t pool_id = rados_pool_lookup(cluster, pool_name.c_str());
  ASSERT_GT(pool_id, 0);
  rados_ioctx_t ioctx;
  ASSERT_EQ(0, rados_ioctx_create(cluster, pool_name.c_str(), &ioctx));
  int64_t pool_id2 = rados_ioctx_get_id(ioctx);
  ASSERT_EQ(pool_id, pool_id2);
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosPools, PoolDelete) {
  rados_t cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  ASSERT_EQ(0, rados_pool_delete(cluster, pool_name.c_str()));
  ASSERT_GT(0, rados_pool_lookup(cluster, pool_name.c_str()));
  ASSERT_EQ(0, rados_pool_create(cluster, pool_name.c_str()));
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosPools, PoolCreateDelete) {
  rados_t cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));

  std::string n = pool_name + "abc123";
  ASSERT_EQ(0, rados_pool_create(cluster, n.c_str()));
  ASSERT_EQ(-EEXIST, rados_pool_create(cluster, n.c_str()));
  ASSERT_EQ(0, rados_pool_delete(cluster, n.c_str()));
  ASSERT_EQ(-ENOENT, rados_pool_delete(cluster, n.c_str()));

  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosPools, AuidTest1) {
  rados_t cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_t ioctx;
  ASSERT_EQ(0, rados_ioctx_create(cluster, pool_name.c_str(), &ioctx));
  ASSERT_EQ(0, rados_ioctx_pool_set_auid(ioctx, 123));
  uint64_t auid;
  ASSERT_EQ(0, rados_ioctx_pool_get_auid(ioctx, &auid));
  ASSERT_EQ(123ull, auid);
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosPools, AuidTest2) {
  rados_t cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  ASSERT_EQ(0, rados_pool_delete(cluster, pool_name.c_str()));
  ASSERT_EQ(0, rados_pool_create_with_auid(cluster, pool_name.c_str(), 456));
  rados_ioctx_t ioctx;
  ASSERT_EQ(0, rados_ioctx_create(cluster, pool_name.c_str(), &ioctx));
  uint64_t auid;
  ASSERT_EQ(0, rados_ioctx_pool_get_auid(ioctx, &auid));
  ASSERT_EQ(456ull, auid);
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosPools, PoolCreateWithCrushRule) {
  rados_t cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));

  std::string pool2_name = get_temp_pool_name();
  ASSERT_EQ(0, rados_pool_create_with_crush_rule(cluster,
			    pool2_name.c_str(), 0));
  ASSERT_EQ(0, rados_pool_delete(cluster, pool2_name.c_str()));

  std::string pool3_name = get_temp_pool_name();
  ASSERT_EQ(0, rados_pool_create_with_all(cluster, pool3_name.c_str(),
					  456ull, 0));
  rados_ioctx_t ioctx;
  ASSERT_EQ(0, rados_ioctx_create(cluster, pool3_name.c_str(), &ioctx));
  uint64_t auid;
  ASSERT_EQ(0, rados_ioctx_pool_get_auid(ioctx, &auid));
  ASSERT_EQ(456ull, auid);
  ASSERT_EQ(0, rados_pool_delete(cluster, pool3_name.c_str()));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}
