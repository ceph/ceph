#include "include/rados/librados.h"
#include "test/rados-api/test.h"

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

