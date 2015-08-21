#include "include/rados/librados.h"
#include "test/librados/test.h"

#include "gtest/gtest.h"
#include <errno.h>

TEST(LibradosSetPriority, SetClientPriority) {
  rados_t cluster;
  
  ASSERT_EQ(0, rados_create(&cluster, NULL));
  ASSERT_EQ(0, rados_conf_read_file(cluster, NULL));
  ASSERT_EQ(0, rados_conf_parse_env(cluster, NULL));
  
  ASSERT_EQ(-ENOENT, rados_set_client_priority(cluster, 64));

  ASSERT_EQ(0, rados_connect(cluster));
  
  ASSERT_EQ(0, rados_set_client_priority(cluster, 64));
  ASSERT_EQ(-EINVAL, rados_set_client_priority(cluster, -1));
  ASSERT_EQ(-EINVAL, rados_set_client_priority(cluster, 256));
  
  rados_shutdown(cluster);
}
