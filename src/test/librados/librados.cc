//#include "common/config.h"
#include "include/rados/librados.h"

#include "gtest/gtest.h"

TEST(Librados, CreateShutdown) {
  rados_t cluster;
  int err;
  err = rados_create(&cluster, "someid");
  EXPECT_EQ(err, 0);

  rados_shutdown(cluster);
}
