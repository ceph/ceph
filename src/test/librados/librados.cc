//#include "common/config.h"
#include "include/rados/librados.h"

#include <errno.h>

#include "gtest/gtest.h"

TEST(Librados, CreateShutdown) {
  rados_t cluster;
  int err;
  err = rados_create(&cluster, "someid");
  EXPECT_EQ(err, 0);

  rados_shutdown(cluster);
}

// A handle pointed at a monmap file that does not exist fails in the
// monmap/config bootstrap, the first of the two mon-contact steps of
// connect(), locally and without touching the network.
static rados_t make_unconnectable_cluster()
{
  rados_t cluster = nullptr;
  EXPECT_EQ(0, rados_create(&cluster, "someid"));
  EXPECT_EQ(0, rados_conf_set(cluster, "mon_host", ""));
  EXPECT_EQ(0, rados_conf_set(cluster, "monmap", "/nonexistent/monmap.bin"));
  return cluster;
}

TEST(Librados, ConnectFailureLeavesHandleUsable) {
  rados_t cluster = make_unconnectable_cluster();

  ASSERT_EQ(-ENOENT, rados_connect(cluster));
  // the failed attempt must not leave the handle stuck in CONNECTING, which
  // would turn every later attempt into -EINPROGRESS
  ASSERT_EQ(-ENOENT, rados_connect(cluster));

  rados_shutdown(cluster);
}
