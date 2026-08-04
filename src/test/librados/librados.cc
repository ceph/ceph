//#include "common/config.h"
#include "include/rados/librados.h"

#include <chrono>
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

static double time_connect(rados_t cluster, int *err)
{
  auto start = std::chrono::steady_clock::now();
  *err = rados_connect(cluster);
  std::chrono::duration<double> elapsed =
    std::chrono::steady_clock::now() - start;
  return elapsed.count();
}

TEST(Librados, ConnectDoesNotRetryByDefault) {
  rados_t cluster = make_unconnectable_cluster();
  // long enough that a single retry could not be mistaken for none
  ASSERT_EQ(0, rados_conf_set(cluster, "rados_connect_retry_interval", "10"));

  int err = 0;
  double elapsed = time_connect(cluster, &err);
  ASSERT_EQ(-ENOENT, err);
  ASSERT_LT(elapsed, 5.0);

  rados_shutdown(cluster);
}

TEST(Librados, ConnectRetriesAreBounded) {
  rados_t cluster = make_unconnectable_cluster();
  ASSERT_EQ(0, rados_conf_set(cluster, "rados_connect_retries", "3"));
  ASSERT_EQ(0, rados_conf_set(cluster, "rados_connect_retry_interval", "0.05"));

  int err = 0;
  double elapsed = time_connect(cluster, &err);
  // three retries with a linear backoff, so at least 1+2+3 intervals, and the
  // original error once the budget is spent rather than an endless retry
  ASSERT_EQ(-ENOENT, err);
  ASSERT_GE(elapsed, 6 * 0.05);

  rados_shutdown(cluster);
}

TEST(Librados, ConnectFailureLeavesHandleUsable) {
  rados_t cluster = make_unconnectable_cluster();

  ASSERT_EQ(-ENOENT, rados_connect(cluster));
  // the failed attempt must not leave the handle stuck in CONNECTING, which
  // would turn every later attempt into -EINPROGRESS
  ASSERT_EQ(-ENOENT, rados_connect(cluster));

  rados_shutdown(cluster);
}
