#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"
#include "include/stringify.h"

#include <algorithm>
#include <errno.h>
#include "gtest/gtest.h"
#include "test/unit.cc"

using namespace librados;

TEST(LibRadosServicePP, RegisterEarly) {
  Rados cluster;
  cluster.init("admin");
  ASSERT_EQ(0, cluster.conf_read_file(NULL));
  cluster.conf_parse_env(NULL);
  string name = string("pid") + stringify(getpid());
  ASSERT_EQ(0, cluster.service_daemon_register(
	      "laundry", name, {{"foo", "bar"}, {"this", "that"}}));
  ASSERT_EQ(-EEXIST, cluster.service_daemon_register(
	      "laundry", name, {{"foo", "bar"}, {"this", "that"}}));
  ASSERT_EQ(0, cluster.connect());
  sleep(5);
  cluster.shutdown();
}

TEST(LibRadosServicePP, RegisterLate) {
  Rados cluster;
  cluster.init("admin");
  ASSERT_EQ(0, cluster.conf_read_file(NULL));
  cluster.conf_parse_env(NULL);
  ASSERT_EQ("", connect_cluster_pp(cluster));
  string name = string("pid") + stringify(getpid());
  ASSERT_EQ(0, cluster.service_daemon_register(
	      "laundry", name, {{"foo", "bar"}, {"this", "that"}}));
}

TEST(LibRadosServicePP, Status) {
  Rados cluster;
  cluster.init("admin");
  ASSERT_EQ(0, cluster.conf_read_file(NULL));
  cluster.conf_parse_env(NULL);
  string name = string("pid") + stringify(getpid());
  ASSERT_EQ(-ENOTCONN, cluster.service_daemon_update_status(
	      {{"testing", "starting"}}));
  ASSERT_EQ(0, cluster.connect());
  ASSERT_EQ(0, cluster.service_daemon_register(
	      "laundry", name, {{"foo", "bar"}, {"this", "that"}}));
  for (int i=0; i<20; ++i) {
    ASSERT_EQ(0, cluster.service_daemon_update_status({
	  {"testing", "running"},
	  {"count", stringify(i)}
	}));
    sleep(1);
  }
  cluster.shutdown();
}
