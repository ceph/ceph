#include <algorithm>
#include <thread>
#include <errno.h>
#include "gtest/gtest.h"

#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "common/config_proxy.h"
#include "test/librados/test_cxx.h"
#include "test/librados/testcase_cxx.h"
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

TEST(LibRadosServicePP, Close) {
  int tries = 20;
  string name = string("close-test-pid") + stringify(getpid());
  int i;
  for (i = 0; i < tries; ++i) {
    cout << "attempt " << i << " of " << tries << std::endl;
    {
      Rados cluster;
      cluster.init("admin");
      ASSERT_EQ(0, cluster.conf_read_file(NULL));
      cluster.conf_parse_env(NULL);
      ASSERT_EQ(0, cluster.connect());
      ASSERT_EQ(0, cluster.service_daemon_register(
		  "laundry", name, {{"foo", "bar"}, {"this", "that"}}));
      sleep(3); // let it register
      cluster.shutdown();
    }
    // mgr updates servicemap every tick
    //sleep(g_conf().get_val<int64_t>("mgr_tick_period"));
    std::this_thread::sleep_for(g_conf().get_val<std::chrono::seconds>(
				  "mgr_tick_period"));
    // make sure we are deregistered
    {
      Rados cluster;
      cluster.init("admin");
      ASSERT_EQ(0, cluster.conf_read_file(NULL));
      cluster.conf_parse_env(NULL);
      ASSERT_EQ(0, cluster.connect());
      bufferlist inbl, outbl;
      ASSERT_EQ(0, cluster.mon_command("{\"prefix\": \"service dump\"}",
				       inbl, &outbl, NULL));
      string s = outbl.to_str();
      cluster.shutdown();

      if (s.find(name) != string::npos) {
	cout << " failed to deregister:\n" << s << std::endl;
      } else {
	break;
      }
    }
  }
  ASSERT_LT(i, tries);
}
