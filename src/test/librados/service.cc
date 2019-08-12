#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "common/config_proxy.h"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"

#include <algorithm>
#include <thread>
#include <errno.h>
#include "gtest/gtest.h"
#include "test/unit.cc"

using namespace librados;

TEST(LibRadosService, RegisterEarly) {
  rados_t cluster;
  ASSERT_EQ(0, rados_create(&cluster, "admin"));
  ASSERT_EQ(0, rados_conf_read_file(cluster, NULL));
  ASSERT_EQ(0, rados_conf_parse_env(cluster, NULL));

  string name = string("pid") + stringify(getpid());
  ASSERT_EQ(0, rados_service_register(cluster, "laundry", name.c_str(),
                                      "foo\0bar\0this\0that\0"));
  ASSERT_EQ(-EEXIST, rados_service_register(cluster, "laundry", name.c_str(),
                                            "foo\0bar\0this\0that\0"));

  ASSERT_EQ(0, rados_connect(cluster));
  sleep(5);
  rados_shutdown(cluster);
}

TEST(LibRadosService, RegisterLate) {
  rados_t cluster;
  ASSERT_EQ(0, rados_create(&cluster, "admin"));
  ASSERT_EQ(0, rados_conf_read_file(cluster, NULL));
  ASSERT_EQ(0, rados_conf_parse_env(cluster, NULL));
  ASSERT_EQ(0, rados_connect(cluster));

  string name = string("pid") + stringify(getpid());
  ASSERT_EQ(0, rados_service_register(cluster, "laundry", name.c_str(),
                                      "foo\0bar\0this\0that\0"));
  ASSERT_EQ(-EEXIST, rados_service_register(cluster, "laundry", name.c_str(),
                                            "foo\0bar\0this\0that\0"));
  rados_shutdown(cluster);
}

TEST(LibRadosService, Status) {
  rados_t cluster;
  ASSERT_EQ(0, rados_create(&cluster, "admin"));
  ASSERT_EQ(0, rados_conf_read_file(cluster, NULL));
  ASSERT_EQ(0, rados_conf_parse_env(cluster, NULL));

  ASSERT_EQ(-ENOTCONN, rados_service_update_status(cluster,
                                                   "testing\0testing\0"));

  ASSERT_EQ(0, rados_connect(cluster));
  string name = string("pid") + stringify(getpid());
  ASSERT_EQ(0, rados_service_register(cluster, "laundry", name.c_str(),
                                      "foo\0bar\0this\0that\0"));

  for (int i=0; i<20; ++i) {
    char buffer[1024];
    snprintf(buffer, sizeof(buffer), "%s%c%s%c%s%c%d%c",
             "testing", '\0', "testing", '\0',
             "count", '\0', i, '\0');
    ASSERT_EQ(0, rados_service_update_status(cluster, buffer));
    sleep(1);
  }
  rados_shutdown(cluster);
}
