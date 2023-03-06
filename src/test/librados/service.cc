#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "common/config_proxy.h"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"
#ifndef _WIN32
#include <sys/resource.h>
#endif

#include <mutex>
#include <condition_variable>
#include <algorithm>
#include <thread>
#include <errno.h>
#include "gtest/gtest.h"
#include "test/unit.cc"

using namespace std;
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

static void status_format_func(const int i, std::mutex &lock,
                               std::condition_variable &cond,
                               int &threads_started, bool &stopped)
{
  rados_t cluster;
  char metadata_buf[4096];

  ASSERT_EQ(0, rados_create(&cluster, "admin"));
  ASSERT_EQ(0, rados_conf_read_file(cluster, NULL));
  ASSERT_EQ(0, rados_conf_parse_env(cluster, NULL));

  ASSERT_EQ(0, rados_connect(cluster));
  if (i == 0) {
    ASSERT_LT(0, sprintf(metadata_buf, "%s%c%s%c",
                         "foo", '\0', "bar", '\0'));
  } else if (i == 1) {
    ASSERT_LT(0, sprintf(metadata_buf, "%s%c%s%c",
                         "daemon_type", '\0', "portal", '\0'));
  } else if (i == 2) {
    ASSERT_LT(0, sprintf(metadata_buf, "%s%c%s%c",
                         "daemon_prefix", '\0', "gateway", '\0'));
  } else {
    string prefix = string("gw") + stringify(i % 4);
    string zone = string("z") + stringify(i % 3);
    ASSERT_LT(0, sprintf(metadata_buf, "%s%c%s%c%s%c%s%c%s%c%s%c%s%c%s%c",
                         "daemon_type", '\0', "portal", '\0',
                         "daemon_prefix", '\0', prefix.c_str(), '\0',
                         "hostname", '\0', prefix.c_str(), '\0',
                         "zone_id", '\0', zone.c_str(), '\0'));
  }
  string name = string("rbd/image") + stringify(i);
  ASSERT_EQ(0, rados_service_register(cluster, "foo", name.c_str(),
		                      metadata_buf));

  std::unique_lock<std::mutex> l(lock);
  threads_started++;
  cond.notify_all();
  cond.wait(l, [&stopped] {
    return stopped;
  });

  rados_shutdown(cluster);
}

TEST(LibRadosService, StatusFormat) {
  const int nthreads = 16;
  std::thread threads[nthreads];
  std::mutex lock;
  std::condition_variable cond;
  bool stopped = false;
  int threads_started = 0;

  // no rlimits on Windows
  #ifndef _WIN32
  // Need a bunch of fd's for this test
  struct rlimit rold, rnew;
  ASSERT_EQ(getrlimit(RLIMIT_NOFILE, &rold), 0);
  rnew = rold;
  rnew.rlim_cur = rnew.rlim_max;
  ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &rnew), 0);
  #endif

  for (int i = 0; i < nthreads; ++i)
    threads[i] = std::thread(status_format_func, i, std::ref(lock),
                             std::ref(cond), std::ref(threads_started),
                             std::ref(stopped));

  {
    std::unique_lock<std::mutex> l(lock);
    cond.wait(l, [&threads_started] {
      return nthreads == threads_started;
    });
  }

  int retry = 60; // mon thrashing may make this take a long time
  while (retry) {
    rados_t cluster;

    ASSERT_EQ(0, rados_create(&cluster, "admin"));
    ASSERT_EQ(0, rados_conf_read_file(cluster, NULL));
    ASSERT_EQ(0, rados_conf_parse_env(cluster, NULL));

    ASSERT_EQ(0, rados_connect(cluster));
    JSONFormatter cmd_f;
    cmd_f.open_object_section("command");
    cmd_f.dump_string("prefix", "status");
    cmd_f.close_section();
    std::ostringstream cmd_stream;
    cmd_f.flush(cmd_stream);
    const std::string serialized_cmd = cmd_stream.str();
    const char *cmd[2];
    cmd[1] = NULL;
    cmd[0] = serialized_cmd.c_str();
    char *outbuf = NULL;
    size_t outlen = 0;
    ASSERT_EQ(0, rados_mon_command(cluster, (const char **)cmd, 1, "", 0,
              &outbuf, &outlen, NULL, NULL));
    std::string out(outbuf, outlen);
    cout << out << std::endl;
    bool success = false;
    auto r1 = out.find("16 portals active (1 hosts, 3 zones)");
    if (std::string::npos != r1) {
      success = true;
    }
    rados_buffer_free(outbuf);
    rados_shutdown(cluster);

    if (success || !retry) {
      break;
    }

    // wait for 2 seconds to make sure all the
    // services have been successfully updated
    // to ceph mon, then retry it.
    sleep(2);
    retry--;
  }

  {
    std::scoped_lock<std::mutex> l(lock);
    stopped = true;
    cond.notify_all();
  }
  for (int i = 0; i < nthreads; ++i)
    threads[i].join();

  ASSERT_NE(0, retry);
  #ifndef _WIN32
  ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &rold), 0);
  #endif
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
