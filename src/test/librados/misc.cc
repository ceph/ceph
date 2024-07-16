// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "gtest/gtest.h"

#include "mds/mdstypes.h"
#include "include/err.h"
#include "include/buffer.h"
#include "include/rbd_types.h"
#include "include/rados.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/scope_guard.h"
#include "include/stringify.h"
#include "common/Checksummer.h"
#include "global/global_context.h"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"
#include "gtest/gtest.h"
#include <sys/time.h>
#ifndef _WIN32
#include <sys/resource.h>
#endif

#include <errno.h>
#include <map>
#include <sstream>
#include <string>
#include <regex>

using namespace std;
using namespace librados;

typedef RadosTest LibRadosMisc;

TEST(LibRadosMiscVersion, Version) {
  int major, minor, extra;
  rados_version(&major, &minor, &extra);
}

static void test_rados_log_cb(void *arg,
                              const char *line,
                              const char *who,
                              uint64_t sec, uint64_t nsec,
                              uint64_t seq, const char *level,
                              const char *msg)
{
    std::cerr << "monitor log callback invoked" << std::endl;
}

TEST(LibRadosMiscConnectFailure, ConnectFailure) {
  rados_t cluster;

  char *id = getenv("CEPH_CLIENT_ID");
  if (id)
    std::cerr << "Client id is: " << id << std::endl;

  ASSERT_EQ(0, rados_create(&cluster, NULL));
  ASSERT_EQ(0, rados_conf_read_file(cluster, NULL));
  ASSERT_EQ(0, rados_conf_parse_env(cluster, NULL));

  ASSERT_EQ(-ENOTCONN, rados_monitor_log(cluster, "error",
                                         test_rados_log_cb, NULL));

  ASSERT_EQ(0, rados_connect(cluster));
  rados_shutdown(cluster);

  ASSERT_EQ(0, rados_create(&cluster, NULL));
  ASSERT_EQ(-ENOENT, rados_connect(cluster));
  rados_shutdown(cluster);
}

TEST(LibRadosMiscConnectFailure, ConnectTimeout) {
  rados_t cluster;

  ASSERT_EQ(0, rados_create(&cluster, NULL));
  ASSERT_EQ(0, rados_conf_set(cluster, "mon_host", "255.0.1.2:3456"));
  ASSERT_EQ(0, rados_conf_set(cluster, "key",
                              "AQAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAA=="));
  ASSERT_EQ(0, rados_conf_set(cluster, "client_mount_timeout", "5s"));

  utime_t start = ceph_clock_now();
  ASSERT_EQ(-ETIMEDOUT, rados_connect(cluster));
  utime_t end = ceph_clock_now();

  utime_t dur = end - start;
  ASSERT_GE(dur, utime_t(5, 0));
  ASSERT_LT(dur, utime_t(15, 0));

  rados_shutdown(cluster);
}

TEST(LibRadosMiscPool, PoolCreationRace) {
  rados_t cluster_a, cluster_b;

  char *id = getenv("CEPH_CLIENT_ID");
  if (id)
    std::cerr << "Client id is: " << id << std::endl;

  ASSERT_EQ(0, rados_create(&cluster_a, NULL));
  ASSERT_EQ(0, rados_conf_read_file(cluster_a, NULL));
  // kludge: i want to --log-file foo and only get cluster b
  //ASSERT_EQ(0, rados_conf_parse_env(cluster_a, NULL));
  ASSERT_EQ(0, rados_conf_set(cluster_a,
			      "objecter_debug_inject_relock_delay", "true"));
  ASSERT_EQ(0, rados_connect(cluster_a));

  ASSERT_EQ(0, rados_create(&cluster_b, NULL));
  ASSERT_EQ(0, rados_conf_read_file(cluster_b, NULL));
  ASSERT_EQ(0, rados_conf_parse_env(cluster_b, NULL));
  ASSERT_EQ(0, rados_connect(cluster_b));

  char poolname[80];
  snprintf(poolname, sizeof(poolname), "poolrace.%d", rand());
  rados_pool_create(cluster_a, poolname);
  rados_ioctx_t a;
  rados_ioctx_create(cluster_a, poolname, &a);

  char pool2name[80];
  snprintf(pool2name, sizeof(pool2name), "poolrace2.%d", rand());
  rados_pool_create(cluster_b, pool2name);

  list<rados_completion_t> cls;
  // this should normally trigger pretty easily, but we need to bound
  // the requests because if we get too many we'll get stuck by always
  // sending enough messages that we hit the socket failure injection.
  int max = 512;
  while (max--) {
    char buf[100];
    rados_completion_t c;
    rados_aio_create_completion2(nullptr, nullptr, &c);
    cls.push_back(c);
    rados_aio_read(a, "PoolCreationRaceObj", c, buf, 100, 0);
    cout << "started " << (void*)c << std::endl;
    if (rados_aio_is_complete(cls.front())) {
      break;
    }
  }
  while (!rados_aio_is_complete(cls.front())) {
    cout << "waiting 1 sec" << std::endl;
    sleep(1);
  }

  cout << " started " << cls.size() << " aios" << std::endl;
  for (auto c : cls) {
    cout << "waiting " << (void*)c << std::endl;
    rados_aio_wait_for_complete_and_cb(c);
    rados_aio_release(c);
  }
  cout << "done." << std::endl;

  rados_ioctx_destroy(a);
  rados_pool_delete(cluster_a, poolname);
  rados_pool_delete(cluster_a, pool2name);
  rados_shutdown(cluster_b);
  rados_shutdown(cluster_a);
}

TEST_F(LibRadosMisc, ClusterFSID) {
  char fsid[37];
  ASSERT_EQ(-ERANGE, rados_cluster_fsid(cluster, fsid, sizeof(fsid) - 1));
  ASSERT_EQ(sizeof(fsid) - 1,
            (size_t)rados_cluster_fsid(cluster, fsid, sizeof(fsid)));
}

TEST_F(LibRadosMisc, Exec) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  char buf2[512];
  int res = rados_exec(ioctx, "foo", "rbd", "get_all_features",
			  NULL, 0, buf2, sizeof(buf2));
  ASSERT_GT(res, 0);
  bufferlist bl;
  bl.append(buf2, res);
  auto iter = bl.cbegin();
  uint64_t all_features;
  decode(all_features, iter);
  // make sure *some* features are specified; don't care which ones
  ASSERT_NE(all_features, (unsigned)0);
}

TEST_F(LibRadosMisc, WriteSame) {
  char buf[128];
  char full[128 * 4];
  char *cmp;

  /* zero the full range before using writesame */
  memset(full, 0, sizeof(full));
  ASSERT_EQ(0, rados_write(ioctx, "ws", full, sizeof(full), 0));

  memset(buf, 0xcc, sizeof(buf));
  /* write the same buf four times */
  ASSERT_EQ(0, rados_writesame(ioctx, "ws", buf, sizeof(buf), sizeof(full), 0));

  /* read back the full buffer and confirm that it matches */
  ASSERT_EQ((int)sizeof(full), rados_read(ioctx, "ws", full, sizeof(full), 0));

  for (cmp = full; cmp < full + sizeof(full); cmp += sizeof(buf)) {
    ASSERT_EQ(0, memcmp(cmp, buf, sizeof(buf)));
  }

  /* write_len not a multiple of data_len should throw error */
  ASSERT_EQ(-EINVAL, rados_writesame(ioctx, "ws", buf, sizeof(buf),
				     (sizeof(buf) * 4) - 1, 0));
  ASSERT_EQ(-EINVAL,
	    rados_writesame(ioctx, "ws", buf, sizeof(buf), sizeof(buf) / 2, 0));
  ASSERT_EQ(-EINVAL,
	    rados_writesame(ioctx, "ws", buf, 0, sizeof(buf), 0));
  /* write_len = data_len, i.e. same as rados_write() */
  ASSERT_EQ(0, rados_writesame(ioctx, "ws", buf, sizeof(buf), sizeof(buf), 0));
}

TEST_F(LibRadosMisc, CmpExt) {
  bufferlist cmp_bl, bad_cmp_bl, write_bl;
  char stored_str[] = "1234567891";
  char mismatch_str[] = "1234577777";

  ASSERT_EQ(0,
	    rados_write(ioctx, "cmpextpp", stored_str, sizeof(stored_str), 0));

  ASSERT_EQ(0,
	    rados_cmpext(ioctx, "cmpextpp", stored_str, sizeof(stored_str), 0));

  ASSERT_EQ(-MAX_ERRNO - 5,
	    rados_cmpext(ioctx, "cmpextpp", mismatch_str, sizeof(mismatch_str), 0));
}

TEST_F(LibRadosMisc, Applications) {
  const char *cmd[] = {"{\"prefix\":\"osd dump\"}", nullptr};
  char *buf, *st;
  size_t buflen, stlen;
  ASSERT_EQ(0, rados_mon_command(cluster, (const char **)cmd, 1, "", 0, &buf,
                                 &buflen, &st, &stlen));
  ASSERT_LT(0u, buflen);
  string result(buf);
  rados_buffer_free(buf);
  rados_buffer_free(st);
  if (!std::regex_search(result, std::regex("require_osd_release [l-z]"))) {
    std::cout << "SKIPPING";
    return;
  }

  char apps[128];
  size_t app_len;

  app_len = sizeof(apps);
  ASSERT_EQ(0, rados_application_list(ioctx, apps, &app_len));
  ASSERT_EQ(6U, app_len);
  ASSERT_EQ(0, memcmp("rados\0", apps, app_len));

  ASSERT_EQ(0, rados_application_enable(ioctx, "app1", 1));
  ASSERT_EQ(-EPERM, rados_application_enable(ioctx, "app2", 0));
  ASSERT_EQ(0, rados_application_enable(ioctx, "app2", 1));

  ASSERT_EQ(-ERANGE, rados_application_list(ioctx, apps, &app_len));
  ASSERT_EQ(16U, app_len);
  ASSERT_EQ(0, rados_application_list(ioctx, apps, &app_len));
  ASSERT_EQ(16U, app_len);
  ASSERT_EQ(0, memcmp("app1\0app2\0rados\0", apps, app_len));

  char keys[128];
  char vals[128];
  size_t key_len;
  size_t val_len;

  key_len = sizeof(keys);
  val_len = sizeof(vals);
  ASSERT_EQ(-ENOENT, rados_application_metadata_list(ioctx, "dne", keys,
                                                     &key_len, vals, &val_len));
  ASSERT_EQ(0, rados_application_metadata_list(ioctx, "app1", keys, &key_len,
                                               vals, &val_len));
  ASSERT_EQ(0U, key_len);
  ASSERT_EQ(0U, val_len);

  ASSERT_EQ(-ENOENT, rados_application_metadata_set(ioctx, "dne", "key",
                                                    "value"));
  ASSERT_EQ(0, rados_application_metadata_set(ioctx, "app1", "key1", "value1"));
  ASSERT_EQ(0, rados_application_metadata_set(ioctx, "app1", "key2", "value2"));

  ASSERT_EQ(-ERANGE, rados_application_metadata_list(ioctx, "app1", keys,
                                                     &key_len, vals, &val_len));
  ASSERT_EQ(10U, key_len);
  ASSERT_EQ(14U, val_len);
  ASSERT_EQ(0, rados_application_metadata_list(ioctx, "app1", keys, &key_len,
                                               vals, &val_len));
  ASSERT_EQ(10U, key_len);
  ASSERT_EQ(14U, val_len);
  ASSERT_EQ(0, memcmp("key1\0key2\0", keys, key_len));
  ASSERT_EQ(0, memcmp("value1\0value2\0", vals, val_len));

  ASSERT_EQ(0, rados_application_metadata_remove(ioctx, "app1", "key1"));
  ASSERT_EQ(0, rados_application_metadata_list(ioctx, "app1", keys, &key_len,
                                               vals, &val_len));
  ASSERT_EQ(5U, key_len);
  ASSERT_EQ(7U, val_len);
  ASSERT_EQ(0, memcmp("key2\0", keys, key_len));
  ASSERT_EQ(0, memcmp("value2\0", vals, val_len));
}

TEST_F(LibRadosMisc, MinCompatOSD) {
  int8_t require_osd_release;
  ASSERT_EQ(0, rados_get_min_compatible_osd(cluster, &require_osd_release));
  ASSERT_LE(-1, require_osd_release);
  ASSERT_GT(CEPH_RELEASE_MAX, require_osd_release);
}

TEST_F(LibRadosMisc, MinCompatClient) {
  int8_t min_compat_client;
  int8_t require_min_compat_client;
  ASSERT_EQ(0, rados_get_min_compatible_client(cluster,
                                               &min_compat_client,
                                               &require_min_compat_client));
  ASSERT_LE(-1, min_compat_client);
  ASSERT_GT(CEPH_RELEASE_MAX, min_compat_client);

  ASSERT_LE(-1, require_min_compat_client);
  ASSERT_GT(CEPH_RELEASE_MAX, require_min_compat_client);
}

static void shutdown_racer_func()
{
  const int niter = 32;
  rados_t rad;
  int i;

  for (i = 0; i < niter; ++i) {
    auto r = connect_cluster(&rad);
    if (getenv("ALLOW_TIMEOUTS")) {
      ASSERT_TRUE(r == "" || r == "rados_connect failed with error -110");
    } else {
      ASSERT_EQ("", r);
    }
    rados_shutdown(rad);
  }
}

#ifndef _WIN32
// See trackers #20988 and #42026
TEST_F(LibRadosMisc, ShutdownRace)
{
  const int nthreads = 128;
  std::thread threads[nthreads];

  // Need a bunch of fd's for this test
  struct rlimit rold, rnew;
  ASSERT_EQ(getrlimit(RLIMIT_NOFILE, &rold), 0);
  rnew = rold;
  rnew.rlim_cur = rnew.rlim_max;
  ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &rnew), 0);

  for (int i = 0; i < nthreads; ++i)
    threads[i] = std::thread(shutdown_racer_func);

  for (int i = 0; i < nthreads; ++i)
    threads[i].join();
  ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &rold), 0);
}
#endif /* _WIN32 */
