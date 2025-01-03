// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "test/librados/test.h"

#include "gtest/gtest.h"
#include <errno.h>
#include <condition_variable>
#include <map>
#include <sstream>
#include <string>

using std::cout;
using std::list;
using std::map;
using std::ostringstream;
using std::string;

TEST(LibRadosCmd, MonDescribe) {
  rados_t cluster;
  ASSERT_EQ("", connect_cluster(&cluster));

  char *buf, *st;
  size_t buflen, stlen;
  char *cmd[2];

  cmd[1] = NULL;

  cmd[0] = (char *)"{\"prefix\":\"get_command_descriptions\"}";
  ASSERT_EQ(0, rados_mon_command(cluster, (const char **)cmd, 1, "", 0, &buf, &buflen, &st, &stlen));
  ASSERT_LT(0u, buflen);
  rados_buffer_free(buf);
  rados_buffer_free(st);

  cmd[0] = (char *)"get_command_descriptions";
  ASSERT_EQ(-EINVAL, rados_mon_command(cluster, (const char **)cmd, 1, "", 0, &buf, &buflen, &st, &stlen));
  rados_buffer_free(buf);
  rados_buffer_free(st);

  cmd[0] = (char *)"asdfqwer";
  ASSERT_EQ(-EINVAL, rados_mon_command(cluster, (const char **)cmd, 1, "{}", 2, &buf, &buflen, &st, &stlen));
  rados_buffer_free(buf);
  rados_buffer_free(st);

  cmd[0] = (char *)"";
  ASSERT_EQ(-EINVAL, rados_mon_command(cluster, (const char **)cmd, 1, "{}", 2, &buf, &buflen, &st, &stlen));
  rados_buffer_free(buf);
  rados_buffer_free(st);

  cmd[0] = (char *)"{}";
  ASSERT_EQ(-EINVAL, rados_mon_command(cluster, (const char **)cmd, 1, "", 0, &buf, &buflen, &st, &stlen));
  rados_buffer_free(buf);
  rados_buffer_free(st);

  cmd[0] = (char *)"{\"abc\":\"something\"}";
  ASSERT_EQ(-EINVAL, rados_mon_command(cluster, (const char **)cmd, 1, "", 0, &buf, &buflen, &st, &stlen));
  rados_buffer_free(buf);
  rados_buffer_free(st);

  cmd[0] = (char *)"{\"prefix\":\"\"}";
  ASSERT_EQ(-EINVAL, rados_mon_command(cluster, (const char **)cmd, 1, "", 0, &buf, &buflen, &st, &stlen));
  rados_buffer_free(buf);
  rados_buffer_free(st);

  cmd[0] = (char *)"{\"prefix\":\"    \"}";
  ASSERT_EQ(-EINVAL, rados_mon_command(cluster, (const char **)cmd, 1, "", 0, &buf, &buflen, &st, &stlen));
  rados_buffer_free(buf);
  rados_buffer_free(st);

  cmd[0] = (char *)"{\"prefix\":\";;;,,,;;,,\"}";
  ASSERT_EQ(-EINVAL, rados_mon_command(cluster, (const char **)cmd, 1, "", 0, &buf, &buflen, &st, &stlen));
  rados_buffer_free(buf);
  rados_buffer_free(st);

  cmd[0] = (char *)"{\"prefix\":\"extra command\"}";
  ASSERT_EQ(-EINVAL, rados_mon_command(cluster, (const char **)cmd, 1, "", 0, &buf, &buflen, &st, &stlen));
  rados_buffer_free(buf);
  rados_buffer_free(st);

  cmd[0] = (char *)"{\"prefix\":\"quorum_status\"}";
  ASSERT_EQ(0, rados_mon_command(cluster, (const char **)cmd, 1, "", 0, &buf, &buflen, &st, &stlen));
  ASSERT_LT(0u, buflen);
  //ASSERT_LT(0u, stlen);
  rados_buffer_free(buf);
  rados_buffer_free(st);
  rados_shutdown(cluster);
}

TEST(LibRadosCmd, OSDCmd) {
  rados_t cluster;
  ASSERT_EQ("", connect_cluster(&cluster));
  int r;
  char *buf, *st;
  size_t buflen, stlen;
  char *cmd[2];
  cmd[1] = NULL;

  // note: tolerate NXIO here in case the cluster is thrashing out underneath us.
  cmd[0] = (char *)"asdfasdf";
  r = rados_osd_command(cluster, 0, (const char **)cmd, 1, "", 0, &buf, &buflen, &st, &stlen);
  rados_buffer_free(buf);
  rados_buffer_free(st);
  ASSERT_TRUE(r == -22 || r == -ENXIO);
  cmd[0] = (char *)"version";
  r = rados_osd_command(cluster, 0, (const char **)cmd, 1, "", 0, &buf, &buflen, &st, &stlen);
  rados_buffer_free(buf);
  rados_buffer_free(st);
  ASSERT_TRUE(r == -22 || r == -ENXIO);
  cmd[0] = (char *)"{\"prefix\":\"version\"}";
  r = rados_osd_command(cluster, 0, (const char **)cmd, 1, "", 0, &buf, &buflen, &st, &stlen);
  ASSERT_TRUE((r == 0 && buflen > 0) || (r == -ENXIO && buflen == 0));
  rados_buffer_free(buf);
  rados_buffer_free(st);
  rados_shutdown(cluster);
}

TEST(LibRadosCmd, PGCmd) {
  rados_t cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));

  char *buf, *st;
  size_t buflen, stlen;
  char *cmd[2];
  cmd[1] = NULL;

  int64_t poolid = rados_pool_lookup(cluster, pool_name.c_str());
  ASSERT_LT(0, poolid);

  string pgid = stringify(poolid) + ".0";

  cmd[0] = (char *)"asdfasdf";
  // note: tolerate NXIO here in case the cluster is thrashing out underneath us.
  int r = rados_pg_command(cluster, pgid.c_str(), (const char **)cmd, 1, "", 0, &buf, &buflen, &st, &stlen);
  ASSERT_TRUE(r == -22 || r == -ENXIO);
  rados_buffer_free(buf);
  rados_buffer_free(st);

  // make sure the pg exists on the osd before we query it
  rados_ioctx_t io;
  rados_ioctx_create(cluster, pool_name.c_str(), &io);
  for (int i=0; i<100; i++) {
    string oid = "obj" + stringify(i);
    ASSERT_EQ(-ENOENT, rados_stat(io, oid.c_str(), NULL, NULL));
  }
  rados_ioctx_destroy(io);

  string qstr = "{\"prefix\":\"pg\", \"cmd\":\"query\", \"pgid\":\"" +  pgid + "\"}";
  cmd[0] = (char *)qstr.c_str();
  // note: tolerate ENOENT/ENXIO here if hte osd is thrashing out underneath us
  r = rados_pg_command(cluster, pgid.c_str(), (const char **)cmd, 1, "", 0,  &buf, &buflen, &st, &stlen);
  ASSERT_TRUE(r == 0 || r == -ENOENT || r == -ENXIO);

  ASSERT_LT(0u, buflen);
  rados_buffer_free(buf);
  rados_buffer_free(st);

  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

struct Log {
  list<string> log;
  std::condition_variable cond;
  std::mutex lock;

  bool contains(const string& str) {
    std::lock_guard<std::mutex> l(lock);
    for (list<string>::iterator p = log.begin(); p != log.end(); ++p) {
      if (p->find(str) != std::string::npos)
	return true;
    }
    return false;
  }
};

void log_cb(void *arg,
	     const char *line,
	     const char *who, uint64_t stampsec, uint64_t stamp_nsec,
	     uint64_t seq, const char *level,
	     const char *msg) {
  Log *l = static_cast<Log *>(arg);
  std::lock_guard<std::mutex> locker(l->lock);
  l->log.push_back(line);
  l->cond.notify_all();
  cout << "got: " << line << std::endl;
}

TEST(LibRadosCmd, WatchLog) {
  rados_t cluster;
  ASSERT_EQ("", connect_cluster(&cluster));
  char *buf, *st;
  char *cmd[2];
  cmd[1] = NULL;
  size_t buflen, stlen;
  Log l;

  ASSERT_EQ(0, rados_monitor_log(cluster, "info", log_cb, &l));
  cmd[0] = (char *)"{\"prefix\":\"log\", \"logtext\":[\"onexx\"]}";
  ASSERT_EQ(0, rados_mon_command(cluster, (const char **)cmd, 1, "", 0, &buf, &buflen, &st, &stlen));
  for (int i=0; !l.contains("onexx"); i++) {
    ASSERT_TRUE(i<100);
    sleep(1);
  }
  ASSERT_TRUE(l.contains("onexx"));

  cmd[0] = (char *)"{\"prefix\":\"log\", \"logtext\":[\"twoxx\"]}";
  ASSERT_EQ(0, rados_monitor_log(cluster, "err", log_cb, &l));
  ASSERT_EQ(0, rados_mon_command(cluster, (const char **)cmd, 1, "", 0, &buf, &buflen, &st, &stlen));
  sleep(2);
  ASSERT_FALSE(l.contains("twoxx"));

  ASSERT_EQ(0, rados_monitor_log(cluster, "info", log_cb, &l));
  cmd[0] = (char *)"{\"prefix\":\"log\", \"logtext\":[\"threexx\"]}";
  ASSERT_EQ(0, rados_mon_command(cluster, (const char **)cmd, 1, "", 0, &buf, &buflen, &st, &stlen));
  for (int i=0; !l.contains("threexx"); i++) {
    ASSERT_TRUE(i<100);
    sleep(1);
  }

  ASSERT_EQ(0, rados_monitor_log(cluster, "info", NULL, NULL));
  cmd[0] = (char *)"{\"prefix\":\"log\", \"logtext\":[\"fourxx\"]}";
  ASSERT_EQ(0, rados_mon_command(cluster, (const char **)cmd, 1, "", 0, &buf, &buflen, &st, &stlen));
  sleep(2);
  ASSERT_FALSE(l.contains("fourxx"));
  rados_shutdown(cluster);
}
