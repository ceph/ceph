#include "mds/mdstypes.h"
#include "include/buffer.h"
#include "include/rbd_types.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "test/librados/test.h"

#include "common/Cond.h"

#include "gtest/gtest.h"
#include <errno.h>
#include <map>
#include <sstream>
#include <string>

using namespace librados;
using ceph::buffer;
using std::map;
using std::ostringstream;
using std::string;

TEST(LibRadosCmd, MonDescribe) {
  rados_t cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));

  char *buf, *st;
  size_t buflen, stlen;

  ASSERT_EQ(0, rados_mon_command(cluster, "{\"prefix\":\"get_command_descriptions\"}", "", 0, &buf, &buflen, &st, &stlen));
  ASSERT_LT(0u, buflen);
  rados_buffer_free(buf);
  rados_buffer_free(st);

  ASSERT_EQ(-EINVAL, rados_mon_command(cluster, "get_command_descriptions", "", 0, &buf, &buflen, &st, &stlen));
  rados_buffer_free(buf);
  rados_buffer_free(st);

  ASSERT_EQ(-EINVAL, rados_mon_command(cluster, "asdfqwer", "{}", 2, &buf, &buflen, &st, &stlen));
  rados_buffer_free(buf);
  rados_buffer_free(st);

  ASSERT_EQ(0, rados_mon_command(cluster, "{\"prefix\":\"mon_status\"}", "", 0, &buf, &buflen, &st, &stlen));
  //ASSERT_LTE(0u, buflen);
  ASSERT_LT(0u, stlen);
  rados_buffer_free(buf);
  rados_buffer_free(st);

  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}
/*
TEST(LibRadosCmd, OSDCmd) {
  rados_t cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));

  char *buf, *st;
  size_t buflen, stlen;

  ASSERT_EQ(-22, rados_osd_command(cluster, 0, "asdfasdf", "", &buf, &buflen, &st, &stlen));
  ASSERT_EQ(-22, rados_osd_command(cluster, 0, "version", "", &buf, &buflen, &st, &stlen));
  ASSERT_EQ(0, rados_osd_command(cluster, 0, "{prefix:\"version\"}", "", &buf, &buflen, &st, &stlen));
  ASSERT_LT(0u, buflen);
  rados_buffer_free(buf);
  rados_buffer_free(st);

  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosCmd, PGCmd) {
  rados_t cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));

  char *buf, *st;
  size_t buflen, stlen;

  int64_t poolid = rados_pool_lookup(cluster, pool_name.c_str());
  ASSERT_LT(0, poolid);

  string pgid = stringify(poolid) + ".0";

  ASSERT_EQ(-22, rados_pg_command(cluster, pgid.c_str(), "asdfasdf", "", &buf, &buflen, &st, &stlen));
  ASSERT_EQ(0, rados_pg_command(cluster, pgid.c_str(), "query", "", &buf, &buflen, &st, &stlen));
  ASSERT_LT(0u, buflen);
  rados_buffer_free(buf);
  rados_buffer_free(st);

  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}
*/
struct Log {
  list<string> log;
  Cond cond;
  Mutex lock;

  Log() : lock("l::lock") {}

  bool contains(string str) {
    Mutex::Locker l(lock);
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
  Log *l = (Log *)arg;
  Mutex::Locker locker(l->lock);
  l->log.push_back(line);
  l->cond.Signal();
  cout << "got: " << line << std::endl;
}

TEST(LibRadosCmd, WatchLog) {
  rados_t cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));

  char *buf, *st;
  size_t buflen, stlen;
  Log l;

  ASSERT_EQ(0, rados_monitor_log(cluster, "info", log_cb, &l));
  ASSERT_EQ(0, rados_mon_command(cluster, "{\"prefix\":\"log\", \"logtext\":[\"onexx\"]}", "", 0, &buf, &buflen, &st, &stlen));
  for (int i=0; !l.contains("onexx"); i++) {
    ASSERT_TRUE(i<100);
    sleep(1);
  }
  ASSERT_TRUE(l.contains("onexx"));

  /*
    changing the subscribe level is currently broken.

  ASSERT_EQ(0, rados_monitor_log(cluster, "err", log_cb, &l));
  ASSERT_EQ(0, rados_mon_command(cluster, "{\"prefix\":\"log\", \"logtext\":[\"twoxx\"]}", "", 0, &buf, &buflen, &st, &stlen));
  sleep(2);
  ASSERT_FALSE(l.contains("twoxx"));
  */

  ASSERT_EQ(0, rados_monitor_log(cluster, "info", log_cb, &l));
  ASSERT_EQ(0, rados_mon_command(cluster, "{\"prefix\":\"log\", \"logtext\":[\"threexx\"]}", "", 0, &buf, &buflen, &st, &stlen));
  for (int i=0; !l.contains("threexx"); i++) {
    ASSERT_TRUE(i<100);
    sleep(1);
  }

  ASSERT_EQ(0, rados_monitor_log(cluster, "info", NULL, NULL));
  ASSERT_EQ(0, rados_mon_command(cluster, "{\"prefix\":\"log\", \"logtext\":[\"fourxx\"]}", "", 0, &buf, &buflen, &st, &stlen));
  sleep(2);
  ASSERT_FALSE(l.contains("fourxx"));


  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}
 
