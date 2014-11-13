#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/atomic.h"
#include "include/utime.h"
#include "common/Thread.h"
#include "common/Clock.h"
#include "test/librados/test.h"

#include "gtest/gtest.h"
#include <semaphore.h>
#include <errno.h>
#include <map>
#include <sstream>
#include <iostream>
#include <string>

#include "test/librados/TestCase.h"


using namespace librados;
using ceph::buffer;
using std::map;
using std::ostringstream;
using std::string;

static sem_t sem;
static atomic_t stop_flag;

class WatchNotifyTestCtx : public WatchCtx
{
public:
    void notify(uint8_t opcode, uint64_t ver, bufferlist& bl)
    {
      sem_post(&sem);
    }
};

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

struct WatcherUnwatcher : public Thread {
  string pool;
  WatcherUnwatcher(string& _pool) : pool(_pool) {}

  void *entry() {
    Rados cluster;
    connect_cluster_pp(cluster);
    while (!stop_flag.read()) {
      IoCtx ioctx;
      cluster.ioctx_create(pool.c_str(), ioctx);

      uint64_t handle;
      WatchNotifyTestCtx watch_ctx;
      ioctx.watch("foo", 0, &handle, &watch_ctx);
      bufferlist bl;
      ioctx.unwatch("foo", handle);
      ioctx.close();
    }
    return NULL;
  }
};

typedef RadosTestParamPP WatchStress;

INSTANTIATE_TEST_CASE_P(WatchStressTests, WatchStress,
			::testing::Values("", "cache"));

TEST_P(WatchStress, Stress1) {
  ASSERT_EQ(0, sem_init(&sem, 0, 0));
  Rados ncluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, ncluster));
  IoCtx nioctx;
  ncluster.ioctx_create(pool_name.c_str(), nioctx);
  WatchNotifyTestCtx ctx;

  WatcherUnwatcher *thr = new WatcherUnwatcher(pool_name);
  thr->create();
  ASSERT_EQ(0, nioctx.create("foo", false));

  for (unsigned i = 0; i < 75; ++i) {
    std::cerr << "Iteration " << i << std::endl;
    uint64_t handle;
    Rados cluster;
    IoCtx ioctx;
    WatchNotifyTestCtx ctx;

    connect_cluster_pp(cluster);
    cluster.ioctx_create(pool_name.c_str(), ioctx);
    ASSERT_EQ(0, ioctx.watch("foo", 0, &handle, &ctx));

    bool do_blacklist = i % 2;
    if (do_blacklist) {
      cluster.test_blacklist_self(true);
      std::cerr << "blacklisted" << std::endl;
      sleep(1);
    }

    bufferlist bl2;
    ASSERT_EQ(0, nioctx.notify("foo", 0, bl2));

    if (do_blacklist) {
      sleep(1); // Give a change to see an incorrect notify
    } else {
      TestAlarm alarm;
      sem_wait(&sem);
    }

    if (do_blacklist) {
      cluster.test_blacklist_self(false);
    }

    ioctx.unwatch("foo", handle);
    ioctx.close();
  }
  stop_flag.set(1);
  thr->join();
  nioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, ncluster));
  sem_destroy(&sem);
}

#pragma GCC diagnostic pop
