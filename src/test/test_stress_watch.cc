#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/utime.h"
#include "common/Thread.h"
#include "common/Clock.h"
#include "test/librados/test_cxx.h"

#include "gtest/gtest.h"
#include <semaphore.h>
#include <errno.h>
#include <map>
#include <sstream>
#include <iostream>
#include <string>
#include <atomic>

#include "test/librados/testcase_cxx.h"


using namespace librados;
using std::map;
using std::ostringstream;
using std::string;

static sem_t *sem;
static std::atomic<bool> stop_flag = { false };

class WatchNotifyTestCtx : public WatchCtx
{
public:
    void notify(uint8_t opcode, uint64_t ver, bufferlist& bl) override
    {
      sem_post(sem);
    }
};

#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

struct WatcherUnwatcher : public Thread {
  string pool;
  explicit WatcherUnwatcher(string& _pool) : pool(_pool) {}

  void *entry() override {
    Rados cluster;
    connect_cluster_pp(cluster);
    while (!stop_flag) {
      IoCtx ioctx;
      cluster.ioctx_create(pool.c_str(), ioctx);

      uint64_t handle;
      WatchNotifyTestCtx watch_ctx;
      int r = ioctx.watch("foo", 0, &handle, &watch_ctx);
      if (r == 0)
        ioctx.unwatch("foo", handle);
      ioctx.close();
    }
    return NULL;
  }
};

typedef RadosTestParamPP WatchStress;

INSTANTIATE_TEST_SUITE_P(WatchStressTests, WatchStress,
			::testing::Values("", "cache"));

TEST_P(WatchStress, Stress1) {
  ASSERT_NE(SEM_FAILED, (sem = sem_open("test_stress_watch", O_CREAT, 0644, 0)));
  Rados ncluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, ncluster));
  IoCtx nioctx;
  ncluster.ioctx_create(pool_name.c_str(), nioctx);

  WatcherUnwatcher *thr = new WatcherUnwatcher(pool_name);
  thr->create("watcher_unwatch");
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
      sem_wait(sem);
    }

    if (do_blacklist) {
      cluster.test_blacklist_self(false);
    }

    ioctx.unwatch("foo", handle);
    ioctx.close();
  }
  stop_flag = true;
  thr->join();
  nioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, ncluster));
  sem_close(sem);
}

#pragma GCC diagnostic pop
#pragma GCC diagnostic warning "-Wpragmas"
