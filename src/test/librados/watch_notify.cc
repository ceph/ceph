#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/rados/rados_types.h"
#include "test/librados/test.h"

#include <errno.h>
#include <semaphore.h>
#include "gtest/gtest.h"

using namespace librados;

static sem_t sem;

static void watch_notify_test_cb(uint8_t opcode, uint64_t ver, void *arg)
{
  sem_post(&sem);
}

class WatchNotifyTestCtx : public WatchCtx
{
public:
    void notify(uint8_t opcode, uint64_t ver, bufferlist& bl)
    {
      sem_post(&sem);
    }
};

TEST(LibRadosWatchNotify, WatchNotifyTest) {
  ASSERT_EQ(0, sem_init(&sem, 0, 0));
  char buf[128];
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  uint64_t handle;
  ASSERT_EQ(0,
      rados_watch(ioctx, "foo", 0, &handle, watch_notify_test_cb, NULL));
  ASSERT_EQ(0, rados_notify(ioctx, "foo", 0, NULL, 0));
  TestAlarm alarm;
  sem_wait(&sem);
  rados_unwatch(ioctx, "foo", handle);
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
  sem_destroy(&sem);
}

TEST(LibRadosWatchNotify, WatchNotifyTestPP) {
  ASSERT_EQ(0, sem_init(&sem, 0, 0));
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), ioctx.write("foo", bl1, sizeof(buf), 0));
  uint64_t handle;
  WatchNotifyTestCtx ctx;
  ASSERT_EQ(0, ioctx.watch("foo", 0, &handle, &ctx));
  std::list<obj_watch_t> watches;
  ASSERT_EQ(0, ioctx.list_watchers("foo", &watches));
  ASSERT_EQ(watches.size(), 1u);
  bufferlist bl2;
  ASSERT_EQ(0, ioctx.notify("foo", 0, bl2));
  TestAlarm alarm;
  sem_wait(&sem);
  ioctx.unwatch("foo", handle);
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
  sem_destroy(&sem);
}

TEST(LibRadosWatchNotify, WatchNotifyTimeoutTestPP) {
  ASSERT_EQ(0, sem_init(&sem, 0, 0));
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  ioctx.set_notify_timeout(1);
  uint64_t handle;
  WatchNotifyTestCtx ctx;
  ASSERT_EQ(0, ioctx.watch("foo", 0, &handle, &ctx));
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
  sem_destroy(&sem);
}
