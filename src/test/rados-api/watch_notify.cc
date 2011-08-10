#include "include/rados/librados.h"
#include "test/rados-api/test.h"

#include <errno.h>
#include <semaphore.h>
#include "gtest/gtest.h"

static sem_t sem;

static void watch_notify_test_cb(uint8_t opcode, uint64_t ver, void *arg)
{
  sem_post(&sem);
}

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

