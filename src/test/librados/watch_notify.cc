#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/rados/rados_types.h"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"

#include <errno.h>
#include <semaphore.h>
#include "gtest/gtest.h"
#include "include/encoding.h"

using namespace librados;

typedef RadosTest LibRadosWatchNotify;
typedef RadosTestParamPP LibRadosWatchNotifyPP;
typedef RadosTestEC LibRadosWatchNotifyEC;
typedef RadosTestECPP LibRadosWatchNotifyECPP;

int notify_sleep = 0;

// notify
static sem_t sem;

static void watch_notify_test_cb(uint8_t opcode, uint64_t ver, void *arg)
{
  std::cout << __func__ << std::endl;
  sem_post(&sem);
}

class WatchNotifyTestCtx : public WatchCtx
{
public:
    void notify(uint8_t opcode, uint64_t ver, bufferlist& bl)
    {
      std::cout << __func__ << std::endl;
      sem_post(&sem);
    }
};

// notify 2
bufferlist notify_bl;
rados_ioctx_t notify_io;
const char *notify_oid = 0;

static void watch_notify2_test_cb(void *arg,
				  uint64_t notify_id,
				  uint64_t handle,
				  uint64_t notifier_gid,
				  void *data,
				  size_t data_len)
{
  std::cout << __func__ << " from " << notifier_gid << " notify_id " << notify_id
	    << " handle " << handle << std::endl;
  assert(notifier_gid > 0);
  notify_bl.clear();
  notify_bl.append((char*)data, data_len);
  if (notify_sleep)
    sleep(notify_sleep);
  rados_notify_ack(notify_io, notify_oid, notify_id, handle, "reply", 5);
}

IoCtx *notify_ioctx;
class WatchNotifyTestCtx2 : public WatchCtx2
{
public:
  void handle_notify(uint64_t notify_id, uint64_t cookie, uint64_t notifier_gid,
		     bufferlist& bl)
  {
    std::cout << __func__ << std::endl;
    notify_bl = bl;
    bufferlist reply;
    reply.append("reply", 5);
    if (notify_sleep)
      sleep(notify_sleep);
    notify_ioctx->notify_ack(notify_oid, notify_id, cookie, reply);
  }
};

TEST_F(LibRadosWatchNotify, WatchNotifyTest) {
  ASSERT_EQ(0, sem_init(&sem, 0, 0));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  uint64_t handle;
  ASSERT_EQ(0,
      rados_watch(ioctx, "foo", 0, &handle, watch_notify_test_cb, NULL));
  ASSERT_EQ(0, rados_notify(ioctx, "foo", 0, NULL, 0));
  TestAlarm alarm;
  sem_wait(&sem);
  rados_unwatch(ioctx, "foo", handle);
  sem_destroy(&sem);
}

TEST_F(LibRadosWatchNotify, WatchNotify2Test) {
  notify_io = ioctx;
  notify_oid = "foo";
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, notify_oid, buf, sizeof(buf), 0));
  uint64_t handle;
  ASSERT_EQ(0,
      rados_watch2(ioctx, notify_oid, &handle, watch_notify2_test_cb, NULL));
  char *reply_buf;
  size_t reply_buf_len;
  ASSERT_EQ(0, rados_notify2(ioctx, notify_oid,
			     "notify", 6, 0,
			     &reply_buf, &reply_buf_len));
  bufferlist reply;
  reply.append(reply_buf, reply_buf_len);
  std::map<uint64_t, bufferlist> reply_map;
  bufferlist::iterator reply_p = reply.begin();
  ::decode(reply_map, reply_p);
  ASSERT_EQ(1, reply_map.size());
  ASSERT_EQ(5, reply_map.begin()->second.length());
  ASSERT_EQ(0, strncmp("reply", reply_map.begin()->second.c_str(), 5));
  rados_unwatch(ioctx, notify_oid, handle);
}

TEST_F(LibRadosWatchNotify, WatchNotify2TimeoutTest) {
  notify_io = ioctx;
  notify_oid = "foo";
  notify_sleep = 3; // 3s
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, notify_oid, buf, sizeof(buf), 0));
  uint64_t handle;
  ASSERT_EQ(0,
      rados_watch2(ioctx, notify_oid, &handle, watch_notify2_test_cb, NULL));
  char *reply_buf;
  size_t reply_buf_len;
  ASSERT_EQ(-ETIMEDOUT, rados_notify2(ioctx, notify_oid,
				      "notify", 6, 1000, // 1s
				      &reply_buf, &reply_buf_len));
  rados_unwatch(ioctx, notify_oid, handle);
}

TEST_P(LibRadosWatchNotifyPP, WatchNotifyTestPP) {
  ASSERT_EQ(0, sem_init(&sem, 0, 0));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
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
  sem_destroy(&sem);
}

TEST_P(LibRadosWatchNotifyPP, WatchNotify2TestPP) {
  notify_oid = "foo";
  notify_ioctx = &ioctx;
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write(notify_oid, bl1, sizeof(buf), 0));
  uint64_t handle;
  WatchNotifyTestCtx2 ctx;
  ASSERT_EQ(0, ioctx.watch2(notify_oid, &handle, &ctx));
  std::list<obj_watch_t> watches;
  ASSERT_EQ(0, ioctx.list_watchers(notify_oid, &watches));
  ASSERT_EQ(watches.size(), 1u);
  bufferlist bl2, bl_reply;
  ASSERT_EQ(0, ioctx.notify2(notify_oid, bl2, 0, &bl_reply));
  bufferlist::iterator p = bl_reply.begin();
  std::map<uint64_t,bufferlist> reply_map;
  ::decode(reply_map, p);
  ASSERT_EQ(1u, reply_map.size());
  ASSERT_EQ(5, reply_map.begin()->second.length());
  ASSERT_EQ(0, strncmp("reply", reply_map.begin()->second.c_str(), 5));
  ioctx.unwatch(notify_oid, handle);
}

TEST_P(LibRadosWatchNotifyPP, WatchNotify2TimeoutTestPP) {
  notify_oid = "foo";
  notify_ioctx = &ioctx;
  notify_sleep = 3;  // 3s
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write(notify_oid, bl1, sizeof(buf), 0));
  uint64_t handle;
  WatchNotifyTestCtx2 ctx;
  ASSERT_EQ(0, ioctx.watch2(notify_oid, &handle, &ctx));
  std::list<obj_watch_t> watches;
  ASSERT_EQ(0, ioctx.list_watchers(notify_oid, &watches));
  ASSERT_EQ(watches.size(), 1u);
  bufferlist bl2, bl_reply;
  ASSERT_EQ(-ETIMEDOUT, ioctx.notify2(notify_oid, bl2, 1000 /* 1s */, &bl_reply));
  ioctx.unwatch(notify_oid, handle);
}

TEST_P(LibRadosWatchNotifyPP, WatchNotifyTimeoutTestPP) {
  ASSERT_EQ(0, sem_init(&sem, 0, 0));
  ioctx.set_notify_timeout(1);
  uint64_t handle;
  WatchNotifyTestCtx ctx;

  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));

  ASSERT_EQ(0, ioctx.watch("foo", 0, &handle, &ctx));
  sem_destroy(&sem);
  ASSERT_EQ(0, ioctx.unwatch("foo", handle));
}

TEST_F(LibRadosWatchNotifyEC, WatchNotifyTest) {
  ASSERT_EQ(0, sem_init(&sem, 0, 0));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  uint64_t handle;
  ASSERT_EQ(0,
      rados_watch(ioctx, "foo", 0, &handle, watch_notify_test_cb, NULL));
  ASSERT_EQ(0, rados_notify(ioctx, "foo", 0, NULL, 0));
  TestAlarm alarm;
  sem_wait(&sem);
  rados_unwatch(ioctx, "foo", handle);
  sem_destroy(&sem);
}

TEST_F(LibRadosWatchNotifyECPP, WatchNotifyTestPP) {
  ASSERT_EQ(0, sem_init(&sem, 0, 0));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
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
  sem_destroy(&sem);
}

TEST_F(LibRadosWatchNotifyECPP, WatchNotifyTimeoutTestPP) {
  ASSERT_EQ(0, sem_init(&sem, 0, 0));
  ioctx.set_notify_timeout(1);
  uint64_t handle;
  WatchNotifyTestCtx ctx;

  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));

  ASSERT_EQ(0, ioctx.watch("foo", 0, &handle, &ctx));
  sem_destroy(&sem);
  ASSERT_EQ(0, ioctx.unwatch("foo", handle));
}


INSTANTIATE_TEST_CASE_P(LibRadosWatchNotifyPPTests, LibRadosWatchNotifyPP,
			::testing::Values("", "cache"));
