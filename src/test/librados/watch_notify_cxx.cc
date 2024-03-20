#include <errno.h>
#include <fcntl.h>
#include <semaphore.h>
#include <set>
#include <map>

#include "gtest/gtest.h"

#include "include/encoding.h"
#include "include/rados/librados.hpp"
#include "include/rados/rados_types.h"
#include "test/librados/test_cxx.h"
#include "test/librados/testcase_cxx.h"
#include "crimson_utils.h"

using namespace librados;

typedef RadosTestECPP LibRadosWatchNotifyECPP;

int notify_sleep = 0;

#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

class LibRadosWatchNotifyPP : public RadosTestParamPP
{
protected:
  bufferlist notify_bl;
  std::set<uint64_t> notify_cookies;
  rados_ioctx_t notify_io;
  const char *notify_oid = nullptr;
  int notify_err = 0;

  friend class WatchNotifyTestCtx2;
  friend class WatchNotifyTestCtx2TimeOut;
};

IoCtx *notify_ioctx;

class WatchNotifyTestCtx2 : public WatchCtx2
{
  LibRadosWatchNotifyPP *notify;

public:
  WatchNotifyTestCtx2(LibRadosWatchNotifyPP *notify)
    : notify(notify)
  {}

  void handle_notify(uint64_t notify_id, uint64_t cookie, uint64_t notifier_gid,
		     bufferlist& bl) override {
    std::cout << __func__ << " cookie " << cookie << " notify_id " << notify_id
	      << " notifier_gid " << notifier_gid << std::endl;
    notify->notify_bl = bl;
    notify->notify_cookies.insert(cookie);
    bufferlist reply;
    reply.append("reply", 5);
    if (notify_sleep)
      sleep(notify_sleep);
    notify_ioctx->notify_ack(notify->notify_oid, notify_id, cookie, reply);
  }

  void handle_error(uint64_t cookie, int err) override {
    std::cout << __func__ << " cookie " << cookie
	      << " err " << err << std::endl;
    ceph_assert(cookie > 1000);
    notify_ioctx->unwatch2(cookie);
    notify->notify_cookies.erase(cookie);
    notify->notify_err = notify_ioctx->watch2(notify->notify_oid, &cookie, this);
    if (notify->notify_err < err ) {
      std::cout << "reconnect notify_err " << notify->notify_err << " err " << err << std::endl;
    }
  }
};

class WatchNotifyTestCtx2TimeOut : public WatchCtx2
{
  LibRadosWatchNotifyPP *notify;

public:
  WatchNotifyTestCtx2TimeOut(LibRadosWatchNotifyPP *notify)
    : notify(notify)
  {}

  void handle_notify(uint64_t notify_id, uint64_t cookie, uint64_t notifier_gid,
		     bufferlist& bl) override {
    std::cout << __func__ << " cookie " << cookie << " notify_id " << notify_id
	      << " notifier_gid " << notifier_gid << std::endl;
    notify->notify_bl = bl;
    notify->notify_cookies.insert(cookie);
    bufferlist reply;
    reply.append("reply", 5);
    if (notify_sleep)
      sleep(notify_sleep);
    notify_ioctx->notify_ack(notify->notify_oid, notify_id, cookie, reply);
  }

  void handle_error(uint64_t cookie, int err) override {
    std::cout << __func__ << " cookie " << cookie
	      << " err " << err << std::endl;
    ceph_assert(cookie > 1000);
    notify->notify_err = err;
  }
};

// notify
static sem_t sem;

class WatchNotifyTestCtx : public WatchCtx
{
public:
  void notify(uint8_t opcode, uint64_t ver, bufferlist& bl) override
  {
    std::cout << __func__ << std::endl;
    sem_post(&sem);
  }
};

TEST_P(LibRadosWatchNotifyPP, WatchNotify) {
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
  ASSERT_EQ(1u, watches.size());
  bufferlist bl2;
  for (unsigned i=0; i<10; ++i) {
    int r = ioctx.notify("foo", 0, bl2);
    if (r == 0) {
      break;
    }
    if (!getenv("ALLOW_TIMEOUTS")) {
      ASSERT_EQ(0, r);
    }
  }
  TestAlarm alarm;
  sem_wait(&sem);
  ioctx.unwatch("foo", handle);
  sem_destroy(&sem);
}

TEST_F(LibRadosWatchNotifyECPP, WatchNotify) {
  SKIP_IF_CRIMSON();
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
  ASSERT_EQ(1u, watches.size());
  bufferlist bl2;
  for (unsigned i=0; i<10; ++i) {
    int r = ioctx.notify("foo", 0, bl2);
    if (r == 0) {
      break;
    }
    if (!getenv("ALLOW_TIMEOUTS")) {
      ASSERT_EQ(0, r);
    }
  }
  TestAlarm alarm;
  sem_wait(&sem);
  ioctx.unwatch("foo", handle);
  sem_destroy(&sem);
}

// --

TEST_P(LibRadosWatchNotifyPP, WatchNotifyTimeout) {
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

TEST_F(LibRadosWatchNotifyECPP, WatchNotifyTimeout) {
  SKIP_IF_CRIMSON();
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

#pragma GCC diagnostic pop
#pragma GCC diagnostic warning "-Wpragmas"

TEST_P(LibRadosWatchNotifyPP, WatchNotify2) {
  notify_oid = "foo";
  notify_ioctx = &ioctx;
  notify_cookies.clear();
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write(notify_oid, bl1, sizeof(buf), 0));
  uint64_t handle;
  WatchNotifyTestCtx2 ctx(this);
  ASSERT_EQ(0, ioctx.watch2(notify_oid, &handle, &ctx));
  ASSERT_GT(ioctx.watch_check(handle), 0);
  std::list<obj_watch_t> watches;
  ASSERT_EQ(0, ioctx.list_watchers(notify_oid, &watches));
  ASSERT_EQ(watches.size(), 1u);
  bufferlist bl2, bl_reply;
  ASSERT_EQ(0, ioctx.notify2(notify_oid, bl2, 300000, &bl_reply));
  auto p = bl_reply.cbegin();
  std::map<std::pair<uint64_t,uint64_t>,bufferlist> reply_map;
  std::set<std::pair<uint64_t,uint64_t> > missed_map;
  decode(reply_map, p);
  decode(missed_map, p);
  ASSERT_EQ(1u, notify_cookies.size());
  ASSERT_EQ(1u, notify_cookies.count(handle));
  ASSERT_EQ(1u, reply_map.size());
  ASSERT_EQ(5u, reply_map.begin()->second.length());
  ASSERT_EQ(0, strncmp("reply", reply_map.begin()->second.c_str(), 5));
  ASSERT_EQ(0u, missed_map.size());
  ASSERT_GT(ioctx.watch_check(handle), 0);
  ioctx.unwatch2(handle);
}

TEST_P(LibRadosWatchNotifyPP, AioWatchNotify2) {
  notify_oid = "foo";
  notify_ioctx = &ioctx;
  notify_cookies.clear();
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write(notify_oid, bl1, sizeof(buf), 0));

  uint64_t handle;
  WatchNotifyTestCtx2 ctx(this);
  librados::AioCompletion *comp = cluster.aio_create_completion();
  ASSERT_EQ(0, ioctx.aio_watch(notify_oid, comp, &handle, &ctx));
  ASSERT_EQ(0, comp->wait_for_complete());
  ASSERT_EQ(0, comp->get_return_value());
  comp->release();

  ASSERT_GT(ioctx.watch_check(handle), 0);
  std::list<obj_watch_t> watches;
  ASSERT_EQ(0, ioctx.list_watchers(notify_oid, &watches));
  ASSERT_EQ(watches.size(), 1u);
  bufferlist bl2, bl_reply;
  ASSERT_EQ(0, ioctx.notify2(notify_oid, bl2, 300000, &bl_reply));
  auto p = bl_reply.cbegin();
  std::map<std::pair<uint64_t,uint64_t>,bufferlist> reply_map;
  std::set<std::pair<uint64_t,uint64_t> > missed_map;
  decode(reply_map, p);
  decode(missed_map, p);
  ASSERT_EQ(1u, notify_cookies.size());
  ASSERT_EQ(1u, notify_cookies.count(handle));
  ASSERT_EQ(1u, reply_map.size());
  ASSERT_EQ(5u, reply_map.begin()->second.length());
  ASSERT_EQ(0, strncmp("reply", reply_map.begin()->second.c_str(), 5));
  ASSERT_EQ(0u, missed_map.size());
  ASSERT_GT(ioctx.watch_check(handle), 0);

  comp = cluster.aio_create_completion();
  ioctx.aio_unwatch(handle, comp);
  ASSERT_EQ(0, comp->wait_for_complete());
  comp->release();
}


TEST_P(LibRadosWatchNotifyPP, AioNotify) {
  notify_oid = "foo";
  notify_ioctx = &ioctx;
  notify_cookies.clear();
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write(notify_oid, bl1, sizeof(buf), 0));
  uint64_t handle;
  WatchNotifyTestCtx2 ctx(this);
  ASSERT_EQ(0, ioctx.watch2(notify_oid, &handle, &ctx));
  ASSERT_GT(ioctx.watch_check(handle), 0);
  std::list<obj_watch_t> watches;
  ASSERT_EQ(0, ioctx.list_watchers(notify_oid, &watches));
  ASSERT_EQ(watches.size(), 1u);
  bufferlist bl2, bl_reply;
  librados::AioCompletion *comp = cluster.aio_create_completion();
  ASSERT_EQ(0, ioctx.aio_notify(notify_oid, comp, bl2, 300000, &bl_reply));
  ASSERT_EQ(0, comp->wait_for_complete());
  ASSERT_EQ(0, comp->get_return_value());
  comp->release();
  std::vector<librados::notify_ack_t> acks;
  std::vector<librados::notify_timeout_t> timeouts;
  ioctx.decode_notify_response(bl_reply, &acks, &timeouts);
  ASSERT_EQ(1u, notify_cookies.size());
  ASSERT_EQ(1u, notify_cookies.count(handle));
  ASSERT_EQ(1u, acks.size());
  ASSERT_EQ(5u, acks[0].payload_bl.length());
  ASSERT_EQ(0, strncmp("reply", acks[0].payload_bl.c_str(), acks[0].payload_bl.length()));
  ASSERT_EQ(0u, timeouts.size());
  ASSERT_GT(ioctx.watch_check(handle), 0);
  ioctx.unwatch2(handle);
  cluster.watch_flush();
}

// --
TEST_P(LibRadosWatchNotifyPP, WatchNotify2Timeout) {
  notify_oid = "foo";
  notify_ioctx = &ioctx;
  notify_sleep = 3;  // 3s
  notify_cookies.clear();
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write(notify_oid, bl1, sizeof(buf), 0));
  uint64_t handle;
  WatchNotifyTestCtx2TimeOut ctx(this);
  ASSERT_EQ(0, ioctx.watch2(notify_oid, &handle, &ctx));
  ASSERT_GT(ioctx.watch_check(handle), 0);
  std::list<obj_watch_t> watches;
  ASSERT_EQ(0, ioctx.list_watchers(notify_oid, &watches));
  ASSERT_EQ(watches.size(), 1u);
  ASSERT_EQ(0u, notify_cookies.size());
  bufferlist bl2, bl_reply;
  std::cout << " trying..." << std::endl;
  ASSERT_EQ(-ETIMEDOUT, ioctx.notify2(notify_oid, bl2, 1000 /* 1s */,
				      &bl_reply));
  std::cout << " timed out" << std::endl;
  ASSERT_GT(ioctx.watch_check(handle), 0);
  ioctx.unwatch2(handle);

  std::cout << " flushing" << std::endl;
  librados::AioCompletion *comp = cluster.aio_create_completion();
  cluster.aio_watch_flush(comp);
  ASSERT_EQ(0, comp->wait_for_complete());
  ASSERT_EQ(0, comp->get_return_value());
  std::cout << " flushed" << std::endl;
  comp->release();
}

TEST_P(LibRadosWatchNotifyPP, WatchNotify3) {
  notify_oid = "foo";
  notify_ioctx = &ioctx;
  notify_cookies.clear();
  uint32_t timeout = 26; // configured timeout
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write(notify_oid, bl1, sizeof(buf), 0));
  uint64_t handle;
  WatchNotifyTestCtx2TimeOut ctx(this);
  ASSERT_EQ(0, ioctx.watch3(notify_oid, &handle, &ctx, timeout));
  ASSERT_GT(ioctx.watch_check(handle), 0);
  std::list<obj_watch_t> watches;
  ASSERT_EQ(0, ioctx.list_watchers(notify_oid, &watches));
  ASSERT_EQ(watches.size(), 1u);
  std::cout << "List watches" << std::endl;
  for (std::list<obj_watch_t>::iterator it = watches.begin();
    it != watches.end(); ++it) {
    ASSERT_EQ(it->timeout_seconds, timeout);
  }
  bufferlist bl2, bl_reply;
  std::cout << "notify2" << std::endl;
  ASSERT_EQ(0, ioctx.notify2(notify_oid, bl2, 300000, &bl_reply));
  std::cout << "notify2 done" << std::endl;
  auto p = bl_reply.cbegin();
  std::map<std::pair<uint64_t,uint64_t>,bufferlist> reply_map;
  std::set<std::pair<uint64_t,uint64_t> > missed_map;
  decode(reply_map, p);
  decode(missed_map, p);
  ASSERT_EQ(1u, notify_cookies.size());
  ASSERT_EQ(1u, notify_cookies.count(handle));
  ASSERT_EQ(1u, reply_map.size());
  ASSERT_EQ(5u, reply_map.begin()->second.length());
  ASSERT_EQ(0, strncmp("reply", reply_map.begin()->second.c_str(), 5));
  ASSERT_EQ(0u, missed_map.size());
  std::cout << "watch_check" << std::endl;
  ASSERT_GT(ioctx.watch_check(handle), 0);
  std::cout << "unwatch2" << std::endl;
  ioctx.unwatch2(handle);

  std::cout << " flushing" << std::endl;
  cluster.watch_flush();
  std::cout << "done" << std::endl;
}
// --

INSTANTIATE_TEST_SUITE_P(LibRadosWatchNotifyPPTests, LibRadosWatchNotifyPP,
			::testing::Values("", "cache"));
