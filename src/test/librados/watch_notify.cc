#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/rados/rados_types.h"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"

#include <errno.h>
#include <fcntl.h>
#include <semaphore.h>
#include "gtest/gtest.h"
#include "include/encoding.h"
#include <set>
#include <map>

using namespace librados;

typedef RadosTest LibRadosWatchNotify;
typedef RadosTestParamPP LibRadosWatchNotifyPP;
typedef RadosTestEC LibRadosWatchNotifyEC;
typedef RadosTestECPP LibRadosWatchNotifyECPP;

int notify_sleep = 0;

// notify
static sem_t *sem;

static void watch_notify_test_cb(uint8_t opcode, uint64_t ver, void *arg)
{
  std::cout << __func__ << std::endl;
  sem_post(sem);
}

class WatchNotifyTestCtx : public WatchCtx
{
public:
    void notify(uint8_t opcode, uint64_t ver, bufferlist& bl)
    {
      std::cout << __func__ << std::endl;
      sem_post(sem);
    }
};

// notify 2
bufferlist notify_bl;
std::set<uint64_t> notify_cookies;
rados_ioctx_t notify_io;
const char *notify_oid = 0;
int notify_err = 0;

static void watch_notify2_test_cb(void *arg,
				  uint64_t notify_id,
				  uint64_t cookie,
				  uint64_t notifier_gid,
				  void *data,
				  size_t data_len)
{
  std::cout << __func__ << " from " << notifier_gid << " notify_id " << notify_id
	    << " cookie " << cookie << std::endl;
  assert(notifier_gid > 0);
  notify_cookies.insert(cookie);
  notify_bl.clear();
  notify_bl.append((char*)data, data_len);
  if (notify_sleep)
    sleep(notify_sleep);
  rados_notify_ack(notify_io, notify_oid, notify_id, cookie, "reply", 5);
}

static void watch_notify2_test_errcb(void *arg, uint64_t cookie, int err)
{
  std::cout << __func__ << " cookie " << cookie << " err " << err << std::endl;
  assert(cookie > 1000);
  notify_err = err;
}

IoCtx *notify_ioctx;
class WatchNotifyTestCtx2 : public WatchCtx2
{
public:
  void handle_notify(uint64_t notify_id, uint64_t cookie, uint64_t notifier_gid,
		     bufferlist& bl) {
    std::cout << __func__ << " cookie " << cookie << " notify_id " << notify_id
	      << " notifier_gid " << notifier_gid << std::endl;
    notify_bl = bl;
    notify_cookies.insert(cookie);
    bufferlist reply;
    reply.append("reply", 5);
    if (notify_sleep)
      sleep(notify_sleep);
    notify_ioctx->notify_ack(notify_oid, notify_id, cookie, reply);
  }

  void handle_error(uint64_t cookie, int err) {
    std::cout << __func__ << " cookie " << cookie
	      << " err " << err << std::endl;
    assert(cookie > 1000);
    notify_err = err;
  }
};

// --

#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

TEST_F(LibRadosWatchNotify, WatchNotify) {
  ASSERT_NE(SEM_FAILED, (sem = sem_open("test_watch_notify_sem", O_CREAT, 0644, 0)));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  uint64_t handle;
  ASSERT_EQ(0,
      rados_watch(ioctx, "foo", 0, &handle, watch_notify_test_cb, NULL));
  ASSERT_EQ(0, rados_notify(ioctx, "foo", 0, NULL, 0));
  TestAlarm alarm;
  sem_wait(sem);
  rados_unwatch(ioctx, "foo", handle);

  // when dne ...
  ASSERT_EQ(-ENOENT,
      rados_watch(ioctx, "dne", 0, &handle, watch_notify_test_cb, NULL));

  sem_close(sem);
}

TEST_P(LibRadosWatchNotifyPP, WatchNotify) {
  ASSERT_NE(SEM_FAILED, (sem = sem_open("test_watch_notify_sem", O_CREAT, 0644, 0)));
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
  sem_wait(sem);
  ioctx.unwatch("foo", handle);
  sem_close(sem);
}

TEST_F(LibRadosWatchNotifyEC, WatchNotify) {
  ASSERT_NE(SEM_FAILED, (sem = sem_open("test_watch_notify_sem", O_CREAT, 0644, 0)));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  uint64_t handle;
  ASSERT_EQ(0,
      rados_watch(ioctx, "foo", 0, &handle, watch_notify_test_cb, NULL));
  ASSERT_EQ(0, rados_notify(ioctx, "foo", 0, NULL, 0));
  TestAlarm alarm;
  sem_wait(sem);
  rados_unwatch(ioctx, "foo", handle);
  sem_close(sem);
}

TEST_F(LibRadosWatchNotifyECPP, WatchNotify) {
  ASSERT_NE(SEM_FAILED, (sem = sem_open("test_watch_notify_sem", O_CREAT, 0644, 0)));
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
  sem_wait(sem);
  ioctx.unwatch("foo", handle);
  sem_close(sem);
}

// --

TEST_P(LibRadosWatchNotifyPP, WatchNotifyTimeout) {
  ASSERT_NE(SEM_FAILED, (sem = sem_open("test_watch_notify_sem", O_CREAT, 0644, 0)));
  ioctx.set_notify_timeout(1);
  uint64_t handle;
  WatchNotifyTestCtx ctx;

  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));

  ASSERT_EQ(0, ioctx.watch("foo", 0, &handle, &ctx));
  sem_close(sem);
  ASSERT_EQ(0, ioctx.unwatch("foo", handle));
}

TEST_F(LibRadosWatchNotifyECPP, WatchNotifyTimeout) {
  ASSERT_NE(SEM_FAILED, (sem = sem_open("test_watch_notify_sem", O_CREAT, 0644, 0)));
  ioctx.set_notify_timeout(1);
  uint64_t handle;
  WatchNotifyTestCtx ctx;

  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));

  ASSERT_EQ(0, ioctx.watch("foo", 0, &handle, &ctx));
  sem_close(sem);
  ASSERT_EQ(0, ioctx.unwatch("foo", handle));
}

#pragma GCC diagnostic pop
#pragma GCC diagnostic warning "-Wpragmas"


// --

TEST_F(LibRadosWatchNotify, Watch2Delete) {
  notify_io = ioctx;
  notify_oid = "foo";
  notify_err = 0;
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, notify_oid, buf, sizeof(buf), 0));
  uint64_t handle;
  ASSERT_EQ(0,
	    rados_watch2(ioctx, notify_oid, &handle,
			 watch_notify2_test_cb,
			 watch_notify2_test_errcb, NULL));
  ASSERT_EQ(0, rados_remove(ioctx, notify_oid));
  int left = 300;
  std::cout << "waiting up to " << left << " for disconnect notification ..."
	    << std::endl;
  while (notify_err == 0 && --left) {
    sleep(1);
  }
  ASSERT_TRUE(left > 0);
  ASSERT_EQ(-ENOTCONN, notify_err);
  ASSERT_EQ(-ENOTCONN, rados_watch_check(ioctx, handle));
  rados_unwatch2(ioctx, handle);
}

TEST_F(LibRadosWatchNotify, Watch2Timeout) {
  notify_io = ioctx;
  notify_oid = "foo";
  notify_cookies.clear();
  notify_err = 0;
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, notify_oid, buf, sizeof(buf), 0));
  uint64_t handle;
  time_t start = time(0);
  ASSERT_EQ(0,
	    rados_watch2(ioctx, notify_oid, &handle,
			 watch_notify2_test_cb,
			 watch_notify2_test_errcb, NULL));
  int age = rados_watch_check(ioctx, handle);
  time_t age_bound = time(0) + 1 - start;
  ASSERT_LT(age, age_bound * 1000);
  ASSERT_GT(age, 0);
  rados_conf_set(cluster, "objecter_inject_no_watch_ping", "true");
  int left = 900;
  std::cout << "waiting up to " << left << " for osd to time us out ..."
	    << std::endl;
  while (notify_err == 0 && --left) {
    sleep(1);
  }
  ASSERT_TRUE(left > 0);
  rados_conf_set(cluster, "objecter_inject_no_watch_ping", "false");
  ASSERT_EQ(-ENOTCONN, notify_err);
  ASSERT_EQ(-ENOTCONN, rados_watch_check(ioctx, handle));

  // a subsequent notify should not reach us
  char *reply_buf = 0;
  size_t reply_buf_len;
  ASSERT_EQ(0, rados_notify2(ioctx, notify_oid,
			     "notify", 6, 300000,
			     &reply_buf, &reply_buf_len));
  {
    bufferlist reply;
    reply.append(reply_buf, reply_buf_len);
    std::map<std::pair<uint64_t,uint64_t>, bufferlist> reply_map;
    std::set<std::pair<uint64_t,uint64_t> > missed_map;
    bufferlist::iterator reply_p = reply.begin();
    ::decode(reply_map, reply_p);
    ::decode(missed_map, reply_p);
    ASSERT_EQ(0u, reply_map.size());
    ASSERT_EQ(0u, missed_map.size());
  }
  ASSERT_EQ(0u, notify_cookies.size());
  ASSERT_EQ(-ENOTCONN, rados_watch_check(ioctx, handle));
  rados_buffer_free(reply_buf);

  // re-watch
  rados_unwatch2(ioctx, handle);
  handle = 0;
  ASSERT_EQ(0,
	    rados_watch2(ioctx, notify_oid, &handle,
			 watch_notify2_test_cb,
			 watch_notify2_test_errcb, NULL));
  ASSERT_GT(rados_watch_check(ioctx, handle), 0);

  // and now a notify will work.
  ASSERT_EQ(0, rados_notify2(ioctx, notify_oid,
			     "notify", 6, 300000,
			     &reply_buf, &reply_buf_len));
  {
    bufferlist reply;
    reply.append(reply_buf, reply_buf_len);
    std::map<std::pair<uint64_t,uint64_t>, bufferlist> reply_map;
    std::set<std::pair<uint64_t,uint64_t> > missed_map;
    bufferlist::iterator reply_p = reply.begin();
    ::decode(reply_map, reply_p);
    ::decode(missed_map, reply_p);
    ASSERT_EQ(1u, reply_map.size());
    ASSERT_EQ(0u, missed_map.size());
    ASSERT_EQ(1u, notify_cookies.count(handle));
    ASSERT_EQ(5u, reply_map.begin()->second.length());
    ASSERT_EQ(0, strncmp("reply", reply_map.begin()->second.c_str(), 5));
  }
  ASSERT_EQ(1u, notify_cookies.size());
  ASSERT_GT(rados_watch_check(ioctx, handle), 0);

  rados_unwatch2(ioctx, handle);
}

// --

TEST_F(LibRadosWatchNotify, WatchNotify2) {
  notify_io = ioctx;
  notify_oid = "foo";
  notify_cookies.clear();
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, notify_oid, buf, sizeof(buf), 0));
  uint64_t handle;
  ASSERT_EQ(0,
      rados_watch2(ioctx, notify_oid, &handle,
		   watch_notify2_test_cb,
		   watch_notify2_test_errcb, NULL));
  ASSERT_GT(rados_watch_check(ioctx, handle), 0);
  char *reply_buf = 0;
  size_t reply_buf_len;
  ASSERT_EQ(0, rados_notify2(ioctx, notify_oid,
			     "notify", 6, 300000,
			     &reply_buf, &reply_buf_len));
  bufferlist reply;
  reply.append(reply_buf, reply_buf_len);
  std::map<std::pair<uint64_t,uint64_t>, bufferlist> reply_map;
  std::set<std::pair<uint64_t,uint64_t> > missed_map;
  bufferlist::iterator reply_p = reply.begin();
  ::decode(reply_map, reply_p);
  ::decode(missed_map, reply_p);
  ASSERT_EQ(1u, reply_map.size());
  ASSERT_EQ(0u, missed_map.size());
  ASSERT_EQ(1u, notify_cookies.size());
  ASSERT_EQ(1u, notify_cookies.count(handle));
  ASSERT_EQ(5u, reply_map.begin()->second.length());
  ASSERT_EQ(0, strncmp("reply", reply_map.begin()->second.c_str(), 5));
  ASSERT_GT(rados_watch_check(ioctx, handle), 0);
  rados_buffer_free(reply_buf);

  // try it on a non-existent object ... our buffer pointers
  // should get zeroed.
  ASSERT_EQ(-ENOENT, rados_notify2(ioctx, "doesnotexist",
				   "notify", 6, 300000,
				   &reply_buf, &reply_buf_len));
  ASSERT_EQ((char*)0, reply_buf);
  ASSERT_EQ(0u, reply_buf_len);

  rados_unwatch2(ioctx, handle);
}

TEST_F(LibRadosWatchNotify, AioNotify) {
  notify_io = ioctx;
  notify_oid = "foo";
  notify_cookies.clear();
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, notify_oid, buf, sizeof(buf), 0));
  uint64_t handle;
  ASSERT_EQ(0,
      rados_watch2(ioctx, notify_oid, &handle,
		   watch_notify2_test_cb,
		   watch_notify2_test_errcb, NULL));
  ASSERT_GT(rados_watch_check(ioctx, handle), 0);
  char *reply_buf = 0;
  size_t reply_buf_len;
  rados_completion_t comp;
  ASSERT_EQ(0, rados_aio_create_completion(NULL, NULL, NULL, &comp));
  ASSERT_EQ(0, rados_aio_notify(ioctx, "foo", comp, "notify", 6, 300000,
                                &reply_buf, &reply_buf_len));
  ASSERT_EQ(0, rados_aio_wait_for_complete(comp));
  ASSERT_EQ(0, rados_aio_get_return_value(comp));
  rados_aio_release(comp);

  bufferlist reply;
  reply.append(reply_buf, reply_buf_len);
  std::map<std::pair<uint64_t,uint64_t>, bufferlist> reply_map;
  std::set<std::pair<uint64_t,uint64_t> > missed_map;
  bufferlist::iterator reply_p = reply.begin();
  ::decode(reply_map, reply_p);
  ::decode(missed_map, reply_p);
  ASSERT_EQ(1u, reply_map.size());
  ASSERT_EQ(0u, missed_map.size());
  ASSERT_EQ(1u, notify_cookies.size());
  ASSERT_EQ(1u, notify_cookies.count(handle));
  ASSERT_EQ(5u, reply_map.begin()->second.length());
  ASSERT_EQ(0, strncmp("reply", reply_map.begin()->second.c_str(), 5));
  ASSERT_GT(rados_watch_check(ioctx, handle), 0);
  rados_buffer_free(reply_buf);

  // try it on a non-existent object ... our buffer pointers
  // should get zeroed.
  ASSERT_EQ(0, rados_aio_create_completion(NULL, NULL, NULL, &comp));
  ASSERT_EQ(0, rados_aio_notify(ioctx, "doesnotexist", comp, "notify", 6,
                                300000, &reply_buf, &reply_buf_len));
  ASSERT_EQ(0, rados_aio_wait_for_complete(comp));
  ASSERT_EQ(-ENOENT, rados_aio_get_return_value(comp));
  rados_aio_release(comp);
  ASSERT_EQ((char*)0, reply_buf);
  ASSERT_EQ(0u, reply_buf_len);

  rados_unwatch2(ioctx, handle);
}

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
  WatchNotifyTestCtx2 ctx;
  ASSERT_EQ(0, ioctx.watch2(notify_oid, &handle, &ctx));
  ASSERT_GT(ioctx.watch_check(handle), 0);
  std::list<obj_watch_t> watches;
  ASSERT_EQ(0, ioctx.list_watchers(notify_oid, &watches));
  ASSERT_EQ(watches.size(), 1u);
  bufferlist bl2, bl_reply;
  ASSERT_EQ(0, ioctx.notify2(notify_oid, bl2, 300000, &bl_reply));
  bufferlist::iterator p = bl_reply.begin();
  std::map<std::pair<uint64_t,uint64_t>,bufferlist> reply_map;
  std::set<std::pair<uint64_t,uint64_t> > missed_map;
  ::decode(reply_map, p);
  ::decode(missed_map, p);
  ASSERT_EQ(1u, notify_cookies.size());
  ASSERT_EQ(1u, notify_cookies.count(handle));
  ASSERT_EQ(1u, reply_map.size());
  ASSERT_EQ(5u, reply_map.begin()->second.length());
  ASSERT_EQ(0, strncmp("reply", reply_map.begin()->second.c_str(), 5));
  ASSERT_EQ(0u, missed_map.size());
  ASSERT_GT(ioctx.watch_check(handle), 0);
  ioctx.unwatch2(handle);
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
  WatchNotifyTestCtx2 ctx;
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
  bufferlist::iterator p = bl_reply.begin();
  std::map<std::pair<uint64_t,uint64_t>,bufferlist> reply_map;
  std::set<std::pair<uint64_t,uint64_t> > missed_map;
  ::decode(reply_map, p);
  ::decode(missed_map, p);
  ASSERT_EQ(1u, notify_cookies.size());
  ASSERT_EQ(1u, notify_cookies.count(handle));
  ASSERT_EQ(1u, reply_map.size());
  ASSERT_EQ(5u, reply_map.begin()->second.length());
  ASSERT_EQ(0, strncmp("reply", reply_map.begin()->second.c_str(), 5));
  ASSERT_EQ(0u, missed_map.size());
  ASSERT_GT(ioctx.watch_check(handle), 0);
  ioctx.unwatch2(handle);
}

// --

TEST_F(LibRadosWatchNotify, WatchNotify2Multi) {
  notify_io = ioctx;
  notify_oid = "foo";
  notify_cookies.clear();
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, notify_oid, buf, sizeof(buf), 0));
  uint64_t handle1, handle2;
  ASSERT_EQ(0,
      rados_watch2(ioctx, notify_oid, &handle1,
		   watch_notify2_test_cb,
		   watch_notify2_test_errcb, NULL));
  ASSERT_EQ(0,
      rados_watch2(ioctx, notify_oid, &handle2,
		   watch_notify2_test_cb,
		   watch_notify2_test_errcb, NULL));
  ASSERT_GT(rados_watch_check(ioctx, handle1), 0);
  ASSERT_GT(rados_watch_check(ioctx, handle2), 0);
  ASSERT_NE(handle1, handle2);
  char *reply_buf = 0;
  size_t reply_buf_len;
  ASSERT_EQ(0, rados_notify2(ioctx, notify_oid,
			     "notify", 6, 300000,
			     &reply_buf, &reply_buf_len));
  bufferlist reply;
  reply.append(reply_buf, reply_buf_len);
  std::map<std::pair<uint64_t,uint64_t>, bufferlist> reply_map;
  std::set<std::pair<uint64_t,uint64_t> > missed_map;
  bufferlist::iterator reply_p = reply.begin();
  ::decode(reply_map, reply_p);
  ::decode(missed_map, reply_p);
  ASSERT_EQ(2u, reply_map.size());
  ASSERT_EQ(5u, reply_map.begin()->second.length());
  ASSERT_EQ(0u, missed_map.size());
  ASSERT_EQ(2u, notify_cookies.size());
  ASSERT_EQ(1u, notify_cookies.count(handle1));
  ASSERT_EQ(1u, notify_cookies.count(handle2));
  ASSERT_EQ(0, strncmp("reply", reply_map.begin()->second.c_str(), 5));
  ASSERT_GT(rados_watch_check(ioctx, handle1), 0);
  ASSERT_GT(rados_watch_check(ioctx, handle2), 0);
  rados_buffer_free(reply_buf);
  rados_unwatch2(ioctx, handle1);
  rados_unwatch2(ioctx, handle2);
}

// --

TEST_F(LibRadosWatchNotify, WatchNotify2Timeout) {
  notify_io = ioctx;
  notify_oid = "foo";
  notify_sleep = 3; // 3s
  notify_cookies.clear();
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, notify_oid, buf, sizeof(buf), 0));
  uint64_t handle;
  ASSERT_EQ(0,
      rados_watch2(ioctx, notify_oid, &handle,
		   watch_notify2_test_cb,
		   watch_notify2_test_errcb, NULL));
  ASSERT_GT(rados_watch_check(ioctx, handle), 0);
  char *reply_buf = 0;
  size_t reply_buf_len;
  ASSERT_EQ(-ETIMEDOUT, rados_notify2(ioctx, notify_oid,
				      "notify", 6, 1000, // 1s
				      &reply_buf, &reply_buf_len));
  ASSERT_EQ(1u, notify_cookies.size());
  {
    bufferlist reply;
    reply.append(reply_buf, reply_buf_len);
    std::map<std::pair<uint64_t,uint64_t>, bufferlist> reply_map;
    std::set<std::pair<uint64_t,uint64_t> > missed_map;
    bufferlist::iterator reply_p = reply.begin();
    ::decode(reply_map, reply_p);
    ::decode(missed_map, reply_p);
    ASSERT_EQ(0u, reply_map.size());
    ASSERT_EQ(1u, missed_map.size());
  }
  rados_buffer_free(reply_buf);

  // we should get the next notify, though!
  notify_sleep = 0;
  notify_cookies.clear();
  ASSERT_EQ(0, rados_notify2(ioctx, notify_oid,
			     "notify", 6, 300000, // 300s
			     &reply_buf, &reply_buf_len));
  ASSERT_EQ(1u, notify_cookies.size());
  ASSERT_GT(rados_watch_check(ioctx, handle), 0);

  rados_unwatch2(ioctx, handle);
  rados_watch_flush(cluster);
}

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
  WatchNotifyTestCtx2 ctx;
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
  cluster.watch_flush();
  std::cout << " flushed" << std::endl;
}

// --

INSTANTIATE_TEST_CASE_P(LibRadosWatchNotifyPPTests, LibRadosWatchNotifyPP,
			::testing::Values("", "cache"));
