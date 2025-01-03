#include "include/rados/librados.h"
#include "include/rados/rados_types.h"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"
#include "crimson_utils.h"

#include <errno.h>
#include <fcntl.h>
#include <semaphore.h>
#include "gtest/gtest.h"
#include "include/encoding.h"
#include <set>
#include <map>

typedef RadosTestEC LibRadosWatchNotifyEC;

int notify_sleep = 0;

// notify
static sem_t sem;

static void watch_notify_test_cb(uint8_t opcode, uint64_t ver, void *arg)
{
  std::cout << __func__ << std::endl;
  sem_post(&sem);
}

class LibRadosWatchNotify : public RadosTest
{
protected:
  // notify 2
  bufferlist notify_bl;
  std::set<uint64_t> notify_cookies;
  rados_ioctx_t notify_io;
  const char *notify_oid = nullptr;
  int notify_err = 0;
  rados_completion_t notify_comp;

  static void watch_notify2_test_cb(void *arg,
                                    uint64_t notify_id,
                                    uint64_t cookie,
                                    uint64_t notifier_gid,
                                    void *data,
                                    size_t data_len);
  static void watch_notify2_test_errcb(void *arg, uint64_t cookie, int err);
  static void watch_notify2_test_errcb_reconnect(void *arg, uint64_t cookie, int err);
  static void watch_notify2_test_errcb_aio_reconnect(void *arg, uint64_t cookie, int err);
};


void LibRadosWatchNotify::watch_notify2_test_cb(void *arg,
				  uint64_t notify_id,
				  uint64_t cookie,
				  uint64_t notifier_gid,
				  void *data,
				  size_t data_len)
{
  std::cout << __func__ << " from " << notifier_gid << " notify_id " << notify_id
	    << " cookie " << cookie << std::endl;
  ceph_assert(notifier_gid > 0);
  auto thiz = reinterpret_cast<LibRadosWatchNotify*>(arg);
  ceph_assert(thiz);
  thiz->notify_cookies.insert(cookie);
  thiz->notify_bl.clear();
  thiz->notify_bl.append((char*)data, data_len);
  if (notify_sleep)
    sleep(notify_sleep);
  thiz->notify_err = 0;
  rados_notify_ack(thiz->notify_io, thiz->notify_oid, notify_id, cookie,
                   "reply", 5);
}

void LibRadosWatchNotify::watch_notify2_test_errcb(void *arg,
                                                   uint64_t cookie,
                                                   int err)
{
  std::cout << __func__ << " cookie " << cookie << " err " << err << std::endl;
  ceph_assert(cookie > 1000);
  auto thiz = reinterpret_cast<LibRadosWatchNotify*>(arg);
  ceph_assert(thiz);
  thiz->notify_err = err;
}

void LibRadosWatchNotify::watch_notify2_test_errcb_reconnect(void *arg,
                                                   uint64_t cookie,
                                                   int err)
{
  std::cout << __func__ << " cookie " << cookie << " err " << err << std::endl;
  ceph_assert(cookie > 1000);
  auto thiz = reinterpret_cast<LibRadosWatchNotify*>(arg);
  ceph_assert(thiz);
  thiz->notify_err = rados_unwatch2(thiz->ioctx, cookie);
  thiz->notify_cookies.erase(cookie); //delete old cookie
  thiz->notify_err = rados_watch2(thiz->ioctx, thiz->notify_oid, &cookie,
                  watch_notify2_test_cb, watch_notify2_test_errcb_reconnect, thiz);
  if (thiz->notify_err < 0) {
    std::cout << __func__ << " reconnect watch failed with error " << thiz->notify_err << std::endl;
    return;
  }
  return;
}


void LibRadosWatchNotify::watch_notify2_test_errcb_aio_reconnect(void *arg,
                                                   uint64_t cookie,
                                                   int err)
{
  std::cout << __func__ << " cookie " << cookie << " err " << err << std::endl;
  ceph_assert(cookie > 1000);
  auto thiz = reinterpret_cast<LibRadosWatchNotify*>(arg);
  ceph_assert(thiz);
  thiz->notify_err = rados_aio_unwatch(thiz->ioctx, cookie, thiz->notify_comp);
  ASSERT_EQ(0, rados_aio_create_completion2(nullptr, nullptr, &thiz->notify_comp));
  thiz->notify_cookies.erase(cookie); //delete old cookie
  thiz->notify_err = rados_aio_watch(thiz->ioctx, thiz->notify_oid, thiz->notify_comp, &cookie,
                  watch_notify2_test_cb, watch_notify2_test_errcb_aio_reconnect, thiz);
  ASSERT_EQ(0, rados_aio_wait_for_complete(thiz->notify_comp));
  ASSERT_EQ(0, rados_aio_get_return_value(thiz->notify_comp));
  rados_aio_release(thiz->notify_comp);
  if (thiz->notify_err < 0) {
    std::cout << __func__ << " reconnect watch failed with error " << thiz->notify_err << std::endl;
    return;
  }
  return;
}

class WatchNotifyTestCtx2;

// --

#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

TEST_F(LibRadosWatchNotify, WatchNotify) {
  ASSERT_EQ(0, sem_init(&sem, 0, 0));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  uint64_t handle;
  ASSERT_EQ(0,
      rados_watch(ioctx, "foo", 0, &handle, watch_notify_test_cb, NULL));
  for (unsigned i=0; i<10; ++i) {
    int r = rados_notify(ioctx, "foo", 0, NULL, 0);
    if (r == 0) {
      break;
    }
    if (!getenv("ALLOW_TIMEOUTS")) {
      ASSERT_EQ(0, r);
    }
  }
  TestAlarm alarm;
  sem_wait(&sem);
  rados_unwatch(ioctx, "foo", handle);

  // when dne ...
  ASSERT_EQ(-ENOENT,
      rados_watch(ioctx, "dne", 0, &handle, watch_notify_test_cb, NULL));

  sem_destroy(&sem);
}

TEST_F(LibRadosWatchNotifyEC, WatchNotify) {
  SKIP_IF_CRIMSON();
  ASSERT_EQ(0, sem_init(&sem, 0, 0));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  uint64_t handle;
  ASSERT_EQ(0,
      rados_watch(ioctx, "foo", 0, &handle, watch_notify_test_cb, NULL));
  for (unsigned i=0; i<10; ++i) {
    int r = rados_notify(ioctx, "foo", 0, NULL, 0);
    if (r == 0) {
      break;
    }
    if (!getenv("ALLOW_TIMEOUTS")) {
      ASSERT_EQ(0, r);
    }
  }
  TestAlarm alarm;
  sem_wait(&sem);
  rados_unwatch(ioctx, "foo", handle);
  sem_destroy(&sem);
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
			 watch_notify2_test_errcb, this));
  ASSERT_EQ(0, rados_remove(ioctx, notify_oid));
  int left = 300;
  std::cout << "waiting up to " << left << " for disconnect notification ..."
	    << std::endl;
  while (notify_err == 0 && --left) {
    sleep(1);
  }
  ASSERT_TRUE(left > 0);
  ASSERT_EQ(-ENOTCONN, notify_err);
  int rados_watch_check_err = rados_watch_check(ioctx, handle);
  // We may hit ENOENT due to socket failure and a forced reconnect
  EXPECT_TRUE(rados_watch_check_err == -ENOTCONN || rados_watch_check_err == -ENOENT)
    << "Where rados_watch_check_err = " << rados_watch_check_err;
  rados_unwatch2(ioctx, handle);
  rados_watch_flush(cluster);
}

TEST_F(LibRadosWatchNotify, AioWatchDelete) {
  notify_io = ioctx;
  notify_oid = "foo";
  notify_err = 0;
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, notify_oid, buf, sizeof(buf), 0));


  rados_completion_t comp;
  uint64_t handle;
  ASSERT_EQ(0, rados_aio_create_completion2(nullptr, nullptr, &comp));
  rados_aio_watch(ioctx, notify_oid, comp, &handle,
                  watch_notify2_test_cb, watch_notify2_test_errcb, this);
  ASSERT_EQ(0, rados_aio_wait_for_complete(comp));
  ASSERT_EQ(0, rados_aio_get_return_value(comp));
  rados_aio_release(comp);
  ASSERT_EQ(0, rados_remove(ioctx, notify_oid));
  int left = 300;
  std::cout << "waiting up to " << left << " for disconnect notification ..."
	    << std::endl;
  while (notify_err == 0 && --left) {
    sleep(1);
  }
  ASSERT_TRUE(left > 0);
  ASSERT_EQ(-ENOTCONN, notify_err);
  int rados_watch_check_err = rados_watch_check(ioctx, handle);
  // We may hit ENOENT due to socket failure injection and a forced reconnect
  EXPECT_TRUE(rados_watch_check_err == -ENOTCONN || rados_watch_check_err == -ENOENT)
    << "Where rados_watch_check_err = " << rados_watch_check_err;
  ASSERT_EQ(0, rados_aio_create_completion2(nullptr, nullptr, &comp));
  rados_aio_unwatch(ioctx, handle, comp);
  ASSERT_EQ(0, rados_aio_wait_for_complete(comp));
  ASSERT_EQ(-ENOENT, rados_aio_get_return_value(comp));
  rados_aio_release(comp);
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
		   watch_notify2_test_errcb_reconnect, this));
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
  auto reply_p = reply.cbegin();
  decode(reply_map, reply_p);
  decode(missed_map, reply_p);
  ASSERT_EQ(1u, reply_map.size());
  ASSERT_EQ(0u, missed_map.size());
  ASSERT_EQ(1u, notify_cookies.size());
  handle = *notify_cookies.begin();
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
  rados_watch_flush(cluster);
}

TEST_F(LibRadosWatchNotify, AioWatchNotify2) {
  notify_io = ioctx;
  notify_oid = "foo";
  notify_cookies.clear();
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, notify_oid, buf, sizeof(buf), 0));
  uint64_t handle;
  ASSERT_EQ(0, rados_aio_create_completion2(nullptr, nullptr, &notify_comp));
  rados_aio_watch(ioctx, notify_oid, notify_comp, &handle,
                  watch_notify2_test_cb, watch_notify2_test_errcb_aio_reconnect, this);
  
  ASSERT_EQ(0, rados_aio_wait_for_complete(notify_comp));
  ASSERT_EQ(0, rados_aio_get_return_value(notify_comp));
  rados_aio_release(notify_comp);

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
  auto reply_p = reply.cbegin();
  decode(reply_map, reply_p);
  decode(missed_map, reply_p);
  ASSERT_EQ(1u, reply_map.size());
  ASSERT_EQ(0u, missed_map.size());
  ASSERT_EQ(1u, notify_cookies.size());
  handle = *notify_cookies.begin();
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

  ASSERT_EQ(0, rados_aio_create_completion2(nullptr, nullptr, &notify_comp));
  rados_aio_unwatch(ioctx, handle, notify_comp);
  ASSERT_EQ(0, rados_aio_wait_for_complete(notify_comp));
  ASSERT_EQ(0, rados_aio_get_return_value(notify_comp));
  rados_aio_release(notify_comp);
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
		   watch_notify2_test_errcb, this));
  ASSERT_GT(rados_watch_check(ioctx, handle), 0);
  char *reply_buf = 0;
  size_t reply_buf_len;
  rados_completion_t comp;
  ASSERT_EQ(0, rados_aio_create_completion2(nullptr, nullptr, &comp));
  ASSERT_EQ(0, rados_aio_notify(ioctx, "foo", comp, "notify", 6, 300000,
                                &reply_buf, &reply_buf_len));
  ASSERT_EQ(0, rados_aio_wait_for_complete(comp));
  ASSERT_EQ(0, rados_aio_get_return_value(comp));
  rados_aio_release(comp);

  size_t nr_acks, nr_timeouts;
  notify_ack_t *acks = nullptr;
  notify_timeout_t *timeouts = nullptr;
  ASSERT_EQ(0, rados_decode_notify_response(reply_buf, reply_buf_len,
                                            &acks, &nr_acks, &timeouts, &nr_timeouts));
  ASSERT_EQ(1u, nr_acks);
  ASSERT_EQ(0u, nr_timeouts);
  ASSERT_EQ(1u, notify_cookies.size());
  ASSERT_EQ(1u, notify_cookies.count(handle));
  ASSERT_EQ(5u, acks[0].payload_len);
  ASSERT_EQ(0, strncmp("reply", acks[0].payload, acks[0].payload_len));
  ASSERT_GT(rados_watch_check(ioctx, handle), 0);
  rados_free_notify_response(acks, nr_acks, timeouts);
  rados_buffer_free(reply_buf);

  // try it on a non-existent object ... our buffer pointers
  // should get zeroed.
  ASSERT_EQ(0, rados_aio_create_completion2(nullptr, nullptr, &comp));
  ASSERT_EQ(0, rados_aio_notify(ioctx, "doesnotexist", comp, "notify", 6,
                                300000, &reply_buf, &reply_buf_len));
  ASSERT_EQ(0, rados_aio_wait_for_complete(comp));
  ASSERT_EQ(-ENOENT, rados_aio_get_return_value(comp));
  rados_aio_release(comp);
  ASSERT_EQ((char*)0, reply_buf);
  ASSERT_EQ(0u, reply_buf_len);

  rados_unwatch2(ioctx, handle);
  rados_watch_flush(cluster);
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
		   watch_notify2_test_errcb, this));
  ASSERT_EQ(0,
      rados_watch2(ioctx, notify_oid, &handle2,
		   watch_notify2_test_cb,
		   watch_notify2_test_errcb, this));
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
  auto reply_p = reply.cbegin();
  decode(reply_map, reply_p);
  decode(missed_map, reply_p);
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
  rados_watch_flush(cluster);
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
		   watch_notify2_test_errcb, this));
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
    auto reply_p = reply.cbegin();
    decode(reply_map, reply_p);
    decode(missed_map, reply_p);
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

  rados_completion_t comp;
  ASSERT_EQ(0, rados_aio_create_completion2(nullptr, nullptr, &comp));
  rados_aio_watch_flush(cluster, comp);
  ASSERT_EQ(0, rados_aio_wait_for_complete(comp));
  ASSERT_EQ(0, rados_aio_get_return_value(comp));
  rados_aio_release(comp);
  rados_buffer_free(reply_buf);

}

TEST_F(LibRadosWatchNotify, Watch3Timeout) {
  notify_io = ioctx;
  notify_oid = "foo";
  notify_cookies.clear();
  notify_err = 0;
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, notify_oid, buf, sizeof(buf), 0));
  uint64_t handle;
  time_t start = time(0);
  const uint32_t timeout = 4;
  {
    // make sure i timeout before the messenger reconnects to the OSD,
    // it will resend a watch request on behalf of the client, and the
    // timer of timeout on OSD side will be reset by the new request.
    char conf[128];
    ASSERT_EQ(0, rados_conf_get(cluster,
                                "ms_connection_idle_timeout",
                                conf, sizeof(conf)));
    auto connection_idle_timeout = std::stoll(conf);
    ASSERT_LT(timeout, connection_idle_timeout);
  }
  ASSERT_EQ(0,
	    rados_watch3(ioctx, notify_oid, &handle,
                     watch_notify2_test_cb, watch_notify2_test_errcb,
                     timeout, this));
  int age = rados_watch_check(ioctx, handle);
  time_t age_bound = time(0) + 1 - start;
  ASSERT_LT(age, age_bound * 1000);
  ASSERT_GT(age, 0);
  rados_conf_set(cluster, "objecter_inject_no_watch_ping", "true");
  // allow a long time here since an osd peering event will renew our
  // watch.
  int left = 256 * timeout;
  std::cout << "waiting up to " << left << " for osd to time us out ..."
	    << std::endl;
  while (notify_err == 0 && --left) {
    sleep(1);
  }
  ASSERT_GT(left, 0);
  rados_conf_set(cluster, "objecter_inject_no_watch_ping", "false");
  ASSERT_EQ(-ENOTCONN, notify_err);
  ASSERT_EQ(-ENOTCONN, rados_watch_check(ioctx, handle));

  // a subsequent notify should not reach us
  char *reply_buf = nullptr;
  size_t reply_buf_len;
  ASSERT_EQ(0, rados_notify2(ioctx, notify_oid,
			     "notify", 6, 300000,
			     &reply_buf, &reply_buf_len));
  {
    bufferlist reply;
    reply.append(reply_buf, reply_buf_len);
    std::map<std::pair<uint64_t,uint64_t>, bufferlist> reply_map;
    std::set<std::pair<uint64_t,uint64_t> > missed_map;
    auto reply_p = reply.cbegin();
    decode(reply_map, reply_p);
    decode(missed_map, reply_p);
    ASSERT_EQ(0u, reply_map.size());
    ASSERT_EQ(0u, missed_map.size());
  }
  ASSERT_EQ(0u, notify_cookies.size());
  ASSERT_EQ(-ENOTCONN, rados_watch_check(ioctx, handle));
  rados_buffer_free(reply_buf);

  // re-watch
  rados_unwatch2(ioctx, handle);
  rados_watch_flush(cluster);

  handle = 0;
  ASSERT_EQ(0,
	    rados_watch2(ioctx, notify_oid, &handle,
			 watch_notify2_test_cb,
			 watch_notify2_test_errcb, this));
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
    auto reply_p = reply.cbegin();
    decode(reply_map, reply_p);
    decode(missed_map, reply_p);
    ASSERT_EQ(1u, reply_map.size());
    ASSERT_EQ(0u, missed_map.size());
    ASSERT_EQ(1u, notify_cookies.count(handle));
    ASSERT_EQ(5u, reply_map.begin()->second.length());
    ASSERT_EQ(0, strncmp("reply", reply_map.begin()->second.c_str(), 5));
  }
  ASSERT_EQ(1u, notify_cookies.size());
  ASSERT_GT(rados_watch_check(ioctx, handle), 0);

  rados_buffer_free(reply_buf);
  rados_unwatch2(ioctx, handle);
  rados_watch_flush(cluster);
}

TEST_F(LibRadosWatchNotify, AioWatchDelete2) {
  notify_io = ioctx;
  notify_oid = "foo";
  notify_err = 0;
  char buf[128];
  uint32_t timeout = 3;
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, notify_oid, buf, sizeof(buf), 0));


  rados_completion_t comp;
  uint64_t handle;
  ASSERT_EQ(0, rados_aio_create_completion2(nullptr, nullptr, &comp));
  rados_aio_watch2(ioctx, notify_oid, comp, &handle,
                  watch_notify2_test_cb, watch_notify2_test_errcb, timeout, this);
  ASSERT_EQ(0, rados_aio_wait_for_complete(comp));
  ASSERT_EQ(0, rados_aio_get_return_value(comp));
  rados_aio_release(comp);
  ASSERT_EQ(0, rados_remove(ioctx, notify_oid));
  int left = 30;
  std::cout << "waiting up to " << left << " for disconnect notification ..."
      << std::endl;
  while (notify_err == 0 && --left) {
    sleep(1);
  }
  ASSERT_TRUE(left > 0);
  ASSERT_EQ(-ENOTCONN, notify_err);
  int rados_watch_check_err = rados_watch_check(ioctx, handle);
  // We may hit ENOENT due to socket failure injection and a forced reconnect
  EXPECT_TRUE(rados_watch_check_err == -ENOTCONN || rados_watch_check_err == -ENOENT)
    << "Where rados_watch_check_err = " << rados_watch_check_err;
  ASSERT_EQ(0, rados_aio_create_completion2(nullptr, nullptr, &comp));
  rados_aio_unwatch(ioctx, handle, comp);
  ASSERT_EQ(0, rados_aio_wait_for_complete(comp));
  ASSERT_EQ(-ENOENT, rados_aio_get_return_value(comp));
  rados_aio_release(comp);
}
