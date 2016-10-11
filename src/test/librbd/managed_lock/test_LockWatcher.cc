// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "test/librbd/test_fixture.h"
#include "librbd/managed_lock/LockWatcher.h"
#include "librbd/Lock.h"
#include "librbd/managed_lock/LockWatcherTypes.h"
#include "include/int_types.h"
#include "test/librbd/test_support.h"
#include "test/librados/test.h"
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "common/errno.h"
#include <boost/assign/std/set.hpp>
#include <boost/assign/std/map.hpp>
#include <iostream>
#include <set>
#include <map>
#include <vector>

using namespace ceph;
using std::string;
using librbd::managed_lock::LockWatcher;
using librbd::managed_lock::NotifyOp;
using namespace boost::assign;

void register_test_watcher() {
}

class TestLockWatcher : public TestFixture {
public:


  TestLockWatcher() : m_watch_ctx(NULL), m_callback_lock("m_callback_lock")
  {
  }

  class WatchCtx : public librados::WatchCtx2 {
  public:
    explicit WatchCtx(TestLockWatcher &parent) : m_parent(parent), m_handle(0) {}

    int watch(const string& oid) {
      m_oid = oid;
      return m_parent.m_ioctx.watch2(m_oid, &m_handle, this);
    }

    int unwatch() {
      return m_parent.m_ioctx.unwatch2(m_handle);
    }

    virtual void handle_notify(uint64_t notify_id,
                               uint64_t cookie,
                               uint64_t notifier_id,
                               bufferlist& bl) {

      try {
	int op;
	bufferlist payload;
	bufferlist::iterator iter = bl.begin();
	DECODE_START(1, iter);
	::decode(op, iter);
	iter.copy_all(payload);
	DECODE_FINISH(iter);

        NotifyOp notify_op = static_cast<NotifyOp>(op);
	std::cout << "NOTIFY: " << notify_op << ", " << notify_id
		  << ", " << cookie << ", " << notifier_id << std::endl;

	Mutex::Locker l(m_parent.m_callback_lock);
        m_parent.m_notify_payloads[notify_op] = payload;

        bufferlist reply;
        if (m_parent.m_notify_acks.count(notify_op) > 0) {
          reply = m_parent.m_notify_acks[notify_op];
	  m_parent.m_notifies += notify_op;
	  m_parent.m_callback_cond.Signal();
        }

	m_parent.m_ioctx.notify_ack(m_oid, notify_id, cookie, reply);
      } catch (...) {
	FAIL();
      }
    }

    virtual void handle_error(uint64_t cookie, int err) {
      std::cerr << "ERROR: " << cookie << ", " << cpp_strerror(err)
		<< std::endl;
    }

    uint64_t get_handle() const {
      return m_handle;
    }

  private:
    TestLockWatcher &m_parent;
    std::string m_oid;
    uint64_t m_handle;
  };

  virtual void TearDown() {
    deregister_watch();
    TestFixture::TearDown();
  }

  int deregister_watch() {
    if (m_watch_ctx != NULL) {
      int r = m_watch_ctx->unwatch();

      librados::Rados rados(m_ioctx);
      rados.watch_flush();

      delete m_watch_ctx;
      m_watch_ctx = NULL;

      delete m_watcher;
      return r;
    }

    delete m_watcher;
    delete m_lock;
    return 0;
  }

  int register_watch(const string& oid) {
    m_lock = new librbd::Lock(m_ioctx, oid);
    m_watcher = new LockWatcher(m_lock);
    m_watch_ctx = new WatchCtx(*this);
    return m_watch_ctx->watch(oid);
  }

  bool wait_for_notifies() {
    Mutex::Locker l(m_callback_lock);
    while (m_notifies.size() < m_notify_acks.size()) {
      int r = m_callback_cond.WaitInterval(
          reinterpret_cast<CephContext *>(m_ioctx.cct()), m_callback_lock,
	  utime_t(10, 0));
      if (r != 0) {
	break;
      }
    }
    return (m_notifies.size() == m_notify_acks.size());
  }

  bufferlist create_response_message(int r) {
    bufferlist bl;
    ::encode(librbd::watcher::ResponseMessage(r), bl);
    return bl;
  }
  typedef std::map<NotifyOp, bufferlist> NotifyOpPayloads;
  typedef std::set<NotifyOp> NotifyOps;

  WatchCtx *m_watch_ctx;
  LockWatcher *m_watcher;
  librbd::Lock *m_lock;

  NotifyOps m_notifies;
  NotifyOpPayloads m_notify_payloads;
  NotifyOpPayloads m_notify_acks;

  Mutex m_callback_lock;
  Cond m_callback_cond;

};

TEST_F(TestLockWatcher, NotifyRequestLock) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_watch(ictx->header_oid));

  m_notify_acks = {{NotifyOp::NOTIFY_OP_REQUEST_LOCK, {}}};
  m_watcher->notify_request_lock();

  C_SaferCond ctx;
  m_watcher->flush(&ctx);
  ctx.wait();

  ASSERT_TRUE(wait_for_notifies());

  NotifyOps expected_notify_ops;
  expected_notify_ops += NotifyOp::NOTIFY_OP_REQUEST_LOCK;
  ASSERT_EQ(expected_notify_ops, m_notifies);
}

TEST_F(TestLockWatcher, NotifyReleasedLock) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_watch(ictx->header_oid));

  m_notify_acks = {{NotifyOp::NOTIFY_OP_RELEASED_LOCK, {}}};
  m_watcher->notify_released_lock();

  ASSERT_TRUE(wait_for_notifies());

  NotifyOps expected_notify_ops;
  expected_notify_ops += NotifyOp::NOTIFY_OP_RELEASED_LOCK;
  ASSERT_EQ(expected_notify_ops, m_notifies);
}

TEST_F(TestLockWatcher, NotifyAcquiredLock) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_watch(ictx->header_oid));

  m_notify_acks = {{NotifyOp::NOTIFY_OP_ACQUIRED_LOCK, {}}};
  m_watcher->notify_acquired_lock();

  ASSERT_TRUE(wait_for_notifies());

  NotifyOps expected_notify_ops;
  expected_notify_ops += NotifyOp::NOTIFY_OP_ACQUIRED_LOCK;
  ASSERT_EQ(expected_notify_ops, m_notifies);
}


