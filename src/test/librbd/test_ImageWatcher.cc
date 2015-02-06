// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "test/librbd/test_fixture.h"
#include "include/int_types.h"
#include "include/stringify.h"
#include "include/rados/librados.h"
#include "include/rbd/librbd.hpp"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/RWLock.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "librbd/AioCompletion.h"
#include "librbd/internal.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "test/librados/test.h"
#include "gtest/gtest.h"
#include <boost/assign/list_of.hpp>
#include <boost/bind.hpp>
#include <iostream>
#include <list>
#include <set>
#include <sstream>
#include <vector>

using namespace ceph;

void register_test_image_watcher() {
}

class TestImageWatcher : public TestFixture {
public:

  TestImageWatcher() : m_watch_ctx(NULL), m_aio_completion_restarts(0),
		       m_expected_aio_restarts(0),
		       m_callback_lock("m_callback_lock")
  {
  }

  enum NotifyOp {
    NOTIFY_OP_ACQUIRED_LOCK = 0,
    NOTIFY_OP_RELEASED_LOCK = 1,
    NOTIFY_OP_REQUEST_LOCK  = 2,
    NOTIFY_OP_HEADER_UPDATE = 3
  };

  class WatchCtx : public librados::WatchCtx2 {
  public:
    WatchCtx(TestImageWatcher &parent) : m_parent(parent), m_handle(0) {}

    int watch(const librbd::ImageCtx &ictx) {
      m_header_oid = ictx.header_oid;
      return m_parent.m_ioctx.watch2(m_header_oid, &m_handle, this);
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

	Mutex::Locker l(m_parent.m_callback_lock);
	if (!m_parent.m_notify_acks.empty()) {
	  NotifyOp notify_op = m_parent.m_notify_acks.front().first;
	  if (notify_op != op) {
	    EXPECT_EQ(notify_op, static_cast<NotifyOp>(op));
	    m_parent.m_notify_acks.clear();
	  } else {
	    m_parent.m_ioctx.notify_ack(m_header_oid, notify_id, cookie,
					m_parent.m_notify_acks.front().second);
	    m_parent.m_notify_acks.pop_front();
	  }
	}

	m_parent.m_notifies.push_back(
	  std::make_pair(static_cast<NotifyOp>(op), payload));
	m_parent.m_callback_cond.Signal();
      } catch (...) {
	FAIL();
      }
    }

    virtual void handle_failed_notify(uint64_t notify_id,
                                      uint64_t cookie,
                                      uint64_t notifier_id) {
    }

    virtual void handle_error(uint64_t cookie, int er) {
    }

    uint64_t get_handle() const {
      return m_handle;
    }

  private:
    TestImageWatcher &m_parent;
    std::string m_header_oid;
    uint64_t m_handle;
  };

  virtual void TearDown() {
    deregister_image_watch();
    TestFixture::TearDown();
  }

  int deregister_image_watch() {
    if (m_watch_ctx != NULL) {
      int r = m_watch_ctx->unwatch();
      delete m_watch_ctx;
      m_watch_ctx = NULL;
      return r;
    }
    return 0;
  }

  int register_image_watch(librbd::ImageCtx &ictx) {
    m_watch_ctx = new WatchCtx(*this);
    return m_watch_ctx->watch(ictx);
  }

  bool wait_for_notifies(librbd::ImageCtx &ictx) {
    Mutex::Locker l(m_callback_lock);
    while (!m_notify_acks.empty()) {
      int r = m_callback_cond.WaitInterval(ictx.cct, m_callback_lock,
					 utime_t(10, 0));
      if (r != 0) {
	break;
      }
    }
    return m_notify_acks.empty();
  }


  librbd::AioCompletion *create_aio_completion(librbd::ImageCtx &ictx) {
    librbd::AioCompletion *aio_completion = new librbd::AioCompletion();
    aio_completion->complete_cb = &handle_aio_completion;
    aio_completion->complete_arg = this;

    aio_completion->init_time(&ictx, librbd::AIO_TYPE_NONE);
    m_aio_completions.insert(aio_completion);
    return aio_completion;
  }

  static void handle_aio_completion(void *arg1, void *arg2) {
    TestImageWatcher *test_image_watcher =
      reinterpret_cast<TestImageWatcher *>(arg2);
    assert(test_image_watcher->m_callback_lock.is_locked());
    test_image_watcher->m_callback_cond.Signal();
  }

  int handle_restart_aio(librbd::ImageCtx *ictx,
			 librbd::AioCompletion *aio_completion) {
    Mutex::Locker l1(m_callback_lock);
    ++m_aio_completion_restarts;

    RWLock::WLocker l2(ictx->owner_lock);
    if (!ictx->image_watcher->is_lock_owner() &&
	m_aio_completion_restarts < m_expected_aio_restarts) {
      EXPECT_EQ(0, ictx->image_watcher->request_lock(
        boost::bind(&TestImageWatcher::handle_restart_aio, this, ictx, _1),
	aio_completion));
    } else {
      {
	Mutex::Locker l2(aio_completion->lock);
	aio_completion->complete();
      }

      m_aio_completions.erase(aio_completion);
      delete aio_completion;
    }

    m_callback_cond.Signal();
    return 0;
  }

  bool wait_for_aio_completions(librbd::ImageCtx &ictx) {
    Mutex::Locker l(m_callback_lock);
    int r = 0;
    while (!m_aio_completions.empty() &&
	   m_aio_completion_restarts < m_expected_aio_restarts) {
      r = m_callback_cond.WaitInterval(ictx.cct, m_callback_lock,
				       utime_t(10, 0));
      if (r != 0) {
        break;
      }
    }
    return (r == 0);
  }

  typedef std::pair<NotifyOp, bufferlist> NotifyOpPayload;
  typedef std::list<NotifyOpPayload> NotifyOpPayloads;

  WatchCtx *m_watch_ctx;
  NotifyOpPayloads m_notifies;
  NotifyOpPayloads m_notify_acks;

  std::set<librbd::AioCompletion *> m_aio_completions;
  uint32_t m_aio_completion_restarts;
  uint32_t m_expected_aio_restarts;

  Mutex m_callback_lock;
  Cond m_callback_cond;

};

TEST_F(TestImageWatcher, IsLockSupported) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  RWLock::WLocker l(ictx->owner_lock);
  ASSERT_TRUE(ictx->image_watcher);
  ASSERT_TRUE(ictx->image_watcher->is_lock_supported());

  ictx->read_only = true;
  ASSERT_FALSE(ictx->image_watcher->is_lock_supported());
  ictx->read_only = false;

  ictx->features &= ~RBD_FEATURE_EXCLUSIVE_LOCK;
  ASSERT_FALSE(ictx->image_watcher->is_lock_supported());
  ictx->features |= RBD_FEATURE_EXCLUSIVE_LOCK;

  ictx->snap_id = 1234;
  ASSERT_FALSE(ictx->image_watcher->is_lock_supported());
  ictx->snap_id = CEPH_NOSNAP;
}

TEST_F(TestImageWatcher, TryLock) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_TRUE(ictx->image_watcher);

  RWLock::WLocker l(ictx->owner_lock);
  ASSERT_EQ(0, ictx->image_watcher->try_lock());
  ASSERT_TRUE(ictx->image_watcher->is_lock_owner());

  std::map<rados::cls::lock::locker_id_t,
           rados::cls::lock::locker_info_t> lockers;
  ClsLockType lock_type;
  ASSERT_EQ(0, rados::cls::lock::get_lock_info(&m_ioctx, ictx->header_oid,
					       RBD_LOCK_NAME, &lockers,
					       &lock_type, NULL));
  ASSERT_EQ(LOCK_EXCLUSIVE, lock_type);
  ASSERT_EQ(1U, lockers.size());
}

TEST_F(TestImageWatcher, TryLockNotifyAnnounceLocked) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_image_watch(*ictx));
  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_ACQUIRED_LOCK, bufferlist()));

  RWLock::WLocker l(ictx->owner_lock);
  ASSERT_EQ(0, ictx->image_watcher->try_lock());

  ASSERT_TRUE(wait_for_notifies(*ictx));

  ASSERT_TRUE(m_notify_acks.empty());
  NotifyOpPayloads expected_notify_ops = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_ACQUIRED_LOCK, bufferlist()));
  ASSERT_EQ(expected_notify_ops, m_notifies);
}

TEST_F(TestImageWatcher, TryLockWithTimedOutOwner) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  // use new Rados connection due to blacklisting
  librados::Rados rados;
  ASSERT_EQ("", connect_cluster_pp(rados));

  librados::IoCtx io_ctx;
  ASSERT_EQ(0, rados.ioctx_create(_pool_name.c_str(), io_ctx));
  librbd::ImageCtx *ictx = new librbd::ImageCtx(m_image_name.c_str(), "", NULL,
					        io_ctx, false);
  ASSERT_EQ(0, librbd::open_image(ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE, "auto 1234"));
  librbd::close_image(ictx);
  io_ctx.close();

  // no watcher on the locked image means we can break the lock
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  RWLock::WLocker l(ictx->owner_lock);
  ASSERT_EQ(0, ictx->image_watcher->try_lock());
  ASSERT_TRUE(ictx->image_watcher->is_lock_owner());

  rados.test_blacklist_self(false);
}

TEST_F(TestImageWatcher, TryLockWithUserExclusiveLock) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE, "manually locked"));

  RWLock::WLocker l(ictx->owner_lock);
  ASSERT_EQ(-EBUSY, ictx->image_watcher->try_lock());
  ASSERT_FALSE(ictx->image_watcher->is_lock_owner());

  ASSERT_EQ(0, unlock_image());
  ASSERT_EQ(0, ictx->image_watcher->try_lock());
  ASSERT_TRUE(ictx->image_watcher->is_lock_owner());
}

TEST_F(TestImageWatcher, TryLockWithUserSharedLocked) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, lock_image(*ictx, LOCK_SHARED, "manually locked"));

  RWLock::WLocker l(ictx->owner_lock);
  ASSERT_EQ(-EBUSY, ictx->image_watcher->try_lock());
  ASSERT_FALSE(ictx->image_watcher->is_lock_owner());

  ASSERT_EQ(0, unlock_image());
  ASSERT_EQ(0, ictx->image_watcher->try_lock());
  ASSERT_TRUE(ictx->image_watcher->is_lock_owner());
}

TEST_F(TestImageWatcher, UnlockNotLocked) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  RWLock::WLocker l(ictx->owner_lock);
  ASSERT_EQ(0, ictx->image_watcher->unlock());
}

TEST_F(TestImageWatcher, UnlockNotifyReleaseLock) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_image_watch(*ictx));
  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_ACQUIRED_LOCK, bufferlist()));

  RWLock::WLocker l(ictx->owner_lock);
  ASSERT_EQ(0, ictx->image_watcher->try_lock());
  ASSERT_TRUE(wait_for_notifies(*ictx));

  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_RELEASED_LOCK, bufferlist()));
  ASSERT_EQ(0, ictx->image_watcher->unlock());
  ASSERT_TRUE(wait_for_notifies(*ictx));

  ASSERT_TRUE(m_notify_acks.empty());
  NotifyOpPayloads expected_notify_ops = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_ACQUIRED_LOCK, bufferlist()))(
    std::make_pair(NOTIFY_OP_RELEASED_LOCK, bufferlist()));
  ASSERT_EQ(expected_notify_ops, m_notifies);
}

TEST_F(TestImageWatcher, UnlockBrokenLock) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  RWLock::WLocker l(ictx->owner_lock);
  ASSERT_EQ(0, ictx->image_watcher->try_lock());

  std::map<rados::cls::lock::locker_id_t,
           rados::cls::lock::locker_info_t> lockers;
  ClsLockType lock_type;
  ASSERT_EQ(0, rados::cls::lock::get_lock_info(&m_ioctx, ictx->header_oid,
                                               RBD_LOCK_NAME, &lockers,
                                               &lock_type, NULL));
  ASSERT_EQ(1U, lockers.size());
  ASSERT_EQ(0, rados::cls::lock::break_lock(&m_ioctx, ictx->header_oid,
					    RBD_LOCK_NAME,
					    lockers.begin()->first.cookie,
					    lockers.begin()->first.locker));

  ASSERT_EQ(0, ictx->image_watcher->unlock());
}

TEST_F(TestImageWatcher, RequestLock) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
			  "auto " + stringify(m_watch_ctx->get_handle())));

  bufferlist bl;
  {
    ENCODE_START(1, 1, bl);
    ::encode(0, bl);
    ENCODE_FINISH(bl);
  }

  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_REQUEST_LOCK, bl));

  m_expected_aio_restarts = 2;
  {
    RWLock::WLocker l(ictx->owner_lock);
    ASSERT_EQ(0, ictx->image_watcher->request_lock(
      boost::bind(&TestImageWatcher::handle_restart_aio, this, ictx, _1),
      create_aio_completion(*ictx)));
    ASSERT_EQ(0, ictx->image_watcher->request_lock(
      boost::bind(&TestImageWatcher::handle_restart_aio, this, ictx, _1),
      create_aio_completion(*ictx)));
  }

  ASSERT_TRUE(wait_for_notifies(*ictx));
  ASSERT_TRUE(m_notify_acks.empty());
  NotifyOpPayloads expected_notify_ops = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_REQUEST_LOCK, bufferlist()));
  ASSERT_EQ(expected_notify_ops, m_notifies);

  ASSERT_EQ(0, unlock_image());

  m_notifies.clear();
  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_RELEASED_LOCK, bufferlist()))(
    std::make_pair(NOTIFY_OP_ACQUIRED_LOCK, bufferlist()));

  bl.clear();
  {
    ENCODE_START(1, 1, bl);
    ::encode(NOTIFY_OP_RELEASED_LOCK, bl);
    ENCODE_FINISH(bl);
  }
  ASSERT_EQ(0, m_ioctx.notify2(ictx->header_oid, bl, 5000, NULL));

  ASSERT_TRUE(wait_for_notifies(*ictx));
  ASSERT_TRUE(m_notify_acks.empty());
  expected_notify_ops = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_RELEASED_LOCK, bufferlist()))(
    std::make_pair(NOTIFY_OP_ACQUIRED_LOCK, bufferlist()));
  ASSERT_EQ(expected_notify_ops, m_notifies);

  ASSERT_TRUE(wait_for_aio_completions(*ictx));
}

TEST_F(TestImageWatcher, RequestLockTimedOut) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
			  "auto " + stringify(m_watch_ctx->get_handle())));

  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_REQUEST_LOCK, bufferlist()));

  m_expected_aio_restarts = 1;
  {
    RWLock::WLocker l(ictx->owner_lock);
    ASSERT_EQ(0, ictx->image_watcher->request_lock(
      boost::bind(&TestImageWatcher::handle_restart_aio, this, ictx, _1),
      create_aio_completion(*ictx)));
  }

  ASSERT_TRUE(wait_for_notifies(*ictx));
  ASSERT_TRUE(m_notify_acks.empty());
  NotifyOpPayloads expected_notify_ops = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_REQUEST_LOCK, bufferlist()));
  ASSERT_EQ(expected_notify_ops, m_notifies);

  ASSERT_TRUE(wait_for_aio_completions(*ictx));
}

TEST_F(TestImageWatcher, RequestLockTryLockRace) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
                          "auto " + stringify(m_watch_ctx->get_handle())));

  bufferlist bl;
  {
    ENCODE_START(1, 1, bl);
    ::encode(0, bl);
    ENCODE_FINISH(bl);
  }

  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_REQUEST_LOCK, bl));

  m_expected_aio_restarts = 1;
  {
    RWLock::WLocker l(ictx->owner_lock);
    ASSERT_EQ(0, ictx->image_watcher->request_lock(
      boost::bind(&TestImageWatcher::handle_restart_aio, this, ictx, _1),
      create_aio_completion(*ictx)));
  }

  ASSERT_TRUE(wait_for_notifies(*ictx));
  ASSERT_TRUE(m_notify_acks.empty());
  NotifyOpPayloads expected_notify_ops = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_REQUEST_LOCK, bufferlist()));
  ASSERT_EQ(expected_notify_ops, m_notifies);

  m_notifies.clear();
  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_RELEASED_LOCK, bufferlist()));

  bl.clear();
  {
    ENCODE_START(1, 1, bl);
    ::encode(NOTIFY_OP_RELEASED_LOCK, bl);
    ENCODE_FINISH(bl);
  }
  ASSERT_EQ(0, m_ioctx.notify2(ictx->header_oid, bl, 5000, NULL));
  ASSERT_TRUE(wait_for_aio_completions(*ictx));
  RWLock::RLocker l(ictx->owner_lock);
  ASSERT_FALSE(ictx->image_watcher->is_lock_owner());
}

TEST_F(TestImageWatcher, RequestLockPreTryLockFailed) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_SHARED, "manually 1234"));

  m_expected_aio_restarts = 3;
  {
    RWLock::WLocker l(ictx->owner_lock);
    ASSERT_EQ(0, ictx->image_watcher->request_lock(
      boost::bind(&TestImageWatcher::handle_restart_aio, this, ictx, _1),
      create_aio_completion(*ictx)));
  }
  ASSERT_TRUE(wait_for_aio_completions(*ictx));
}

TEST_F(TestImageWatcher, RequestLockPostTryLockFailed) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
                          "auto " + stringify(m_watch_ctx->get_handle())));

  bufferlist bl;
  {
    ENCODE_START(1, 1, bl);
    ::encode(0, bl);
    ENCODE_FINISH(bl);
  }

  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_REQUEST_LOCK, bl));

  m_expected_aio_restarts = 1;
  {
    RWLock::WLocker l(ictx->owner_lock);
    ASSERT_EQ(0, ictx->image_watcher->request_lock(
      boost::bind(&TestImageWatcher::handle_restart_aio, this, ictx, _1),
      create_aio_completion(*ictx)));
  }

  ASSERT_TRUE(wait_for_notifies(*ictx));
  ASSERT_TRUE(m_notify_acks.empty());
  NotifyOpPayloads expected_notify_ops = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_REQUEST_LOCK, bufferlist()));
  ASSERT_EQ(expected_notify_ops, m_notifies);

  ASSERT_EQ(0, unlock_image());
  ASSERT_EQ(0, lock_image(*ictx, LOCK_SHARED, "manually 1234"));

  m_notifies.clear();
  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_RELEASED_LOCK, bufferlist()));

  bl.clear();
  {
    ENCODE_START(1, 1, bl);
    ::encode(NOTIFY_OP_RELEASED_LOCK, bl);
    ENCODE_FINISH(bl);
  }
  ASSERT_EQ(0, m_ioctx.notify2(ictx->header_oid, bl, 5000, NULL));
  ASSERT_TRUE(wait_for_aio_completions(*ictx));
}

TEST_F(TestImageWatcher, NotifyHeaderUpdate) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_image_watch(*ictx));

  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_HEADER_UPDATE, bufferlist()));
  librbd::ImageWatcher::notify_header_update(m_ioctx, ictx->header_oid);

  ASSERT_TRUE(wait_for_notifies(*ictx));

  ASSERT_TRUE(m_notify_acks.empty());
  NotifyOpPayloads expected_notify_ops = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_HEADER_UPDATE, bufferlist()));
  ASSERT_EQ(expected_notify_ops, m_notifies);
}
