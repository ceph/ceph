// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "test/librbd/test_fixture.h"
#include "include/int_types.h"
#include "include/stringify.h"
#include "include/rados/librados.h"
#include "include/rbd/librbd.hpp"
#include "common/Cond.h"
#include "common/errno.h"
#include "common/Mutex.h"
#include "common/RWLock.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "librbd/AioCompletion.h"
#include "librbd/internal.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/WatchNotifyTypes.h"
#include "test/librados/test.h"
#include "gtest/gtest.h"
#include <boost/assign/list_of.hpp>
#include <boost/assign/std/set.hpp>
#include <boost/assign/std/map.hpp>
#include <boost/bind.hpp>
#include <boost/scope_exit.hpp>
#include <boost/thread/thread.hpp>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <vector>

using namespace ceph;
using namespace boost::assign;
using namespace librbd::WatchNotify;

void register_test_image_watcher() {
}

class TestImageWatcher : public TestFixture {
public:

  TestImageWatcher() : m_watch_ctx(NULL), m_aio_completion_restarts(0),
		       m_expected_aio_restarts(0),
		       m_callback_lock("m_callback_lock")
  {
  }

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

	m_parent.m_ioctx.notify_ack(m_header_oid, notify_id, cookie, reply);
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

      librados::Rados rados(m_ioctx);
      rados.watch_flush();

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
    while (m_notifies.size() < m_notify_acks.size()) {
      int r = m_callback_cond.WaitInterval(ictx.cct, m_callback_lock,
					 utime_t(10, 0));
      if (r != 0) {
	break;
      }
    }
    return (m_notifies.size() == m_notify_acks.size());
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
        (m_expected_aio_restarts == 0 ||
	 m_aio_completion_restarts < m_expected_aio_restarts)) {
      ictx->image_watcher->request_lock(
        boost::bind(&TestImageWatcher::handle_restart_aio, this, ictx, _1),
	aio_completion);
    } else {
      {
	Mutex::Locker l2(aio_completion->lock);
	aio_completion->complete(ictx->cct);
      }

      m_aio_completions.erase(aio_completion);
      aio_completion->release();
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

  bufferlist create_response_message(int r) {
    bufferlist bl;
    ::encode(ResponseMessage(r), bl);
    return bl;
  }

  bool extract_async_request_id(NotifyOp op, AsyncRequestId *id) {
    if (m_notify_payloads.count(op) == 0) {
      return false;
    }

    bufferlist payload = m_notify_payloads[op];
    bufferlist::iterator iter = payload.begin();

    switch (op) {
    case NOTIFY_OP_FLATTEN:
      {
        FlattenPayload payload;
        payload.decode(2, iter);
        *id = payload.async_request_id;
      }
      return true;
    case NOTIFY_OP_RESIZE:
      {
        ResizePayload payload;
        payload.decode(2, iter);
        *id = payload.async_request_id;
      }
      return true;
    default:
      break;
    }
    return false;
  }

  int notify_async_progress(librbd::ImageCtx *ictx, const AsyncRequestId &id,
                            uint64_t offset, uint64_t total) {
    bufferlist bl;
    ::encode(NotifyMessage(AsyncProgressPayload(id, offset, total)), bl);
    return m_ioctx.notify2(ictx->header_oid, bl, 5000, NULL);
  }

  int notify_async_complete(librbd::ImageCtx *ictx, const AsyncRequestId &id,
                            int r) {
    bufferlist bl;
    ::encode(NotifyMessage(AsyncCompletePayload(id, r)), bl);
    return m_ioctx.notify2(ictx->header_oid, bl, 5000, NULL);
  }

  typedef std::map<NotifyOp, bufferlist> NotifyOpPayloads;
  typedef std::set<NotifyOp> NotifyOps;

  WatchCtx *m_watch_ctx;

  NotifyOps m_notifies;
  NotifyOpPayloads m_notify_payloads;
  NotifyOpPayloads m_notify_acks;

  AsyncRequestId m_async_request_id;

  std::set<librbd::AioCompletion *> m_aio_completions;
  uint32_t m_aio_completion_restarts;
  uint32_t m_expected_aio_restarts;

  Mutex m_callback_lock;
  Cond m_callback_cond;

};

struct ProgressContext : public librbd::ProgressContext {
  Mutex mutex;
  Cond cond;
  bool received;
  uint64_t offset;
  uint64_t total;

  ProgressContext() : mutex("ProgressContext::mutex"), received(false),
                      offset(0), total(0) {}

  virtual int update_progress(uint64_t offset_, uint64_t total_) {
    Mutex::Locker l(mutex);
    offset = offset_;
    total = total_;
    received = true;
    cond.Signal();
    return 0;
  }

  bool wait(librbd::ImageCtx *ictx, uint64_t offset_, uint64_t total_) {
    Mutex::Locker l(mutex);
    while (!received) {
      int r = cond.WaitInterval(ictx->cct, mutex, utime_t(10, 0));
      if (r != 0) {
	break;
      }
    }
    return (received && offset == offset_ && total == total_);
  }
};

struct FlattenTask {
  librbd::ImageCtx *ictx;
  ProgressContext *progress_context;
  int result;

  FlattenTask(librbd::ImageCtx *ictx_, ProgressContext *ctx)
    : ictx(ictx_), progress_context(ctx), result(0) {}

  void operator()() {
    RWLock::RLocker l(ictx->owner_lock);
    result = ictx->image_watcher->notify_flatten(0, *progress_context);
  }
};

struct ResizeTask {
  librbd::ImageCtx *ictx;
  ProgressContext *progress_context;
  int result;

  ResizeTask(librbd::ImageCtx *ictx_, ProgressContext *ctx)
    : ictx(ictx_), progress_context(ctx), result(0) {}

  void operator()() {
    RWLock::RLocker l(ictx->owner_lock);
    result = ictx->image_watcher->notify_resize(0, 0, *progress_context);
  }
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

  {
    RWLock::WLocker l(ictx->owner_lock);
    ASSERT_EQ(0, ictx->image_watcher->try_lock());
    ASSERT_TRUE(ictx->image_watcher->is_lock_owner());
  }

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

  {
    RWLock::WLocker l(ictx->owner_lock);
    ASSERT_EQ(0, ictx->image_watcher->try_lock());
  }

  ASSERT_TRUE(wait_for_notifies(*ictx));

  NotifyOps expected_notify_ops;
  expected_notify_ops += NOTIFY_OP_ACQUIRED_LOCK;
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

  {
    RWLock::WLocker l(ictx->owner_lock);
    ASSERT_EQ(0, ictx->image_watcher->try_lock());
  }
  ASSERT_TRUE(wait_for_notifies(*ictx));

  m_notify_acks += std::make_pair(NOTIFY_OP_RELEASED_LOCK, bufferlist());
  {
    RWLock::WLocker l(ictx->owner_lock);
    ASSERT_EQ(0, ictx->image_watcher->unlock());
  }
  ASSERT_TRUE(wait_for_notifies(*ictx));

  NotifyOps expected_notify_ops;
  expected_notify_ops += NOTIFY_OP_ACQUIRED_LOCK, NOTIFY_OP_RELEASED_LOCK;
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

  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_REQUEST_LOCK, create_response_message(0)));

  {
    RWLock::WLocker l(ictx->owner_lock);
    ictx->image_watcher->request_lock(
      boost::bind(&TestImageWatcher::handle_restart_aio, this, ictx, _1),
      create_aio_completion(*ictx));
    ictx->image_watcher->request_lock(
      boost::bind(&TestImageWatcher::handle_restart_aio, this, ictx, _1),
      create_aio_completion(*ictx));
  }

  ASSERT_TRUE(wait_for_notifies(*ictx));
  NotifyOps expected_notify_ops;
  expected_notify_ops += NOTIFY_OP_REQUEST_LOCK;
  ASSERT_EQ(expected_notify_ops, m_notifies);

  ASSERT_EQ(0, unlock_image());

  m_notifies.clear();
  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_RELEASED_LOCK, bufferlist()))(
    std::make_pair(NOTIFY_OP_ACQUIRED_LOCK, bufferlist()));

  bufferlist bl;
  {
    ENCODE_START(1, 1, bl);
    ::encode(NOTIFY_OP_RELEASED_LOCK, bl);
    ENCODE_FINISH(bl);
  }
  ASSERT_EQ(0, m_ioctx.notify2(ictx->header_oid, bl, 5000, NULL));

  ASSERT_TRUE(wait_for_notifies(*ictx));
  expected_notify_ops.clear();
  expected_notify_ops += NOTIFY_OP_RELEASED_LOCK, NOTIFY_OP_ACQUIRED_LOCK;
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

  {
    RWLock::WLocker l(ictx->owner_lock);
    ictx->image_watcher->request_lock(
      boost::bind(&TestImageWatcher::handle_restart_aio, this, ictx, _1),
      create_aio_completion(*ictx));
  }

  ASSERT_TRUE(wait_for_notifies(*ictx));
  NotifyOps expected_notify_ops;
  expected_notify_ops += NOTIFY_OP_REQUEST_LOCK;
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

  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_REQUEST_LOCK, create_response_message(0)));

  {
    RWLock::WLocker l(ictx->owner_lock);
    ictx->image_watcher->request_lock(
      boost::bind(&TestImageWatcher::handle_restart_aio, this, ictx, _1),
      create_aio_completion(*ictx));
  }

  ASSERT_TRUE(wait_for_notifies(*ictx));
  NotifyOps expected_notify_ops;
  expected_notify_ops += NOTIFY_OP_REQUEST_LOCK;
  ASSERT_EQ(expected_notify_ops, m_notifies);

  m_notifies.clear();
  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_RELEASED_LOCK, bufferlist()));

  bufferlist bl;
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

  {
    RWLock::WLocker l(ictx->owner_lock);
    ictx->image_watcher->request_lock(
      boost::bind(&TestImageWatcher::handle_restart_aio, this, ictx, _1),
      create_aio_completion(*ictx));
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

  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_REQUEST_LOCK, create_response_message(0)));

  m_expected_aio_restarts = 1;
  {
    RWLock::WLocker l(ictx->owner_lock);
    ictx->image_watcher->request_lock(
      boost::bind(&TestImageWatcher::handle_restart_aio, this, ictx, _1),
      create_aio_completion(*ictx));
  }

  ASSERT_TRUE(wait_for_notifies(*ictx));
  NotifyOps expected_notify_ops;
  expected_notify_ops += NOTIFY_OP_REQUEST_LOCK;
  ASSERT_EQ(expected_notify_ops, m_notifies);

  ASSERT_EQ(0, unlock_image());
  ASSERT_EQ(0, lock_image(*ictx, LOCK_SHARED, "manually 1234"));

  m_notifies.clear();
  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_RELEASED_LOCK, bufferlist()));

  bufferlist bl;
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

  NotifyOps expected_notify_ops;
  expected_notify_ops += NOTIFY_OP_HEADER_UPDATE;
  ASSERT_EQ(expected_notify_ops, m_notifies);
}

TEST_F(TestImageWatcher, NotifyFlatten) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
        "auto " + stringify(m_watch_ctx->get_handle())));

  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_FLATTEN, create_response_message(0)));

  ProgressContext progress_context;
  FlattenTask flatten_task(ictx, &progress_context);
  boost::thread thread(boost::ref(flatten_task));

  ASSERT_TRUE(wait_for_notifies(*ictx));

  NotifyOps expected_notify_ops;
  expected_notify_ops += NOTIFY_OP_FLATTEN;
  ASSERT_EQ(expected_notify_ops, m_notifies);

  AsyncRequestId async_request_id;
  ASSERT_TRUE(extract_async_request_id(NOTIFY_OP_FLATTEN, &async_request_id));

  ASSERT_EQ(0, notify_async_progress(ictx, async_request_id, 10, 20));
  ASSERT_TRUE(progress_context.wait(ictx, 10, 20));

  ASSERT_EQ(0, notify_async_complete(ictx, async_request_id, 0));

  ASSERT_TRUE(thread.timed_join(boost::posix_time::seconds(10)));
  ASSERT_EQ(0, flatten_task.result);
}

TEST_F(TestImageWatcher, NotifyResize) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
        "auto " + stringify(m_watch_ctx->get_handle())));

  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_RESIZE, create_response_message(0)));

  ProgressContext progress_context;
  ResizeTask resize_task(ictx, &progress_context);
  boost::thread thread(boost::ref(resize_task));

  ASSERT_TRUE(wait_for_notifies(*ictx));

  NotifyOps expected_notify_ops;
  expected_notify_ops += NOTIFY_OP_RESIZE;
  ASSERT_EQ(expected_notify_ops, m_notifies);

  AsyncRequestId async_request_id;
  ASSERT_TRUE(extract_async_request_id(NOTIFY_OP_RESIZE, &async_request_id));

  ASSERT_EQ(0, notify_async_progress(ictx, async_request_id, 10, 20));
  ASSERT_TRUE(progress_context.wait(ictx, 10, 20));

  ASSERT_EQ(0, notify_async_complete(ictx, async_request_id, 0));

  ASSERT_TRUE(thread.timed_join(boost::posix_time::seconds(10)));
  ASSERT_EQ(0, resize_task.result);
}

TEST_F(TestImageWatcher, NotifySnapCreate) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
        "auto " + stringify(m_watch_ctx->get_handle())));

  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_SNAP_CREATE, create_response_message(0)));

  RWLock::RLocker l(ictx->owner_lock);
  ASSERT_EQ(0, ictx->image_watcher->notify_snap_create("snap"));

  NotifyOps expected_notify_ops;
  expected_notify_ops += NOTIFY_OP_SNAP_CREATE;
  ASSERT_EQ(expected_notify_ops, m_notifies);
}

TEST_F(TestImageWatcher, NotifySnapCreateError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
        "auto " + stringify(m_watch_ctx->get_handle())));

  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_SNAP_CREATE, create_response_message(-EEXIST)));

  RWLock::RLocker l(ictx->owner_lock);
  ASSERT_EQ(-EEXIST, ictx->image_watcher->notify_snap_create("snap"));

  NotifyOps expected_notify_ops;
  expected_notify_ops += NOTIFY_OP_SNAP_CREATE;
  ASSERT_EQ(expected_notify_ops, m_notifies);
}

TEST_F(TestImageWatcher, NotifyAsyncTimedOut) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
        "auto " + stringify(m_watch_ctx->get_handle())));

  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_FLATTEN, bufferlist()));

  ProgressContext progress_context;
  FlattenTask flatten_task(ictx, &progress_context);
  boost::thread thread(boost::ref(flatten_task));

  ASSERT_TRUE(thread.timed_join(boost::posix_time::seconds(10)));
  ASSERT_EQ(-ETIMEDOUT, flatten_task.result);
}

TEST_F(TestImageWatcher, NotifyAsyncError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
        "auto " + stringify(m_watch_ctx->get_handle())));

  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_FLATTEN, create_response_message(-EIO)));

  ProgressContext progress_context;
  FlattenTask flatten_task(ictx, &progress_context);
  boost::thread thread(boost::ref(flatten_task));

  ASSERT_TRUE(thread.timed_join(boost::posix_time::seconds(10)));
  ASSERT_EQ(-EIO, flatten_task.result);
}

TEST_F(TestImageWatcher, NotifyAsyncCompleteError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
        "auto " + stringify(m_watch_ctx->get_handle())));

  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_FLATTEN, create_response_message(0)));

  ProgressContext progress_context;
  FlattenTask flatten_task(ictx, &progress_context);
  boost::thread thread(boost::ref(flatten_task));

  ASSERT_TRUE(wait_for_notifies(*ictx));

  NotifyOps expected_notify_ops;
  expected_notify_ops += NOTIFY_OP_FLATTEN;
  ASSERT_EQ(expected_notify_ops, m_notifies);

  AsyncRequestId async_request_id;
  ASSERT_TRUE(extract_async_request_id(NOTIFY_OP_FLATTEN, &async_request_id));

  ASSERT_EQ(0, notify_async_complete(ictx, async_request_id, -ESHUTDOWN));

  ASSERT_TRUE(thread.timed_join(boost::posix_time::seconds(10)));
  ASSERT_EQ(-ESHUTDOWN, flatten_task.result);
}

TEST_F(TestImageWatcher, NotifyAsyncRequestTimedOut) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  md_config_t *conf = ictx->cct->_conf;
  int timed_out_seconds = conf->rbd_request_timed_out_seconds;
  conf->set_val("rbd_request_timed_out_seconds", "0");
  BOOST_SCOPE_EXIT( (timed_out_seconds)(conf) ) {
    conf->set_val("rbd_request_timed_out_seconds",
                  stringify(timed_out_seconds).c_str());
  } BOOST_SCOPE_EXIT_END;
  ASSERT_EQ(0, conf->rbd_request_timed_out_seconds);

  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
			  "auto " + stringify(m_watch_ctx->get_handle())));

  m_notify_acks = boost::assign::list_of(
    std::make_pair(NOTIFY_OP_FLATTEN, create_response_message(0)));

  ProgressContext progress_context;
  FlattenTask flatten_task(ictx, &progress_context);
  boost::thread thread(boost::ref(flatten_task));

  ASSERT_TRUE(wait_for_notifies(*ictx));

  ASSERT_TRUE(thread.timed_join(boost::posix_time::seconds(10)));
  ASSERT_EQ(-ERESTART, flatten_task.result);
}
