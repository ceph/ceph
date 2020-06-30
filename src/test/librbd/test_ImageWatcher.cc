// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "include/int_types.h"
#include "include/stringify.h"
#include "include/rados/librados.h"
#include "include/rbd/librbd.hpp"
#include "common/Cond.h"
#include "common/ceph_mutex.h"
#include "common/errno.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "librbd/internal.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/WatchNotifyTypes.h"
#include "librbd/io/AioCompletion.h"
#include "test/librados/test.h"
#include "gtest/gtest.h"
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
using namespace librbd::watch_notify;

void register_test_image_watcher() {
}

class TestImageWatcher : public TestFixture {
public:

  TestImageWatcher() : m_watch_ctx(NULL)
  {
  }

  class WatchCtx : public librados::WatchCtx2 {
  public:
    explicit WatchCtx(TestImageWatcher &parent) : m_parent(parent), m_handle(0) {}

    int watch(const librbd::ImageCtx &ictx) {
      m_header_oid = ictx.header_oid;
      return m_parent.m_ioctx.watch2(m_header_oid, &m_handle, this);
    }

    int unwatch() {
      return m_parent.m_ioctx.unwatch2(m_handle);
    }

    void handle_notify(uint64_t notify_id,
                               uint64_t cookie,
                               uint64_t notifier_id,
                               bufferlist& bl) override {
      try {
	int op;
	bufferlist payload;
	auto iter = bl.cbegin();
	DECODE_START(1, iter);
	decode(op, iter);
	iter.copy_all(payload);
	DECODE_FINISH(iter);

        NotifyOp notify_op = static_cast<NotifyOp>(op);
        /*
	std::cout << "NOTIFY: " << notify_op << ", " << notify_id
		  << ", " << cookie << ", " << notifier_id << std::endl;
        */

	std::lock_guard l{m_parent.m_callback_lock};
        m_parent.m_notify_payloads[notify_op] = payload;

        bufferlist reply;
        if (m_parent.m_notify_acks.count(notify_op) > 0) {
          reply = m_parent.m_notify_acks[notify_op];
	  m_parent.m_notifies += notify_op;
	  m_parent.m_callback_cond.notify_all();
        }

	m_parent.m_ioctx.notify_ack(m_header_oid, notify_id, cookie, reply);
      } catch (...) {
	FAIL();
      }
    }

    void handle_error(uint64_t cookie, int err) override {
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

  void TearDown() override {
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
    std::unique_lock l{m_callback_lock};
    while (m_notifies.size() < m_notify_acks.size()) {
      if (m_callback_cond.wait_for(l, 10s) == std::cv_status::timeout) {
	break;
      }
    }
    return (m_notifies.size() == m_notify_acks.size());
  }

  bufferlist create_response_message(int r) {
    bufferlist bl;
    encode(ResponseMessage(r), bl);
    return bl;
  }

  bool extract_async_request_id(NotifyOp op, AsyncRequestId *id) {
    if (m_notify_payloads.count(op) == 0) {
      return false;
    }

    bufferlist payload = m_notify_payloads[op];
    auto iter = payload.cbegin();

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
    case NOTIFY_OP_SNAP_CREATE:
      {
        SnapCreatePayload payload;
        payload.decode(7, iter);
        *id = payload.async_request_id;
      }
      return true;
    case NOTIFY_OP_REBUILD_OBJECT_MAP:
      {
        RebuildObjectMapPayload payload;
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
    encode(NotifyMessage(new AsyncProgressPayload(id, offset, total)), bl);
    return m_ioctx.notify2(ictx->header_oid, bl, 5000, NULL);
  }

  int notify_async_complete(librbd::ImageCtx *ictx, const AsyncRequestId &id,
                            int r) {
    bufferlist bl;
    encode(NotifyMessage(new AsyncCompletePayload(id, r)), bl);
    return m_ioctx.notify2(ictx->header_oid, bl, 5000, NULL);
  }

  typedef std::map<NotifyOp, bufferlist> NotifyOpPayloads;
  typedef std::set<NotifyOp> NotifyOps;

  WatchCtx *m_watch_ctx;

  NotifyOps m_notifies;
  NotifyOpPayloads m_notify_payloads;
  NotifyOpPayloads m_notify_acks;

  AsyncRequestId m_async_request_id;

  ceph::mutex m_callback_lock = ceph::make_mutex("m_callback_lock");
  ceph::condition_variable m_callback_cond;

};

struct ProgressContext : public librbd::ProgressContext {
  ceph::mutex mutex = ceph::make_mutex("ProgressContext::mutex");
  ceph::condition_variable cond;
  bool received;
  uint64_t offset;
  uint64_t total;

  ProgressContext() : received(false),
                      offset(0), total(0) {}

  int update_progress(uint64_t offset_, uint64_t total_) override {
    std::lock_guard l{mutex};
    offset = offset_;
    total = total_;
    received = true;
    cond.notify_all();
    return 0;
  }

  bool wait(librbd::ImageCtx *ictx, uint64_t offset_, uint64_t total_) {
    std::unique_lock l{mutex};
    while (!received) {
      if (cond.wait_for(l, 10s) == std::cv_status::timeout) {
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
    std::shared_lock l{ictx->owner_lock};
    C_SaferCond ctx;
    ictx->image_watcher->notify_flatten(0, *progress_context, &ctx);
    result = ctx.wait();
  }
};

struct ResizeTask {
  librbd::ImageCtx *ictx;
  ProgressContext *progress_context;
  int result;

  ResizeTask(librbd::ImageCtx *ictx_, ProgressContext *ctx)
    : ictx(ictx_), progress_context(ctx), result(0) {}

  void operator()() {
    std::shared_lock l{ictx->owner_lock};
    C_SaferCond ctx;
    ictx->image_watcher->notify_resize(0, 0, true, *progress_context, &ctx);
    result = ctx.wait();
  }
};

struct SnapCreateTask {
  librbd::ImageCtx *ictx;
  ProgressContext *progress_context;
  int result;

  SnapCreateTask(librbd::ImageCtx *ictx_, ProgressContext *ctx)
    : ictx(ictx_), progress_context(ctx), result(0) {}

  void operator()() {
    std::shared_lock l{ictx->owner_lock};
    C_SaferCond ctx;
    ictx->image_watcher->notify_snap_create(0, cls::rbd::UserSnapshotNamespace(),
                                            "snap", 0, *progress_context, &ctx);
    ASSERT_EQ(0, ctx.wait());
  }
};

struct RebuildObjectMapTask {
  librbd::ImageCtx *ictx;
  ProgressContext *progress_context;
  int result;

  RebuildObjectMapTask(librbd::ImageCtx *ictx_, ProgressContext *ctx)
    : ictx(ictx_), progress_context(ctx), result(0) {}

  void operator()() {
    std::shared_lock l{ictx->owner_lock};
    C_SaferCond ctx;
    ictx->image_watcher->notify_rebuild_object_map(0, *progress_context, &ctx);
    result = ctx.wait();
  }
};

TEST_F(TestImageWatcher, NotifyHeaderUpdate) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_image_watch(*ictx));

  m_notify_acks = {{NOTIFY_OP_HEADER_UPDATE, {}}};
  ictx->notify_update();

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

  m_notify_acks = {{NOTIFY_OP_FLATTEN, create_response_message(0)}};

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

  m_notify_acks = {{NOTIFY_OP_RESIZE, create_response_message(0)}};

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

TEST_F(TestImageWatcher, NotifyRebuildObjectMap) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
        "auto " + stringify(m_watch_ctx->get_handle())));

  m_notify_acks = {{NOTIFY_OP_REBUILD_OBJECT_MAP, create_response_message(0)}};

  ProgressContext progress_context;
  RebuildObjectMapTask rebuild_task(ictx, &progress_context);
  boost::thread thread(boost::ref(rebuild_task));

  ASSERT_TRUE(wait_for_notifies(*ictx));

  NotifyOps expected_notify_ops;
  expected_notify_ops += NOTIFY_OP_REBUILD_OBJECT_MAP;
  ASSERT_EQ(expected_notify_ops, m_notifies);

  AsyncRequestId async_request_id;
  ASSERT_TRUE(extract_async_request_id(NOTIFY_OP_REBUILD_OBJECT_MAP,
                                       &async_request_id));

  ASSERT_EQ(0, notify_async_progress(ictx, async_request_id, 10, 20));
  ASSERT_TRUE(progress_context.wait(ictx, 10, 20));

  ASSERT_EQ(0, notify_async_complete(ictx, async_request_id, 0));

  ASSERT_TRUE(thread.timed_join(boost::posix_time::seconds(10)));
  ASSERT_EQ(0, rebuild_task.result);
}

TEST_F(TestImageWatcher, NotifySnapCreate) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
        "auto " + stringify(m_watch_ctx->get_handle())));

  m_notify_acks = {{NOTIFY_OP_SNAP_CREATE, create_response_message(0)}};

  ProgressContext progress_context;
  SnapCreateTask snap_create_task(ictx, &progress_context);
  boost::thread thread(boost::ref(snap_create_task));

  ASSERT_TRUE(wait_for_notifies(*ictx));

  NotifyOps expected_notify_ops;
  expected_notify_ops += NOTIFY_OP_SNAP_CREATE;
  ASSERT_EQ(expected_notify_ops, m_notifies);

  AsyncRequestId async_request_id;
  ASSERT_TRUE(extract_async_request_id(NOTIFY_OP_SNAP_CREATE,
                                       &async_request_id));

  ASSERT_EQ(0, notify_async_progress(ictx, async_request_id, 1, 10));
  ASSERT_TRUE(progress_context.wait(ictx, 1, 10));

  ASSERT_EQ(0, notify_async_complete(ictx, async_request_id, 0));

  ASSERT_TRUE(thread.timed_join(boost::posix_time::seconds(10)));
  ASSERT_EQ(0, snap_create_task.result);
}

TEST_F(TestImageWatcher, NotifySnapCreateError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
        "auto " + stringify(m_watch_ctx->get_handle())));

  m_notify_acks = {{NOTIFY_OP_SNAP_CREATE, create_response_message(-EEXIST)}};

  std::shared_lock l{ictx->owner_lock};
  C_SaferCond notify_ctx;
  librbd::NoOpProgressContext prog_ctx;
  ictx->image_watcher->notify_snap_create(0, cls::rbd::UserSnapshotNamespace(),
                                          "snap", 0, prog_ctx, &notify_ctx);
  ASSERT_EQ(-EEXIST, notify_ctx.wait());

  NotifyOps expected_notify_ops;
  expected_notify_ops += NOTIFY_OP_SNAP_CREATE;
  ASSERT_EQ(expected_notify_ops, m_notifies);
}

TEST_F(TestImageWatcher, NotifySnapRename) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
        "auto " + stringify(m_watch_ctx->get_handle())));

  m_notify_acks = {{NOTIFY_OP_SNAP_RENAME, create_response_message(0)}};

  std::shared_lock l{ictx->owner_lock};
  C_SaferCond notify_ctx;
  ictx->image_watcher->notify_snap_rename(1, "snap-rename", &notify_ctx);
  ASSERT_EQ(0, notify_ctx.wait());

  NotifyOps expected_notify_ops;
  expected_notify_ops += NOTIFY_OP_SNAP_RENAME;
  ASSERT_EQ(expected_notify_ops, m_notifies);
}

TEST_F(TestImageWatcher, NotifySnapRenameError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
        "auto " + stringify(m_watch_ctx->get_handle())));

  m_notify_acks = {{NOTIFY_OP_SNAP_RENAME, create_response_message(-EEXIST)}};

  std::shared_lock l{ictx->owner_lock};
  C_SaferCond notify_ctx;
  ictx->image_watcher->notify_snap_rename(1, "snap-rename", &notify_ctx);
  ASSERT_EQ(-EEXIST, notify_ctx.wait());

  NotifyOps expected_notify_ops;
  expected_notify_ops += NOTIFY_OP_SNAP_RENAME;
  ASSERT_EQ(expected_notify_ops, m_notifies);
}

TEST_F(TestImageWatcher, NotifySnapRemove) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
        "auto " + stringify(m_watch_ctx->get_handle())));

  m_notify_acks = {{NOTIFY_OP_SNAP_REMOVE, create_response_message(0)}};

  std::shared_lock l{ictx->owner_lock};
  C_SaferCond notify_ctx;
  ictx->image_watcher->notify_snap_remove(cls::rbd::UserSnapshotNamespace(),
					  "snap",
					  &notify_ctx);
  ASSERT_EQ(0, notify_ctx.wait());

  NotifyOps expected_notify_ops;
  expected_notify_ops += NOTIFY_OP_SNAP_REMOVE;
  ASSERT_EQ(expected_notify_ops, m_notifies);
}

TEST_F(TestImageWatcher, NotifySnapProtect) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
        "auto " + stringify(m_watch_ctx->get_handle())));

  m_notify_acks = {{NOTIFY_OP_SNAP_PROTECT, create_response_message(0)}};

  std::shared_lock l{ictx->owner_lock};
  C_SaferCond notify_ctx;
  ictx->image_watcher->notify_snap_protect(cls::rbd::UserSnapshotNamespace(),
					   "snap",
					   &notify_ctx);
  ASSERT_EQ(0, notify_ctx.wait());

  NotifyOps expected_notify_ops;
  expected_notify_ops += NOTIFY_OP_SNAP_PROTECT;
  ASSERT_EQ(expected_notify_ops, m_notifies);
}

TEST_F(TestImageWatcher, NotifySnapUnprotect) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
        "auto " + stringify(m_watch_ctx->get_handle())));

  m_notify_acks = {{NOTIFY_OP_SNAP_UNPROTECT, create_response_message(0)}};

  std::shared_lock l{ictx->owner_lock};
  C_SaferCond notify_ctx;
  ictx->image_watcher->notify_snap_unprotect(cls::rbd::UserSnapshotNamespace(),
					     "snap",
					     &notify_ctx);
  ASSERT_EQ(0, notify_ctx.wait());

  NotifyOps expected_notify_ops;
  expected_notify_ops += NOTIFY_OP_SNAP_UNPROTECT;
  ASSERT_EQ(expected_notify_ops, m_notifies);
}

TEST_F(TestImageWatcher, NotifyRename) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
        "auto " + stringify(m_watch_ctx->get_handle())));

  m_notify_acks = {{NOTIFY_OP_RENAME, create_response_message(0)}};

  std::shared_lock l{ictx->owner_lock};
  C_SaferCond notify_ctx;
  ictx->image_watcher->notify_rename("new_name", &notify_ctx);
  ASSERT_EQ(0, notify_ctx.wait());

  NotifyOps expected_notify_ops;
  expected_notify_ops += NOTIFY_OP_RENAME;
  ASSERT_EQ(expected_notify_ops, m_notifies);
}

TEST_F(TestImageWatcher, NotifyAsyncTimedOut) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
        "auto " + stringify(m_watch_ctx->get_handle())));

  m_notify_acks = {{NOTIFY_OP_FLATTEN, {}}};

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

  m_notify_acks = {{NOTIFY_OP_FLATTEN, create_response_message(-EIO)}};

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

  m_notify_acks = {{NOTIFY_OP_FLATTEN, create_response_message(0)}};

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

  ictx->config.set_val("rbd_request_timed_out_seconds", "0");

  ASSERT_EQ(0, register_image_watch(*ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE,
			  "auto " + stringify(m_watch_ctx->get_handle())));

  m_notify_acks = {{NOTIFY_OP_FLATTEN, create_response_message(0)}};

  ProgressContext progress_context;
  FlattenTask flatten_task(ictx, &progress_context);
  boost::thread thread(boost::ref(flatten_task));

  ASSERT_TRUE(wait_for_notifies(*ictx));

  ASSERT_TRUE(thread.timed_join(boost::posix_time::seconds(10)));
  ASSERT_EQ(-ETIMEDOUT, flatten_task.result);
}

