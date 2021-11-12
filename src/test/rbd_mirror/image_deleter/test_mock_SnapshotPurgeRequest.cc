// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_deleter/SnapshotPurgeRequest.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockExclusiveLock.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockImageState.h"
#include "test/librbd/mock/MockOperations.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  static MockTestImageCtx *s_instance;
  static MockTestImageCtx *create(const std::string &image_name,
                                  const std::string &image_id,
                                  const char *snap, librados::IoCtx& p,
                                  bool read_only) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  MockTestImageCtx(librbd::ImageCtx &image_ctx)
      : librbd::MockImageCtx(image_ctx) {
    s_instance = this;
  }
};

MockTestImageCtx *MockTestImageCtx::s_instance = nullptr;

} // anonymous namespace
} // namespace librbd

#include "tools/rbd_mirror/image_deleter/SnapshotPurgeRequest.cc"

namespace rbd {
namespace mirror {
namespace image_deleter {

using ::testing::_;
using ::testing::Invoke;
using ::testing::InSequence;
using ::testing::WithArg;

class TestMockImageDeleterSnapshotPurgeRequest : public TestMockFixture {
public:
  typedef SnapshotPurgeRequest<librbd::MockTestImageCtx> MockSnapshotPurgeRequest;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &m_local_image_ctx));
  }

  void expect_set_journal_policy(librbd::MockTestImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, set_journal_policy(_))
      .WillOnce(Invoke([](librbd::journal::Policy* policy) {
                  delete policy;
                }));
  }

  void expect_open(librbd::MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.state, open(true, _))
      .WillOnce(WithArg<1>(Invoke([this, &mock_image_ctx, r](Context* ctx) {
                             EXPECT_EQ(0U, mock_image_ctx.read_only_mask &
                                             librbd::IMAGE_READ_ONLY_FLAG_NON_PRIMARY);
                             m_threads->work_queue->queue(ctx, r);
                           })));
  }

  void expect_close(librbd::MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.state, close(_))
      .WillOnce(Invoke([this, r](Context* ctx) {
                  m_threads->work_queue->queue(ctx, r);
                }));
  }

  void expect_acquire_lock(librbd::MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.exclusive_lock, acquire_lock(_))
      .WillOnce(Invoke([this, r](Context* ctx) {
                  m_threads->work_queue->queue(ctx, r);
                }));
  }

  void expect_get_snap_namespace(librbd::MockTestImageCtx &mock_image_ctx,
                                 uint64_t snap_id,
                                 const cls::rbd::SnapshotNamespace &snap_namespace,
                                 int r) {
    EXPECT_CALL(mock_image_ctx, get_snap_namespace(snap_id, _))
      .WillOnce(WithArg<1>(Invoke([snap_namespace, r](cls::rbd::SnapshotNamespace *ns) {
                             *ns = snap_namespace;
                             return r;
                           })));
  }

  void expect_get_snap_name(librbd::MockTestImageCtx &mock_image_ctx,
                            uint64_t snap_id, const std::string& name,
                            int r) {
    EXPECT_CALL(mock_image_ctx, get_snap_name(snap_id, _))
      .WillOnce(WithArg<1>(Invoke([name, r](std::string *n) {
                             *n = name;
                             return r;
                           })));
  }

  void expect_is_snap_protected(librbd::MockTestImageCtx &mock_image_ctx,
                                uint64_t snap_id, bool is_protected, int r) {
    EXPECT_CALL(mock_image_ctx, is_snap_protected(snap_id, _))
      .WillOnce(WithArg<1>(Invoke([is_protected, r](bool *prot) {
                             *prot = is_protected;
                             return r;
                           })));
  }

  void expect_snap_unprotect(librbd::MockTestImageCtx &mock_image_ctx,
                             const cls::rbd::SnapshotNamespace& ns,
                             const std::string& name, int r) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_snap_unprotect(ns, name, _))
      .WillOnce(WithArg<2>(Invoke([this, r](Context* ctx) {
                             m_threads->work_queue->queue(ctx, r);
                           })));
  }

  void expect_snap_remove(librbd::MockTestImageCtx &mock_image_ctx,
                          const cls::rbd::SnapshotNamespace& ns,
                          const std::string& name, int r) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_snap_remove(ns, name, _))
      .WillOnce(WithArg<2>(Invoke([this, r](Context* ctx) {
                             m_threads->work_queue->queue(ctx, r);
                           })));
  }

  void expect_start_op(librbd::MockTestImageCtx &mock_image_ctx, bool success) {
    EXPECT_CALL(*mock_image_ctx.exclusive_lock, start_op(_))
      .WillOnce(Invoke([success](int* r) {
                  auto f = [](int r) {};
                  if (!success) {
                    *r = -EROFS;
                    return static_cast<LambdaContext<decltype(f)>*>(nullptr);
                  }
                  return new LambdaContext(std::move(f));
                }));
  }

  librbd::ImageCtx *m_local_image_ctx;
};

TEST_F(TestMockImageDeleterSnapshotPurgeRequest, SuccessJournal) {
  {
    std::unique_lock image_locker{m_local_image_ctx->image_lock};
    m_local_image_ctx->add_snap(cls::rbd::UserSnapshotNamespace{}, "snap1", 1,
                                0, {}, RBD_PROTECTION_STATUS_PROTECTED, 0, {});
    m_local_image_ctx->add_snap(cls::rbd::UserSnapshotNamespace{}, "snap2", 2,
                                0, {}, RBD_PROTECTION_STATUS_UNPROTECTED, 0,
                                {});
  }

  librbd::MockTestImageCtx mock_image_ctx(*m_local_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_set_journal_policy(mock_image_ctx);
  expect_open(mock_image_ctx, 0);
  expect_acquire_lock(mock_image_ctx, 0);

  expect_get_snap_namespace(mock_image_ctx, 2,
                            cls::rbd::UserSnapshotNamespace{}, 0);
  expect_get_snap_name(mock_image_ctx, 2, "snap2", 0);
  expect_is_snap_protected(mock_image_ctx, 2, false, 0);
  expect_start_op(mock_image_ctx, true);
  expect_snap_remove(mock_image_ctx, cls::rbd::UserSnapshotNamespace{}, "snap2",
                     0);

  expect_get_snap_namespace(mock_image_ctx, 1,
                            cls::rbd::UserSnapshotNamespace{}, 0);
  expect_get_snap_name(mock_image_ctx, 1, "snap1", 0);
  expect_is_snap_protected(mock_image_ctx, 1, true, 0);
  expect_start_op(mock_image_ctx, true);
  expect_snap_unprotect(mock_image_ctx, cls::rbd::UserSnapshotNamespace{},
                        "snap1", 0);
  expect_start_op(mock_image_ctx, true);
  expect_snap_remove(mock_image_ctx, cls::rbd::UserSnapshotNamespace{}, "snap1",
                     0);

  expect_close(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = MockSnapshotPurgeRequest::create(m_local_io_ctx, mock_image_ctx.id,
                                              &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDeleterSnapshotPurgeRequest, SuccessSnapshot) {
  {
    std::unique_lock image_locker{m_local_image_ctx->image_lock};
    m_local_image_ctx->add_snap(cls::rbd::UserSnapshotNamespace{}, "snap1", 1,
                                0, {}, RBD_PROTECTION_STATUS_PROTECTED, 0, {});
    m_local_image_ctx->add_snap(cls::rbd::UserSnapshotNamespace{}, "snap2", 2,
                                0, {}, RBD_PROTECTION_STATUS_UNPROTECTED, 0,
                                {});
  }

  librbd::MockTestImageCtx mock_image_ctx(*m_local_image_ctx);

  InSequence seq;
  expect_set_journal_policy(mock_image_ctx);
  expect_open(mock_image_ctx, 0);

  expect_get_snap_namespace(mock_image_ctx, 2,
                            cls::rbd::UserSnapshotNamespace{}, 0);
  expect_get_snap_name(mock_image_ctx, 2, "snap2", 0);
  expect_is_snap_protected(mock_image_ctx, 2, false, 0);
  expect_snap_remove(mock_image_ctx, cls::rbd::UserSnapshotNamespace{}, "snap2",
                     0);

  expect_get_snap_namespace(mock_image_ctx, 1,
                            cls::rbd::UserSnapshotNamespace{}, 0);
  expect_get_snap_name(mock_image_ctx, 1, "snap1", 0);
  expect_is_snap_protected(mock_image_ctx, 1, true, 0);
  expect_snap_unprotect(mock_image_ctx, cls::rbd::UserSnapshotNamespace{},
                        "snap1", 0);
  expect_snap_remove(mock_image_ctx, cls::rbd::UserSnapshotNamespace{}, "snap1",
                     0);

  expect_close(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = MockSnapshotPurgeRequest::create(m_local_io_ctx, mock_image_ctx.id,
                                              &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDeleterSnapshotPurgeRequest, OpenError) {
  {
    std::unique_lock image_locker{m_local_image_ctx->image_lock};
    m_local_image_ctx->add_snap(cls::rbd::UserSnapshotNamespace{}, "snap1", 1,
                                0, {}, RBD_PROTECTION_STATUS_UNPROTECTED, 0,
                                {});
  }

  librbd::MockTestImageCtx mock_image_ctx(*m_local_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_set_journal_policy(mock_image_ctx);
  expect_open(mock_image_ctx, -EPERM);

  C_SaferCond ctx;
  auto req = MockSnapshotPurgeRequest::create(m_local_io_ctx, mock_image_ctx.id,
                                              &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockImageDeleterSnapshotPurgeRequest, AcquireLockError) {
  {
    std::unique_lock image_locker{m_local_image_ctx->image_lock};
    m_local_image_ctx->add_snap(cls::rbd::UserSnapshotNamespace{}, "snap1", 1,
                                0, {}, RBD_PROTECTION_STATUS_UNPROTECTED, 0,
                                {});
  }

  librbd::MockTestImageCtx mock_image_ctx(*m_local_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_set_journal_policy(mock_image_ctx);
  expect_open(mock_image_ctx, 0);
  expect_acquire_lock(mock_image_ctx, -EPERM);
  expect_close(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  auto req = MockSnapshotPurgeRequest::create(m_local_io_ctx, mock_image_ctx.id,
                                              &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockImageDeleterSnapshotPurgeRequest, SnapUnprotectBusy) {
  {
    std::unique_lock image_locker{m_local_image_ctx->image_lock};
    m_local_image_ctx->add_snap(cls::rbd::UserSnapshotNamespace{}, "snap1", 1,
                                0, {}, RBD_PROTECTION_STATUS_PROTECTED, 0, {});
  }

  librbd::MockTestImageCtx mock_image_ctx(*m_local_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_set_journal_policy(mock_image_ctx);
  expect_open(mock_image_ctx, 0);
  expect_acquire_lock(mock_image_ctx, 0);

  expect_get_snap_namespace(mock_image_ctx, 1,
                            cls::rbd::UserSnapshotNamespace{}, 0);
  expect_get_snap_name(mock_image_ctx, 1, "snap1", 0);
  expect_is_snap_protected(mock_image_ctx, 1, true, 0);
  expect_start_op(mock_image_ctx, true);
  expect_snap_unprotect(mock_image_ctx, cls::rbd::UserSnapshotNamespace{},
                        "snap1", -EBUSY);

  expect_close(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  auto req = MockSnapshotPurgeRequest::create(m_local_io_ctx, mock_image_ctx.id,
                                              &ctx);
  req->send();
  ASSERT_EQ(-EBUSY, ctx.wait());
}

TEST_F(TestMockImageDeleterSnapshotPurgeRequest, SnapUnprotectError) {
  {
    std::unique_lock image_locker{m_local_image_ctx->image_lock};
    m_local_image_ctx->add_snap(cls::rbd::UserSnapshotNamespace{}, "snap1", 1,
                                0, {}, RBD_PROTECTION_STATUS_PROTECTED, 0, {});
  }

  librbd::MockTestImageCtx mock_image_ctx(*m_local_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_set_journal_policy(mock_image_ctx);
  expect_open(mock_image_ctx, 0);
  expect_acquire_lock(mock_image_ctx, 0);

  expect_get_snap_namespace(mock_image_ctx, 1,
                            cls::rbd::UserSnapshotNamespace{}, 0);
  expect_get_snap_name(mock_image_ctx, 1, "snap1", 0);
  expect_is_snap_protected(mock_image_ctx, 1, true, 0);
  expect_start_op(mock_image_ctx, true);
  expect_snap_unprotect(mock_image_ctx, cls::rbd::UserSnapshotNamespace{},
                        "snap1", -EPERM);

  expect_close(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  auto req = MockSnapshotPurgeRequest::create(m_local_io_ctx, mock_image_ctx.id,
                                              &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockImageDeleterSnapshotPurgeRequest, SnapRemoveError) {
  {
    std::unique_lock image_locker{m_local_image_ctx->image_lock};
    m_local_image_ctx->add_snap(cls::rbd::UserSnapshotNamespace{}, "snap1", 1,
                                0, {}, RBD_PROTECTION_STATUS_UNPROTECTED, 0,
                                {});
  }

  librbd::MockTestImageCtx mock_image_ctx(*m_local_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_set_journal_policy(mock_image_ctx);
  expect_open(mock_image_ctx, 0);
  expect_acquire_lock(mock_image_ctx, 0);

  expect_get_snap_namespace(mock_image_ctx, 1,
                            cls::rbd::UserSnapshotNamespace{}, 0);
  expect_get_snap_name(mock_image_ctx, 1, "snap1", 0);
  expect_is_snap_protected(mock_image_ctx, 1, false, 0);
  expect_start_op(mock_image_ctx, true);
  expect_snap_remove(mock_image_ctx, cls::rbd::UserSnapshotNamespace{}, "snap1",
                     -EINVAL);

  expect_close(mock_image_ctx, -EPERM);

  C_SaferCond ctx;
  auto req = MockSnapshotPurgeRequest::create(m_local_io_ctx, mock_image_ctx.id,
                                              &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageDeleterSnapshotPurgeRequest, CloseError) {
  {
    std::unique_lock image_locker{m_local_image_ctx->image_lock};
    m_local_image_ctx->add_snap(cls::rbd::UserSnapshotNamespace{}, "snap1", 1,
                                0, {}, RBD_PROTECTION_STATUS_UNPROTECTED, 0,
                                {});
  }

  librbd::MockTestImageCtx mock_image_ctx(*m_local_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_set_journal_policy(mock_image_ctx);
  expect_open(mock_image_ctx, 0);
  expect_acquire_lock(mock_image_ctx, 0);

  expect_get_snap_namespace(mock_image_ctx, 1,
                            cls::rbd::UserSnapshotNamespace{}, 0);
  expect_get_snap_name(mock_image_ctx, 1, "snap1", 0);
  expect_is_snap_protected(mock_image_ctx, 1, false, 0);
  expect_start_op(mock_image_ctx, true);
  expect_snap_remove(mock_image_ctx, cls::rbd::UserSnapshotNamespace{}, "snap1",
                     0);

  expect_close(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  auto req = MockSnapshotPurgeRequest::create(m_local_io_ctx, mock_image_ctx.id,
                                              &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace image_deleter
} // namespace mirror
} // namespace rbd
