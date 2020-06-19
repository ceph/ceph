// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/deep_copy/SetHeadRequest.h"
#include "librbd/deep_copy/SnapshotCopyRequest.h"
#include "librbd/deep_copy/SnapshotCreateRequest.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/test_support.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  explicit MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace deep_copy {

template <>
class SetHeadRequest<librbd::MockTestImageCtx> {
public:
  static SetHeadRequest* s_instance;
  Context *on_finish;

  static SetHeadRequest* create(librbd::MockTestImageCtx *image_ctx,
                                uint64_t size,
                                const cls::rbd::ParentImageSpec &parent_spec,
                                uint64_t parent_overlap, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  SetHeadRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

template <>
struct SnapshotCreateRequest<librbd::MockTestImageCtx> {
  static SnapshotCreateRequest* s_instance;
  static SnapshotCreateRequest* create(librbd::MockTestImageCtx* image_ctx,
                                       const std::string &snap_name,
                                       const cls::rbd::SnapshotNamespace &snap_namespace,
                                       uint64_t size,
                                       const cls::rbd::ParentImageSpec &parent_spec,
                                       uint64_t parent_overlap,
                                       Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  Context *on_finish = nullptr;

  SnapshotCreateRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

SetHeadRequest<librbd::MockTestImageCtx>* SetHeadRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
SnapshotCreateRequest<librbd::MockTestImageCtx>* SnapshotCreateRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace deep_copy
} // namespace librbd

// template definitions
#include "librbd/deep_copy/SnapshotCopyRequest.cc"
template class librbd::deep_copy::SnapshotCopyRequest<librbd::MockTestImageCtx>;

namespace librbd {
namespace deep_copy {

using ::testing::_;
using ::testing::DoAll;
using ::testing::DoDefault;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::InvokeWithoutArgs;
using ::testing::Return;
using ::testing::ReturnNew;
using ::testing::SetArgPointee;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockDeepCopySnapshotCopyRequest : public TestMockFixture {
public:
  typedef SetHeadRequest<librbd::MockTestImageCtx> MockSetHeadRequest;
  typedef SnapshotCopyRequest<librbd::MockTestImageCtx> MockSnapshotCopyRequest;
  typedef SnapshotCreateRequest<librbd::MockTestImageCtx> MockSnapshotCreateRequest;

  librbd::ImageCtx *m_src_image_ctx;
  librbd::ImageCtx *m_dst_image_ctx;
  asio::ContextWQ *m_work_queue;

  librbd::SnapSeqs m_snap_seqs;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &m_src_image_ctx));

    librbd::RBD rbd;
    std::string dst_image_name = get_temp_image_name();
    ASSERT_EQ(0, create_image_pp(rbd, m_ioctx, dst_image_name, m_image_size));
    ASSERT_EQ(0, open_image(dst_image_name, &m_dst_image_ctx));

    librbd::ImageCtx::get_work_queue(m_src_image_ctx->cct, &m_work_queue);
  }

  void prepare_exclusive_lock(librbd::MockImageCtx &mock_image_ctx,
                              librbd::MockExclusiveLock &mock_exclusive_lock) {
    if ((mock_image_ctx.features & RBD_FEATURE_EXCLUSIVE_LOCK) == 0) {
      return;
    }
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  void expect_test_features(librbd::MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, test_features(_, _))
      .WillRepeatedly(WithArg<0>(Invoke([&mock_image_ctx](uint64_t features) {
              return (mock_image_ctx.features & features) != 0;
            })));
    EXPECT_CALL(mock_image_ctx, test_features(_))
      .WillRepeatedly(WithArg<0>(Invoke([&mock_image_ctx](uint64_t features) {
              return (mock_image_ctx.features & features) != 0;
            })));
  }

  void expect_start_op(librbd::MockExclusiveLock &mock_exclusive_lock) {
    if ((m_src_image_ctx->features & RBD_FEATURE_EXCLUSIVE_LOCK) == 0) {
      return;
    }
    EXPECT_CALL(mock_exclusive_lock, start_op(_)).WillOnce(Return(new LambdaContext([](int){})));
  }

  void expect_get_snap_namespace(librbd::MockTestImageCtx &mock_image_ctx,
                                 uint64_t snap_id) {
    EXPECT_CALL(mock_image_ctx, get_snap_namespace(snap_id, _))
      .WillOnce(Invoke([&mock_image_ctx](uint64_t snap_id,
                                         cls::rbd::SnapshotNamespace* snap_ns) {
                  auto it = mock_image_ctx.snap_info.find(snap_id);
                  *snap_ns = it->second.snap_namespace;
                  return 0;
                }));
  }

  void expect_snap_create(librbd::MockTestImageCtx &mock_image_ctx,
                          MockSnapshotCreateRequest &mock_snapshot_create_request,
                          const std::string &snap_name, uint64_t snap_id, int r) {
    EXPECT_CALL(mock_snapshot_create_request, send())
      .WillOnce(DoAll(Invoke([&mock_image_ctx, snap_id, snap_name]() {
                        inject_snap(mock_image_ctx, snap_id, snap_name);
                      }),
                      Invoke([this, &mock_snapshot_create_request, r]() {
                        m_work_queue->queue(mock_snapshot_create_request.on_finish, r);
                      })));
  }

  void expect_snap_remove(librbd::MockTestImageCtx &mock_image_ctx,
                          const std::string &snap_name, int r) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_snap_remove(_, StrEq(snap_name), _))
                  .WillOnce(WithArg<2>(Invoke([this, r](Context *ctx) {
                              m_work_queue->queue(ctx, r);
                            })));
  }

  void expect_snap_protect(librbd::MockTestImageCtx &mock_image_ctx,
                           const std::string &snap_name, int r) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_snap_protect(_, StrEq(snap_name), _))
                  .WillOnce(WithArg<2>(Invoke([this, r](Context *ctx) {
                              m_work_queue->queue(ctx, r);
                            })));
  }

  void expect_snap_unprotect(librbd::MockTestImageCtx &mock_image_ctx,
                             const std::string &snap_name, int r) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_snap_unprotect(_, StrEq(snap_name), _))
                  .WillOnce(WithArg<2>(Invoke([this, r](Context *ctx) {
                              m_work_queue->queue(ctx, r);
                            })));
  }

  void expect_snap_is_protected(librbd::MockTestImageCtx &mock_image_ctx,
                                uint64_t snap_id, bool is_protected, int r) {
    EXPECT_CALL(mock_image_ctx, is_snap_protected(snap_id, _))
                  .WillOnce(DoAll(SetArgPointee<1>(is_protected),
                                  Return(r)));
  }

  void expect_snap_is_unprotected(librbd::MockTestImageCtx &mock_image_ctx,
                                  uint64_t snap_id, bool is_unprotected, int r) {
    EXPECT_CALL(mock_image_ctx, is_snap_unprotected(snap_id, _))
                  .WillOnce(DoAll(SetArgPointee<1>(is_unprotected),
                                  Return(r)));
  }

  void expect_set_head(MockSetHeadRequest &mock_set_head_request, int r) {
    EXPECT_CALL(mock_set_head_request, send())
      .WillOnce(Invoke([&mock_set_head_request, r]() {
            mock_set_head_request.on_finish->complete(r);
          }));
  }

  static void inject_snap(librbd::MockTestImageCtx &mock_image_ctx,
                          uint64_t snap_id, const std::string &snap_name) {
    mock_image_ctx.snap_ids[{cls::rbd::UserSnapshotNamespace(),
			     snap_name}] = snap_id;
  }

  MockSnapshotCopyRequest *create_request(
      librbd::MockTestImageCtx &mock_src_image_ctx,
      librbd::MockTestImageCtx &mock_dst_image_ctx,
      librados::snap_t src_snap_id_start,
      librados::snap_t src_snap_id_end,
      librados::snap_t dst_snap_id_start,
      Context *on_finish) {
    return new MockSnapshotCopyRequest(&mock_src_image_ctx, &mock_dst_image_ctx,
                                       src_snap_id_start, src_snap_id_end,
                                       dst_snap_id_start, false, m_work_queue,
                                       &m_snap_seqs, on_finish);
  }

  int create_snap(librbd::ImageCtx *image_ctx,
                  const cls::rbd::SnapshotNamespace& snap_ns,
                  const std::string &snap_name, bool protect) {
    NoOpProgressContext prog_ctx;
    int r = image_ctx->operations->snap_create(snap_ns, snap_name.c_str(), 0,
                                               prog_ctx);
    if (r < 0) {
      return r;
    }

    if (protect) {
      EXPECT_TRUE(boost::get<cls::rbd::UserSnapshotNamespace>(&snap_ns) !=
                    nullptr);
      r = image_ctx->operations->snap_protect(snap_ns, snap_name.c_str());
      if (r < 0) {
        return r;
      }
    }

    r = image_ctx->state->refresh();
    if (r < 0) {
      return r;
    }
    return 0;
  }

  int create_snap(librbd::ImageCtx *image_ctx, const std::string &snap_name,
                  bool protect = false) {
    return create_snap(image_ctx, cls::rbd::UserSnapshotNamespace{}, snap_name,
                       protect);
  }

  void validate_snap_seqs(const librbd::SnapSeqs &snap_seqs) {
    ASSERT_EQ(snap_seqs, m_snap_seqs);
  }
};

TEST_F(TestMockDeepCopySnapshotCopyRequest, Empty) {
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockSetHeadRequest mock_set_head_request;

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_set_head(mock_set_head_request, 0);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_src_image_ctx,
                                                    mock_dst_image_ctx, 0,
                                                    CEPH_NOSNAP, 0, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());

  validate_snap_seqs({});
}

TEST_F(TestMockDeepCopySnapshotCopyRequest, SnapCreate) {
  ASSERT_EQ(0, create_snap(m_src_image_ctx, "snap1"));
  ASSERT_EQ(0, create_snap(m_src_image_ctx, "snap2"));

  uint64_t src_snap_id1 = m_src_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];
  uint64_t src_snap_id2 = m_src_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap2"}];

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockSnapshotCreateRequest mock_snapshot_create_request;
  MockSetHeadRequest mock_set_head_request;

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_get_snap_namespace(mock_src_image_ctx, src_snap_id1);
  expect_start_op(mock_exclusive_lock);
  expect_snap_create(mock_dst_image_ctx, mock_snapshot_create_request, "snap1", 12, 0);
  expect_get_snap_namespace(mock_src_image_ctx, src_snap_id2);
  expect_start_op(mock_exclusive_lock);
  expect_snap_create(mock_dst_image_ctx, mock_snapshot_create_request, "snap2", 14, 0);
  expect_snap_is_protected(mock_src_image_ctx, src_snap_id1, false, 0);
  expect_snap_is_protected(mock_src_image_ctx, src_snap_id2, false, 0);
  expect_set_head(mock_set_head_request, 0);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_src_image_ctx,
                                                    mock_dst_image_ctx, 0,
                                                    CEPH_NOSNAP, 0, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());

  validate_snap_seqs({{src_snap_id1, 12}, {src_snap_id2, 14}});
}

TEST_F(TestMockDeepCopySnapshotCopyRequest, SnapCreateError) {
  ASSERT_EQ(0, create_snap(m_src_image_ctx, "snap1"));

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockSnapshotCreateRequest mock_snapshot_create_request;

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  uint64_t src_snap_id1 = mock_src_image_ctx.snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];

  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_get_snap_namespace(mock_src_image_ctx, src_snap_id1);
  expect_start_op(mock_exclusive_lock);
  expect_snap_create(mock_dst_image_ctx, mock_snapshot_create_request, "snap1",
                     12, -EINVAL);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_src_image_ctx,
                                                    mock_dst_image_ctx, 0,
                                                    CEPH_NOSNAP, 0, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockDeepCopySnapshotCopyRequest, SnapCreateCancel) {
  ASSERT_EQ(0, create_snap(m_src_image_ctx, "snap1"));

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockSnapshotCreateRequest mock_snapshot_create_request;

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  uint64_t src_snap_id1 = m_src_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];
  expect_get_snap_namespace(mock_src_image_ctx, src_snap_id1);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_src_image_ctx,
                                                    mock_dst_image_ctx, 0,
                                                    CEPH_NOSNAP, 0, &ctx);
  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_start_op(mock_exclusive_lock);
  EXPECT_CALL(mock_snapshot_create_request, send())
    .WillOnce(DoAll(InvokeWithoutArgs([request]() {
	    request->cancel();
	  }),
	Invoke([this, &mock_snapshot_create_request]() {
	    m_work_queue->queue(mock_snapshot_create_request.on_finish, 0);
	  })));

  request->send();
  ASSERT_EQ(-ECANCELED, ctx.wait());
}

TEST_F(TestMockDeepCopySnapshotCopyRequest, SnapRemoveAndCreate) {
  ASSERT_EQ(0, create_snap(m_src_image_ctx, "snap1"));
  ASSERT_EQ(0, create_snap(m_dst_image_ctx, "snap1"));

  uint64_t src_snap_id1 = m_src_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];
  uint64_t dst_snap_id1 = m_dst_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockSnapshotCreateRequest mock_snapshot_create_request;
  MockSetHeadRequest mock_set_head_request;

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_snap_is_unprotected(mock_dst_image_ctx,
                             m_dst_image_ctx->snap_ids[
                               {cls::rbd::UserSnapshotNamespace(), "snap1"}],
                             true, 0);
  expect_get_snap_namespace(mock_dst_image_ctx, dst_snap_id1);
  expect_start_op(mock_exclusive_lock);
  expect_snap_remove(mock_dst_image_ctx, "snap1", 0);
  expect_get_snap_namespace(mock_src_image_ctx, src_snap_id1);
  expect_start_op(mock_exclusive_lock);
  expect_snap_create(mock_dst_image_ctx, mock_snapshot_create_request, "snap1", 12, 0);
  expect_snap_is_protected(mock_src_image_ctx, src_snap_id1, false, 0);
  expect_set_head(mock_set_head_request, 0);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_src_image_ctx,
                                                    mock_dst_image_ctx, 0,
                                                    CEPH_NOSNAP, 0, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());

  validate_snap_seqs({{src_snap_id1, 12}});
}

TEST_F(TestMockDeepCopySnapshotCopyRequest, SnapRemoveError) {
  ASSERT_EQ(0, create_snap(m_dst_image_ctx, "snap1"));

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  uint64_t dst_snap_id1 = m_dst_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];

  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_snap_is_unprotected(mock_dst_image_ctx,
                             m_dst_image_ctx->snap_ids[
                               {cls::rbd::UserSnapshotNamespace(), "snap1"}],
                             true, 0);
  expect_get_snap_namespace(mock_dst_image_ctx, dst_snap_id1);
  expect_start_op(mock_exclusive_lock);
  expect_snap_remove(mock_dst_image_ctx, "snap1", -EINVAL);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_src_image_ctx,
                                                    mock_dst_image_ctx, 0,
                                                    CEPH_NOSNAP, 0, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockDeepCopySnapshotCopyRequest, SnapUnprotect) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  ASSERT_EQ(0, create_snap(m_src_image_ctx, "snap1", true));
  ASSERT_EQ(0, create_snap(m_dst_image_ctx, "snap1", true));

  uint64_t src_snap_id1 = m_src_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];
  uint64_t dst_snap_id1 = m_dst_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];
  m_snap_seqs[src_snap_id1] = dst_snap_id1;

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockSetHeadRequest mock_set_head_request;

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_snap_is_unprotected(mock_dst_image_ctx, dst_snap_id1, false, 0);
  expect_snap_is_unprotected(mock_src_image_ctx, src_snap_id1, true, 0);
  expect_start_op(mock_exclusive_lock);
  expect_snap_unprotect(mock_dst_image_ctx, "snap1", 0);
  expect_get_snap_namespace(mock_dst_image_ctx, dst_snap_id1);
  expect_get_snap_namespace(mock_src_image_ctx, src_snap_id1);
  expect_snap_is_protected(mock_src_image_ctx, src_snap_id1, false, 0);
  expect_set_head(mock_set_head_request, 0);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_src_image_ctx,
                                                    mock_dst_image_ctx, 0,
                                                    CEPH_NOSNAP, 0, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());

  validate_snap_seqs({{src_snap_id1, dst_snap_id1}});
}

TEST_F(TestMockDeepCopySnapshotCopyRequest, SnapUnprotectError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  ASSERT_EQ(0, create_snap(m_src_image_ctx, "snap1", true));
  ASSERT_EQ(0, create_snap(m_dst_image_ctx, "snap1", true));

  uint64_t src_snap_id1 = m_src_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];
  uint64_t dst_snap_id1 = m_dst_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];
  m_snap_seqs[src_snap_id1] = dst_snap_id1;

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_snap_is_unprotected(mock_dst_image_ctx, dst_snap_id1, false, 0);
  expect_snap_is_unprotected(mock_src_image_ctx, src_snap_id1, true, 0);
  expect_start_op(mock_exclusive_lock);
  expect_snap_unprotect(mock_dst_image_ctx, "snap1", -EBUSY);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_src_image_ctx,
                                                    mock_dst_image_ctx, 0,
                                                    CEPH_NOSNAP, 0, &ctx);
  request->send();
  ASSERT_EQ(-EBUSY, ctx.wait());
}

TEST_F(TestMockDeepCopySnapshotCopyRequest, SnapUnprotectCancel) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  ASSERT_EQ(0, create_snap(m_src_image_ctx, "snap1", true));
  ASSERT_EQ(0, create_snap(m_dst_image_ctx, "snap1", true));

  uint64_t src_snap_id1 = m_src_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];
  uint64_t dst_snap_id1 = m_dst_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];
  m_snap_seqs[src_snap_id1] = dst_snap_id1;

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_src_image_ctx,
                                                    mock_dst_image_ctx, 0,
                                                    CEPH_NOSNAP, 0, &ctx);
  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_snap_is_unprotected(mock_dst_image_ctx, dst_snap_id1, false, 0);
  expect_snap_is_unprotected(mock_src_image_ctx, src_snap_id1, true, 0);
  expect_start_op(mock_exclusive_lock);
  EXPECT_CALL(*mock_dst_image_ctx.operations,
	      execute_snap_unprotect(_, StrEq("snap1"), _))
    .WillOnce(DoAll(InvokeWithoutArgs([request]() {
	    request->cancel();
	  }),
	WithArg<2>(Invoke([this](Context *ctx) {
	    m_work_queue->queue(ctx, 0);
	    }))));

  request->send();
  ASSERT_EQ(-ECANCELED, ctx.wait());
}

TEST_F(TestMockDeepCopySnapshotCopyRequest, SnapUnprotectRemove) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  ASSERT_EQ(0, create_snap(m_src_image_ctx, "snap1", true));
  ASSERT_EQ(0, create_snap(m_dst_image_ctx, "snap1", true));

  uint64_t src_snap_id1 = m_src_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];
  uint64_t dst_snap_id1 = m_dst_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockSnapshotCreateRequest mock_snapshot_create_request;
  MockSetHeadRequest mock_set_head_request;

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_snap_is_unprotected(mock_dst_image_ctx,
                             m_dst_image_ctx->snap_ids[
                               {cls::rbd::UserSnapshotNamespace(), "snap1"}],
                             false, 0);
  expect_start_op(mock_exclusive_lock);
  expect_snap_unprotect(mock_dst_image_ctx, "snap1", 0);
  expect_get_snap_namespace(mock_dst_image_ctx, dst_snap_id1);
  expect_start_op(mock_exclusive_lock);
  expect_snap_remove(mock_dst_image_ctx, "snap1", 0);
  expect_get_snap_namespace(mock_src_image_ctx, src_snap_id1);
  expect_start_op(mock_exclusive_lock);
  expect_snap_create(mock_dst_image_ctx, mock_snapshot_create_request, "snap1",
                     12, 0);
  expect_snap_is_protected(mock_src_image_ctx, src_snap_id1, false, 0);
  expect_set_head(mock_set_head_request, 0);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_src_image_ctx,
                                                    mock_dst_image_ctx, 0,
                                                    CEPH_NOSNAP, 0, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());

  validate_snap_seqs({{src_snap_id1, 12}});
}

TEST_F(TestMockDeepCopySnapshotCopyRequest, SnapCreateProtect) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  ASSERT_EQ(0, create_snap(m_src_image_ctx, "snap1", true));

  uint64_t src_snap_id1 = m_src_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockSnapshotCreateRequest mock_snapshot_create_request;
  MockSetHeadRequest mock_set_head_request;

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_get_snap_namespace(mock_src_image_ctx, src_snap_id1);
  expect_start_op(mock_exclusive_lock);
  expect_snap_create(mock_dst_image_ctx, mock_snapshot_create_request, "snap1",
                     12, 0);
  expect_snap_is_protected(mock_src_image_ctx, src_snap_id1, true, 0);
  expect_snap_is_protected(mock_dst_image_ctx, 12, false, 0);
  expect_start_op(mock_exclusive_lock);
  expect_snap_protect(mock_dst_image_ctx, "snap1", 0);
  expect_set_head(mock_set_head_request, 0);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_src_image_ctx,
                                                    mock_dst_image_ctx, 0,
                                                    CEPH_NOSNAP, 0, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());

  validate_snap_seqs({{src_snap_id1, 12}});
}

TEST_F(TestMockDeepCopySnapshotCopyRequest, SnapProtect) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  ASSERT_EQ(0, create_snap(m_src_image_ctx, "snap1", true));
  ASSERT_EQ(0, create_snap(m_dst_image_ctx, "snap1", true));

  uint64_t src_snap_id1 = m_src_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];
  uint64_t dst_snap_id1 = m_dst_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];
  m_snap_seqs[src_snap_id1] = dst_snap_id1;

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockSetHeadRequest mock_set_head_request;

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_snap_is_unprotected(mock_dst_image_ctx, dst_snap_id1, true, 0);
  expect_get_snap_namespace(mock_dst_image_ctx, dst_snap_id1);
  expect_get_snap_namespace(mock_src_image_ctx, src_snap_id1);
  expect_snap_is_protected(mock_src_image_ctx, src_snap_id1, true, 0);
  expect_snap_is_protected(mock_dst_image_ctx, dst_snap_id1, false, 0);
  expect_start_op(mock_exclusive_lock);
  expect_snap_protect(mock_dst_image_ctx, "snap1", 0);
  expect_set_head(mock_set_head_request, 0);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_src_image_ctx,
                                                    mock_dst_image_ctx, 0,
                                                    CEPH_NOSNAP, 0, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());

  validate_snap_seqs({{src_snap_id1, dst_snap_id1}});
}

TEST_F(TestMockDeepCopySnapshotCopyRequest, SnapProtectError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  ASSERT_EQ(0, create_snap(m_src_image_ctx, "snap1", true));
  ASSERT_EQ(0, create_snap(m_dst_image_ctx, "snap1", true));

  uint64_t src_snap_id1 = m_src_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];
  uint64_t dst_snap_id1 = m_dst_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];
  m_snap_seqs[src_snap_id1] = dst_snap_id1;

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_snap_is_unprotected(mock_dst_image_ctx, dst_snap_id1, true, 0);
  expect_get_snap_namespace(mock_dst_image_ctx, dst_snap_id1);
  expect_get_snap_namespace(mock_src_image_ctx, src_snap_id1);
  expect_snap_is_protected(mock_src_image_ctx, src_snap_id1, true, 0);
  expect_snap_is_protected(mock_dst_image_ctx, dst_snap_id1, false, 0);
  expect_start_op(mock_exclusive_lock);
  expect_snap_protect(mock_dst_image_ctx, "snap1", -EINVAL);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_src_image_ctx,
                                                    mock_dst_image_ctx, 0,
                                                    CEPH_NOSNAP, 0, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockDeepCopySnapshotCopyRequest, SnapProtectCancel) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  ASSERT_EQ(0, create_snap(m_src_image_ctx, "snap1", true));
  ASSERT_EQ(0, create_snap(m_dst_image_ctx, "snap1", true));

  uint64_t src_snap_id1 = m_src_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];
  uint64_t dst_snap_id1 = m_dst_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];
  m_snap_seqs[src_snap_id1] = dst_snap_id1;

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_src_image_ctx,
                                                    mock_dst_image_ctx, 0,
                                                    CEPH_NOSNAP, 0, &ctx);
  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_snap_is_unprotected(mock_dst_image_ctx, dst_snap_id1, true, 0);
  expect_get_snap_namespace(mock_dst_image_ctx, dst_snap_id1);
  expect_get_snap_namespace(mock_src_image_ctx, src_snap_id1);
  expect_snap_is_protected(mock_src_image_ctx, src_snap_id1, true, 0);
  expect_snap_is_protected(mock_dst_image_ctx, dst_snap_id1, false, 0);
  expect_start_op(mock_exclusive_lock);
  EXPECT_CALL(*mock_dst_image_ctx.operations,
	      execute_snap_protect(_, StrEq("snap1"), _))
    .WillOnce(DoAll(InvokeWithoutArgs([request]() {
	    request->cancel();
	  }),
	WithArg<2>(Invoke([this](Context *ctx) {
	      m_work_queue->queue(ctx, 0);
	    }))));

  request->send();
  ASSERT_EQ(-ECANCELED, ctx.wait());
}

TEST_F(TestMockDeepCopySnapshotCopyRequest, SetHeadError) {
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockSetHeadRequest mock_set_head_request;

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_set_head(mock_set_head_request, -EINVAL);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_src_image_ctx,
                                                    mock_dst_image_ctx, 0,
                                                    CEPH_NOSNAP, 0, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockDeepCopySnapshotCopyRequest, NoSetHead) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  ASSERT_EQ(0, create_snap(m_src_image_ctx, "snap1", true));

  uint64_t src_snap_id1 = m_src_image_ctx->snap_ids[
    {cls::rbd::UserSnapshotNamespace(), "snap1"}];

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockSnapshotCreateRequest mock_snapshot_create_request;

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_get_snap_namespace(mock_src_image_ctx, src_snap_id1);
  expect_start_op(mock_exclusive_lock);
  expect_snap_create(mock_dst_image_ctx, mock_snapshot_create_request, "snap1",
                     12, 0);
  expect_snap_is_protected(mock_src_image_ctx, src_snap_id1, false, 0);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_src_image_ctx,
                                                    mock_dst_image_ctx,0,
                                                    src_snap_id1, 0, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());

  validate_snap_seqs({{src_snap_id1, 12}});
}

TEST_F(TestMockDeepCopySnapshotCopyRequest, StartEndLimit) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  ASSERT_EQ(0, create_snap(m_src_image_ctx, "snap1", false));
  ASSERT_EQ(0, create_snap(m_src_image_ctx, "snap2", false));
  ASSERT_EQ(0, create_snap(m_src_image_ctx,
                           {cls::rbd::MirrorSnapshotNamespace{
                              cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY,
                              {"peer uuid1"}, "", CEPH_NOSNAP}},
                           "snap3", false));
  auto src_snap_id1 = m_src_image_ctx->snaps[2];
  auto src_snap_id2 = m_src_image_ctx->snaps[1];
  auto src_snap_id3 = m_src_image_ctx->snaps[0];

  ASSERT_EQ(0, create_snap(m_dst_image_ctx, "snap0", true));
  ASSERT_EQ(0, create_snap(m_dst_image_ctx, "snap1", false));
  ASSERT_EQ(0, create_snap(m_dst_image_ctx, "snap3", false));
  auto dst_snap_id1 = m_dst_image_ctx->snaps[1];
  auto dst_snap_id3 = m_dst_image_ctx->snaps[0];

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockSnapshotCreateRequest mock_snapshot_create_request;

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_snap_is_unprotected(mock_dst_image_ctx, dst_snap_id3,
                             true, 0);

  expect_get_snap_namespace(mock_dst_image_ctx, dst_snap_id3);
  expect_start_op(mock_exclusive_lock);
  expect_snap_remove(mock_dst_image_ctx, "snap3", 0);

  expect_get_snap_namespace(mock_src_image_ctx, src_snap_id2);
  expect_start_op(mock_exclusive_lock);
  expect_snap_create(mock_dst_image_ctx, mock_snapshot_create_request, "snap2",
                     12, 0);
  expect_get_snap_namespace(mock_src_image_ctx, src_snap_id3);

  expect_snap_is_protected(mock_src_image_ctx, src_snap_id2, false, 0);
  expect_snap_is_protected(mock_src_image_ctx, src_snap_id3, false, 0);

  MockSetHeadRequest mock_set_head_request;
  expect_set_head(mock_set_head_request, 0);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_src_image_ctx,
                                                    mock_dst_image_ctx,
                                                    src_snap_id1,
                                                    src_snap_id3,
                                                    dst_snap_id1, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());

  validate_snap_seqs({{src_snap_id2, 12}, {src_snap_id3, CEPH_NOSNAP}});
}

} // namespace deep_copy
} // namespace librbd
