// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Operations.h"
#include "librbd/deep_copy/Handler.h"
#include "librbd/deep_copy/ImageCopyRequest.h"
#include "librbd/deep_copy/ObjectCopyRequest.h"
#include "librbd/image/CloseRequest.h"
#include "librbd/image/OpenRequest.h"
#include "librbd/object_map/DiffRequest.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/test_support.h"
#include <boost/scope_exit.hpp>

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  static MockTestImageCtx* s_instance;
  static MockTestImageCtx* create(const std::string &image_name,
                                  const std::string &image_id,
                                  librados::snap_t snap_id, librados::IoCtx& p,
                                  bool read_only) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  explicit MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
    s_instance = this;
  }

  MOCK_METHOD0(destroy, void());
};

MockTestImageCtx* MockTestImageCtx::s_instance = nullptr;

} // anonymous namespace

namespace deep_copy {

template <>
struct ObjectCopyRequest<librbd::MockTestImageCtx> {
  static ObjectCopyRequest* s_instance;
  static ObjectCopyRequest* create(
      librbd::MockTestImageCtx *src_image_ctx,
      librbd::MockTestImageCtx *dst_image_ctx,
      librados::snap_t src_snap_id_start,
      librados::snap_t dst_snap_id_start,
      const SnapMap &snap_map,
      uint64_t object_number, bool flatten, Handler* handler,
      Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    std::lock_guard locker{s_instance->lock};
    s_instance->snap_map = &snap_map;
    s_instance->object_contexts[object_number] = on_finish;
    s_instance->cond.notify_all();
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  ceph::mutex lock = ceph::make_mutex("lock");
  ceph::condition_variable cond;

  const SnapMap *snap_map = nullptr;
  std::map<uint64_t, Context *> object_contexts;

  ObjectCopyRequest() {
    s_instance = this;
  }
};

ObjectCopyRequest<librbd::MockTestImageCtx>* ObjectCopyRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace deep_copy

namespace image {

template <>
struct CloseRequest<MockTestImageCtx> {
  Context* on_finish = nullptr;
  static CloseRequest* s_instance;
  static CloseRequest* create(MockTestImageCtx *image_ctx, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  CloseRequest() {
    s_instance = this;
  }
};

CloseRequest<MockTestImageCtx>* CloseRequest<MockTestImageCtx>::s_instance = nullptr;

template <>
struct OpenRequest<MockTestImageCtx> {
  Context* on_finish = nullptr;
  static OpenRequest* s_instance;
  static OpenRequest* create(MockTestImageCtx *image_ctx,
                             bool skip_open_parent, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  OpenRequest() {
    s_instance = this;
  }
};

OpenRequest<MockTestImageCtx>* OpenRequest<MockTestImageCtx>::s_instance = nullptr;

} // namespace image

namespace object_map {

template <>
struct DiffRequest<MockTestImageCtx> {
  BitVector<2>* object_diff_state = nullptr;
  Context* on_finish = nullptr;
  static DiffRequest* s_instance;
  static DiffRequest* create(MockTestImageCtx *image_ctx,
                             uint64_t snap_id_start, uint64_t snap_id_end,
                             BitVector<2>* object_diff_state,
                             Context* on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->object_diff_state = object_diff_state;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  DiffRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

DiffRequest<MockTestImageCtx>* DiffRequest<MockTestImageCtx>::s_instance = nullptr;

} // namespace object_map
} // namespace librbd

// template definitions
#include "librbd/deep_copy/ImageCopyRequest.cc"
template class librbd::deep_copy::ImageCopyRequest<librbd::MockTestImageCtx>;

namespace librbd {
namespace deep_copy {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;

class TestMockDeepCopyImageCopyRequest : public TestMockFixture {
public:
  typedef ImageCopyRequest<librbd::MockTestImageCtx> MockImageCopyRequest;
  typedef ObjectCopyRequest<librbd::MockTestImageCtx> MockObjectCopyRequest;
  typedef object_map::DiffRequest<librbd::MockTestImageCtx> MockDiffRequest;

  librbd::ImageCtx *m_src_image_ctx;
  librbd::ImageCtx *m_dst_image_ctx;
  ThreadPool *m_thread_pool;
  ContextWQ *m_work_queue;
  librbd::SnapSeqs m_snap_seqs;
  SnapMap m_snap_map;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &m_src_image_ctx));

    librbd::RBD rbd;
    std::string dst_image_name = get_temp_image_name();
    ASSERT_EQ(0, create_image_pp(rbd, m_ioctx, dst_image_name, m_image_size));
    ASSERT_EQ(0, open_image(dst_image_name, &m_dst_image_ctx));

    librbd::ImageCtx::get_thread_pool_instance(m_src_image_ctx->cct,
                                               &m_thread_pool, &m_work_queue);
  }

  void expect_get_image_size(librbd::MockTestImageCtx &mock_image_ctx,
                             uint64_t size) {
    EXPECT_CALL(mock_image_ctx, get_image_size(_))
      .WillOnce(Return(size)).RetiresOnSaturation();
  }

  void expect_diff_send(MockDiffRequest& mock_request,
                        const BitVector<2>& diff_state, int r) {
    EXPECT_CALL(mock_request, send())
      .WillOnce(Invoke([this, &mock_request, diff_state, r]() {
                  if (r >= 0) {
                    *mock_request.object_diff_state = diff_state;
                  }
                  m_work_queue->queue(mock_request.on_finish, r);
                }));
  }

  void expect_object_copy_send(MockObjectCopyRequest &mock_object_copy_request) {
    EXPECT_CALL(mock_object_copy_request, send());
  }

  bool complete_object_copy(MockObjectCopyRequest &mock_object_copy_request,
                            uint64_t object_num, Context **object_ctx, int r) {
    std::unique_lock locker{mock_object_copy_request.lock};
    while (mock_object_copy_request.object_contexts.count(object_num) == 0) {
      if (mock_object_copy_request.cond.wait_for(locker, 10s) ==
	  std::cv_status::timeout) {
        return false;
      }
    }

    if (object_ctx != nullptr) {
      *object_ctx = mock_object_copy_request.object_contexts[object_num];
    } else {
      m_work_queue->queue(mock_object_copy_request.object_contexts[object_num],
                          r);
    }
    return true;
  }

  SnapMap wait_for_snap_map(MockObjectCopyRequest &mock_object_copy_request) {
    std::unique_lock locker{mock_object_copy_request.lock};
    while (mock_object_copy_request.snap_map == nullptr) {
      if (mock_object_copy_request.cond.wait_for(locker, 10s) ==
	  std::cv_status::timeout) {
        return SnapMap();
      }
    }
    return *mock_object_copy_request.snap_map;
  }

  int create_snap(librbd::ImageCtx *image_ctx, const char* snap_name,
                  librados::snap_t *snap_id) {
    NoOpProgressContext prog_ctx;
    int r = image_ctx->operations->snap_create(
        cls::rbd::UserSnapshotNamespace(), snap_name, 0, prog_ctx);
    if (r < 0) {
      return r;
    }

    r = image_ctx->state->refresh();
    if (r < 0) {
      return r;
    }

    if (image_ctx->snap_ids.count({cls::rbd::UserSnapshotNamespace(),
                                   snap_name}) == 0) {
      return -ENOENT;
    }

    if (snap_id != nullptr) {
      *snap_id = image_ctx->snap_ids[{cls::rbd::UserSnapshotNamespace(),
                                      snap_name}];
    }
    return 0;
  }

  int create_snap(const char* snap_name,
                  librados::snap_t *src_snap_id_ = nullptr) {
    librados::snap_t src_snap_id;
    int r = create_snap(m_src_image_ctx, snap_name, &src_snap_id);
    if (r < 0) {
      return r;
    }

    if (src_snap_id_ != nullptr) {
      *src_snap_id_ = src_snap_id;
    }

    librados::snap_t dst_snap_id;
    r = create_snap(m_dst_image_ctx, snap_name, &dst_snap_id);
    if (r < 0) {
      return r;
    }

    // collection of all existing snaps in dst image
    SnapIds dst_snap_ids({dst_snap_id});
    if (!m_snap_map.empty()) {
      dst_snap_ids.insert(dst_snap_ids.end(),
                          m_snap_map.rbegin()->second.begin(),
                          m_snap_map.rbegin()->second.end());
    }
    m_snap_map[src_snap_id] = dst_snap_ids;
    m_snap_seqs[src_snap_id] = dst_snap_id;
    return 0;
  }
};

TEST_F(TestMockDeepCopyImageCopyRequest, SimpleImage) {
  librados::snap_t snap_id_end;
  ASSERT_EQ(0, create_snap("copy", &snap_id_end));

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockObjectCopyRequest mock_object_copy_request;

  InSequence seq;
  MockDiffRequest mock_diff_request;
  expect_diff_send(mock_diff_request, {}, -EINVAL);
  expect_get_image_size(mock_src_image_ctx, 1 << m_src_image_ctx->order);
  expect_get_image_size(mock_src_image_ctx, 0);
  expect_object_copy_send(mock_object_copy_request);

  librbd::deep_copy::NoOpHandler no_op;
  C_SaferCond ctx;
  auto request = new MockImageCopyRequest(&mock_src_image_ctx,
                                          &mock_dst_image_ctx,
                                          0, snap_id_end, 0, false, boost::none,
                                          m_snap_seqs, &no_op, &ctx);
  request->send();

  ASSERT_EQ(m_snap_map, wait_for_snap_map(mock_object_copy_request));
  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 0, nullptr, 0));
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopyImageCopyRequest, FastDiff) {
  librados::snap_t snap_id_end;
  ASSERT_EQ(0, create_snap("copy", &snap_id_end));

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  InSequence seq;

  MockDiffRequest mock_diff_request;
  BitVector<2> diff_state;
  diff_state.resize(1);
  expect_diff_send(mock_diff_request, diff_state, 0);

  expect_get_image_size(mock_src_image_ctx, 1 << m_src_image_ctx->order);
  expect_get_image_size(mock_src_image_ctx, 0);

  librbd::deep_copy::NoOpHandler no_op;
  C_SaferCond ctx;
  auto request = new MockImageCopyRequest(&mock_src_image_ctx,
                                          &mock_dst_image_ctx,
                                          0, snap_id_end, 0, false, boost::none,
                                          m_snap_seqs, &no_op, &ctx);
  request->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopyImageCopyRequest, OutOfOrder) {
  std::string max_ops_str;
  ASSERT_EQ(0, _rados.conf_get("rbd_concurrent_management_ops", max_ops_str));
  ASSERT_EQ(0, _rados.conf_set("rbd_concurrent_management_ops", "10"));
  BOOST_SCOPE_EXIT( (max_ops_str) ) {
    ASSERT_EQ(0, _rados.conf_set("rbd_concurrent_management_ops",
                                 max_ops_str.c_str()));
  } BOOST_SCOPE_EXIT_END;

  librados::snap_t snap_id_end;
  ASSERT_EQ(0, create_snap("copy", &snap_id_end));

  uint64_t object_count = 55;

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockObjectCopyRequest mock_object_copy_request;

  MockDiffRequest mock_diff_request;
  expect_diff_send(mock_diff_request, {}, -EINVAL);
  expect_get_image_size(mock_src_image_ctx,
                        object_count * (1 << m_src_image_ctx->order));
  expect_get_image_size(mock_src_image_ctx, 0);

  EXPECT_CALL(mock_object_copy_request, send()).Times(object_count);

  class Handler : public librbd::deep_copy::NoOpHandler {
  public:
    uint64_t object_count;
    librbd::deep_copy::ObjectNumber expected_object_number;

    Handler(uint64_t object_count)
      : object_count(object_count) {
    }

    int update_progress(uint64_t object_no, uint64_t end_object_no) override {
      EXPECT_LE(object_no, object_count);
      EXPECT_EQ(end_object_no, object_count);
      if (!expected_object_number) {
        expected_object_number = 0;
      } else {
        expected_object_number = *expected_object_number + 1;
      }
      EXPECT_EQ(*expected_object_number, object_no - 1);

      return 0;
    }
  } handler(object_count);

  C_SaferCond ctx;
  auto request = new MockImageCopyRequest(&mock_src_image_ctx,
                                          &mock_dst_image_ctx,
                                          0, snap_id_end, 0, false, boost::none,
                                          m_snap_seqs, &handler, &ctx);
  request->send();

  std::map<uint64_t, Context*> copy_contexts;
  ASSERT_EQ(m_snap_map, wait_for_snap_map(mock_object_copy_request));
  for (uint64_t i = 0; i < object_count; ++i) {
    if (i % 10 == 0) {
      ASSERT_TRUE(complete_object_copy(mock_object_copy_request, i,
                  &copy_contexts[i], 0));
    } else {
      ASSERT_TRUE(complete_object_copy(mock_object_copy_request, i, nullptr,
                                       0));
    }
  }

  for (auto& pair : copy_contexts) {
    pair.second->complete(0);
  }

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopyImageCopyRequest, SnapshotSubset) {
  librados::snap_t snap_id_start;
  librados::snap_t snap_id_end;
  ASSERT_EQ(0, create_snap("snap1"));
  ASSERT_EQ(0, create_snap("snap2", &snap_id_start));
  ASSERT_EQ(0, create_snap("copy", &snap_id_end));

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockObjectCopyRequest mock_object_copy_request;

  InSequence seq;
  MockDiffRequest mock_diff_request;
  expect_diff_send(mock_diff_request, {}, -EINVAL);
  expect_get_image_size(mock_src_image_ctx, 1 << m_src_image_ctx->order);
  expect_get_image_size(mock_src_image_ctx, 0);
  expect_get_image_size(mock_src_image_ctx, 0);
  expect_get_image_size(mock_src_image_ctx, 0);
  expect_object_copy_send(mock_object_copy_request);

  librbd::deep_copy::NoOpHandler no_op;
  C_SaferCond ctx;
  auto request = new MockImageCopyRequest(&mock_src_image_ctx,
                                          &mock_dst_image_ctx,
                                          snap_id_start, snap_id_end, 0, false,
                                          boost::none, m_snap_seqs, &no_op,
                                          &ctx);
  request->send();

  SnapMap snap_map(m_snap_map);
  snap_map.erase(snap_map.begin());
  ASSERT_EQ(snap_map, wait_for_snap_map(mock_object_copy_request));

  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 0, nullptr, 0));
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopyImageCopyRequest, RestartPartialSync) {
  librados::snap_t snap_id_end;
  ASSERT_EQ(0, create_snap("copy", &snap_id_end));

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockObjectCopyRequest mock_object_copy_request;

  InSequence seq;
  MockDiffRequest mock_diff_request;
  expect_diff_send(mock_diff_request, {}, -EINVAL);
  expect_get_image_size(mock_src_image_ctx, 2 * (1 << m_src_image_ctx->order));
  expect_get_image_size(mock_src_image_ctx, 0);
  expect_object_copy_send(mock_object_copy_request);

  librbd::deep_copy::NoOpHandler no_op;
  C_SaferCond ctx;
  auto request = new MockImageCopyRequest(&mock_src_image_ctx,
                                          &mock_dst_image_ctx,
                                          0, snap_id_end, 0, false,
                                          librbd::deep_copy::ObjectNumber{0U},
                                          m_snap_seqs, &no_op, &ctx);
  request->send();

  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 1, nullptr, 0));
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopyImageCopyRequest, Cancel) {
  std::string max_ops_str;
  ASSERT_EQ(0, _rados.conf_get("rbd_concurrent_management_ops", max_ops_str));
  ASSERT_EQ(0, _rados.conf_set("rbd_concurrent_management_ops", "1"));
  BOOST_SCOPE_EXIT( (max_ops_str) ) {
    ASSERT_EQ(0, _rados.conf_set("rbd_concurrent_management_ops",
                                 max_ops_str.c_str()));
  } BOOST_SCOPE_EXIT_END;

  librados::snap_t snap_id_end;
  ASSERT_EQ(0, create_snap("copy", &snap_id_end));

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockObjectCopyRequest mock_object_copy_request;

  InSequence seq;
  MockDiffRequest mock_diff_request;
  expect_diff_send(mock_diff_request, {}, -EINVAL);
  expect_get_image_size(mock_src_image_ctx, 1 << m_src_image_ctx->order);
  expect_get_image_size(mock_src_image_ctx, 0);
  expect_object_copy_send(mock_object_copy_request);

  librbd::deep_copy::NoOpHandler no_op;
  C_SaferCond ctx;
  auto request = new MockImageCopyRequest(&mock_src_image_ctx,
                                          &mock_dst_image_ctx,
                                          0, snap_id_end, 0, false, boost::none,
                                          m_snap_seqs, &no_op, &ctx);
  request->send();

  ASSERT_EQ(m_snap_map, wait_for_snap_map(mock_object_copy_request));
  request->cancel();

  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 0, nullptr, 0));
  ASSERT_EQ(-ECANCELED, ctx.wait());
}

TEST_F(TestMockDeepCopyImageCopyRequest, Cancel_Inflight_Sync) {
  std::string max_ops_str;
  ASSERT_EQ(0, _rados.conf_get("rbd_concurrent_management_ops", max_ops_str));
  ASSERT_EQ(0, _rados.conf_set("rbd_concurrent_management_ops", "3"));
  BOOST_SCOPE_EXIT( (max_ops_str) ) {
    ASSERT_EQ(0, _rados.conf_set("rbd_concurrent_management_ops",
                                 max_ops_str.c_str()));
  } BOOST_SCOPE_EXIT_END;

  librados::snap_t snap_id_end;
  ASSERT_EQ(0, create_snap("copy", &snap_id_end));

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockObjectCopyRequest mock_object_copy_request;

  InSequence seq;
  MockDiffRequest mock_diff_request;
  expect_diff_send(mock_diff_request, {}, -EINVAL);
  expect_get_image_size(mock_src_image_ctx, 6 * (1 << m_src_image_ctx->order));
  expect_get_image_size(mock_src_image_ctx, m_image_size);

  EXPECT_CALL(mock_object_copy_request, send()).Times(6);

  struct Handler : public librbd::deep_copy::NoOpHandler {
    librbd::deep_copy::ObjectNumber object_number;

    int update_progress(uint64_t object_no, uint64_t end_object_no) override {
      object_number = object_number ? *object_number + 1 : 0;
      return 0;
    }
  } handler;

  C_SaferCond ctx;
  auto request = new MockImageCopyRequest(&mock_src_image_ctx,
                                          &mock_dst_image_ctx,
                                          0, snap_id_end, 0, false, boost::none,
                                          m_snap_seqs, &handler, &ctx);
  request->send();

  ASSERT_EQ(m_snap_map, wait_for_snap_map(mock_object_copy_request));

  Context *cancel_ctx = nullptr;
  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 0, nullptr, 0));
  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 1, nullptr, 0));
  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 2, nullptr, 0));
  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 3, &cancel_ctx,
                                   0));
  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 4, nullptr, 0));
  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 5, nullptr, 0));

  request->cancel();
  cancel_ctx->complete(0);

  ASSERT_EQ(-ECANCELED, ctx.wait());
  ASSERT_EQ(5u, handler.object_number.get());
}

TEST_F(TestMockDeepCopyImageCopyRequest, MissingSnap) {
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::deep_copy::NoOpHandler no_op;
  C_SaferCond ctx;
  auto request = new MockImageCopyRequest(&mock_src_image_ctx,
                                          &mock_dst_image_ctx,
                                          0, 123, 0, false, boost::none,
                                          m_snap_seqs, &no_op, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockDeepCopyImageCopyRequest, MissingFromSnap) {
  librados::snap_t snap_id_end;
  ASSERT_EQ(0, create_snap("copy", &snap_id_end));

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::deep_copy::NoOpHandler no_op;
  C_SaferCond ctx;
  auto request = new MockImageCopyRequest(&mock_src_image_ctx,
                                          &mock_dst_image_ctx,
                                          123, snap_id_end, 0, false,
                                          boost::none, m_snap_seqs, &no_op,
                                          &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockDeepCopyImageCopyRequest, EmptySnapMap) {
  librados::snap_t snap_id_start;
  librados::snap_t snap_id_end;
  ASSERT_EQ(0, create_snap("snap1", &snap_id_start));
  ASSERT_EQ(0, create_snap("copy", &snap_id_end));

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::deep_copy::NoOpHandler no_op;
  C_SaferCond ctx;
  auto request = new MockImageCopyRequest(&mock_src_image_ctx,
                                          &mock_dst_image_ctx,
                                          snap_id_start, snap_id_end, 0, false,
                                          boost::none, {{0, 0}}, &no_op, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockDeepCopyImageCopyRequest, EmptySnapSeqs) {
  librados::snap_t snap_id_start;
  librados::snap_t snap_id_end;
  ASSERT_EQ(0, create_snap("snap1", &snap_id_start));
  ASSERT_EQ(0, create_snap("copy", &snap_id_end));

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::deep_copy::NoOpHandler no_op;
  C_SaferCond ctx;
  auto request = new MockImageCopyRequest(&mock_src_image_ctx,
                                          &mock_dst_image_ctx,
                                          snap_id_start, snap_id_end, 0, false,
                                          boost::none, {}, &no_op, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace deep_copy
} // namespace librbd
