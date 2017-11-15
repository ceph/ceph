// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/deep_copy/ImageCopyRequest.h"
#include "librbd/deep_copy/ObjectCopyRequest.h"
#include "librbd/internal.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/test_support.h"
#include <boost/scope_exit.hpp>

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace deep_copy {

template <>
struct ObjectCopyRequest<librbd::MockTestImageCtx> {
  static ObjectCopyRequest* s_instance;
  static ObjectCopyRequest* create(
      librbd::MockTestImageCtx *src_image_ctx,
      librbd::MockTestImageCtx *dst_image_ctx, const SnapMap &snap_map,
      uint64_t object_number, Context *on_finish) {
    assert(s_instance != nullptr);
    Mutex::Locker locker(s_instance->lock);
    s_instance->snap_map = &snap_map;
    s_instance->object_contexts[object_number] = on_finish;
    s_instance->cond.Signal();
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  Mutex lock;
  Cond cond;

  const SnapMap *snap_map = nullptr;
  std::map<uint64_t, Context *> object_contexts;

  ObjectCopyRequest() : lock("lock") {
    s_instance = this;
  }
};

ObjectCopyRequest<librbd::MockTestImageCtx>* ObjectCopyRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace deep_copy
} // namespace librbd

// template definitions
#include "librbd/deep_copy/ImageCopyRequest.cc"
template class librbd::deep_copy::ImageCopyRequest<librbd::MockTestImageCtx>;

namespace librbd {
namespace deep_copy {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Return;

class TestMockDeepCopyImageCopyRequest : public TestMockFixture {
public:
  typedef ImageCopyRequest<librbd::MockTestImageCtx> MockImageCopyRequest;
  typedef ObjectCopyRequest<librbd::MockTestImageCtx> MockObjectCopyRequest;

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

  void expect_object_copy_send(MockObjectCopyRequest &mock_object_copy_request) {
    EXPECT_CALL(mock_object_copy_request, send());
  }

  bool complete_object_copy(MockObjectCopyRequest &mock_object_copy_request,
                            uint64_t object_num, Context **object_ctx, int r) {
    Mutex::Locker locker(mock_object_copy_request.lock);
    while (mock_object_copy_request.object_contexts.count(object_num) == 0) {
      if (mock_object_copy_request.cond.WaitInterval(mock_object_copy_request.lock,
                                                     utime_t(10, 0)) != 0) {
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
    Mutex::Locker locker(mock_object_copy_request.lock);
    while (mock_object_copy_request.snap_map == nullptr) {
      if (mock_object_copy_request.cond.WaitInterval(mock_object_copy_request.lock,
                                                     utime_t(10, 0)) != 0) {
        return SnapMap();
      }
    }
    return *mock_object_copy_request.snap_map;
  }

  int create_snap(librbd::ImageCtx *image_ctx, const char* snap_name,
                  librados::snap_t *snap_id) {
    int r = image_ctx->operations->snap_create(
        cls::rbd::UserSnapshotNamespace(), snap_name);
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
  expect_get_image_size(mock_src_image_ctx, 1 << m_src_image_ctx->order);
  expect_get_image_size(mock_src_image_ctx, 0);
  expect_object_copy_send(mock_object_copy_request);

  librbd::NoOpProgressContext no_op;
  C_SaferCond ctx;
  auto request = new MockImageCopyRequest(&mock_src_image_ctx,
                                          &mock_dst_image_ctx,
                                          0, snap_id_end, boost::none,
                                          m_snap_seqs, &no_op, &ctx);
  request->send();

  ASSERT_EQ(m_snap_map, wait_for_snap_map(mock_object_copy_request));
  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 0, nullptr, 0));
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopyImageCopyRequest, OutOfOrder) {
  librados::snap_t snap_id_end;
  ASSERT_EQ(0, create_snap("copy", &snap_id_end));

  uint64_t object_count = 55;

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockObjectCopyRequest mock_object_copy_request;

  expect_get_image_size(mock_src_image_ctx,
                        object_count * (1 << m_src_image_ctx->order));
  expect_get_image_size(mock_src_image_ctx, 0);

  EXPECT_CALL(mock_object_copy_request, send()).Times(object_count);

  class ProgressContext : public librbd::ProgressContext {
  public:
    uint64_t object_count;
    librbd::deep_copy::ObjectNumber expected_object_number;

    ProgressContext(uint64_t object_count)
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
  } prog_ctx(object_count);

  C_SaferCond ctx;
  auto request = new MockImageCopyRequest(&mock_src_image_ctx,
                                          &mock_dst_image_ctx,
                                          0, snap_id_end, boost::none,
                                          m_snap_seqs, &prog_ctx, &ctx);
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
  expect_get_image_size(mock_src_image_ctx, 1 << m_src_image_ctx->order);
  expect_get_image_size(mock_src_image_ctx, 0);
  expect_get_image_size(mock_src_image_ctx, 0);
  expect_get_image_size(mock_src_image_ctx, 0);
  expect_object_copy_send(mock_object_copy_request);

  librbd::NoOpProgressContext no_op;
  C_SaferCond ctx;
  auto request = new MockImageCopyRequest(&mock_src_image_ctx,
                                          &mock_dst_image_ctx,
                                          snap_id_start, snap_id_end,
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
  expect_get_image_size(mock_src_image_ctx, 2 * (1 << m_src_image_ctx->order));
  expect_get_image_size(mock_src_image_ctx, 0);
  expect_object_copy_send(mock_object_copy_request);

  librbd::NoOpProgressContext no_op;
  C_SaferCond ctx;
  auto request = new MockImageCopyRequest(&mock_src_image_ctx,
                                          &mock_dst_image_ctx,
                                          0, snap_id_end,
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
  expect_get_image_size(mock_src_image_ctx, 1 << m_src_image_ctx->order);
  expect_get_image_size(mock_src_image_ctx, 0);
  expect_object_copy_send(mock_object_copy_request);

  librbd::NoOpProgressContext no_op;
  C_SaferCond ctx;
  auto request = new MockImageCopyRequest(&mock_src_image_ctx,
                                          &mock_dst_image_ctx,
                                          0, snap_id_end, boost::none,
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
  expect_get_image_size(mock_src_image_ctx, 6 * (1 << m_src_image_ctx->order));
  expect_get_image_size(mock_src_image_ctx, m_image_size);

  EXPECT_CALL(mock_object_copy_request, send()).Times(6);

  struct ProgressContext : public librbd::ProgressContext {
    librbd::deep_copy::ObjectNumber object_number;

    int update_progress(uint64_t object_no, uint64_t end_object_no) override {
      object_number = object_number ? *object_number + 1 : 0;
      return 0;
    }
  } prog_ctx;

  C_SaferCond ctx;
  auto request = new MockImageCopyRequest(&mock_src_image_ctx,
                                          &mock_dst_image_ctx,
                                          0, snap_id_end, boost::none,
                                          m_snap_seqs, &prog_ctx, &ctx);
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
  ASSERT_EQ(5u, prog_ctx.object_number.get());
}

TEST_F(TestMockDeepCopyImageCopyRequest, MissingSnap) {
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::NoOpProgressContext no_op;
  C_SaferCond ctx;
  auto request = new MockImageCopyRequest(&mock_src_image_ctx,
                                          &mock_dst_image_ctx,
                                          0, 123, boost::none,
                                          m_snap_seqs, &no_op, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockDeepCopyImageCopyRequest, MissingFromSnap) {
  librados::snap_t snap_id_end;
  ASSERT_EQ(0, create_snap("copy", &snap_id_end));

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::NoOpProgressContext no_op;
  C_SaferCond ctx;
  auto request = new MockImageCopyRequest(&mock_src_image_ctx,
                                          &mock_dst_image_ctx,
                                          123, snap_id_end, boost::none,
                                          m_snap_seqs, &no_op, &ctx);
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

  librbd::NoOpProgressContext no_op;
  C_SaferCond ctx;
  auto request = new MockImageCopyRequest(&mock_src_image_ctx,
                                          &mock_dst_image_ctx,
                                          snap_id_start, snap_id_end,
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

  librbd::NoOpProgressContext no_op;
  C_SaferCond ctx;
  auto request = new MockImageCopyRequest(&mock_src_image_ctx,
                                          &mock_dst_image_ctx,
                                          snap_id_start, snap_id_end,
                                          boost::none, {}, &no_op, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace deep_copy
} // namespace librbd
