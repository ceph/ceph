// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_TEST_MOCK_FIXTURE_H
#define CEPH_TEST_LIBRBD_TEST_MOCK_FIXTURE_H

#include "test/librbd/test_fixture.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "common/WorkQueue.h"
#include <boost/shared_ptr.hpp>
#include <gmock/gmock.h>

namespace librados {
class TestRadosClient;
class MockTestMemIoCtxImpl;
class MockTestMemRadosClient;
}
namespace librbd {
class MockImageCtx;
}

ACTION_P(CopyInBufferlist, str) {
  arg0->append(str);
}

ACTION_P2(CompleteContext, r, wq) {
  ContextWQ *context_wq = reinterpret_cast<ContextWQ *>(wq);
  if (context_wq != NULL) {
    context_wq->queue(arg0, r);
  } else {
    arg0->complete(r);
  }
}

ACTION_P(DispatchContext, wq) {
  wq->queue(arg0, arg1);
}

ACTION_P3(FinishRequest, request, r, mock) {
  librbd::MockImageCtx *mock_image_ctx =
    reinterpret_cast<librbd::MockImageCtx *>(mock);
  mock_image_ctx->image_ctx->op_work_queue->queue(request->on_finish, r);
}

ACTION_P(GetReference, ref_object) {
  ref_object->get();
}

ACTION_P(Notify, ctx) {
  ctx->complete(0);
}

MATCHER_P(ContentsEqual, bl, "") {
  // TODO fix const-correctness of bufferlist
  return const_cast<bufferlist &>(arg).contents_equal(
    const_cast<bufferlist &>(bl));
}

class TestMockFixture : public TestFixture {
public:
  typedef boost::shared_ptr<librados::TestRadosClient> TestRadosClientPtr;

  static void SetUpTestCase();
  static void TearDownTestCase();

  virtual void SetUp();
  virtual void TearDown();

  ::testing::NiceMock<librados::MockTestMemRadosClient> &get_mock_rados_client() {
    return *s_mock_rados_client;
  }
  librados::MockTestMemIoCtxImpl &get_mock_io_ctx(librados::IoCtx &ioctx);

  void expect_op_work_queue(librbd::MockImageCtx &mock_image_ctx);
  void expect_unlock_exclusive_lock(librbd::ImageCtx &ictx);

private:
  static TestRadosClientPtr s_test_rados_client;
  static ::testing::NiceMock<librados::MockTestMemRadosClient> *s_mock_rados_client;
};

#endif // CEPH_TEST_LIBRBD_TEST_MOCK_FIXTURE_H
