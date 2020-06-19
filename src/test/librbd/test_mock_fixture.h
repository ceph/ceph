// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_TEST_MOCK_FIXTURE_H
#define CEPH_TEST_LIBRBD_TEST_MOCK_FIXTURE_H

#include "test/librbd/test_fixture.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/LibradosTestStub.h"
#include "librbd/asio/ContextWQ.h"
#include <boost/shared_ptr.hpp>
#include <gmock/gmock.h>

namespace librados {
class TestCluster;
class MockTestMemCluster;
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
  librbd::asio::ContextWQ *context_wq = reinterpret_cast<
    librbd::asio::ContextWQ *>(wq);
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

MATCHER_P(ContentsEqual, bl, "") {
  // TODO fix const-correctness of bufferlist
  return const_cast<bufferlist &>(arg).contents_equal(
    const_cast<bufferlist &>(bl));
}

class TestMockFixture : public TestFixture {
public:
  typedef boost::shared_ptr<librados::TestCluster> TestClusterRef;

  static void SetUpTestCase();
  static void TearDownTestCase();

  void TearDown() override;

  void expect_op_work_queue(librbd::MockImageCtx &mock_image_ctx);
  void expect_unlock_exclusive_lock(librbd::ImageCtx &ictx);

  void initialize_features(librbd::ImageCtx *ictx,
                           librbd::MockImageCtx &mock_image_ctx,
                           librbd::MockExclusiveLock &mock_exclusive_lock,
                           librbd::MockJournal &mock_journal,
                           librbd::MockObjectMap &mock_object_map);

  void expect_is_journal_appending(librbd::MockJournal &mock_journal,
                                   bool appending);
  void expect_is_journal_replaying(librbd::MockJournal &mock_journal);
  void expect_is_journal_ready(librbd::MockJournal &mock_journal);
  void expect_allocate_op_tid(librbd::MockImageCtx &mock_image_ctx);
  void expect_append_op_event(librbd::MockImageCtx &mock_image_ctx,
                              bool can_affect_io, int r);
  void expect_commit_op_event(librbd::MockImageCtx &mock_image_ctx, int r);

private:
  static TestClusterRef s_test_cluster;
};

#endif // CEPH_TEST_LIBRBD_TEST_MOCK_FIXTURE_H
