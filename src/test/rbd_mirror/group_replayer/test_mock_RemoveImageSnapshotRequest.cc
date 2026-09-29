// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/group_replayer/RemoveImageSnapshotRequest.cc"

namespace rbd {
namespace mirror {
namespace group_replayer {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;

class TestMockGroupReplayerRemoveImageSnapshotRequest : public TestMockFixture {
public:
  using MockRequest = RemoveImageSnapshotRequest<librbd::MockImageCtx>;
  librbd::ImageCtx* m_local_image_ctx = nullptr;

  void SetUp() override {
    TestMockFixture::SetUp();
    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &m_local_image_ctx));
  }

  void expect_open(librbd::MockImageCtx& image_ctx, int r) {
    EXPECT_CALL(*image_ctx.state, open(librbd::OPEN_FLAG_SKIP_OPEN_PARENT, _))
      .WillOnce(Invoke([this, &image_ctx, r](uint64_t, Context* ctx) {
        EXPECT_EQ(0U, image_ctx.read_only_mask &
                        librbd::IMAGE_READ_ONLY_FLAG_NON_PRIMARY);
        m_threads->work_queue->queue(ctx, r);
      }));
  }

  void send_request(librbd::MockImageCtx& image_ctx, int expected_r) {
    librbd::MockImageCtx::s_instance = &image_ctx;
    C_SaferCond ctx;
    auto req = MockRequest::create(m_local_io_ctx, m_local_io_ctx.get_id(),
      image_ctx.id, 123, "group-snap", &ctx);
    req->send();
    EXPECT_EQ(expected_r, ctx.wait());
    librbd::MockImageCtx::s_instance = nullptr;
  }

  void check_snapshot(const cls::rbd::SnapshotNamespace& snap_namespace,
                      bool missing, bool remove, int expected_r) {
    librbd::MockImageCtx image_ctx(*m_local_image_ctx);
    librbd::SnapInfo snap_info("image-snap", snap_namespace, m_image_size,
                               {}, RBD_PROTECTION_STATUS_UNPROTECTED, 0, {});
    InSequence seq;
    expect_open(image_ctx, 0);
    EXPECT_CALL(image_ctx, get_snap_info(123))
      .WillOnce(Return(missing ? nullptr : &snap_info));
    if (remove) {
      EXPECT_CALL(*image_ctx.operations,
                  snap_remove(snap_namespace, "image-snap", _))
        .WillOnce(Invoke([this](
            const cls::rbd::SnapshotNamespace&, const std::string&,
            Context* ctx) {
          m_threads->work_queue->queue(ctx, 0);
        }));
    } else {
      EXPECT_CALL(*image_ctx.operations, snap_remove(_, _, _)).Times(0);
    }
    EXPECT_CALL(*image_ctx.state, close(_))
      .WillOnce(Invoke([this](Context* ctx) {
        m_threads->work_queue->queue(ctx, 0);
      }));
    send_request(image_ctx, expected_r);
  }

  cls::rbd::MirrorSnapshotNamespace mirror_namespace() {
    cls::rbd::MirrorSnapshotNamespace ns;
    ns.group_snap_id = "group-snap";
    return ns;
  }
};

// Test sequence:
// 1. Open an image with a matching mirror or user group snapshot.
// 2. Remove that snapshot and close the image.
// Expected: Both namespaces can be cleaned up, including for former members.
TEST_F(TestMockGroupReplayerRemoveImageSnapshotRequest, GroupSnapshots) {
  for (const auto& ns : {
         cls::rbd::SnapshotNamespace{mirror_namespace()},
         cls::rbd::SnapshotNamespace{cls::rbd::ImageSnapshotNamespaceGroup{
           m_local_io_ctx.get_id(), "old-group", "group-snap"}}}) {
    check_snapshot(ns, false, true, 0);
  }
}

// Test sequence:
// 1. Supply a mirror snapshot for another group snapshot ID or a user snapshot.
// 2. Request group snapshot cleanup.
// Expected: Return -ESTALE without removing either unrelated snapshot.
TEST_F(TestMockGroupReplayerRemoveImageSnapshotRequest, UnrelatedSnapshots) {
  auto ns = mirror_namespace();
  ns.group_snap_id = "another-snapshot";
  for (const auto& unrelated : {
         cls::rbd::SnapshotNamespace{ns},
         cls::rbd::SnapshotNamespace{cls::rbd::UserSnapshotNamespace{}}}) {
    check_snapshot(unrelated, false, false, -ESTALE);
  }
}

// Test sequence:
// 1. Open the image but report the requested snapshot missing.
// 2. Run group snapshot cleanup.
// Expected: Close the image and succeed because the snapshot is already gone.
TEST_F(TestMockGroupReplayerRemoveImageSnapshotRequest, MissingSnapshot) {
  check_snapshot(mirror_namespace(), true, false, 0);
}

// Test sequence:
// 1. Return -ENOENT while opening the image.
// 2. Run snapshot cleanup for the saved image ID.
// Expected: Succeed without closing an unopened image or issuing removal.
TEST_F(TestMockGroupReplayerRemoveImageSnapshotRequest, MissingImage) {
  librbd::MockImageCtx image_ctx(*m_local_image_ctx);
  expect_open(image_ctx, -ENOENT);
  EXPECT_CALL(*image_ctx.state, close(_)).Times(0);
  EXPECT_CALL(*image_ctx.operations, snap_remove(_, _, _)).Times(0);
  send_request(image_ctx, 0);
}

} // namespace group_replayer
} // namespace mirror
} // namespace rbd
