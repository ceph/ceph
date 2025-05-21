// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_replayer/journal/EventPreprocessor.h"
#include "test/journal/mock/MockJournaler.h"
#include "test/librbd/mock/MockImageCtx.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  explicit MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace journal {

template <>
struct TypeTraits<librbd::MockTestImageCtx> {
  typedef ::journal::MockJournaler Journaler;
};

} // namespace journal
} // namespace librbd

// template definitions
#include "tools/rbd_mirror/image_replayer/journal/EventPreprocessor.cc"

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace journal {

using testing::_;
using testing::WithArg;

class TestMockImageReplayerJournalEventPreprocessor : public TestMockFixture {
public:
  typedef EventPreprocessor<librbd::MockTestImageCtx> MockEventPreprocessor;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &m_local_image_ctx));
  }

  void expect_image_refresh(librbd::MockTestImageCtx &mock_remote_image_ctx, int r) {
    EXPECT_CALL(*mock_remote_image_ctx.state, refresh(_))
      .WillOnce(CompleteContext(r));
  }

  void expect_update_client(::journal::MockJournaler &mock_journaler, int r) {
    EXPECT_CALL(mock_journaler, update_client(_, _))
      .WillOnce(WithArg<1>(CompleteContext(r)));
  }

  librbd::ImageCtx *m_local_image_ctx;
  librbd::journal::MirrorPeerClientMeta m_client_meta;

};

TEST_F(TestMockImageReplayerJournalEventPreprocessor, IsNotRequired) {
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  ::journal::MockJournaler mock_remote_journaler;

  MockEventPreprocessor event_preprocessor(mock_local_image_ctx,
                                           mock_remote_journaler,
                                           "local mirror uuid",
                                           &m_client_meta,
                                           m_threads->work_queue);

  librbd::journal::EventEntry event_entry{librbd::journal::RenameEvent{}};
  ASSERT_FALSE(event_preprocessor.is_required(event_entry));
}

TEST_F(TestMockImageReplayerJournalEventPreprocessor, IsRequiredSnapMapPrune) {
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  ::journal::MockJournaler mock_remote_journaler;

  m_client_meta.snap_seqs = {{1, 2}, {3, 4}};
  MockEventPreprocessor event_preprocessor(mock_local_image_ctx,
                                           mock_remote_journaler,
                                           "local mirror uuid",
                                           &m_client_meta,
                                           m_threads->work_queue);

  librbd::journal::EventEntry event_entry{librbd::journal::RenameEvent{}};
  ASSERT_TRUE(event_preprocessor.is_required(event_entry));
}

TEST_F(TestMockImageReplayerJournalEventPreprocessor, IsRequiredSnapRename) {
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  ::journal::MockJournaler mock_remote_journaler;

  MockEventPreprocessor event_preprocessor(mock_local_image_ctx,
                                           mock_remote_journaler,
                                           "local mirror uuid",
                                           &m_client_meta,
                                           m_threads->work_queue);

  librbd::journal::EventEntry event_entry{librbd::journal::SnapRenameEvent{}};
  ASSERT_TRUE(event_preprocessor.is_required(event_entry));
}

TEST_F(TestMockImageReplayerJournalEventPreprocessor, PreprocessSnapMapPrune) {
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  ::journal::MockJournaler mock_remote_journaler;

  expect_image_refresh(mock_local_image_ctx, 0);
  expect_update_client(mock_remote_journaler, 0);

  mock_local_image_ctx.snap_info = {
    {6, librbd::SnapInfo{"snap", cls::rbd::UserSnapshotNamespace(), 0U, {}, 0U, 0U, utime_t()}}};
  m_client_meta.snap_seqs = {{1, 2}, {3, 4}, {5, 6}};
  MockEventPreprocessor event_preprocessor(mock_local_image_ctx,
                                           mock_remote_journaler,
                                           "local mirror uuid",
                                           &m_client_meta,
                                           m_threads->work_queue);

  librbd::journal::EventEntry event_entry{librbd::journal::RenameEvent{}};
  C_SaferCond ctx;
  event_preprocessor.preprocess(&event_entry, &ctx);
  ASSERT_EQ(0, ctx.wait());

  librbd::SnapSeqs expected_snap_seqs = {{5, 6}};
  ASSERT_EQ(expected_snap_seqs, m_client_meta.snap_seqs);
}

TEST_F(TestMockImageReplayerJournalEventPreprocessor, PreprocessSnapRename) {
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  ::journal::MockJournaler mock_remote_journaler;

  expect_image_refresh(mock_local_image_ctx, 0);
  expect_update_client(mock_remote_journaler, 0);

  mock_local_image_ctx.snap_ids = {{{cls::rbd::UserSnapshotNamespace(), "snap"}, 6}};
  mock_local_image_ctx.snap_info = {
    {6, librbd::SnapInfo{"snap", cls::rbd::UserSnapshotNamespace(), 0U, {}, 0U, 0U, utime_t()}}};
  MockEventPreprocessor event_preprocessor(mock_local_image_ctx,
                                           mock_remote_journaler,
                                           "local mirror uuid",
                                           &m_client_meta,
                                           m_threads->work_queue);

  librbd::journal::EventEntry event_entry{
    librbd::journal::SnapRenameEvent{0, 5, "snap", "new_snap"}};
  C_SaferCond ctx;
  event_preprocessor.preprocess(&event_entry, &ctx);
  ASSERT_EQ(0, ctx.wait());

  librbd::SnapSeqs expected_snap_seqs = {{5, 6}};
  ASSERT_EQ(expected_snap_seqs, m_client_meta.snap_seqs);

  librbd::journal::SnapRenameEvent *event =
    std::get_if<librbd::journal::SnapRenameEvent>(&event_entry.event);
  ASSERT_EQ(6U, event->snap_id);
}

TEST_F(TestMockImageReplayerJournalEventPreprocessor, PreprocessSnapRenameMissing) {
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  ::journal::MockJournaler mock_remote_journaler;

  expect_image_refresh(mock_local_image_ctx, 0);

  MockEventPreprocessor event_preprocessor(mock_local_image_ctx,
                                           mock_remote_journaler,
                                           "local mirror uuid",
                                           &m_client_meta,
                                           m_threads->work_queue);

  librbd::journal::EventEntry event_entry{
    librbd::journal::SnapRenameEvent{0, 5, "snap", "new_snap"}};
  C_SaferCond ctx;
  event_preprocessor.preprocess(&event_entry, &ctx);
  ASSERT_EQ(-ENOENT, ctx.wait());

  librbd::journal::SnapRenameEvent *event =
    std::get_if<librbd::journal::SnapRenameEvent>(&event_entry.event);
  ASSERT_EQ(CEPH_NOSNAP, event->snap_id);
}

TEST_F(TestMockImageReplayerJournalEventPreprocessor, PreprocessSnapRenameKnown) {
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  ::journal::MockJournaler mock_remote_journaler;

  expect_image_refresh(mock_local_image_ctx, 0);

  mock_local_image_ctx.snap_info = {
    {6, librbd::SnapInfo{"snap", cls::rbd::UserSnapshotNamespace(), 0U, {}, 0U, 0U, utime_t()}}};
  m_client_meta.snap_seqs = {{5, 6}};
  MockEventPreprocessor event_preprocessor(mock_local_image_ctx,
                                           mock_remote_journaler,
                                           "local mirror uuid",
                                           &m_client_meta,
                                           m_threads->work_queue);

  librbd::journal::EventEntry event_entry{
    librbd::journal::SnapRenameEvent{0, 5, "snap", "new_snap"}};
  C_SaferCond ctx;
  event_preprocessor.preprocess(&event_entry, &ctx);
  ASSERT_EQ(0, ctx.wait());

  librbd::SnapSeqs expected_snap_seqs = {{5, 6}};
  ASSERT_EQ(expected_snap_seqs, m_client_meta.snap_seqs);

  librbd::journal::SnapRenameEvent *event =
    std::get_if<librbd::journal::SnapRenameEvent>(&event_entry.event);
  ASSERT_EQ(6U, event->snap_id);
}

TEST_F(TestMockImageReplayerJournalEventPreprocessor, PreprocessRefreshError) {
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  ::journal::MockJournaler mock_remote_journaler;

  expect_image_refresh(mock_local_image_ctx, -EINVAL);

  MockEventPreprocessor event_preprocessor(mock_local_image_ctx,
                                           mock_remote_journaler,
                                           "local mirror uuid",
                                           &m_client_meta,
                                           m_threads->work_queue);

  librbd::journal::EventEntry event_entry{librbd::journal::RenameEvent{}};
  C_SaferCond ctx;
  event_preprocessor.preprocess(&event_entry, &ctx);
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerJournalEventPreprocessor, PreprocessClientUpdateError) {
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  ::journal::MockJournaler mock_remote_journaler;

  expect_image_refresh(mock_local_image_ctx, 0);
  expect_update_client(mock_remote_journaler, -EINVAL);

  mock_local_image_ctx.snap_ids = {{{cls::rbd::UserSnapshotNamespace(), "snap"}, 6}};
  mock_local_image_ctx.snap_info = {
    {6, librbd::SnapInfo{"snap", cls::rbd::UserSnapshotNamespace(), 0U, {}, 0U, 0U, utime_t()}}};
  MockEventPreprocessor event_preprocessor(mock_local_image_ctx,
                                           mock_remote_journaler,
                                           "local mirror uuid",
                                           &m_client_meta,
                                           m_threads->work_queue);

  librbd::journal::EventEntry event_entry{
    librbd::journal::SnapRenameEvent{0, 5, "snap", "new_snap"}};
  C_SaferCond ctx;
  event_preprocessor.preprocess(&event_entry, &ctx);
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd
