// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_replayer/journal/PrepareReplayRequest.h"
#include "tools/rbd_mirror/image_replayer/journal/StateBuilder.h"
#include "test/journal/mock/MockJournaler.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockJournal.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
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
} // namespace rbd

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace journal {

template<>
struct StateBuilder<librbd::MockTestImageCtx> {
  StateBuilder(librbd::MockTestImageCtx& local_image_ctx,
               ::journal::MockJournaler& remote_journaler,
               const librbd::journal::MirrorPeerClientMeta& remote_client_meta)
    : local_image_ctx(&local_image_ctx),
      local_image_id(local_image_ctx.id),
      remote_journaler(&remote_journaler),
      remote_client_meta(remote_client_meta) {
  }

  librbd::MockTestImageCtx* local_image_ctx;
  std::string local_image_id;

  std::string remote_mirror_uuid = "remote mirror uuid";
  ::journal::MockJournaler* remote_journaler = nullptr;
  librbd::journal::MirrorPeerClientMeta remote_client_meta;
};

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

// template definitions
#include "tools/rbd_mirror/image_replayer/journal/PrepareReplayRequest.cc"

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace journal {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockImageReplayerJournalPrepareReplayRequest : public TestMockFixture {
public:
  typedef PrepareReplayRequest<librbd::MockTestImageCtx> MockPrepareReplayRequest;
  typedef StateBuilder<librbd::MockTestImageCtx> MockStateBuilder;
  typedef std::list<cls::journal::Tag> Tags;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &m_local_image_ctx));
  }

  void expect_journaler_get_client(::journal::MockJournaler &mock_journaler,
                                   const std::string &client_id,
                                   cls::journal::Client &client, int r) {
    EXPECT_CALL(mock_journaler, get_client(StrEq(client_id), _, _))
      .WillOnce(DoAll(WithArg<1>(Invoke([client](cls::journal::Client *out_client) {
                                          *out_client = client;
                                        })),
                      WithArg<2>(Invoke([this, r](Context *on_finish) {
                                          m_threads->work_queue->queue(on_finish, r);
                                        }))));
  }

  void expect_journaler_update_client(::journal::MockJournaler &mock_journaler,
                                      const librbd::journal::ClientData &client_data,
                                      int r) {
    bufferlist bl;
    encode(client_data, bl);

    EXPECT_CALL(mock_journaler, update_client(ContentsEqual(bl), _))
      .WillOnce(WithArg<1>(Invoke([this, r](Context *on_finish) {
                                    m_threads->work_queue->queue(on_finish, r);
                                  })));
  }

  void expect_journaler_get_tags(::journal::MockJournaler &mock_journaler,
                                 uint64_t tag_class, const Tags& tags,
                                 int r) {
    EXPECT_CALL(mock_journaler, get_tags(tag_class, _, _))
      .WillOnce(DoAll(WithArg<1>(Invoke([tags](Tags *out_tags) {
                                          *out_tags = tags;
                                        })),
                      WithArg<2>(Invoke([this, r](Context *on_finish) {
                                          m_threads->work_queue->queue(on_finish, r);
                                        }))));
  }

  void expect_journal_get_tag_tid(librbd::MockJournal &mock_journal,
                                  uint64_t tag_tid) {
    EXPECT_CALL(mock_journal, get_tag_tid()).WillOnce(Return(tag_tid));
  }

  void expect_journal_get_tag_data(librbd::MockJournal &mock_journal,
                                   const librbd::journal::TagData &tag_data) {
    EXPECT_CALL(mock_journal, get_tag_data()).WillOnce(Return(tag_data));
  }

  void expect_is_resync_requested(librbd::MockJournal &mock_journal,
                                  bool do_resync, int r) {
    EXPECT_CALL(mock_journal, is_resync_requested(_))
      .WillOnce(DoAll(SetArgPointee<0>(do_resync),
                      Return(r)));
  }

  bufferlist encode_tag_data(const librbd::journal::TagData &tag_data) {
    bufferlist bl;
    encode(tag_data, bl);
    return bl;
  }

  MockPrepareReplayRequest* create_request(
      MockStateBuilder& mock_state_builder,
      librbd::mirror::PromotionState remote_promotion_state,
      const std::string& local_mirror_uuid,
      bool* resync_requested, bool* syncing, Context* on_finish) {
    return new MockPrepareReplayRequest(
      local_mirror_uuid, remote_promotion_state, nullptr, &mock_state_builder,
      resync_requested, syncing, on_finish);
  }

  librbd::ImageCtx *m_local_image_ctx = nullptr;
};

TEST_F(TestMockImageReplayerJournalPrepareReplayRequest, Success) {
  InSequence seq;

  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;

  // check initial state
  expect_is_resync_requested(mock_journal, false, 0);
  expect_journal_get_tag_tid(mock_journal, 345);
  expect_journal_get_tag_data(mock_journal, {"remote mirror uuid"});

  // lookup remote image tag class
  librbd::journal::ClientData client_data{
    librbd::journal::ImageClientMeta{123}};
  cls::journal::Client client;
  encode(client_data, client.data);
  ::journal::MockJournaler mock_remote_journaler;
  expect_journaler_get_client(mock_remote_journaler,
                              librbd::Journal<>::IMAGE_CLIENT_ID,
                              client, 0);

  // single promotion event
  Tags tags = {
    {2, 123, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                              librbd::Journal<>::LOCAL_MIRROR_UUID,
                              true, 344, 99})},
  };
  expect_journaler_get_tags(mock_remote_journaler, 123, tags, 0);

  C_SaferCond ctx;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;
  mirror_peer_client_meta.image_id = mock_local_image_ctx.id;
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      mirror_peer_client_meta);
  bool resync_requested;
  bool syncing;
  auto request = create_request(
    mock_state_builder, librbd::mirror::PROMOTION_STATE_PRIMARY,
    "local mirror uuid", &resync_requested, &syncing, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_FALSE(resync_requested);
  ASSERT_FALSE(syncing);
}

TEST_F(TestMockImageReplayerJournalPrepareReplayRequest, NoLocalJournal) {
  InSequence seq;

  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  C_SaferCond ctx;
  ::journal::MockJournaler mock_remote_journaler;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;
  mirror_peer_client_meta.image_id = mock_local_image_ctx.id;
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      mirror_peer_client_meta);
  bool resync_requested;
  bool syncing;
  auto request = create_request(
    mock_state_builder, librbd::mirror::PROMOTION_STATE_PRIMARY,
    "local mirror uuid", &resync_requested, &syncing, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerJournalPrepareReplayRequest, ResyncRequested) {
  InSequence seq;

  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;

  // check initial state
  expect_is_resync_requested(mock_journal, true, 0);
  expect_journal_get_tag_tid(mock_journal, 345);
  expect_journal_get_tag_data(mock_journal, {"remote mirror uuid"});

  C_SaferCond ctx;
  ::journal::MockJournaler mock_remote_journaler;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;
  mirror_peer_client_meta.image_id = mock_local_image_ctx.id;
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      mirror_peer_client_meta);
  bool resync_requested;
  bool syncing;
  auto request = create_request(
    mock_state_builder, librbd::mirror::PROMOTION_STATE_PRIMARY,
    "local mirror uuid", &resync_requested, &syncing, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_TRUE(resync_requested);
  ASSERT_FALSE(syncing);
}

TEST_F(TestMockImageReplayerJournalPrepareReplayRequest, ResyncRequestedError) {
  InSequence seq;

  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;

  // check initial state
  expect_is_resync_requested(mock_journal, false, -EINVAL);

  C_SaferCond ctx;
  ::journal::MockJournaler mock_remote_journaler;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;
  mirror_peer_client_meta.image_id = mock_local_image_ctx.id;
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      mirror_peer_client_meta);
  bool resync_requested;
  bool syncing;
  auto request = create_request(
    mock_state_builder, librbd::mirror::PROMOTION_STATE_PRIMARY,
    "local mirror uuid", &resync_requested, &syncing, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerJournalPrepareReplayRequest, Syncing) {
  InSequence seq;

  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;

  // check initial state
  expect_is_resync_requested(mock_journal, false, 0);
  expect_journal_get_tag_tid(mock_journal, 345);
  expect_journal_get_tag_data(mock_journal, {"remote mirror uuid"});

  C_SaferCond ctx;
  ::journal::MockJournaler mock_remote_journaler;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  mirror_peer_client_meta.image_id = mock_local_image_ctx.id;
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      mirror_peer_client_meta);
  bool resync_requested;
  bool syncing;
  auto request = create_request(
    mock_state_builder, librbd::mirror::PROMOTION_STATE_PRIMARY,
    "local mirror uuid", &resync_requested, &syncing, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_FALSE(resync_requested);
  ASSERT_TRUE(syncing);
}

TEST_F(TestMockImageReplayerJournalPrepareReplayRequest, GetRemoteTagClassError) {
  InSequence seq;

  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;

  // check initial state
  expect_is_resync_requested(mock_journal, false, 0);
  expect_journal_get_tag_tid(mock_journal, 345);
  expect_journal_get_tag_data(mock_journal, {"remote mirror uuid"});

  // lookup remote image tag class
  librbd::journal::ClientData client_data{
    librbd::journal::ImageClientMeta{123}};
  cls::journal::Client client;
  encode(client_data, client.data);
  ::journal::MockJournaler mock_remote_journaler;
  expect_journaler_get_client(mock_remote_journaler,
                              librbd::Journal<>::IMAGE_CLIENT_ID,
                              client, -EINVAL);

  C_SaferCond ctx;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;
  mirror_peer_client_meta.image_id = mock_local_image_ctx.id;
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      mirror_peer_client_meta);
  bool resync_requested;
  bool syncing;
  auto request = create_request(
    mock_state_builder, librbd::mirror::PROMOTION_STATE_PRIMARY,
    "local mirror uuid", &resync_requested, &syncing, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerJournalPrepareReplayRequest, GetRemoteTagsError) {
  InSequence seq;

  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;

  // check initial state
  expect_is_resync_requested(mock_journal, false, 0);
  expect_journal_get_tag_tid(mock_journal, 345);
  expect_journal_get_tag_data(mock_journal, {"remote mirror uuid"});

  // lookup remote image tag class
  librbd::journal::ClientData client_data{
    librbd::journal::ImageClientMeta{123}};
  cls::journal::Client client;
  encode(client_data, client.data);
  ::journal::MockJournaler mock_remote_journaler;
  expect_journaler_get_client(mock_remote_journaler,
                              librbd::Journal<>::IMAGE_CLIENT_ID,
                              client, 0);

  // single promotion event
  Tags tags = {
    {2, 123, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                              librbd::Journal<>::LOCAL_MIRROR_UUID,
                              true, 344, 99})},
  };
  expect_journaler_get_tags(mock_remote_journaler, 123, tags, -EINVAL);

  C_SaferCond ctx;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;
  mirror_peer_client_meta.image_id = mock_local_image_ctx.id;
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      mirror_peer_client_meta);
  bool resync_requested;
  bool syncing;
  auto request = create_request(
    mock_state_builder, librbd::mirror::PROMOTION_STATE_PRIMARY,
    "local mirror uuid", &resync_requested, &syncing, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerJournalPrepareReplayRequest, LocalDemotedRemoteSyncingState) {
  InSequence seq;

  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;

  // check initial state
  expect_is_resync_requested(mock_journal, false, 0);
  expect_journal_get_tag_tid(mock_journal, 345);
  expect_journal_get_tag_data(mock_journal, {librbd::Journal<>::ORPHAN_MIRROR_UUID,
                                             "remote mirror uuid", true, 4, 1});

  // update client state
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta{
    mock_local_image_ctx.id};
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;
  librbd::journal::ClientData client_data;
  client_data.client_meta = mirror_peer_client_meta;
  ::journal::MockJournaler mock_remote_journaler;
  expect_journaler_update_client(mock_remote_journaler, client_data, 0);

  // lookup remote image tag class
  client_data = {librbd::journal::ImageClientMeta{123}};
  cls::journal::Client client;
  encode(client_data, client.data);
  expect_journaler_get_client(mock_remote_journaler,
                              librbd::Journal<>::IMAGE_CLIENT_ID,
                              client, 0);

  // remote demotion / promotion event
  Tags tags = {
    {2, 123, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                              librbd::Journal<>::LOCAL_MIRROR_UUID,
                              true, 1, 99})},
    {3, 123, encode_tag_data({librbd::Journal<>::ORPHAN_MIRROR_UUID,
                              librbd::Journal<>::LOCAL_MIRROR_UUID,
                              true, 2, 1})},
    {4, 123, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                              librbd::Journal<>::ORPHAN_MIRROR_UUID,
                              true, 3, 1})},
    {5, 123, encode_tag_data({librbd::Journal<>::ORPHAN_MIRROR_UUID,
                              librbd::Journal<>::LOCAL_MIRROR_UUID,
                              true, 4, 1})},
    {6, 123, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                              librbd::Journal<>::ORPHAN_MIRROR_UUID,
                              true, 5, 1})},
    {7, 123, encode_tag_data({librbd::Journal<>::ORPHAN_MIRROR_UUID,
                              librbd::Journal<>::LOCAL_MIRROR_UUID,
                              true, 6, 1})},
    {8, 123, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                              librbd::Journal<>::ORPHAN_MIRROR_UUID,
                              true, 7, 1})}
  };
  expect_journaler_get_tags(mock_remote_journaler, 123, tags, 0);

  C_SaferCond ctx;
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  mirror_peer_client_meta.image_id = mock_local_image_ctx.id;
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      mirror_peer_client_meta);
  bool resync_requested;
  bool syncing;
  auto request = create_request(
    mock_state_builder, librbd::mirror::PROMOTION_STATE_PRIMARY,
    "local mirror uuid", &resync_requested, &syncing, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_FALSE(resync_requested);
  ASSERT_FALSE(syncing);
}

TEST_F(TestMockImageReplayerJournalPrepareReplayRequest, UpdateClientError) {
  InSequence seq;

  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;

  // check initial state
  expect_is_resync_requested(mock_journal, false, 0);
  expect_journal_get_tag_tid(mock_journal, 345);
  expect_journal_get_tag_data(mock_journal, {"remote mirror uuid"});

  // lookup remote image tag class
  librbd::journal::ClientData client_data{
    librbd::journal::ImageClientMeta{123}};
  cls::journal::Client client;
  encode(client_data, client.data);
  ::journal::MockJournaler mock_remote_journaler;
  expect_journaler_get_client(mock_remote_journaler,
                              librbd::Journal<>::IMAGE_CLIENT_ID,
                              client, 0);

  // single promotion event
  Tags tags = {
    {2, 123, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                              librbd::Journal<>::LOCAL_MIRROR_UUID,
                              true, 344, 99})},
  };
  expect_journaler_get_tags(mock_remote_journaler, 123, tags, 0);

  C_SaferCond ctx;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;
  mirror_peer_client_meta.image_id = mock_local_image_ctx.id;
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      mirror_peer_client_meta);
  bool resync_requested;
  bool syncing;
  auto request = create_request(
    mock_state_builder, librbd::mirror::PROMOTION_STATE_PRIMARY,
    "local mirror uuid", &resync_requested, &syncing, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_FALSE(resync_requested);
  ASSERT_FALSE(syncing);
}

TEST_F(TestMockImageReplayerJournalPrepareReplayRequest, RemoteDemotePromote) {
  InSequence seq;

  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;

  // check initial state
  expect_is_resync_requested(mock_journal, false, 0);
  expect_journal_get_tag_tid(mock_journal, 345);
  expect_journal_get_tag_data(mock_journal, {"remote mirror uuid"});

  // lookup remote image tag class
  librbd::journal::ClientData client_data{
    librbd::journal::ImageClientMeta{123}};
  cls::journal::Client client;
  encode(client_data, client.data);
  ::journal::MockJournaler mock_remote_journaler;
  expect_journaler_get_client(mock_remote_journaler,
                              librbd::Journal<>::IMAGE_CLIENT_ID,
                              client, 0);

  // remote demotion / promotion event
  Tags tags = {
    {2, 123, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                              librbd::Journal<>::LOCAL_MIRROR_UUID,
                              true, 1, 99})},
    {3, 123, encode_tag_data({librbd::Journal<>::ORPHAN_MIRROR_UUID,
                              librbd::Journal<>::LOCAL_MIRROR_UUID,
                              true, 2, 1})},
    {4, 123, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                              librbd::Journal<>::ORPHAN_MIRROR_UUID,
                              true, 2, 1})},
    {5, 123, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                              librbd::Journal<>::ORPHAN_MIRROR_UUID,
                              true, 4, 369})}
  };
  expect_journaler_get_tags(mock_remote_journaler, 123, tags, 0);

  C_SaferCond ctx;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;
  mirror_peer_client_meta.image_id = mock_local_image_ctx.id;
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      mirror_peer_client_meta);
  bool resync_requested;
  bool syncing;
  auto request = create_request(
    mock_state_builder, librbd::mirror::PROMOTION_STATE_PRIMARY,
    "local mirror uuid", &resync_requested, &syncing, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_FALSE(resync_requested);
  ASSERT_FALSE(syncing);
}

TEST_F(TestMockImageReplayerJournalPrepareReplayRequest, MultipleRemoteDemotePromotes) {
  InSequence seq;

  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;

  // check initial state
  expect_is_resync_requested(mock_journal, false, 0);
  expect_journal_get_tag_tid(mock_journal, 345);
  expect_journal_get_tag_data(mock_journal, {librbd::Journal<>::ORPHAN_MIRROR_UUID,
                                             "remote mirror uuid", true, 4, 1});

  // lookup remote image tag class
  librbd::journal::ClientData client_data{
    librbd::journal::ImageClientMeta{123}};
  cls::journal::Client client;
  encode(client_data, client.data);
  ::journal::MockJournaler mock_remote_journaler;
  expect_journaler_get_client(mock_remote_journaler,
                              librbd::Journal<>::IMAGE_CLIENT_ID,
                              client, 0);

  // remote demotion / promotion event
  Tags tags = {
    {2, 123, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                              librbd::Journal<>::LOCAL_MIRROR_UUID,
                              true, 1, 99})},
    {3, 123, encode_tag_data({librbd::Journal<>::ORPHAN_MIRROR_UUID,
                              librbd::Journal<>::LOCAL_MIRROR_UUID,
                              true, 2, 1})},
    {4, 123, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                              librbd::Journal<>::ORPHAN_MIRROR_UUID,
                              true, 3, 1})},
    {5, 123, encode_tag_data({librbd::Journal<>::ORPHAN_MIRROR_UUID,
                              librbd::Journal<>::LOCAL_MIRROR_UUID,
                              true, 4, 1})},
    {6, 123, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                              librbd::Journal<>::ORPHAN_MIRROR_UUID,
                              true, 5, 1})},
    {7, 123, encode_tag_data({librbd::Journal<>::ORPHAN_MIRROR_UUID,
                              librbd::Journal<>::LOCAL_MIRROR_UUID,
                              true, 6, 1})},
    {8, 123, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                              librbd::Journal<>::ORPHAN_MIRROR_UUID,
                              true, 7, 1})}
  };
  expect_journaler_get_tags(mock_remote_journaler, 123, tags, 0);

  C_SaferCond ctx;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;
  mirror_peer_client_meta.image_id = mock_local_image_ctx.id;
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      mirror_peer_client_meta);
  bool resync_requested;
  bool syncing;
  auto request = create_request(
    mock_state_builder, librbd::mirror::PROMOTION_STATE_PRIMARY,
    "local mirror uuid", &resync_requested, &syncing, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_FALSE(resync_requested);
  ASSERT_FALSE(syncing);
}

TEST_F(TestMockImageReplayerJournalPrepareReplayRequest, LocalDemoteRemotePromote) {
  InSequence seq;

  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;

  // check initial state
  expect_is_resync_requested(mock_journal, false, 0);
  expect_journal_get_tag_tid(mock_journal, 346);
  expect_journal_get_tag_data(mock_journal,
                              {librbd::Journal<>::ORPHAN_MIRROR_UUID,
                               librbd::Journal<>::LOCAL_MIRROR_UUID,
                               true, 345, 1});

  // lookup remote image tag class
  librbd::journal::ClientData client_data{
    librbd::journal::ImageClientMeta{123}};
  cls::journal::Client client;
  encode(client_data, client.data);
  ::journal::MockJournaler mock_remote_journaler;
  expect_journaler_get_client(mock_remote_journaler,
                              librbd::Journal<>::IMAGE_CLIENT_ID,
                              client, 0);

  // remote demotion / promotion event
  Tags tags = {
    {2, 123, encode_tag_data({"local mirror uuid", "local mirror uuid",
                              true, 344, 99})},
    {3, 123, encode_tag_data({librbd::Journal<>::ORPHAN_MIRROR_UUID,
                              "local mirror uuid", true, 345, 1})},
    {4, 123, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                              librbd::Journal<>::ORPHAN_MIRROR_UUID,
                              true, 3, 1})}
  };
  expect_journaler_get_tags(mock_remote_journaler, 123, tags, 0);

  C_SaferCond ctx;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;
  mirror_peer_client_meta.image_id = mock_local_image_ctx.id;
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      mirror_peer_client_meta);
  bool resync_requested;
  bool syncing;
  auto request = create_request(
    mock_state_builder, librbd::mirror::PROMOTION_STATE_PRIMARY,
    "local mirror uuid", &resync_requested, &syncing, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_FALSE(resync_requested);
  ASSERT_FALSE(syncing);
}

TEST_F(TestMockImageReplayerJournalPrepareReplayRequest, SplitBrainForcePromote) {
  InSequence seq;

  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;

  // check initial state
  expect_is_resync_requested(mock_journal, false, 0);
  expect_journal_get_tag_tid(mock_journal, 345);
  expect_journal_get_tag_data(mock_journal, {librbd::Journal<>::LOCAL_MIRROR_UUID,
                                             librbd::Journal<>::ORPHAN_MIRROR_UUID,
                                             true, 344, 0});

  // lookup remote image tag class
  librbd::journal::ClientData client_data{
    librbd::journal::ImageClientMeta{123}};
  cls::journal::Client client;
  encode(client_data, client.data);
  ::journal::MockJournaler mock_remote_journaler;
  expect_journaler_get_client(mock_remote_journaler,
                              librbd::Journal<>::IMAGE_CLIENT_ID,
                              client, 0);

  // remote demotion / promotion event
  Tags tags = {
    {2, 123, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                              librbd::Journal<>::LOCAL_MIRROR_UUID,
                              true, 1, 99})},
    {3, 123, encode_tag_data({librbd::Journal<>::ORPHAN_MIRROR_UUID,
                              librbd::Journal<>::LOCAL_MIRROR_UUID,
                              true, 2, 1})}
  };
  expect_journaler_get_tags(mock_remote_journaler, 123, tags, 0);

  C_SaferCond ctx;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;
  mirror_peer_client_meta.image_id = mock_local_image_ctx.id;
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      mirror_peer_client_meta);
  bool resync_requested;
  bool syncing;
  auto request = create_request(
    mock_state_builder, librbd::mirror::PROMOTION_STATE_PRIMARY,
    "local mirror uuid", &resync_requested, &syncing, &ctx);
  request->send();
  ASSERT_EQ(-EEXIST, ctx.wait());
}

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd
