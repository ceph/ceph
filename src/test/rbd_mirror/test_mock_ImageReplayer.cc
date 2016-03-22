// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "librbd/journal/Replay.h"
#include "tools/rbd_mirror/ImageReplayer.h"
#include "tools/rbd_mirror/image_replayer/BootstrapRequest.h"
#include "tools/rbd_mirror/image_replayer/CloseImageRequest.h"
#include "tools/rbd_mirror/image_replayer/OpenLocalImageRequest.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockJournal.h"
#include "test/rbd_mirror/mock/MockJournaler.h"

namespace librbd {

struct MockImageReplayerJournal;

struct MockImageReplayerImageCtx : public MockImageCtx {
  MockImageReplayerJournal *journal = nullptr;
};

struct MockImageReplayerJournal : public MockJournal {
  MOCK_METHOD1(start_external_replay, int(journal::Replay<MockImageReplayerImageCtx> **));
  MOCK_METHOD0(stop_external_replay, void());
};

namespace journal {

template<>
struct Replay<MockImageReplayerImageCtx> {
  MOCK_METHOD3(process, void(bufferlist::iterator *, Context *, Context *));
  MOCK_METHOD1(flush, void(Context*));
  MOCK_METHOD2(shut_down, void(bool, Context*));
};

template <>
struct TypeTraits<MockImageReplayerImageCtx> {
  typedef ::journal::MockJournalerProxy Journaler;
  typedef ::journal::MockReplayEntryProxy ReplayEntry;
};

struct MirrorPeerClientMeta;

} // namespace journal
} // namespace librbd

namespace rbd {
namespace mirror {
namespace image_replayer {

template<>
struct BootstrapRequest<librbd::MockImageReplayerImageCtx> {
  static BootstrapRequest* s_instance;
  Context *on_finish = nullptr;

  static BootstrapRequest* create(librados::IoCtx &local_io_ctx,
                                  librados::IoCtx &remote_io_ctx,
                                  librbd::MockImageReplayerImageCtx **local_image_ctx,
                                  const std::string &local_image_name,
                                  const std::string &remote_image_id,
                                  ContextWQ *work_queue, SafeTimer *timer,
                                  Mutex *timer_lock,
                                  const std::string &mirror_uuid,
                                  ::journal::MockJournalerProxy *journaler,
                                  librbd::journal::MirrorPeerClientMeta *client_meta,
                                  Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  BootstrapRequest() {
    assert(s_instance == nullptr);
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

template<>
struct CloseImageRequest<librbd::MockImageReplayerImageCtx> {
  static CloseImageRequest* s_instance;
  Context *on_finish = nullptr;

  static CloseImageRequest* create(librbd::MockImageReplayerImageCtx **image_ctx,
                                   ContextWQ *work_queue, bool destroy_only,
                                   Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  CloseImageRequest() {
    assert(s_instance == nullptr);
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

template<>
struct OpenLocalImageRequest<librbd::MockImageReplayerImageCtx> {
  static OpenLocalImageRequest* s_instance;
  Context *on_finish = nullptr;

  static OpenLocalImageRequest* create(librados::IoCtx &local_io_ctx,
                                       librbd::MockImageReplayerImageCtx **local_image_ctx,
                                       const std::string &local_image_name,
                                       const std::string &local_image_id,
                                       ContextWQ *work_queue,
                                       Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  OpenLocalImageRequest() {
    assert(s_instance == nullptr);
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

BootstrapRequest<librbd::MockImageReplayerImageCtx>* BootstrapRequest<librbd::MockImageReplayerImageCtx>::s_instance = nullptr;
CloseImageRequest<librbd::MockImageReplayerImageCtx>* CloseImageRequest<librbd::MockImageReplayerImageCtx>::s_instance = nullptr;
OpenLocalImageRequest<librbd::MockImageReplayerImageCtx>* OpenLocalImageRequest<librbd::MockImageReplayerImageCtx>::s_instance = nullptr;

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

// template definitions
#include "tools/rbd_mirror/ImageReplayer.cc"
template class rbd::mirror::ImageReplayer<librbd::MockImageReplayerImageCtx>;

namespace rbd {
namespace mirror {

class TestMockImageReplayer : public TestMockFixture {
public:
  typedef ImageReplayer<librbd::MockImageReplayerImageCtx> MockImageReplayer;

  virtual void SetUp() {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_remote_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_remote_io_ctx, m_image_name, &m_remote_image_ctx));
  }

  librbd::ImageCtx *m_remote_image_ctx;
};

TEST_F(TestMockImageReplayer, Blah) {
}

} // namespace mirror
} // namespace rbd
