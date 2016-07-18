// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "librbd/journal/Replay.h"
#include "tools/rbd_mirror/ImageReplayer.h"
#include "tools/rbd_mirror/image_replayer/BootstrapRequest.h"
#include "tools/rbd_mirror/image_replayer/CloseImageRequest.h"
#include "tools/rbd_mirror/ImageSyncThrottler.h"
#include "test/journal/mock/MockJournaler.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockJournal.h"

namespace librbd {

namespace {

struct MockTestJournal;

struct MockTestImageCtx : public MockImageCtx {
  MockTestJournal *journal = nullptr;
};

struct MockTestJournal : public MockJournal {
  MOCK_METHOD3(start_external_replay, void(journal::Replay<MockTestImageCtx> **,
                                           Context *on_finish,
                                           Context *on_close_request));
  MOCK_METHOD0(stop_external_replay, void());
};

} // anonymous namespace

namespace journal {

template<>
struct Replay<MockTestImageCtx> {
  MOCK_METHOD3(process, void(bufferlist::iterator *, Context *, Context *));
  MOCK_METHOD1(flush, void(Context*));
  MOCK_METHOD2(shut_down, void(bool, Context*));
};

template <>
struct TypeTraits<MockTestImageCtx> {
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
struct BootstrapRequest<librbd::MockTestImageCtx> {
  static BootstrapRequest* s_instance;
  Context *on_finish = nullptr;

  static BootstrapRequest* create(librados::IoCtx &local_io_ctx,
        librados::IoCtx &remote_io_ctx,
        rbd::mirror::ImageSyncThrottlerRef<librbd::MockTestImageCtx> image_sync_throttler,
        librbd::MockTestImageCtx **local_image_ctx,
        const std::string &local_image_name,
        const std::string &remote_image_id,
        const std::string &global_image_id,
        ContextWQ *work_queue, SafeTimer *timer,
        Mutex *timer_lock,
        const std::string &local_mirror_uuid,
        const std::string &remote_mirror_uuid,
        ::journal::MockJournalerProxy *journaler,
        librbd::journal::MirrorPeerClientMeta *client_meta,
        Context *on_finish,
        rbd::mirror::ProgressContext *progress_ctx = nullptr) {
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  BootstrapRequest() {
    assert(s_instance == nullptr);
    s_instance = this;
  }

  void put() {
  }

  void get() {
  }

  MOCK_METHOD0(send, void());
  MOCK_METHOD0(cancel, void());
};

template<>
struct CloseImageRequest<librbd::MockTestImageCtx> {
  static CloseImageRequest* s_instance;
  Context *on_finish = nullptr;

  static CloseImageRequest* create(librbd::MockTestImageCtx **image_ctx,
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
struct ReplayStatusFormatter<librbd::MockTestImageCtx> {
  static ReplayStatusFormatter* s_instance;

  static ReplayStatusFormatter* create(::journal::MockJournalerProxy *journaler,
				       const std::string &mirror_uuid) {
    assert(s_instance != nullptr);
    return s_instance;
  }

  ReplayStatusFormatter() {
    assert(s_instance == nullptr);
    s_instance = this;
  }

  MOCK_METHOD2(get_or_send_update, bool(std::string *description, Context *on_finish));
};

BootstrapRequest<librbd::MockTestImageCtx>* BootstrapRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
CloseImageRequest<librbd::MockTestImageCtx>* CloseImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
ReplayStatusFormatter<librbd::MockTestImageCtx>* ReplayStatusFormatter<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

// template definitions
#include "tools/rbd_mirror/ImageReplayer.cc"
template class rbd::mirror::ImageReplayer<librbd::MockTestImageCtx>;

namespace rbd {
namespace mirror {

class TestMockImageReplayer : public TestMockFixture {
public:
  typedef ImageReplayer<librbd::MockTestImageCtx> MockImageReplayer;

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
