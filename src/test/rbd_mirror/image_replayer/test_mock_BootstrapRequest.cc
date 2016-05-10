// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "librbd/journal/TypeTraits.h"
#include "tools/rbd_mirror/ImageSync.h"
#include "tools/rbd_mirror/image_replayer/BootstrapRequest.h"
#include "tools/rbd_mirror/image_replayer/CloseImageRequest.h"
#include "tools/rbd_mirror/image_replayer/OpenLocalImageRequest.h"
#include "test/journal/mock/MockJournaler.h"
#include "test/librbd/mock/MockImageCtx.h"

namespace librbd {
namespace journal {

template <>
struct TypeTraits<librbd::MockImageCtx> {
  typedef ::journal::MockJournaler Journaler;
};

} // namespace journal
} // namespace librbd

namespace rbd {
namespace mirror {

class ProgressContext;

template<>
struct ImageSync<librbd::MockImageCtx> {
  static ImageSync* s_instance;
  Context *on_finish = nullptr;

  static ImageSync* create(librbd::MockImageCtx *local_image_ctx,
                           librbd::MockImageCtx *remote_image_ctx,
                           SafeTimer *timer, Mutex *timer_lock,
                           const std::string &mirror_uuid,
                           ::journal::MockJournaler *journaler,
                           librbd::journal::MirrorPeerClientMeta *client_meta,
                           Context *on_finish,
                           ProgressContext *progress_ctx = nullptr) {
    assert(s_instance != nullptr);
    return s_instance;
  }

  ImageSync() {
    assert(s_instance == nullptr);
    s_instance = this;
  }

  MOCK_METHOD0(start, void());
};

ImageSync<librbd::MockImageCtx>* ImageSync<librbd::MockImageCtx>::s_instance = nullptr;

namespace image_replayer {

template<>
struct CloseImageRequest<librbd::MockImageCtx> {
  static CloseImageRequest* s_instance;
  Context *on_finish = nullptr;

  static CloseImageRequest* create(librbd::MockImageCtx **image_ctx,
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
struct OpenLocalImageRequest<librbd::MockImageCtx> {
  static OpenLocalImageRequest* s_instance;
  Context *on_finish = nullptr;

  static OpenLocalImageRequest* create(librados::IoCtx &local_io_ctx,
                                       librbd::MockImageCtx **local_image_ctx,
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

CloseImageRequest<librbd::MockImageCtx>* CloseImageRequest<librbd::MockImageCtx>::s_instance = nullptr;
OpenLocalImageRequest<librbd::MockImageCtx>* OpenLocalImageRequest<librbd::MockImageCtx>::s_instance = nullptr;

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

// template definitions
#include "tools/rbd_mirror/image_replayer/BootstrapRequest.cc"
template class rbd::mirror::image_replayer::BootstrapRequest<librbd::MockImageCtx>;

namespace rbd {
namespace mirror {
namespace image_replayer {

class TestMockImageReplayerBootstrapRequest : public TestMockFixture {
public:
  typedef BootstrapRequest<librbd::MockImageCtx> MockBootstrapRequest;

};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd
