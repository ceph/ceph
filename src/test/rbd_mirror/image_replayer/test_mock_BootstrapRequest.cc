// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "librbd/journal/TypeTraits.h"
#include "tools/rbd_mirror/ImageSync.h"
#include "tools/rbd_mirror/ImageSyncThrottler.h"
#include "tools/rbd_mirror/image_replayer/BootstrapRequest.h"
#include "tools/rbd_mirror/image_replayer/CloseImageRequest.h"
#include "tools/rbd_mirror/image_replayer/CreateImageRequest.h"
#include "tools/rbd_mirror/image_replayer/OpenImageRequest.h"
#include "tools/rbd_mirror/image_replayer/OpenLocalImageRequest.h"
#include "test/journal/mock/MockJournaler.h"
#include "test/librbd/mock/MockImageCtx.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
};

} // anonymous namespace

namespace journal {

template <>
struct TypeTraits<librbd::MockTestImageCtx> {
  typedef ::journal::MockJournaler Journaler;
};

} // namespace journal
} // namespace librbd

namespace rbd {
namespace mirror {

class ProgressContext;

template<>
struct ImageSync<librbd::MockTestImageCtx> {
  static ImageSync* s_instance;
  Context *on_finish = nullptr;

  static ImageSync* create(librbd::MockTestImageCtx *local_image_ctx,
                           librbd::MockTestImageCtx *remote_image_ctx,
                           SafeTimer *timer, Mutex *timer_lock,
                           const std::string &mirror_uuid,
                           ::journal::MockJournaler *journaler,
                           librbd::journal::MirrorPeerClientMeta *client_meta,
                           ContextWQ *work_queue, Context *on_finish,
                           ProgressContext *progress_ctx = nullptr) {
    assert(s_instance != nullptr);
    return s_instance;
  }

  ImageSync() {
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

ImageSync<librbd::MockTestImageCtx>* ImageSync<librbd::MockTestImageCtx>::s_instance = nullptr;

template<>
struct ImageSyncThrottler<librbd::MockTestImageCtx> {
  MOCK_METHOD10(start_sync, void(librbd::MockTestImageCtx *local_image_ctx,
                                 librbd::MockTestImageCtx *remote_image_ctx,
                                 SafeTimer *timer, Mutex *timer_lock,
                                 const std::string &mirror_uuid,
                                 ::journal::MockJournaler *journaler,
                                 librbd::journal::MirrorPeerClientMeta *client_meta,
                                 ContextWQ *work_queue, Context *on_finish,
                                 ProgressContext *progress_ctx));
  MOCK_METHOD2(cancel_sync, void(librados::IoCtx &local_io_ctx,
                                 const std::string& mirror_uuid));
};

namespace image_replayer {

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
struct CreateImageRequest<librbd::MockTestImageCtx> {
  static CreateImageRequest* s_instance;
  Context *on_finish = nullptr;

  static CreateImageRequest* create(librados::IoCtx &local_io_ctx,
                                    ContextWQ *work_queue,
                                    const std::string &global_image_id,
                                    const std::string &remote_mirror_uuid,
                                    const std::string &local_image_name,
                                    librbd::MockTestImageCtx *remote_image_ctx,
                                    Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  CreateImageRequest() {
    assert(s_instance == nullptr);
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

template<>
struct OpenImageRequest<librbd::MockTestImageCtx> {
  static OpenImageRequest* s_instance;
  Context *on_finish = nullptr;

  static OpenImageRequest* create(librados::IoCtx &io_ctx,
                                  librbd::MockTestImageCtx **image_ctx,
                                  const std::string &local_image_id,
                                  bool read_only, ContextWQ *work_queue,
                                  Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  OpenImageRequest() {
    assert(s_instance == nullptr);
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

template<>
struct OpenLocalImageRequest<librbd::MockTestImageCtx> {
  static OpenLocalImageRequest* s_instance;
  Context *on_finish = nullptr;

  static OpenLocalImageRequest* create(librados::IoCtx &local_io_ctx,
                                       librbd::MockTestImageCtx **local_image_ctx,
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

CloseImageRequest<librbd::MockTestImageCtx>*
  CloseImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
CreateImageRequest<librbd::MockTestImageCtx>*
  CreateImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
OpenImageRequest<librbd::MockTestImageCtx>*
  OpenImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
OpenLocalImageRequest<librbd::MockTestImageCtx>*
  OpenLocalImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

// template definitions
#include "tools/rbd_mirror/image_replayer/BootstrapRequest.cc"
template class rbd::mirror::image_replayer::BootstrapRequest<librbd::MockTestImageCtx>;

namespace rbd {
namespace mirror {
namespace image_replayer {

class TestMockImageReplayerBootstrapRequest : public TestMockFixture {
public:
  typedef BootstrapRequest<librbd::MockTestImageCtx> MockBootstrapRequest;

};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd
