// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_BOOTSTRAP_REQUEST_H
#define RBD_MIRROR_IMAGE_REPLAYER_BOOTSTRAP_REQUEST_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "cls/journal/cls_journal_types.h"
#include "librbd/journal/TypeTraits.h"
#include <string>

class Context;
class ContextWQ;
class Mutex;
class SafeTimer;
namespace journal { class Journaler; }
namespace librbd { class ImageCtx; }
namespace librbd { namespace journal { struct MirrorPeerClientMeta; } }

namespace rbd {
namespace mirror {
namespace image_replayer {

template <typename ImageCtxT = librbd::ImageCtx>
class BootstrapRequest {
public:
  typedef librbd::journal::TypeTraits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::Journaler Journaler;
  typedef librbd::journal::MirrorPeerClientMeta MirrorPeerClientMeta;

  static BootstrapRequest* create(librados::IoCtx &local_io_ctx,
                                  librados::IoCtx &remote_io_ctx,
                                  ImageCtxT **local_image_ctx,
                                  const std::string &local_image_name,
                                  const std::string &remote_image_id,
                                  ContextWQ *work_queue, SafeTimer *timer,
                                  Mutex *timer_lock,
                                  const std::string &mirror_uuid,
                                  Journaler *journaler,
                                  MirrorPeerClientMeta *client_meta,
                                  Context *on_finish) {
    return new BootstrapRequest(local_io_ctx, remote_io_ctx, local_image_ctx,
                                local_image_name, remote_image_id, work_queue,
                                timer, timer_lock, mirror_uuid, journaler,
                                client_meta, on_finish);
  }

  BootstrapRequest(librados::IoCtx &local_io_ctx,
                   librados::IoCtx &remote_io_ctx,
                   ImageCtxT **local_image_ctx,
                   const std::string &local_image_name,
                   const std::string &remote_image_id, ContextWQ *work_queue,
                   SafeTimer *timer, Mutex *timer_lock,
                   const std::string &mirror_uuid, Journaler *journaler,
                   MirrorPeerClientMeta *client_meta, Context *on_finish);
  ~BootstrapRequest();

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_CLIENT * * * * * * * * * * * * * * * *
   *    |                                     *
   *    v (skip if not needed)                * (error)
   * REGISTER_CLIENT  * * * * * * * * * * * * *
   *    |                                     *
   *    v                                     *
   * OPEN_REMOTE_IMAGE  * * * * * * * * * * * *
   *    |                                     *
   *    v                                     *
   * OPEN_LOCAL_IMAGE * * * * * * * * * * * * *
   *    |   .   ^                             *
   *    |   .   |                             *
   *    |   .   \-----------------------\     *
   *    |   .                           |     *
   *    |   . (image sync requested)    |     *
   *    |   . . > REMOVE_LOCAL_IMAGE  * * * * *
   *    |   .                   |       |     *
   *    |   . (image doesn't    |       |     *
   *    |   .  exist)           v       |     *
   *    |   . . > CREATE_LOCAL_IMAGE  * * * * *
   *    |             |                 |     *
   *    |             \-----------------/     *
   *    |                                     *
   *    v (skip if not needed)                *
   * UPDATE_CLIENT                            *
   *    |                                     *
   *    v (skip if not needed)                *
   * IMAGE_SYNC * * * > CLOSE_LOCAL_IMAGE     *
   *    |                         |           *
   *    |     /-------------------/           *
   *    |     |                               *
   *    v     v                               *
   * CLOSE_REMOTE_IMAGE < * * * * * * * * * * *
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */
  librados::IoCtx &m_local_io_ctx;
  librados::IoCtx &m_remote_io_ctx;
  ImageCtxT **m_local_image_ctx;
  std::string m_local_image_name;
  std::string m_local_image_id;
  std::string m_remote_image_id;
  ContextWQ *m_work_queue;
  SafeTimer *m_timer;
  Mutex *m_timer_lock;
  std::string m_mirror_uuid;
  Journaler *m_journaler;
  MirrorPeerClientMeta *m_client_meta;
  Context *m_on_finish;

  cls::journal::Client m_client;
  ImageCtxT *m_remote_image_ctx = nullptr;
  int m_ret_val = 0;

  void get_client();
  void handle_get_client(int r);

  void register_client();
  void handle_register_client(int r);

  void open_remote_image();
  void handle_open_remote_image(int r);

  void open_local_image();
  void handle_open_local_image(int r);

  void remove_local_image();
  void handle_remove_local_image(int r);

  void create_local_image();
  void handle_create_local_image(int r);

  void update_client();
  void handle_update_client(int r);

  void image_sync();
  void handle_image_sync(int r);

  void close_local_image();
  void handle_close_local_image(int r);

  void close_remote_image();
  void handle_close_remote_image(int r);

  void finish(int r);

  bool decode_client_meta();
};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::BootstrapRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_BOOTSTRAP_REQUEST_H
