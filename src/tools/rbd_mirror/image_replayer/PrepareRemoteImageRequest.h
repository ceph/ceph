// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_PREPARE_REMOTE_IMAGE_REQUEST_H
#define RBD_MIRROR_IMAGE_REPLAYER_PREPARE_REMOTE_IMAGE_REQUEST_H

#include "include/buffer_fwd.h"
#include "include/rados/librados_fwd.hpp"
#include "cls/journal/cls_journal_types.h"
#include "journal/Settings.h"
#include "librbd/journal/TypeTraits.h"
#include <string>

namespace journal { class Journaler; }
namespace journal { class Settings; }
namespace librbd { struct ImageCtx; }
namespace librbd { namespace journal { struct MirrorPeerClientMeta; } }

struct Context;
struct ContextWQ;

namespace rbd {
namespace mirror {

template <typename> struct Threads;

namespace image_replayer {

template <typename ImageCtxT = librbd::ImageCtx>
class PrepareRemoteImageRequest {
public:
  typedef librbd::journal::TypeTraits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::Journaler Journaler;
  typedef librbd::journal::MirrorPeerClientMeta MirrorPeerClientMeta;

  static PrepareRemoteImageRequest *create(Threads<ImageCtxT> *threads,
                                           librados::IoCtx &remote_io_ctx,
                                           const std::string &global_image_id,
                                           const std::string &local_mirror_uuid,
                                           const std::string &local_image_id,
                                           const journal::Settings &settings,
                                           std::string *remote_mirror_uuid,
                                           std::string *remote_image_id,
                                           Journaler **remote_journaler,
                                           cls::journal::ClientState *client_state,
                                           MirrorPeerClientMeta *client_meta,
                                           Context *on_finish) {
    return new PrepareRemoteImageRequest(threads, remote_io_ctx,
                                         global_image_id, local_mirror_uuid,
                                         local_image_id, settings,
                                         remote_mirror_uuid, remote_image_id,
                                         remote_journaler, client_state,
                                         client_meta, on_finish);
  }

  PrepareRemoteImageRequest(Threads<ImageCtxT> *threads,
                           librados::IoCtx &remote_io_ctx,
                           const std::string &global_image_id,
                           const std::string &local_mirror_uuid,
                           const std::string &local_image_id,
                           const journal::Settings &journal_settings,
                           std::string *remote_mirror_uuid,
                           std::string *remote_image_id,
                           Journaler **remote_journaler,
                           cls::journal::ClientState *client_state,
                           MirrorPeerClientMeta *client_meta,
                           Context *on_finish)
    : m_threads(threads), m_remote_io_ctx(remote_io_ctx),
      m_global_image_id(global_image_id),
      m_local_mirror_uuid(local_mirror_uuid), m_local_image_id(local_image_id),
      m_journal_settings(journal_settings),
      m_remote_mirror_uuid(remote_mirror_uuid),
      m_remote_image_id(remote_image_id),
      m_remote_journaler(remote_journaler), m_client_state(client_state),
      m_client_meta(client_meta), m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_REMOTE_MIRROR_UUID
   *    |
   *    v
   * GET_REMOTE_IMAGE_ID
   *    |
   *    v
   * GET_CLIENT
   *    |
   *    v (skip if not needed)
   * REGISTER_CLIENT
   *    |
   *    v
   * <finish>

   * @endverbatim
   */

  Threads<ImageCtxT> *m_threads;
  librados::IoCtx &m_remote_io_ctx;
  std::string m_global_image_id;
  std::string m_local_mirror_uuid;
  std::string m_local_image_id;
  journal::Settings m_journal_settings;
  std::string *m_remote_mirror_uuid;
  std::string *m_remote_image_id;
  Journaler **m_remote_journaler;
  cls::journal::ClientState *m_client_state;
  MirrorPeerClientMeta *m_client_meta;
  Context *m_on_finish;

  bufferlist m_out_bl;
  cls::journal::Client m_client;

  void get_remote_mirror_uuid();
  void handle_get_remote_mirror_uuid(int r);

  void get_remote_image_id();
  void handle_get_remote_image_id(int r);

  void get_client();
  void handle_get_client(int r);

  void register_client();
  void handle_register_client(int r);

  void finish(int r);
};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::PrepareRemoteImageRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_PREPARE_REMOTE_IMAGE_REQUEST_H
