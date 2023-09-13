// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_PREPARE_REMOTE_IMAGE_REQUEST_H
#define RBD_MIRROR_IMAGE_REPLAYER_PREPARE_REMOTE_IMAGE_REQUEST_H

#include "include/buffer_fwd.h"
#include "include/rados/librados_fwd.hpp"
#include "cls/journal/cls_journal_types.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include "librbd/mirror/Types.h"
#include "tools/rbd_mirror/Types.h"
#include <string>

namespace journal { class Journaler; }
namespace journal { struct CacheManagerHandler; }
namespace librbd { struct ImageCtx; }
namespace librbd { namespace journal { struct MirrorPeerClientMeta; } }

struct Context;

namespace rbd {
namespace mirror {

template <typename> struct Threads;

namespace image_replayer {

template <typename> class StateBuilder;

template <typename ImageCtxT = librbd::ImageCtx>
class PrepareRemoteImageRequest {
public:
  typedef librbd::journal::TypeTraits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::Journaler Journaler;
  typedef librbd::journal::MirrorPeerClientMeta MirrorPeerClientMeta;

  static PrepareRemoteImageRequest *create(
      Threads<ImageCtxT> *threads,
      librados::IoCtx &local_io_ctx,
      librados::IoCtx &remote_io_ctx,
      const std::string &global_image_id,
      const std::string &local_mirror_uuid,
      const RemotePoolMeta& remote_pool_meta,
      ::journal::CacheManagerHandler *cache_manager_handler,
      StateBuilder<ImageCtxT>** state_builder,
      Context *on_finish) {
    return new PrepareRemoteImageRequest(threads, local_io_ctx, remote_io_ctx,
                                         global_image_id, local_mirror_uuid,
                                         remote_pool_meta,
                                         cache_manager_handler, state_builder,
                                         on_finish);
  }

  PrepareRemoteImageRequest(
      Threads<ImageCtxT> *threads,
      librados::IoCtx &local_io_ctx,
      librados::IoCtx &remote_io_ctx,
      const std::string &global_image_id,
      const std::string &local_mirror_uuid,
      const RemotePoolMeta& remote_pool_meta,
      ::journal::CacheManagerHandler *cache_manager_handler,
      StateBuilder<ImageCtxT>** state_builder,
      Context *on_finish)
    : m_threads(threads),
      m_local_io_ctx(local_io_ctx),
      m_remote_io_ctx(remote_io_ctx),
      m_global_image_id(global_image_id),
      m_local_mirror_uuid(local_mirror_uuid),
      m_remote_pool_meta(remote_pool_meta),
      m_cache_manager_handler(cache_manager_handler),
      m_state_builder(state_builder),
      m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_REMOTE_IMAGE_ID
   *    |
   *    v
   * GET_REMOTE_MIRROR_INFO
   *    |
   *    | (journal)
   *    \-----------> GET_CLIENT
   *    |                 |
   *    |                 v (skip if not needed)
   *    |             REGISTER_CLIENT
   *    |                 |
   *    |                 |
   *    |/----------------/
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  Threads<ImageCtxT> *m_threads;
  librados::IoCtx &m_local_io_ctx;
  librados::IoCtx &m_remote_io_ctx;
  std::string m_global_image_id;
  std::string m_local_mirror_uuid;
  RemotePoolMeta m_remote_pool_meta;
  ::journal::CacheManagerHandler *m_cache_manager_handler;
  StateBuilder<ImageCtxT>** m_state_builder;
  Context *m_on_finish;

  bufferlist m_out_bl;
  std::string m_remote_image_id;
  cls::rbd::MirrorImage m_mirror_image;
  librbd::mirror::PromotionState m_promotion_state =
    librbd::mirror::PROMOTION_STATE_UNKNOWN;
  std::string m_primary_mirror_uuid;

  // journal-based mirroring
  Journaler *m_remote_journaler = nullptr;
  cls::journal::Client m_client;

  void get_remote_image_id();
  void handle_get_remote_image_id(int r);

  void get_mirror_info();
  void handle_get_mirror_info(int r);

  void get_client();
  void handle_get_client(int r);

  void register_client();
  void handle_register_client(int r);

  void finalize_journal_state_builder(cls::journal::ClientState client_state,
                                      const MirrorPeerClientMeta& client_meta);
  void finalize_snapshot_state_builder();

  void finish(int r);
};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::PrepareRemoteImageRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_PREPARE_REMOTE_IMAGE_REQUEST_H
