// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_JOURNAL_CREATE_LOCAL_IMAGE_REQUEST_H
#define RBD_MIRROR_IMAGE_REPLAYER_JOURNAL_CREATE_LOCAL_IMAGE_REQUEST_H

#include "include/rados/librados_fwd.hpp"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include <string>

struct Context;
namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

class ProgressContext;
template <typename> struct Threads;

namespace image_replayer {
namespace journal {

template <typename ImageCtxT>
class CreateLocalImageRequest {
public:
  typedef librbd::journal::MirrorPeerClientMeta MirrorPeerClientMeta;
  typedef librbd::journal::TypeTraits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::Journaler Journaler;
  typedef rbd::mirror::ProgressContext ProgressContext;

  static CreateLocalImageRequest* create(
      Threads<ImageCtxT>* threads, librados::IoCtx& local_io_ctx,
      ImageCtxT* remote_image_ctx, Journaler* remote_journaler,
      const std::string& global_image_id,
      const std::string& remote_mirror_uuid,
      MirrorPeerClientMeta* client_meta, ProgressContext* progress_ctx,
      std::string* local_image_id, Context* on_finish) {
    return new CreateLocalImageRequest(threads, local_io_ctx, remote_image_ctx,
                                       remote_journaler, global_image_id,
                                       remote_mirror_uuid, client_meta,
                                       progress_ctx, local_image_id, on_finish);
  }

  CreateLocalImageRequest(
      Threads<ImageCtxT>* threads, librados::IoCtx& local_io_ctx,
      ImageCtxT* remote_image_ctx, Journaler* remote_journaler,
      const std::string& global_image_id,
      const std::string& remote_mirror_uuid,
      MirrorPeerClientMeta* client_meta, ProgressContext* progress_ctx,
      std::string* local_image_id, Context* on_finish)
    : m_threads(threads), m_local_io_ctx(local_io_ctx),
      m_remote_image_ctx(remote_image_ctx),
      m_remote_journaler(remote_journaler),
      m_global_image_id(global_image_id),
      m_remote_mirror_uuid(remote_mirror_uuid), m_client_meta(client_meta),
      m_progress_ctx(progress_ctx), m_local_image_id(local_image_id),
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
   * UNREGISTER_CLIENT
   *    |
   *    v
   * REGISTER_CLIENT
   *    |
   *    |     . . . . . . . . . UPDATE_CLIENT_IMAGE
   *    |     .                         ^
   *    v     v         (id exists)     *
   * CREATE_LOCAL_IMAGE * * * * * * * * *
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  Threads<ImageCtxT>* m_threads;
  librados::IoCtx& m_local_io_ctx;
  ImageCtxT* m_remote_image_ctx;
  Journaler* m_remote_journaler;
  std::string m_global_image_id;
  std::string m_remote_mirror_uuid;
  MirrorPeerClientMeta* m_client_meta;
  ProgressContext* m_progress_ctx;
  std::string* m_local_image_id;
  Context* m_on_finish;

  void unregister_client();
  void handle_unregister_client(int r);

  void register_client();
  void handle_register_client(int r);

  void create_local_image();
  void handle_create_local_image(int r);

  void update_client_image();
  void handle_update_client_image(int r);

  void finish(int r);

  void update_progress(const std::string& description);

};

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::journal::CreateLocalImageRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_JOURNAL_CREATE_LOCAL_IMAGE_REQUEST_H
