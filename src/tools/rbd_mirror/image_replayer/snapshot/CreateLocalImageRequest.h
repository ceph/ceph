// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_SNAPSHOT_CREATE_LOCAL_IMAGE_REQUEST_H
#define RBD_MIRROR_IMAGE_REPLAYER_SNAPSHOT_CREATE_LOCAL_IMAGE_REQUEST_H

#include "include/rados/librados_fwd.hpp"
#include "tools/rbd_mirror/BaseRequest.h"
#include <string>

struct Context;
namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> class PoolMetaCache;
class ProgressContext;
template <typename> struct Threads;

namespace image_replayer {
namespace snapshot {

template <typename> class StateBuilder;

template <typename ImageCtxT>
class CreateLocalImageRequest : public BaseRequest {
public:
  typedef rbd::mirror::ProgressContext ProgressContext;

  static CreateLocalImageRequest* create(
      Threads<ImageCtxT>* threads,
      librados::IoCtx& local_io_ctx,
      ImageCtxT* remote_image_ctx,
      const std::string& global_image_id,
      PoolMetaCache<ImageCtxT>* pool_meta_cache,
      ProgressContext* progress_ctx,
      StateBuilder<ImageCtxT>* state_builder,
      Context* on_finish) {
    return new CreateLocalImageRequest(threads, local_io_ctx, remote_image_ctx,
                                       global_image_id, pool_meta_cache,
                                       progress_ctx, state_builder, on_finish);
  }

  CreateLocalImageRequest(
      Threads<ImageCtxT>* threads,
      librados::IoCtx& local_io_ctx,
      ImageCtxT* remote_image_ctx,
      const std::string& global_image_id,
      PoolMetaCache<ImageCtxT>* pool_meta_cache,
      ProgressContext* progress_ctx,
      StateBuilder<ImageCtxT>* state_builder,
      Context* on_finish)
    : BaseRequest(on_finish),
      m_threads(threads),
      m_local_io_ctx(local_io_ctx),
      m_remote_image_ctx(remote_image_ctx),
      m_global_image_id(global_image_id),
      m_pool_meta_cache(pool_meta_cache),
      m_progress_ctx(progress_ctx),
      m_state_builder(state_builder) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * DISABLE_MIRROR_IMAGE < * * * * * *
   *    |                             *
   *    v                             *
   * REMOVE_MIRROR_IMAGE              *
   *    |                             *
   *    v                             *
   * ADD_MIRROR_IMAGE                 *
   *    |                             *
   *    v               (id exists)   *
   * CREATE_LOCAL_IMAGE * * * * * * * *
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  Threads<ImageCtxT>* m_threads;
  librados::IoCtx& m_local_io_ctx;
  ImageCtxT* m_remote_image_ctx;
  std::string m_global_image_id;
  PoolMetaCache<ImageCtxT>* m_pool_meta_cache;
  ProgressContext* m_progress_ctx;
  StateBuilder<ImageCtxT>* m_state_builder;

  void disable_mirror_image();
  void handle_disable_mirror_image(int r);

  void remove_mirror_image();
  void handle_remove_mirror_image(int r);

  void add_mirror_image();
  void handle_add_mirror_image(int r);

  void create_local_image();
  void handle_create_local_image(int r);

  void update_progress(const std::string& description);

};

} // namespace snapshot
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::snapshot::CreateLocalImageRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_SNAPSHOT_CREATE_LOCAL_IMAGE_REQUEST_H
