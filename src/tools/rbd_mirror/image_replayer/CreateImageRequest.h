// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_CREATE_IMAGE_REQUEST_H
#define RBD_MIRROR_IMAGE_REPLAYER_CREATE_IMAGE_REQUEST_H

#include "include/int_types.h"
#include "include/types.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/Types.h"
#include <string>

class Context;
namespace librbd { class ImageCtx; }
namespace librbd { class ImageOptions; }

namespace rbd {
namespace mirror {

class PoolMetaCache;
template <typename> struct Threads;

namespace image_replayer {

template <typename ImageCtxT = librbd::ImageCtx>
class CreateImageRequest {
public:
  static CreateImageRequest *create(
      Threads<ImageCtxT> *threads,
      librados::IoCtx &local_io_ctx,
      const std::string &global_image_id,
      const std::string &remote_mirror_uuid,
      const std::string &local_image_name,
      const std::string &local_image_id,
      ImageCtxT *remote_image_ctx,
      PoolMetaCache* pool_meta_cache,
      cls::rbd::MirrorImageMode mirror_image_mode,
      Context *on_finish) {
    return new CreateImageRequest(threads, local_io_ctx, global_image_id,
                                  remote_mirror_uuid, local_image_name,
                                  local_image_id, remote_image_ctx,
                                  pool_meta_cache, mirror_image_mode,
                                  on_finish);
  }

  CreateImageRequest(
      Threads<ImageCtxT> *threads, librados::IoCtx &local_io_ctx,
      const std::string &global_image_id,
      const std::string &remote_mirror_uuid,
      const std::string &local_image_name,
      const std::string &local_image_id,
      ImageCtxT *remote_image_ctx,
      PoolMetaCache* pool_meta_cache,
      cls::rbd::MirrorImageMode mirror_image_mode,
      Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>  * * * * * * * * * * * * * * * * * * * * * * * * * * * *
   *    |                                                           *
   *    | (non-clone)                                               *
   *    |\------------> CREATE_IMAGE ---------------------\         * (error)
   *    |                                                 |         *
   *    | (clone)                                         |         *
   *    \-------------> GET_PARENT_GLOBAL_IMAGE_ID  * * * | * * *   *
   *                        |                             |       * *
   *                        v                             |         *
   *                    GET_LOCAL_PARENT_IMAGE_ID * * * * | * * *   *
   *                        |                             |       * *
   *                        v                             |         *
   *                    OPEN_REMOTE_PARENT  * * * * * * * | * * *   *
   *                        |                             |       * *
   *                        v                             |         *
   *                    CLONE_IMAGE                       |         *
   *                        |                             |         *
   *                        v                             |         *
   *                    CLOSE_REMOTE_PARENT               |         *
   *                        |                             v         *
   *                        \------------------------> <finish> < * *
   * @endverbatim
   */

  Threads<ImageCtxT> *m_threads;
  librados::IoCtx &m_local_io_ctx;
  std::string m_global_image_id;
  std::string m_remote_mirror_uuid;
  std::string m_local_image_name;
  std::string m_local_image_id;
  ImageCtxT *m_remote_image_ctx;
  PoolMetaCache* m_pool_meta_cache;
  cls::rbd::MirrorImageMode m_mirror_image_mode;
  Context *m_on_finish;

  librados::IoCtx m_remote_parent_io_ctx;
  ImageCtxT *m_remote_parent_image_ctx = nullptr;
  cls::rbd::ParentImageSpec m_remote_parent_spec;

  librados::IoCtx m_local_parent_io_ctx;
  cls::rbd::ParentImageSpec m_local_parent_spec;

  bufferlist m_out_bl;
  std::string m_parent_global_image_id;
  std::string m_parent_pool_name;
  int m_ret_val = 0;

  void create_image();
  void handle_create_image(int r);

  void get_parent_global_image_id();
  void handle_get_parent_global_image_id(int r);

  void get_local_parent_image_id();
  void handle_get_local_parent_image_id(int r);

  void open_remote_parent_image();
  void handle_open_remote_parent_image(int r);

  void clone_image();
  void handle_clone_image(int r);

  void close_remote_parent_image();
  void handle_close_remote_parent_image(int r);

  void error(int r);
  void finish(int r);

  int validate_parent();

  void populate_image_options(librbd::ImageOptions* image_options);

};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::CreateImageRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_CREATE_IMAGE_REQUEST_H
