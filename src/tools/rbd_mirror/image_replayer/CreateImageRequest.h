// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_CREATE_IMAGE_REQUEST_H
#define RBD_MIRROR_IMAGE_REPLAYER_CREATE_IMAGE_REQUEST_H

#include "include/int_types.h"
#include "include/types.h"
#include "include/rados/librados.hpp"
#include "librbd/parent_types.h"
#include <string>

class Context;
class ContextWQ;
namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {
namespace image_replayer {

template <typename ImageCtxT = librbd::ImageCtx>
class CreateImageRequest {
public:
  static CreateImageRequest *create(librados::IoCtx &local_io_ctx,
                                    ContextWQ *work_queue,
                                    const std::string &global_image_id,
                                    const std::string &remote_mirror_uuid,
                                    const std::string &local_image_name,
                                    ImageCtxT *remote_image_ctx,
                                    Context *on_finish) {
    return new CreateImageRequest(local_io_ctx, work_queue, global_image_id,
                                  remote_mirror_uuid, local_image_name,
                                  remote_image_ctx, on_finish);
  }

  CreateImageRequest(librados::IoCtx &local_io_ctx, ContextWQ *work_queue,
                     const std::string &global_image_id,
                     const std::string &remote_mirror_uuid,
                     const std::string &local_image_name,
                     ImageCtxT *remote_image_ctx,
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
   *                    OPEN_LOCAL_PARENT * * * * * * *   |         *
   *                        |                         *   |         *
   *                        v                         *   |         *
   *                    SET_LOCAL_PARENT_SNAP         *   |         *
   *                        |         *               *   |         *
   *                        v         *               *   |         *
   *                    CLONE_IMAGE   *               *   |         *
   *                        |         *               *   |         *
   *                        v         v               *   |         *
   *                    CLOSE_LOCAL_PARENT            *   |         *
   *                        |                         *   |         *
   *                        v                         *   |         *
   *                    CLOSE_REMOTE_PARENT < * * * * *   |         *
   *                        |                             v         *
   *                        \------------------------> <finish> < * *
   * @endverbatim
   */

  librados::IoCtx &m_local_io_ctx;
  ContextWQ *m_work_queue;
  std::string m_global_image_id;
  std::string m_remote_mirror_uuid;
  std::string m_local_image_name;
  ImageCtxT *m_remote_image_ctx;
  Context *m_on_finish;

  librados::IoCtx m_remote_parent_io_ctx;
  ImageCtxT *m_remote_parent_image_ctx = nullptr;
  librbd::parent_spec m_remote_parent_spec;

  librados::IoCtx m_local_parent_io_ctx;
  ImageCtxT *m_local_parent_image_ctx = nullptr;
  librbd::parent_spec m_local_parent_spec;

  bufferlist m_out_bl;
  std::string m_parent_global_image_id;
  std::string m_parent_pool_name;
  std::string m_parent_snap_name;
  int m_ret_val = 0;

  void create_image();
  void handle_create_image(int r);

  void get_parent_global_image_id();
  void handle_get_parent_global_image_id(int r);

  void get_local_parent_image_id();
  void handle_get_local_parent_image_id(int r);

  void open_remote_parent_image();
  void handle_open_remote_parent_image(int r);

  void open_local_parent_image();
  void handle_open_local_parent_image(int r);

  void set_local_parent_snap();
  void handle_set_local_parent_snap(int r);

  void clone_image();
  void handle_clone_image(int r);

  void close_local_parent_image();
  void handle_close_local_parent_image(int r);

  void close_remote_parent_image();
  void handle_close_remote_parent_image(int r);

  void error(int r);
  void finish(int r);

  int validate_parent();

};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::CreateImageRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_CREATE_IMAGE_REQUEST_H
