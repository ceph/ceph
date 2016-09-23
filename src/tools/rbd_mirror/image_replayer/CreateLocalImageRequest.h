// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_CREATE_LOCAL_IMAGE_REQUEST_H
#define RBD_MIRROR_IMAGE_REPLAYER_CREATE_LOCAL_IMAGE_REQUEST_H

#include "include/int_types.h"
#include "librbd/internal.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_types.h"

using librados::IoCtx;

class Context;
class ContextWQ;
class CephContext;

namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {
namespace image_replayer {

template<typename ImageCtxT = librbd::ImageCtx>
class CreateLocalImageRequest {
public:
  static CreateLocalImageRequest* create(IoCtx &local_io_ctx,
                                         std::string *local_image_id,
                                         const std::string &local_image_name,
                                         const std::string &global_image_id,
                                         const std::string &remote_mirror_uuid,
                                         ImageCtxT *remote_image_ctx,
                                         ContextWQ *op_work_queue,
                                         Context *on_finish) {
    return new CreateLocalImageRequest(local_io_ctx, local_image_id, local_image_name,
                                       global_image_id, remote_mirror_uuid,
                                       remote_image_ctx, op_work_queue, on_finish);
  }

  void send();

private:
  CreateLocalImageRequest(IoCtx &local_io_ctx,
                          std::string *local_image_id,
                          const std::string &local_image_name,
                          const std::string &global_image_id,
                          const std::string &remote_mirror_uuid,
                          ImageCtxT *remote_image_ctx,
                          ContextWQ *op_work_queue,
                          Context *on_finish);

  IoCtx m_local_io_ctx;
  std::string *m_local_image_id;
  std::string m_local_image_name;
  std::string m_global_image_id;
  std::string m_remote_mirror_uuid;
  ImageCtxT* m_remote_image_ctx;
  ContextWQ *m_op_work_queue;
  Context *m_on_finish;

  int m_ret_val;
  CephContext *m_cct;
  bufferlist m_out_bl;
  std::string m_curr_image_id;
  cls::rbd::MirrorImage m_mirror_image;
  librbd::NoOpProgressContext m_no_op_prog_ctx;

  void get_local_image_state();
  Context* handle_get_local_image_state(int *result);

  void remove_local_image_continue();
  Context* handle_remove_local_image_continue(int *result);

  void create_local_image_checkpoint_begin();
  Context* handle_create_local_image_checkpoint_begin(int *result);

  void create_local_image();
  Context* handle_create_local_image(int *result);

  void create_local_image_checkpoint_remove();
  Context* handle_create_local_image_checkpoint_remove(int *result);

  void create_local_image_checkpoint_end();
  Context* handle_create_local_image_checkpoint_end(int *result);

  void send_notify_watcher();
  Context* handle_notify_watcher(int *result);

  void remove_local_image_done();
  Context* handle_remove_local_image_done(int *result);
};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::CreateLocalImageRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_CREATE_LOCAL_IMAGE_REQUEST_H
