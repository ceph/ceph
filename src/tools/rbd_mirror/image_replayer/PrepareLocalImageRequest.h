// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_PREPARE_LOCAL_IMAGE_REQUEST_H
#define RBD_MIRROR_IMAGE_REPLAYER_PREPARE_LOCAL_IMAGE_REQUEST_H

#include "include/buffer.h"
#include "include/rados/librados_fwd.hpp"
#include <string>

namespace librbd { struct ImageCtx; }

struct Context;
struct ContextWQ;

namespace rbd {
namespace mirror {
namespace image_replayer {

template <typename ImageCtxT = librbd::ImageCtx>
class PrepareLocalImageRequest {
public:
  static PrepareLocalImageRequest *create(librados::IoCtx &io_ctx,
                                          const std::string &global_image_id,
                                          std::string *local_image_id,
                                          std::string *local_image_name,
                                          std::string *tag_owner,
                                          ContextWQ *work_queue,
                                          Context *on_finish) {
    return new PrepareLocalImageRequest(io_ctx, global_image_id, local_image_id,
                                        local_image_name, tag_owner, work_queue,
                                        on_finish);
  }

  PrepareLocalImageRequest(librados::IoCtx &io_ctx,
                           const std::string &global_image_id,
                           std::string *local_image_id,
                           std::string *local_image_name,
                           std::string *tag_owner,
                           ContextWQ *work_queue,
                           Context *on_finish)
    : m_io_ctx(io_ctx), m_global_image_id(global_image_id),
      m_local_image_id(local_image_id), m_local_image_name(local_image_name),
      m_tag_owner(tag_owner), m_work_queue(work_queue), m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_LOCAL_IMAGE_ID
   *    |
   *    v
   * GET_LOCAL_IMAGE_NAME
   *    |
   *    v
   * GET_MIRROR_STATE
   *    |
   *    v
   * <finish>

   * @endverbatim
   */

  librados::IoCtx &m_io_ctx;
  std::string m_global_image_id;
  std::string *m_local_image_id;
  std::string *m_local_image_name;
  std::string *m_tag_owner;
  ContextWQ *m_work_queue;
  Context *m_on_finish;

  bufferlist m_out_bl;

  void get_local_image_id();
  void handle_get_local_image_id(int r);

  void get_local_image_name();
  void handle_get_local_image_name(int r);

  void get_mirror_state();
  void handle_get_mirror_state(int r);

  void get_tag_owner();
  void handle_get_tag_owner(int r);

  void finish(int r);

};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::PrepareLocalImageRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_PREPARE_LOCAL_IMAGE_REQUEST_H
