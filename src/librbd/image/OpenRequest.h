// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_OPEN_REQUEST_H
#define CEPH_LIBRBD_IMAGE_OPEN_REQUEST_H

#include "include/buffer.h"
#include <map>
#include <string>

class Context;

namespace librbd {

class ImageCtx;

namespace image {

template <typename ImageCtxT = ImageCtx>
class OpenRequest {
public:
  static OpenRequest *create(ImageCtxT *image_ctx, uint64_t flags,
                             Context *on_finish) {
    return new OpenRequest(image_ctx, flags, on_finish);
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    | (v1)
   *    |-----> V1_DETECT_HEADER
   *    |           |
   *    |           \-------------------------------\
   *    | (v2)                                      |
   *    \-----> V2_DETECT_HEADER                    |
   *                |                               |
   *                v                               |
   *            V2_GET_ID|NAME                      |
   *                |                               |
   *                v (skip if have name)           |
   *            V2_GET_NAME_FROM_TRASH              |
   *                |                               |
   *                v                               |
   *            V2_GET_INITIAL_METADATA             |
   *                |                               |
   *                v                               |
   *            V2_GET_STRIPE_UNIT_COUNT (skip if   |
   *                |                     disabled) |
   *                v                               |
   *            V2_GET_CREATE_TIMESTAMP             |
   *                |                               |
   *                v                               |
   *            V2_GET_ACCESS_MODIFIY_TIMESTAMP     |
   *                |                               |
   *                v                               |
   *            V2_GET_DATA_POOL --------------> REFRESH
   *                                                |
   *                                                v
   *                                             INIT_CACHE
   *                                                |
   *                                                v
   *                                             REGISTER_WATCH (skip if
   *                                                |            read-only)
   *                                                v
   *                                             SET_SNAP (skip if no snap)
   *                                                |
   *                                                v
   *                                             <finish>
   *                                                ^
   *     (on error)                                 |
   *    * * * * * * > CLOSE ------------------------/
   *
   * @endverbatim
   */

  OpenRequest(ImageCtxT *image_ctx, uint64_t flags, Context *on_finish);

  ImageCtxT *m_image_ctx;
  bool m_skip_open_parent_image;
  Context *m_on_finish;

  bufferlist m_out_bl;
  int m_error_result;

  void send_v1_detect_header();
  Context *handle_v1_detect_header(int *result);

  void send_v2_detect_header();
  Context *handle_v2_detect_header(int *result);

  void send_v2_get_id();
  Context *handle_v2_get_id(int *result);

  void send_v2_get_name();
  Context *handle_v2_get_name(int *result);

  void send_v2_get_name_from_trash();
  Context *handle_v2_get_name_from_trash(int *result);

  void send_v2_get_initial_metadata();
  Context *handle_v2_get_initial_metadata(int *result);

  void send_v2_get_stripe_unit_count();
  Context *handle_v2_get_stripe_unit_count(int *result);

  void send_v2_get_create_timestamp();
  Context *handle_v2_get_create_timestamp(int *result);

  void send_v2_get_access_modify_timestamp();
  Context *handle_v2_get_access_modify_timestamp(int *result);

  void send_v2_get_data_pool();
  Context *handle_v2_get_data_pool(int *result);

  void send_refresh();
  Context *handle_refresh(int *result);

  Context *send_init_cache(int *result);

  Context *send_register_watch(int *result);
  Context *handle_register_watch(int *result);

  Context *send_set_snap(int *result);
  Context *handle_set_snap(int *result);

  Context *finalize(int r);

  void send_close_image(int error_result);
  Context *handle_close_image(int *result);

};

} // namespace image
} // namespace librbd

extern template class librbd::image::OpenRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_OPEN_REQUEST_H
