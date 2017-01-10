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
  static OpenRequest *create(ImageCtxT *image_ctx, bool skip_open_parent,
                             Context *on_finish) {
    return new OpenRequest(image_ctx, skip_open_parent, on_finish);
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
   *                v                               |
   *            V2_GET_IMMUTABLE_METADATA           |
   *                |                               |
   *                v                               |
   *            V2_GET_STRIPE_UNIT_COUNT            |
   *                |                               |
   *                v                               v
   *      /---> V2_APPLY_METADATA -------------> REGISTER_WATCH (skip if
   *      |         |                               |            read-only)
   *      \---------/                               v
   *                                             REFRESH
   *                                                |
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

  OpenRequest(ImageCtxT *image_ctx, bool skip_open_parent, Context *on_finish);

  ImageCtxT *m_image_ctx;
  bool m_skip_open_parent_image;
  Context *m_on_finish;

  bufferlist m_out_bl;
  int m_error_result;

  std::string m_last_metadata_key;
  std::map<std::string, bufferlist> m_metadata;

  void send_v1_detect_header();
  Context *handle_v1_detect_header(int *result);

  void send_v2_detect_header();
  Context *handle_v2_detect_header(int *result);

  void send_v2_get_id();
  Context *handle_v2_get_id(int *result);

  void send_v2_get_name();
  Context *handle_v2_get_name(int *result);

  void send_v2_get_immutable_metadata();
  Context *handle_v2_get_immutable_metadata(int *result);

  void send_v2_get_stripe_unit_count();
  Context *handle_v2_get_stripe_unit_count(int *result);

  void send_v2_apply_metadata();
  Context *handle_v2_apply_metadata(int *result);

  void send_register_watch();
  Context *handle_register_watch(int *result);

  void send_refresh();
  Context *handle_refresh(int *result);

  Context *send_set_snap(int *result);
  Context *handle_set_snap(int *result);

  void send_close_image(int error_result);
  Context *handle_close_image(int *result);

};

} // namespace image
} // namespace librbd

extern template class librbd::image::OpenRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_OPEN_REQUEST_H
