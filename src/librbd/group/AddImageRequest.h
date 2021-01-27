// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_GROUP_ADD_IMAGE_REQUEST_H
#define CEPH_LIBRBD_GROUP_ADD_IMAGE_REQUEST_H

#include "include/int_types.h"
#include "include/types.h"
#include "include/rados/librados.hpp"
#include <string>

class Context;

namespace librbd {

struct ImageCtx;

namespace group {

template <typename ImageCtxT = librbd::ImageCtx>
class AddImageRequest {
public:
  static AddImageRequest *create(librados::IoCtx &group_io_ctx,
                                 const std::string &group_id,
                                 librados::IoCtx &image_io_ctx,
                                 const std::string &image_id,
                                 Context *on_finish) {
    return new AddImageRequest(group_io_ctx, group_id, image_io_ctx, image_id,
                               on_finish);
  }

  AddImageRequest(librados::IoCtx &group_io_ctx,
                  const std::string &group_id,
                  librados::IoCtx &image_io_ctx,
                  const std::string &image_id,
                  Context *on_finish)
    : m_group_io_ctx(group_io_ctx), m_group_id(group_id),
      m_image_io_ctx(image_io_ctx), m_image_id(image_id),
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
   *  PRE_LINK
   *    |
   *    v
   *  ADD_GROUP
   *    |
   *    v
   *  POST_LINK
   *    |
   *    v
   *  <finish>
   *
   * @endverbatim
   */

  librados::IoCtx &m_group_io_ctx;
  const std::string m_group_id;
  librados::IoCtx &m_image_io_ctx;
  const std::string m_image_id;
  Context *m_on_finish;

  int m_ret_val = 0;

  void pre_link();
  void handle_pre_link(int r);

  void add_group();
  void handle_add_group(int r);

  void post_link();
  void handle_post_link(int r);

  void finish(int r);
};

} // namespace group
} // namespace librbd

extern template class librbd::group::AddImageRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_GROUP_ADD_IMAGE_REQUEST_H
