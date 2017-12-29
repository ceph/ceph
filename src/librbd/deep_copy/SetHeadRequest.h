// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_DEEP_COPY_SET_HEAD_REQUEST_H
#define CEPH_LIBRBD_DEEP_COPY_SET_HEAD_REQUEST_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "common/snap_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/Types.h"
#include <map>
#include <set>
#include <string>
#include <tuple>

class Context;

namespace librbd {
namespace deep_copy {

template <typename ImageCtxT = librbd::ImageCtx>
class SetHeadRequest {
public:
  static SetHeadRequest* create(ImageCtxT *image_ctx, uint64_t size,
                                const librbd::ParentSpec &parent_spec,
                                uint64_t parent_overlap,
                                Context *on_finish) {
    return new SetHeadRequest(image_ctx, size, parent_spec, parent_overlap,
                              on_finish);
  }

  SetHeadRequest(ImageCtxT *image_ctx, uint64_t size,
                 const librbd::ParentSpec &parent_spec, uint64_t parent_overlap,
                 Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v (skip if not needed)
   * SET_SIZE
   *    |
   *    v (skip if not needed)
   * REMOVE_PARENT
   *    |
   *    v (skip if not needed)
   * SET_PARENT
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT *m_image_ctx;
  uint64_t m_size;
  librbd::ParentSpec m_parent_spec;
  uint64_t m_parent_overlap;
  Context *m_on_finish;

  CephContext *m_cct;

  void send_set_size();
  void handle_set_size(int r);

  void send_remove_parent();
  void handle_remove_parent(int r);

  void send_set_parent();
  void handle_set_parent(int r);

  Context *start_lock_op();

  void finish(int r);
};

} // namespace deep_copy
} // namespace librbd

extern template class librbd::deep_copy::SetHeadRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_DEEP_COPY_SET_HEAD_REQUEST_H
