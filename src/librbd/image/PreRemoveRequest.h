// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_PRE_REMOVE_REQUEST_H
#define CEPH_LIBRBD_IMAGE_PRE_REMOVE_REQUEST_H

#include "include/rados/librados.hpp"
#include "include/buffer.h"
#include "librbd/ImageCtx.h"
#include <list>
#include <map>

class Context;

namespace librbd {
namespace image {

template <typename ImageCtxT>
class PreRemoveRequest {
public:

  static PreRemoveRequest *create(ImageCtxT *image_ctx, bool force,
                                  Context *on_finish) {
    return new PreRemoveRequest(image_ctx, force, on_finish);
  }

  PreRemoveRequest(ImageCtxT *image_ctx, bool force, Context *on_finish)
    : m_image_ctx(image_ctx), m_force(force), m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   *       <start>
   *          |
   *          v
   *   CHECK EXCLUSIVE LOCK
   *          |
   *          v (skip if not needed)
   *  ACQUIRE EXCLUSIVE LOCK
   *          |
   *          v
   *   CHECK IMAGE WATCHERS
   *          |
   *          v
   *     CHECK GROUP
   *          |
   *          |   /------\
   *          |   |      |
   *          v   v      |
   *    REMOVE SNAPS ----/
   *          |
   *          v
   *      <finish>
   *
   * @endverbatim
   */

  ImageCtxT* m_image_ctx;
  bool m_force;
  Context* m_on_finish;

  decltype(m_image_ctx->exclusive_lock) m_exclusive_lock = nullptr;

  bufferlist m_out_bl;
  std::list<obj_watch_t> m_watchers;

  std::map<uint64_t, SnapInfo> m_snap_infos;

  void acquire_exclusive_lock();
  void handle_exclusive_lock(int r);
  void handle_exclusive_lock_force(int r);

  void validate_image_removal();
  void check_image_snaps();

  void list_image_watchers();
  void handle_list_image_watchers(int r);

  void check_image_watchers();

  void check_group();
  void handle_check_group(int r);

  void remove_snapshot();
  void handle_remove_snapshot(int r);

  void finish(int r);

};

} // namespace image
} // namespace librbd

extern template class librbd::image::PreRemoveRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_PRE_REMOVE_REQUEST_H
