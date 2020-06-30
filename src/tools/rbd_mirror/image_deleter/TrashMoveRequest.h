// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_DELETE_TRASH_MOVE_REQUEST_H
#define CEPH_RBD_MIRROR_IMAGE_DELETE_TRASH_MOVE_REQUEST_H

#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/mirror/Types.h"
#include <string>

struct Context;
namespace librbd {
struct ImageCtx;
namespace asio { struct ContextWQ; }
} // namespace librbd

namespace rbd {
namespace mirror {
namespace image_deleter {

template <typename ImageCtxT = librbd::ImageCtx>
class TrashMoveRequest {
public:
  static TrashMoveRequest* create(librados::IoCtx& io_ctx,
                                  const std::string& global_image_id,
                                  bool resync,
                                  librbd::asio::ContextWQ* op_work_queue,
                                  Context* on_finish) {
    return new TrashMoveRequest(io_ctx, global_image_id, resync, op_work_queue,
                                on_finish);
  }

  TrashMoveRequest(librados::IoCtx& io_ctx, const std::string& global_image_id,
                   bool resync, librbd::asio::ContextWQ* op_work_queue,
                   Context* on_finish)
    : m_io_ctx(io_ctx), m_global_image_id(global_image_id), m_resync(resync),
      m_op_work_queue(op_work_queue), m_on_finish(on_finish) {
  }

  void send();

private:
  /*
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_MIRROR_IMAGE_ID
   *    |
   *    v
   * GET_MIRROR_INFO
   *    |
   *    v
   * DISABLE_MIRROR_IMAGE
   *    |
   *    v
   * OPEN_IMAGE
   *    |
   *    v (skip if not needed)
   * RESET_JOURNAL
   *    |
   *    v (skip if not needed)
   * ACQUIRE_LOCK
   *    |
   *    v
   * TRASH_MOVE
   *    |
   *    v
   * REMOVE_MIRROR_IMAGE
   *    |
   *    v
   * CLOSE_IMAGE
   *    |
   *    v
   * NOTIFY_TRASH_ADD
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  librados::IoCtx &m_io_ctx;
  std::string m_global_image_id;
  bool m_resync;
  librbd::asio::ContextWQ *m_op_work_queue;
  Context *m_on_finish;

  ceph::bufferlist m_out_bl;
  std::string m_image_id;
  cls::rbd::MirrorImage m_mirror_image;
  librbd::mirror::PromotionState m_promotion_state;
  std::string m_primary_mirror_uuid;
  cls::rbd::TrashImageSpec m_trash_image_spec;
  ImageCtxT *m_image_ctx = nullptr;;
  int m_ret_val = 0;
  bool m_moved_to_trash = false;

  void get_mirror_image_id();
  void handle_get_mirror_image_id(int r);

  void get_mirror_info();
  void handle_get_mirror_info(int r);

  void disable_mirror_image();
  void handle_disable_mirror_image(int r);

  void open_image();
  void handle_open_image(int r);

  void reset_journal();
  void handle_reset_journal(int r);

  void acquire_lock();
  void handle_acquire_lock(int r);

  void trash_move();
  void handle_trash_move(int r);

  void remove_mirror_image();
  void handle_remove_mirror_image(int r);

  void close_image();
  void handle_close_image(int r);

  void notify_trash_add();
  void handle_notify_trash_add(int r);

  void finish(int r);

};

} // namespace image_deleter
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_deleter::TrashMoveRequest<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_IMAGE_DELETE_TRASH_WATCHER_H
