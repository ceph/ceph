// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_TRASH_REMOVE_REQUEST_H
#define CEPH_LIBRBD_TRASH_REMOVE_REQUEST_H

#include "include/common_fwd.h"
#include "include/utime.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include <string>

class Context;

namespace librbd {

struct ImageCtx;
class ProgressContext;
namespace asio { struct ContextWQ; }

namespace trash {

template <typename ImageCtxT = librbd::ImageCtx>
class RemoveRequest {
public:
  static RemoveRequest* create(librados::IoCtx &io_ctx,
                               const std::string &image_id,
                               asio::ContextWQ *op_work_queue, bool force,
                               ProgressContext &prog_ctx, Context *on_finish) {
    return new RemoveRequest(io_ctx, image_id, op_work_queue, force, prog_ctx,
                             on_finish);
  }

  static RemoveRequest* create(librados::IoCtx &io_ctx, ImageCtxT *image_ctx,
                               asio::ContextWQ *op_work_queue, bool force,
                               ProgressContext &prog_ctx, Context *on_finish) {
    return new RemoveRequest(io_ctx, image_ctx, op_work_queue, force, prog_ctx,
                             on_finish);
  }


  RemoveRequest(librados::IoCtx &io_ctx, const std::string &image_id,
                asio::ContextWQ *op_work_queue, bool force,
                ProgressContext &prog_ctx, Context *on_finish)
    : m_io_ctx(io_ctx), m_image_id(image_id), m_op_work_queue(op_work_queue),
      m_force(force), m_prog_ctx(prog_ctx), m_on_finish(on_finish),
      m_cct(reinterpret_cast<CephContext *>(io_ctx.cct())) {
  }

  RemoveRequest(librados::IoCtx &io_ctx, ImageCtxT *image_ctx,
                asio::ContextWQ *op_work_queue, bool force,
                ProgressContext &prog_ctx, Context *on_finish)
    : m_io_ctx(io_ctx), m_image_ctx(image_ctx), m_image_id(m_image_ctx->id),
      m_op_work_queue(op_work_queue), m_force(force), m_prog_ctx(prog_ctx),
      m_on_finish(on_finish),
      m_cct(reinterpret_cast<CephContext *>(io_ctx.cct())) {
  }

  void send();

private:
  /*
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * SET_STATE (removing) * * * * * * *> CLOSE_IMAGE
   *    |                                    |
   *    v                                    |
   * REMOVE_IMAGE * * *> SET_STATE (normal)  |
   *    |                   |                |
   *    v                   |                |
   * REMOVE_TRASH_ENTRY     |                |
   *    |                   |                |
   *    v                   |                |
   * <finish> <-------------/<---------------/
   *
   * @endverbatim
   */

  librados::IoCtx &m_io_ctx;
  ImageCtxT *m_image_ctx = nullptr;
  std::string m_image_id;
  asio::ContextWQ *m_op_work_queue;
  bool m_force;
  ProgressContext &m_prog_ctx;
  Context *m_on_finish;

  CephContext *m_cct;

  cls::rbd::TrashImageState m_trash_set_state =
      cls::rbd::TRASH_IMAGE_STATE_REMOVING;
  cls::rbd::TrashImageState m_trash_expect_state =
      cls::rbd::TRASH_IMAGE_STATE_NORMAL;
  int m_ret_val = 0;

  void set_state();
  void handle_set_state(int r);

  void close_image();
  void handle_close_image(int r);

  void remove_image();
  void handle_remove_image(int r);

  void remove_trash_entry();
  void handle_remove_trash_entry(int r);

  void finish(int r);
};

} // namespace trash
} // namespace librbd

extern template class librbd::trash::RemoveRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_TRASH_REMOVE_REQUEST_H
