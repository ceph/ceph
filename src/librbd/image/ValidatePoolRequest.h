// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_VALIDATE_POOL_REQUEST_H
#define CEPH_LIBRBD_IMAGE_VALIDATE_POOL_REQUEST_H

#include "include/rados/librados.hpp"
#include "include/buffer.h"

class CephContext;
class Context;
class ContextWQ;

namespace librbd {

struct ImageCtx;

namespace image {

template <typename ImageCtxT>
class ValidatePoolRequest {
public:
  static ValidatePoolRequest* create(librados::IoCtx& io_ctx,
                                     ContextWQ *op_work_queue,
                                     Context *on_finish) {
    return new ValidatePoolRequest(io_ctx, op_work_queue, on_finish);
  }

  ValidatePoolRequest(librados::IoCtx& io_ctx, ContextWQ *op_work_queue,
                      Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v           (overwrites validated)
   * READ RBD INFO  . . . . . . . . .
   *    |   .                       .
   *    |   . (snapshots validated) .
   *    |   . . . . . . . . . .     .
   *    v                     .     .
   * CREATE SNAPSHOT          .     .
   *    |                     .     .
   *    v                     .     .
   * WRITE RBD INFO           .     .
   *    |                     .     .
   *    v                     .     .
   * REMOVE SNAPSHOT          .     .
   *    |                     .     .
   *    v                     .     .
   * OVERWRITE RBD INFO < . . .     .
   *    |                           .
   *    v                           .
   * <finish>  < . . . . . .  . . . .`
   *
   * @endverbatim
   */

  librados::IoCtx m_io_ctx;
  CephContext* m_cct;
  ContextWQ* m_op_work_queue;
  Context* m_on_finish;

  int m_ret_val = 0;
  bufferlist m_out_bl;
  uint64_t m_snap_id = 0;

  void read_rbd_info();
  void handle_read_rbd_info(int r);

  void create_snapshot();
  void handle_create_snapshot(int r);

  void write_rbd_info();
  void handle_write_rbd_info(int r);

  void remove_snapshot();
  void handle_remove_snapshot(int r);

  void overwrite_rbd_info();
  void handle_overwrite_rbd_info(int r);

  void finish(int r);

};

} // namespace image
} // namespace librbd

extern template class librbd::image::ValidatePoolRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_VALIDATE_POOL_REQUEST_H
