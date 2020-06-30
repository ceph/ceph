// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OBJECT_MAP_LOCK_REQUEST_H
#define CEPH_LIBRBD_OBJECT_MAP_LOCK_REQUEST_H

#include "include/buffer.h"
#include "cls/lock/cls_lock_types.h"
#include <map>

class Context;

namespace librbd {

class ImageCtx;

namespace object_map {

template <typename ImageCtxT = ImageCtx>
class LockRequest {
public:
  static LockRequest* create(ImageCtxT &image_ctx, Context *on_finish) {
    return new LockRequest(image_ctx, on_finish);
  }
  LockRequest(ImageCtxT &image_ctx, Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>         /------------------------------------- BREAK_LOCKS * * *
   *    |            |                                        ^             *
   *    |            |                                        |             *
   *    |            |                                        |             *
   *    |            v   (EBUSY && !broke_lock)               |             *
   *    \---------> LOCK_OBJECT_MAP * * * * * * * * * * * > GET_LOCK_INFO * *
   *                 |  *       ^                             *             *
   *                 |  *       *                             *             *
   *                 |  *       *  (ENOENT)                   *             *
   *                 |  *       * * * * * * * * * * * * * * * *             *
   *                 |  *                                                   *
   *                 |  * (other errors)                                    *
   *                 |  *                                                   *
   *                 v  v                         (other errors)            *
   *               <finish> < * * * * * * * * * * * * * * * * * * * * * * * *
   *
   * @endverbatim
   */

  ImageCtxT &m_image_ctx;
  Context *m_on_finish;

  bool m_broke_lock;
  std::map<rados::cls::lock::locker_id_t,
           rados::cls::lock::locker_info_t> m_lockers;
  bufferlist m_out_bl;

  void send_lock();
  Context *handle_lock(int *ret_val);

  void send_get_lock_info();
  Context *handle_get_lock_info(int *ret_val);

  void send_break_locks();
  Context *handle_break_locks(int *ret_val);
};

} // namespace object_map
} // namespace librbd

extern template class librbd::object_map::LockRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OBJECT_MAP_LOCK_REQUEST_H
