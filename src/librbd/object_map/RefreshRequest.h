// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OBJECT_MAP_REFRESH_REQUEST_H
#define CEPH_LIBRBD_OBJECT_MAP_REFRESH_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "common/bit_vector.hpp"

namespace librbd {

class ImageCtx;

namespace object_map {

template <typename ImageCtxT = ImageCtx>
class RefreshRequest {
public:
  RefreshRequest(ImageCtxT &image_ctx, ceph::BitVector<2> *object_map,
                 uint64_t snap_id, Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start> -----> LOCK (skip if snapshot)
   *    *             |
   *    *             v  (other errors)
   *    *           LOAD * * * * * * * > INVALIDATE ------------\
   *    *             |    *                                    |
   *    *             |    * (-EINVAL or too small)             |
   *    *             |    * * * * * * > INVALIDATE_AND_RESIZE  |
   *    *             |                      |              *   |
   *    *             |                      |              *   |
   *    *             |                      v              *   |
   *    *             |                    RESIZE           *   |
   *    *             |                      |              *   |
   *    *             |                      |  * * * * * * *   |
   *    *             |                      |  *               |
   *    *             |                      v  v               |
   *    *             \--------------------> LOCK <-------------/
   *    *                                     |
   *    v                                     v
   * INVALIDATE_AND_CLOSE ---------------> <finish>
   *
   * @endverbatim
   */

  ImageCtxT &m_image_ctx;
  ceph::BitVector<2> *m_object_map;
  uint64_t m_snap_id;
  Context *m_on_finish;

  uint64_t m_object_count;
  ceph::BitVector<2> m_on_disk_object_map;
  bool m_truncate_on_disk_object_map;
  bufferlist m_out_bl;

  void send_lock();
  Context *handle_lock(int *ret_val);

  void send_load();
  Context *handle_load(int *ret_val);

  void send_invalidate();
  Context *handle_invalidate(int *ret_val);

  void send_resize_invalidate();
  Context *handle_resize_invalidate(int *ret_val);

  void send_resize();
  Context *handle_resize(int *ret_val);

  void send_invalidate_and_close();
  Context *handle_invalidate_and_close(int *ret_val);

  void apply();
};

} // namespace object_map
} // namespace librbd

extern template class librbd::object_map::RefreshRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OBJECT_MAP_REFRESH_REQUEST_H
