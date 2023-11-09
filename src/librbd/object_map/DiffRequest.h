// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OBJECT_MAP_DIFF_REQUEST_H
#define CEPH_LIBRBD_OBJECT_MAP_DIFF_REQUEST_H

#include "include/int_types.h"
#include "common/bit_vector.hpp"
#include "common/ceph_mutex.h"
#include "librbd/object_map/Types.h"
#include <set>

struct Context;

namespace librbd {

struct ImageCtx;

namespace object_map {

template <typename ImageCtxT>
class DiffRequest {
public:
  static DiffRequest* create(ImageCtxT* image_ctx, uint64_t snap_id_start,
                             uint64_t snap_id_end, bool diff_iterate_range,
                             BitVector<2>* object_diff_state,
                             Context* on_finish) {
    return new DiffRequest(image_ctx, snap_id_start, snap_id_end,
                           diff_iterate_range, object_diff_state, on_finish);
  }

  DiffRequest(ImageCtxT* image_ctx, uint64_t snap_id_start,
              uint64_t snap_id_end, bool diff_iterate_range,
              BitVector<2>* object_diff_state, Context* on_finish)
    : m_image_ctx(image_ctx), m_snap_id_start(snap_id_start),
      m_snap_id_end(snap_id_end), m_diff_iterate_range(diff_iterate_range),
      m_object_diff_state(object_diff_state), m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    |     /---------\
   *    |     |         |
   *    v     v         |
   * LOAD_OBJECT_MAP ---/
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */
  ImageCtxT* m_image_ctx;
  uint64_t m_snap_id_start;
  uint64_t m_snap_id_end;
  bool m_diff_iterate_range;
  BitVector<2>* m_object_diff_state;
  Context* m_on_finish;

  std::set<uint64_t> m_snap_ids;
  uint64_t m_current_snap_id = 0;
  bool m_ignore_enoent = false;

  uint64_t m_current_size = 0;

  BitVector<2> m_object_map;
  bool m_object_diff_state_valid = false;

  bufferlist m_out_bl;

  void load_object_map(std::shared_lock<ceph::shared_mutex>* image_locker);
  void handle_load_object_map(int r);

  void finish(int r);

};

} // namespace object_map
} // namespace librbd

extern template class librbd::object_map::DiffRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OBJECT_MAP_DIFF_REQUEST_H
