// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OBJECT_MAP_SNAPSHOT_REMOVE_REQUEST_H
#define CEPH_LIBRBD_OBJECT_MAP_SNAPSHOT_REMOVE_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "common/bit_vector.hpp"
#include "librbd/AsyncRequest.h"

namespace librbd {
namespace object_map {

class SnapshotRemoveRequest : public AsyncRequest<> {
public:
  /**
   * Snapshot rollback goes through the following state machine:
   *
   * @verbatim
   *
   * <start> -----------> STATE_LOAD_MAP ----\
   *    .                      *             |
   *    .                      * (error)     |
   *    . (invalid object map) v             |
   *    . . . > STATE_INVALIDATE_NEXT_MAP    |
   *    .                      |             |
   *    .                      |             |
   *    . (fast diff disabled) v             v
   *    . . . . . . . . . . > STATE_REMOVE_MAP
   *                                 |
   *                                 v
   *                             <finish>
   *
   * @endverbatim
   *
   * The _LOAD_MAP state is skipped if the fast diff feature is disabled.
   * If the fast diff feature is enabled and the snapshot is flagged as
   * invalid, the next snapshot / HEAD object mapis flagged as invalid;
   * otherwise, the state machine proceeds to remove the object map.
   */
  enum State {
    STATE_LOAD_MAP,
    STATE_REMOVE_SNAPSHOT,
    STATE_INVALIDATE_NEXT_MAP,
    STATE_REMOVE_MAP
  };

  SnapshotRemoveRequest(ImageCtx &image_ctx, ceph::BitVector<2> *object_map,
                        uint64_t snap_id, Context *on_finish)
    : AsyncRequest(image_ctx, on_finish), m_object_map(*object_map),
      m_snap_id(snap_id), m_next_snap_id(CEPH_NOSNAP) {
  }

  virtual void send();

protected:
  virtual bool should_complete(int r);

  virtual int filter_return_code(int r) const {
    if (m_state == STATE_REMOVE_MAP && r == -ENOENT) {
      return 0;
    }
    return r;
  }

private:
  State m_state;
  ceph::BitVector<2> &m_object_map;
  uint64_t m_snap_id;
  uint64_t m_next_snap_id;

  ceph::BitVector<2> m_snap_object_map;
  bufferlist m_out_bl;

  void send_load_map();
  void send_remove_snapshot();
  void send_invalidate_next_map();
  void send_remove_map();

  void compute_next_snap_id();
  void update_object_map();
};

} // namespace object_map
} // namespace librbd

#endif // CEPH_LIBRBD_OBJECT_MAP_SNAPSHOT_REMOVE_REQUEST_H
