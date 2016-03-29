// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OBJECT_MAP_SNAPSHOT_CREATE_REQUEST_H
#define CEPH_LIBRBD_OBJECT_MAP_SNAPSHOT_CREATE_REQUEST_H

#include "include/int_types.h"
#include "common/bit_vector.hpp"
#include "librbd/object_map/Request.h"

class Context;

namespace librbd {

class ImageCtx;

namespace object_map {

class SnapshotCreateRequest : public Request {
public:
  /**
   * Snapshot create goes through the following state machine:
   *
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * STATE_READ_MAP
   *    |
   *    v            (skip)
   * STATE_WRITE_MAP . . . . . . .
   *    |                        .
   *    v                        v
   * STATE_ADD_SNAPSHOT ---> <finish>
   *
   * @endverbatim
   *
   * The _ADD_SNAPSHOT state is skipped if the FAST_DIFF feature isn't enabled.
   */
  enum State {
    STATE_READ_MAP,
    STATE_WRITE_MAP,
    STATE_ADD_SNAPSHOT
  };

  SnapshotCreateRequest(ImageCtx &image_ctx, ceph::BitVector<2> *object_map,
                        uint64_t snap_id, Context *on_finish)
    : Request(image_ctx, snap_id, on_finish),
      m_object_map(*object_map), m_ret_val(0) {
  }

  virtual void send();

protected:
  virtual bool should_complete(int r);

private:
  State m_state;
  ceph::BitVector<2> &m_object_map;

  bufferlist m_read_bl;
  int m_ret_val;

  void send_read_map();
  void send_write_map();
  bool send_add_snapshot();

  void update_object_map();

};

} // namespace object_map
} // namespace librbd

#endif // CEPH_LIBRBD_OBJECT_MAP_SNAPSHOT_CREATE_REQUEST_H
