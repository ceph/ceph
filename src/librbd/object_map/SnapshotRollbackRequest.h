// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OBJECT_MAP_SNAPSHOT_ROLLBACK_REQUEST_H
#define CEPH_LIBRBD_OBJECT_MAP_SNAPSHOT_ROLLBACK_REQUEST_H

#include "include/int_types.h"
#include "librbd/object_map/Request.h"

class Context;

namespace librbd {

class ImageCtx;

namespace object_map {

class SnapshotRollbackRequest : public Request {
public:
  /**
   * Snapshot rollback goes through the following state machine:
   *
   * @verbatim
   *
   * <start>
   *    |
   *    v           (error)
   * STATE_READ_MAP * * * * > STATE_INVALIDATE_MAP
   *    |                         |
   *    v                         v
   * STATE_WRITE_MAP -------> <finish>
   *
   * @endverbatim
   *
   * If an error occurs within the READ_MAP state, the associated snapshot's
   * object map will be flagged as invalid.  Otherwise, an error from any state
   * will result in the HEAD object map being flagged as invalid via the base
   * class.
   */
  enum State {
    STATE_READ_MAP,
    STATE_INVALIDATE_MAP,
    STATE_WRITE_MAP
  };

  SnapshotRollbackRequest(ImageCtx &image_ctx, uint64_t snap_id,
                          Context *on_finish)
    : Request(image_ctx, CEPH_NOSNAP, on_finish),
      m_snap_id(snap_id), m_ret_val(0) {
    assert(snap_id != CEPH_NOSNAP);
  }

  virtual void send();

protected:
  virtual bool should_complete(int r);

private:
  State m_state;
  uint64_t m_snap_id;
  int m_ret_val;

  bufferlist m_read_bl;

  void send_read_map();
  void send_invalidate_map();
  void send_write_map();

};

} // namespace object_map
} // namespace librbd

#endif // CEPH_LIBRBD_OBJECT_MAP_SNAPSHOT_ROLLBACK_REQUEST_H
