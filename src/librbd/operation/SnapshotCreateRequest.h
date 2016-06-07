// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OPERATION_SNAPSHOT_CREATE_REQUEST_H
#define CEPH_LIBRBD_OPERATION_SNAPSHOT_CREATE_REQUEST_H

#include "librbd/operation/Request.h"
#include "librbd/parent_types.h"
#include <string>

class Context;

namespace librbd {

class ImageCtx;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class SnapshotCreateRequest : public Request<ImageCtxT> {
public:
  /**
   * Snap Create goes through the following state machine:
   *
   * @verbatim
   *
   *            <start>
   *               |
   *               v
   *           STATE_SUSPEND_REQUESTS
   *               |
   *               v
   *           STATE_SUSPEND_AIO * * * * * * * * * * * * *
   *               |                                     *
   *               v                                     *
   *           STATE_APPEND_OP_EVENT (skip if journal    *
   *               |                  disabled)          *
   *   (retry)     v                                     *
   *   . . . > STATE_ALLOCATE_SNAP_ID                    *
   *   .           |                                     *
   *   .           v                                     *
   *   . . . . STATE_CREATE_SNAP * * * * * * * * * *     *
   *               |                               *     *
   *               v                               *     *
   *           STATE_CREATE_OBJECT_MAP (skip if    *     *
   *               |                    disabled)  *     *
   *               |                               *     *
   *               |                               v     *
   *               |              STATE_RELEASE_SNAP_ID  *
   *               |                     |               *
   *               |                     v               *
   *               \----------------> <finish> < * * * * *
   *
   * @endverbatim
   *
   * The _CREATE_STATE state may repeat back to the _ALLOCATE_SNAP_ID state
   * if a stale snapshot context is allocated. If the create operation needs
   * to abort, the error path is followed to record the result in the journal
   * (if enabled) and bubble the originating error code back to the client.
   */
  SnapshotCreateRequest(ImageCtxT &image_ctx, Context *on_finish,
		        const std::string &snap_name, uint64_t journal_op_tid,
                        bool skip_object_map);

protected:
  virtual void send_op();
  virtual bool should_complete(int r) {
    return true;
  }
  virtual bool can_affect_io() const override {
    return true;
  }
  virtual journal::Event create_event(uint64_t op_tid) const {
    return journal::SnapCreateEvent(op_tid, m_snap_name);
  }

private:
  std::string m_snap_name;
  bool m_skip_object_map;

  int m_ret_val;

  uint64_t m_snap_id;
  uint64_t m_size;
  parent_info m_parent_info;

  void send_suspend_requests();
  Context *handle_suspend_requests(int *result);

  void send_suspend_aio();
  Context *handle_suspend_aio(int *result);

  void send_append_op_event();
  Context *handle_append_op_event(int *result);

  void send_allocate_snap_id();
  Context *handle_allocate_snap_id(int *result);

  void send_create_snap();
  Context *handle_create_snap(int *result);

  Context *send_create_object_map();
  Context *handle_create_object_map(int *result);

  void send_release_snap_id();
  Context *handle_release_snap_id(int *result);

  void update_snap_context();

  void save_result(int *result) {
    if (m_ret_val == 0 && *result < 0) {
      m_ret_val = *result;
    }
  }
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::SnapshotCreateRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_SNAPSHOT_CREATE_REQUEST_H
