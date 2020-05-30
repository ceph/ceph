// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OPERATION_SNAPSHOT_CREATE_REQUEST_H
#define CEPH_LIBRBD_OPERATION_SNAPSHOT_CREATE_REQUEST_H

#include "cls/rbd/cls_rbd_types.h"
#include "librbd/Types.h"
#include "librbd/operation/Request.h"
#include <string>

class Context;

namespace librbd {

class ImageCtx;
class ProgressContext;

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
   *           STATE_NOTIFY_QUIESCE  * * * * * * * * * * * * *
   *               |                                         *
   *               v                                         *
   *           STATE_SUSPEND_REQUESTS                        *
   *               |                                         *
   *               v                                         *
   *           STATE_SUSPEND_AIO * * * * * * * * * * * * * * *
   *               |                                         *
   *               v                                         *
   *           STATE_APPEND_OP_EVENT (skip if journal        *
   *               |                  disabled)              *
   *   (retry)     v                                         *
   *   . . . > STATE_ALLOCATE_SNAP_ID                        *
   *   .           |                                         *
   *   .           v                                         *
   *   . . . . STATE_CREATE_SNAP * * * * * * * * * * *       *
   *               |                                 *       *
   *               v                                 *       *
   *           STATE_CREATE_OBJECT_MAP (skip if      *       *
   *               |                    disabled)    *       *
   *               v                                 *       *
   *           STATE_CREATE_IMAGE_STATE (skip if     *       *
   *               |                     not mirror  *       *
   *               |                     snapshot)   *       *
   *               |                                 v       *
   *               |              STATE_RELEASE_SNAP_ID      *
   *               |                     |                   *
   *               |                     v                   *
   *               \------------> STATE_NOTIFY_UNQUIESCE < * *
   *                                     |
   *                                     v
   *                                  <finish>
   * @endverbatim
   *
   * The _CREATE_STATE state may repeat back to the _ALLOCATE_SNAP_ID state
   * if a stale snapshot context is allocated. If the create operation needs
   * to abort, the error path is followed to record the result in the journal
   * (if enabled) and bubble the originating error code back to the client.
   */
  SnapshotCreateRequest(ImageCtxT &image_ctx, Context *on_finish,
                        const cls::rbd::SnapshotNamespace &snap_namespace,
                        const std::string &snap_name, uint64_t journal_op_tid,
                        uint64_t flags, ProgressContext &prog_ctx);

protected:
  void send_op() override;
  bool should_complete(int r) override {
    return true;
  }
  bool can_affect_io() const override {
    return true;
  }
  journal::Event create_event(uint64_t op_tid) const override {
    return journal::SnapCreateEvent(op_tid, m_snap_namespace, m_snap_name);
  }

private:
  cls::rbd::SnapshotNamespace m_snap_namespace;
  std::string m_snap_name;
  bool m_skip_object_map;
  bool m_skip_notify_quiesce;
  ProgressContext &m_prog_ctx;

  uint64_t m_request_id = 0;
  int m_ret_val = 0;
  bool m_writes_blocked = false;

  uint64_t m_snap_id = CEPH_NOSNAP;
  uint64_t m_size;
  ParentImageInfo m_parent_info;

  void send_notify_quiesce();
  Context *handle_notify_quiesce(int *result);

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

  Context *send_create_image_state();
  Context *handle_create_image_state(int *result);

  void send_release_snap_id();
  Context *handle_release_snap_id(int *result);

  Context *send_notify_unquiesce();
  Context *handle_notify_unquiesce(int *result);

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
