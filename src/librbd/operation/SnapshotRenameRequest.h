// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OPERATION_SNAPSHOT_RENAME_REQUEST_H
#define CEPH_LIBRBD_OPERATION_SNAPSHOT_RENAME_REQUEST_H

#include "librbd/operation/Request.h"
#include <string>

class Context;

namespace librbd {

class ImageCtx;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class SnapshotRenameRequest : public Request<ImageCtxT> {
public:
  /**
   * Snap Rename goes through the following state machine:
   *
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * STATE_RENAME_SNAP
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   *
   */
  enum State {
    STATE_RENAME_SNAP
  };

  SnapshotRenameRequest(ImageCtxT &image_ctx, Context *on_finish,
                        uint64_t snap_id, const std::string &snap_name);

  journal::Event create_event(uint64_t op_tid) const override;

protected:
  void send_op() override;
  bool should_complete(int r) override;

private:
  uint64_t m_snap_id;
  std::string m_snap_name;
  State m_state;

  void send_rename_snap();
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::SnapshotRenameRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_SNAPSHOT_RENAME_REQUEST_H
