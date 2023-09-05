// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OPERATION_SNAPSHOT_REMOVE_REQUEST_H
#define CEPH_LIBRBD_OPERATION_SNAPSHOT_REMOVE_REQUEST_H

#include "librbd/operation/Request.h"
#include "include/buffer.h"
#include "librbd/Types.h"
#include <string>

class Context;

namespace librbd {

class ImageCtx;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class SnapshotRemoveRequest : public Request<ImageCtxT> {
public:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * TRASH_SNAP
   *    |
   *    v (skip if unsupported)
   * GET_SNAP
   *    |
   *    v (skip if unnecessary)
   * LIST_CHILDREN <-------------\
   *    |                        |
   *    v (skip if unnecessary)  | (repeat as needed)
   * DETACH_STALE_CHILD ---------/
   *    |
   *    v (skip if unnecessary)
   * DETACH_CHILD
   *    |
   *    v (skip if disabled/in-use)
   * REMOVE_OBJECT_MAP
   *    |
   *    v (skip if not mirror snapshot)
   * REMOVE_IMAGE_STATE
   *    |
   *    v (skip if in-use)
   * RELEASE_SNAP_ID
   *    |
   *    v (skip if in-use)
   * REMOVE_SNAP
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  static SnapshotRemoveRequest *create(
      ImageCtxT &image_ctx, const cls::rbd::SnapshotNamespace &snap_namespace,
      const std::string &snap_name, uint64_t snap_id, Context *on_finish) {
    return new SnapshotRemoveRequest(image_ctx, on_finish, snap_namespace,
                                     snap_name, snap_id);
  }

  SnapshotRemoveRequest(ImageCtxT &image_ctx, Context *on_finish,
			const cls::rbd::SnapshotNamespace &snap_namespace,
		        const std::string &snap_name,
			uint64_t snap_id);

protected:
  void send_op() override;
  bool should_complete(int r) override;

  journal::Event create_event(uint64_t op_tid) const override {
    return journal::SnapRemoveEvent(op_tid, m_snap_namespace, m_snap_name);
  }

private:
  cls::rbd::SnapshotNamespace m_snap_namespace;
  cls::rbd::ChildImageSpecs m_child_images;
  std::string m_snap_name;
  uint64_t m_snap_id;
  bool m_trashed_snapshot = false;
  bool m_child_attached = false;

  ceph::bufferlist m_out_bl;

  void trash_snap();
  void handle_trash_snap(int r);

  void get_snap();
  void handle_get_snap(int r);

  void list_children();
  void handle_list_children(int r);

  void detach_stale_child();
  void handle_detach_stale_child(int r);

  void detach_child();
  void handle_detach_child(int r);

  void remove_object_map();
  void handle_remove_object_map(int r);

  void remove_image_state();
  void handle_remove_image_state(int r);

  void release_snap_id();
  void handle_release_snap_id(int r);

  void remove_snap();
  void handle_remove_snap(int r);

  void remove_snap_context();
  int scan_for_parents(cls::rbd::ParentImageSpec &pspec);

};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::SnapshotRemoveRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_SNAPSHOT_REMOVE_REQUEST_H
