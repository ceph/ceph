// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_DEEP_COPY_SNAPSHOT_COPY_REQUEST_H
#define CEPH_LIBRBD_DEEP_COPY_SNAPSHOT_COPY_REQUEST_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "common/RefCountedObj.h"
#include "common/snap_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/Types.h"
#include <map>
#include <set>
#include <string>
#include <tuple>

class Context;

namespace librbd {

namespace asio { struct ContextWQ; }

namespace deep_copy {

template <typename ImageCtxT = librbd::ImageCtx>
class SnapshotCopyRequest : public RefCountedObject {
public:
  static SnapshotCopyRequest* create(ImageCtxT *src_image_ctx,
                                     ImageCtxT *dst_image_ctx,
                                     librados::snap_t src_snap_id_start,
                                     librados::snap_t src_snap_id_end,
                                     librados::snap_t dst_snap_id_start,
                                     bool flatten, asio::ContextWQ *work_queue,
                                     SnapSeqs *snap_seqs, Context *on_finish) {
    return new SnapshotCopyRequest(src_image_ctx, dst_image_ctx,
                                   src_snap_id_start, src_snap_id_end,
                                   dst_snap_id_start, flatten, work_queue,
                                   snap_seqs, on_finish);
  }

  SnapshotCopyRequest(ImageCtxT *src_image_ctx, ImageCtxT *dst_image_ctx,
                      librados::snap_t src_snap_id_start,
                      librados::snap_t src_snap_id_end,
                      librados::snap_t dst_snap_id_start,
                      bool flatten, asio::ContextWQ *work_queue,
                      SnapSeqs *snap_seqs, Context *on_finish);

  void send();
  void cancel();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    |   /-----------\
   *    |   |           |
   *    v   v           | (repeat as needed)
   * UNPROTECT_SNAP ----/
   *    |
   *    |   /-----------\
   *    |   |           |
   *    v   v           | (repeat as needed)
   * REMOVE_SNAP -------/
   *    |
   *    |   /-----------\
   *    |   |           |
   *    v   v           | (repeat as needed)
   * CREATE_SNAP -------/
   *    |
   *    |   /-----------\
   *    |   |           |
   *    v   v           | (repeat as needed)
   * PROTECT_SNAP ------/
   *    |
   *    v
   * SET_HEAD (skip if not needed)
   *    |
   *    v
   * RESIZE_OBJECT_MAP (skip if not needed)
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  typedef std::set<librados::snap_t> SnapIdSet;

  ImageCtxT *m_src_image_ctx;
  ImageCtxT *m_dst_image_ctx;
  librados::snap_t m_src_snap_id_start;
  librados::snap_t m_src_snap_id_end;
  librados::snap_t m_dst_snap_id_start;
  bool m_flatten;
  asio::ContextWQ *m_work_queue;
  SnapSeqs *m_snap_seqs_result;
  SnapSeqs m_snap_seqs;
  Context *m_on_finish;

  CephContext *m_cct;
  SnapIdSet m_src_snap_ids;
  SnapIdSet m_dst_snap_ids;
  librados::snap_t m_prev_snap_id = CEPH_NOSNAP;

  std::string m_snap_name;
  cls::rbd::SnapshotNamespace m_snap_namespace;

  cls::rbd::ParentImageSpec m_dst_parent_spec;

  ceph::mutex m_lock;
  bool m_canceled = false;

  void send_snap_unprotect();
  void handle_snap_unprotect(int r);

  void send_snap_remove();
  void handle_snap_remove(int r);

  void send_snap_create();
  void handle_snap_create(int r);

  void send_snap_protect();
  void handle_snap_protect(int r);

  void send_set_head();
  void handle_set_head(int r);

  void send_resize_object_map();
  void handle_resize_object_map(int r);

  bool handle_cancellation();

  void error(int r);

  int validate_parent(ImageCtxT *image_ctx, cls::rbd::ParentImageSpec *spec);

  Context *start_lock_op(int* r);
  Context *start_lock_op(ceph::shared_mutex &owner_locki, int* r);

  void finish(int r);
};

} // namespace deep_copy
} // namespace librbd

extern template class librbd::deep_copy::SnapshotCopyRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_DEEP_COPY_SNAPSHOT_COPY_REQUEST_H
