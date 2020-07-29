// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_DEEP_COPY_REQUEST_H
#define CEPH_LIBRBD_DEEP_COPY_REQUEST_H

#include "common/ceph_mutex.h"
#include "common/RefCountedObj.h"
#include "include/int_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/Types.h"
#include "librbd/deep_copy/Types.h"

#include <map>
#include <vector>

class Context;

namespace librbd {

class ImageCtx;
namespace asio { struct ContextWQ; }

namespace deep_copy {

template <typename> class ImageCopyRequest;
template <typename> class SnapshotCopyRequest;
struct Handler;

}

template <typename ImageCtxT = ImageCtx>
class DeepCopyRequest : public RefCountedObject {
public:
  static DeepCopyRequest* create(ImageCtxT *src_image_ctx,
                                 ImageCtxT *dst_image_ctx,
                                 librados::snap_t src_snap_id_start,
                                 librados::snap_t src_snap_id_end,
                                 librados::snap_t dst_snap_id_start,
                                 bool flatten,
                                 const deep_copy::ObjectNumber &object_number,
                                 asio::ContextWQ *work_queue,
                                 SnapSeqs *snap_seqs,
                                 deep_copy::Handler *handler,
                                 Context *on_finish) {
    return new DeepCopyRequest(src_image_ctx, dst_image_ctx, src_snap_id_start,
                               src_snap_id_end, dst_snap_id_start, flatten,
                               object_number, work_queue, snap_seqs, handler,
                               on_finish);
  }

  DeepCopyRequest(ImageCtxT *src_image_ctx, ImageCtxT *dst_image_ctx,
                  librados::snap_t src_snap_id_start,
                  librados::snap_t src_snap_id_end,
                  librados::snap_t dst_snap_id_start,
                  bool flatten, const deep_copy::ObjectNumber &object_number,
                  asio::ContextWQ *work_queue, SnapSeqs *snap_seqs,
                  deep_copy::Handler *handler, Context *on_finish);
  ~DeepCopyRequest();

  void send();
  void cancel();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * COPY_SNAPSHOTS
   *    |
   *    v
   * COPY_IMAGE . . . . . . . . . . . . . .
   *    |                                 .
   *    v                                 .
   * COPY_OBJECT_MAP (skip if object      .
   *    |             map disabled)       .
   *    v                                 .
   * REFRESH_OBJECT_MAP (skip if object   . (image copy canceled)
   *    |                map disabled)    .
   *    v                                 .
   * COPY_METADATA                        .
   *    |                                 .
   *    v                                 .
   * <finish> < . . . . . . . . . . . . . .
   *
   * @endverbatim
   */

  typedef std::vector<librados::snap_t> SnapIds;
  typedef std::map<librados::snap_t, SnapIds> SnapMap;

  ImageCtxT *m_src_image_ctx;
  ImageCtxT *m_dst_image_ctx;
  librados::snap_t m_src_snap_id_start;
  librados::snap_t m_src_snap_id_end;
  librados::snap_t m_dst_snap_id_start;
  bool m_flatten;
  deep_copy::ObjectNumber m_object_number;
  asio::ContextWQ *m_work_queue;
  SnapSeqs *m_snap_seqs;
  deep_copy::Handler *m_handler;
  Context *m_on_finish;

  CephContext *m_cct;
  ceph::mutex m_lock;
  bool m_canceled = false;

  deep_copy::SnapshotCopyRequest<ImageCtxT> *m_snapshot_copy_request = nullptr;
  deep_copy::ImageCopyRequest<ImageCtxT> *m_image_copy_request = nullptr;
  decltype(ImageCtxT::object_map) m_object_map = nullptr;

  void send_copy_snapshots();
  void handle_copy_snapshots(int r);

  void send_copy_image();
  void handle_copy_image(int r);

  void send_copy_object_map();
  void handle_copy_object_map(int r);

  void send_refresh_object_map();
  void handle_refresh_object_map(int r);

  void send_copy_metadata();
  void handle_copy_metadata(int r);

  int validate_copy_points();

  void finish(int r);
};

} // namespace librbd

extern template class librbd::DeepCopyRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_DEEP_COPY_REQUEST_H
