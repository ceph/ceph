// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_DEEP_COPY_IMAGE_DEEP_COPY_REQUEST_H
#define CEPH_LIBRBD_DEEP_COPY_IMAGE_DEEP_COPY_REQUEST_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "common/bit_vector.hpp"
#include "common/ceph_mutex.h"
#include "common/RefCountedObj.h"
#include "librbd/Types.h"
#include "librbd/deep_copy/Types.h"
#include <functional>
#include <map>
#include <queue>
#include <set>
#include <vector>
#include <boost/optional.hpp>

class Context;

namespace librbd {

class ImageCtx;

namespace deep_copy {

class Handler;

template <typename ImageCtxT = ImageCtx>
class ImageCopyRequest : public RefCountedObject {
public:
  static ImageCopyRequest* create(ImageCtxT *src_image_ctx,
                                  ImageCtxT *dst_image_ctx,
                                  librados::snap_t src_snap_id_start,
                                  librados::snap_t src_snap_id_end,
                                  librados::snap_t dst_snap_id_start,
                                  bool flatten,
                                  const ObjectNumber &object_number,
                                  const SnapSeqs &snap_seqs,
                                  Handler *handler,
                                  Context *on_finish) {
    return new ImageCopyRequest(src_image_ctx, dst_image_ctx, src_snap_id_start,
                                src_snap_id_end, dst_snap_id_start, flatten,
                                object_number, snap_seqs, handler, on_finish);
  }

  ImageCopyRequest(ImageCtxT *src_image_ctx, ImageCtxT *dst_image_ctx,
                   librados::snap_t src_snap_id_start,
                   librados::snap_t src_snap_id_end,
                   librados::snap_t dst_snap_id_start,
                   bool flatten, const ObjectNumber &object_number,
                   const SnapSeqs &snap_seqs, Handler *handler,
                   Context *on_finish);

  void send();
  void cancel();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * COMPUTE_DIFF
   *    |
   *    |      . . . . .
   *    |      .       .  (parallel execution of
   *    v      v       .   multiple objects at once)
   * COPY_OBJECT . . . .
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT *m_src_image_ctx;
  ImageCtxT *m_dst_image_ctx;
  librados::snap_t m_src_snap_id_start;
  librados::snap_t m_src_snap_id_end;
  librados::snap_t m_dst_snap_id_start;
  bool m_flatten;
  ObjectNumber m_object_number;
  SnapSeqs m_snap_seqs;
  Handler *m_handler;
  Context *m_on_finish;

  CephContext *m_cct;
  ceph::mutex m_lock;
  bool m_canceled = false;

  uint64_t m_object_no = 0;
  uint64_t m_end_object_no = 0;
  uint64_t m_current_ops = 0;
  std::priority_queue<
    uint64_t, std::vector<uint64_t>, std::greater<uint64_t>> m_copied_objects;
  bool m_updating_progress = false;
  SnapMap m_snap_map;
  int m_ret_val = 0;

  BitVector<2> m_object_diff_state;

  void map_src_objects(uint64_t dst_object, std::set<uint64_t> *src_objects);

  void compute_diff();
  void handle_compute_diff(int r);

  void send_object_copies();
  void send_next_object_copy();
  void handle_object_copy(uint64_t object_no, int r);

  void finish(int r);
};

} // namespace deep_copy
} // namespace librbd

extern template class librbd::deep_copy::ImageCopyRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_DEEP_COPY_IMAGE_DEEP_COPY_REQUEST_H
