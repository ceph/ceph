// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_DEEP_COPY_OBJECT_COPY_REQUEST_H
#define CEPH_LIBRBD_DEEP_COPY_OBJECT_COPY_REQUEST_H

#include "include/int_types.h"
#include "include/interval_set.h"
#include "include/rados/librados.hpp"
#include "common/snap_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/deep_copy/Types.h"
#include "librbd/io/Types.h"
#include <list>
#include <map>
#include <string>

class Context;
class RWLock;

namespace librbd {

namespace io { class AsyncOperation; }

namespace deep_copy {

struct Handler;

template <typename ImageCtxT = librbd::ImageCtx>
class ObjectCopyRequest {
public:
  static ObjectCopyRequest* create(ImageCtxT *src_image_ctx,
                                   ImageCtxT *dst_image_ctx,
                                   librados::snap_t src_snap_id_start,
                                   librados::snap_t dst_snap_id_start,
                                   const SnapMap &snap_map,
                                   uint64_t object_number, uint32_t flags,
                                   Handler* handler, Context *on_finish) {
    return new ObjectCopyRequest(src_image_ctx, dst_image_ctx,
                                 src_snap_id_start, dst_snap_id_start, snap_map,
                                 object_number, flags, handler, on_finish);
  }

  ObjectCopyRequest(ImageCtxT *src_image_ctx, ImageCtxT *dst_image_ctx,
                    librados::snap_t src_snap_id_start,
                    librados::snap_t dst_snap_id_start, const SnapMap &snap_map,
                    uint64_t object_number, uint32_t flags, Handler* handler,
                    Context *on_finish);

  void send();

  // testing support
  inline librados::IoCtx &get_src_io_ctx() {
    return m_src_io_ctx;
  }
  inline librados::IoCtx &get_dst_io_ctx() {
    return m_dst_io_ctx;
  }

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * LIST_SNAPS
   *    |
   *    |/---------\
   *    |          | (repeat for each snapshot)
   *    v          |
   * READ ---------/
   *    |
   *    |     /-----------\
   *    |     |           | (repeat for each snapshot)
   *    v     v           |
   * UPDATE_OBJECT_MAP ---/ (skip if object
   *    |                    map disabled)
   *    |     /-----------\
   *    |     |           | (repeat for each snapshot)
   *    v     v           |
   * WRITE_OBJECT --------/
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  struct ReadOp {
    interval_set<uint64_t> image_interval;
    io::Extents image_extent_map;
    bufferlist out_bl;
  };

  typedef std::pair<librados::snap_t, librados::snap_t> WriteReadSnapIds;

  ImageCtxT *m_src_image_ctx;
  ImageCtxT *m_dst_image_ctx;
  CephContext *m_cct;
  librados::snap_t m_src_snap_id_start;
  librados::snap_t m_dst_snap_id_start;
  SnapMap m_snap_map;
  uint64_t m_dst_object_number;
  uint32_t m_flags;
  Handler* m_handler;
  Context *m_on_finish;

  decltype(m_src_image_ctx->data_ctx) m_src_io_ctx;
  decltype(m_dst_image_ctx->data_ctx) m_dst_io_ctx;
  std::string m_dst_oid;

  io::Extents m_image_extents;
  io::ImageArea m_image_area = io::ImageArea::DATA;

  io::SnapshotDelta m_snapshot_delta;

  std::map<WriteReadSnapIds, ReadOp> m_read_ops;
  std::list<WriteReadSnapIds> m_read_snaps;
  io::SnapshotSparseBufferlist m_snapshot_sparse_bufferlist;

  std::map<librados::snap_t, interval_set<uint64_t>> m_dst_data_interval;
  std::map<librados::snap_t, interval_set<uint64_t>> m_dst_zero_interval;
  std::map<librados::snap_t, uint8_t> m_dst_object_state;
  std::map<librados::snap_t, bool> m_dst_object_may_exist;

  io::AsyncOperation* m_src_async_op = nullptr;

  void send_list_snaps();
  void handle_list_snaps(int r);

  void send_read();
  void handle_read(int r);

  void send_update_object_map();
  void handle_update_object_map(int r);

  void process_copyup();
  void send_write_object();
  void handle_write_object(int r);

  Context *start_lock_op(ceph::shared_mutex &owner_lock, int* r);

  void compute_read_ops();
  void merge_write_ops();
  void compute_zero_ops();

  void compute_dst_object_may_exist();
  uint64_t compute_starting_end_size();
  void finish(int r);
};

} // namespace deep_copy
} // namespace librbd

extern template class librbd::deep_copy::ObjectCopyRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_DEEP_COPY_OBJECT_COPY_REQUEST_H
