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
#include <optional>
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
   *    |     /----------------------\
   *    |     |                      |
   *    v     v                      | (repeat for each src object)
   * LIST_SNAPS < * * *              |
   *    |             * (-ENOENT and snap set stale)
   *    |   * * * * * *              |
   *    |   * /-----------\          |
   *    |   * |           | (repeat for each snapshot)
   *    v   * v           |          |
   * READ_OBJECT ---------/          |
   *    |     |                      |
   *    |     \----------------------/
   *    v
   * READ_FROM_PARENT (skip if not needed)
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

  struct SrcObjectExtent {
    uint64_t object_no = 0;
    uint64_t offset = 0;
    uint64_t length = 0;
    bool noent = false;

    SrcObjectExtent() {
    }
    SrcObjectExtent(uint64_t object_no, uint64_t offset, uint64_t length)
      : object_no(object_no), offset(offset), length(length) {
    }
  };

  typedef std::map<uint64_t, SrcObjectExtent> SrcObjectExtents;

  enum CopyOpType {
    COPY_OP_TYPE_WRITE,
    COPY_OP_TYPE_ZERO,
    COPY_OP_TYPE_TRUNC,
    COPY_OP_TYPE_REMOVE,
    COPY_OP_TYPE_REMOVE_TRUNC,
  };

  typedef std::map<uint64_t, uint64_t> ExtentMap;

  struct CopyOp {
    CopyOp(CopyOpType type, uint64_t src_offset, uint64_t dst_offset,
           uint64_t length)
      : type(type), src_offset(src_offset), dst_offset(dst_offset),
        length(length) {
    }

    CopyOpType type;
    uint64_t src_offset;
    uint64_t dst_offset;
    uint64_t length;

    ExtentMap src_extent_map;
    ExtentMap dst_extent_map;
    bufferlist out_bl;
  };

  typedef std::list<CopyOp> CopyOps;
  typedef std::pair<librados::snap_t, librados::snap_t> WriteReadSnapIds;
  typedef std::map<librados::snap_t, uint8_t> SnapObjectStates;
  typedef std::map<librados::snap_t, std::map<uint64_t, uint64_t>> SnapObjectSizes;

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

  std::set<uint64_t> m_src_objects;
  uint64_t m_src_ono;
  std::string m_src_oid;
  SrcObjectExtents m_src_object_extents;
  librados::snap_set_t m_snap_set;
  int m_snap_ret = 0;
  bool m_retry_missing_read = false;
  librados::snap_set_t m_retry_snap_set;
  bool m_read_whole_object = false;

  std::map<WriteReadSnapIds, CopyOps> m_read_ops;
  std::list<WriteReadSnapIds> m_read_snaps;
  std::map<librados::snap_t, CopyOps> m_write_ops;
  std::map<librados::snap_t, interval_set<uint64_t>> m_zero_interval;
  std::map<librados::snap_t, interval_set<uint64_t>> m_dst_zero_interval;
  std::map<librados::snap_t, uint8_t> m_dst_object_state;
  std::map<librados::snap_t, bool> m_dst_object_may_exist;
  std::optional<uint64_t> m_dst_object_size = std::nullopt;
  bufferlist m_read_from_parent_data;

  io::AsyncOperation* m_src_async_op = nullptr;

  void send_list_snaps();
  void handle_list_snaps(int r);

  void send_read_object();
  void handle_read_object(int r);

  void send_read_from_parent();
  void handle_read_from_parent(int r);

  void send_update_object_map();
  void handle_update_object_map(int r);

  void send_write_object();
  void handle_write_object(int r);

  Context *start_lock_op(ceph::shared_mutex &owner_lock, int* r);

  uint64_t src_to_dst_object_offset(uint64_t objectno, uint64_t offset);

  void compute_src_object_extents();
  void compute_read_ops();
  void compute_read_from_parent_ops(io::Extents *image_extents);
  void merge_write_ops();
  void compute_zero_ops();

  void compute_dst_object_may_exist();

  void finish(int r);
};

} // namespace deep_copy
} // namespace librbd

extern template class librbd::deep_copy::ObjectCopyRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_DEEP_COPY_OBJECT_COPY_REQUEST_H
