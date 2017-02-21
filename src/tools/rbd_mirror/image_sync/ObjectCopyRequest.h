// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_SYNC_OBJECT_COPY_REQUEST_H
#define RBD_MIRROR_IMAGE_SYNC_OBJECT_COPY_REQUEST_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "common/snap_types.h"
#include "librbd/ImageCtx.h"
#include <list>
#include <map>
#include <string>
#include <vector>

class Context;

namespace rbd {
namespace mirror {
namespace image_sync {

template <typename ImageCtxT = librbd::ImageCtx>
class ObjectCopyRequest {
public:
  typedef std::vector<librados::snap_t> SnapIds;
  typedef std::map<librados::snap_t, SnapIds> SnapMap;

  static ObjectCopyRequest* create(ImageCtxT *local_image_ctx,
                                   ImageCtxT *remote_image_ctx,
                                   const SnapMap *snap_map,
                                   uint64_t object_number, Context *on_finish) {
    return new ObjectCopyRequest(local_image_ctx, remote_image_ctx, snap_map,
                                 object_number, on_finish);
  }

  ObjectCopyRequest(ImageCtxT *local_image_ctx, ImageCtxT *remote_image_ctx,
                    const SnapMap *snap_map, uint64_t object_number,
                    Context *on_finish);

  void send();

  // testing support
  inline librados::IoCtx &get_local_io_ctx() {
    return m_local_io_ctx;
  }
  inline librados::IoCtx &get_remote_io_ctx() {
    return m_remote_io_ctx;
  }

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * LIST_SNAPS < * * *
   *    |             * (-ENOENT and snap set stale)
   *    |   * * * * * *
   *    |   *
   *    v   *
   * READ_OBJECT <--------\
   *    |                 | (repeat for each snapshot)
   *    v                 |
   * WRITE_OBJECT --------/
   *    |
   *    |     /-----------\
   *    |     |           | (repeat for each snapshot)
   *    v     v           |
   * UPDATE_OBJECT_MAP ---/ (skip if object
   *    |                    map disabled)
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  enum SyncOpType {
    SYNC_OP_TYPE_WRITE,
    SYNC_OP_TYPE_TRUNC,
    SYNC_OP_TYPE_REMOVE
  };

  struct SyncOp {
    SyncOp(SyncOpType type, uint64_t offset, uint64_t length)
      : type(type), offset(offset), length(length) {
    }

    SyncOpType type;
    uint64_t offset;
    uint64_t length;

    bufferlist out_bl;
  };

  typedef std::list<SyncOp> SyncOps;
  typedef std::pair<librados::snap_t, librados::snap_t> WriteReadSnapIds;
  typedef std::map<WriteReadSnapIds, SyncOps> SnapSyncOps;
  typedef std::map<librados::snap_t, uint8_t> SnapObjectStates;

  ImageCtxT *m_local_image_ctx;
  ImageCtxT *m_remote_image_ctx;
  const SnapMap *m_snap_map;
  uint64_t m_object_number;
  Context *m_on_finish;

  decltype(m_local_image_ctx->data_ctx) m_local_io_ctx;
  decltype(m_remote_image_ctx->data_ctx) m_remote_io_ctx;
  std::string m_local_oid;
  std::string m_remote_oid;

  librados::snap_set_t m_snap_set;
  int m_snap_ret;

  bool m_retry_missing_read = false;
  librados::snap_set_t m_retry_snap_set;

  SnapSyncOps m_snap_sync_ops;
  SnapObjectStates m_snap_object_states;

  void send_list_snaps();
  void handle_list_snaps(int r);

  void send_read_object();
  void handle_read_object(int r);

  void send_write_object();
  void handle_write_object(int r);

  void send_update_object_map();
  void handle_update_object_map(int r);

  void compute_diffs();
  void finish(int r);

};

} // namespace image_sync
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_sync::ObjectCopyRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_SYNC_OBJECT_COPY_REQUEST_H
