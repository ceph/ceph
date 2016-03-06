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
#include <tuple>
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
   * LIST_SNAPS
   *    |
   *    v
   * READ_OBJECT <----\
   *    |             | (repeat for each snapshot)
   *    v             |
   * WRITE_OBJECT ----/
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

  typedef std::tuple<SyncOpType, uint64_t, uint64_t, bufferlist> SyncOp;
  typedef std::list<SyncOp> SyncOps;
  typedef std::map<librados::snap_t, SyncOps> SnapSyncOps;

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

  SnapSyncOps m_snap_sync_ops;

  void send_list_snaps();
  void handle_list_snaps(int r);

  void send_read_object();
  void handle_read_object(int r);

  void send_write_object();
  void handle_write_object(int r);

  void compute_diffs();
  void finish(int r);

};

} // namespace image_sync
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_sync::ObjectCopyRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_SYNC_OBJECT_COPY_REQUEST_H
