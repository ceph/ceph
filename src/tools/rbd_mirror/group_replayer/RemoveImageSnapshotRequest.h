// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef RBD_MIRROR_GROUP_REPLAYER_REMOVE_IMAGE_SNAPSHOT_REQUEST_H
#define RBD_MIRROR_GROUP_REPLAYER_REMOVE_IMAGE_SNAPSHOT_REQUEST_H

#include "cls/rbd/cls_rbd_types.h"
#include "include/rados/librados.hpp"
#include <string>

struct Context;
namespace librbd { struct ImageCtx; }

namespace rbd {
namespace mirror {
namespace group_replayer {

template <typename ImageCtxT = librbd::ImageCtx>
class RemoveImageSnapshotRequest {
public:
  static RemoveImageSnapshotRequest* create(
      librados::IoCtx& group_io_ctx, int64_t image_pool_id,
      const std::string& image_id, uint64_t snap_id,
      const std::string& group_snap_id, Context* on_finish) {
    return new RemoveImageSnapshotRequest(group_io_ctx, image_pool_id, image_id,
                                          snap_id, group_snap_id, on_finish);
  }

  RemoveImageSnapshotRequest(
      librados::IoCtx& group_io_ctx, int64_t image_pool_id,
      const std::string& image_id, uint64_t snap_id,
      const std::string& group_snap_id, Context* on_finish)
    : m_group_io_ctx(group_io_ctx), m_image_pool_id(image_pool_id),
      m_image_id(image_id), m_snap_id(snap_id), m_group_snap_id(group_snap_id),
      m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   * <start>
   *    |
   *    v
   * OPEN_IMAGE
   *    |
   *    v
   * VALIDATE_SNAPSHOT
   *    |
   *    v
   * REMOVE_SNAPSHOT
   *    |
   *    v
   * CLOSE_IMAGE
   *    |
   *    v
   * <finish>
   * @endverbatim
   */
  librados::IoCtx& m_group_io_ctx;
  int64_t m_image_pool_id;
  std::string m_image_id;
  uint64_t m_snap_id;
  std::string m_group_snap_id;
  Context* m_on_finish;
  librados::IoCtx m_image_io_ctx;
  ImageCtxT* m_image_ctx = nullptr;
  cls::rbd::SnapshotNamespace m_snap_namespace;
  std::string m_snap_name;
  int m_ret_val = 0;

  void handle_open_image(int r);
  void close_image(int r);
  void handle_close_image(int r);
  void finish(int r);
};

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::group_replayer::RemoveImageSnapshotRequest<
  librbd::ImageCtx>;

#endif // RBD_MIRROR_GROUP_REPLAYER_REMOVE_IMAGE_SNAPSHOT_REQUEST_H
