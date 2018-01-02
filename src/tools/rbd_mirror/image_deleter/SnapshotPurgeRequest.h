// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_DELETER_SNAPSHOT_PURGE_REQUEST_H
#define CEPH_RBD_MIRROR_IMAGE_DELETER_SNAPSHOT_PURGE_REQUEST_H

#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include <string>
#include <vector>

class Context;
namespace librbd { struct ImageCtx; }

namespace rbd {
namespace mirror {
namespace image_deleter {

template <typename ImageCtxT = librbd::ImageCtx>
class SnapshotPurgeRequest {
public:
  static SnapshotPurgeRequest* create(librados::IoCtx &io_ctx,
                                      const std::string &image_id,
                                      Context *on_finish) {
    return new SnapshotPurgeRequest(io_ctx, image_id, on_finish);
  }

  SnapshotPurgeRequest(librados::IoCtx &io_ctx, const std::string &image_id,
                       Context *on_finish)
    : m_io_ctx(io_ctx), m_image_id(image_id), m_on_finish(on_finish) {
  }

  void send();

private:
  /*
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * OPEN_IMAGE
   *    |
   *    v
   * ACQUIRE_LOCK
   *    |
   *    | (repeat for each snapshot)
   *    |/------------------------\
   *    |                         |
   *    v (skip if not needed)    |
   * SNAP_UNPROTECT               |
   *    |                         |
   *    v (skip if not needed)    |
   * SNAP_REMOVE -----------------/
   *    |
   *    v
   * CLOSE_IMAGE
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  librados::IoCtx &m_io_ctx;
  std::string m_image_id;
  Context *m_on_finish;

  ImageCtxT *m_image_ctx = nullptr;
  int m_ret_val = 0;

  std::vector<librados::snap_t> m_snaps;
  cls::rbd::SnapshotNamespace m_snap_namespace;
  std::string m_snap_name;

  void open_image();
  void handle_open_image(int r);

  void acquire_lock();
  void handle_acquire_lock(int r);

  void snap_unprotect();
  void handle_snap_unprotect(int r);

  void snap_remove();
  void handle_snap_remove(int r);

  void close_image();
  void handle_close_image(int r);

  void finish(int r);

  Context *start_lock_op();

};

} // namespace image_deleter
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_deleter::SnapshotPurgeRequest<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_IMAGE_DELETER_SNAPSHOT_PURGE_REQUEST_H

