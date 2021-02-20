// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_CREATE_PRIMARY_REQUEST_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_CREATE_PRIMARY_REQUEST_H

#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/internal.h"
#include "librbd/mirror/snapshot/Types.h"

#include <string>
#include <set>

struct Context;

namespace librbd {

struct ImageCtx;

namespace mirror {
namespace snapshot {

template <typename ImageCtxT = librbd::ImageCtx>
class CreatePrimaryRequest {
public:
  static CreatePrimaryRequest *create(ImageCtxT *image_ctx,
                                      const std::string& global_image_id,
                                      uint64_t clean_since_snap_id,
                                      uint64_t snap_create_flags,
                                      uint32_t flags, uint64_t *snap_id,
                                      Context *on_finish) {
    return new CreatePrimaryRequest(image_ctx, global_image_id,
                                    clean_since_snap_id, snap_create_flags, flags,
                                    snap_id, on_finish);
  }

  CreatePrimaryRequest(ImageCtxT *image_ctx,
                       const std::string& global_image_id,
                       uint64_t clean_since_snap_id, uint64_t snap_create_flags,
                       uint32_t flags, uint64_t *snap_id, Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_MIRROR_PEERS
   *    |
   *    v
   * CREATE_SNAPSHOT
   *    |
   *    v
   * REFRESH_IMAGE
   *    |
   *    v
   * UNLINK_PEER (skip if not needed,
   *    |         repeat if needed)
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT *m_image_ctx;
  std::string m_global_image_id;
  uint64_t m_clean_since_snap_id;
  const uint64_t m_snap_create_flags;
  const uint32_t m_flags;
  uint64_t *m_snap_id;
  Context *m_on_finish;

  librados::IoCtx m_default_ns_ctx;
  std::set<std::string> m_mirror_peer_uuids;
  std::string m_snap_name;

  bufferlist m_out_bl;
  NoOpProgressContext m_prog_ctx;

  void get_mirror_peers();
  void handle_get_mirror_peers(int r);

  void create_snapshot();
  void handle_create_snapshot(int r);

  void refresh_image();
  void handle_refresh_image(int r);

  void unlink_peer();
  void handle_unlink_peer(int r);

  void finish(int r);
};

} // namespace snapshot
} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::snapshot::CreatePrimaryRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_CREATE_PRIMARY_REQUEST_H
