// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_UNLINK_PEER_REQUEST_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_UNLINK_PEER_REQUEST_H

#include "include/buffer.h"

#include <string>
#include <set>

struct Context;

namespace librbd {

struct ImageCtx;

namespace mirror {
namespace snapshot {

template <typename ImageCtxT = librbd::ImageCtx>
class UnlinkPeerRequest {
public:
  static UnlinkPeerRequest *create(ImageCtxT *image_ctx, uint64_t snap_id,
                                   const std::string &mirror_peer_uuid,
                                   Context *on_finish) {
    return new UnlinkPeerRequest(image_ctx, snap_id, mirror_peer_uuid,
                                 on_finish);
  }

  UnlinkPeerRequest(ImageCtxT *image_ctx, uint64_t snap_id,
                    const std::string &mirror_peer_uuid, Context *on_finish)
    : m_image_ctx(image_ctx), m_snap_id(snap_id),
      m_mirror_peer_uuid(mirror_peer_uuid), m_on_finish(on_finish) {
  }

  void send();

private:
  /*
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * REFRESH_IMAGE <--------------------------\
   *    |                     ^ (not found    |
   *    |                     *  or last)     |
   *    |                     *               |
   *    |\---------------> UNLINK_PEER --> NOTIFY_UPDATE
   *    |   (not last peer or
   *    |    no newer mirror
   *    |    snap exists)
   *    |
   *    |\---------------> REMOVE_SNAPSHOT
   *    |   (last peer and    |
   *    |    newer mirror     |
   *    |    snap exists)     |
   *    |                     |
   *    |(peer not found)     |
   *    v                     |
   * <finish> <---------------/
   *
   * @endverbatim
   */

  ImageCtxT *m_image_ctx;
  uint64_t m_snap_id;
  std::string m_mirror_peer_uuid;
  Context *m_on_finish;

  bool m_newer_mirror_snapshots = false;

  void refresh_image();
  void handle_refresh_image(int r);

  void unlink_peer();
  void handle_unlink_peer(int r);

  void notify_update();
  void handle_notify_update(int r);

  void remove_snapshot();
  void handle_remove_snapshot(int r);

  void finish(int r);
};

} // namespace snapshot
} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::snapshot::UnlinkPeerRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_UNLINK_PEER_REQUEST_H
