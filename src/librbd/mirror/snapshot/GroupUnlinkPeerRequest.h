// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_UNLINK_PEER_REQUEST_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_UNLINK_PEER_REQUEST_H

#include "include/int_types.h"
#include "include/types.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_types.h"

#include <string>
#include <vector>

class Context;

namespace librbd {

struct ImageCtx;

namespace mirror {
namespace snapshot {

template <typename ImageCtxT = librbd::ImageCtx>
class GroupUnlinkPeerRequest {
public:
  static GroupUnlinkPeerRequest *create(
      librados::IoCtx &group_io_ctx, const std::string &group_id,
      std::set<std::string> *mirror_peer_uuids, 
      std::vector<ImageCtx *> *image_ctxs,
      Context *on_finish) {
    return new GroupUnlinkPeerRequest(group_io_ctx, group_id,
                                      mirror_peer_uuids, image_ctxs,
                                      on_finish);
  }

  GroupUnlinkPeerRequest(librados::IoCtx &group_io_ctx,
                         const std::string &group_id,
                         std::set<std::string> *mirror_peer_uuids, 
                         std::vector<ImageCtx *> *image_ctxs,
                         Context *on_finish)
    : m_group_io_ctx(group_io_ctx), m_group_id(group_id),
      m_mirror_peer_uuids(mirror_peer_uuids), m_image_ctxs(image_ctxs),
      m_on_finish(on_finish) {
    m_cct = (CephContext *)group_io_ctx.cct();
  }

  void send();

private:
  librados::IoCtx &m_group_io_ctx;
  const std::string m_group_id;
  std::set<std::string> *m_mirror_peer_uuids;
  std::vector<ImageCtx *> *m_image_ctxs;
  Context *m_on_finish;

  uint64_t m_max_snaps;
  uint64_t m_retry_count = 0;
  bool m_has_newer_mirror_snap = false;
  CephContext *m_cct;

  std::vector<cls::rbd::GroupSnapshot> m_group_snaps;
  std::map<std::string, ImageCtx *> m_image_ctx_map;
  std::string m_group_snap_id;

  void unlink_peer();

  void list_group_snaps();
  void handle_list_group_snaps(int r);

  void process_snapshot(cls::rbd::GroupSnapshot group_snap,
                        std::string mirror_peer_uuid);

  void remove_peer_uuid(cls::rbd::GroupSnapshot group_snap,
                        std::string mirror_peer_uuid);
  void handle_remove_peer_uuid(int r, cls::rbd::GroupSnapshot group_snap);

  void remove_group_snapshot(cls::rbd::GroupSnapshot group_snap);
  void handle_remove_group_snapshot(int r, cls::rbd::GroupSnapshot group_snap);

  void remove_snap_metadata();
  void handle_remove_snap_metadata(int r);

  void remove_image_snapshot(ImageCtx *image_ctx, uint64_t snap_id,
                             C_Gather *ctx);
  void finish(int r);
};

} // namespace snapshot
} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::snapshot::GroupUnlinkPeerRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_UNLINK_PEER_REQUEST_H
