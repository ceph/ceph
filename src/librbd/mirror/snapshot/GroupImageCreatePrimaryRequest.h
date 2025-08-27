// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_IMAGE_CREATE_PRIMARY_REQUEST_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_IMAGE_CREATE_PRIMARY_REQUEST_H

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
class GroupImageCreatePrimaryRequest {
public:
  static GroupImageCreatePrimaryRequest *create(
      CephContext* cct, const std::vector<ImageCtxT *> &image_ctxs,
      const std::vector<std::string> &global_image_ids,
      uint64_t group_snap_create_flags, uint32_t flags,
      const std::string &group_snap_id, std::vector<uint64_t> *snap_ids,
      Context *on_finish) {
    return new GroupImageCreatePrimaryRequest(
      cct, image_ctxs, global_image_ids, group_snap_create_flags, flags,
      group_snap_id, snap_ids, on_finish);
  }

  GroupImageCreatePrimaryRequest(
    CephContext* cct, const std::vector<ImageCtxT *> &image_ctxs,
    const std::vector<std::string> &global_image_ids,
    uint64_t group_snap_create_flags, uint32_t flags,
    const std::string &group_snap_id, std::vector<uint64_t> *snap_ids,
    Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v                    (on-error)
   * GET_MIRROR_PEERS . . . . . . > . . . . . . . .
   *    |                                         .
   *    v                    (on-error)           .
   * NOTIFY_QUIESCE  . . . . . . > . . . .        .
   *    |                                .        .
   *    v                    (on-error)  .        .
   * ACQUIRE_EXCLUSIVE_LOCKS . . .       .        .
   *    |                        .       .        .
   *    v                        .       .        .
   * CREATE_SNAPSHOTS            .       .        .
   *    |                        v       .        .
   *    v                        .       v        .
   * REFRESH_IMAGES              .       .        .
   *    |                        .       .        .
   *    v                        .       .        .
   * RELEASE_EXCLUSIVE_LOCKS . . .       .        .
   *    |                                .        .
   *    v                                .        .
   * NOTIFY_UNQUIESCE  . . . . < . . . . .        .
   *    |                                         .
   *    v                                         .
   * <finish> . . . . . . . . . . < . . . . . . . .
   *
   * @endverbatim
   */

  CephContext *m_cct;
  const std::vector<ImageCtxT *> &m_image_ctxs;
  const std::vector<std::string> &m_global_image_ids;
  uint64_t m_group_snap_create_flags;
  const uint32_t m_flags;
  const std::string m_group_snap_id;
  std::vector<uint64_t> *m_snap_ids;
  Context *m_on_finish;

  std::vector<std::set<std::string>> m_mirror_peers_uuids;
  std::vector<std::string> m_snap_names;
  std::vector<librados::IoCtx> m_default_ns_ctxs;
  std::vector<bufferlist> m_out_bls;

  std::vector<uint64_t> m_quiesce_requests;
  bool m_release_locks = false;
  int m_ret_code = 0;

  NoOpProgressContext m_prog_ctx;

  void get_mirror_peers();
  void handle_get_mirror_peers(int r);

  void create_snapshots();
  void handle_create_snapshots(int r);

  void refresh_images();
  void handle_refresh_images(int r);

  void notify_quiesce();
  void handle_notify_quiesce(int r);

  void notify_unquiesce();
  void handle_notify_unquiesce(int r);

  void acquire_exclusive_locks();
  void handle_acquire_exclusive_locks(int r);

  void release_exclusive_locks();
  void handle_release_exclusive_locks(int r);

  void finish(int r);
};

} // namespace snapshot
} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::snapshot::GroupImageCreatePrimaryRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_IMAGE_CREATE_PRIMARY_REQUEST_H
