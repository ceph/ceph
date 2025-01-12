// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_PREPARE_IMAGES_REQUEST_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_PREPARE_IMAGES_REQUEST_H

#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "librbd/ImageWatcher.h"
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
class GroupPrepareImagesRequest {
public:
  static GroupPrepareImagesRequest *create(
      librados::IoCtx& group_ioctx, const std::string& group_id,
      cls::rbd::MirrorSnapshotState state, uint32_t flags,
      cls::rbd::GroupSnapshot *group_snap,
      std::vector<librbd::ImageCtx *> *image_ctxs,
      std::vector<uint64_t> *quiesce_requests,
      Context *on_finish) {
    return new GroupPrepareImagesRequest(
      group_ioctx, group_id, state, flags, group_snap, image_ctxs,
      quiesce_requests, on_finish);
  }

  GroupPrepareImagesRequest(
    librados::IoCtx& group_ioctx, const std::string& group_id,
    cls::rbd::MirrorSnapshotState state, uint32_t flags,
    cls::rbd::GroupSnapshot *group_snap,
    std::vector<librbd::ImageCtx *> *image_ctxs,
    std::vector<uint64_t> *quiesce_requests,
    Context *on_finish);

  void send();

private:
  /**
   *@verbatim
   *
   *                <start>
   *                   |
   *                   v                        (on error)
   *                LIST GROUP IMAGES--------------------------------------------------->
   *                   |                                                   ^             |
   *                   v                                                   |             |
   *                OPEN GROUP IMAGES                                      |             |
   *                   |          \             (on error)                 |             |
   *                   v           \------------------------------> CLOSE IMAGES         |
   *   . . . . . .  SET SNAP METADATA                                      ^             |
   *   . (skip quiesce |          \             (on error)                 |             |
   *   .  flag)        v           \------------------------------> REMOVE SNAP METADATA |
   *   .            NOTIFY QUIESCE REQUESTS                                ^             |
   *   .               |          \             (on error)                 |             |
   *   .               v           \------------------------------> NOTIFY UNQUIESCE     |
   *   . . . . . >  ACQUIRE IMAGE EXCLUSIVE LOCKS                          ^             |
   *                   |          \             (on error)                 |             |
   *                   |           \----------------------------------------             |
   *                   |                                                                 |
   *                   v                                                                 v
   *                <finish>-----------------------<--------------------------------------
   *
   * @endverbatim
   */

  librados::IoCtx& m_group_ioctx;
  std::string m_group_id;
  cls::rbd::MirrorSnapshotState m_state;
  const uint32_t m_flags;
  cls::rbd::GroupSnapshot *m_group_snap;
  std::vector<ImageCtx *> *m_image_ctxs;
  std::vector<uint64_t> *m_quiesce_requests;
  Context *m_on_finish;

  CephContext *m_cct;

  librados::IoCtx m_default_ns_ioctx;
  uint64_t m_internal_flags;
  std::set<std::string> m_mirror_peer_uuids;
  bufferlist m_outbl;
  NoOpProgressContext m_prog_ctx;
  int m_ret_code = 0;
  cls::rbd::GroupImageSpec m_start_after;
  std::vector<cls::rbd::GroupImageStatus> m_images;

  void check_snap_create_flags();

  void get_mirror_peer_list();
  void handle_get_mirror_peer_list(int r);

  void list_group_images();
  void handle_list_group_images(int r);

  void open_group_images();
  void handle_open_group_images(int r);

  void set_snap_metadata();

  void notify_quiesce();
  void handle_notify_quiesce(int r);

  void acquire_image_exclusive_locks();
  void handle_acquire_image_exclusive_locks(int r);

  void finish(int r);

  // cleanup
  void remove_snap_metadata();

  void notify_unquiesce();
  void handle_notify_unquiesce(int r);

  void close_images();
  void handle_close_images(int r);

};

} // namespace snapshot
} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::snapshot::GroupPrepareImagesRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_PREPARE_IMAGES_REQUEST_H
