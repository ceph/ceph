// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_CREATE_PRIMARY_REQUEST_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_CREATE_PRIMARY_REQUEST_H

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
class GroupCreatePrimaryRequest {
public:
  static GroupCreatePrimaryRequest *create(librados::IoCtx& group_ioctx,
                                    const std::string& group_name,
                                    uint32_t flags, std::string *snap_id,
                                    Context *on_finish) {
    return new GroupCreatePrimaryRequest(group_ioctx, group_name, flags,
                                         snap_id, on_finish);
  }

  GroupCreatePrimaryRequest(librados::IoCtx& group_ioctx,
                     const std::string& group_name,
                     uint32_t flags, std::string *snap_id,
                     Context *on_finish)
    : m_group_ioctx(group_ioctx), m_group_name(group_name), m_flags(flags),
      m_snap_id(snap_id), m_on_finish(on_finish) {
    m_cct = (CephContext *)group_ioctx.cct();
  }

  void send();

private:
  // TODO: Complete the diagram
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET GROUP ID
   *    |
   *    v
   * GET LAST MIRROR SNAPSHOT STATE
   *    |
   *    v
   * PREPARE GROUP IMAGES
   *    |
   *    |                 (on error)
   * CREATE IMAGE SNAPS . . . . . . . REMOVE INCOMPLETE GROUP SNAP
   *    |                                  .
   *    v                                  .
   * NOTIFY UNQUIESCE < . . . . . . . . .  .
   *    |
   *    v
   * UNLINK PEER GROUP
   *    |
   *    v
   * CLOSE IMAGES
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  librados::IoCtx m_group_ioctx;
  const std::string m_group_name;
  const uint32_t m_flags;
  std::string *m_snap_id;
  Context *m_on_finish;

  CephContext *m_cct;

  bufferlist m_outbl;
  std::string m_group_id;
  cls::rbd::MirrorGroup m_mirror_group;
  std::vector<cls::rbd::GroupSnapshot> m_existing_group_snaps;
  cls::rbd::GroupSnapshot m_group_snap;
  std::vector<ImageCtx *> m_image_ctxs;
  std::vector<uint64_t> m_quiesce_requests;
  std::vector<uint64_t> m_image_snap_ids;
  int m_ret_code=0;

  void get_group_id();
  void handle_get_group_id(int r);

  void get_mirror_group();
  void handle_get_mirror_group(int r);

  void get_last_mirror_snapshot_state();
  void handle_get_last_mirror_snapshot_state(int r);

  void generate_group_snap();

  void prepare_group_images();
  void handle_prepare_group_images(int r);

  void create_image_snaps();
  void handle_create_image_snaps(int r);

  void notify_unquiesce();
  void handle_notify_unquiesce(int r);

  void unlink_peer_group();
  void handle_unlink_peer_group(int r);

  void close_images();
  void handle_close_images(int r);

  void finish(int r);

  // cleanup
  void remove_incomplete_group_snap();
  void handle_remove_incomplete_group_snap(int r);
};

} // namespace snapshot
} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::snapshot::GroupCreatePrimaryRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_CREATE_PRIMARY_REQUEST_H
