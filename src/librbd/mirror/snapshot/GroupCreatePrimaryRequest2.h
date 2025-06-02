// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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
class GroupCreatePrimaryRequest2 {
public:
  static GroupCreatePrimaryRequest2 *create(librados::IoCtx& group_ioctx,
                                           const std::string& group_name,
                                           const std::string& global_group_id,
                                           uint64_t snap_create_flags,
                                           std::string *snap_id,
                                           Context *on_finish) {
    return new GroupCreatePrimaryRequest2(group_ioctx, group_name, global_group_id,
                                         snap_create_flags, snap_id, on_finish);
  }
// TODO: Allow demoted flag?
  GroupCreatePrimaryRequest2(librados::IoCtx& group_ioctx,
                     const std::string& group_name,
                     const std::string& global_group_id,
                     uint64_t snap_create_flags, std::string *snap_id,
                     Context *on_finish)
    : m_group_ioctx(group_ioctx), m_group_name(group_name),
      m_global_group_id (global_group_id),
      m_snap_create_flags(snap_create_flags), m_snap_id(snap_id),
      m_on_finish(on_finish) {
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
   * LIST_GROUP_IMAGES
   *    |
   *    v
   * OPEN_IMAGES
   *    |
   *    v
   * SET_GROUP_INCOMPLETE_SNAP
   *    |
   *    v
   * CREATE IMAGE SNAPS
   *    |
   *    v
   * SET_GROUP_COMPLETE_SNAP
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
  const std::string m_global_group_id;
  const uint64_t m_snap_create_flags;
  std::string *m_snap_id;
  Context *m_on_finish;

  CephContext *m_cct;

  bufferlist m_outbl;
  std::string m_group_id;
  cls::rbd::GroupSnapshot m_group_snap;
  std::vector<ImageCtx *> m_image_ctxs;
  std::vector<uint64_t> m_image_snap_ids;
  std::vector<uint64_t> m_clean_since_snap_ids;
  std::vector<cls::rbd::MirrorImage> m_mirror_images;
  std::vector<std::string> m_global_image_ids;

  std::set<std::string> m_mirror_peer_uuids;
  librados::IoCtx m_default_ns_ioctx;
  int m_ret_code=0;
  cls::rbd::GroupImageSpec m_start_after;
  std::vector<cls::rbd::GroupImageStatus> m_images;
  NoOpProgressContext m_prog_ctx;

  void get_group_id();
  void handle_get_group_id(int r);

  void generate_group_snap();

  void create_image_snapshots();
  void handle_create_image_snapshots(int r);

  void unlink_peer_group();
  void handle_unlink_peer_group(int r);

  void close_images();
  void handle_close_images(int r);

  void get_mirror_peer_list();
  void handle_get_mirror_peer_list(int r);

  void list_group_images();
  void handle_list_group_images(int r);

  void open_group_images();
  void handle_open_group_images(int r);

  void get_mirror_images();
  void handle_get_mirror_images(int r);

  void set_snap_metadata();
  void handle_set_snap_metadata(int r);

  // cleanup
  void remove_incomplete_group_snap();
  void handle_remove_incomplete_group_snap(int r);

  void remove_snap_metadata();
  void handle_remove_snap_metadata(int r);

  void finish(int r);

};

} // namespace snapshot
} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::snapshot::GroupCreatePrimaryRequest2<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_CREATE_PRIMARY_REQUEST_H
