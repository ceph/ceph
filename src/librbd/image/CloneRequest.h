// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_CLONE_REQUEST_H
#define CEPH_LIBRBD_IMAGE_CLONE_REQUEST_H

#include "cls/rbd/cls_rbd_types.h"
#include "common/config_fwd.h"
#include "librbd/internal.h"
#include "include/rbd/librbd.hpp"

class Context;

using librados::IoCtx;

namespace librbd {

namespace asio { struct ContextWQ; }

namespace image {

template <typename ImageCtxT = ImageCtx>
class CloneRequest {
public:
  static CloneRequest *create(
      ConfigProxy& config, IoCtx& parent_io_ctx,
      const std::string& parent_image_id,
      const std::string& parent_snap_name,
      const cls::rbd::SnapshotNamespace& parent_snap_namespace,
      uint64_t parent_snap_id,
      IoCtx &c_ioctx, const std::string &c_name,
      const std::string &c_id, ImageOptions c_options,
      cls::rbd::MirrorImageMode mirror_image_mode,
      const std::string &non_primary_global_image_id,
      const std::string &primary_mirror_uuid,
      asio::ContextWQ *op_work_queue, Context *on_finish) {
    return new CloneRequest(config, parent_io_ctx, parent_image_id,
                            parent_snap_name, parent_snap_namespace,
                            parent_snap_id, c_ioctx, c_name, c_id, c_options,
                            mirror_image_mode, non_primary_global_image_id,
                            primary_mirror_uuid, op_work_queue, on_finish);
  }

  CloneRequest(ConfigProxy& config, IoCtx& parent_io_ctx,
               const std::string& parent_image_id,
               const std::string& parent_snap_name,
               const cls::rbd::SnapshotNamespace& parent_snap_namespace,
               uint64_t parent_snap_id,
               IoCtx &c_ioctx, const std::string &c_name,
               const std::string &c_id, ImageOptions c_options,
               cls::rbd::MirrorImageMode mirror_image_mode,
               const std::string &non_primary_global_image_id,
               const std::string &primary_mirror_uuid,
               asio::ContextWQ *op_work_queue, Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * OPEN PARENT
   *    |
   *    v
   * VALIDATE CHILD                    <finish>
   *    |                                 ^
   *    v                                 |
   * CREATE CHILD * * * * * * * * * > CLOSE PARENT
   *    |                                 ^
   *    v                                 |
   * OPEN CHILD * * * * * * * * * * > REMOVE CHILD
   *    |                                 ^
   *    v                                 |
   * ATTACH PARENT  * * * * * * * * > CLOSE CHILD
   *    |                               ^
   *    v                               *
   * ATTACH CHILD * * * * * * * * * * * *
   *    |                               *
   *    v                               *
   * COPY META DATA * * * * * * * * * * ^
   *    |                               *
   *    v (skip if not needed)          *
   * GET MIRROR MODE  * * * * * * * * * ^
   *    |                               *
   *    v (skip if not needed)          *
   * SET MIRROR ENABLED * * * * * * * * *
   *    |
   *    v
   * CLOSE CHILD
   *    |
   *    v
   * CLOSE PARENT
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ConfigProxy& m_config;
  IoCtx &m_parent_io_ctx;
  std::string m_parent_image_id;
  std::string m_parent_snap_name;
  cls::rbd::SnapshotNamespace m_parent_snap_namespace;
  uint64_t m_parent_snap_id;
  ImageCtxT *m_parent_image_ctx;

  IoCtx &m_ioctx;
  std::string m_name;
  std::string m_id;
  ImageOptions m_opts;
  cls::rbd::ParentImageSpec m_pspec;
  ImageCtxT *m_imctx;
  cls::rbd::MirrorMode m_mirror_mode = cls::rbd::MIRROR_MODE_DISABLED;
  cls::rbd::MirrorImageMode m_mirror_image_mode;
  const std::string m_non_primary_global_image_id;
  const std::string m_primary_mirror_uuid;
  NoOpProgressContext m_no_op;
  asio::ContextWQ *m_op_work_queue;
  Context *m_on_finish;

  CephContext *m_cct;
  uint64_t m_clone_format = 2;
  bool m_use_p_features;
  uint64_t m_features;
  bufferlist m_out_bl;
  uint64_t m_size;
  int m_r_saved = 0;

  void validate_options();

  void open_parent();
  void handle_open_parent(int r);

  void validate_parent();

  void validate_child();
  void handle_validate_child(int r);

  void create_child();
  void handle_create_child(int r);

  void open_child();
  void handle_open_child(int r);

  void attach_parent();
  void handle_attach_parent(int r);

  void attach_child();
  void handle_attach_child(int r);

  void copy_metadata();
  void handle_copy_metadata(int r);

  void get_mirror_mode();
  void handle_get_mirror_mode(int r);

  void enable_mirror();
  void handle_enable_mirror(int r);

  void close_child();
  void handle_close_child(int r);

  void remove_child();
  void handle_remove_child(int r);

  void close_parent();
  void handle_close_parent(int r);

  void complete(int r);
};

} //namespace image
} //namespace librbd

extern template class librbd::image::CloneRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_CLONE_REQUEST_H
