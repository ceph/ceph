// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_CLONE_REQUEST_H
#define CEPH_LIBRBD_IMAGE_CLONE_REQUEST_H

#include "cls/rbd/cls_rbd_types.h"
#include "include/rbd/librbd.hpp"
#include "librbd/internal.h"

class Context;

using librados::IoCtx;

namespace librbd {
namespace image {

template <typename ImageCtxT = ImageCtx>
class CloneRequest {
public:
  static CloneRequest *create(IoCtx& parent_io_ctx,
                              const std::string& parent_image_id,
                              const std::string& parent_snap_name,
                              uint64_t parent_snap_id,
                              IoCtx &c_ioctx, const std::string &c_name,
                              const std::string &c_id, ImageOptions c_options,
			      const std::string &non_primary_global_image_id,
			      const std::string &primary_mirror_uuid,
			      ContextWQ *op_work_queue, Context *on_finish) {
    return new CloneRequest(parent_io_ctx, parent_image_id, parent_snap_name,
                            parent_snap_id, c_ioctx, c_name, c_id, c_options,
                            non_primary_global_image_id, primary_mirror_uuid,
                            op_work_queue, on_finish);
  }

  CloneRequest(IoCtx& parent_io_ctx,
               const std::string& parent_image_id,
               const std::string& parent_snap_name,
               uint64_t parent_snap_id,
               IoCtx &c_ioctx, const std::string &c_name,
               const std::string &c_id, ImageOptions c_options,
               const std::string &non_primary_global_image_id,
               const std::string &primary_mirror_uuid,
               ContextWQ *op_work_queue, Context *on_finish);

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
   * SET PARENT * * * * * * * * * * > CLOSE CHILD
   *    |                               ^
   *    |\--------\                     *
   *    |         |                     *
   *    |         v (clone v2 disabled) *
   *    |     V1 ADD CHILD  * * * * * * ^
   *    |         |                     *
   *    |         v                     *
   *    |     V1 VALIDATE PROTECTED * * ^
   *    |         |                     *
   *    v         |                     *
   * V2 SET CLONE * * * * * * * * * * * ^
   *    |         |                     *
   *    v         |                     *
   * V2 ATTACH CHILD  * * * * * * * * * *
   *    |         |                     *
   *    v         v                     *
   * GET PARENT META  * * * * * * * * * ^
   *    |                               *
   *    v (skip if not needed)          *
   * SET CHILD META * * * * * * * * * * ^
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

  IoCtx &m_parent_io_ctx;
  std::string m_parent_image_id;
  std::string m_parent_snap_name;
  uint64_t m_parent_snap_id;
  ImageCtxT *m_parent_image_ctx;

  IoCtx &m_ioctx;
  std::string m_name;
  std::string m_id;
  ImageOptions m_opts;
  ParentSpec m_pspec;
  ImageCtxT *m_imctx;
  cls::rbd::MirrorMode m_mirror_mode = cls::rbd::MIRROR_MODE_DISABLED;
  const std::string m_non_primary_global_image_id;
  const std::string m_primary_mirror_uuid;
  NoOpProgressContext m_no_op;
  ContextWQ *m_op_work_queue;
  Context *m_on_finish;

  CephContext *m_cct;
  uint32_t m_clone_format = 2;
  bool m_use_p_features;
  uint64_t m_features;
  map<string, bufferlist> m_pairs;
  std::string m_last_metadata_key;
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

  void set_parent();
  void handle_set_parent(int r);

  void v2_set_op_feature();
  void handle_v2_set_op_feature(int r);

  void v2_child_attach();
  void handle_v2_child_attach(int r);

  void v1_add_child();
  void handle_v1_add_child(int r);

  void v1_refresh();
  void handle_v1_refresh(int r);

  void metadata_list();
  void handle_metadata_list(int r);

  void metadata_set();
  void handle_metadata_set(int r);

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
