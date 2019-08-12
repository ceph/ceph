// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_API_MIGRATION_H
#define CEPH_LIBRBD_API_MIGRATION_H

#include "include/int_types.h"
#include "include/rados/librados_fwd.hpp"
#include "include/rbd/librbd.hpp"
#include "cls/rbd/cls_rbd_types.h"

#include <vector>

namespace librbd {

class ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
class Migration {
public:
  static int prepare(librados::IoCtx& io_ctx, const std::string &image_name,
                     librados::IoCtx& dest_io_ctx,
                     const std::string &dest_image_name, ImageOptions& opts);
  static int execute(librados::IoCtx& io_ctx, const std::string &image_name,
                     ProgressContext &prog_ctx);
  static int abort(librados::IoCtx& io_ctx, const std::string &image_name,
                   ProgressContext &prog_ctx);
  static int commit(librados::IoCtx& io_ctx, const std::string &image_name,
                    ProgressContext &prog_ctx);
  static int status(librados::IoCtx& io_ctx, const std::string &image_name,
                    image_migration_status_t *status);

private:
  CephContext* m_cct;
  ImageCtxT *m_src_image_ctx;
  librados::IoCtx m_src_io_ctx;
  librados::IoCtx &m_dst_io_ctx;
  bool m_src_old_format;
  std::string m_src_image_name;
  std::string m_src_image_id;
  std::string m_src_header_oid;
  std::string m_dst_image_name;
  std::string m_dst_image_id;
  std::string m_dst_header_oid;
  ImageOptions &m_image_options;
  bool m_flatten;
  bool m_mirroring;
  ProgressContext *m_prog_ctx;

  cls::rbd::MigrationSpec m_src_migration_spec;
  cls::rbd::MigrationSpec m_dst_migration_spec;

  Migration(ImageCtxT *image_ctx, librados::IoCtx& dest_io_ctx,
            const std::string &dest_image_name, const std::string &dst_image_id,
            ImageOptions& opts, bool flatten, bool mirroring,
            cls::rbd::MigrationState state, const std::string &state_desc,
            ProgressContext *prog_ctx);

  int prepare();
  int execute();
  int abort();
  int commit();
  int status(image_migration_status_t *status);

  int set_state(cls::rbd::MigrationState state, const std::string &description);

  int list_src_snaps(std::vector<librbd::snap_info_t> *snaps);
  int validate_src_snaps();
  int disable_mirroring(ImageCtxT *image_ctx, bool *was_enabled);
  int enable_mirroring(ImageCtxT *image_ctx, bool was_enabled);
  int set_migration();
  int unlink_src_image();
  int relink_src_image();
  int create_dst_image();
  int remove_group(ImageCtxT *image_ctx, group_info_t *group_info);
  int add_group(ImageCtxT *image_ctx, group_info_t &group_info);
  int update_group(ImageCtxT *from_image_ctx, ImageCtxT *to_image_ctx);
  int remove_migration(ImageCtxT *image_ctx);
  int relink_children(ImageCtxT *from_image_ctx, ImageCtxT *to_image_ctx);
  int remove_src_image();

  int v1_set_migration();
  int v2_set_migration();
  int v1_unlink_src_image();
  int v2_unlink_src_image();
  int v1_relink_src_image();
  int v2_relink_src_image();

  int relink_child(ImageCtxT *from_image_ctx, ImageCtxT *to_image_ctx,
                   const librbd::snap_info_t &src_snap,
                   const librbd::linked_image_spec_t &child_image,
                   bool migration_abort, bool reattach_child);
};

} // namespace api
} // namespace librbd

extern template class librbd::api::Migration<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_API_MIGRATION_H
