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
  static int prepare_import(const std::string& source_spec,
                            librados::IoCtx& dest_io_ctx,
                            const std::string &dest_image_name,
                            ImageOptions& opts);
  static int execute(librados::IoCtx& io_ctx, const std::string &image_name,
                     ProgressContext &prog_ctx);
  static int abort(librados::IoCtx& io_ctx, const std::string &image_name,
                   ProgressContext &prog_ctx);
  static int commit(librados::IoCtx& io_ctx, const std::string &image_name,
                    ProgressContext &prog_ctx);
  static int status(librados::IoCtx& io_ctx, const std::string &image_name,
                    image_migration_status_t *status);

  static int get_source_spec(ImageCtxT* image_ctx, std::string* source_spec);

private:
  CephContext* m_cct;
  ImageCtx* m_src_image_ctx;
  ImageCtx* m_dst_image_ctx;
  librados::IoCtx m_dst_io_ctx;
  std::string m_dst_image_name;
  std::string m_dst_image_id;
  std::string m_dst_header_oid;
  ImageOptions &m_image_options;
  bool m_flatten;
  bool m_mirroring;
  cls::rbd::MirrorImageMode m_mirror_image_mode;
  ProgressContext *m_prog_ctx;

  cls::rbd::MigrationSpec m_src_migration_spec;
  cls::rbd::MigrationSpec m_dst_migration_spec;

  Migration(ImageCtx* src_image_ctx, ImageCtx* dst_image_ctx,
            const cls::rbd::MigrationSpec& dst_migration_spec,
            ImageOptions& opts, ProgressContext *prog_ctx);

  int prepare();
  int prepare_import();
  int execute();
  int abort();
  int commit();
  int status(image_migration_status_t *status);

  int set_state(ImageCtxT* image_ctx, const std::string& image_description,
                cls::rbd::MigrationState state,
                const std::string &description);
  int set_state(cls::rbd::MigrationState state, const std::string &description);

  int list_src_snaps(ImageCtxT* image_ctx,
                     std::vector<librbd::snap_info_t> *snaps);
  int validate_src_snaps(ImageCtxT* image_ctx);
  int disable_mirroring(ImageCtxT* image_ctx, bool *was_enabled,
                        cls::rbd::MirrorImageMode *mirror_image_mode);
  int enable_mirroring(ImageCtxT* image_ctx, bool was_enabled,
                       cls::rbd::MirrorImageMode mirror_image_mode);
  int set_src_migration(ImageCtxT* image_ctx);
  int unlink_src_image(ImageCtxT* image_ctx);
  int relink_src_image(ImageCtxT* image_ctx);
  int create_dst_image(ImageCtxT** image_ctx);
  int remove_group(ImageCtxT* image_ctx, group_info_t *group_info);
  int add_group(ImageCtxT* image_ctx, group_info_t &group_info);
  int update_group(ImageCtxT *from_image_ctx, ImageCtxT *to_image_ctx);
  int remove_migration(ImageCtxT* image_ctx);
  int relink_children(ImageCtxT *from_image_ctx, ImageCtxT *to_image_ctx);
  int remove_src_image(ImageCtxT** image_ctx);

  int v1_set_src_migration(ImageCtxT* image_ctx);
  int v2_set_src_migration(ImageCtxT* image_ctx);
  int v1_unlink_src_image(ImageCtxT* image_ctx);
  int v2_unlink_src_image(ImageCtxT* image_ctx);
  int v1_relink_src_image(ImageCtxT* image_ctx);
  int v2_relink_src_image(ImageCtxT* image_ctx);

  int relink_child(ImageCtxT *from_image_ctx, ImageCtxT *to_image_ctx,
                   const librbd::snap_info_t &src_snap,
                   const librbd::linked_image_spec_t &child_image,
                   bool migration_abort, bool reattach_child);

  int revert_data(ImageCtxT* src_image_ctx, ImageCtxT* dst_image_ctx,
                  ProgressContext *prog_ctx);
};

} // namespace api
} // namespace librbd

extern template class librbd::api::Migration<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_API_MIGRATION_H
