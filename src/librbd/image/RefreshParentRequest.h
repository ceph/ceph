// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_REFRESH_PARENT_REQUEST_H
#define CEPH_LIBRBD_IMAGE_REFRESH_PARENT_REQUEST_H

#include "include/int_types.h"
#include "librbd/Types.h"

class Context;

namespace librbd {

class ImageCtx;

namespace image {

template <typename ImageCtxT = ImageCtx>
class RefreshParentRequest {
public:
  static RefreshParentRequest *create(ImageCtxT &child_image_ctx,
                                      const ParentImageInfo &parent_md,
                                      const MigrationInfo &migration_info,
                                      Context *on_finish) {
    return new RefreshParentRequest(child_image_ctx, parent_md, migration_info,
                                    on_finish);
  }

  static bool is_refresh_required(ImageCtxT &child_image_ctx,
                                  const ParentImageInfo &parent_md,
                                  const MigrationInfo &migration_info);

  void send();
  void apply();
  void finalize(Context *on_finish);

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    | (open required)
   *    |----------------> OPEN_PARENT * * * * * * * * * * * * * * *
   *    |                     |                                    *
   *    |                     v                        (on error)  *
   *    \----------------> <apply>                                 *
   *                          |                                    *
   *                          | (close required)                   *
   *                          |-----------------> CLOSE_PARENT     *
   *                          |                      |             *
   *                          |                      v             *
   *                          |                   RESET_EXISTENCE  *
   *                          |                      |             *
   *                          |                      v             *
   *                          \-----------------> <finish> < * * * *
   *
   * @endverbatim
   */

  RefreshParentRequest(ImageCtxT &child_image_ctx,
                       const ParentImageInfo &parent_md,
                       const MigrationInfo &migration_info, Context *on_finish);

  ImageCtxT &m_child_image_ctx;
  ParentImageInfo m_parent_md;
  MigrationInfo m_migration_info;
  Context *m_on_finish;

  ImageCtxT *m_parent_image_ctx;
  uint64_t m_parent_snap_id;

  int m_error_result;

  static bool is_close_required(ImageCtxT &child_image_ctx,
                                const ParentImageInfo &parent_md,
                                const MigrationInfo &migration_info);
  static bool is_open_required(ImageCtxT &child_image_ctx,
                               const ParentImageInfo &parent_md,
                               const MigrationInfo &migration_info);
  static bool does_parent_exist(ImageCtxT &child_image_ctx,
                                const ParentImageInfo &parent_md,
                                const MigrationInfo &migration_info);

  void send_open_parent();
  Context *handle_open_parent(int *result);

  void send_close_parent();
  Context *handle_close_parent(int *result);

  void send_reset_existence_cache();
  Context *handle_reset_existence_cache(int *result);

  void send_complete(int r);

  void save_result(int *result) {
    if (m_error_result == 0 && *result < 0) {
      m_error_result = *result;
    }
  }

};

} // namespace image
} // namespace librbd

extern template class librbd::image::RefreshParentRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_REFRESH_PARENT_REQUEST_H
