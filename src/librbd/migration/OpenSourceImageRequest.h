// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_OPEN_SOURCE_IMAGE_REQUEST_H
#define CEPH_LIBRBD_MIGRATION_OPEN_SOURCE_IMAGE_REQUEST_H

#include "librbd/Types.h"
#include <memory>

struct Context;

namespace librbd {

struct ImageCtx;

namespace migration {

struct FormatInterface;

template <typename ImageCtxT>
class OpenSourceImageRequest {
public:
  static OpenSourceImageRequest* create(ImageCtxT* destination_image_ctx,
                                        uint64_t src_snap_id,
                                        const MigrationInfo &migration_info,
                                        ImageCtxT** source_image_ctx,
                                        Context* on_finish) {
    return new OpenSourceImageRequest(destination_image_ctx, src_snap_id,
                                      migration_info, source_image_ctx,
                                      on_finish);
  }

  OpenSourceImageRequest(ImageCtxT* destination_image_ctx,
                         uint64_t src_snap_id,
                         const MigrationInfo &migration_info,
                         ImageCtxT** source_image_ctx,
                         Context* on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * OPEN_SOURCE
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT* m_dst_image_ctx;
  uint64_t m_src_snap_id;
  MigrationInfo m_migration_info;
  ImageCtxT** m_src_image_ctx;
  Context* m_on_finish;

  std::unique_ptr<FormatInterface> m_format;

  void open_source();
  void handle_open_source(int r);

  void finish(int r);

};

} // namespace migration
} // namespace librbd

extern template class librbd::migration::OpenSourceImageRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIGRATION_OPEN_SOURCE_IMAGE_REQUEST_H
