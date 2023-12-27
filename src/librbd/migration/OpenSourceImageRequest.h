// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_OPEN_SOURCE_IMAGE_REQUEST_H
#define CEPH_LIBRBD_MIGRATION_OPEN_SOURCE_IMAGE_REQUEST_H

#include "include/rados/librados_fwd.hpp"
#include "librbd/Types.h"
#include <map>
#include <memory>

struct Context;

namespace librbd {

struct ImageCtx;

namespace migration {

template <typename ImageCtxT>
struct FormatInterface;

template <typename ImageCtxT>
class OpenSourceImageRequest {
public:
  static OpenSourceImageRequest* create(librados::IoCtx& io_ctx,
                                        ImageCtxT* destination_image_ctx,
                                        uint64_t src_snap_id,
                                        const MigrationInfo &migration_info,
                                        ImageCtxT** source_image_ctx,
                                        Context* on_finish) {
    return new OpenSourceImageRequest(io_ctx, destination_image_ctx,
                                      src_snap_id, migration_info,
                                      source_image_ctx, on_finish);
  }

  OpenSourceImageRequest(librados::IoCtx& io_ctx,
                         ImageCtxT* destination_image_ctx,
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
   * GET_IMAGE_SIZE  * * * * * * *
   *    |                        *
   *    v                        v
   * GET_SNAPSHOTS * * * * > CLOSE_IMAGE
   *    |                        |
   *    v                        |
   * <finish> <------------------/
   *
   * @endverbatim
   */

  CephContext* m_cct;
  librados::IoCtx& m_io_ctx;
  ImageCtxT* m_dst_image_ctx;
  uint64_t m_src_snap_id;
  MigrationInfo m_migration_info;
  ImageCtxT** m_src_image_ctx;
  Context* m_on_finish;

  std::unique_ptr<FormatInterface<ImageCtxT>> m_format;

  uint64_t m_image_size = 0;
  SnapInfos m_snap_infos;

  void open_source();
  void handle_open_source(int r);

  void get_image_size();
  void handle_get_image_size(int r);

  void get_snapshots();
  void handle_get_snapshots(int r);

  void close_image(int r);

  void register_image_dispatch();

  void finish(int r);

};

} // namespace migration
} // namespace librbd

extern template class librbd::migration::OpenSourceImageRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIGRATION_OPEN_SOURCE_IMAGE_REQUEST_H
