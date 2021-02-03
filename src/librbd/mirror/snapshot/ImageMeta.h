// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_IMAGE_META_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_IMAGE_META_H

#include "include/rados/librados.hpp"
#include <string>

struct Context;

namespace librbd {

struct ImageCtx;

namespace mirror {
namespace snapshot {

template <typename ImageCtxT>
class ImageMeta {
public:
  static ImageMeta* create(ImageCtxT* image_ctx,
                           const std::string& mirror_uuid) {
    return new ImageMeta(image_ctx, mirror_uuid);
  }

  ImageMeta(ImageCtxT* image_ctx, const std::string& mirror_uuid);

  void load(Context* on_finish);
  void save(Context* on_finish);

  bool resync_requested = false;

private:
  /**
   * @verbatim
   *
   * <start>
   *   |
   *   v
   * METADATA_GET
   *   |
   *   v
   * <idle>
   *   |
   *   v
   * METADATA_SET
   *   |
   *   v
   * NOTIFY_UPDATE
   *   |
   *   v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT* m_image_ctx;
  std::string m_mirror_uuid;

  bufferlist m_out_bl;

  void handle_load(Context* on_finish, int r);

  void handle_save(Context* on_finish, int r);

  void notify_update(Context* on_finish);
  void handle_notify_update(Context* on_finish, int r);

};

} // namespace snapshot
} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::snapshot::ImageMeta<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_IMAGE_META_H
