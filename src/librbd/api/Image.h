// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRBD_API_IMAGE_H
#define LIBRBD_API_IMAGE_H

#include "include/rbd/librbd.hpp"
#include "include/rados/librados_fwd.hpp"
#include "librbd/Types.h"
#include <map>
#include <set>
#include <string>

namespace librbd {

class ImageOptions;
class ProgressContext;

struct ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
struct Image {
  typedef std::map<std::string, std::string> ImageNameToIds;

  static int get_op_features(ImageCtxT *ictx, uint64_t *op_features);

  static int list_images(librados::IoCtx& io_ctx,
                         std::vector<image_spec_t> *images);
  static int list_images_v2(librados::IoCtx& io_ctx,
                            ImageNameToIds *images);

  static int get_parent(ImageCtxT *ictx,
                        librbd::linked_image_spec_t *parent_image,
                        librbd::snap_spec_t *parent_snap);

  static int list_children(ImageCtxT *ictx,
                           std::vector<librbd::linked_image_spec_t> *images);
  static int list_children(ImageCtxT *ictx,
                           const cls::rbd::ParentImageSpec &parent_spec,
                           std::vector<librbd::linked_image_spec_t> *images);

  static int list_descendants(IoCtx& io_ctx, const std::string &image_id,
                              const std::optional<size_t> &max_level,
                              std::vector<librbd::linked_image_spec_t> *images);
  static int list_descendants(ImageCtxT *ictx,
                              const std::optional<size_t> &max_level,
                              std::vector<librbd::linked_image_spec_t> *images);
  static int list_descendants(ImageCtxT *ictx,
                              const cls::rbd::ParentImageSpec &parent_spec,
                              const std::optional<size_t> &max_level,
                              std::vector<librbd::linked_image_spec_t> *images);

  static int deep_copy(ImageCtxT *ictx, librados::IoCtx& dest_md_ctx,
                       const char *destname, ImageOptions& opts,
                       ProgressContext &prog_ctx);
  static int deep_copy(ImageCtxT *src, ImageCtxT *dest, bool flatten,
                       ProgressContext &prog_ctx);

  static int snap_set(ImageCtxT *ictx,
                      const cls::rbd::SnapshotNamespace &snap_namespace,
	              const char *snap_name);
  static int snap_set(ImageCtxT *ictx, uint64_t snap_id);

  static int remove(librados::IoCtx& io_ctx, const std::string &image_name,
                    ProgressContext& prog_ctx);

};

} // namespace api
} // namespace librbd

extern template class librbd::api::Image<librbd::ImageCtx>;

#endif // LIBRBD_API_IMAGE_H
