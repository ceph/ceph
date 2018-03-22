// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRBD_API_IMAGE_H
#define LIBRBD_API_IMAGE_H

#include "librbd/Types.h"
#include <map>
#include <set>
#include <string>

namespace librados { struct IoCtx; }

namespace librbd {

class ImageOptions;
class ProgressContext;

struct ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
struct Image {
  typedef std::pair<int64_t, std::string> PoolSpec;
  typedef std::set<std::string> ImageIds;
  typedef std::map<PoolSpec, ImageIds> PoolImageIds;
  typedef std::map<std::string, std::string> ImageNameToIds;

  static int get_op_features(ImageCtxT *ictx, uint64_t *op_features);

  static int list_images(librados::IoCtx& io_ctx,
                         ImageNameToIds *images);

  static int list_children(ImageCtxT *ictx, const ParentSpec &parent_spec,
                           PoolImageIds *pool_image_ids);

  static int deep_copy(ImageCtxT *ictx, librados::IoCtx& dest_md_ctx,
                       const char *destname, ImageOptions& opts,
                       ProgressContext &prog_ctx);
  static int deep_copy(ImageCtxT *src, ImageCtxT *dest,
                       ProgressContext &prog_ctx);
};

} // namespace api
} // namespace librbd

extern template class librbd::api::Image<librbd::ImageCtx>;

#endif // LIBRBD_API_IMAGE_H
