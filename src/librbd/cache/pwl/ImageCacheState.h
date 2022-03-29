// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_RWL_IMAGE_CACHE_STATE_H
#define CEPH_LIBRBD_CACHE_RWL_IMAGE_CACHE_STATE_H

#include "json_spirit/json_spirit.h"
#include "librbd/ImageCtx.h"
#include "librbd/cache/Types.h"
#include <string>

namespace ceph {
  class Formatter;
}

namespace librbd {

namespace plugin { template <typename> struct Api; }

namespace cache {
namespace pwl {

template <typename ImageCtxT = ImageCtx>
class ImageCacheState {
private:
  ImageCtxT* m_image_ctx;
  plugin::Api<ImageCtxT>& m_plugin_api;
public:
  bool present = false;
  bool empty = true;
  bool clean = true;
  std::string host;
  std::string path;
  std::string mode;
  uint64_t size = 0;
  /* After reloading, the following data does not need to be read,
   * but recalculated. */
  utime_t stats_timestamp;
  uint64_t allocated_bytes = 0;
  uint64_t cached_bytes = 0;
  uint64_t dirty_bytes = 0;
  uint64_t free_bytes = 0;
  uint64_t hits_full = 0;
  uint64_t hits_partial = 0;
  uint64_t misses = 0;
  uint64_t hit_bytes = 0;
  uint64_t miss_bytes = 0;

  ImageCacheState(ImageCtxT* image_ctx, plugin::Api<ImageCtxT>& plugin_api);

  ImageCacheState(ImageCtxT* image_ctx, json_spirit::mObject& f,
                  plugin::Api<ImageCtxT>& plugin_api);

  ~ImageCacheState() {}

  ImageCacheType get_image_cache_mode() const {
    if (mode == "rwl") {
      return IMAGE_CACHE_TYPE_RWL;
    } else if (mode == "ssd") {
      return IMAGE_CACHE_TYPE_SSD;
    }
    return IMAGE_CACHE_TYPE_UNKNOWN;
  }

  void write_image_cache_state(Context *on_finish);

  void clear_image_cache_state(Context *on_finish);

  static ImageCacheState<ImageCtxT>* create_image_cache_state(
    ImageCtxT* image_ctx, plugin::Api<ImageCtxT>& plugin_api, int &r);

  static ImageCacheState<ImageCtxT>* get_image_cache_state(
    ImageCtxT* image_ctx, plugin::Api<ImageCtxT>& plugin_api);

  bool is_valid();
};

} // namespace pwl
} // namespace cache
} // namespace librbd

extern template class librbd::cache::pwl::ImageCacheState<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_RWL_IMAGE_CACHE_STATE_H
