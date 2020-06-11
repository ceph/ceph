// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_RWL_IMAGE_CACHE_STATE_H
#define CEPH_LIBRBD_CACHE_RWL_IMAGE_CACHE_STATE_H 

#include "librbd/ImageCtx.h"
#include "librbd/cache/Types.h"
#include <string>

class JSONFormattable;
namespace ceph {
  class Formatter;
}

namespace librbd {
namespace cache {
namespace rwl {

template <typename ImageCtxT = ImageCtx>
class ImageCacheState {
private:
  ImageCtxT* m_image_ctx;
public:
  bool present = true;
  bool empty = true;
  bool clean = true;
  std::string host;
  std::string path;
  uint64_t size;
  bool log_periodic_stats;

  ImageCacheState(ImageCtxT* image_ctx);

  ImageCacheState(ImageCtxT* image_ctx, JSONFormattable& f);

  ~ImageCacheState() {}

  ImageCacheType get_image_cache_type() const {
    return IMAGE_CACHE_TYPE_RWL;
  }


  void write_image_cache_state(Context *on_finish);

  void clear_image_cache_state(Context *on_finish);

  void dump(ceph::Formatter *f) const;

  static void get_image_cache_state(ImageCtxT* image_ctx, Context *on_finish);

  bool is_valid();
};

} // namespace rwl
} // namespace cache
} // namespace librbd

extern template class librbd::cache::rwl::ImageCacheState<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_RWL_IMAGE_CACHE_STATE_H
