// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_json.h"
#include "include/buffer_fwd.h"
#include "include/int_types.h"
#include "librbd/io/Types.h"
#include "cls/rbd/cls_rbd_client.h"
#include "ImageCache.h"
#include "librbd/ImageCtx.h"
#include "librbd/Operations.h"
#include <vector>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::ImageCache: "  \
                           <<  __func__ << ": "
class Context;

namespace librbd {

namespace cache {

template <typename I>
ImageCacheState<I>::ImageCacheState(I* image_ctx, JSONFormattable& f): m_image_ctx(image_ctx)  {
    m_present = (bool)f["present"];
    m_empty = (bool)f["empty"];
    m_clean = (bool)f["clean"];
}

template <typename I>
void ImageCacheState<I>::dump(Formatter *f) const {
  ::encode_json("present", m_present, f);
  ::encode_json("empty", m_empty, f);
  ::encode_json("clean", m_clean, f);
  ::encode_json("cache_type", m_cache_type, f);
}

template <typename I>
void ImageCacheState<I>::write_image_cache_state(Context *on_finish) {
  std::shared_lock owner_lock{m_image_ctx->owner_lock};
  JSONFormattable f;
  ::encode_json("image_cache_state", *this, &f);
  std::ostringstream oss;
  f.flush(oss);
  std::string image_state_json = oss.str();

  ldout(m_image_ctx->cct, 20) << __func__ << " Store state: " << image_state_json << dendl;
  m_image_ctx->operations->execute_metadata_set("image_cache_state", image_state_json, on_finish);
}

template <typename I>
void ImageCacheState<I>::clear_image_cache_state(Context *on_finish) {
  std::shared_lock owner_lock{m_image_ctx->owner_lock};
  ldout(m_image_ctx->cct, 20) << __func__ << " Remove state: " << dendl;
  m_image_ctx->operations->execute_metadata_remove("image_cache_state", on_finish);
}

template <typename I>
void ImageCache<I>::clear_image_cache_state(Context *on_finish) {
  m_cache_state->clear_image_cache_state(on_finish);
}

} // namespace cache
} // namespace librbd

template class librbd::cache::ImageCache<librbd::ImageCtx>;
template class librbd::cache::ImageCacheState<librbd::ImageCtx>;


