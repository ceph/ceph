// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


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
#define dout_prefix *_dout << "librbd::cache: "  \
                           <<  __func__ << ": "
class Context;

namespace librbd {

namespace cache {

template <typename I>
std::string ImageCacheState<I>::get_json_list() {
  std::string present_str, empty_str, clean_str, type_str;
  if (m_present) {
    present_str = "1";
  } else {
    present_str = "0";
  }
  if (m_empty) {
    empty_str = "1";
  } else {
    empty_str = "0";
  }
  if (m_clean) {
    clean_str = "1";
  } else {
    clean_str = "0";
  }
  auto cache_type = get_image_cache_type();
  switch (cache_type) {
    case IMAGE_CACHE_TYPE_RWL:
      type_str = "1";
      break;
    default:
      type_str = "2";
  }
  std::string json_str = "\"present\": \"" + present_str + "\"," +
                    "\"empty\": \"" + empty_str + "\"," +
                    "\"clean\": \"" + clean_str + "\"," +
		    "\"cache_type\":\"" + type_str + "\",";
  return json_str;
}

template <typename I>
void ImageCacheState<I>::write_image_cache_state(Context *on_finish) {
  Context *metadata_ctx = on_finish;
  {
    RWLock::RLocker owner_lock(m_image_ctx->owner_lock);
    std::string image_state_json = get_json_string();
    m_image_ctx->operations->execute_metadata_set("image_cache_state", image_state_json, metadata_ctx);
    ldout(m_image_ctx->cct, 20) << __func__ << " Store state: " << image_state_json << dendl;
  }
  metadata_ctx->sync_complete(0);
}

} // namespace cache
} // namespace librbd

template class librbd::cache::ImageCache<librbd::ImageCtx>;
template class librbd::cache::ImageCacheState<librbd::ImageCtx>;


