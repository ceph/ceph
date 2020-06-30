// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/rwl/ImageCacheState.h"
#include "librbd/ImageCtx.h"
#include "librbd/Operations.h"
#include "common/environment.h"
#include "common/hostname.h"
#include "common/config_proxy.h"
#include "common/ceph_json.h"

#undef dout_subsys
#define dout_subsys ceph_subsys_rbd_rwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::rwl::ImageCacheState: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace rwl {

template <typename I>
const std::string ImageCacheState<I>::image_cache_state = ".librbd/image_cache_state";

template <typename I>
ImageCacheState<I>::ImageCacheState(I *image_ctx) : m_image_ctx(image_ctx) {
  ldout(image_ctx->cct, 20) << "Initialize RWL cache state with config data. " << dendl;

  ConfigProxy &config = image_ctx->config;
  host = ceph_get_short_hostname();
  path = config.get_val<std::string>("rbd_rwl_path");
  size = config.get_val<uint64_t>("rbd_rwl_size");
  log_periodic_stats = config.get_val<bool>("rbd_rwl_log_periodic_stats");
}

template <typename I>
ImageCacheState<I>::ImageCacheState(I *image_ctx, JSONFormattable &f) : m_image_ctx(image_ctx) {
  ldout(image_ctx->cct, 20) << "Initialize RWL cache state with data from server side" << dendl;

  present = (bool)f["present"];
  empty = (bool)f["empty"];
  clean = (bool)f["clean"];
  host = (string)f["rwl_host"];
  path = (string)f["rwl_path"];
  uint64_t rwl_size;
  std::istringstream iss(f["rwl_size"]);
  iss >> rwl_size;
  size = rwl_size;

  // Others from config
  ConfigProxy &config = image_ctx->config;
  log_periodic_stats = config.get_val<bool>("rbd_rwl_log_periodic_stats");
}

template <typename I>
void ImageCacheState<I>::write_image_cache_state(Context *on_finish) {
  std::shared_lock owner_lock{m_image_ctx->owner_lock};
  JSONFormattable f;
  ::encode_json(image_cache_state.c_str(), *this, &f);
  std::ostringstream oss;
  f.flush(oss);
  std::string image_state_json = oss.str();

  ldout(m_image_ctx->cct, 20) << __func__ << " Store state: " << image_state_json << dendl;
  m_image_ctx->operations->execute_metadata_set(image_cache_state.c_str(), image_state_json, on_finish);
}

template <typename I>
void ImageCacheState<I>::clear_image_cache_state(Context *on_finish) {
  std::shared_lock owner_lock{m_image_ctx->owner_lock};
  ldout(m_image_ctx->cct, 20) << __func__ << " Remove state: " << dendl;
  m_image_ctx->operations->execute_metadata_remove(image_cache_state.c_str(), on_finish);
}

template <typename I>
void ImageCacheState<I>::dump(ceph::Formatter *f) const {
  ::encode_json("present", present, f);
  ::encode_json("empty", empty, f);
  ::encode_json("clean", clean, f);
  ::encode_json("cache_type", (int)get_image_cache_type(), f);
  ::encode_json("rwl_host", host, f);
  ::encode_json("rwl_path", path, f);
  ::encode_json("rwl_size", size, f);
}

} // namespace rwl
} // namespace cache
} // namespace librbd

template class librbd::cache::rwl::ImageCacheState<librbd::ImageCtx>;
