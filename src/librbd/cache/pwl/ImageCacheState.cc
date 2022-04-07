// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/Types.h"
#include "librbd/cache/Utils.h"
#include "librbd/cache/pwl/ImageCacheState.h"
#include "librbd/ImageCtx.h"
#include "librbd/Operations.h"
#include "common/config_proxy.h"
#include "common/environment.h"
#include "common/hostname.h"
#include "librbd/plugin/Api.h"

#undef dout_subsys
#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl::ImageCacheState: " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace pwl {

template <typename I>
void ImageCacheState<I>::init_from_config() {
  ldout(m_image_ctx->cct, 20) << dendl;

  present = false;
  empty = true;
  clean = true;
  host = "";
  path = "";
  ConfigProxy &config = m_image_ctx->config;
  mode = config.get_val<std::string>("rbd_persistent_cache_mode");
  size = 0;
}

template <typename I>
bool ImageCacheState<I>::init_from_metadata(json_spirit::mValue& json_root) {
  ldout(m_image_ctx->cct, 20) << dendl;

  try {
    auto& o = json_root.get_obj();
    present = o["present"].get_bool();
    empty = o["empty"].get_bool();
    clean = o["clean"].get_bool();
    host = o["host"].get_str();
    path = o["path"].get_str();
    mode = o["mode"].get_str();
    size = o["size"].get_uint64();
  } catch (std::runtime_error& e) {
    lderr(m_image_ctx->cct) << "failed to parse cache state: " << e.what()
                            << dendl;
    return false;
  }

  return true;
}

template <typename I>
void ImageCacheState<I>::write_image_cache_state(Context *on_finish) {
  stats_timestamp = ceph_clock_now();
  std::shared_lock owner_lock{m_image_ctx->owner_lock};
  json_spirit::mObject o;
  o["present"] = present;
  o["empty"] = empty;
  o["clean"] = clean;
  o["host"] = host;
  o["path"] = path;
  o["mode"] = mode;
  o["size"] = size;
  o["stats_timestamp"] = stats_timestamp.sec();
  o["allocated_bytes"] = allocated_bytes;
  o["cached_bytes"] = cached_bytes;
  o["dirty_bytes"] = dirty_bytes;
  o["free_bytes"] = free_bytes;
  o["hits_full"] = hits_full;
  o["hits_partial"] = hits_partial;
  o["misses"] = misses;
  o["hit_bytes"] = hit_bytes;
  o["miss_bytes"] = miss_bytes;
  std::string image_state_json = json_spirit::write(o);

  ldout(m_image_ctx->cct, 20) << __func__ << " Store state: "
                              << image_state_json << dendl;
  m_plugin_api.execute_image_metadata_set(m_image_ctx, IMAGE_CACHE_STATE,
                                          image_state_json, on_finish);
}

template <typename I>
void ImageCacheState<I>::clear_image_cache_state(Context *on_finish) {
  std::shared_lock owner_lock{m_image_ctx->owner_lock};
  ldout(m_image_ctx->cct, 20) << __func__ << " Remove state: " << dendl;
  m_plugin_api.execute_image_metadata_remove(
    m_image_ctx, IMAGE_CACHE_STATE, on_finish);
}

template <typename I>
ImageCacheState<I>* ImageCacheState<I>::create_image_cache_state(
    I* image_ctx, plugin::Api<I>& plugin_api, int &r) {
  std::string cache_state_str;
  ImageCacheState<I>* cache_state = nullptr;

  r = 0;
  bool dirty_cache = plugin_api.test_image_features(image_ctx, RBD_FEATURE_DIRTY_CACHE);
  if (dirty_cache) {
    cls_client::metadata_get(&image_ctx->md_ctx, image_ctx->header_oid,
                             IMAGE_CACHE_STATE, &cache_state_str);
  }

  ldout(image_ctx->cct, 20) << "image_cache_state: " << cache_state_str << dendl;

  bool pwl_enabled = cache::util::is_pwl_enabled(*image_ctx);
  bool cache_desired = pwl_enabled;
  cache_desired &= !image_ctx->read_only;
  cache_desired &= !plugin_api.test_image_features(image_ctx, RBD_FEATURE_MIGRATING);
  cache_desired &= !plugin_api.test_image_features(image_ctx, RBD_FEATURE_JOURNALING);
  cache_desired &= !image_ctx->old_format;

  if (!dirty_cache && !cache_desired) {
    ldout(image_ctx->cct, 20) << "Do not desire to use image cache." << dendl;
  } else if (dirty_cache && !cache_desired) {
    lderr(image_ctx->cct) << "There's a dirty cache, but RWL cache is disabled."
                          << dendl;
    r = -EINVAL;
  }else if ((!dirty_cache || cache_state_str.empty()) && cache_desired) {
    cache_state = new ImageCacheState<I>(image_ctx, plugin_api);
    cache_state->init_from_config();
  } else {
    ceph_assert(!cache_state_str.empty());
    json_spirit::mValue json_root;
    if (!json_spirit::read(cache_state_str.c_str(), json_root)) {
      lderr(image_ctx->cct) << "failed to parse cache state" << dendl;
      r = -EINVAL;
      return nullptr;
    }
    cache_state = new ImageCacheState<I>(image_ctx, plugin_api);
    if (!cache_state->init_from_metadata(json_root)) {
      delete cache_state;
      r = -EINVAL;
      return nullptr;
    }
    if (!cache_state->present) {
      cache_state->init_from_config();
    }
  }
  return cache_state;
}

template <typename I>
ImageCacheState<I>* ImageCacheState<I>::get_image_cache_state(
    I* image_ctx, plugin::Api<I>& plugin_api) {
  ImageCacheState<I>* cache_state = nullptr;
  string cache_state_str;
  cls_client::metadata_get(&image_ctx->md_ctx, image_ctx->header_oid,
			   IMAGE_CACHE_STATE, &cache_state_str);
  if (!cache_state_str.empty()) {
    // ignore errors, best effort
    cache_state = new ImageCacheState<I>(image_ctx, plugin_api);
    json_spirit::mValue json_root;
    if (!json_spirit::read(cache_state_str.c_str(), json_root)) {
      lderr(image_ctx->cct) << "failed to parse cache state" << dendl;
    } else {
      cache_state->init_from_metadata(json_root);
    }
  }
  return cache_state;
}

template <typename I>
bool ImageCacheState<I>::is_valid() {
  if (this->present &&
      (host.compare(ceph_get_short_hostname()) != 0)) {
    auto cleanstring = "dirty";
    if (this->clean) {
      cleanstring = "clean";
    }
    lderr(m_image_ctx->cct) << "An image cache (RWL) remains on another host "
                            << host << " which is " << cleanstring
                            << ". Flush/close the image there to remove the "
                            << "image cache" << dendl;
    return false;
  }
  return true;
}

} // namespace pwl
} // namespace cache
} // namespace librbd

template class librbd::cache::pwl::ImageCacheState<librbd::ImageCtx>;
