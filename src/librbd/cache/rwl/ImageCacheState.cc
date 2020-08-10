// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/Types.h"
#include "librbd/cache/Utils.h"
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
#define dout_prefix *_dout << "librbd::cache::rwl::ImageCacheState: " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace rwl {

namespace {
bool get_json_format(const std::string& s, JSONFormattable *f) {
  JSONParser p;
  bool success = p.parse(s.c_str(), s.size());
  if (success) {
    decode_json_obj(*f, &p);
  }
  return success;
}
} // namespace

template <typename I>
ImageCacheState<I>::ImageCacheState(I *image_ctx) : m_image_ctx(image_ctx) {
  ldout(image_ctx->cct, 20) << "Initialize RWL cache state with config data. "
                            << dendl;

  ConfigProxy &config = image_ctx->config;
  host = ceph_get_short_hostname();
  path = config.get_val<std::string>("rbd_rwl_path");
  size = config.get_val<uint64_t>("rbd_rwl_size");
  log_periodic_stats = config.get_val<bool>("rbd_rwl_log_periodic_stats");
}

template <typename I>
ImageCacheState<I>::ImageCacheState(
    I *image_ctx, JSONFormattable &f) : m_image_ctx(image_ctx) {
  ldout(image_ctx->cct, 20) << "Initialize RWL cache state with data from "
                            << "server side"<< dendl;

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
  ::encode_json(IMAGE_CACHE_STATE.c_str(), *this, &f);
  std::ostringstream oss;
  f.flush(oss);
  std::string image_state_json = oss.str();

  ldout(m_image_ctx->cct, 20) << __func__ << " Store state: "
                              << image_state_json << dendl;
  m_image_ctx->operations->execute_metadata_set(IMAGE_CACHE_STATE,
                                                image_state_json, on_finish);
}

template <typename I>
void ImageCacheState<I>::clear_image_cache_state(Context *on_finish) {
  std::shared_lock owner_lock{m_image_ctx->owner_lock};
  ldout(m_image_ctx->cct, 20) << __func__ << " Remove state: " << dendl;
  m_image_ctx->operations->execute_metadata_remove(IMAGE_CACHE_STATE, on_finish);
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

template <typename I>
ImageCacheState<I>* ImageCacheState<I>::get_image_cache_state(
    I* image_ctx, int &r) {
  std::string cache_state_str;
  ImageCacheState<I>* cache_state = nullptr;
  ldout(image_ctx->cct, 20) << "image_cache_state:" << cache_state_str << dendl;

  r = 0;
  bool dirty_cache = image_ctx->test_features(RBD_FEATURE_DIRTY_CACHE);
  if (dirty_cache) {
    cls_client::metadata_get(&image_ctx->md_ctx, image_ctx->header_oid,
                             IMAGE_CACHE_STATE, &cache_state_str);
  }

  bool rwl_enabled = cache::util::is_rwl_enabled(*image_ctx);
  bool cache_desired = rwl_enabled;
  cache_desired &= !image_ctx->read_only;
  cache_desired &= !image_ctx->test_features(RBD_FEATURE_MIGRATING);
  cache_desired &= !image_ctx->test_features(RBD_FEATURE_JOURNALING);
  cache_desired &= !image_ctx->old_format;

  if (!dirty_cache && !cache_desired) {
    ldout(image_ctx->cct, 20) << "Do not desire to use image cache." << dendl;
  } else if (dirty_cache && !cache_desired) {
    lderr(image_ctx->cct) << "There's a dirty cache, but RWL cache is disabled."
                          << dendl;
    r = -EINVAL;
  }else if ((!dirty_cache || cache_state_str.empty()) && cache_desired) {
    cache_state = new ImageCacheState<I>(image_ctx);
  } else {
    ceph_assert(!cache_state_str.empty());
    JSONFormattable f;
    bool success = get_json_format(cache_state_str, &f);
    if (!success) {
      lderr(image_ctx->cct) << "Failed to parse cache state: "
                            << cache_state_str << dendl;
      r = -EINVAL;
      return nullptr;
    }

    bool cache_exists = (bool)f["present"];
    int cache_type = (int)f["cache_type"];

    switch (cache_type) {
      case IMAGE_CACHE_TYPE_RWL:
        if (!cache_exists) {
          cache_state = new ImageCacheState<I>(image_ctx);
        } else {
          cache_state = new ImageCacheState<I>(image_ctx, f);
        }
        break;
      default:
	r = -EINVAL;
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

} // namespace rwl
} // namespace cache
} // namespace librbd

template class librbd::cache::rwl::ImageCacheState<librbd::ImageCtx>;
