// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Config.h"
#include "cls/rbd/cls_rbd_client.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/api/PoolMetadata.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Config: " << __func__ << ": "

namespace librbd {
namespace api {

namespace {

const uint32_t MAX_KEYS = 64;

typedef std::map<std::string, std::pair<std::string, config_source_t>> Parent;

struct Options : Parent {
  Options(bool image_apply_only_options) :
    Parent{
      {"rbd_atime_update_interval", {}},
      {"rbd_balance_parent_reads", {}},
      {"rbd_balance_snap_reads", {}},
      {"rbd_blacklist_expire_seconds", {}},
      {"rbd_blacklist_on_break_lock", {}},
      {"rbd_cache", {}},
      {"rbd_cache_block_writes_upfront", {}},
      {"rbd_cache_max_dirty", {}},
      {"rbd_cache_max_dirty_age", {}},
      {"rbd_cache_max_dirty_object", {}},
      {"rbd_cache_size", {}},
      {"rbd_cache_target_dirty", {}},
      {"rbd_cache_writethrough_until_flush", {}},
      {"rbd_clone_copy_on_read", {}},
      {"rbd_concurrent_management_ops", {}},
      {"rbd_journal_commit_age", {}},
      {"rbd_journal_max_concurrent_object_sets", {}},
      {"rbd_journal_max_payload_bytes", {}},
      {"rbd_journal_object_flush_age", {}},
      {"rbd_journal_object_flush_bytes", {}},
      {"rbd_journal_object_flush_interval", {}},
      {"rbd_journal_order", {}},
      {"rbd_journal_pool", {}},
      {"rbd_journal_splay_width", {}},
      {"rbd_localize_parent_reads", {}},
      {"rbd_localize_snap_reads", {}},
      {"rbd_mirroring_delete_delay", {}},
      {"rbd_mirroring_replay_delay", {}},
      {"rbd_mirroring_resync_after_disconnect", {}},
      {"rbd_mtime_update_interval", {}},
      {"rbd_non_blocking_aio", {}},
      {"rbd_qos_bps_limit", {}},
      {"rbd_qos_iops_limit", {}},
      {"rbd_qos_read_bps_limit", {}},
      {"rbd_qos_read_iops_limit", {}},
      {"rbd_qos_write_bps_limit", {}},
      {"rbd_qos_write_iops_limit", {}},
      {"rbd_readahead_disable_after_bytes", {}},
      {"rbd_readahead_max_bytes", {}},
      {"rbd_readahead_trigger_requests", {}},
      {"rbd_request_timed_out_seconds", {}},
      {"rbd_skip_partial_discard", {}},
      {"rbd_sparse_read_threshold_bytes", {}},
    } {
    if (!image_apply_only_options) {
      Parent image_create_opts = {
        {"rbd_default_data_pool", {}},
        {"rbd_default_features", {}},
        {"rbd_default_order", {}},
        {"rbd_default_stripe_count", {}},
        {"rbd_default_stripe_unit", {}},
        {"rbd_journal_order", {}},
        {"rbd_journal_pool", {}},
        {"rbd_journal_splay_width", {}},
      };
      insert(image_create_opts.begin(), image_create_opts.end());
    }
  }

  int init(librados::IoCtx& io_ctx) {
    CephContext *cct = (CephContext *)io_ctx.cct();

    for (auto &it : *this) {
      int r = cct->_conf.get_val(it.first.c_str(), &it.second.first);
      ceph_assert(r == 0);
      it.second.second = RBD_CONFIG_SOURCE_CONFIG;
    }

    std::string last_key = ImageCtx::METADATA_CONF_PREFIX;
    bool more_results = true;

    while (more_results) {
      std::map<std::string, bufferlist> pairs;

      int r = librbd::api::PoolMetadata<>::list(io_ctx, last_key, MAX_KEYS,
                                                &pairs);
      if (r < 0) {
        return r;
      }

      if (pairs.empty()) {
        break;
      }

      more_results = (pairs.size() == MAX_KEYS);
      last_key = pairs.rbegin()->first;

      for (auto kv : pairs) {
        std::string key;
        if (!util::is_metadata_config_override(kv.first, &key)) {
          more_results = false;
          break;
        }
        auto it = find(key);
        if (it != end()) {
          it->second = {{kv.second.c_str(), kv.second.length()},
                        RBD_CONFIG_SOURCE_POOL};
        }
      }
    }
    return 0;
  }
};

} // anonymous namespace

template <typename I>
bool Config<I>::is_option_name(librados::IoCtx& io_ctx,
                               const std::string &name) {
  Options opts(false);

  return (opts.find(name) != opts.end());
}

template <typename I>
int Config<I>::list(librados::IoCtx& io_ctx,
                    std::vector<config_option_t> *options) {
  Options opts(false);

  int r = opts.init(io_ctx);
  if (r < 0) {
    return r;
  }

  for (auto &it : opts) {
    options->push_back({it.first, it.second.first, it.second.second});
  }

  return 0;
}

template <typename I>
bool Config<I>::is_option_name(I *image_ctx, const std::string &name) {
  Options opts(true);

  return (opts.find(name) != opts.end());
}

template <typename I>
int Config<I>::list(I *image_ctx, std::vector<config_option_t> *options) {
  CephContext *cct = image_ctx->cct;
  Options opts(true);

  int r = opts.init(image_ctx->md_ctx);
  if (r < 0) {
    return r;
  }

  std::string last_key = ImageCtx::METADATA_CONF_PREFIX;
  bool more_results = true;

  while (more_results) {
    std::map<std::string, bufferlist> pairs;

    r = cls_client::metadata_list(&image_ctx->md_ctx, image_ctx->header_oid,
                                  last_key, MAX_KEYS, &pairs);
    if (r < 0) {
      lderr(cct) << "failed reading image metadata: " << cpp_strerror(r)
                 << dendl;
      return r;
    }

    if (pairs.empty()) {
      break;
    }

    more_results = (pairs.size() == MAX_KEYS);
    last_key = pairs.rbegin()->first;

    for (auto kv : pairs) {
      std::string key;
      if (!util::is_metadata_config_override(kv.first, &key)) {
        more_results = false;
        break;
      }
      auto it = opts.find(key);
      if (it != opts.end()) {
        it->second = {{kv.second.c_str(), kv.second.length()},
                      RBD_CONFIG_SOURCE_IMAGE};
      }
    }
  }

  for (auto &it : opts) {
    options->push_back({it.first, it.second.first, it.second.second});
  }

  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::Config<librbd::ImageCtx>;
