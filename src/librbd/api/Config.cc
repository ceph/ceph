// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Config.h"
#include "cls/rbd/cls_rbd_client.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/api/PoolMetadata.h"
#include <boost/algorithm/string/predicate.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Config: " << __func__ << ": "

namespace librbd {
namespace api {

namespace {

const uint32_t MAX_KEYS = 64;

typedef std::map<std::string_view, std::pair<std::string, config_source_t>> Parent;

static std::set<std::string_view> EXCLUDE_OPTIONS {
    "rbd_auto_exclusive_lock_until_manual_request",
    "rbd_default_format",
    "rbd_default_map_options",
    "rbd_default_pool",
    "rbd_discard_on_zeroed_write_same",
    "rbd_op_thread_timeout",
    "rbd_op_threads",
    "rbd_tracing",
    "rbd_validate_names",
    "rbd_validate_pool",
    "rbd_mirror_pool_replayers_refresh_interval"
  };
static std::set<std::string_view> EXCLUDE_IMAGE_OPTIONS {
    "rbd_default_clone_format",
    "rbd_default_data_pool",
    "rbd_default_features",
    "rbd_default_format",
    "rbd_default_order",
    "rbd_default_stripe_count",
    "rbd_default_stripe_unit",
    "rbd_journal_order",
    "rbd_journal_pool",
    "rbd_journal_splay_width"
  };

struct Options : Parent {
  librados::IoCtx m_io_ctx;

  Options(librados::IoCtx& io_ctx, bool image_apply_only_options) {
    m_io_ctx.dup(io_ctx);
    m_io_ctx.set_namespace("");

    CephContext *cct = reinterpret_cast<CephContext *>(m_io_ctx.cct());

    const std::string rbd_key_prefix("rbd_");
    const std::string rbd_mirror_key_prefix("rbd_mirror_");
    auto& schema = cct->_conf.get_schema();
    for (auto& pair : schema) {
      if (!boost::starts_with(pair.first, rbd_key_prefix)) {
        continue;
      } else if (EXCLUDE_OPTIONS.count(pair.first) != 0) {
        continue;
      } else if (image_apply_only_options &&
                 EXCLUDE_IMAGE_OPTIONS.count(pair.first) != 0) {
        continue;
      } else if (image_apply_only_options &&
                 boost::starts_with(pair.first, rbd_mirror_key_prefix)) {
        continue;
      }

      insert({pair.first, {}});
    }
  }

  int init() {
    CephContext *cct = (CephContext *)m_io_ctx.cct();

    for (auto& [k,v] : *this) {
      int r = cct->_conf.get_val(k, &v.first);
      ceph_assert(r == 0);
      v.second = RBD_CONFIG_SOURCE_CONFIG;
    }

    std::string last_key = ImageCtx::METADATA_CONF_PREFIX;
    bool more_results = true;

    while (more_results) {
      std::map<std::string, bufferlist> pairs;

      int r = librbd::api::PoolMetadata<>::list(m_io_ctx, last_key, MAX_KEYS,
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
  Options opts(io_ctx, false);

  return (opts.find(name) != opts.end());
}

template <typename I>
int Config<I>::list(librados::IoCtx& io_ctx,
                    std::vector<config_option_t> *options) {
  Options opts(io_ctx, false);

  int r = opts.init();
  if (r < 0) {
    return r;
  }

  for (auto& [k,v] : opts) {
    options->push_back({std::string{k}, v.first, v.second});
  }

  return 0;
}

template <typename I>
bool Config<I>::is_option_name(I *image_ctx, const std::string &name) {
  Options opts(image_ctx->md_ctx, true);

  return (opts.find(name) != opts.end());
}

template <typename I>
int Config<I>::list(I *image_ctx, std::vector<config_option_t> *options) {
  CephContext *cct = image_ctx->cct;
  Options opts(image_ctx->md_ctx, true);

  int r = opts.init();
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

  for (auto& [k,v] : opts) {
    options->push_back({std::string{k}, v.first, v.second});
  }

  return 0;
}

template <typename I>
void Config<I>::apply_pool_overrides(librados::IoCtx& io_ctx,
                                     ConfigProxy* config) {
  CephContext *cct = reinterpret_cast<CephContext *>(io_ctx.cct());

  Options opts(io_ctx, false);
  int r = opts.init();
  if (r < 0) {
    lderr(cct) << "failed to read pool config overrides: " << cpp_strerror(r)
               << dendl;
    return;
  }

  for (auto& [k,v] : opts) {
    if (v.second == RBD_CONFIG_SOURCE_POOL) {
      r = config->set_val(k, v.first);
      if (r < 0) {
        lderr(cct) << "failed to override pool config " << k << "="
                   << v.first << ": " << cpp_strerror(r) << dendl;
      }
    }
  }
}

} // namespace api
} // namespace librbd

template class librbd::api::Config<librbd::ImageCtx>;
