// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/ConfigWatcher.h"
#include "common/config_obs.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/api/Config.h"
#include <deque>
#include <string>
#include <vector>
#include <boost/algorithm/string/predicate.hpp>

#include <shared_mutex> // for std::shared_lock

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ConfigWatcher: " \
                           << __func__ << ": "

namespace librbd {

template <typename I>
struct ConfigWatcher<I>::Observer : public md_config_obs_t {
  ConfigWatcher<I>* m_config_watcher;

  std::deque<std::string> m_config_key_strs;
  mutable std::vector<const char*> m_config_keys;

  Observer(CephContext* cct, ConfigWatcher<I>* config_watcher)
    : m_config_watcher(config_watcher) {
    const std::string rbd_key_prefix("rbd_");
    auto& schema = cct->_conf.get_schema();
    for (auto& pair : schema) {
      // watch all "rbd_" keys for simplicity
      if (!boost::starts_with(pair.first, rbd_key_prefix)) {
        continue;
      }

      m_config_key_strs.emplace_back(pair.first);
    }

    m_config_keys.reserve(m_config_key_strs.size());
    for (auto& key : m_config_key_strs) {
      m_config_keys.emplace_back(key.c_str());
    }
    m_config_keys.emplace_back(nullptr);
  }

  const char** get_tracked_conf_keys() const override {
    ceph_assert(!m_config_keys.empty());
    return &m_config_keys[0];
  }

  void handle_conf_change(const ConfigProxy& conf,
                          const std::set <std::string> &changed) override {
    m_config_watcher->handle_global_config_change(changed);
  }
};

template <typename I>
ConfigWatcher<I>::ConfigWatcher(I& image_ctx)
  : m_image_ctx(image_ctx) {
}

template <typename I>
ConfigWatcher<I>::~ConfigWatcher() {
  ceph_assert(m_observer == nullptr);
}

template <typename I>
void ConfigWatcher<I>::init() {
  auto cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  m_observer = new Observer(cct, this);
  cct->_conf.add_observer(m_observer);
}

template <typename I>
void ConfigWatcher<I>::shut_down() {
  auto cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  ceph_assert(m_observer != nullptr);
  cct->_conf.remove_observer(m_observer);

  delete m_observer;
  m_observer = nullptr;
}

template <typename I>
void ConfigWatcher<I>::handle_global_config_change(
    std::set<std::string> changed_keys) {

  {
    // ignore any global changes that are being overridden
    std::shared_lock image_locker{m_image_ctx.image_lock};
    for (auto& key : m_image_ctx.config_overrides) {
      changed_keys.erase(key);
    }
  }
  if (changed_keys.empty()) {
    return;
  }

  auto cct = m_image_ctx.cct;
  ldout(cct, 10) << "changed_keys=" << changed_keys << dendl;

  // refresh the image to pick up any global config overrides
  m_image_ctx.state->handle_update_notification();
}

} // namespace librbd

template class librbd::ConfigWatcher<librbd::ImageCtx>;
