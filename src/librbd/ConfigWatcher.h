// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CONFIG_WATCHER_H
#define CEPH_LIBRBD_CONFIG_WATCHER_H

#include <set>
#include <string>

struct Context;

namespace librbd {

struct ImageCtx;

template <typename ImageCtxT>
class ConfigWatcher {
public:
  static ConfigWatcher* create(ImageCtxT& image_ctx) {
    return new ConfigWatcher(image_ctx);
  }

  ConfigWatcher(ImageCtxT& image_ctx);
  ~ConfigWatcher();

  ConfigWatcher(const ConfigWatcher&) = delete;
  ConfigWatcher& operator=(const ConfigWatcher&) = delete;

  void init();
  void shut_down();

private:
  struct Observer;

  ImageCtxT& m_image_ctx;

  Observer* m_observer = nullptr;

  void handle_global_config_change(std::set<std::string> changed);

};

} // namespace librbd

extern template class librbd::ConfigWatcher<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CONFIG_WATCHER_H
