// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include "rgw_realm_watcher.h"
#include "include/rados/librados.hpp"

class DoutPrefixProvider;
class RGWRealm;

namespace rgw::rados {

class RadosRealmWatcher : public RGWRealmWatcher,
                          public librados::WatchCtx2 {
 public:
  RadosRealmWatcher(const DoutPrefixProvider* dpp, CephContext* cct,
                    librados::Rados& rados, const RGWRealm& realm);
  ~RadosRealmWatcher() override;

  /// respond to realm notifications by calling the appropriate watcher
  void handle_notify(uint64_t notify_id, uint64_t cookie,
                     uint64_t notifier_id, bufferlist& bl) override;

  /// reestablish the watch if it gets disconnected
  void handle_error(uint64_t cookie, int err) override;

 private:
  CephContext *const cct;

  librados::IoCtx pool_ctx;
  uint64_t watch_handle = 0;
  std::string watch_oid;

  int watch_start(const DoutPrefixProvider* dpp,
                  librados::Rados& rados,
                  const RGWRealm& realm);
  int watch_restart();
  void watch_stop();
};

} // namespace rgw::rados
