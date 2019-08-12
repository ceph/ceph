// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_REALM_WATCHER_H
#define RGW_REALM_WATCHER_H

#include "include/rados/librados.hpp"
#include "include/ceph_assert.h"
#include "common/Timer.h"
#include "common/Cond.h"

class RGWRados;
class RGWRealm;

enum class RGWRealmNotify {
  Reload,
  ZonesNeedPeriod,
};
WRITE_RAW_ENCODER(RGWRealmNotify);

/**
 * RGWRealmWatcher establishes a watch on the current RGWRealm's control object,
 * and forwards notifications to registered observers.
 */
class RGWRealmWatcher : public librados::WatchCtx2 {
 public:
  /**
   * Watcher is an interface that allows the RGWRealmWatcher to pass
   * notifications on to other interested objects.
   */
  class Watcher {
   public:
    virtual ~Watcher() = default;

    virtual void handle_notify(RGWRealmNotify type,
                               bufferlist::const_iterator& p) = 0;
  };

  RGWRealmWatcher(CephContext* cct, const RGWRealm& realm);
  ~RGWRealmWatcher() override;

  /// register a watcher for the given notification type
  void add_watcher(RGWRealmNotify type, Watcher& watcher);

  /// respond to realm notifications by calling the appropriate watcher
  void handle_notify(uint64_t notify_id, uint64_t cookie,
                     uint64_t notifier_id, bufferlist& bl) override;

  /// reestablish the watch if it gets disconnected
  void handle_error(uint64_t cookie, int err) override;

 private:
  CephContext *const cct;

  /// keep a separate Rados client whose lifetime is independent of RGWRados
  /// so that we don't miss notifications during realm reconfiguration
  librados::Rados rados;
  librados::IoCtx pool_ctx;
  uint64_t watch_handle = 0;
  std::string watch_oid;

  int watch_start(const RGWRealm& realm);
  int watch_restart();
  void watch_stop();

  std::map<RGWRealmNotify, Watcher&> watchers;
};

#endif // RGW_REALM_WATCHER_H
