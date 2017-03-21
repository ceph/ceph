// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_REALM_RELOADER_H
#define RGW_REALM_RELOADER_H

#include "rgw_realm_watcher.h"
#include "common/Cond.h"

class RGWRados;

/**
 * RGWRealmReloader responds to new period notifications by recreating RGWRados
 * with the updated realm configuration.
 */
class RGWRealmReloader : public RGWRealmWatcher::Watcher {
 public:
  /**
   * Pauser is an interface to pause/resume frontends. Frontend cooperation
   * is required to ensure that they stop issuing requests on the old
   * RGWRados instance, and restart with the updated configuration.
   *
   * This abstraction avoids a depency on class RGWFrontend.
   */
  class Pauser {
   public:
    virtual ~Pauser() = default;

    /// pause all frontends while realm reconfiguration is in progress
    virtual void pause() = 0;
    /// resume all frontends with the given RGWRados instance
    virtual void resume(RGWRados* store) = 0;
  };

  RGWRealmReloader(RGWRados*& store, Pauser* frontends);
  ~RGWRealmReloader();

  /// respond to realm notifications by scheduling a reload()
  void handle_notify(RGWRealmNotify type, bufferlist::iterator& p) override;

 private:
  /// pause frontends and replace the RGWRados instance
  void reload();

  class C_Reload; //< Context that calls reload()

  /// main()'s RGWRados pointer as a reference, modified by reload()
  RGWRados*& store;
  Pauser *const frontends;

  /// reload() takes a significant amount of time, so we don't want to run
  /// it in the handle_notify() thread. we choose a timer thread instead of a
  /// Finisher because it allows us to cancel events that were scheduled while
  /// reload() is still running
  SafeTimer timer;
  Mutex mutex; //< protects access to timer and reload_scheduled
  Cond cond; //< to signal reload() after an invalid realm config
  C_Reload* reload_scheduled; //< reload() context if scheduled
};

#endif // RGW_REALM_RELOADER_H
