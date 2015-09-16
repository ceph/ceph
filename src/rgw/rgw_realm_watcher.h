// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_REALM_WATCHER_H
#define RGW_REALM_WATCHER_H

#include "include/rados/librados.hpp"
#include "common/Timer.h"
#include "common/Cond.h"

class RGWRados;

/**
 * RGWRealmWatcher establishes a watch on the current RGWRealm's control object,
 * and responds to notifications by recreating RGWRados with the updated realm
 * configuration.
 */
class RGWRealmWatcher : public librados::WatchCtx2 {
 public:
  /**
   * FrontendPauser is an interface to pause/resume frontends. Frontend
   * cooperation is required to ensure that they stop issuing requests on the
   * old RGWRados instance, and restart with the updated configuration.
   *
   * This abstraction avoids a depency on class RGWFrontend, which is only
   * defined in rgw_main.cc
   */
  class FrontendPauser {
   public:
    virtual ~FrontendPauser() = default;

    /// pause all frontends while realm reconfiguration is in progress
    virtual void pause() = 0;
    /// resume all frontends with the given RGWRados instance
    virtual void resume(RGWRados *store) = 0;
  };

  RGWRealmWatcher(CephContext *cct, RGWRados *&store,
                  FrontendPauser *frontends);
  ~RGWRealmWatcher();

  /// respond to realm notifications by scheduling a reconfigure()
  void handle_notify(uint64_t notify_id, uint64_t cookie,
                     uint64_t notifier_id, bufferlist& bl) override;

  /// reestablish the watch if it gets disconnected
  void handle_error(uint64_t cookie, int err) override;

 private:
  CephContext *cct;
  /// main()'s RGWRados pointer as a reference, modified by reconfigure()
  RGWRados *&store;
  FrontendPauser *frontends;

  /// to prevent a race between reconfigure() and another realm notify, we need
  /// to keep the watch open during reconfiguration. this means we need a
  /// separate Rados client whose lifetime is independent of RGWRados
  librados::Rados rados;
  librados::IoCtx pool_ctx;
  uint64_t watch_handle;
  std::string watch_oid;

  int watch_start();
  int watch_restart();
  void watch_stop();

  /// reconfigure() takes a significant amount of time, so we don't want to run
  /// it in the handle_notify() thread. we choose a timer thread, because we
  /// also want to add a delay (see rgw_realm_reconfigure_delay) so that we can
  /// batch up notifications within that window
  SafeTimer timer;
  Mutex mutex; //< protects access to timer and reconfigure_scheduled
  Cond cond; //< to signal reconfigure() after an invalid realm config
  bool reconfigure_scheduled; //< true if reconfigure() is scheduled in timer

  /// pause frontends and replace the RGWRados
  void reconfigure();

  class C_Reconfigure; //< Context to call reconfigure()
};

#endif // RGW_REALM_WATCHER_H
