// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_realm_reloader.h"
#include "rgw_rados.h"

#include "rgw_bucket.h"
#include "rgw_log.h"
#include "rgw_rest.h"
#include "rgw_user.h"

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "rgw realm reloader: ")


// safe callbacks from SafeTimer are unneccessary. reload() can take a long
// time, so we don't want to hold the mutex and block handle_notify() for the
// duration
static constexpr bool USE_SAFE_TIMER_CALLBACKS = false;


RGWRealmReloader::RGWRealmReloader(RGWRados*& store, Pauser* frontends)
  : store(store),
    frontends(frontends),
    timer(store->ctx(), mutex, USE_SAFE_TIMER_CALLBACKS),
    mutex("RGWRealmReloader"),
    reload_scheduled(nullptr)
{
  timer.init();
}

RGWRealmReloader::~RGWRealmReloader()
{
  Mutex::Locker lock(mutex);
  timer.shutdown();
}

class RGWRealmReloader::C_Reload : public Context {
  RGWRealmReloader* reloader;
 public:
  C_Reload(RGWRealmReloader* reloader) : reloader(reloader) {}
  void finish(int r) { reloader->reload(); }
};

void RGWRealmReloader::handle_notify(RGWRealmNotify type,
                                     bufferlist::iterator& p)
{
  if (!store) {
    /* we're in the middle of reload */
    return;
  }

  CephContext *const cct = store->ctx();

  Mutex::Locker lock(mutex);
  if (reload_scheduled) {
    ldout(cct, 4) << "Notification on realm, reconfiguration "
        "already scheduled" << dendl;
    return;
  }

  reload_scheduled = new C_Reload(this);
  cond.SignalOne(); // wake reload() if it blocked on a bad configuration

  // schedule reload() with a delay so we can batch up changes
  auto delay = cct->_conf->rgw_realm_reconfigure_delay;
  timer.add_event_after(delay, reload_scheduled);

  ldout(cct, 4) << "Notification on realm, reconfiguration scheduled in "
      << delay << 's' << dendl;
}

void RGWRealmReloader::reload()
{
  CephContext *const cct = store->ctx();
  ldout(cct, 1) << "Pausing frontends for realm update..." << dendl;

  frontends->pause();

  ldout(cct, 1) << "Frontends paused" << dendl;

  // TODO: make RGWRados responsible for rgw_log_usage lifetime
  rgw_log_usage_finalize();

  // destroy the existing store
  RGWStoreManager::close_storage(store);
  store = nullptr;

  ldout(cct, 1) << "Store closed" << dendl;
  {
    // allow a new notify to reschedule us. it's important that we do this
    // before we start loading the new realm, or we could miss some updates
    Mutex::Locker lock(mutex);
    reload_scheduled = nullptr;
  }

  while (!store) {
    // recreate and initialize a new store
    store = RGWStoreManager::get_storage(cct,
                                         cct->_conf->rgw_enable_gc_threads,
                                         cct->_conf->rgw_enable_quota_threads,
                                         cct->_conf->rgw_run_sync_thread);

    ldout(cct, 1) << "Creating new store" << dendl;

    RGWRados* store_cleanup = nullptr;
    {
      Mutex::Locker lock(mutex);

      // failure to recreate RGWRados is not a recoverable error, but we
      // don't want to assert or abort the entire cluster.  instead, just
      // sleep until we get another notification, and retry until we get
      // a working configuration
      if (store == nullptr) {
        lderr(cct) << "Failed to reinitialize RGWRados after a realm "
            "configuration update. Waiting for a new update." << dendl;

        // sleep until another event is scheduled
        while (!reload_scheduled)
          cond.Wait(mutex);

        ldout(cct, 1) << "Woke up with a new configuration, retrying "
            "RGWRados initialization." << dendl;
      }

      if (reload_scheduled) {
        // cancel the event; we'll handle it now
        timer.cancel_event(reload_scheduled);
        reload_scheduled = nullptr;

        // if we successfully created a store, clean it up outside of the lock,
        // then continue to loop and recreate another
        std::swap(store, store_cleanup);
      }
    }

    if (store_cleanup) {
      ldout(cct, 4) << "Got another notification, restarting RGWRados "
          "initialization." << dendl;

      RGWStoreManager::close_storage(store_cleanup);
    }
  }

  ldout(cct, 1) << "Finishing initialization of new store" << dendl;
  // finish initializing the new store
  ldout(cct, 1) << " - REST subsystem init" << dendl;
  rgw_rest_init(cct, store, store->get_zonegroup());
  ldout(cct, 1) << " - user subsystem init" << dendl;
  rgw_user_init(store);
  ldout(cct, 1) << " - user subsystem init" << dendl;
  rgw_bucket_init(store->meta_mgr);
  ldout(cct, 1) << " - usage subsystem init" << dendl;
  rgw_log_usage_init(cct, store);

  ldout(cct, 1) << "Resuming frontends with new realm configuration." << dendl;

  frontends->resume(store);
}
