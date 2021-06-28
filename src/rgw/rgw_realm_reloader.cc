// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_realm_reloader.h"

#include "rgw_bucket.h"
#include "rgw_log.h"
#include "rgw_rest.h"
#include "rgw_user.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h"

#include "services/svc_zone.h"

#include "common/errno.h"

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "rgw realm reloader: ")


// safe callbacks from SafeTimer are unneccessary. reload() can take a long
// time, so we don't want to hold the mutex and block handle_notify() for the
// duration
static constexpr bool USE_SAFE_TIMER_CALLBACKS = false;


RGWRealmReloader::RGWRealmReloader(rgw::sal::Store*& store, std::map<std::string, std::string>& service_map_meta,
                                   Pauser* frontends)
  : store(store),
    service_map_meta(service_map_meta),
    frontends(frontends),
    timer(store->ctx(), mutex, USE_SAFE_TIMER_CALLBACKS),
    mutex(ceph::make_mutex("RGWRealmReloader")),
    reload_scheduled(nullptr)
{
  timer.init();
}

RGWRealmReloader::~RGWRealmReloader()
{
  std::lock_guard lock{mutex};
  timer.shutdown();
}

class RGWRealmReloader::C_Reload : public Context {
  RGWRealmReloader* reloader;
 public:
  explicit C_Reload(RGWRealmReloader* reloader) : reloader(reloader) {}
  void finish(int r) override { reloader->reload(); }
};

void RGWRealmReloader::handle_notify(RGWRealmNotify type,
                                     bufferlist::const_iterator& p)
{
  if (!store) {
    /* we're in the middle of reload */
    return;
  }

  CephContext *const cct = store->ctx();

  std::lock_guard lock{mutex};
  if (reload_scheduled) {
    ldout(cct, 4) << "Notification on realm, reconfiguration "
        "already scheduled" << dendl;
    return;
  }

  reload_scheduled = new C_Reload(this);
  cond.notify_one(); // wake reload() if it blocked on a bad configuration

  // schedule reload() without delay
  timer.add_event_after(0, reload_scheduled);

  ldout(cct, 4) << "Notification on realm, reconfiguration scheduled" << dendl;
}

void RGWRealmReloader::reload()
{
  CephContext *const cct = store->ctx();
  const DoutPrefix dp(cct, dout_subsys, "rgw realm reloader: ");
  ldpp_dout(&dp, 1) << "Pausing frontends for realm update..." << dendl;

  frontends->pause();

  ldpp_dout(&dp, 1) << "Frontends paused" << dendl;

  // TODO: make RGWRados responsible for rgw_log_usage lifetime
  rgw_log_usage_finalize();

  // destroy the existing store
  StoreManager::close_storage(store);
  store = nullptr;

  ldpp_dout(&dp, 1) << "Store closed" << dendl;
  {
    // allow a new notify to reschedule us. it's important that we do this
    // before we start loading the new realm, or we could miss some updates
    std::lock_guard lock{mutex};
    reload_scheduled = nullptr;
  }


  while (!store) {
    // recreate and initialize a new store
    store =
      StoreManager::get_storage(&dp, cct,
				   "rados",
				   cct->_conf->rgw_enable_gc_threads,
				   cct->_conf->rgw_enable_lc_threads,
				   cct->_conf->rgw_enable_quota_threads,
				   cct->_conf->rgw_run_sync_thread,
				   cct->_conf.get_val<bool>("rgw_dynamic_resharding"),
				   cct->_conf->rgw_cache_enabled);

    ldpp_dout(&dp, 1) << "Creating new store" << dendl;

    rgw::sal::Store* store_cleanup = nullptr;
    {
      std::unique_lock lock{mutex};

      // failure to recreate RGWRados is not a recoverable error, but we
      // don't want to assert or abort the entire cluster.  instead, just
      // sleep until we get another notification, and retry until we get
      // a working configuration
      if (store == nullptr) {
        ldpp_dout(&dp, -1) << "Failed to reinitialize RGWRados after a realm "
            "configuration update. Waiting for a new update." << dendl;

        // sleep until another event is scheduled
	cond.wait(lock, [this] { return reload_scheduled; });
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
      ldpp_dout(&dp, 4) << "Got another notification, restarting RGWRados "
          "initialization." << dendl;

      StoreManager::close_storage(store_cleanup);
    }
  }

  int r = store->register_to_service_map(&dp, "rgw", service_map_meta);
  if (r < 0) {
    ldpp_dout(&dp, -1) << "ERROR: failed to register to service map: " << cpp_strerror(-r) << dendl;

    /* ignore error */
  }

  ldpp_dout(&dp, 1) << "Finishing initialization of new store" << dendl;
  // finish initializing the new store
  ldpp_dout(&dp, 1) << " - REST subsystem init" << dendl;
  rgw_rest_init(cct, store->get_zone()->get_zonegroup());
  ldpp_dout(&dp, 1) << " - usage subsystem init" << dendl;
  rgw_log_usage_init(cct, store);

  ldpp_dout(&dp, 1) << "Resuming frontends with new realm configuration." << dendl;

  frontends->resume(store);
}
