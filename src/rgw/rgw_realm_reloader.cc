// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_realm_reloader.h"

#include "rgw_auth_registry.h"
#include "rgw_bucket.h"
#include "rgw_log.h"
#include "rgw_rest.h"
#include "rgw_user.h"
#include "rgw_process_env.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h"

#include "services/svc_zone.h"

#include "common/errno.h"

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "rgw realm reloader: ")


// safe callbacks from SafeTimer are unnecessary. reload() can take a long
// time, so we don't want to hold the mutex and block handle_notify() for the
// duration
static constexpr bool USE_SAFE_TIMER_CALLBACKS = false;


RGWRealmReloader::RGWRealmReloader(RGWProcessEnv& env,
                                   const rgw::auth::ImplicitTenants& implicit_tenants,
                                   std::map<std::string, std::string>& service_map_meta,
                                   Pauser* frontends,
				   boost::asio::io_context& io_context)
  : env(env),
    implicit_tenants(implicit_tenants),
    service_map_meta(service_map_meta),
    frontends(frontends),
    io_context(io_context),
    timer(env.driver->ctx(), mutex, USE_SAFE_TIMER_CALLBACKS),
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
  if (!env.driver) {
    /* we're in the middle of reload */
    return;
  }

  CephContext *const cct = env.driver->ctx();

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
  CephContext *const cct = env.driver->ctx();
  const DoutPrefix dp(cct, dout_subsys, "rgw realm reloader: ");
  ldpp_dout(&dp, 1) << "Pausing frontends for realm update..." << dendl;

  frontends->pause();

  ldpp_dout(&dp, 1) << "Frontends paused" << dendl;

  // TODO: make RGWRados responsible for rgw_log_usage lifetime
  rgw_log_usage_finalize();

  // destroy the existing driver
  DriverManager::close_storage(env.driver);
  env.driver = nullptr;

  ldpp_dout(&dp, 1) << "driver closed" << dendl;
  {
    // allow a new notify to reschedule us. it's important that we do this
    // before we start loading the new realm, or we could miss some updates
    std::lock_guard lock{mutex};
    reload_scheduled = nullptr;
  }


  while (!env.driver) {
    // reload the new configuration from ConfigStore
    int r = env.site->load(&dp, null_yield, env.cfgstore);
    if (r == 0) {
      ldpp_dout(&dp, 1) << "Creating new driver" << dendl;

      // recreate and initialize a new driver
      DriverManager::Config cfg;
      cfg.store_name = "rados";
      cfg.filter_name = "none";
      env.driver = DriverManager::get_storage(&dp, cct, cfg, io_context,
          cct->_conf->rgw_enable_gc_threads,
          cct->_conf->rgw_enable_lc_threads,
          cct->_conf->rgw_enable_quota_threads,
          cct->_conf->rgw_run_sync_thread,
          cct->_conf.get_val<bool>("rgw_dynamic_resharding"),
          true, null_yield, // run notification thread
          cct->_conf->rgw_cache_enabled);
    }

    rgw::sal::Driver* store_cleanup = nullptr;
    {
      std::unique_lock lock{mutex};

      // failure to recreate RGWRados is not a recoverable error, but we
      // don't want to assert or abort the entire cluster.  instead, just
      // sleep until we get another notification, and retry until we get
      // a working configuration
      if (env.driver == nullptr) {
        ldpp_dout(&dp, -1) << "Failed to reload realm after a period "
            "configuration update. Waiting for a new update." << dendl;

        // sleep until another event is scheduled
	cond.wait(lock, [this] { return reload_scheduled; });
        ldpp_dout(&dp, 1) << "Woke up with a new configuration, retrying "
            "realm reload." << dendl;
      }

      if (reload_scheduled) {
        // cancel the event; we'll handle it now
        timer.cancel_event(reload_scheduled);
        reload_scheduled = nullptr;

        // if we successfully created a driver, clean it up outside of the lock,
        // then continue to loop and recreate another
        std::swap(env.driver, store_cleanup);
      }
    }

    if (store_cleanup) {
      ldpp_dout(&dp, 4) << "Got another notification, restarting realm "
          "reload." << dendl;

      DriverManager::close_storage(store_cleanup);
    }
  }

  int r = env.driver->register_to_service_map(&dp, "rgw", service_map_meta);
  if (r < 0) {
    ldpp_dout(&dp, -1) << "ERROR: failed to register to service map: " << cpp_strerror(-r) << dendl;

    /* ignore error */
  }

  ldpp_dout(&dp, 1) << "Finishing initialization of new driver" << dendl;
  // finish initializing the new driver
  ldpp_dout(&dp, 1) << " - REST subsystem init" << dendl;
  rgw_rest_init(cct, env.driver->get_zone()->get_zonegroup());
  ldpp_dout(&dp, 1) << " - usage subsystem init" << dendl;
  rgw_log_usage_init(cct, env.driver);

  /* Initialize the registry of auth strategies which will coordinate
   * the dynamic reconfiguration. */
  env.auth_registry = rgw::auth::StrategyRegistry::create(
      cct, implicit_tenants, env.driver);
  env.lua.manager = env.driver->get_lua_manager(env.lua.manager->luarocks_path());
  if (env.lua.background) {
    env.lua.background->set_manager(env.lua.manager.get());
  }

  ldpp_dout(&dp, 1) << "Resuming frontends with new realm configuration." << dendl;

  frontends->resume(env.driver);
}
