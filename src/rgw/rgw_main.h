// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <vector>
#include <map>
#include <string>

#include "common/async/context_pool.h"

#include "rgw_common.h"
#include "rgw_rest.h"
#include "rgw_frontend.h"
#include "rgw_period_pusher.h"
#include "rgw_realm_reloader.h"
#include "rgw_ldap.h"
#include "rgw_lua.h"
#include "rgw_dmclock_scheduler_ctx.h"
#include "rgw_ratelimit.h"


class RGWPauser : public RGWRealmReloader::Pauser {
  std::vector<Pauser*> pausers;

public:
  ~RGWPauser() override = default;

  void add_pauser(Pauser* pauser) {
    pausers.push_back(pauser);
  }

  void pause() override {
    std::for_each(pausers.begin(), pausers.end(), [](Pauser* p){p->pause();});
  }
  void resume(rgw::sal::Driver* driver) override {
    std::for_each(pausers.begin(), pausers.end(), [driver](Pauser* p){p->resume(driver);});
  }

};

namespace rgw {

namespace lua { class Background; }
namespace sal { class ConfigStore; }

class RGWLib;
class AppMain {
  /* several components should be initalized only if librgw is
    * also serving HTTP */
  bool have_http_frontend{false};
  bool nfs{false};

  std::vector<RGWFrontend*> fes;
  std::vector<RGWFrontendConfig*> fe_configs;
  std::multimap<string, RGWFrontendConfig*> fe_map;
  std::unique_ptr<rgw::LDAPHelper> ldh;
  OpsLogSink* olog = nullptr;
  RGWREST rest;
  std::unique_ptr<rgw::lua::Background> lua_background;
  std::unique_ptr<rgw::auth::ImplicitTenants> implicit_tenant_context;
  std::unique_ptr<rgw::dmclock::SchedulerCtx> sched_ctx;
  std::unique_ptr<ActiveRateLimiter> ratelimiter;
  std::map<std::string, std::string> service_map_meta;
  // wow, realm reloader has a lot of parts
  std::unique_ptr<RGWRealmReloader> reloader;
  std::unique_ptr<RGWPeriodPusher> pusher;
  std::unique_ptr<RGWFrontendPauser> fe_pauser;
  std::unique_ptr<RGWRealmWatcher> realm_watcher;
  std::unique_ptr<RGWPauser> rgw_pauser;
  std::unique_ptr<sal::ConfigStore> cfgstore;
  SiteConfig site;
  const DoutPrefixProvider* dpp;
  RGWProcessEnv env;
  void need_context_pool();
  std::optional<ceph::async::io_context_pool> context_pool;
public:
  AppMain(const DoutPrefixProvider* dpp);
  ~AppMain();

  void shutdown(std::function<void(void)> finalize_async_signals
	       = []() { /* nada */});

  sal::ConfigStore* get_config_store() const {
    return cfgstore.get();
  }
  rgw::sal::Driver* get_driver() {
    return env.driver;
  }

  rgw::LDAPHelper* get_ldh() {
    return ldh.get();
  }

  void init_frontends1(bool nfs = false);
  void init_numa();
  int init_storage();
  void init_perfcounters();
  void init_http_clients();
  void cond_init_apis();
  void init_ldap();
  void init_opslog();
  int init_frontends2(RGWLib* rgwlib = nullptr);
  void init_tracepoints();
  void init_notification_endpoints();
  void init_lua();

  bool have_http() {
    return have_http_frontend;
  }

  static OpsLogFile* ops_log_file;
}; /* AppMain */
} // namespace rgw

static inline RGWRESTMgr *set_logging(RGWRESTMgr* mgr)
{
  mgr->set_logging(true);
  return mgr;
}

static inline RGWRESTMgr *rest_filter(rgw::sal::Driver* driver, int dialect, RGWRESTMgr* orig)
{
  RGWSyncModuleInstanceRef sync_module = driver->get_sync_module();
  if (sync_module) {
    return sync_module->get_rest_filter(dialect, orig);
  } else {
    return orig;
  }
}

