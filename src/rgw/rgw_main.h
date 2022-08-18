// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 New Dream Network
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
#include "rgw_common.h"
#include "rgw_rest.h"
#include "rgw_frontend.h"
#include "rgw_period_pusher.h"
#include "rgw_realm_reloader.h"
#include "rgw_ldap.h"
#include "rgw_lua.h"
#include "rgw_lua_background.h"
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
  void resume(rgw::sal::Store* store) override {
    std::for_each(pausers.begin(), pausers.end(), [store](Pauser* p){p->resume(store);});
  }

};

namespace rgw {

  class RGWLib;

  class InitHelper {
    /* several components should be initalized only if librgw is
     * also serving HTTP */
    bool have_http_frontend{false};
    bool nfs{false};

    std::vector<RGWFrontend*>& fes;
    std::vector<RGWFrontendConfig*>& fe_configs;
    std::multimap<string, RGWFrontendConfig*>& fe_map;
    std::unique_ptr<rgw::LDAPHelper>& ldh;
    OpsLogSink*& olog;
    RGWREST& rest;
    std::unique_ptr<rgw::lua::Background>& lua_background;
    std::unique_ptr<rgw::auth::ImplicitTenants>& implicit_tenant_context;
    std::unique_ptr<rgw::dmclock::SchedulerCtx>& sched_ctx;
    std::unique_ptr<ActiveRateLimiter>& ratelimiter;
    // wow, realm reloader has a lot of parts
    std::unique_ptr<RGWRealmReloader>& reloader;
    std::unique_ptr<RGWPeriodPusher>& pusher;
    std::unique_ptr<RGWFrontendPauser>& fe_pauser;
    std::unique_ptr<RGWRealmWatcher>& realm_watcher;
    std::unique_ptr<RGWPauser>& rgw_pauser;
    rgw::sal::Store*& store;
    DoutPrefixProvider* dpp;

  public:
    InitHelper(std::vector<RGWFrontend*>& fes,
	      std::vector<RGWFrontendConfig*>& fe_configs,
	      std::multimap<string, RGWFrontendConfig*>& fe_map,
	      std::unique_ptr<rgw::LDAPHelper>& ldh,
	      OpsLogSink*& olog,
	      RGWREST& rest,
	      std::unique_ptr<rgw::lua::Background>& lua_background,
        std::unique_ptr<rgw::auth::ImplicitTenants>& implicit_tenant_context,
	      std::unique_ptr<rgw::dmclock::SchedulerCtx>& sched_ctx,
	      std::unique_ptr<ActiveRateLimiter>& ratelimiter,
        std::unique_ptr<RGWRealmReloader>& reloader,
        std::unique_ptr<RGWPeriodPusher>& pusher,
        std::unique_ptr<RGWFrontendPauser>& fe_pauser,
        std::unique_ptr<RGWRealmWatcher>& realm_watcher,
        std::unique_ptr<RGWPauser>& rgw_pauser,
	      rgw::sal::Store*& store,
	      DoutPrefixProvider* dpp)
      : fes(fes), fe_configs(fe_configs), fe_map(fe_map), ldh(ldh), olog(olog),
	      rest(rest), lua_background(lua_background),
        implicit_tenant_context(implicit_tenant_context), sched_ctx(sched_ctx),
	      ratelimiter(ratelimiter), reloader(reloader), pusher(pusher),
        fe_pauser(fe_pauser), realm_watcher(realm_watcher), rgw_pauser(rgw_pauser),
        store(store), dpp(dpp)
      {}
    
    void init_frontends1(bool nfs = false);
    void init_storage();
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
  }; /* InitHelper */

} // namespace rgw

static inline RGWRESTMgr *set_logging(RGWRESTMgr* mgr)
{
  mgr->set_logging(true);
  return mgr;
}

static inline RGWRESTMgr *rest_filter(rgw::sal::Store* store, int dialect, RGWRESTMgr* orig)
{
  RGWSyncModuleInstanceRef sync_module = store->get_sync_module();
  if (sync_module) {
    return sync_module->get_rest_filter(dialect, orig);
  } else {
    return orig;
  }
}

