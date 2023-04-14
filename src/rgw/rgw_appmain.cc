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

#include <boost/intrusive/list.hpp>
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/TracepointProvider.h"
#include "common/openssl_opts_handler.h"
#include "common/numa.h"
#include "include/compat.h"
#include "include/str_list.h"
#include "include/stringify.h"
#include "rgw_main.h"
#include "rgw_common.h"
#include "rgw_sal.h"
#include "rgw_sal_config.h"
#include "rgw_period_pusher.h"
#include "rgw_realm_reloader.h"
#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_swift.h"
#include "rgw_rest_admin.h"
#include "rgw_rest_info.h"
#include "rgw_rest_usage.h"
#include "rgw_rest_bucket.h"
#include "rgw_rest_metadata.h"
#include "rgw_rest_log.h"
#include "rgw_rest_config.h"
#include "rgw_rest_realm.h"
#include "rgw_rest_ratelimit.h"
#include "rgw_rest_zero.h"
#include "rgw_swift_auth.h"
#include "rgw_log.h"
#include "rgw_lib.h"
#include "rgw_frontend.h"
#include "rgw_lib_frontend.h"
#include "rgw_tools.h"
#include "rgw_resolve.h"
#include "rgw_process.h"
#include "rgw_frontend.h"
#include "rgw_http_client_curl.h"
#include "rgw_kmip_client.h"
#include "rgw_kmip_client_impl.h"
#include "rgw_perf_counters.h"
#include "rgw_signal.h"
#ifdef WITH_RADOSGW_AMQP_ENDPOINT
#include "rgw_amqp.h"
#endif
#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
#include "rgw_kafka.h"
#endif
#ifdef WITH_ARROW_FLIGHT
#include "rgw_flight_frontend.h"
#endif
#include "rgw_asio_frontend.h"
#include "rgw_dmclock_scheduler_ctx.h"
#include "rgw_lua.h"
#ifdef WITH_RADOSGW_DBSTORE
#include "rgw_sal_dbstore.h"
#endif
#include "rgw_lua_background.h"
#include "services/svc_zone.h"

#ifdef HAVE_SYS_PRCTL_H
#include <sys/prctl.h>
#endif

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace {
  TracepointProvider::Traits rgw_op_tracepoint_traits(
    "librgw_op_tp.so", "rgw_op_tracing");
  TracepointProvider::Traits rgw_rados_tracepoint_traits(
    "librgw_rados_tp.so", "rgw_rados_tracing");
}

OpsLogFile* rgw::AppMain::ops_log_file;

rgw::AppMain::AppMain(const DoutPrefixProvider* dpp) : dpp(dpp)
{
}
rgw::AppMain::~AppMain() = default;

void rgw::AppMain::init_frontends1(bool nfs) 
{
  this->nfs = nfs;
  std::string fe_key = (nfs) ? "rgw_nfs_frontends" : "rgw_frontends";
  std::vector<std::string> frontends;
  std::string rgw_frontends_str = g_conf().get_val<string>(fe_key);
  g_conf().early_expand_meta(rgw_frontends_str, &cerr);
  get_str_vec(rgw_frontends_str, ",", frontends);

  /* default frontends */
  if (nfs) {
    const auto is_rgw_nfs = [](const auto& s){return s == "rgw-nfs";};
    if (std::find_if(frontends.begin(), frontends.end(), is_rgw_nfs) == frontends.end()) {
      frontends.push_back("rgw-nfs");
    }
  } else {
    if (frontends.empty()) {
      frontends.push_back("beast");
    }
  }

  for (auto &f : frontends) {
    if (f.find("beast") != string::npos) {
      have_http_frontend = true;
      if (f.find("port") != string::npos) {
        // check for the most common ws problems
        if ((f.find("port=") == string::npos) ||
            (f.find("port= ") != string::npos)) {
          derr <<
    R"(WARNING: radosgw frontend config found unexpected spacing around 'port'
    (ensure frontend port parameter has the form 'port=80' with no spaces
    before or after '='))"
               << dendl;
        }
      }
    } else {
      if (f.find("civetweb") != string::npos) {
        have_http_frontend = true;
      }
    } /* fe !beast */

    RGWFrontendConfig *config = new RGWFrontendConfig(f);
    int r = config->init();
    if (r < 0) {
      delete config;
      cerr << "ERROR: failed to init config: " << f << std::endl;
      continue;
    }

    fe_configs.push_back(config);
    fe_map.insert(
        pair<string, RGWFrontendConfig *>(config->get_framework(), config));
  } /* for each frontend */

  // maintain existing region root pool for new multisite objects
  if (!g_conf()->rgw_region_root_pool.empty()) {
    const char *root_pool = g_conf()->rgw_region_root_pool.c_str();
    if (g_conf()->rgw_zonegroup_root_pool.empty()) {
      g_conf().set_val_or_die("rgw_zonegroup_root_pool", root_pool);
    }
    if (g_conf()->rgw_period_root_pool.empty()) {
      g_conf().set_val_or_die("rgw_period_root_pool", root_pool);
    }
    if (g_conf()->rgw_realm_root_pool.empty()) {
      g_conf().set_val_or_die("rgw_realm_root_pool", root_pool);
    }
  }

  // for region -> zonegroup conversion (must happen before
  // common_init_finish())
  if (!g_conf()->rgw_region.empty() && g_conf()->rgw_zonegroup.empty()) {
    g_conf().set_val_or_die("rgw_zonegroup", g_conf()->rgw_region.c_str());
  }

  ceph::crypto::init_openssl_engine_once();
} /* init_frontends1 */

void rgw::AppMain::init_numa()
{
  if (nfs) {
    return;
  }

  int numa_node = g_conf().get_val<int64_t>("rgw_numa_node");
  size_t numa_cpu_set_size = 0;
  cpu_set_t numa_cpu_set;

  if (numa_node >= 0) {
    int r = get_numa_node_cpu_set(numa_node, &numa_cpu_set_size, &numa_cpu_set);
    if (r < 0) {
      dout(1) << __func__ << " unable to determine rgw numa node " << numa_node
              << " CPUs" << dendl;
      numa_node = -1;
    } else {
      r = set_cpu_affinity_all_threads(numa_cpu_set_size, &numa_cpu_set);
      if (r < 0) {
        derr << __func__ << " failed to set numa affinity: " << cpp_strerror(r)
        << dendl;
      }
    }
  } else {
    dout(1) << __func__ << " not setting numa affinity" << dendl;
  }
} /* init_numa */

int rgw::AppMain::init_storage()
{
  auto config_store_type = g_conf().get_val<std::string>("rgw_config_store");
  cfgstore = DriverManager::create_config_store(dpp, config_store_type);
  if (!cfgstore) {
    return -EIO;
  }
  env.cfgstore = cfgstore.get();

  int r = site.load(dpp, null_yield, cfgstore.get());
  if (r < 0) {
    return r;
  }
  env.site = &site;

  auto run_gc =
    (g_conf()->rgw_enable_gc_threads &&
      ((!nfs) || (nfs && g_conf()->rgw_nfs_run_gc_threads)));

  auto run_lc =
    (g_conf()->rgw_enable_lc_threads &&
      ((!nfs) || (nfs && g_conf()->rgw_nfs_run_lc_threads)));

  auto run_quota =
    (g_conf()->rgw_enable_quota_threads &&
      ((!nfs) || (nfs && g_conf()->rgw_nfs_run_quota_threads)));

  auto run_sync =
    (g_conf()->rgw_run_sync_thread &&
      ((!nfs) || (nfs && g_conf()->rgw_nfs_run_sync_thread)));

  DriverManager::Config cfg = DriverManager::get_config(false, g_ceph_context);
  env.driver = DriverManager::get_storage(dpp, dpp->get_cct(),
          cfg,
	  context_pool,
          run_gc,
          run_lc,
          run_quota,
          run_sync,
          g_conf().get_val<bool>("rgw_dynamic_resharding"),
          true, null_yield, // run notification thread
          g_conf()->rgw_cache_enabled);
  if (!env.driver) {
    return -EIO;
  }
  return 0;
} /* init_storage */

void rgw::AppMain::init_perfcounters()
{
  (void) rgw_perf_start(dpp->get_cct());
} /* init_perfcounters */

void rgw::AppMain::init_http_clients()
{
  rgw_init_resolver();
  rgw::curl::setup_curl(fe_map);
  rgw_http_client_init(dpp->get_cct());
  rgw_kmip_client_init(*new RGWKMIPManagerImpl(dpp->get_cct()));
} /* init_http_clients */

void rgw::AppMain::cond_init_apis() 
{
   rgw_rest_init(g_ceph_context, env.driver->get_zone()->get_zonegroup());

  if (have_http_frontend) {
    std::vector<std::string> apis;
    get_str_vec(g_conf()->rgw_enable_apis, apis);

    std::map<std::string, bool> apis_map;
    for (auto &api : apis) {
      apis_map[api] = true;
    }

    /* warn about insecure keystone secret config options */
    if (!(g_ceph_context->_conf->rgw_keystone_admin_token.empty() ||
          g_ceph_context->_conf->rgw_keystone_admin_password.empty())) {
      dout(0)
          << "WARNING: rgw_keystone_admin_token and "
             "rgw_keystone_admin_password should be avoided as they can "
             "expose secrets.  Prefer the new rgw_keystone_admin_token_path "
             "and rgw_keystone_admin_password_path options, which read their "
             "secrets from files."
          << dendl;
    }

    // S3 website mode is a specialization of S3
    const bool s3website_enabled = apis_map.count("s3website") > 0;
    const bool sts_enabled = apis_map.count("sts") > 0;
    const bool iam_enabled = apis_map.count("iam") > 0;
    const bool pubsub_enabled =
        apis_map.count("pubsub") > 0 || apis_map.count("notifications") > 0;
    // Swift API entrypoint could placed in the root instead of S3
    const bool swift_at_root = g_conf()->rgw_swift_url_prefix == "/";
    if (apis_map.count("s3") > 0 || s3website_enabled) {
      if (!swift_at_root) {
        rest.register_default_mgr(set_logging(
            rest_filter(env.driver, RGW_REST_S3,
                        new RGWRESTMgr_S3(s3website_enabled, sts_enabled,
                                          iam_enabled, pubsub_enabled))));
      } else {
        derr << "Cannot have the S3 or S3 Website enabled together with "
             << "Swift API placed in the root of hierarchy" << dendl;
      }
    }

    if (apis_map.count("swift") > 0) {
      RGWRESTMgr_SWIFT* const swift_resource = new RGWRESTMgr_SWIFT;

      if (! g_conf()->rgw_cross_domain_policy.empty()) {
        swift_resource->register_resource("crossdomain.xml",
                            set_logging(new RGWRESTMgr_SWIFT_CrossDomain));
      }

      swift_resource->register_resource("healthcheck",
                            set_logging(new RGWRESTMgr_SWIFT_HealthCheck));

      swift_resource->register_resource("info",
                            set_logging(new RGWRESTMgr_SWIFT_Info));

      if (! swift_at_root) {
        rest.register_resource(g_conf()->rgw_swift_url_prefix,
                            set_logging(rest_filter(env.driver, RGW_REST_SWIFT,
                                                    swift_resource)));
      } else {
        if (env.driver->get_zone()->get_zonegroup().get_zone_count() > 1) {
          derr << "Placing Swift API in the root of URL hierarchy while running"
              << " multi-site configuration requires another instance of RadosGW"
              << " with S3 API enabled!" << dendl;
        }

        rest.register_default_mgr(set_logging(swift_resource));
      }
    }

    if (apis_map.count("swift_auth") > 0) {
      rest.register_resource(g_conf()->rgw_swift_auth_entry,
                set_logging(new RGWRESTMgr_SWIFT_Auth));
    }

    if (apis_map.count("admin") > 0) {
      RGWRESTMgr_Admin *admin_resource = new RGWRESTMgr_Admin;
      admin_resource->register_resource("info", new RGWRESTMgr_Info);
      admin_resource->register_resource("usage", new RGWRESTMgr_Usage);
      /* Register driver-specific admin APIs */
      env.driver->register_admin_apis(admin_resource);
      rest.register_resource(g_conf()->rgw_admin_entry, admin_resource);
    }

    if (apis_map.count("zero")) {
      rest.register_resource("zero", new rgw::RESTMgr_Zero());
    }
  } /* have_http_frontend */
} /* init_apis */

void rgw::AppMain::init_ldap()
{
  CephContext* cct = env.driver->ctx();
  const string &ldap_uri = cct->_conf->rgw_ldap_uri;
  const string &ldap_binddn = cct->_conf->rgw_ldap_binddn;
  const string &ldap_searchdn = cct->_conf->rgw_ldap_searchdn;
  const string &ldap_searchfilter = cct->_conf->rgw_ldap_searchfilter;
  const string &ldap_dnattr = cct->_conf->rgw_ldap_dnattr;
  std::string ldap_bindpw = parse_rgw_ldap_bindpw(cct);

  ldh.reset(new rgw::LDAPHelper(ldap_uri, ldap_binddn,
            ldap_bindpw.c_str(), ldap_searchdn, ldap_searchfilter, ldap_dnattr));
  ldh->init();
  ldh->bind();
} /* init_ldap */

void rgw::AppMain::init_opslog()
{
  rgw_log_usage_init(dpp->get_cct(), env.driver);

  OpsLogManifold *olog_manifold = new OpsLogManifold();
  if (!g_conf()->rgw_ops_log_socket_path.empty()) {
    OpsLogSocket *olog_socket =
        new OpsLogSocket(g_ceph_context, g_conf()->rgw_ops_log_data_backlog);
    olog_socket->init(g_conf()->rgw_ops_log_socket_path);
    olog_manifold->add_sink(olog_socket);
  }
  if (!g_conf()->rgw_ops_log_file_path.empty()) {
    ops_log_file =
        new OpsLogFile(g_ceph_context, g_conf()->rgw_ops_log_file_path,
                       g_conf()->rgw_ops_log_data_backlog);
    ops_log_file->start();
    olog_manifold->add_sink(ops_log_file);
  }
  olog_manifold->add_sink(new OpsLogRados(env.driver));
  olog = olog_manifold;
} /* init_opslog */

int rgw::AppMain::init_frontends2(RGWLib* rgwlib)
{
  int r{0};
  vector<string> frontends_def;
  std::string frontend_defs_str =
    g_conf().get_val<string>("rgw_frontend_defaults");
  get_str_vec(frontend_defs_str, ",", frontends_def);

  service_map_meta["pid"] = stringify(getpid());

  std::map<std::string, std::unique_ptr<RGWFrontendConfig> > fe_def_map;
  for (auto& f : frontends_def) {
    RGWFrontendConfig *config = new RGWFrontendConfig(f);
    int r = config->init();
    if (r < 0) {
      delete config;
      cerr << "ERROR: failed to init default config: " << f << std::endl;
      continue;
    }
    fe_def_map[config->get_framework()].reset(config);
  }

  /* Initialize the registry of auth strategies which will coordinate
   * the dynamic reconfiguration. */
  implicit_tenant_context.reset(new rgw::auth::ImplicitTenants{g_conf()});
  g_conf().add_observer(implicit_tenant_context.get());

  /* allocate a mime table (you'd never guess that from the name) */
  rgw_tools_init(dpp, dpp->get_cct());

  /* Header custom behavior */
  rest.register_x_headers(g_conf()->rgw_log_http_headers);

  sched_ctx.reset(new rgw::dmclock::SchedulerCtx{dpp->get_cct()});
  ratelimiter.reset(new ActiveRateLimiter{dpp->get_cct()});
  ratelimiter->start();

  // initialize RGWProcessEnv
  env.rest = &rest;
  env.olog = olog;
  env.auth_registry = rgw::auth::StrategyRegistry::create(
      dpp->get_cct(), *implicit_tenant_context, env.driver);
  env.ratelimiting = ratelimiter.get();

  int fe_count = 0;
  for (multimap<string, RGWFrontendConfig *>::iterator fiter = fe_map.begin();
       fiter != fe_map.end(); ++fiter, ++fe_count) {
    RGWFrontendConfig *config = fiter->second;
    string framework = config->get_framework();

    auto def_iter = fe_def_map.find(framework);
    if (def_iter != fe_def_map.end()) {
      config->set_default_config(*def_iter->second);
    }

    RGWFrontend* fe = nullptr;

    if (framework == "loadgen") {
      fe = new RGWLoadGenFrontend(env, config);
    }
    else if (framework == "beast") {
      fe = new RGWAsioFrontend(env, config, *sched_ctx);
    }
    else if (framework == "rgw-nfs") {
      fe = new RGWLibFrontend(env, config);
      if (rgwlib) {
        rgwlib->set_fe(static_cast<RGWLibFrontend*>(fe));
      }
    }
    else if (framework == "arrow_flight") {
#ifdef WITH_ARROW_FLIGHT
      int port;
      config->get_val("port", 8077, &port);
      fe = new rgw::flight::FlightFrontend(env, config, port);
#else
      derr << "WARNING: arrow_flight frontend requested, but not included in build; skipping" << dendl;
      continue;
#endif
    }

    service_map_meta["frontend_type#" + stringify(fe_count)] = framework;
    service_map_meta["frontend_config#" + stringify(fe_count)] = config->get_config();

    if (! fe) {
      dout(0) << "WARNING: skipping unknown framework: " << framework << dendl;
      continue;
    }

    dout(0) << "starting handler: " << fiter->first << dendl;
    int r = fe->init();
    if (r < 0) {
      derr << "ERROR: failed initializing frontend" << dendl;
      return -r;
    }
    r = fe->run();
    if (r < 0) {
      derr << "ERROR: failed run" << dendl;
      return -r;
    }

    fes.push_back(fe);
  }

  std::string daemon_type = (nfs) ? "rgw-nfs" : "rgw";
  r = env.driver->register_to_service_map(dpp, daemon_type, service_map_meta);
  if (r < 0) {
    derr << "ERROR: failed to register to service map: " << cpp_strerror(-r) << dendl;
    /* ignore error */
  }

  if (env.driver->get_name() == "rados") {
    // add a watcher to respond to realm configuration changes
    pusher = std::make_unique<RGWPeriodPusher>(dpp, env.driver, null_yield);
    fe_pauser = std::make_unique<RGWFrontendPauser>(fes, pusher.get());
    rgw_pauser = std::make_unique<RGWPauser>();
    rgw_pauser->add_pauser(fe_pauser.get());
    if (env.lua.background) {
      rgw_pauser->add_pauser(env.lua.background);
    }
    reloader = std::make_unique<RGWRealmReloader>(
      env, *implicit_tenant_context, service_map_meta, rgw_pauser.get(), context_pool);
    realm_watcher = std::make_unique<RGWRealmWatcher>(dpp, g_ceph_context,
				  static_cast<rgw::sal::RadosStore*>(env.driver)->svc()->zone->get_realm());
    realm_watcher->add_watcher(RGWRealmNotify::Reload, *reloader);
    realm_watcher->add_watcher(RGWRealmNotify::ZonesNeedPeriod, *pusher.get());
  }

  return r;
} /* init_frontends2 */

void rgw::AppMain::init_tracepoints()
{
  TracepointProvider::initialize<rgw_rados_tracepoint_traits>(dpp->get_cct());
  TracepointProvider::initialize<rgw_op_tracepoint_traits>(dpp->get_cct());
  tracing::rgw::tracer.init(dpp->get_cct(), "rgw");
} /* init_tracepoints() */

void rgw::AppMain::init_notification_endpoints()
{
#ifdef WITH_RADOSGW_AMQP_ENDPOINT
  if (!rgw::amqp::init(dpp->get_cct())) {
    derr << "ERROR: failed to initialize AMQP manager" << dendl;
  }
#endif
#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
  if (!rgw::kafka::init(dpp->get_cct())) {
    derr << "ERROR: failed to initialize Kafka manager" << dendl;
  }
#endif
} /* init_notification_endpoints */

void rgw::AppMain::init_lua()
{
  rgw::sal::Driver* driver = env.driver;
  int r{0};
  std::string install_dir;

#ifdef WITH_RADOSGW_LUA_PACKAGES
  rgw::lua::packages_t failed_packages;
  r = rgw::lua::install_packages(dpp, driver, null_yield, g_conf().get_val<std::string>("rgw_luarocks_location"),
                                 failed_packages, install_dir);
  if (r < 0) {
    ldpp_dout(dpp, 5) << "WARNING: failed to install Lua packages from allowlist. error: " << r
            << dendl;
  }
  for (const auto &p : failed_packages) {
    ldpp_dout(dpp, 5) << "WARNING: failed to install Lua package: " << p
            << " from allowlist" << dendl;
  }
#endif

  env.lua.manager = env.driver->get_lua_manager(install_dir);
  if (driver->get_name() == "rados") { /* Supported for only RadosStore */
    lua_background = std::make_unique<
      rgw::lua::Background>(driver, dpp->get_cct(), env.lua.manager.get());
    lua_background->start();
    env.lua.background = lua_background.get();
    static_cast<rgw::sal::RadosLuaManager*>(env.lua.manager.get())->watch_reload(dpp);
  }
} /* init_lua */

void rgw::AppMain::shutdown(std::function<void(void)> finalize_async_signals)
{
  if (env.driver->get_name() == "rados") {
    reloader.reset(); // stop the realm reloader
    static_cast<rgw::sal::RadosLuaManager*>(env.lua.manager.get())->unwatch_reload(dpp);
  }

  for (auto& fe : fes) {
    fe->stop();
  }

  for (auto& fe : fes) {
    fe->join();
    delete fe;
  }

  for (auto& fec : fe_configs) {
    delete fec;
  }

  ldh.reset(nullptr); // deletes
  finalize_async_signals(); // callback
  rgw_log_usage_finalize();
  
  delete olog;

  if (lua_background) {
    lua_background->shutdown();
  }

  cfgstore.reset(); // deletes
  DriverManager::close_storage(env.driver);

  rgw_tools_cleanup();
  rgw_shutdown_resolver();
  rgw_http_client_cleanup();
  rgw_kmip_client_cleanup();
  rgw::curl::cleanup_curl();
  g_conf().remove_observer(implicit_tenant_context.get());
  implicit_tenant_context.reset(); // deletes
#ifdef WITH_RADOSGW_AMQP_ENDPOINT
  rgw::amqp::shutdown();
#endif
#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
  rgw::kafka::shutdown();
#endif
  rgw_perf_stop(g_ceph_context);
  ratelimiter.reset(); // deletes--ensure this happens before we destruct
} /* AppMain::shutdown */
