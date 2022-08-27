// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <boost/intrusive/list.hpp>
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/safe_io.h"
#include "common/TracepointProvider.h"
#include "common/openssl_opts_handler.h"
#include "common/numa.h"
#include "include/compat.h"
#include "include/str_list.h"
#include "include/stringify.h"
#include "rgw_common.h"
#include "rgw_sal_rados.h"
#include "rgw_period_pusher.h"
#include "rgw_realm_reloader.h"
#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_swift.h"
#include "rgw_rest_admin.h"
#include "rgw_rest_info.h"
#include "rgw_rest_usage.h"
#include "rgw_rest_user.h"
#include "rgw_rest_sts.h"
#include "rgw_swift_auth.h"
#include "rgw_log.h"
#include "rgw_lib.h"
#include "rgw_frontend.h"
#include "rgw_lib_frontend.h"
#include "rgw_tools.h"
#include "rgw_resolve.h"
#include "rgw_request.h"
#include "rgw_process.h"
#include "rgw_frontend.h"
#include "rgw_http_client_curl.h"
#include "rgw_kmip_client.h"
#include "rgw_kmip_client_impl.h"
#include "rgw_perf_counters.h"
#ifdef WITH_RADOSGW_AMQP_ENDPOINT
#include "rgw_amqp.h"
#endif
#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
#include "rgw_kafka.h"
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

using namespace std;

static constexpr auto dout_subsys = ceph_subsys_rgw;

namespace {
  TracepointProvider::Traits rgw_op_tracepoint_traits(
    "librgw_op_tp.so", "rgw_op_tracing");
  TracepointProvider::Traits rgw_rados_tracepoint_traits(
    "librgw_rados_tp.so", "rgw_rados_tracing");
}

static sig_t sighandler_alrm;

class RGWProcess;

static int signal_fd[2] = {0, 0};

void signal_shutdown()
{
  int val = 0;
  int ret = write(signal_fd[0], (char *)&val, sizeof(val));
  if (ret < 0) {
    derr << "ERROR: " << __func__ << ": write() returned "
         << cpp_strerror(errno) << dendl;
  }
}

static void wait_shutdown()
{
  int val;
  int r = safe_read_exact(signal_fd[1], &val, sizeof(val));
  if (r < 0) {
    derr << "safe_read_exact returned with error" << dendl;
  }
}

static int signal_fd_init()
{
  return socketpair(AF_UNIX, SOCK_STREAM, 0, signal_fd);
}

static void signal_fd_finalize()
{
  close(signal_fd[0]);
  close(signal_fd[1]);
}

static void handle_sigterm(int signum)
{
  dout(1) << __func__ << dendl;

  // send a signal to make fcgi's accept(2) wake up.  unfortunately the
  // initial signal often isn't sufficient because we race with accept's
  // check of the flag wet by ShutdownPending() above.
  if (signum != SIGUSR1) {
    signal_shutdown();

    // safety net in case we get stuck doing an orderly shutdown.
    uint64_t secs = g_ceph_context->_conf->rgw_exit_timeout_secs;
    if (secs)
      alarm(secs);
    dout(1) << __func__ << " set alarm for " << secs << dendl;
  }

}

static OpsLogFile* ops_log_file = nullptr;

static void rgw_sighup_handler(int signum) {
    if (ops_log_file != nullptr) {
        ops_log_file->reopen();
    }
    sighup_handler(signum);
}

static void godown_alarm(int signum)
{
  _exit(0);
}

class C_InitTimeout : public Context {
public:
  C_InitTimeout() {}
  void finish(int r) override {
    derr << "Initialization timeout, failed to initialize" << dendl;
    exit(1);
  }
};

static int usage()
{
  cout << "usage: radosgw [options...]" << std::endl;
  cout << "options:\n";
  cout << "  --rgw-region=<region>     region in which radosgw runs\n";
  cout << "  --rgw-zone=<zone>         zone in which radosgw runs\n";
  cout << "  --rgw-socket-path=<path>  specify a unix domain socket path\n";
  cout << "  -m monaddress[:port]      connect to specified monitor\n";
  cout << "  --keyring=<path>          path to radosgw keyring\n";
  cout << "  --logfile=<logfile>       file to log debug output\n";
  cout << "  --debug-rgw=<log-level>/<memory-level>  set radosgw debug level\n";
  generic_server_usage();

  return 0;
}

void rgw::AppMain::init_frontends1(bool nfs) 
{
  this->nfs = nfs;
  std::string fe_key = (nfs) ? "rgw_nfs_frontends" : "rgw_frontends";
  std::vector<std::string> frontends;
  std::string rgw_frontends_str = g_conf().get_val<string>(fe_key);
  g_conf().early_expand_meta(rgw_frontends_str, &cerr);
  get_str_vec(rgw_frontends_str, ",", frontends);

  if (!nfs) {
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

void rgw::AppMain::init_storage()
{
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

  StoreManager::Config cfg = StoreManager::get_config(false, g_ceph_context);
  store = StoreManager::get_storage(dpp, dpp->get_cct(),
          cfg,
          run_gc,
          run_lc,
          run_quota,
          run_sync,
          g_conf().get_val<bool>("rgw_dynamic_resharding"),
          g_conf()->rgw_cache_enabled);

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
   rgw_rest_init(g_ceph_context, store->get_zone()->get_zonegroup());

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
            rest_filter(store, RGW_REST_S3,
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
                            set_logging(rest_filter(store, RGW_REST_SWIFT,
                                                    swift_resource)));
      } else {
        if (store->get_zone()->get_zonegroup().get_zone_count() > 1) {
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
      admin_resource->register_resource("user", new RGWRESTMgr_User);

      /* Register store-specific admin APIs */
      store->register_admin_apis(admin_resource);
      rest.register_resource(g_conf()->rgw_admin_entry, admin_resource);
    }
  } /* have_http_frontend */
} /* init_apis */

void rgw::AppMain::init_ldap()
{
  const string &ldap_uri = store->ctx()->_conf->rgw_ldap_uri;
  const string &ldap_binddn = store->ctx()->_conf->rgw_ldap_binddn;
  const string &ldap_searchdn = store->ctx()->_conf->rgw_ldap_searchdn;
  const string &ldap_searchfilter = store->ctx()->_conf->rgw_ldap_searchfilter;
  const string &ldap_dnattr = store->ctx()->_conf->rgw_ldap_dnattr;
  std::string ldap_bindpw = parse_rgw_ldap_bindpw(store->ctx());

  ldh.reset(new rgw::LDAPHelper(ldap_uri, ldap_binddn,
            ldap_bindpw.c_str(), ldap_searchdn, ldap_searchfilter, ldap_dnattr));
  ldh->init();
  ldh->bind();
} /* init_ldap */

void rgw::AppMain::init_opslog()
{
  rgw_log_usage_init(dpp->get_cct(), store);

  OpsLogManifold *olog_manifold = new OpsLogManifold();
  if (!g_conf()->rgw_ops_log_socket_path.empty()) {
    OpsLogSocket *olog_socket =
        new OpsLogSocket(g_ceph_context, g_conf()->rgw_ops_log_data_backlog);
    olog_socket->init(g_conf()->rgw_ops_log_socket_path);
    olog_manifold->add_sink(olog_socket);
  }
  OpsLogFile *ops_log_file;
  if (!g_conf()->rgw_ops_log_file_path.empty()) {
    ops_log_file =
        new OpsLogFile(g_ceph_context, g_conf()->rgw_ops_log_file_path,
                       g_conf()->rgw_ops_log_data_backlog);
    ops_log_file->start();
    olog_manifold->add_sink(ops_log_file);
  }
  olog_manifold->add_sink(new OpsLogRados(store));
  olog = olog_manifold;
} /* init_opslog */

int rgw::AppMain::init_frontends2(RGWLib* rgwlib)
{
  int r{0};
  vector<string> frontends_def;
  std::string frontend_defs_str =
    g_conf().get_val<string>("rgw_frontend_defaults");
  get_str_vec(frontend_defs_str, ",", frontends_def);

  std::map<std::string, std::string> service_map_meta;
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
  auto auth_registry =
    rgw::auth::StrategyRegistry::create(dpp->get_cct(), *(implicit_tenant_context.get()), store);

  /* allocate a mime table (you'd never guess that from the name) */
  rgw_tools_init(dpp, dpp->get_cct());

  /* Header custom behavior */
  rest.register_x_headers(g_conf()->rgw_log_http_headers);

  sched_ctx.reset(new rgw::dmclock::SchedulerCtx{dpp->get_cct()});
  ratelimiter.reset(new ActiveRateLimiter{dpp->get_cct()});
  ratelimiter->start();

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
      int port;
      config->get_val("port", 80, &port);
      std::string uri_prefix;
      config->get_val("prefix", "", &uri_prefix);

      RGWProcessEnv env = {store, &rest, olog, port, uri_prefix,
	    auth_registry, ratelimiter.get(), lua_background.get()};

      fe = new RGWLoadGenFrontend(env, config);
    }
    else if (framework == "beast") {
      int port;
      config->get_val("port", 80, &port);
      std::string uri_prefix;
      config->get_val("prefix", "", &uri_prefix);
      RGWProcessEnv env{store, &rest, olog, port, uri_prefix,
	    auth_registry, ratelimiter.get(), lua_background.get()};
      fe = new RGWAsioFrontend(env, config, *(sched_ctx.get()));
    }
    else if (framework == "rgw-nfs") {
      int port = 80;
      RGWProcessEnv env = { store, &rest, olog, port };
      fe = new RGWLibFrontend(env, config);
      if (rgwlib) {
        rgwlib->set_fe(static_cast<RGWLibFrontend*>(fe));
      }
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

  std::string daemon_type = (nfs) ? "rgw-nfs" : "nfs";
  r = store->register_to_service_map(dpp, daemon_type, service_map_meta);
  if (r < 0) {
    derr << "ERROR: failed to register to service map: " << cpp_strerror(-r) << dendl;
    /* ignore error */
  }

  if (store->get_name() == "rados") {
    // add a watcher to respond to realm configuration changes
    pusher = std::make_unique<RGWPeriodPusher>(dpp, store, null_yield);
    fe_pauser = std::make_unique<RGWFrontendPauser>(fes, *(implicit_tenant_context.get()), pusher.get());
    rgw_pauser = std::make_unique<RGWPauser>();
    rgw_pauser->add_pauser(fe_pauser.get());
    if (lua_background) {
      rgw_pauser->add_pauser(lua_background.get());
    }
    reloader = std::make_unique<RGWRealmReloader>(store, service_map_meta, rgw_pauser.get());
    realm_watcher = std::make_unique<RGWRealmWatcher>(dpp, g_ceph_context,
				  static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->get_realm());
    realm_watcher->add_watcher(RGWRealmNotify::Reload, *reloader);
    realm_watcher->add_watcher(RGWRealmNotify::ZonesNeedPeriod, *pusher.get());
  }

  return r;
} /* init_frontends2 */

void rgw::AppMain::init_tracepoints()
{
  TracepointProvider::initialize<rgw_rados_tracepoint_traits>(dpp->get_cct());
  TracepointProvider::initialize<rgw_op_tracepoint_traits>(dpp->get_cct());
  tracing::rgw::tracer.init("rgw");
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
  int r{0};
  const auto &luarocks_path =
      g_conf().get_val<std::string>("rgw_luarocks_location");
  if (luarocks_path.empty()) {
    store->set_luarocks_path("");
  } else {
    store->set_luarocks_path(luarocks_path + "/" + g_conf()->name.to_str());
  }
#ifdef WITH_RADOSGW_LUA_PACKAGES
  rgw::lua::packages_t failed_packages;
  std::string output;
  r = rgw::lua::install_packages(dpp, store, null_yield, failed_packages,
                                 output);
  if (r < 0) {
    dout(1) << "WARNING: failed to install lua packages from allowlist"
            << dendl;
  }
  if (!output.empty()) {
    dout(10) << "INFO: lua packages installation output: \n" << output << dendl;
  }
  for (const auto &p : failed_packages) {
    dout(5) << "WARNING: failed to install lua package: " << p
            << " from allowlist" << dendl;
  }
#endif

  if (store->get_name() == "rados") { /* Supported for only RadosStore */
    lua_background = std::make_unique<
      rgw::lua::Background>(store, dpp->get_cct(), store->get_luarocks_path());
    lua_background->start();
  }
} /* init_lua */

void rgw::AppMain::shutdown()
{
  if (store->get_name() == "rados") {
    reloader.reset(); // stop the realm reloader
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

  unregister_async_signal_handler(SIGUSR1, handle_sigterm);
  shutdown_async_signal_handler();

  rgw_log_usage_finalize();
  
  delete olog;

  if (lua_background) {
    lua_background->shutdown();
  }

  StoreManager::close_storage(store);

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
} /* AppMain::shutdown */

/*
 * start up the RADOS connection and then handle HTTP messages as they come in
 */
int main (int argc, char *argv[])
{ 
  int r{0};

  // dout() messages will be sent to stderr, but FCGX wants messages on stdout
  // Redirect stderr to stdout.
  TEMP_FAILURE_RETRY(close(STDERR_FILENO));
  if (TEMP_FAILURE_RETRY(dup2(STDOUT_FILENO, STDERR_FILENO)) < 0) {
    int err = errno;
    cout << "failed to redirect stderr to stdout: " << cpp_strerror(err)
         << std::endl;
    return ENOSYS;
  }

  /* alternative default for module */
  map<string,string> defaults = {
    { "debug_rgw", "1/5" },
    { "keyring", "$rgw_data/keyring" },
    { "objecter_inflight_ops", "24576" },
    // require a secure mon connection by default
    { "ms_mon_client_mode", "secure" },
    { "auth_client_required", "cephx" }
  };

  auto args = argv_to_vec(argc, argv);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  int flags = CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS;
  // Prevent global_init() from dropping permissions until frontends can bind
  // privileged ports
  flags |= CINIT_FLAG_DEFER_DROP_PRIVILEGES;

  auto cct = rgw_global_init(&defaults, args, CEPH_ENTITY_TYPE_CLIENT,
			     CODE_ENVIRONMENT_DAEMON, flags);

  DoutPrefix dp(cct.get(), dout_subsys, "rgw main: ");
  rgw::AppMain main(&dp);

  main.init_frontends1(false /* nfs */);
  main.init_numa();

  if (g_conf()->daemonize) {
    global_init_daemonize(g_ceph_context);
  }
  ceph::mutex mutex = ceph::make_mutex("main");
  SafeTimer init_timer(g_ceph_context, mutex);
  init_timer.init();
  mutex.lock();
  init_timer.add_event_after(g_conf()->rgw_init_timeout, new C_InitTimeout);
  mutex.unlock();

  common_init_finish(g_ceph_context);
  init_async_signal_handler();

  /* XXXX check locations thru sighandler_alrm */
  register_async_signal_handler(SIGHUP, rgw_sighup_handler);
  r = signal_fd_init();
  if (r < 0) {
    derr << "ERROR: unable to initialize signal fds" << dendl;
  exit(1);
  }

  register_async_signal_handler(SIGTERM, handle_sigterm);
  register_async_signal_handler(SIGINT, handle_sigterm);
  register_async_signal_handler(SIGUSR1, handle_sigterm);
  sighandler_alrm = signal(SIGALRM, godown_alarm);


  main.init_storage();
  if (! main.get_store()) {
    mutex.lock();
    init_timer.cancel_all_events();
    init_timer.shutdown();
    mutex.unlock();

    derr << "Couldn't init storage provider (RADOS)" << dendl;
    return EIO;
  }

  main.init_perfcounters();
  main.init_http_clients();
  main.cond_init_apis();

  mutex.lock();
  init_timer.cancel_all_events();
  init_timer.shutdown();
  mutex.unlock();

  main.init_ldap();
  main.init_opslog();
  main.init_tracepoints();
  main.init_frontends2(nullptr /* RGWLib */);
  main.init_notification_endpoints();
  main.init_lua();

#if defined(HAVE_SYS_PRCTL_H)
  if (prctl(PR_SET_DUMPABLE, 1) == -1) {
    cerr << "warning: unable to set dumpable flag: " << cpp_strerror(errno) << std::endl;
  }
#endif

  wait_shutdown();

  derr << "shutting down" << dendl;
  main.shutdown();

  unregister_async_signal_handler(SIGHUP, rgw_sighup_handler);
  unregister_async_signal_handler(SIGTERM, handle_sigterm);
  unregister_async_signal_handler(SIGINT, handle_sigterm);
  unregister_async_signal_handler(SIGUSR1, handle_sigterm);
  shutdown_async_signal_handler();

  dout(1) << "final shutdown" << dendl;

  signal_fd_finalize();

  return 0;
}
