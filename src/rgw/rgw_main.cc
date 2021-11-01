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
#include "rgw_sal.h"
#include "rgw_period_pusher.h"
#include "rgw_realm_reloader.h"
#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_swift.h"
#include "rgw_rest_admin.h"
#include "rgw_rest_info.h"
#include "rgw_rest_usage.h"
#include "rgw_rest_user.h"
#include "rgw_rest_bucket.h"
#include "rgw_rest_metadata.h"
#include "rgw_rest_log.h"
#include "rgw_rest_config.h"
#include "rgw_rest_realm.h"
#include "rgw_rest_sts.h"
#include "rgw_swift_auth.h"
#include "rgw_log.h"
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
#ifdef WITH_RADOSGW_LUA_PACKAGES
#include "rgw_lua.h"
#endif
#ifdef WITH_RADOSGW_DBSTORE
#include "rgw_sal_dbstore.h"
#endif

#include "services/svc_zone.h"

#ifdef HAVE_SYS_PRCTL_H
#include <sys/prctl.h>
#endif

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace {
TracepointProvider::Traits rgw_op_tracepoint_traits("librgw_op_tp.so",
                                                 "rgw_op_tracing");
TracepointProvider::Traits rgw_rados_tracepoint_traits("librgw_rados_tp.so",
                                                 "rgw_rados_tracing");
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

static RGWRESTMgr *set_logging(RGWRESTMgr *mgr)
{
  mgr->set_logging(true);
  return mgr;
}

static RGWRESTMgr *rest_filter(rgw::sal::Store* store, int dialect, RGWRESTMgr *orig)
{
  RGWSyncModuleInstanceRef sync_module = store->get_sync_module();
  if (sync_module) {
    return sync_module->get_rest_filter(dialect, orig);
  } else {
    return orig;
  }
}

/*
 * start up the RADOS connection and then handle HTTP messages as they come in
 */
int radosgw_Main(int argc, const char **argv)
{
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

  auto cct = global_init(&defaults, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_DAEMON, flags);

  // First, let's determine which frontends are configured.
  list<string> frontends;
  string rgw_frontends_str = g_conf().get_val<string>("rgw_frontends");
  g_conf().early_expand_meta(rgw_frontends_str, &cerr);
  get_str_list(rgw_frontends_str, ",", frontends);
  multimap<string, RGWFrontendConfig *> fe_map;
  list<RGWFrontendConfig *> configs;
  if (frontends.empty()) {
    frontends.push_back("beast");
  }
  for (list<string>::iterator iter = frontends.begin(); iter != frontends.end(); ++iter) {
    string& f = *iter;

    if (f.find("beast") != string::npos) {
      if (f.find("port") != string::npos) {
        // check for the most common ws problems
        if ((f.find("port=") == string::npos) ||
            (f.find("port= ") != string::npos)) {
          derr << "WARNING: radosgw frontend config found unexpected spacing around 'port' "
               << "(ensure frontend port parameter has the form 'port=80' with no spaces "
               << "before or after '=')" << dendl;
        }
      }
    }

    RGWFrontendConfig *config = new RGWFrontendConfig(f);
    int r = config->init();
    if (r < 0) {
      delete config;
      cerr << "ERROR: failed to init config: " << f << std::endl;
      return EINVAL;
    }

    configs.push_back(config);

    string framework = config->get_framework();
    fe_map.insert(pair<string, RGWFrontendConfig*>(framework, config));
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

  // for region -> zonegroup conversion (must happen before common_init_finish())
  if (!g_conf()->rgw_region.empty() && g_conf()->rgw_zonegroup.empty()) {
    g_conf().set_val_or_die("rgw_zonegroup", g_conf()->rgw_region.c_str());
  }

  if (g_conf()->daemonize) {
    global_init_daemonize(g_ceph_context);
  }
  ceph::mutex mutex = ceph::make_mutex("main");
  SafeTimer init_timer(g_ceph_context, mutex);
  init_timer.init();
  mutex.lock();
  init_timer.add_event_after(g_conf()->rgw_init_timeout, new C_InitTimeout);
  mutex.unlock();

  ceph::crypto::init_openssl_engine_once();

  common_init_finish(g_ceph_context);

  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);

  TracepointProvider::initialize<rgw_rados_tracepoint_traits>(g_ceph_context);
  TracepointProvider::initialize<rgw_op_tracepoint_traits>(g_ceph_context);

  const DoutPrefix dp(cct.get(), dout_subsys, "rgw main: ");
  int r = rgw_tools_init(&dp, g_ceph_context);
  if (r < 0) {
    derr << "ERROR: unable to initialize rgw tools" << dendl;
    return -r;
  }

  rgw_init_resolver();
  rgw::curl::setup_curl(fe_map);
  rgw_http_client_init(g_ceph_context);
  rgw_kmip_client_init(*new RGWKMIPManagerImpl(g_ceph_context));
  
  lsubdout(cct, rgw, 1) << "rgw_d3n: rgw_d3n_l1_local_datacache_enabled=" << cct->_conf->rgw_d3n_l1_local_datacache_enabled << dendl;
  if (cct->_conf->rgw_d3n_l1_local_datacache_enabled) {
    lsubdout(cct, rgw, 1) << "rgw_d3n: rgw_d3n_l1_datacache_persistent_path='" << cct->_conf->rgw_d3n_l1_datacache_persistent_path << "'" << dendl;
    lsubdout(cct, rgw, 1) << "rgw_d3n: rgw_d3n_l1_datacache_size=" << cct->_conf->rgw_d3n_l1_datacache_size << dendl;
    lsubdout(cct, rgw, 1) << "rgw_d3n: rgw_d3n_l1_evict_cache_on_start=" << cct->_conf->rgw_d3n_l1_evict_cache_on_start << dendl;
    lsubdout(cct, rgw, 1) << "rgw_d3n: rgw_d3n_l1_fadvise=" << cct->_conf->rgw_d3n_l1_fadvise << dendl;
    lsubdout(cct, rgw, 1) << "rgw_d3n: rgw_d3n_l1_eviction_policy=" << cct->_conf->rgw_d3n_l1_eviction_policy << dendl;
  }
  bool rgw_d3n_datacache_enabled = cct->_conf->rgw_d3n_l1_local_datacache_enabled;
  if (rgw_d3n_datacache_enabled && (cct->_conf->rgw_max_chunk_size != cct->_conf->rgw_obj_stripe_size)) {
    lsubdout(cct, rgw_datacache, 0) << "rgw_d3n:  WARNING: D3N DataCache disabling (D3N requires that the chunk_size equals stripe_size)" << dendl;
    rgw_d3n_datacache_enabled = false;
  }
  if (rgw_d3n_datacache_enabled && !cct->_conf->rgw_beast_enable_async) {
    lsubdout(cct, rgw_datacache, 0) << "rgw_d3n:  WARNING: D3N DataCache disabling (D3N requires yield context - rgw_beast_enable_async=true)" << dendl;
    rgw_d3n_datacache_enabled = false;
  }
  lsubdout(cct, rgw, 1) << "D3N datacache enabled: " << rgw_d3n_datacache_enabled << dendl;

  std::string rgw_store = (!rgw_d3n_datacache_enabled) ? "rados" : "d3n";

  const auto& config_store = g_conf().get_val<std::string>("rgw_backend_store");
#ifdef WITH_RADOSGW_DBSTORE
  if (config_store == "dbstore") {
    rgw_store = "dbstore";
  }
#endif

#ifdef WITH_RADOSGW_MOTR
  if (config_store == "motr") {
    rgw_store = "motr";
  }
#endif

  rgw::sal::Store* store =
    StoreManager::get_storage(&dp, g_ceph_context,
				 rgw_store,
				 g_conf()->rgw_enable_gc_threads,
				 g_conf()->rgw_enable_lc_threads,
				 g_conf()->rgw_enable_quota_threads,
				 g_conf()->rgw_run_sync_thread,
				 g_conf().get_val<bool>("rgw_dynamic_resharding"),
				 g_conf()->rgw_cache_enabled);
  if (!store) {
    mutex.lock();
    init_timer.cancel_all_events();
    init_timer.shutdown();
    mutex.unlock();

    derr << "Couldn't init storage provider (RADOS)" << dendl;
    return EIO;
  }
  r = rgw_perf_start(g_ceph_context);
  if (r < 0) {
    derr << "ERROR: failed starting rgw perf" << dendl;
    return -r;
  }

  rgw_rest_init(g_ceph_context, store->get_zone()->get_zonegroup());

  mutex.lock();
  init_timer.cancel_all_events();
  init_timer.shutdown();
  mutex.unlock();

  rgw_log_usage_init(g_ceph_context, store);

  RGWREST rest;

  list<string> apis;

  get_str_list(g_conf()->rgw_enable_apis, apis);

  map<string, bool> apis_map;
  for (list<string>::iterator li = apis.begin(); li != apis.end(); ++li) {
    apis_map[*li] = true;
  }

  /* warn about insecure keystone secret config options */
  if (!(g_ceph_context->_conf->rgw_keystone_admin_token.empty() ||
	g_ceph_context->_conf->rgw_keystone_admin_password.empty())) {
    dout(0) << "WARNING: rgw_keystone_admin_token and rgw_keystone_admin_password should be avoided as they can expose secrets.  Prefer the new rgw_keystone_admin_token_path and rgw_keystone_admin_password_path options, which read their secrets from files." << dendl;
  }

  // S3 website mode is a specialization of S3
  const bool s3website_enabled = apis_map.count("s3website") > 0;
  const bool sts_enabled = apis_map.count("sts") > 0;
  const bool iam_enabled = apis_map.count("iam") > 0;
  const bool pubsub_enabled = apis_map.count("pubsub") > 0 || apis_map.count("notifications") > 0;
  // Swift API entrypoint could placed in the root instead of S3
  const bool swift_at_root = g_conf()->rgw_swift_url_prefix == "/";
  if (apis_map.count("s3") > 0 || s3website_enabled) {
    if (! swift_at_root) {
      rest.register_default_mgr(set_logging(rest_filter(store, RGW_REST_S3,
                                                        new RGWRESTMgr_S3(s3website_enabled, sts_enabled, iam_enabled, pubsub_enabled))));
    } else {
      derr << "Cannot have the S3 or S3 Website enabled together with "
           << "Swift API placed in the root of hierarchy" << dendl;
      return EINVAL;
    }
  }

  if (pubsub_enabled) {
#ifdef WITH_RADOSGW_AMQP_ENDPOINT
    if (!rgw::amqp::init(cct.get())) {
        dout(1) << "ERROR: failed to initialize AMQP manager" << dendl;
    }
#endif
#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
    if (!rgw::kafka::init(cct.get())) {
        dout(1) << "ERROR: failed to initialize Kafka manager" << dendl;
    }
#endif
  }

  const auto& luarocks_path = g_conf().get_val<std::string>("rgw_luarocks_location");
  if (luarocks_path.empty()) {
    store->set_luarocks_path("");
  } else {
    store->set_luarocks_path(luarocks_path+"/"+g_conf()->name.to_str());
  }
#ifdef WITH_RADOSGW_LUA_PACKAGES
  rgw::sal::RadosStore *rados = dynamic_cast<rgw::sal::RadosStore*>(store);
  if (rados) { /* Supported for only RadosStore */
    rgw::lua::packages_t failed_packages;
    std::string output;
    r = rgw::lua::install_packages(&dp, rados, null_yield, failed_packages, output);
    if (r < 0) {
      dout(1) << "ERROR: failed to install lua packages from allowlist" << dendl;
    }
    if (!output.empty()) {
      dout(10) << "INFO: lua packages installation output: \n" << output << dendl; 
    }
    for (const auto& p : failed_packages) {
      dout(5) << "WARNING: failed to install lua package: " << p << " from allowlist" << dendl;
    }
  }
#endif

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
      if (store->get_zone()->get_zonegroup().zones.size() > 1) {
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
    /* XXX dang part of this is RADOS specific */
    admin_resource->register_resource("bucket", new RGWRESTMgr_Bucket);
  
    /*Registering resource for /admin/metadata */
    admin_resource->register_resource("metadata", new RGWRESTMgr_Metadata);
    /* XXX dang ifdef these RADOS ? */
    admin_resource->register_resource("log", new RGWRESTMgr_Log);
    admin_resource->register_resource("config", new RGWRESTMgr_Config);
    admin_resource->register_resource("realm", new RGWRESTMgr_Realm);
    rest.register_resource(g_conf()->rgw_admin_entry, admin_resource);
  }

  /* Initialize the registry of auth strategies which will coordinate
   * the dynamic reconfiguration. */
  rgw::auth::ImplicitTenants implicit_tenant_context{g_conf()};
  g_conf().add_observer(&implicit_tenant_context);
  auto auth_registry = \
    rgw::auth::StrategyRegistry::create(g_ceph_context, implicit_tenant_context, store);

  /* Header custom behavior */
  rest.register_x_headers(g_conf()->rgw_log_http_headers);

  if (cct->_conf.get_val<std::string>("rgw_scheduler_type") == "dmclock" &&
      !cct->check_experimental_feature_enabled("dmclock")){
    derr << "dmclock scheduler type is experimental and needs to be"
	 << "set in the option enable experimental data corrupting features"
	 << dendl;
    return EINVAL;
  }

  rgw::dmclock::SchedulerCtx sched_ctx{cct.get()};

  OpsLogManifold *olog = new OpsLogManifold();
  if (!g_conf()->rgw_ops_log_socket_path.empty()) {
    OpsLogSocket* olog_socket = new OpsLogSocket(g_ceph_context, g_conf()->rgw_ops_log_data_backlog);
    olog_socket->init(g_conf()->rgw_ops_log_socket_path);
    olog->add_sink(olog_socket);
  }
  OpsLogFile* ops_log_file;
  if (!g_conf()->rgw_ops_log_file_path.empty()) {
    ops_log_file = new OpsLogFile(g_ceph_context, g_conf()->rgw_ops_log_file_path, g_conf()->rgw_ops_log_data_backlog);
    ops_log_file->start();
    olog->add_sink(ops_log_file);
  }
  olog->add_sink(new OpsLogRados(store));

  r = signal_fd_init();
  if (r < 0) {
    derr << "ERROR: unable to initialize signal fds" << dendl;
    exit(1);
  }

  register_async_signal_handler(SIGTERM, handle_sigterm);
  register_async_signal_handler(SIGINT, handle_sigterm);
  register_async_signal_handler(SIGUSR1, handle_sigterm);
  sighandler_alrm = signal(SIGALRM, godown_alarm);

  map<string, string> service_map_meta;
  service_map_meta["pid"] = stringify(getpid());

  list<RGWFrontend *> fes;

  string frontend_defs_str = g_conf().get_val<string>("rgw_frontend_defaults");

  list<string> frontends_def;
  get_str_list(frontend_defs_str, ",", frontends_def);

  map<string, std::unique_ptr<RGWFrontendConfig> > fe_def_map;
  for (auto& f : frontends_def) {
    RGWFrontendConfig *config = new RGWFrontendConfig(f);
    int r = config->init();
    if (r < 0) {
      delete config;
      cerr << "ERROR: failed to init default config: " << f << std::endl;
      return EINVAL;
    }

    fe_def_map[config->get_framework()].reset(config);
  }

  int fe_count = 0;

  for (multimap<string, RGWFrontendConfig *>::iterator fiter = fe_map.begin();
       fiter != fe_map.end(); ++fiter, ++fe_count) {
    RGWFrontendConfig *config = fiter->second;
    string framework = config->get_framework();

    auto def_iter = fe_def_map.find(framework);
    if (def_iter != fe_def_map.end()) {
      config->set_default_config(*def_iter->second);
    }

    RGWFrontend *fe = NULL;

    if (framework == "loadgen") {
      int port;
      config->get_val("port", 80, &port);
      std::string uri_prefix;
      config->get_val("prefix", "", &uri_prefix);

      RGWProcessEnv env = { store, &rest, olog, port, uri_prefix, auth_registry };

      fe = new RGWLoadGenFrontend(env, config);
    }
    else if (framework == "beast") {
      int port;
      config->get_val("port", 80, &port);
      std::string uri_prefix;
      config->get_val("prefix", "", &uri_prefix);
      RGWProcessEnv env{ store, &rest, olog, port, uri_prefix, auth_registry };
      fe = new RGWAsioFrontend(env, config, sched_ctx);
    }

    service_map_meta["frontend_type#" + stringify(fe_count)] = framework;
    service_map_meta["frontend_config#" + stringify(fe_count)] = config->get_config();

    if (fe == NULL) {
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

  r = store->register_to_service_map(&dp, "rgw", service_map_meta);
  if (r < 0) {
    derr << "ERROR: failed to register to service map: " << cpp_strerror(-r) << dendl;

    /* ignore error */
  }


  // add a watcher to respond to realm configuration changes
  RGWPeriodPusher pusher(&dp, store, null_yield);
  RGWFrontendPauser pauser(fes, implicit_tenant_context, &pusher);
  auto reloader = std::make_unique<RGWRealmReloader>(store,
						     service_map_meta, &pauser);

  RGWRealmWatcher realm_watcher(&dp, g_ceph_context, store->get_zone()->get_realm());
  realm_watcher.add_watcher(RGWRealmNotify::Reload, *reloader);
  realm_watcher.add_watcher(RGWRealmNotify::ZonesNeedPeriod, pusher);

#if defined(HAVE_SYS_PRCTL_H)
  if (prctl(PR_SET_DUMPABLE, 1) == -1) {
    cerr << "warning: unable to set dumpable flag: " << cpp_strerror(errno) << std::endl;
  }
#endif

  wait_shutdown();

  derr << "shutting down" << dendl;

  reloader.reset(); // stop the realm reloader

  for (list<RGWFrontend *>::iterator liter = fes.begin(); liter != fes.end();
       ++liter) {
    RGWFrontend *fe = *liter;
    fe->stop();
  }

  for (list<RGWFrontend *>::iterator liter = fes.begin(); liter != fes.end();
       ++liter) {
    RGWFrontend *fe = *liter;
    fe->join();
    delete fe;
  }

  for (list<RGWFrontendConfig *>::iterator liter = configs.begin();
       liter != configs.end(); ++liter) {
    RGWFrontendConfig *fec = *liter;
    delete fec;
  }

  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGTERM, handle_sigterm);
  unregister_async_signal_handler(SIGINT, handle_sigterm);
  unregister_async_signal_handler(SIGUSR1, handle_sigterm);
  shutdown_async_signal_handler();

  rgw_log_usage_finalize();
  delete olog;

  StoreManager::close_storage(store);
  rgw::auth::s3::LDAPEngine::shutdown();
  rgw_tools_cleanup();
  rgw_shutdown_resolver();
  rgw_http_client_cleanup();
  rgw_kmip_client_cleanup();
  rgw::curl::cleanup_curl();
  g_conf().remove_observer(&implicit_tenant_context);
#ifdef WITH_RADOSGW_AMQP_ENDPOINT
  rgw::amqp::shutdown();
#endif
#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
  rgw::kafka::shutdown();
#endif

  rgw_perf_stop(g_ceph_context);

  dout(1) << "final shutdown" << dendl;

  signal_fd_finalize();

  return 0;
}

extern "C" {

int radosgw_main(int argc, const char** argv)
{
  return radosgw_Main(argc, argv);
}

} /* extern "C" */

