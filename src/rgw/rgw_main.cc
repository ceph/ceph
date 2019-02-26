// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/safe_io.h"
#include "common/TracepointProvider.h"
#include "include/compat.h"
#include "include/str_list.h"
#include "include/stringify.h"
#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_otp.h"
#include "rgw_period_pusher.h"
#include "rgw_realm_reloader.h"
#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_swift.h"
#include "rgw_rest_admin.h"
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
#include "rgw_perf_counters.h"
#if defined(WITH_RADOSGW_BEAST_FRONTEND)
#include "rgw_asio_frontend.h"
#endif /* WITH_RADOSGW_BEAST_FRONTEND */

#include "rgw_dmclock_scheduler_ctx.h"

#include "services/svc_zone.h"

#ifdef HAVE_SYS_PRCTL_H
#include <sys/prctl.h>
#endif

#define dout_subsys ceph_subsys_rgw

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
#if defined(WITH_RADOSGW_FCGI_FRONTEND)
  FCGX_ShutdownPending();
#endif

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

static RGWRESTMgr *rest_filter(RGWRados *store, int dialect, RGWRESTMgr *orig)
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
int main(int argc, const char **argv)
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
    { "objecter_inflight_ops", "24576" }
  };

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  // First, let's determine which frontends are configured.
  int flags = CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS;
  global_pre_init(
    &defaults, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_DAEMON,
    flags);

  list<string> frontends;
  g_conf().early_expand_meta(g_conf()->rgw_frontends, &cerr);
  get_str_list(g_conf()->rgw_frontends, ",", frontends);
  multimap<string, RGWFrontendConfig *> fe_map;
  list<RGWFrontendConfig *> configs;
  if (frontends.empty()) {
    frontends.push_back("civetweb");
  }
  for (list<string>::iterator iter = frontends.begin(); iter != frontends.end(); ++iter) {
    string& f = *iter;

    if (f.find("civetweb") != string::npos || f.find("beast") != string::npos) {
      // If civetweb or beast is configured as a frontend, prevent global_init() from
      // dropping permissions by setting the appropriate flag.
      flags |= CINIT_FLAG_DEFER_DROP_PRIVILEGES;
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

  // Now that we've determined which frontend(s) to use, continue with global
  // initialization. Passing false as the final argument ensures that
  // global_pre_init() is not invoked twice.
  // claim the reference and release it after subsequent destructors have fired
  auto cct = global_init(&defaults, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_DAEMON,
			 flags, "rgw_data", false);

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
  Mutex mutex("main");
  SafeTimer init_timer(g_ceph_context, mutex);
  init_timer.init();
  mutex.Lock();
  init_timer.add_event_after(g_conf()->rgw_init_timeout, new C_InitTimeout);
  mutex.Unlock();

  common_init_finish(g_ceph_context);

  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);

  TracepointProvider::initialize<rgw_rados_tracepoint_traits>(g_ceph_context);
  TracepointProvider::initialize<rgw_op_tracepoint_traits>(g_ceph_context);

  int r = rgw_tools_init(g_ceph_context);
  if (r < 0) {
    derr << "ERROR: unable to initialize rgw tools" << dendl;
    return -r;
  }

  rgw_init_resolver();
  rgw::curl::setup_curl(fe_map);
  rgw_http_client_init(g_ceph_context);
  
#if defined(WITH_RADOSGW_FCGI_FRONTEND)
  FCGX_Init();
#endif

  RGWRados *store =
    RGWStoreManager::get_storage(g_ceph_context,
				 g_conf()->rgw_enable_gc_threads,
				 g_conf()->rgw_enable_lc_threads,
				 g_conf()->rgw_enable_quota_threads,
				 g_conf()->rgw_run_sync_thread,
				 g_conf().get_val<bool>("rgw_dynamic_resharding"),
				 g_conf()->rgw_cache_enabled);
  if (!store) {
    mutex.Lock();
    init_timer.cancel_all_events();
    init_timer.shutdown();
    mutex.Unlock();

    derr << "Couldn't init storage provider (RADOS)" << dendl;
    return EIO;
  }
  r = rgw_perf_start(g_ceph_context);
  if (r < 0) {
    derr << "ERROR: failed starting rgw perf" << dendl;
    return -r;
  }

  rgw_rest_init(g_ceph_context, store, store->svc.zone->get_zonegroup());

  mutex.Lock();
  init_timer.cancel_all_events();
  init_timer.shutdown();
  mutex.Unlock();

  rgw_user_init(store);
  rgw_bucket_init(store->meta_mgr);
  rgw_otp_init(store);
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
  // Swift API entrypoint could placed in the root instead of S3
  const bool swift_at_root = g_conf()->rgw_swift_url_prefix == "/";
  if (apis_map.count("s3") > 0 || s3website_enabled) {
    if (! swift_at_root) {
      rest.register_default_mgr(set_logging(rest_filter(store, RGW_REST_S3,
                                                        new RGWRESTMgr_S3(s3website_enabled, sts_enabled))));
    } else {
      derr << "Cannot have the S3 or S3 Website enabled together with "
           << "Swift API placed in the root of hierarchy" << dendl;
      return EINVAL;
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
      if (store->svc.zone->get_zonegroup().zones.size() > 1) {
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
    admin_resource->register_resource("usage", new RGWRESTMgr_Usage);
    admin_resource->register_resource("user", new RGWRESTMgr_User);
    admin_resource->register_resource("bucket", new RGWRESTMgr_Bucket);
  
    /*Registering resource for /admin/metadata */
    admin_resource->register_resource("metadata", new RGWRESTMgr_Metadata);
    admin_resource->register_resource("log", new RGWRESTMgr_Log);
    admin_resource->register_resource("config", new RGWRESTMgr_Config);
    admin_resource->register_resource("realm", new RGWRESTMgr_Realm);
    rest.register_resource(g_conf()->rgw_admin_entry, admin_resource);
  }

  /* Initialize the registry of auth strategies which will coordinate
   * the dynamic reconfiguration. */
  auto auth_registry = \
    rgw::auth::StrategyRegistry::create(g_ceph_context, store);

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

  OpsLogSocket *olog = NULL;

  if (!g_conf()->rgw_ops_log_socket_path.empty()) {
    olog = new OpsLogSocket(g_ceph_context, g_conf()->rgw_ops_log_data_backlog);
    olog->init(g_conf()->rgw_ops_log_socket_path);
  }

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

  int fe_count = 0;

  for (multimap<string, RGWFrontendConfig *>::iterator fiter = fe_map.begin();
       fiter != fe_map.end(); ++fiter, ++fe_count) {
    RGWFrontendConfig *config = fiter->second;
    string framework = config->get_framework();
    RGWFrontend *fe = NULL;

    if (framework == "civetweb" || framework == "mongoose") {
      framework = "civetweb";
      std::string uri_prefix;
      config->get_val("prefix", "", &uri_prefix);

      RGWProcessEnv env = { store, &rest, olog, 0, uri_prefix, auth_registry };
      //TODO: move all of scheduler initializations to frontends?

      fe = new RGWCivetWebFrontend(env, config, sched_ctx);
    }
    else if (framework == "loadgen") {
      int port;
      config->get_val("port", 80, &port);
      std::string uri_prefix;
      config->get_val("prefix", "", &uri_prefix);

      RGWProcessEnv env = { store, &rest, olog, port, uri_prefix, auth_registry };

      fe = new RGWLoadGenFrontend(env, config);
    }
#if defined(WITH_RADOSGW_BEAST_FRONTEND)
    else if (framework == "beast") {
      int port;
      config->get_val("port", 80, &port);
      std::string uri_prefix;
      config->get_val("prefix", "", &uri_prefix);
      RGWProcessEnv env{ store, &rest, olog, port, uri_prefix, auth_registry };
      fe = new RGWAsioFrontend(env, config, sched_ctx);
    }
#endif /* WITH_RADOSGW_BEAST_FRONTEND */
#if defined(WITH_RADOSGW_FCGI_FRONTEND)
    else if (framework == "fastcgi" || framework == "fcgi") {
      framework = "fastcgi";
      std::string uri_prefix;
      config->get_val("prefix", "", &uri_prefix);
      RGWProcessEnv fcgi_pe = { store, &rest, olog, 0, uri_prefix, auth_registry };

      fe = new RGWFCGXFrontend(fcgi_pe, config);
    }
#endif /* WITH_RADOSGW_FCGI_FRONTEND */

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

  r = store->register_to_service_map("rgw", service_map_meta);
  if (r < 0) {
    derr << "ERROR: failed to register to service map: " << cpp_strerror(-r) << dendl;

    /* ignore error */
  }


  // add a watcher to respond to realm configuration changes
  RGWPeriodPusher pusher(store);
  RGWFrontendPauser pauser(fes, &pusher);
  RGWRealmReloader reloader(store, service_map_meta, &pauser);

  RGWRealmWatcher realm_watcher(g_ceph_context, store->svc.zone->get_realm());
  realm_watcher.add_watcher(RGWRealmNotify::Reload, reloader);
  realm_watcher.add_watcher(RGWRealmNotify::ZonesNeedPeriod, pusher);

#if defined(HAVE_SYS_PRCTL_H)
  if (prctl(PR_SET_DUMPABLE, 1) == -1) {
    cerr << "warning: unable to set dumpable flag: " << cpp_strerror(errno) << std::endl;
  }
#endif

  wait_shutdown();

  derr << "shutting down" << dendl;

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

  RGWStoreManager::close_storage(store);
  rgw::auth::s3::LDAPEngine::shutdown();
  rgw_tools_cleanup();
  rgw_shutdown_resolver();
  rgw_http_client_cleanup();
  rgw::curl::cleanup_curl();

  rgw_perf_stop(g_ceph_context);

  dout(1) << "final shutdown" << dendl;

  signal_fd_finalize();

  return 0;
}
