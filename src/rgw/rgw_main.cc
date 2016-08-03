// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>

#include <curl/curl.h>

#include <boost/intrusive_ptr.hpp>

#include "acconfig.h"

#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/safe_io.h"
#include "include/compat.h"
#include "include/str_list.h"
#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_user.h"
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
#include "rgw_rest_opstate.h"
#include "rgw_replica_log.h"
#include "rgw_rest_replica_log.h"
#include "rgw_rest_config.h"
#include "rgw_rest_realm.h"
#include "rgw_swift_auth.h"
#include "rgw_swift.h"
#include "rgw_log.h"
#include "rgw_tools.h"
#include "rgw_resolve.h"

#include "rgw_request.h"
#include "rgw_process.h"
#include "rgw_frontend.h"

#include <map>
#include <string>
#include <vector>

#include "include/types.h"
#include "common/BackTrace.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

static sig_t sighandler_alrm;

class RGWProcess;

static int signal_fd[2] = {0, 0};
static atomic_t disable_signal_fd;

void signal_shutdown()
{
  if (!disable_signal_fd.read()) {
    int val = 0;
    int ret = write(signal_fd[0], (char *)&val, sizeof(val));
    if (ret < 0) {
      int err = -errno;
      derr << "ERROR: " << __func__ << ": write() returned "
	   << cpp_strerror(-err) << dendl;
    }
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

int signal_fd_init()
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
  FCGX_ShutdownPending();

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

#ifdef HAVE_CURL_MULTI_WAIT
static void check_curl()
{
}
#else
static void check_curl()
{
  derr << "WARNING: libcurl doesn't support curl_multi_wait()" << dendl;
  derr << "WARNING: cross zone / region transfer performance may be affected" << dendl;
}
#endif

class C_InitTimeout : public Context {
public:
  C_InitTimeout() {}
  void finish(int r) {
    derr << "Initialization timeout, failed to initialize" << dendl;
    exit(1);
  }
};

int usage()
{
  cerr << "usage: radosgw [options...]" << std::endl;
  cerr << "options:\n";
  cerr << "  --rgw-region=<region>     region in which radosgw runs\n";
  cerr << "  --rgw-zone=<zone>         zone in which radosgw runs\n";
  cerr << "  --rgw-socket-path=<path>  specify a unix domain socket path\n";
  cerr << "  -m monaddress[:port]      connect to specified monitor\n";
  cerr << "  --keyring=<path>          path to radosgw keyring\n";
  cerr << "  --logfile=<logfile>       file to log debug output\n";
  cerr << "  --debug-rgw=<log-level>/<memory-level>  set radosgw debug level\n";
  generic_server_usage();

  return 0;
}

static RGWRESTMgr *set_logging(RGWRESTMgr *mgr)
{
  mgr->set_logging(true);
  return mgr;
}

void intrusive_ptr_add_ref(CephContext* cct) { cct->get(); }
void intrusive_ptr_release(CephContext* cct) { cct->put(); }

/*
 * start up the RADOS connection and then handle HTTP messages as they come in
 */
int main(int argc, const char **argv)
{
  // dout() messages will be sent to stderr, but FCGX wants messages on stdout
  // Redirect stderr to stdout.
  TEMP_FAILURE_RETRY(close(STDERR_FILENO));
  if (TEMP_FAILURE_RETRY(dup2(STDOUT_FILENO, STDERR_FILENO) < 0)) {
    int err = errno;
    cout << "failed to redirect stderr to stdout: " << cpp_strerror(err)
	 << std::endl;
    return ENOSYS;
  }

  /* alternative default for module */
  vector<const char *> def_args;
  def_args.push_back("--debug-rgw=1/5");
  def_args.push_back("--keyring=$rgw_data/keyring");

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  // First, let's determine which frontends are configured.
  int flags = CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS;
  global_pre_init(&def_args, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_DAEMON,
		  flags);

  list<string> frontends;
  get_str_list(g_conf->rgw_frontends, ",", frontends);
  multimap<string, RGWFrontendConfig *> fe_map;
  list<RGWFrontendConfig *> configs;
  if (frontends.empty()) {
    frontends.push_back("fastcgi");
  }
  for (list<string>::iterator iter = frontends.begin(); iter != frontends.end(); ++iter) {
    string& f = *iter;

    if (f.find("civetweb") != string::npos) {
      // If civetweb is configured as a frontend, prevent global_init() from
      // dropping permissions by setting the appropriate flag.
      flags |= CINIT_FLAG_DEFER_DROP_PRIVILEGES;
      if (f.find("port") != string::npos) {
	// check for the most common ws problems
	if ((f.find("port=") == string::npos) ||
	    (f.find("port= ") != string::npos)) {
	  derr << "WARNING: civetweb frontend config found unexpected spacing around 'port' (ensure civetweb port parameter has the form 'port=80' with no spaces before or after '=')" << dendl;
	}
      }
    }

    RGWFrontendConfig *config = new RGWFrontendConfig(f);
    int r = config->init();
    if (r < 0) {
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
  global_init(&def_args, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_DAEMON,
	      flags, "rgw_data", false);

  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ++i) {
    if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      return 0;
    }
  }

  check_curl();

  if (g_conf->daemonize) {
    global_init_daemonize(g_ceph_context);
  }
  Mutex mutex("main");
  SafeTimer init_timer(g_ceph_context, mutex);
  init_timer.init();
  mutex.Lock();
  init_timer.add_event_after(g_conf->rgw_init_timeout, new C_InitTimeout);
  mutex.Unlock();

  // Enable the perf counter before starting the service thread
  g_ceph_context->enable_perf_counter();

  common_init_finish(g_ceph_context);

  // claim the reference and release it after subsequent destructors have fired
  boost::intrusive_ptr<CephContext> cct(g_ceph_context, false);

  rgw_tools_init(g_ceph_context);

  rgw_init_resolver();
  
  curl_global_init(CURL_GLOBAL_ALL);
  
  FCGX_Init();

  int r = 0;
  RGWRados *store = RGWStoreManager::get_storage(g_ceph_context,
      g_conf->rgw_enable_gc_threads, g_conf->rgw_enable_quota_threads,
      g_conf->rgw_run_sync_thread);
  if (!store) {
    mutex.Lock();
    init_timer.cancel_all_events();
    init_timer.shutdown();
    mutex.Unlock();

    derr << "Couldn't init storage provider (RADOS)" << dendl;
    return EIO;
  }
  r = rgw_perf_start(g_ceph_context);

  rgw_rest_init(g_ceph_context, store, store->get_zonegroup());

  mutex.Lock();
  init_timer.cancel_all_events();
  init_timer.shutdown();
  mutex.Unlock();

  if (r) 
    return 1;

  rgw_user_init(store);
  rgw_bucket_init(store->meta_mgr);
  rgw_log_usage_init(g_ceph_context, store);

  RGWREST rest;

  list<string> apis;
  bool do_swift = false;

  get_str_list(g_conf->rgw_enable_apis, apis);

  map<string, bool> apis_map;
  for (list<string>::iterator li = apis.begin(); li != apis.end(); ++li) {
    apis_map[*li] = true;
  }

  // S3 website mode is a specialization of S3
  bool s3website_enabled = apis_map.count("s3website") > 0;
  if (apis_map.count("s3") > 0 || s3website_enabled)
    rest.register_default_mgr(set_logging(new RGWRESTMgr_S3(s3website_enabled)));

  if (apis_map.count("swift") > 0) {
    do_swift = true;
    swift_init(g_ceph_context);
    rest.register_resource(g_conf->rgw_swift_url_prefix,
			   set_logging(new RGWRESTMgr_SWIFT));
  }

  if (apis_map.count("swift_auth") > 0)
    rest.register_resource(g_conf->rgw_swift_auth_entry,
			   set_logging(new RGWRESTMgr_SWIFT_Auth));

  if (apis_map.count("admin") > 0) {
    RGWRESTMgr_Admin *admin_resource = new RGWRESTMgr_Admin;
    admin_resource->register_resource("usage", new RGWRESTMgr_Usage);
    admin_resource->register_resource("user", new RGWRESTMgr_User);
    admin_resource->register_resource("bucket", new RGWRESTMgr_Bucket);
  
    /*Registering resource for /admin/metadata */
    admin_resource->register_resource("metadata", new RGWRESTMgr_Metadata);
    admin_resource->register_resource("log", new RGWRESTMgr_Log);
    admin_resource->register_resource("opstate", new RGWRESTMgr_Opstate);
    admin_resource->register_resource("replica_log", new RGWRESTMgr_ReplicaLog);
    admin_resource->register_resource("config", new RGWRESTMgr_Config);
    admin_resource->register_resource("realm", new RGWRESTMgr_Realm);
    rest.register_resource(g_conf->rgw_admin_entry, admin_resource);
  }

  OpsLogSocket *olog = NULL;

  if (!g_conf->rgw_ops_log_socket_path.empty()) {
    olog = new OpsLogSocket(g_ceph_context, g_conf->rgw_ops_log_data_backlog);
    olog->init(g_conf->rgw_ops_log_socket_path);
  }

  r = signal_fd_init();
  if (r < 0) {
    derr << "ERROR: unable to initialize signal fds" << dendl;
    exit(1);
  }

  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);
  register_async_signal_handler(SIGTERM, handle_sigterm);
  register_async_signal_handler(SIGINT, handle_sigterm);
  register_async_signal_handler(SIGUSR1, handle_sigterm);
  sighandler_alrm = signal(SIGALRM, godown_alarm);

  list<RGWFrontend *> fes;

  for (multimap<string, RGWFrontendConfig *>::iterator fiter = fe_map.begin();
       fiter != fe_map.end(); ++fiter) {
    RGWFrontendConfig *config = fiter->second;
    string framework = config->get_framework();
    RGWFrontend *fe;
    if (framework == "fastcgi" || framework == "fcgi") {
      RGWProcessEnv fcgi_pe = { store, &rest, olog, 0 };

      fe = new RGWFCGXFrontend(fcgi_pe, config);
    } else if (framework == "civetweb" || framework == "mongoose") {
      RGWProcessEnv env = { store, &rest, olog, 0 };

      fe = new RGWMongooseFrontend(env, config);
    } else if (framework == "loadgen") {
      int port;
      config->get_val("port", 80, &port);

      RGWProcessEnv env = { store, &rest, olog, port };

      fe = new RGWLoadGenFrontend(env, config);
    } else {
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

  // add a watcher to respond to realm configuration changes
  RGWPeriodPusher pusher(store);
  RGWFrontendPauser pauser(fes, &pusher);
  RGWRealmReloader reloader(store, &pauser);

  RGWRealmWatcher realm_watcher(g_ceph_context, store->realm);
  realm_watcher.add_watcher(RGWRealmNotify::Reload, reloader);
  realm_watcher.add_watcher(RGWRealmNotify::ZonesNeedPeriod, pusher);

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

  if (do_swift) {
    swift_finalize();
  }

  rgw_log_usage_finalize();

  delete olog;

  RGWStoreManager::close_storage(store);

  rgw_tools_cleanup();
  rgw_shutdown_resolver();
  curl_global_cleanup();

  rgw_perf_stop(g_ceph_context);

  dout(1) << "final shutdown" << dendl;

  signal_fd_finalize();

  return 0;
}
