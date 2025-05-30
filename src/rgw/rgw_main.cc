// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <boost/intrusive/list.hpp>
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/TracepointProvider.h"
#include "rgw_main.h"
#include "rgw_signal.h"
#include "rgw_common.h"
#include "rgw_lib.h"
#include "rgw_log.h"
#include "rgw_exporter.h"

#ifdef HAVE_SYS_PRCTL_H
#include <sys/prctl.h>
#endif

using namespace std;

static constexpr auto dout_subsys = ceph_subsys_rgw;

static sig_t sighandler_alrm;

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
/*
// Global pointer to the exporter instance
RGWExporter* rgw_exporter = nullptr;

// Function to initialize the exporter
void rgw_init_exporter(CephContext* cct) {
    rgw_exporter = new RGWExporter(cct);
    rgw_exporter->start(); // Start the background thread (e.g., updating every 30 seconds)
}

// Main initialization function for RGW
int rgw_main_init(CephContext* cct) {
    // HSTTODO: other RGW initialization code

    // Initialize the exporter to start collecting usage metrics
    rgw_init_exporter(cct);
    
    return 0;
}

// Main shutdown function for RGW
void rgw_main_shutdown() {
    // HSTTODO:  other shutdown code 

    // Shut down the exporter and clean up resources
    if (rgw_exporter) {
        rgw_exporter->stop(); // Signal the background thread to stop and wait for it to join
        delete rgw_exporter;
        rgw_exporter = nullptr;
    }
}
*/
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

/*
 * start up the RADOS connection and then handle HTTP messages as they come in
 *
 * This has an uncaught exception. Even if the exception is caught, the program
 * would need to be terminated, so the warning is simply suppressed.
 */
// coverity[root_function:SUPPRESS]
int main(int argc, char *argv[])
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
  map<std::string,std::string> defaults = {
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
  register_async_signal_handler(SIGHUP, rgw::signal::sighup_handler);
  r = rgw::signal::signal_fd_init();
  if (r < 0) {
    derr << "ERROR: unable to initialize signal fds" << dendl;
  exit(1);
  }

  register_async_signal_handler(SIGTERM, rgw::signal::handle_sigterm);
  register_async_signal_handler(SIGINT, rgw::signal::handle_sigterm);
  register_async_signal_handler(SIGUSR1, rgw::signal::handle_sigterm);
  register_async_signal_handler(SIGXFSZ, rgw::signal::sig_handler_noop);
  sighandler_alrm = signal(SIGALRM, godown_alarm);

  main.init_perfcounters();
  main.init_http_clients();

  r = main.init_storage();
  if (r < 0) {
    mutex.lock();
    init_timer.cancel_all_events();
    init_timer.shutdown();
    mutex.unlock();

    derr << "Couldn't init storage provider (RADOS)" << dendl;
    return -r;
  }

  if (cct->_conf->rgw_enable_usage_perf_counters) {
    dout(1) << "Starting RGW usage exporter with LMDB backing" << dendl;
    g_rgw_exporter = new RGWExporter(cct.get(), main.store.get());
  }
  
  main.cond_init_apis();

  mutex.lock();
  init_timer.cancel_all_events();
  init_timer.shutdown();
  mutex.unlock();

  main.init_ldap();
  main.init_opslog();
  main.init_tracepoints();
  main.init_lua();
  r = main.init_frontends2(nullptr /* RGWLib */);
  if (r != 0) {
    derr << "ERROR:  initialize frontend fail, r = " << r << dendl;
    main.shutdown();
    return r;
  }

#if defined(HAVE_SYS_PRCTL_H)
  if (prctl(PR_SET_DUMPABLE, 1) == -1) {
    cerr << "warning: unable to set dumpable flag: " << cpp_strerror(errno) << std::endl;
  }
#endif

  rgw::signal::wait_shutdown();

  derr << "shutting down" << dendl;

  if (g_rgw_exporter) {
    g_rgw_exporter->stop();
    delete g_rgw_exporter;
    g_rgw_exporter = nullptr;
  }
  
  if (g_rgw_exporter) {
    g_rgw_exporter->shutdown();
    delete g_rgw_exporter;
    g_rgw_exporter = nullptr;
  }  
  
  const auto finalize_async_signals = []() {
    unregister_async_signal_handler(SIGHUP, rgw::signal::sighup_handler);
    unregister_async_signal_handler(SIGTERM, rgw::signal::handle_sigterm);
    unregister_async_signal_handler(SIGINT, rgw::signal::handle_sigterm);
    unregister_async_signal_handler(SIGUSR1, rgw::signal::handle_sigterm);
    unregister_async_signal_handler(SIGXFSZ, rgw::signal::sig_handler_noop);
    shutdown_async_signal_handler();
  };

  if (g_rgw_exporter) {
    g_rgw_exporter->shutdown();
    delete g_rgw_exporter;
    g_rgw_exporter = nullptr;
  }
  
  main.shutdown(finalize_async_signals);

  dout(1) << "final shutdown" << dendl;

  rgw::signal::signal_fd_finalize();

  return 0;
} /* main(int argc, char* argv[]) */
