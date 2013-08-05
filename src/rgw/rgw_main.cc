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

#include "acconfig.h"
#ifdef FASTCGI_INCLUDE_DIR
# include "fastcgi/fcgiapp.h"
#else
# include "fcgiapp.h"
#endif

#include "rgw_fcgi.h"

#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "common/Timer.h"
#include "common/Throttle.h"
#include "include/str_list.h"
#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_acl.h"
#include "rgw_user.h"
#include "rgw_op.h"
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
#include "rgw_swift_auth.h"
#include "rgw_swift.h"
#include "rgw_log.h"
#include "rgw_tools.h"
#include "rgw_resolve.h"

#include <map>
#include <string>
#include <vector>
#include <iostream>
#include <sstream>

#include "include/types.h"
#include "common/BackTrace.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

static sighandler_t sighandler_usr1;
static sighandler_t sighandler_alrm;
static sighandler_t sighandler_term;

class RGWProcess;

static RGWProcess *pprocess = NULL;


#define SOCKET_BACKLOG 1024

struct RGWRequest
{
  FCGX_Request fcgx;
  uint64_t id;
  struct req_state *s;
  string req_str;
  RGWOp *op;
  utime_t ts;

  RGWRequest() : id(0), s(NULL), op(NULL) {
  }

  ~RGWRequest() {
    delete s;
  }
 
  req_state *init_state(CephContext *cct, RGWEnv *env) { 
    s = new req_state(cct, env);
    return s;
  }

  void log_format(struct req_state *s, const char *fmt, ...)
  {
#define LARGE_SIZE 1024
    char buf[LARGE_SIZE];
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);

    log(s, buf);
  }

  void log_init() {
    ts = ceph_clock_now(g_ceph_context);
  }

  void log(struct req_state *s, const char *msg) {
    if (s->info.method && req_str.size() == 0) {
      req_str = s->info.method;
      req_str.append(" ");
      req_str.append(s->info.request_uri);
    }
    utime_t t = ceph_clock_now(g_ceph_context) - ts;
    dout(2) << "req " << id << ":" << t << ":" << s->dialect << ":" << req_str << ":" << (op ? op->name() : "") << ":" << msg << dendl;
  }
};

class RGWProcess {
  RGWRados *store;
  OpsLogSocket *olog;
  deque<RGWRequest *> m_req_queue;
  ThreadPool m_tp;
  Throttle req_throttle;
  RGWREST *rest;
  int sock_fd;

  struct RGWWQ : public ThreadPool::WorkQueue<RGWRequest> {
    RGWProcess *process;
    RGWWQ(RGWProcess *p, time_t timeout, time_t suicide_timeout, ThreadPool *tp)
      : ThreadPool::WorkQueue<RGWRequest>("RGWWQ", timeout, suicide_timeout, tp), process(p) {}

    bool _enqueue(RGWRequest *req) {
      process->m_req_queue.push_back(req);
      perfcounter->inc(l_rgw_qlen);
      dout(20) << "enqueued request req=" << hex << req << dec << dendl;
      _dump_queue();
      return true;
    }
    void _dequeue(RGWRequest *req) {
      assert(0);
    }
    bool _empty() {
      return process->m_req_queue.empty();
    }
    RGWRequest *_dequeue() {
      if (process->m_req_queue.empty())
	return NULL;
      RGWRequest *req = process->m_req_queue.front();
      process->m_req_queue.pop_front();
      dout(20) << "dequeued request req=" << hex << req << dec << dendl;
      _dump_queue();
      perfcounter->inc(l_rgw_qlen, -1);
      return req;
    }
    void _process(RGWRequest *req) {
      perfcounter->inc(l_rgw_qactive);
      process->handle_request(req);
      process->req_throttle.put(1);
      perfcounter->inc(l_rgw_qactive, -1);
    }
    void _dump_queue() {
      deque<RGWRequest *>::iterator iter;
      if (process->m_req_queue.empty()) {
        dout(20) << "RGWWQ: empty" << dendl;
        return;
      }
      dout(20) << "RGWWQ:" << dendl;
      for (iter = process->m_req_queue.begin(); iter != process->m_req_queue.end(); ++iter) {
        dout(20) << "req: " << hex << *iter << dec << dendl;
      }
    }
    void _clear() {
      assert(process->m_req_queue.empty());
    }
  } req_wq;

  uint64_t max_req_id;

public:
  RGWProcess(CephContext *cct, RGWRados *rgwstore, OpsLogSocket *_olog, int num_threads, RGWREST *_rest)
    : store(rgwstore), olog(_olog), m_tp(cct, "RGWProcess::m_tp", num_threads),
      req_throttle(cct, "rgw_ops", num_threads * 2),
      rest(_rest), sock_fd(-1),
      req_wq(this, g_conf->rgw_op_thread_timeout,
	     g_conf->rgw_op_thread_suicide_timeout, &m_tp),
      max_req_id(0) {}
  void run();
  void handle_request(RGWRequest *req);

  void close_fd() {
    if (sock_fd >= 0)
      close(sock_fd);
  }
};

void RGWProcess::run()
{
  sock_fd = 0;
  if (!g_conf->rgw_socket_path.empty()) {
    string path_str = g_conf->rgw_socket_path;

    /* this is necessary, as FCGX_OpenSocket might not return an error, but rather ungracefully exit */
    int fd = open(path_str.c_str(), O_CREAT, 0644);
    if (fd < 0) {
      int err = errno;
      /* ENXIO is actually expected, we'll get that if we try to open a unix domain socket */
      if (err != ENXIO) {
        dout(0) << "ERROR: cannot create socket: path=" << path_str << " error=" << cpp_strerror(err) << dendl;
        return;
      }
    } else {
      close(fd);
    }

    const char *path = path_str.c_str();
    sock_fd = FCGX_OpenSocket(path, SOCKET_BACKLOG);
    if (sock_fd < 0) {
      dout(0) << "ERROR: FCGX_OpenSocket (" << path << ") returned " << sock_fd << dendl;
      return;
    }
    if (chmod(path, 0777) < 0) {
      dout(0) << "WARNING: couldn't set permissions on unix domain socket" << dendl;
    }
  } else if (!g_conf->rgw_port.empty()) {
    string bind = g_conf->rgw_host + ":" + g_conf->rgw_port;
    sock_fd = FCGX_OpenSocket(bind.c_str(), SOCKET_BACKLOG);
    if (sock_fd < 0) {
      dout(0) << "ERROR: FCGX_OpenSocket (" << bind.c_str() << ") returned " << sock_fd << dendl;
      return;
    }
  }

  m_tp.start();

  for (;;) {
    RGWRequest *req = new RGWRequest;
    req->id = ++max_req_id;
    dout(10) << "allocated request req=" << hex << req << dec << dendl;
    FCGX_InitRequest(&req->fcgx, sock_fd, 0);
    req_throttle.get(1);
    int ret = FCGX_Accept_r(&req->fcgx);
    if (ret < 0) {
      delete req;
      dout(0) << "ERROR: FCGX_Accept_r returned " << ret << dendl;
      req_throttle.put(1);
      break;
    }

    req_wq.queue(req);
  }

  m_tp.stop();
}

static void godown_handler(int signum)
{
  FCGX_ShutdownPending();
  pprocess->close_fd();
  signal(signum, sighandler_usr1);
  uint64_t secs = g_ceph_context->_conf->rgw_exit_timeout_secs;
  if (secs)
    alarm(secs);
}

static void godown_alarm(int signum)
{
  _exit(0);
}

static int call_log_intent(RGWRados *store, void *ctx, rgw_obj& obj, RGWIntentEvent intent)
{
  struct req_state *s = (struct req_state *)ctx;
  return rgw_log_intent(store, s, obj, intent);
}

void RGWProcess::handle_request(RGWRequest *req)
{
  FCGX_Request *fcgx = &req->fcgx;
  int ret;
  RGWEnv rgw_env;
  RGWFCGX client_io(fcgx);

  req->log_init();

  dout(1) << "====== starting new request req=" << hex << req << dec << " =====" << dendl;
  perfcounter->inc(l_rgw_req);

  rgw_env.init(g_ceph_context, fcgx->envp);

  struct req_state *s = req->init_state(g_ceph_context, &rgw_env);
  s->obj_ctx = store->create_context(s);
  store->set_intent_cb(s->obj_ctx, call_log_intent);

  s->req_id = store->unique_id(req->id);

  req->log(s, "initializing");

  RGWOp *op = NULL;
  int init_error = 0;
  bool should_log = false;
  RGWRESTMgr *mgr;
  RGWHandler *handler = rest->get_handler(store, s, &client_io, &mgr, &init_error);
  if (init_error != 0) {
    abort_early(s, init_error);
    goto done;
  }

  should_log = mgr->get_logging();

  req->log(s, "getting op");
  op = handler->get_op(store);
  if (!op) {
    abort_early(s, -ERR_METHOD_NOT_ALLOWED);
    goto done;
  }
  req->op = op;

  req->log(s, "authorizing");
  ret = handler->authorize();
  if (ret < 0) {
    dout(10) << "failed to authorize request" << dendl;
    abort_early(s, ret);
    goto done;
  }

  if (s->user.suspended) {
    dout(10) << "user is suspended, uid=" << s->user.user_id << dendl;
    abort_early(s, -ERR_USER_SUSPENDED);
    goto done;
  }
  req->log(s, "reading permissions");
  ret = handler->read_permissions(op);
  if (ret < 0) {
    abort_early(s, ret);
    goto done;
  }

  req->log(s, "verifying op mask");
  ret = op->verify_op_mask();
  if (ret < 0) {
    abort_early(s, ret);
    goto done;
  }

  req->log(s, "verifying op permissions");
  ret = op->verify_permission();
  if (ret < 0) {
    if (s->system_request) {
      dout(2) << "overriding permissions due to system operation" << dendl;
    } else {
      abort_early(s, ret);
      goto done;
    }
  }

  req->log(s, "verifying op params");
  ret = op->verify_params();
  if (ret < 0) {
    abort_early(s, ret);
    goto done;
  }

  if (s->expect_cont)
    dump_continue(s);

  req->log(s, "executing");
  op->execute();
  op->complete();
done:
  if (should_log) {
    rgw_log_op(store, s, (op ? op->name() : "unknown"), olog);
  }

  int http_ret = s->err.http_ret;

  req->log_format(s, "http status=%d", http_ret);

  if (handler)
    handler->put_op(op);
  rest->put_handler(handler);
  store->destroy_context(s->obj_ctx);
  FCGX_Finish_r(fcgx);

  dout(1) << "====== req done req=" << hex << req << dec << " http_status=" << http_ret << " ======" << dendl;
  delete req;
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
  cerr << "   --rgw-region=<region>     region in which radosgw runs\n";
  cerr << "   --rgw-zone=<zone>         zone in which radosgw runs\n";
  generic_server_usage();
  return 0;
}

static RGWRESTMgr *set_logging(RGWRESTMgr *mgr)
{
  mgr->set_logging(true);
  return mgr;
}

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
  def_args.push_back("--debug-rgw=20");
  def_args.push_back("--keyring=$rgw_data/keyring");
  def_args.push_back("--log-file=/var/log/radosgw/$cluster-$name");

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  global_init(&def_args, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_DAEMON,
	      CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);

  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ++i) {
    if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      return 0;
    }
  }

  check_curl();

  if (g_conf->daemonize) {
    if (g_conf->rgw_socket_path.empty() and g_conf->rgw_port.empty()) {
      cerr << "radosgw: must specify 'rgw socket path' or 'rgw port' to run as a daemon" << std::endl;
      exit(1);
    }

    global_init_daemonize(g_ceph_context, 0);
  }
  Mutex mutex("main");
  SafeTimer init_timer(g_ceph_context, mutex);
  init_timer.init();
  mutex.Lock();
  init_timer.add_event_after(g_conf->rgw_init_timeout, new C_InitTimeout);
  mutex.Unlock();

  common_init_finish(g_ceph_context);

  rgw_tools_init(g_ceph_context);

  rgw_init_resolver();
  rgw_rest_init(g_ceph_context);
  
  curl_global_init(CURL_GLOBAL_ALL);
  
  sighandler_usr1 = signal(SIGUSR1, godown_handler);
  sighandler_alrm = signal(SIGALRM, godown_alarm);
  
  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);

  FCGX_Init();

  sighandler_term = signal(SIGTERM, godown_alarm);
  

  int r = 0;
  RGWRados *store = RGWStoreManager::get_storage(g_ceph_context, true);
  if (!store) {
    derr << "Couldn't init storage provider (RADOS)" << dendl;
    r = EIO;
  }
  if (!r)
    r = rgw_perf_start(g_ceph_context);

  mutex.Lock();
  init_timer.cancel_all_events();
  init_timer.shutdown();
  mutex.Unlock();

  if (r) 
    return 1;

  rgw_user_init(store->meta_mgr);
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

  if (apis_map.count("s3") > 0)
    rest.register_default_mgr(set_logging(new RGWRESTMgr_S3));

  if (apis_map.count("swift") > 0) {
    do_swift = true;
    swift_init(g_ceph_context);
    rest.register_resource(g_conf->rgw_swift_url_prefix, set_logging(new RGWRESTMgr_SWIFT));
  }

  if (apis_map.count("swift_auth") > 0)
    rest.register_resource(g_conf->rgw_swift_auth_entry, set_logging(new RGWRESTMgr_SWIFT_Auth));

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
    rest.register_resource(g_conf->rgw_admin_entry, admin_resource);
  }

  OpsLogSocket *olog = NULL;

  if (!g_conf->rgw_ops_log_socket_path.empty()) {
    olog = new OpsLogSocket(g_ceph_context, g_conf->rgw_ops_log_data_backlog);
    olog->init(g_conf->rgw_ops_log_socket_path);
  }

  pprocess = new RGWProcess(g_ceph_context, store, olog, g_conf->rgw_thread_pool_size, &rest);

  pprocess->run();

  delete pprocess;

  if (do_swift) {
    swift_finalize();
  }

  rgw_log_usage_finalize();

  delete olog;

  rgw_perf_stop(g_ceph_context);

  unregister_async_signal_handler(SIGHUP, sighup_handler);

  RGWStoreManager::close_storage(store);

  rgw_tools_cleanup();
  curl_global_cleanup();
  g_ceph_context->put();

  shutdown_async_signal_handler();

  ceph::crypto::shutdown();

  return 0;
}

