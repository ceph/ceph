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
#include "common/QueueRing.h"
#include "common/safe_io.h"
#include "include/str_list.h"
#include "rgw_common.h"
// #include "rgw_user.h"
#include "rgw_rest.h"
#include "rgw_rest_sts.h"
// #include "rgw_rest_user.h"
// #include "rgw_rest_config.h"
#include "rgw_resolve.h"
#include "rgw_loadgen.h"
#include "rgw_civetweb.h"
#include "rgw_civetweb_log.h"

#include "civetweb/civetweb.h"

#include <map>
#include <string>
#include <vector>
#include <iostream>
#include <sstream>

#include "include/types.h"
#include "common/BackTrace.h"

#define dout_subsys ceph_subsys_sts

using namespace std;

static sig_t sighandler_alrm;

class STSProcess;

static int signal_fd[2] = {0, 0};
static atomic_t disable_signal_fd;

static void signal_shutdown();


#define SOCKET_BACKLOG 1024

struct STSStore {
  int next_id;
  list<string>hostnames;	// XXX "region"...
  STSStore() : next_id(0) { };
  int get_new_req_id() {
    return ++next_id;
  }
};

struct STSRequest
{
  uint64_t id;
  RGWOp *op;		// XXX fixme.
  struct req_state *s;
  string req_str;
  utime_t ts;

  STSRequest(uint64_t id) : id(id), op(NULL), s(NULL) {
  }

  virtual ~STSRequest() {}

  void init_state(req_state *_s) {
    s = _s;
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
    dout(2) << "req " << id << ":" << t << ":" << s->dialect << ":" << req_str <<
// ":" << (op ? op->name() : "") <<
":" << msg << dendl;
  }
};

class STSFrontendConfig {
  string config;
  map<string, string> config_map;
  int parse_config(const string& config, map<string, string>& config_map);
  string framework;
public:
  STSFrontendConfig(const string& _conf) : config(_conf) {}
  int init() {
    int ret = parse_config(config, config_map);
    if (ret < 0)
      return ret;
    return 0;
  }
  bool get_val(const string& key, const string& def_val, string *out);
  bool get_val(const string& key, int def_val, int *out);

  map<string, string>& get_config_map() { return config_map; }

  string get_framework() { return framework; }
};


struct STSFCGXRequest : public STSRequest {
  FCGX_Request *fcgx;
  QueueRing<FCGX_Request *> *qr;

  STSFCGXRequest(uint64_t req_id, QueueRing<FCGX_Request *> *_qr) : STSRequest(req_id), qr(_qr) {
    qr->dequeue(&fcgx);
  }

  ~STSFCGXRequest() {
    FCGX_Finish_r(fcgx);
    qr->enqueue(fcgx);
  }
};

struct STSProcessEnv {
  STSStore *store;
  RGWREST *rest;
  int port;
};

class STSProcess {
  deque<STSRequest *> m_req_queue;
protected:
  STSStore *store;
  ThreadPool m_tp;
  Throttle req_throttle;
  RGWREST *rest;
  STSFrontendConfig *conf;
  int sock_fd;

  struct STSWQ : public ThreadPool::WorkQueue<STSRequest> {
    STSProcess *process;
    STSWQ(STSProcess *p, time_t timeout, time_t suicide_timeout, ThreadPool *tp)
      : ThreadPool::WorkQueue<STSRequest>("STSWQ", timeout, suicide_timeout, tp), process(p) {}

    bool _enqueue(STSRequest *req) {
      process->m_req_queue.push_back(req);
//      perfcounter->inc(l_sts_qlen);
      dout(20) << "enqueued request req=" << hex << req << dec << dendl;
      _dump_queue();
      return true;
    }
    void _dequeue(STSRequest *req) {
      assert(0);
    }
    bool _empty() {
      return process->m_req_queue.empty();
    }
    STSRequest *_dequeue() {
      if (process->m_req_queue.empty())
	return NULL;
      STSRequest *req = process->m_req_queue.front();
      process->m_req_queue.pop_front();
      dout(20) << "dequeued request req=" << hex << req << dec << dendl;
      _dump_queue();
//      perfcounter->inc(l_sts_qlen, -1);
      return req;
    }
    using ThreadPool::WorkQueue<STSRequest>::_process;
    void _process(STSRequest *req) {
//      perfcounter->inc(l_sts_qactive);
      process->handle_request(req);
      process->req_throttle.put(1);
//      perfcounter->inc(l_sts_qactive, -1);
    }
    void _dump_queue() {
      if (!g_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
        return;
      }
      deque<STSRequest *>::iterator iter;
      if (process->m_req_queue.empty()) {
        dout(20) << "STSWQ: empty" << dendl;
        return;
      }
      dout(20) << "STSWQ:" << dendl;
      for (iter = process->m_req_queue.begin(); iter != process->m_req_queue.end(); ++iter) {
        dout(20) << "req: " << hex << *iter << dec << dendl;
      }
    }
    void _clear() {
      assert(process->m_req_queue.empty());
    }
  } req_wq;

public:
  STSProcess(CephContext *cct, STSProcessEnv *pe, int num_threads, STSFrontendConfig *_conf)
    : store(pe->store), m_tp(cct, "STSProcess::m_tp", num_threads),
      req_throttle(cct, "rgw_ops", num_threads * 2),
      rest(pe->rest),
      conf(_conf),
      sock_fd(-1),
      req_wq(this, g_conf->rgw_op_thread_timeout,
	     g_conf->rgw_op_thread_suicide_timeout, &m_tp) {}
  virtual ~STSProcess() {}
  virtual void run() = 0;
  virtual void handle_request(STSRequest *req) = 0;

  void close_fd() {
    if (sock_fd >= 0) {
      ::close(sock_fd);
      sock_fd = -1;
    }
  }
};


class STSFCGXProcess : public STSProcess {
  int max_connections;
public:
  STSFCGXProcess(CephContext *cct, STSProcessEnv *pe, int num_threads, STSFrontendConfig *_conf) :
    STSProcess(cct, pe, num_threads, _conf),
    max_connections(num_threads + (num_threads >> 3)) /* have a bit more connections than threads so that requests
                                                       are still accepted even if we're still processing older requests */
    {}
  void run();
  void handle_request(STSRequest *req);
};

void STSFCGXProcess::run()
{
  string socket_path;
  string socket_port;
  string socket_host;

  conf->get_val("socket_path", "", &socket_path);
  conf->get_val("socket_port", g_conf->sts_port, &socket_port);
  conf->get_val("socket_host", g_conf->sts_host, &socket_host);

  if (socket_path.empty() && socket_port.empty() && socket_host.empty()) {
    socket_path = g_conf->sts_socket_path;
    if (socket_path.empty()) {
      dout(0) << "ERROR: no socket server point defined, cannot start fcgi frontend" << dendl;
      return;
    }
  }

  if (!socket_path.empty()) {
    string path_str = socket_path;

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
  } else if (!socket_port.empty()) {
    string bind = socket_host + ":" + socket_port;
    sock_fd = FCGX_OpenSocket(bind.c_str(), SOCKET_BACKLOG);
    if (sock_fd < 0) {
      dout(0) << "ERROR: FCGX_OpenSocket (" << bind.c_str() << ") returned " << sock_fd << dendl;
      return;
    }
  }

  m_tp.start();

  FCGX_Request fcgx_reqs[max_connections];

  QueueRing<FCGX_Request *> qr(max_connections);
  for (int i = 0; i < max_connections; i++) {
    FCGX_Request *fcgx = &fcgx_reqs[i];
    FCGX_InitRequest(fcgx, sock_fd, 0);
    qr.enqueue(fcgx);
  }

  for (;;) {
    STSFCGXRequest *req = new STSFCGXRequest(store->get_new_req_id(), &qr);
    dout(10) << "allocated request req=" << hex << req << dec << dendl;
    req_throttle.get(1);
    int ret = FCGX_Accept_r(req->fcgx);
    if (ret < 0) {
      delete req;
      dout(0) << "ERROR: FCGX_Accept_r returned " << ret << dendl;
      req_throttle.put(1);
      break;
    }

    req_wq.queue(req);
  }

  m_tp.drain(&req_wq);
  m_tp.stop();

  dout(20) << "cleaning up fcgx connections" << dendl;

  for (int i = 0; i < max_connections; i++) {
    FCGX_Finish_r(&fcgx_reqs[i]);
  }
}

static void signal_shutdown()
{
  if (!disable_signal_fd.read()) {
    int val = 0;
    int ret = write(signal_fd[0], (char *)&val, sizeof(val));
    if (ret < 0) {
      int err = -errno;
      derr << "ERROR: " << __func__ << ": write() returned " << cpp_strerror(-err) << dendl;
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

static int process_request(STSStore *store, RGWREST *rest, STSRequest *req, RGWClientIO *client_io)
{
  int ret = 0;

  client_io->init(g_ceph_context);

  req->log_init();

  dout(1) << "====== starting new request req=" << hex << req << dec << " =====" << dendl;
//  perfcounter->inc(l_sts_req);

  RGWEnv& rgw_env = client_io->get_env();

  struct req_state rstate(g_ceph_context, &rgw_env);

  struct req_state *s = &rstate;

  s->err = new sts_err();

//  s->req_id = store->unique_id(req->id);
//  s->trans_id = store->unique_trans_id(req->id);

  req->log_format(s, "initializing for trans_id = %s", s->trans_id.c_str());

  RGWOp *op = NULL;
  int init_error = 0;
  RGWRESTMgr *mgr;
		// XXX fixme: do something better than (RGWRados*)
  RGWHandler *handler = rest->get_handler((RGWRados*)store, s, client_io, &mgr, &init_error);
  if (init_error != 0) {
    abort_early(s, NULL, init_error);
    goto done;
  }

  req->log(s, "getting op");
  op = handler->get_op((RGWRados *)store);	// XXX this cast is like really wrong...
  if (!op) {
    abort_early(s, NULL, -ERR_METHOD_NOT_ALLOWED);
    goto done;
  }
  req->op = op;

  req->log(s, "authorizing");
  ret = handler->authorize();
  if (ret < 0) {
    dout(10) << "failed to authorize request" << dendl;
    abort_early(s, op->dump_access_control_f(), ret);
    goto done;
  }

  if (s->user.suspended) {
    dout(10) << "user is suspended, uid=" << s->user.user_id << dendl;
    abort_early(s, op->dump_access_control_f(), -ERR_USER_SUSPENDED);
    goto done;
  }
  req->log(s, "reading permissions");
  ret = handler->read_permissions(op);
  if (ret < 0) {
    abort_early(s, op->dump_access_control_f(), ret);
    goto done;
  }

  req->log(s, "init op");
  ret = op->init_processing();
  if (ret < 0) {
    abort_early(s, op->dump_access_control_f(), ret);
    goto done;
  }

  req->log(s, "verifying op mask");
  ret = op->verify_op_mask();
  if (ret < 0) {
    abort_early(s, op->dump_access_control_f(), ret);
    goto done;
  }

  req->log(s, "verifying op permissions");
  ret = op->verify_permission();
  if (ret < 0) {
    if (s->system_request) {
      dout(2) << "overriding permissions due to system operation" << dendl;
    } else {
      abort_early(s, op->dump_access_control_f(), ret);
      goto done;
    }
  }

  req->log(s, "verifying op params");
  ret = op->verify_params();
  if (ret < 0) {
    abort_early(s, op->dump_access_control_f(), ret);
    goto done;
  }

  req->log(s, "executing");
  op->pre_exec();
  op->execute();
  op->complete();
done:
  int r = client_io->complete_request();
  if (r < 0) {
    dout(0) << "ERROR: client_io->complete_request() returned " << r << dendl;
  }

  int http_ret = s->err->http_ret_E;

  req->log_format(s, "http status=%d", http_ret);

  if (handler)
    handler->put_op(op);
  rest->put_handler(handler);

  dout(1) << "====== req done req=" << hex << req << dec << " http_status=" << http_ret << " ======" << dendl;

  return (ret < 0 ? ret : s->err->ret_E);
}

void STSFCGXProcess::handle_request(STSRequest *r)
{
  STSFCGXRequest *req = static_cast<STSFCGXRequest *>(r);
  FCGX_Request *fcgx = req->fcgx;
  RGWFCGX client_io(fcgx);

 
  int ret = process_request(store, rest, req, &client_io);
  if (ret < 0) {
    /* we don't really care about return code */
    dout(20) << "process_request() returned " << ret << dendl;
  }

  FCGX_Finish_r(fcgx);

  delete req;
}

static int civetweb_callback(struct mg_connection *conn) {
  struct mg_request_info *req_info = mg_get_request_info(conn);
  STSProcessEnv *pe = static_cast<STSProcessEnv *>(req_info->user_data);
  RGWREST *rest = pe->rest;

  STSRequest *req = new STSRequest(pe->store->get_new_req_id());
  RGWMongoose client_io(conn, pe->port);

  int ret = process_request(pe->store, rest, req, &client_io);
  if (ret < 0) {
    /* we don't really care about return code */
    dout(20) << "process_request() returned " << ret << dendl;
  }

  delete req;

// Mark as processed
  return 1;
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
  cerr << "  --sts-socket-path=<path>  specify a unix domain socket path\n";
  cerr << "  -m monaddress[:port]      connect to specified monitor\n";
  cerr << "  --logfile=<logfile>       file to log debug output\n";
  cerr << "  --debug-sts=<log-level>/<memory-level>  set radosgw debug level\n";
  generic_server_usage();

  return 0;
}

static RGWRESTMgr *set_logging(RGWRESTMgr *mgr)
{
  mgr->set_logging(true);
  return mgr;
}


int STSFrontendConfig::parse_config(const string& config, map<string, string>& config_map)
{
  list<string> config_list;
  get_str_list(config, " ", config_list);

  list<string>::iterator iter;
  for (iter = config_list.begin(); iter != config_list.end(); ++iter) {
    string& entry = *iter;
    string key;
    string val;

    if (framework.empty()) {
      framework = entry;
      dout(0) << "framework: " << framework << dendl;
      continue;
    }

    ssize_t pos = entry.find('=');
    if (pos < 0) {
      dout(0) << "framework conf key: " << entry << dendl;
      config_map[entry] = "";
      continue;
    }

    int ret = parse_key_value(entry, key, val);
    if (ret < 0) {
      cerr << "ERROR: can't parse " << entry << std::endl;
      return ret;
    }

    dout(0) << "framework conf key: " << key << ", val: " << val << dendl;
    config_map[key] = val;
  }

  return 0;
}


bool STSFrontendConfig::get_val(const string& key, const string& def_val, string *out)
{
 map<string, string>::iterator iter = config_map.find(key);
 if (iter == config_map.end()) {
   *out = def_val;
   return false;
 }

 *out = iter->second;
 return true;
}


bool STSFrontendConfig::get_val(const string& key, int def_val, int *out)
{
  string str;
  bool found = get_val(key, "", &str);
  if (!found) {
    *out = def_val;
    return false;
  }
  string err;
  *out = strict_strtol(str.c_str(), 10, &err);
  if (!err.empty()) {
    cerr << "error parsing int: " << str << ": " << err << std::endl;
    return -EINVAL;
  }
  return 0;
}

class STSFrontend {
public:
  virtual ~STSFrontend() {}

  virtual int init() = 0;

  virtual int run() = 0;
  virtual void stop() = 0;
  virtual void join() = 0;
};

class STSProcessControlThread : public Thread {
  STSProcess *pprocess;
public:
  STSProcessControlThread(STSProcess *_pprocess) : pprocess(_pprocess) {}

  void *entry() {
    pprocess->run();
    return NULL;
  }
};

class STSProcessFrontend : public STSFrontend {
protected:
  STSFrontendConfig *conf;
  STSProcess *pprocess;
  STSProcessEnv env;
  STSProcessControlThread *thread;

public:
  STSProcessFrontend(STSProcessEnv& pe, STSFrontendConfig *_conf) : conf(_conf), pprocess(NULL), env(pe), thread(NULL) {
  }

  ~STSProcessFrontend() {
    delete thread;
    delete pprocess;
  }

  int run() {
    assert(pprocess); /* should have initialized by init() */
    thread = new STSProcessControlThread(pprocess);
    thread->create();
    return 0;
  }

  void stop() {
    pprocess->close_fd();
    thread->kill(SIGUSR1);
  }

  void join() {
    thread->join();
  }
};

class STSFCGXFrontend : public STSProcessFrontend {
public:
  STSFCGXFrontend(STSProcessEnv& pe, STSFrontendConfig *_conf) : STSProcessFrontend(pe, _conf) {}

  int init() {
    pprocess = new STSFCGXProcess(g_ceph_context, &env, g_conf->sts_thread_pool_size, conf);
    return 0;
  }
};

class STSMongooseFrontend : public STSFrontend {
  STSFrontendConfig *conf;
  struct mg_context *ctx;
  STSProcessEnv env;

  void set_conf_default(map<string, string>& m, const string& key, const string& def_val) {
    if (m.find(key) == m.end()) {
      m[key] = def_val;
    }
  }

public:
  STSMongooseFrontend(STSProcessEnv& pe, STSFrontendConfig *_conf) : conf(_conf), ctx(NULL), env(pe) {
  }

  int init() {
    return 0;
  }

  int run() {
    char thread_pool_buf[32];
    snprintf(thread_pool_buf, sizeof(thread_pool_buf), "%d", (int)g_conf->sts_thread_pool_size);
    string port_str;
    map<string, string> conf_map = conf->get_config_map();
    conf->get_val("port", "80", &port_str);
    conf_map.erase("port");
    conf_map["listening_ports"] = port_str;
    set_conf_default(conf_map, "enable_keep_alive", "yes");
    set_conf_default(conf_map, "num_threads", thread_pool_buf);
    set_conf_default(conf_map, "decode_url", "no");

    const char *options[conf_map.size() * 2 + 1];
    int i = 0;
    for (map<string, string>::iterator iter = conf_map.begin(); iter != conf_map.end(); ++iter) {
      options[i] = iter->first.c_str();
      options[i + 1] = iter->second.c_str();
      dout(20)<< "civetweb config: " << options[i] << ": " << (options[i + 1] ? options[i + 1] : "<null>") << dendl;
      i += 2;
    }
    options[i] = NULL;

    struct mg_callbacks cb;
    memset((void *)&cb, 0, sizeof(cb));
    cb.begin_request = civetweb_callback;
    cb.log_message = rgw_civetweb_log_callback;
    cb.log_access = rgw_civetweb_log_access_callback;
    ctx = mg_start(&cb, &env, (const char **)&options);

    if (!ctx) {
      return -EIO;
    }

    return 0;
  }

  void stop() {
    if (ctx) {
      mg_stop(ctx);
    }
  }

  void join() {
  }
};

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
  def_args.push_back("--debug-sts=1/5");
  def_args.push_back("--keyring=$sts_data/keyring");
  def_args.push_back("--log-file=/var/log/radosgw/$cluster-$name.log");

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
    global_init_daemonize(g_ceph_context, 0);
  }
  Mutex mutex("main");
  SafeTimer init_timer(g_ceph_context, mutex);
  init_timer.init();
  mutex.Lock();
  init_timer.add_event_after(g_conf->sts_init_timeout, new C_InitTimeout);
  mutex.Unlock();

  common_init_finish(g_ceph_context);

  rgw_init_resolver();
  
  curl_global_init(CURL_GLOBAL_ALL);
  
  FCGX_Init();

  int r = 0;

  STSStore *store = new STSStore();
  rgw_rest_init(g_ceph_context, store->hostnames);
  r = rgw_perf_start(g_ceph_context);

  mutex.Lock();
  init_timer.cancel_all_events();
  init_timer.shutdown();
  mutex.Unlock();

  if (r) 
    return 1;

  RGWREST rest;

  list<string> apis;

  get_str_list(g_conf->rgw_enable_apis, apis);

  map<string, bool> apis_map;
  for (list<string>::iterator li = apis.begin(); li != apis.end(); ++li) {
    apis_map[*li] = true;
  }

/* XXX todo: check: radosgw - does it work if only "s3" is regsitered?
* It "shouldn't" even though maybe it should.
* that's because RGWRESTMgr::get_resource_mgr()
* checks resources_by_size.empty()
* and does NOT return default_mgr if resources_by_size == 0
* and "s3" doesn't register any resources; only
* "swift", "admin", "swift_auth" do that. 
*/
  if (apis_map.count("sts") > 0) {
    rest.register_default_mgr(set_logging(new RGWRESTMgr_STS));
    rest.register_resource("stupid", set_logging(new RGWRESTMgr_STS));
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

  list<string> frontends;
  get_str_list(g_conf->rgw_frontends, ",", frontends);

  multimap<string, STSFrontendConfig *> fe_map;
  list<STSFrontendConfig *> configs;
  if (frontends.empty()) {
    frontends.push_back("fastcgi");
  }
  for (list<string>::iterator iter = frontends.begin(); iter != frontends.end(); ++iter) {
    string& f = *iter;

    STSFrontendConfig *config = new STSFrontendConfig(f);
    int r = config->init();
    if (r < 0) {
      cerr << "ERROR: failed to init config: " << f << std::endl;
      return EINVAL;
    }

    configs.push_back(config);

    string framework = config->get_framework();
    fe_map.insert(pair<string, STSFrontendConfig*>(framework, config));
  }

  list<STSFrontend *> fes;

  for (multimap<string, STSFrontendConfig *>::iterator fiter = fe_map.begin(); fiter != fe_map.end(); ++fiter) {
    STSFrontendConfig *config = fiter->second;
    string framework = config->get_framework();
    STSFrontend *fe;
    if (framework == "fastcgi" || framework == "fcgi") {
      STSProcessEnv fcgi_pe = { store, &rest, 0 };

      fe = new STSFCGXFrontend(fcgi_pe, config);
    } else if (framework == "civetweb" || framework == "mongoose") {
      int port;
      config->get_val("port", 80, &port);

      STSProcessEnv env = { store, &rest, port };

      fe = new STSMongooseFrontend(env, config);
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
    fe->run();

    fes.push_back(fe);
  }

  wait_shutdown();

  derr << "shutting down" << dendl;

  for (list<STSFrontend *>::iterator liter = fes.begin(); liter != fes.end(); ++liter) {
    STSFrontend *fe = *liter;
    fe->stop();
  }

  for (list<STSFrontend *>::iterator liter = fes.begin(); liter != fes.end(); ++liter) {
    STSFrontend *fe = *liter;
    fe->join();
    delete fe;
  }

  for (list<STSFrontendConfig *>::iterator liter = configs.begin(); liter != configs.end(); ++liter) {
    STSFrontendConfig *fec = *liter;
    delete fec;
  }

  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGTERM, handle_sigterm);
  unregister_async_signal_handler(SIGINT, handle_sigterm);
  unregister_async_signal_handler(SIGUSR1, handle_sigterm);
  shutdown_async_signal_handler();

//  rgw_log_usage_finalize();

  rgw_shutdown_resolver();
  curl_global_cleanup();

  dout(1) << "final shutdown" << dendl;
  g_ceph_context->put();

  signal_fd_finalize();

  return 0;
}

