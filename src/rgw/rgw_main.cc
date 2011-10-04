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

#include "fcgiapp.h"

#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "common/Timer.h"
#include "rgw_common.h"
#include "rgw_access.h"
#include "rgw_acl.h"
#include "rgw_user.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_swift.h"
#include "rgw_log.h"

#include <map>
#include <string>
#include <vector>
#include <iostream>
#include <sstream>

#include "include/types.h"
#include "common/BackTrace.h"

using namespace std;

static sighandler_t sighandler_usr1;
static sighandler_t sighandler_alrm;


#define SOCKET_BACKLOG 20

static void godown_handler(int signum)
{
  FCGX_ShutdownPending();
  signal(signum, sighandler_usr1);
  alarm(5);
}

static void godown_alarm(int signum)
{
  _exit(0);
}

class RGWProcess {
  deque<FCGX_Request *> m_fcgx_queue;
  ThreadPool m_tp;

  struct RGWWQ : public ThreadPool::WorkQueue<FCGX_Request> {
    RGWProcess *process;
    RGWWQ(RGWProcess *p, time_t timeout, time_t suicide_timeout, ThreadPool *tp)
      : ThreadPool::WorkQueue<FCGX_Request>("RGWWQ", timeout, suicide_timeout, tp), process(p) {}

    bool _enqueue(FCGX_Request *req) {
      process->m_fcgx_queue.push_back(req);
      RGW_LOG(20) << "enqueued request fcgx=" << hex << req << dec << dendl;
      _dump_queue();
      return true;
    }
    void _dequeue(FCGX_Request *req) {
      assert(0);
    }
    bool _empty() {
      return process->m_fcgx_queue.empty();
    }
    FCGX_Request *_dequeue() {
      if (process->m_fcgx_queue.empty())
	return NULL;
      FCGX_Request *req = process->m_fcgx_queue.front();
      process->m_fcgx_queue.pop_front();
      RGW_LOG(20) << "dequeued request fcgx=" << hex << req << dec << dendl;
      _dump_queue();
      return req;
    }
    void _process(FCGX_Request *fcgx) {
      process->handle_request(fcgx);
    }
    void _dump_queue() {
      deque<FCGX_Request *>::iterator iter;
      if (process->m_fcgx_queue.size() == 0) {
        RGW_LOG(20) << "RGWWQ: empty" << dendl;
        return;
      }
      RGW_LOG(20) << "RGWWQ:" << dendl;
      for (iter = process->m_fcgx_queue.begin(); iter != process->m_fcgx_queue.end(); ++iter) {
        RGW_LOG(20) << "fcgx: " << hex << *iter << dec << dendl;
      }
    }
    void _clear() {
      assert(process->m_fcgx_queue.empty());
    }
  } req_wq;

public:
  RGWProcess(CephContext *cct, int num_threads)
    : m_tp(cct, "RGWProcess::m_tp", num_threads),
      req_wq(this, g_conf->rgw_op_thread_timeout,
	     g_conf->rgw_op_thread_suicide_timeout, &m_tp) {}
  void run();
  void handle_request(FCGX_Request *fcgx);
};

void RGWProcess::run()
{
  int s = 0;
  if (!g_conf->rgw_socket_path.empty()) {
    string path_str = g_conf->rgw_socket_path;
    const char *path = path_str.c_str();
    s = FCGX_OpenSocket(path, SOCKET_BACKLOG);
    if (s < 0) {
      RGW_LOG(0) << "ERROR: FCGX_OpenSocket (" << path << ") returned " << s << dendl;
      return;
    }
    if (chmod(path, 0777) < 0) {
      RGW_LOG(0) << "WARNING: couldn't set permissions on unix domain socket" << dendl;
    }
  }

  m_tp.start();

  for (;;) {
    FCGX_Request *fcgx = new FCGX_Request;
    RGW_LOG(10) << "allocated request fcgx=" << hex << fcgx << dec << dendl;
    FCGX_InitRequest(fcgx, s, 0);
    int ret = FCGX_Accept_r(fcgx);
    if (ret < 0)
      break;

    req_wq.queue(fcgx);
  }

  m_tp.stop();
}

static int call_log_intent(void *ctx, rgw_obj& obj, RGWIntentEvent intent)
{
  struct req_state *s = (struct req_state *)ctx;
  return rgw_log_intent(s, obj, intent);
}

void RGWProcess::handle_request(FCGX_Request *fcgx)
{
  RGWRESTMgr rest;
  int ret;
  RGWEnv rgw_env;

  RGW_LOG(0) << "====== starting new request fcgx=" << hex << fcgx << dec << " =====" << dendl;

  rgw_env.init(fcgx->envp);

  struct req_state *s = new req_state(&rgw_env);
  s->obj_ctx = rgwstore->create_context(s);
  rgwstore->set_intent_cb(s->obj_ctx, call_log_intent);

  RGWOp *op = NULL;
  int init_error = 0;
  RGWHandler *handler = rest.get_handler(s, fcgx, &init_error);

  if (init_error != 0) {
    abort_early(s, init_error);
    goto done;
  }

  ret = handler->authorize();
  if (ret < 0) {
    RGW_LOG(10) << "failed to authorize request" << dendl;
    abort_early(s, ret);
    goto done;
  }
  if (s->user.suspended) {
    RGW_LOG(10) << "user is suspended, uid=" << s->user.user_id << dendl;
    abort_early(s, -ERR_USER_SUSPENDED);
    goto done;
  }
  ret = handler->read_permissions();
  if (ret < 0) {
    abort_early(s, ret);
    goto done;
  }

  op = handler->get_op();
  if (op) {
    ret = op->verify_permission();
    if (ret < 0) {
      abort_early(s, ret);
      goto done;
    }

    if (s->expect_cont)
      dump_continue(s);

    op->execute();
  } else {
    abort_early(s, -ERR_METHOD_NOT_ALLOWED);
  }
done:
  rgw_log_op(s);

  int http_ret = s->err.http_ret;

  handler->put_op(op);
  rgwstore->destroy_context(s->obj_ctx);
  delete s;
  FCGX_Finish_r(fcgx);
  delete fcgx;

  RGW_LOG(0) << "====== req done fcgx=" << hex << fcgx << dec << " http_status=" << http_ret << " ======" << dendl;
}

/*
 * start up the RADOS connection and then handle HTTP messages as they come in
 */
int main(int argc, const char **argv)
{
  curl_global_init(CURL_GLOBAL_ALL);

  // dout() messages will be sent to stderr, but FCGX wants messages on stdout
  // Redirect stderr to stdout.
  TEMP_FAILURE_RETRY(close(STDERR_FILENO));
  if (TEMP_FAILURE_RETRY(dup2(STDOUT_FILENO, STDERR_FILENO) < 0)) {
    int err = errno;
    cout << "failed to redirect stderr to stdout: " << cpp_strerror(err)
	 << std::endl;
    return ENOSYS;
  }

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  global_init(args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  sighandler_usr1 = signal(SIGUSR1, godown_handler);
  sighandler_alrm = signal(SIGALRM, godown_alarm);

  FCGX_Init();

  RGWStoreManager store_manager;

  if (!store_manager.init("rados", g_ceph_context)) {
    derr << "Couldn't init storage provider (RADOS)" << dendl;
    return EIO;
  }

  RGWProcess process(g_ceph_context, g_conf->rgw_thread_pool_size);

  process.run();

  return 0;
}

