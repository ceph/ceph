// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/errno.h"
#include "common/Throttle.h"
#include "common/WorkQueue.h"

#include "rgw_rados.h"
#include "rgw_rest.h"
#include "rgw_frontend.h"
#include "rgw_request.h"
#include "rgw_process.h"
#include "rgw_loadgen.h"
#include "rgw_client_io.h"

#define dout_subsys ceph_subsys_rgw

void RGWProcess::RGWWQ::_dump_queue()
{
  if (!g_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
    return;
  }
  deque<RGWRequest *>::iterator iter;
  if (process->m_req_queue.empty()) {
    dout(20) << "RGWWQ: empty" << dendl;
    return;
  }
  dout(20) << "RGWWQ:" << dendl;
  for (iter = process->m_req_queue.begin();
       iter != process->m_req_queue.end(); ++iter) {
    dout(20) << "req: " << hex << *iter << dec << dendl;
  }
} /* RGWProcess::RGWWQ::_dump_queue */

void RGWFCGXProcess::run()
{
  string socket_path;
  string socket_port;
  string socket_host;

  conf->get_val("socket_path", "", &socket_path);
  conf->get_val("socket_port", g_conf->rgw_port, &socket_port);
  conf->get_val("socket_host", g_conf->rgw_host, &socket_host);

  if (socket_path.empty() && socket_port.empty() && socket_host.empty()) {
    socket_path = g_conf->rgw_socket_path;
    if (socket_path.empty()) {
      dout(0) << "ERROR: no socket server point defined, cannot "
	"start fcgi frontend" << dendl;
      return;
    }
  }

  if (!socket_path.empty()) {
    string path_str = socket_path;

    /* this is necessary, as FCGX_OpenSocket might not return an
	 * error, but rather ungracefully exit */
    int fd = open(path_str.c_str(), O_CREAT, 0644);
    if (fd < 0) {
      int err = errno;
      /* ENXIO is actually expected, we'll get that if we try to open
	   * a unix domain socket */
      if (err != ENXIO) {
		  dout(0) << "ERROR: cannot create socket: path=" << path_str
				  << " error=" << cpp_strerror(err) << dendl;
		  return;
      }
    } else {
		close(fd);
    }

    const char *path = path_str.c_str();
    sock_fd = FCGX_OpenSocket(path, SOCKET_BACKLOG);
    if (sock_fd < 0) {
      dout(0) << "ERROR: FCGX_OpenSocket (" << path << ") returned "
	      << sock_fd << dendl;
      return;
    }
    if (chmod(path, 0777) < 0) {
      dout(0) << "WARNING: couldn't set permissions on unix domain socket"
	      << dendl;
    }
  } else if (!socket_port.empty()) {
    string bind = socket_host + ":" + socket_port;
    sock_fd = FCGX_OpenSocket(bind.c_str(), SOCKET_BACKLOG);
    if (sock_fd < 0) {
      dout(0) << "ERROR: FCGX_OpenSocket (" << bind.c_str() << ") returned "
	      << sock_fd << dendl;
      return;
    }
  }

  m_tp.start();

  FCGX_Request fcgx_reqs[max_connections];

  QueueRing<FCGX_Request*> qr(max_connections);
  for (int i = 0; i < max_connections; i++) {
    FCGX_Request* fcgx = &fcgx_reqs[i];
    FCGX_InitRequest(fcgx, sock_fd, 0);
    qr.enqueue(fcgx);
  }

  for (;;) {
    RGWFCGXRequest* req = new RGWFCGXRequest(store->get_new_req_id(), &qr);
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
} /* RGWFCGXProcess::run */

void RGWFCGXProcess::handle_request(RGWRequest *r)
{
  RGWFCGXRequest *req = static_cast<RGWFCGXRequest *>(r);
  FCGX_Request *fcgx = req->fcgx;
  RGWFCGX client_io(fcgx);

 
  int ret = process_request(store, rest, req, &client_io, olog);
  if (ret < 0) {
    /* we don't really care about return code */
    dout(20) << "process_request() returned " << ret << dendl;
  }

  FCGX_Finish_r(fcgx);

  delete req;
} /* RGWFCGXProcess::handle_request */

void RGWLoadGenProcess::checkpoint()
{
  m_tp.drain(&req_wq);
}

void RGWLoadGenProcess::run()
{
  m_tp.start(); /* start thread pool */

  int i;

  int num_objs;

  conf->get_val("num_objs", 1000, &num_objs);

  int num_buckets;
  conf->get_val("num_buckets", 1, &num_buckets);

  vector<string> buckets(num_buckets);

  atomic_t failed;

  for (i = 0; i < num_buckets; i++) {
    buckets[i] = "/loadgen";
    string& bucket = buckets[i];
    append_rand_alpha(NULL, bucket, bucket, 16);

    /* first create a bucket */
    gen_request("PUT", bucket, 0, &failed);
    checkpoint();
  }

  string *objs = new string[num_objs];

  if (failed.read()) {
    derr << "ERROR: bucket creation failed" << dendl;
    goto done;
  }

  for (i = 0; i < num_objs; i++) {
    char buf[16 + 1];
    gen_rand_alphanumeric(NULL, buf, sizeof(buf));
    buf[16] = '\0';
    objs[i] = buckets[i % num_buckets] + "/" + buf;
  }

  for (i = 0; i < num_objs; i++) {
    gen_request("PUT", objs[i], 4096, &failed);
  }

  checkpoint();

  if (failed.read()) {
    derr << "ERROR: bucket creation failed" << dendl;
    goto done;
  }

  for (i = 0; i < num_objs; i++) {
    gen_request("GET", objs[i], 4096, NULL);
  }

  checkpoint();

  for (i = 0; i < num_objs; i++) {
    gen_request("DELETE", objs[i], 0, NULL);
  }

  checkpoint();

  for (i = 0; i < num_buckets; i++) {
    gen_request("DELETE", buckets[i], 0, NULL);
  }

done:
  checkpoint();

  m_tp.stop();

  delete[] objs;

  signal_shutdown();
} /* RGWLoadGenProcess::run() */

void RGWLoadGenProcess::gen_request(const string& method,
				    const string& resource,
				    int content_length, atomic_t* fail_flag)
{
  RGWLoadGenRequest* req =
    new RGWLoadGenRequest(store->get_new_req_id(), method, resource,
			  content_length, fail_flag);
  dout(10) << "allocated request req=" << hex << req << dec << dendl;
  req_throttle.get(1);
  req_wq.queue(req);
} /* RGWLoadGenProcess::gen_request */

void RGWLoadGenProcess::handle_request(RGWRequest *r)
{
  RGWLoadGenRequest *req = static_cast<RGWLoadGenRequest *>(r);

  RGWLoadGenRequestEnv env;

  utime_t tm = ceph_clock_now(NULL);

  env.port = 80;
  env.content_length = req->content_length;
  env.content_type = "binary/octet-stream";
  env.request_method = req->method;
  env.uri = req->resource;
  env.set_date(tm);
  env.sign(access_key);

  RGWLoadGenIO client_io(&env);

  int ret = process_request(store, rest, req, &client_io, olog);
  if (ret < 0) {
    /* we don't really care about return code */
    dout(20) << "process_request() returned " << ret << dendl;

    if (req->fail_flag) {
      req->fail_flag->inc();
    }
  }

  delete req;
} /* RGWLoadGenProcess::handle_request */

int process_request(RGWRados* store, RGWREST* rest, RGWRequest* req,
		    RGWClientIO* client_io, OpsLogSocket* olog)
{
  int ret = 0;

  client_io->init(g_ceph_context);

  req->log_init();

  dout(1) << "====== starting new request req=" << hex << req << dec
	  << " =====" << dendl;
  perfcounter->inc(l_rgw_req);

  RGWEnv& rgw_env = client_io->get_env();

  struct req_state rstate(g_ceph_context, &rgw_env);

  struct req_state* s = &rstate;

  RGWObjectCtx rados_ctx(store, s);
  s->obj_ctx = &rados_ctx;

  s->req_id = store->unique_id(req->id);
  s->trans_id = store->unique_trans_id(req->id);

  req->log_format(s, "initializing for trans_id = %s", s->trans_id.c_str());

  RGWOp* op = NULL;
  int init_error = 0;
  bool should_log = false;
  RGWRESTMgr* mgr;
  RGWHandler* handler = rest->get_handler(store, s, client_io, &mgr,
					  &init_error);
  if (init_error != 0) {
    abort_early(s, NULL, init_error, NULL);
    goto done;
  }
  dout(10) << "handler=" << typeid(*handler).name() << dendl;

  should_log = mgr->get_logging();

  req->log_format(s, "getting op %d", s->op);
  op = handler->get_op(store);
  if (!op) {
    abort_early(s, NULL, -ERR_METHOD_NOT_ALLOWED, handler);
    goto done;
  }
  req->op = op;
  dout(10) << "op=" << typeid(*op).name() << dendl;

  req->log(s, "authorizing");
  ret = handler->authorize();
  if (ret < 0) {
    dout(10) << "failed to authorize request" << dendl;
    abort_early(s, NULL, ret, handler);
    goto done;
  }

  req->log(s, "normalizing buckets and tenants");
  ret = handler->postauth_init();
  if (ret < 0) {
    dout(10) << "failed to run post-auth init" << dendl;
    abort_early(s, op, ret, handler);
    goto done;
  }

  if (s->user.suspended) {
    dout(10) << "user is suspended, uid=" << s->user.user_id << dendl;
    abort_early(s, op, -ERR_USER_SUSPENDED, handler);
    goto done;
  }

  req->log(s, "init permissions");
  ret = handler->init_permissions(op);
  if (ret < 0) {
    abort_early(s, op, ret, handler);
    goto done;
  }

  /**
   * Only some accesses support website mode, and website mode does NOT apply
   * if you are using the REST endpoint either (ergo, no authenticated access)
   */
  req->log(s, "recalculating target");
  ret = handler->retarget(op, &op);
  if (ret < 0) {
    abort_early(s, op, ret, handler);
    goto done;
  }
  req->op = op;

  req->log(s, "reading permissions");
  ret = handler->read_permissions(op);
  if (ret < 0) {
    abort_early(s, op, ret, handler);
    goto done;
  }

  req->log(s, "init op");
  ret = op->init_processing();
  if (ret < 0) {
    abort_early(s, op, ret, handler);
    goto done;
  }

  req->log(s, "verifying op mask");
  ret = op->verify_op_mask();
  if (ret < 0) {
    abort_early(s, op, ret, handler);
    goto done;
  }

  req->log(s, "verifying op permissions");
  ret = op->verify_permission();
  if (ret < 0) {
    if (s->system_request) {
      dout(2) << "overriding permissions due to system operation" << dendl;
    } else {
      abort_early(s, op, ret, handler);
      goto done;
    }
  }

  req->log(s, "verifying op params");
  ret = op->verify_params();
  if (ret < 0) {
    abort_early(s, op, ret, handler);
    goto done;
  }

  req->log(s, "pre-executing");
  op->pre_exec();

  req->log(s, "executing");
  op->execute();

  req->log(s, "completing");
  op->complete();
done:
  int r = client_io->complete_request();
  if (r < 0) {
    dout(0) << "ERROR: client_io->complete_request() returned " << r << dendl;
  }
  if (should_log) {
    rgw_log_op(store, s, (op ? op->name() : "unknown"), olog);
  }

  int http_ret = s->err.http_ret;

  int op_ret = 0;
  if (op) {
    op_ret = op->get_ret();
  }

  req->log_format(s, "op status=%d", op_ret);
  req->log_format(s, "http status=%d", http_ret);

  if (handler)
    handler->put_op(op);
  rest->put_handler(handler);

  dout(1) << "====== req done req=" << hex << req << dec
	  << " op status=" << op_ret
	  << " http_status=" << http_ret
	  << " ======"
	  << dendl;

  return (ret < 0 ? ret : s->err.ret);
} /* process_request */
