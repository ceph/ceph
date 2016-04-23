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

int process_request(RGWRados* store, RGWREST* rest, RGWRequest* req,
		    RGWStreamIO* client_io, OpsLogSocket* olog)
{
  int ret = 0;

  client_io->init(g_ceph_context);

  req->log_init();

  dout(1) << "====== starting new request req=" << hex << req << dec
	  << " =====" << dendl;
  perfcounter->inc(l_rgw_req);

  RGWEnv& rgw_env = client_io->get_env();

  RGWUserInfo userinfo;

  struct req_state rstate(g_ceph_context, &rgw_env, &userinfo);
  struct req_state *s = &rstate;

  RGWObjectCtx rados_ctx(store, s);
  s->obj_ctx = &rados_ctx;

  s->req_id = store->unique_id(req->id);
  s->trans_id = store->unique_trans_id(req->id);
  s->host_id = store->host_id;

  req->log_format(s, "initializing for trans_id = %s", s->trans_id.c_str());

  RGWOp* op = NULL;
  int init_error = 0;
  bool should_log = false;
  RGWRESTMgr *mgr;
  RGWHandler_REST *handler = rest->get_handler(store, s, client_io, &mgr,
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

  s->op_type = op->get_type();

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

  if (s->user->suspended) {
    dout(10) << "user is suspended, uid=" << s->user->user_id << dendl;
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
    } else if (s->admin_request) {
      dout(2) << "overriding permissions due to admin operation" << dendl;
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
