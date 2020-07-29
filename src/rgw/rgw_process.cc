// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/errno.h"
#include "common/Throttle.h"
#include "common/WorkQueue.h"
#include "include/scope_guard.h"

#include "rgw_rados.h"
#include "rgw_dmclock_scheduler.h"
#include "rgw_rest.h"
#include "rgw_frontend.h"
#include "rgw_request.h"
#include "rgw_process.h"
#include "rgw_loadgen.h"
#include "rgw_client_io.h"
#include "rgw_opa.h"
#include "rgw_perf_counters.h"

#include "services/svc_zone_utils.h"

#define dout_subsys ceph_subsys_rgw

using rgw::dmclock::Scheduler;

void RGWProcess::RGWWQ::_dump_queue()
{
  if (!g_conf()->subsys.should_gather<ceph_subsys_rgw, 20>()) {
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

auto schedule_request(Scheduler *scheduler, req_state *s, RGWOp *op)
{
  using rgw::dmclock::SchedulerCompleter;
  if (!scheduler)
    return std::make_pair(0,SchedulerCompleter{});

  const auto client = op->dmclock_client();
  const auto cost = op->dmclock_cost();
  ldpp_dout(op,10) << "scheduling with dmclock client=" << static_cast<int>(client)
		   << " cost=" << cost << dendl;
  return scheduler->schedule_request(client, {},
                                     req_state::Clock::to_double(s->time),
                                     cost,
                                     s->yield);
}

bool RGWProcess::RGWWQ::_enqueue(RGWRequest* req) {
  process->m_req_queue.push_back(req);
  perfcounter->inc(l_rgw_qlen);
  dout(20) << "enqueued request req=" << hex << req << dec << dendl;
  _dump_queue();
  return true;
}

RGWRequest* RGWProcess::RGWWQ::_dequeue() {
  if (process->m_req_queue.empty())
    return NULL;
  RGWRequest *req = process->m_req_queue.front();
  process->m_req_queue.pop_front();
  dout(20) << "dequeued request req=" << hex << req << dec << dendl;
  _dump_queue();
  perfcounter->inc(l_rgw_qlen, -1);
  return req;
}

void RGWProcess::RGWWQ::_process(RGWRequest *req, ThreadPool::TPHandle &) {
  perfcounter->inc(l_rgw_qactive);
  process->handle_request(req);
  process->req_throttle.put(1);
  perfcounter->inc(l_rgw_qactive, -1);
}

int rgw_process_authenticated(RGWHandler_REST * const handler,
                              RGWOp *& op,
                              RGWRequest * const req,
                              req_state * const s,
                              const bool skip_retarget)
{
  ldpp_dout(op, 2) << "init permissions" << dendl;
  int ret = handler->init_permissions(op);
  if (ret < 0) {
    return ret;
  }

  /**
   * Only some accesses support website mode, and website mode does NOT apply
   * if you are using the REST endpoint either (ergo, no authenticated access)
   */
  if (! skip_retarget) {
    ldpp_dout(op, 2) << "recalculating target" << dendl;
    ret = handler->retarget(op, &op);
    if (ret < 0) {
      return ret;
    }
    req->op = op;
  } else {
    ldpp_dout(op, 2) << "retargeting skipped because of SubOp mode" << dendl;
  }

  /* If necessary extract object ACL and put them into req_state. */
  ldpp_dout(op, 2) << "reading permissions" << dendl;
  ret = handler->read_permissions(op);
  if (ret < 0) {
    return ret;
  }

  ldpp_dout(op, 2) << "init op" << dendl;
  ret = op->init_processing();
  if (ret < 0) {
    return ret;
  }

  ldpp_dout(op, 2) << "verifying op mask" << dendl;
  ret = op->verify_op_mask();
  if (ret < 0) {
    return ret;
  }

  /* Check if OPA is used to authorize requests */
  if (s->cct->_conf->rgw_use_opa_authz) {
    ret = rgw_opa_authorize(op, s);
    if (ret < 0) {
      return ret;
    }
  }

  ldpp_dout(op, 2) << "verifying op permissions" << dendl;
  ret = op->verify_permission();
  if (ret < 0) {
    if (s->system_request) {
      dout(2) << "overriding permissions due to system operation" << dendl;
    } else if (s->auth.identity->is_admin_of(s->user->get_id())) {
      dout(2) << "overriding permissions due to admin operation" << dendl;
    } else {
      return ret;
    }
  }

  ldpp_dout(op, 2) << "verifying op params" << dendl;
  ret = op->verify_params();
  if (ret < 0) {
    return ret;
  }

  ldpp_dout(op, 2) << "pre-executing" << dendl;
  op->pre_exec();

  ldpp_dout(op, 2) << "executing" << dendl;
  op->execute();

  ldpp_dout(op, 2) << "completing" << dendl;
  op->complete();

  return 0;
}

int process_request(rgw::sal::RGWRadosStore* const store,
                    RGWREST* const rest,
                    RGWRequest* const req,
                    const std::string& frontend_prefix,
                    const rgw_auth_registry_t& auth_registry,
                    RGWRestfulIO* const client_io,
                    OpsLogSocket* const olog,
                    optional_yield yield,
		    rgw::dmclock::Scheduler *scheduler,
                    int* http_ret)
{
  int ret = client_io->init(g_ceph_context);

  dout(1) << "====== starting new request req=" << hex << req << dec
	  << " =====" << dendl;
  perfcounter->inc(l_rgw_req);

  RGWEnv& rgw_env = client_io->get_env();

  rgw::sal::RGWRadosUser user(store);

  struct req_state rstate(g_ceph_context, &rgw_env, &user, req->id);
  struct req_state *s = &rstate;

  RGWObjectCtx rados_ctx(store, s);
  s->obj_ctx = &rados_ctx;

  auto sysobj_ctx = store->svc()->sysobj->init_obj_ctx();
  s->sysobj_ctx = &sysobj_ctx;

  if (ret < 0) {
    s->cio = client_io;
    abort_early(s, nullptr, ret, nullptr);
    return ret;
  }

  s->req_id = store->svc()->zone_utils->unique_id(req->id);
  s->trans_id = store->svc()->zone_utils->unique_trans_id(req->id);
  s->host_id = store->getRados()->host_id;
  s->yield = yield;

  ldpp_dout(s, 2) << "initializing for trans_id = " << s->trans_id << dendl;

  RGWOp* op = nullptr;
  int init_error = 0;
  bool should_log = false;
  RGWRESTMgr *mgr;
  RGWHandler_REST *handler = rest->get_handler(store, s,
                                               auth_registry,
                                               frontend_prefix,
                                               client_io, &mgr, &init_error);
  rgw::dmclock::SchedulerCompleter c;
  if (init_error != 0) {
    abort_early(s, nullptr, init_error, nullptr);
    goto done;
  }
  dout(10) << "handler=" << typeid(*handler).name() << dendl;

  should_log = mgr->get_logging();

  ldpp_dout(s, 2) << "getting op " << s->op << dendl;
  op = handler->get_op();
  if (!op) {
    abort_early(s, NULL, -ERR_METHOD_NOT_ALLOWED, handler);
    goto done;
  }
  std::tie(ret,c) = schedule_request(scheduler, s, op);
  if (ret < 0) {
    if (ret == -EAGAIN) {
      ret = -ERR_RATE_LIMITED;
    }
    ldpp_dout(op,0) << "Scheduling request failed with " << ret << dendl;
    abort_early(s, op, ret, handler);
    goto done;
  }
  req->op = op;
  dout(10) << "op=" << typeid(*op).name() << dendl;

  s->op_type = op->get_type();

  try {
    ldpp_dout(op, 2) << "verifying requester" << dendl;
    ret = op->verify_requester(auth_registry);
    if (ret < 0) {
      dout(10) << "failed to authorize request" << dendl;
      abort_early(s, op, ret, handler);
      goto done;
    }

    /* FIXME: remove this after switching all handlers to the new authentication
     * infrastructure. */
    if (nullptr == s->auth.identity) {
      s->auth.identity = rgw::auth::transform_old_authinfo(s);
    }

    ldpp_dout(op, 2) << "normalizing buckets and tenants" << dendl;
    ret = handler->postauth_init();
    if (ret < 0) {
      dout(10) << "failed to run post-auth init" << dendl;
      abort_early(s, op, ret, handler);
      goto done;
    }

    if (s->user->get_info().suspended) {
      dout(10) << "user is suspended, uid=" << s->user->get_id() << dendl;
      abort_early(s, op, -ERR_USER_SUSPENDED, handler);
      goto done;
    }

    ret = rgw_process_authenticated(handler, op, req, s);
    if (ret < 0) {
      abort_early(s, op, ret, handler);
      goto done;
    }
  } catch (const ceph::crypto::DigestException& e) {
    dout(0) << "authentication failed" << e.what() << dendl;
    abort_early(s, op, -ERR_INVALID_SECRET_KEY, handler);
  }

done:
  try {
    client_io->complete_request();
  } catch (rgw::io::Exception& e) {
    dout(0) << "ERROR: client_io->complete_request() returned "
            << e.what() << dendl;
  }

  if (should_log) {
    rgw_log_op(store->getRados(), rest, s, (op ? op->name() : "unknown"), olog);
  }

  if (http_ret != nullptr) {
    *http_ret = s->err.http_ret;
  }
  int op_ret = 0;
  if (op) {
    op_ret = op->get_ret();
    ldpp_dout(op, 2) << "op status=" << op_ret << dendl;
    ldpp_dout(op, 2) << "http status=" << s->err.http_ret << dendl;
  } else {
    ldpp_dout(s, 2) << "http status=" << s->err.http_ret << dendl;
  }
  if (handler)
    handler->put_op(op);
  rest->put_handler(handler);

  dout(1) << "====== req done req=" << hex << req << dec
	  << " op status=" << op_ret
	  << " http_status=" << s->err.http_ret
	  << " latency=" << s->time_elapsed()
	  << " ======"
	  << dendl;

  return (ret < 0 ? ret : s->err.ret);
} /* process_request */
