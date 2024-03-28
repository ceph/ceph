// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/errno.h"
#include "common/Throttle.h"
#include "common/WorkQueue.h"
#include "include/scope_guard.h"

#include <utility>
#include "rgw_auth_registry.h"
#include "rgw_dmclock_scheduler.h"
#include "rgw_rest.h"
#include "rgw_frontend.h"
#include "rgw_request.h"
#include "rgw_process.h"
#include "rgw_loadgen.h"
#include "rgw_client_io.h"
#include "rgw_opa.h"
#include "rgw_perf_counters.h"
#include "rgw_lua.h"
#include "rgw_lua_request.h"
#include "rgw_tracer.h"
#include "rgw_ratelimit.h"

#include "services/svc_zone_utils.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;
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
  if (s->cct->_conf->subsys.should_gather(ceph_subsys_rgw, 10)) {
    ldpp_dout(op,10) << "scheduling with "
		     << s->cct->_conf.get_val<std::string>("rgw_scheduler_type")
		     << " client=" << static_cast<int>(client)
		     << " cost=" << cost << dendl;
  }
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
  process->handle_request(this, req);
  process->req_throttle.put(1);
  perfcounter->inc(l_rgw_qactive, -1);
}
bool rate_limit(rgw::sal::Driver* driver, req_state* s) {
  // we dont want to limit health check or system or admin requests
  const auto& is_admin_or_system = s->user->get_info();
  if ((s->op_type ==  RGW_OP_GET_HEALTH_CHECK) || is_admin_or_system.admin || is_admin_or_system.system)
    return false;
  std::string userfind;
  RGWRateLimitInfo global_user;
  RGWRateLimitInfo global_bucket;
  RGWRateLimitInfo global_anon;
  RGWRateLimitInfo* bucket_ratelimit;
  RGWRateLimitInfo* user_ratelimit;
  driver->get_ratelimit(global_bucket, global_user, global_anon);
  bucket_ratelimit = &global_bucket;
  user_ratelimit = &global_user;
  s->user->get_id().to_str(userfind);
  userfind = "u" + userfind;
  s->ratelimit_user_name = userfind;
  std::string bucketfind = !rgw::sal::Bucket::empty(s->bucket.get()) ? "b" + s->bucket->get_marker() : "";
  s->ratelimit_bucket_marker = bucketfind;
  const char *method = s->info.method;

  auto iter = s->user->get_attrs().find(RGW_ATTR_RATELIMIT);
  if(iter != s->user->get_attrs().end()) {
    try {
      RGWRateLimitInfo user_ratelimit_temp;
      bufferlist& bl = iter->second;
      auto biter = bl.cbegin();
      decode(user_ratelimit_temp, biter);
      // override global rate limiting only if local rate limiting is enabled
      if (user_ratelimit_temp.enabled)
        *user_ratelimit = user_ratelimit_temp;
    } catch (buffer::error& err) {
      ldpp_dout(s, 0) << "ERROR: failed to decode rate limit" << dendl;
      return -EIO;
    }
  }
  if (s->user->get_id().id == RGW_USER_ANON_ID && global_anon.enabled) {
    *user_ratelimit = global_anon;
  }
  bool limit_bucket = false;
  bool limit_user = s->ratelimit_data->should_rate_limit(method, s->ratelimit_user_name, s->time, user_ratelimit);

  if(!rgw::sal::Bucket::empty(s->bucket.get()))
  {
    iter = s->bucket->get_attrs().find(RGW_ATTR_RATELIMIT);
    if(iter != s->bucket->get_attrs().end()) {
      try {
        RGWRateLimitInfo bucket_ratelimit_temp;
        bufferlist& bl = iter->second;
        auto biter = bl.cbegin();
        decode(bucket_ratelimit_temp, biter);
        // override global rate limiting only if local rate limiting is enabled
        if (bucket_ratelimit_temp.enabled)
          *bucket_ratelimit = bucket_ratelimit_temp;
      } catch (buffer::error& err) {
        ldpp_dout(s, 0) << "ERROR: failed to decode rate limit" << dendl;
        return -EIO;
      }
    }
    if (!limit_user) {
      limit_bucket = s->ratelimit_data->should_rate_limit(method, s->ratelimit_bucket_marker, s->time, bucket_ratelimit);
    }
  }
  if(limit_bucket && !limit_user) {
    s->ratelimit_data->giveback_tokens(method, s->ratelimit_user_name);
  }
  s->user_ratelimit = *user_ratelimit;
  s->bucket_ratelimit = *bucket_ratelimit;
  return (limit_user || limit_bucket);
}

int rgw_process_authenticated(RGWHandler_REST * const handler,
                              RGWOp *& op,
                              RGWRequest * const req,
                              req_state * const s,
			                        optional_yield y,
                              rgw::sal::Driver* driver,
                              const bool skip_retarget)
{
  ldpp_dout(op, 2) << "init permissions" << dendl;
  int ret = handler->init_permissions(op, y);
  if (ret < 0) {
    return ret;
  }

  /**
   * Only some accesses support website mode, and website mode does NOT apply
   * if you are using the REST endpoint either (ergo, no authenticated access)
   */
  if (! skip_retarget) {
    ldpp_dout(op, 2) << "recalculating target" << dendl;
    ret = handler->retarget(op, &op, y);
    if (ret < 0) {
      return ret;
    }
    req->op = op;
  } else {
    ldpp_dout(op, 2) << "retargeting skipped because of SubOp mode" << dendl;
  }

  /* If necessary extract object ACL and put them into req_state. */
  ldpp_dout(op, 2) << "reading permissions" << dendl;
  ret = handler->read_permissions(op, y);
  if (ret < 0) {
    return ret;
  }

  ldpp_dout(op, 2) << "init op" << dendl;
  ret = op->init_processing(y);
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
  {
    auto span = tracing::rgw::tracer.add_span("verify_permission", s->trace);
    std::swap(span, s->trace);
    ret = op->verify_permission(y);
    std::swap(span, s->trace);
  }
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

  ldpp_dout(op, 2) << "check rate limiting" << dendl;
  if (rate_limit(driver, s)) {
    return -ERR_RATE_LIMITED;
  }
  ldpp_dout(op, 2) << "executing" << dendl;
  {
    auto span = tracing::rgw::tracer.add_span("execute", s->trace);
    std::swap(span, s->trace);
    op->execute(y);
    std::swap(span, s->trace);
  }

  ldpp_dout(op, 2) << "completing" << dendl;
  op->complete();

  return 0;
}

int process_request(const RGWProcessEnv& penv,
                    RGWRequest* const req,
                    const std::string& frontend_prefix,
                    RGWRestfulIO* const client_io,
                    optional_yield yield,
		    rgw::dmclock::Scheduler *scheduler,
                    string* user,
                    ceph::coarse_real_clock::duration* latency,
                    int* http_ret)
{
  int ret = client_io->init(g_ceph_context);
  dout(1) << "====== starting new request req=" << hex << req << dec
	  << " =====" << dendl;
  perfcounter->inc(l_rgw_req);

  RGWEnv& rgw_env = client_io->get_env();

  req_state rstate(g_ceph_context, penv, &rgw_env, req->id);
  req_state *s = &rstate;

  s->ratelimit_data = penv.ratelimiting->get_active();

  rgw::sal::Driver* driver = penv.driver;
  std::unique_ptr<rgw::sal::User> u = driver->get_user(rgw_user());
  s->set_user(u);

  if (ret < 0) {
    s->cio = client_io;
    abort_early(s, nullptr, ret, nullptr, yield);
    return ret;
  }

  s->req_id = driver->zone_unique_id(req->id);
  s->trans_id = driver->zone_unique_trans_id(req->id);
  s->host_id = driver->get_host_id();
  s->yield = yield;

  ldpp_dout(s, 2) << "initializing for trans_id = " << s->trans_id << dendl;

  RGWOp* op = nullptr;
  int init_error = 0;
  bool should_log = false;
  RGWREST* rest = penv.rest;
  RGWRESTMgr *mgr;
  RGWHandler_REST *handler = rest->get_handler(driver, s,
                                               *penv.auth_registry,
                                               frontend_prefix,
                                               client_io, &mgr, &init_error);
  rgw::dmclock::SchedulerCompleter c;

  if (init_error != 0) {
    abort_early(s, nullptr, init_error, nullptr, yield);
    goto done;
  }
  ldpp_dout(s, 10) << "handler=" << typeid(*handler).name() << dendl;

  should_log = mgr->get_logging();

  ldpp_dout(s, 2) << "getting op " << s->op << dendl;
  op = handler->get_op();
  if (!op) {
    abort_early(s, NULL, -ERR_METHOD_NOT_ALLOWED, handler, yield);
    goto done;
  }
  {
    s->trace_enabled = tracing::rgw::tracer.is_enabled();
    std::string script;
    auto rc = rgw::lua::read_script(s, penv.lua.manager.get(), s->bucket_tenant, s->yield, rgw::lua::context::preRequest, script);
    if (rc == -ENOENT) {
      // no script, nothing to do
    } else if (rc < 0) {
      ldpp_dout(op, 5) << "WARNING: failed to read pre request script. error: " << rc << dendl;
    } else {
      rc = rgw::lua::request::execute(driver, rest, penv.olog, s, op, script);
      if (rc < 0) {
        ldpp_dout(op, 5) << "WARNING: failed to execute pre request script. error: " << rc << dendl;
      }
    }
  }
  std::tie(ret,c) = schedule_request(scheduler, s, op);
  if (ret < 0) {
    if (ret == -EAGAIN) {
      ret = -ERR_RATE_LIMITED;
    }
    ldpp_dout(op,0) << "Scheduling request failed with " << ret << dendl;
    abort_early(s, op, ret, handler, yield);
    goto done;
  }
  req->op = op;
  ldpp_dout(op, 10) << "op=" << typeid(*op).name() << dendl;
  s->op_type = op->get_type();

  try {
    ldpp_dout(op, 2) << "verifying requester" << dendl;
    ret = op->verify_requester(*penv.auth_registry, yield);
    if (ret < 0) {
      dout(10) << "failed to authorize request" << dendl;
      abort_early(s, op, ret, handler, yield);
      goto done;
    }

    /* FIXME: remove this after switching all handlers to the new authentication
     * infrastructure. */
    ldpp_dout(op, 2) << "Transform old auth info" << dendl;
    if (nullptr == s->auth.identity) {
      s->auth.identity = rgw::auth::transform_old_authinfo(s);
    }

    ldpp_dout(op, 2) << "normalizing buckets and tenants" << dendl;
    ret = handler->postauth_init(yield);
    if (ret < 0) {
      dout(10) << "failed to run post-auth init" << dendl;
      abort_early(s, op, ret, handler, yield);
      goto done;
    }

    if (s->user->get_info().suspended) {
      dout(10) << "user is suspended, uid=" << s->user->get_id() << dendl;
      abort_early(s, op, -ERR_USER_SUSPENDED, handler, yield);
      goto done;
    }

    s->trace = tracing::rgw::tracer.start_trace(op->name(), s->trace_enabled);
    s->trace->SetAttribute(tracing::rgw::TRANS_ID, s->trans_id);

    ret = rgw_process_authenticated(handler, op, req, s, yield, driver);
    if (ret < 0) {
      abort_early(s, op, ret, handler, yield);
      goto done;
    }
  } catch (const ceph::crypto::DigestException& e) {
    dout(0) << "authentication failed" << e.what() << dendl;
    abort_early(s, op, -ERR_INVALID_SECRET_KEY, handler, yield);
  }

done:
  if (op) {
    if (s->trace) {
      s->trace->SetAttribute(tracing::rgw::OP_RESULT, op->get_ret());
      s->trace->SetAttribute(tracing::rgw::HOST_ID, driver->get_host_id());

      if (!rgw::sal::User::empty(s->user)) {
        s->trace->SetAttribute(tracing::rgw::USER_ID, s->user->get_id().id);
      }
      if (!rgw::sal::Bucket::empty(s->bucket)) {
        s->trace->SetAttribute(tracing::rgw::BUCKET_NAME, s->bucket->get_name());
      }
      if (!rgw::sal::Object::empty(s->object)) {
        s->trace->SetAttribute(tracing::rgw::OBJECT_NAME, s->object->get_name());
      }
    }
    std::string script;
    auto rc = rgw::lua::read_script(s, penv.lua.manager.get(), s->bucket_tenant, s->yield, rgw::lua::context::postRequest, script);
    if (rc == -ENOENT) {
      // no script, nothing to do
    } else if (rc < 0) {
      ldpp_dout(op, 5) << "WARNING: failed to read post request script. error: " << rc << dendl;
    } else {
      rc = rgw::lua::request::execute(driver, rest, penv.olog, s, op, script);
      if (rc < 0) {
        ldpp_dout(op, 5) << "WARNING: failed to execute post request script. error: " << rc << dendl;
      }
    }
  }

  try {
    client_io->complete_request();
  } catch (rgw::io::Exception& e) {
    dout(0) << "ERROR: client_io->complete_request() returned "
            << e.what() << dendl;
  }
  if (should_log) {
    rgw_log_op(rest, s, op, penv.olog);
  }

  if (http_ret != nullptr) {
    *http_ret = s->err.http_ret;
  }
  int op_ret = 0;

  if (user && !rgw::sal::User::empty(s->user.get())) {
    *user = s->user->get_id().to_str();
  }

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

  const auto lat = s->time_elapsed();
  if (latency) {
    *latency = lat;
  }
  dout(1) << "====== req done req=" << hex << req << dec
	  << " op status=" << op_ret
	  << " http_status=" << s->err.http_ret
	  << " latency=" << lat
	  << " ======"
	  << dendl;

  return (ret < 0 ? ret : s->err.ret);
} /* process_request */
