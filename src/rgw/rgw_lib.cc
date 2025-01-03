// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <sys/types.h>
#include <string.h>
#include <chrono>

#include "include/rados/librgw.h"
#include "rgw_acl.h"

#include "include/str_list.h"
#include "global/signal_handler.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/dout.h"

#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_log.h"
#include "rgw_frontend.h"
#include "rgw_request.h"
#include "rgw_process.h"
#include "rgw_auth.h"
#include "rgw_lib.h"
#include "rgw_lib_frontend.h"
#include "rgw_perf_counters.h"
#include "rgw_signal.h"
#include "rgw_main.h"

#include <errno.h>
#include <thread>
#include <string>
#include <mutex>

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw {

  RGWLib* g_rgwlib = nullptr;

  class C_InitTimeout : public Context {
  public:
    C_InitTimeout() {}
    void finish(int r) override {
      derr << "Initialization timeout, failed to initialize" << dendl;
      exit(1);
    }
  };

  void RGWLibProcess::checkpoint()
  {
    m_tp.drain(&req_wq);
  }

#define MIN_EXPIRE_S 120

  void RGWLibProcess::run()
  {
    /* write completion interval */
    RGWLibFS::write_completion_interval_s =
      cct->_conf->rgw_nfs_write_completion_interval_s;

    /* start write timer */
    RGWLibFS::write_timer.resume();

    /* gc loop */
    while (! shutdown) {
      lsubdout(cct, rgw, 5) << "RGWLibProcess GC" << dendl;

      /* dirent invalidate timeout--basically, the upper-bound on
       * inconsistency with the S3 namespace */
      auto expire_s = cct->_conf->rgw_nfs_namespace_expire_secs;

      /* delay between gc cycles */
      auto delay_s = std::max(int64_t(1), std::min(int64_t(MIN_EXPIRE_S), expire_s/2));

      unique_lock uniq(mtx);
    restart:
      int cur_gen = gen;
      for (auto iter = mounted_fs.begin(); iter != mounted_fs.end();
	   ++iter) {
	RGWLibFS* fs = iter->first->ref();
	uniq.unlock();
	fs->gc();
        const DoutPrefix dp(cct, dout_subsys, "librgw: ");
	fs->update_user(&dp);
	fs->rele();
	uniq.lock();
	if (cur_gen != gen)
	  goto restart; /* invalidated */
      }
      cv.wait_for(uniq, std::chrono::seconds(delay_s));
      uniq.unlock();
    }
  }

  void RGWLibProcess::handle_request(const DoutPrefixProvider *dpp, RGWRequest* r)
  {
    /*
     * invariant: valid requests are derived from RGWLibRequest
     */
    RGWLibRequest* req = static_cast<RGWLibRequest*>(r);

    // XXX move RGWLibIO and timing setup into process_request

#if 0 /* XXX */
    utime_t tm = ceph_clock_now();
#endif

    RGWLibIO io_ctx;

    int ret = process_request(req, &io_ctx);
    if (ret < 0) {
      /* we don't really care about return code */
      dout(20) << "process_request() returned " << ret << dendl;

    }
    delete req;
  } /* handle_request */

  int RGWLibProcess::process_request(RGWLibRequest* req)
  {
    // XXX move RGWLibIO and timing setup into process_request

#if 0 /* XXX */
    utime_t tm = ceph_clock_now();
#endif

    RGWLibIO io_ctx;

    int ret = process_request(req, &io_ctx);
    if (ret < 0) {
      /* we don't really care about return code */
      dout(20) << "process_request() returned " << ret << dendl;
    }
    return ret;
  } /* process_request */

  static inline void abort_req(req_state *s, RGWOp *op, int err_no)
  {
    if (!s)
      return;

    /* XXX the dump_errno and dump_bucket_from_state behaviors in
     * the abort_early (rgw_rest.cc) might be valuable, but aren't
     * safe to call presently as they return HTTP data */

    perfcounter->inc(l_rgw_failed_req);
  } /* abort_req */

  int RGWLibProcess::process_request(RGWLibRequest* req, RGWLibIO* io)
  {
    int ret = 0;
    bool should_log = true; // XXX

    dout(1) << "====== " << __func__
	    << " starting new request req=" << hex << req << dec
	    << " ======" << dendl;

    /*
     * invariant: valid requests are derived from RGWOp--well-formed
     * requests should have assigned RGWRequest::op in their descendant
     * constructor--if not, the compiler can find it, at the cost of
     * a runtime check
     */
    RGWOp *op = (req->op) ? req->op : dynamic_cast<RGWOp*>(req);
    if (! op) {
      ldpp_dout(op, 1) << "failed to derive cognate RGWOp (invalid op?)" << dendl;
      return -EINVAL;
    }

    io->init(req->cct);

    perfcounter->inc(l_rgw_req);

    RGWEnv& rgw_env = io->get_env();

    /* XXX
     * until major refactoring of req_state and req_info, we need
     * to build their RGWEnv boilerplate from the RGWLibRequest,
     * pre-staging any strings (HTTP_HOST) that provoke a crash when
     * not found
     */

    /* XXX for now, use "";  could be a legit hostname, or, in future,
     * perhaps a tenant (Yehuda) */
    rgw_env.set("HTTP_HOST", "");

    /* XXX and -then- bloat up req_state with string copies from it */
    req_state rstate(req->cct, env, &rgw_env, req->id);
    req_state *s = &rstate;

    // XXX fix this
    s->cio = io;

    /* XXX and -then- stash req_state pointers everywhere they are needed */
    ret = req->init(rgw_env, env.driver, io, s);
    if (ret < 0) {
      ldpp_dout(op, 10) << "failed to initialize request" << dendl;
      abort_req(s, op, ret);
      goto done;
    }

    s->trace = tracing::rgw::tracer.start_trace(op->name());

    /* req is-a RGWOp, currently initialized separately */
    ret = req->op_init();
    if (ret < 0) {
      dout(10) << "failed to initialize RGWOp" << dendl;
      abort_req(s, op, ret);
      goto done;
    }

    /* now expected by rgw_log_op() */
    rgw_env.set("REQUEST_METHOD", s->info.method);
    rgw_env.set("REQUEST_URI", s->info.request_uri);
    rgw_env.set("QUERY_STRING", "");

    try {
      /* XXX authorize does less here then in the REST path, e.g.,
       * the user's info is cached, but still incomplete */
      ldpp_dout(s, 2) << "authorizing" << dendl;
      ret = req->authorize(op, null_yield);
      if (ret < 0) {
	dout(10) << "failed to authorize request" << dendl;
	abort_req(s, op, ret);
	goto done;
      }

      /* FIXME: remove this after switching all handlers to the new
       * authentication infrastructure. */
      if (! s->auth.identity) {
        auto result = rgw::auth::transform_old_authinfo(
            op, null_yield, env.driver, s->user.get());
        if (!result) {
          ret = result.error();
          abort_req(s, op, ret);
          goto done;
        }
	s->auth.identity = std::move(result).value();
      }

      ldpp_dout(s, 2) << "reading op permissions" << dendl;
      ret = req->read_permissions(op, null_yield);
      if (ret < 0) {
	abort_req(s, op, ret);
	goto done;
      }

      ldpp_dout(s, 2) << "init op" << dendl;
      ret = op->init_processing(null_yield);
      if (ret < 0) {
	abort_req(s, op, ret);
	goto done;
      }

      ldpp_dout(s, 2) << "verifying op mask" << dendl;
      ret = op->verify_op_mask();
      if (ret < 0) {
	abort_req(s, op, ret);
	goto done;
      }

      ldpp_dout(s, 2) << "verifying op permissions" << dendl;
      ret = op->verify_permission(null_yield);
      if (ret < 0) {
	if (s->system_request) {
	  ldpp_dout(op, 2) << "overriding permissions due to system operation" << dendl;
	} else if (s->auth.identity->is_admin_of(s->user->get_id())) {
	  ldpp_dout(op, 2) << "overriding permissions due to admin operation" << dendl;
	} else {
	  abort_req(s, op, ret);
	  goto done;
	}
      }

      ldpp_dout(s, 2) << "verifying op params" << dendl;
      ret = op->verify_params();
      if (ret < 0) {
	abort_req(s, op, ret);
	goto done;
      }

      ldpp_dout(s, 2) << "executing" << dendl;
      op->pre_exec();
      op->execute(null_yield);
      op->complete();

    } catch (const ceph::crypto::DigestException& e) {
      dout(0) << "authentication failed" << e.what() << dendl;
      abort_req(s, op, -ERR_INVALID_SECRET_KEY);
    }

  done:
    try {
      io->complete_request();
    } catch (rgw::io::Exception& e) {
      dout(0) << "ERROR: io->complete_request() returned "
              << e.what() << dendl;
    }
    if (should_log) {
      rgw_log_op(nullptr /* !rest */, s, op, env.olog);
    }

    int http_ret = s->err.http_ret;

    ldpp_dout(s, 2) << "http status=" << http_ret << dendl;

    ldpp_dout(op, 1) << "====== " << __func__
	    << " req done req=" << hex << req << dec << " http_status="
	    << http_ret
	    << " ======" << dendl;

    return (ret < 0 ? ret : s->err.ret);
  } /* process_request */

  int RGWLibProcess::start_request(RGWLibContinuedReq* req)
  {

    dout(1) << "====== " << __func__
	    << " starting new continued request req=" << hex << req << dec
	    << " ======" << dendl;

    /*
     * invariant: valid requests are derived from RGWOp--well-formed
     * requests should have assigned RGWRequest::op in their descendant
     * constructor--if not, the compiler can find it, at the cost of
     * a runtime check
     */
    RGWOp *op = (req->op) ? req->op : dynamic_cast<RGWOp*>(req);
    if (! op) {
      ldpp_dout(op, 1) << "failed to derive cognate RGWOp (invalid op?)" << dendl;
      return -EINVAL;
    }

    req_state* s = req->get_state();
    RGWLibIO& io_ctx = req->get_io();
    RGWEnv& rgw_env = io_ctx.get_env();

    rgw_env.set("HTTP_HOST", "");

    int ret = req->init(rgw_env, env.driver, &io_ctx, s);
    if (ret < 0) {
      ldpp_dout(op, 10) << "failed to initialize request" << dendl;
      abort_req(s, op, ret);
      goto done;
    }

    /* req is-a RGWOp, currently initialized separately */
    ret = req->op_init();
    if (ret < 0) {
      dout(10) << "failed to initialize RGWOp" << dendl;
      abort_req(s, op, ret);
      goto done;
    }

    /* XXX authorize does less here then in the REST path, e.g.,
     * the user's info is cached, but still incomplete */
    ldpp_dout(s, 2) << "authorizing" << dendl;
    ret = req->authorize(op, null_yield);
    if (ret < 0) {
      dout(10) << "failed to authorize request" << dendl;
      abort_req(s, op, ret);
      goto done;
    }

    /* FIXME: remove this after switching all handlers to the new authentication
     * infrastructure. */
    if (! s->auth.identity) {
      auto result = rgw::auth::transform_old_authinfo(
          op, null_yield, env.driver, s->user.get());
      if (!result) {
        ret = result.error();
        abort_req(s, op, ret);
        goto done;
      }
      s->auth.identity = std::move(result).value();
    }

    ldpp_dout(s, 2) << "reading op permissions" << dendl;
    ret = req->read_permissions(op, null_yield);
    if (ret < 0) {
      abort_req(s, op, ret);
      goto done;
    }

    ldpp_dout(s, 2) << "init op" << dendl;
    ret = op->init_processing(null_yield);
    if (ret < 0) {
      abort_req(s, op, ret);
      goto done;
    }

    ldpp_dout(s, 2) << "verifying op mask" << dendl;
    ret = op->verify_op_mask();
    if (ret < 0) {
      abort_req(s, op, ret);
      goto done;
    }

    ldpp_dout(s, 2) << "verifying op permissions" << dendl;
    ret = op->verify_permission(null_yield);
    if (ret < 0) {
      if (s->system_request) {
	ldpp_dout(op, 2) << "overriding permissions due to system operation" << dendl;
      } else if (s->auth.identity->is_admin_of(s->user->get_id())) {
	ldpp_dout(op, 2) << "overriding permissions due to admin operation" << dendl;
      } else {
	abort_req(s, op, ret);
	goto done;
      }
    }

    ldpp_dout(s, 2) << "verifying op params" << dendl;
    ret = op->verify_params();
    if (ret < 0) {
      abort_req(s, op, ret);
      goto done;
    }

    op->pre_exec();
    req->exec_start();

  done:
    return (ret < 0 ? ret : s->err.ret);
  }

  int RGWLibProcess::finish_request(RGWLibContinuedReq* req)
  {
    RGWOp *op = (req->op) ? req->op : dynamic_cast<RGWOp*>(req);
    if (! op) {
      ldpp_dout(op, 1) << "failed to derive cognate RGWOp (invalid op?)" << dendl;
      return -EINVAL;
    }

    int ret = req->exec_finish();
    int op_ret = op->get_ret();

    ldpp_dout(op, 1) << "====== " << __func__
	    << " finishing continued request req=" << hex << req << dec
	    << " op status=" << op_ret
	    << " ======" << dendl;

    perfcounter->inc(l_rgw_req);

    return ret;
  }

  int RGWLibFrontend::init()
  {
    std::string uri_prefix; // empty
    pprocess = new RGWLibProcess(g_ceph_context, env,
				 g_conf()->rgw_thread_pool_size, uri_prefix, conf);
    return 0;
  }

  void RGWLib::set_fe(rgw::RGWLibFrontend* fe)
  {
    this->fe = fe;
  }

  int RGWLib::init()
  {
    vector<const char*> args;
    return init(args);
  }

  int RGWLib::init(vector<const char*>& args)
  {
    int r{0};
    /* alternative default for module */
    map<std::string,std::string> defaults = {
      { "debug_rgw", "1/5" },
      { "keyring", "$rgw_data/keyring" },
      { "log_file", "/var/log/radosgw/$cluster-$name.log" },
      { "objecter_inflight_ops", "24576" },
      // require a secure mon connection by default
      { "ms_mon_client_mode", "secure" },
      { "auth_client_required", "cephx" },
    };

    cct = rgw_global_init(&defaults, args,
			  CEPH_ENTITY_TYPE_CLIENT,
			  CODE_ENVIRONMENT_DAEMON,
			  CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);

    ceph::mutex mutex = ceph::make_mutex("main");
    SafeTimer init_timer(g_ceph_context, mutex);
    init_timer.init();
    mutex.lock();
    init_timer.add_event_after(g_conf()->rgw_init_timeout, new C_InitTimeout);
    mutex.unlock();

    /* stage all front-ends (before common-init-finish) */
    main.init_frontends1(true /* nfs */);

    common_init_finish(g_ceph_context);

    main.init_perfcounters();
    main.init_http_clients();

    main.init_storage();
    if (! main.get_driver()) {
      mutex.lock();
      init_timer.cancel_all_events();
      init_timer.shutdown();
      mutex.unlock();

      derr << "Couldn't init storage provider (RADOS)" << dendl;
      return -EIO;
    }

    main.cond_init_apis();

    mutex.lock();
    init_timer.cancel_all_events();
    init_timer.shutdown();
    mutex.unlock();

    main.init_ldap();
    main.init_opslog();

    init_async_signal_handler();
    register_async_signal_handler(SIGUSR1, rgw::signal::handle_sigterm);

    main.init_tracepoints();
    r = main.init_frontends2(this /* rgwlib */);
    if (r != 0) {
      derr << "ERROR: unable to initialize frontend, r = " << r << dendl;
      main.shutdown();
      return r;
    }

    main.init_lua();

    return 0;
  } /* RGWLib::init() */

  int RGWLib::stop()
  {
    derr << "shutting down" << dendl;

    const auto finalize_async_signals = []() {
      unregister_async_signal_handler(SIGUSR1, rgw::signal::handle_sigterm);
      shutdown_async_signal_handler();
    };

    main.shutdown(finalize_async_signals);

    return 0;
  } /* RGWLib::stop() */

  int RGWLibIO::set_uid(rgw::sal::Driver* driver, const rgw_user& uid)
  {
    const DoutPrefix dp(driver->ctx(), dout_subsys, "librgw: ");
    std::unique_ptr<rgw::sal::User> user = driver->get_user(uid);
    /* object exists, but policy is broken */
    int ret = user->load_user(&dp, null_yield);
    if (ret < 0) {
      derr << "ERROR: failed reading user info: uid=" << uid << " ret="
	   << ret << dendl;
      return ret;
    }
    user_info = user->get_info();
    return 0;
  }

  int RGWLibRequest::read_permissions(RGWOp* op, optional_yield y) {
    /* bucket and object ops */
    int ret =
      rgw_build_bucket_policies(op, g_rgwlib->get_driver(), get_state(), y);
    if (ret < 0) {
      ldpp_dout(op, 10) << "read_permissions (bucket policy) on "
				  << get_state()->bucket << ":"
				  << get_state()->object
				  << " only_bucket=" << only_bucket()
				  << " ret=" << ret << dendl;
      if (ret == -ENODATA)
	ret = -EACCES;
    } else if (! only_bucket()) {
      /* object ops */
      ret = rgw_build_object_policies(op, g_rgwlib->get_driver(), get_state(),
				      op->prefetch_data(), y);
      if (ret < 0) {
	ldpp_dout(op, 10) << "read_permissions (object policy) on"
				    << get_state()->bucket << ":"
				    << get_state()->object
				    << " ret=" << ret << dendl;
	if (ret == -ENODATA)
	  ret = -EACCES;
      }
    }
    return ret;
  } /* RGWLibRequest::read_permissions */

  int RGWHandler_Lib::authorize(const DoutPrefixProvider *dpp, optional_yield y)
  {
    /* TODO: handle
     *  1. subusers
     *  2. anonymous access
     *  3. system access
     *  4. ?
     *
     *  Much or all of this depends on handling the cached authorization
     *  correctly (e.g., dealing with keystone) at mount time.
     */
    s->perm_mask = RGW_PERM_FULL_CONTROL;

    // populate the owner info
    s->owner.id = s->user->get_id();
    s->owner.display_name = s->user->get_display_name();

    return 0;
  } /* RGWHandler_Lib::authorize */

} /* namespace rgw */
