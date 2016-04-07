// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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

#include "include/types.h"
#include "include/rados/librgw.h"
#include "rgw/rgw_acl_s3.h"
#include "rgw_acl.h"

#include "include/str_list.h"
#include "global/global_init.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/Throttle.h"
#include "common/WorkQueue.h"
#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/dout.h"

#include "rgw_rados.h"
#include "rgw_resolve.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_frontend.h"
#include "rgw_request.h"
#include "rgw_process.h"
#include "rgw_rest_user.h"
#include "rgw_rest_s3.h"
#include "rgw_os_lib.h"
#include "rgw_auth_s3.h"
#include "rgw_lib.h"
#include "rgw_lib_frontend.h"

#include <errno.h>
#include <chrono>
#include <thread>
#include <string>
#include <string.h>
#include <mutex>

#define dout_subsys ceph_subsys_rgw

bool global_stop = false;

namespace rgw {

  using std::string;

  static std::mutex librgw_mtx;

  RGWLib rgwlib;

  class C_InitTimeout : public Context {
  public:
    C_InitTimeout() {}
    void finish(int r) {
      derr << "Initialization timeout, failed to initialize" << dendl;
      exit(1);
    }
  };

  void RGWLibProcess::checkpoint()
  {
    m_tp.drain(&req_wq);
  }

  void RGWLibProcess::run()
  {
    while (! shutdown) {
      lsubdout(cct, rgw, 5) << "RGWLibProcess GC" << dendl;
      unique_lock uniq(mtx);
    restart:
      int cur_gen = gen;
      for (auto iter = mounted_fs.begin(); iter != mounted_fs.end();
	   ++iter) {
	RGWLibFS* fs = iter->first->ref();
	uniq.unlock();
	fs->gc();
	fs->rele();
	uniq.lock();
	if (cur_gen != gen)
	  goto restart; /* invalidated */
      }
      uniq.unlock();
      std::this_thread::sleep_for(std::chrono::seconds(120));
    }
  }

  void RGWLibProcess::handle_request(RGWRequest* r)
  {
    /*
     * invariant: valid requests are derived from RGWLibRequst
     */
    RGWLibRequest* req = static_cast<RGWLibRequest*>(r);

    // XXX move RGWLibIO and timing setup into process_request

#if 0 /* XXX */
    utime_t tm = ceph_clock_now(NULL);
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
    utime_t tm = ceph_clock_now(NULL);
#endif

    RGWLibIO io_ctx;

    int ret = process_request(req, &io_ctx);
    if (ret < 0) {
      /* we don't really care about return code */
      dout(20) << "process_request() returned " << ret << dendl;
    }
    return ret;
  } /* process_request */

  static inline void abort_req(struct req_state *s, RGWOp *op, int err_no)
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
      dout(1) << "failed to derive cognate RGWOp (invalid op?)" << dendl;
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
    struct req_state rstate(req->cct, &rgw_env, req->get_user());
    struct req_state *s = &rstate;

    // XXX fix this
    s->cio = io;

    RGWObjectCtx rados_ctx(store, s); // XXX holds std::map

    /* XXX and -then- stash req_state pointers everywhere they are needed */
    ret = req->init(rgw_env, &rados_ctx, io, s);
    if (ret < 0) {
      dout(10) << "failed to initialize request" << dendl;
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
    req->log(s, "authorizing");
    ret = req->authorize();
    if (ret < 0) {
      dout(10) << "failed to authorize request" << dendl;
      abort_req(s, op, ret);
      goto done;
    }

    req->log(s, "reading op permissions");
    ret = req->read_permissions(op);
    if (ret < 0) {
      abort_req(s, op, ret);
      goto done;
    }

    req->log(s, "init op");
    ret = op->init_processing();
    if (ret < 0) {
      abort_req(s, op, ret);
      goto done;
    }

    req->log(s, "verifying op mask");
    ret = op->verify_op_mask();
    if (ret < 0) {
      abort_req(s, op, ret);
      goto done;
    }

    req->log(s, "verifying op permissions");
    ret = op->verify_permission();
    if (ret < 0) {
      if (s->system_request) {
	dout(2) << "overriding permissions due to system operation" << dendl;
      } else {
	abort_req(s, op, ret);
	goto done;
      }
    }

    req->log(s, "verifying op params");
    ret = op->verify_params();
    if (ret < 0) {
      abort_req(s, op, ret);
      goto done;
    }

    req->log(s, "executing");
    op->pre_exec();
    op->execute();
    op->complete();

  done:
    int r = io->complete_request();
    if (r < 0) {
      dout(0) << "ERROR: io->complete_request() returned " << r << dendl;
    }
    if (should_log) {
      rgw_log_op(store, s, (op ? op->name() : "unknown"), olog);
    }

    int http_ret = s->err.http_ret;

    req->log_format(s, "http status=%d", http_ret);

    dout(1) << "====== " << __func__
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
      dout(1) << "failed to derive cognate RGWOp (invalid op?)" << dendl;
      return -EINVAL;
    }

    struct req_state* s = req->get_state();

    /* req is-a RGWOp, currently initialized separately */
    int ret = req->op_init();
    if (ret < 0) {
      dout(10) << "failed to initialize RGWOp" << dendl;
      abort_req(s, op, ret);
      goto done;
    }

    /* XXX authorize does less here then in the REST path, e.g.,
     * the user's info is cached, but still incomplete */
    req->log(s, "authorizing");
    ret = req->authorize();
    if (ret < 0) {
      dout(10) << "failed to authorize request" << dendl;
      abort_req(s, op, ret);
      goto done;
    }

    req->log(s, "reading op permissions");
    ret = req->read_permissions(op);
    if (ret < 0) {
      abort_req(s, op, ret);
      goto done;
    }

    req->log(s, "init op");
    ret = op->init_processing();
    if (ret < 0) {
      abort_req(s, op, ret);
      goto done;
    }

    req->log(s, "verifying op mask");
    ret = op->verify_op_mask();
    if (ret < 0) {
      abort_req(s, op, ret);
      goto done;
    }

    req->log(s, "verifying op permissions");
    ret = op->verify_permission();
    if (ret < 0) {
      if (s->system_request) {
	dout(2) << "overriding permissions due to system operation" << dendl;
      } else {
	abort_req(s, op, ret);
	goto done;
      }
    }

    req->log(s, "verifying op params");
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
      dout(1) << "failed to derive cognate RGWOp (invalid op?)" << dendl;
      return -EINVAL;
    }

    int ret = req->exec_finish();
    int op_ret = op->get_ret();

    dout(1) << "====== " << __func__
	    << " finishing continued request req=" << hex << req << dec
	    << " op status=" << op_ret
	    << " ======" << dendl;

    return ret;
  }

  int RGWLibFrontend::init()
  {
    pprocess = new RGWLibProcess(g_ceph_context, &env,
				 g_conf->rgw_thread_pool_size, conf);
    return 0;
  }

  int RGWLib::init()
  {
    vector<const char*> args;
    return init(args);
  }

  int RGWLib::init(vector<const char*>& args)
  {
    int r = 0;

    /* alternative default for module */
    vector<const char *> def_args;
    def_args.push_back("--debug-rgw=1/5");
    def_args.push_back("--keyring=$rgw_data/keyring");
    def_args.push_back("--log-file=/var/log/radosgw/$cluster-$name.log");

    global_init(&def_args, args,
		CEPH_ENTITY_TYPE_CLIENT,
		CODE_ENVIRONMENT_DAEMON,
		CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);

    Mutex mutex("main");
    SafeTimer init_timer(g_ceph_context, mutex);
    init_timer.init();
    mutex.Lock();
    init_timer.add_event_after(g_conf->rgw_init_timeout, new C_InitTimeout);
    mutex.Unlock();

    common_init_finish(g_ceph_context);

    rgw_tools_init(g_ceph_context);

    rgw_init_resolver();

    store = RGWStoreManager::get_storage(g_ceph_context,
					 g_conf->rgw_enable_gc_threads,
					 g_conf->rgw_enable_quota_threads,
					 g_conf->rgw_run_sync_thread);

    if (!store) {
      mutex.Lock();
      init_timer.cancel_all_events();
      init_timer.shutdown();
      mutex.Unlock();

      derr << "Couldn't init storage provider (RADOS)" << dendl;
      return -EIO;
    }

    r = rgw_perf_start(g_ceph_context);

    rgw_rest_init(g_ceph_context, store, store->get_zonegroup());

    mutex.Lock();
    init_timer.cancel_all_events();
    init_timer.shutdown();
    mutex.Unlock();

    if (r)
      return -EIO;

    const string& ldap_uri = store->ctx()->_conf->rgw_ldap_uri;
    const string& ldap_binddn = store->ctx()->_conf->rgw_ldap_binddn;
    const string& ldap_searchdn = store->ctx()->_conf->rgw_ldap_searchdn;
    const string& ldap_dnattr =
      store->ctx()->_conf->rgw_ldap_dnattr;

    ldh = new rgw::LDAPHelper(ldap_uri, ldap_binddn, ldap_searchdn,
			      ldap_dnattr);
    ldh->init();
    ldh->bind();

    rgw_user_init(store);
    rgw_bucket_init(store->meta_mgr);
    rgw_log_usage_init(g_ceph_context, store);

    // XXX ex-RGWRESTMgr_lib, mgr->set_logging(true)

    if (!g_conf->rgw_ops_log_socket_path.empty()) {
      olog = new OpsLogSocket(g_ceph_context, g_conf->rgw_ops_log_data_backlog);
      olog->init(g_conf->rgw_ops_log_socket_path);
    }

    int port = 80;
    RGWProcessEnv env = { store, &rest, olog, port };

    fec = new RGWFrontendConfig("rgwlib");
    fe = new RGWLibFrontend(env, fec);

    fe->init();
    if (r < 0) {
      derr << "ERROR: failed initializing frontend" << dendl;
      return r;
    }

    fe->run();

    return 0;
  } /* RGWLib::init() */

  int RGWLib::stop()
  {
    derr << "shutting down" << dendl;

    fe->stop();

    fe->join();

    delete fe;
    delete fec;
    delete ldh;

    rgw_log_usage_finalize();

    delete olog;

    RGWStoreManager::close_storage(store);

    rgw_tools_cleanup();
    rgw_shutdown_resolver();

    rgw_perf_stop(g_ceph_context);

    dout(1) << "final shutdown" << dendl;
    g_ceph_context->put();

    ceph::crypto::shutdown();

    return 0;
  } /* RGWLib::stop() */

  int RGWLibIO::set_uid(RGWRados *store, const rgw_user& uid)
  {
    int ret = rgw_get_user_info_by_uid(store, uid, user_info, NULL);
    if (ret < 0) {
      derr << "ERROR: failed reading user info: uid=" << uid << " ret="
	   << ret << dendl;
    }
    return ret;
  }

  int RGWLibRequest::read_permissions(RGWOp* op) {
    /* bucket and object ops */
    int ret =
      rgw_build_bucket_policies(rgwlib.get_store(), get_state());
    if (ret < 0) {
      ldout(get_state()->cct, 10) << "read_permissions (bucket policy) on "
				  << get_state()->bucket << ":"
				  << get_state()->object
				  << " only_bucket=" << only_bucket()
				  << " ret=" << ret << dendl;
      if (ret == -ENODATA)
	ret = -EACCES;
    } else if (! only_bucket()) {
      /* object ops */
      ret = rgw_build_object_policies(rgwlib.get_store(), get_state(),
				      op->prefetch_data());
      if (ret < 0) {
	ldout(get_state()->cct, 10) << "read_permissions (object policy) on"
				    << get_state()->bucket << ":"
				    << get_state()->object
				    << " ret=" << ret << dendl;
	if (ret == -ENODATA)
	  ret = -EACCES;
      }
    }
    return ret;
  } /* RGWLibRequest::read_permissions */

  int RGWHandler_Lib::authorize()
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
    s->owner.set_id(s->user->user_id);
    s->owner.set_name(s->user->display_name);

    return 0;
  } /* RGWHandler_Lib::authorize */

} /* namespace rgw */

extern "C" {

int librgw_create(librgw_t* rgw, int argc, char **argv)
{
  using namespace rgw;

  int rc = -EINVAL;

  if (! g_ceph_context) {
    std::lock_guard<std::mutex> lg(librgw_mtx);
    if (! g_ceph_context) {
      vector<const char*> args;
      argv_to_vec(argc, const_cast<const char**>(argv), args);
      rc = rgwlib.init(args);
    }
  }

  *rgw = g_ceph_context->get();

  return rc;
}

void librgw_shutdown(librgw_t rgw)
{
  using namespace rgw;

  CephContext* cct = static_cast<CephContext*>(rgw);
  rgwlib.stop();
  cct->put();
}

} /* extern "C" */
