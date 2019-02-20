// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_PROCESS_H
#define RGW_PROCESS_H

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_acl.h"
#include "rgw_auth_registry.h"
#include "rgw_user.h"
#include "rgw_op.h"
#include "rgw_rest.h"

#include "include/ceph_assert.h"

#include "common/WorkQueue.h"
#include "common/Throttle.h"

#include <atomic>

#if !defined(dout_subsys)
#define dout_subsys ceph_subsys_rgw
#define def_dout_subsys
#endif

#define dout_context g_ceph_context

extern void signal_shutdown();

namespace rgw::dmclock {
  class Scheduler;
}

struct RGWProcessEnv {
  RGWRados *store;
  RGWREST *rest;
  OpsLogSocket *olog;
  int port;
  std::string uri_prefix;
  std::shared_ptr<rgw::auth::StrategyRegistry> auth_registry;
};

class RGWFrontendConfig;

class RGWProcess {
  deque<RGWRequest*> m_req_queue;
protected:
  CephContext *cct;
  RGWRados* store;
  rgw_auth_registry_ptr_t auth_registry;
  OpsLogSocket* olog;
  ThreadPool m_tp;
  Throttle req_throttle;
  RGWREST* rest;
  RGWFrontendConfig* conf;
  int sock_fd;
  std::string uri_prefix;

  struct RGWWQ : public ThreadPool::WorkQueue<RGWRequest> {
    RGWProcess* process;
    RGWWQ(RGWProcess* p, time_t timeout, time_t suicide_timeout, ThreadPool* tp)
      : ThreadPool::WorkQueue<RGWRequest>("RGWWQ", timeout, suicide_timeout,
					  tp), process(p) {}

    bool _enqueue(RGWRequest* req) override;

    void _dequeue(RGWRequest* req) override {
      ceph_abort();
    }

    bool _empty() override {
      return process->m_req_queue.empty();
    }

    RGWRequest* _dequeue() override;

    using ThreadPool::WorkQueue<RGWRequest>::_process;

    void _process(RGWRequest *req, ThreadPool::TPHandle &) override;

    void _dump_queue();

    void _clear() override {
      ceph_assert(process->m_req_queue.empty());
    }
  } req_wq;

public:
  RGWProcess(CephContext* const cct,
             RGWProcessEnv* const pe,
             const int num_threads,
             RGWFrontendConfig* const conf)
    : cct(cct),
      store(pe->store),
      auth_registry(pe->auth_registry),
      olog(pe->olog),
      m_tp(cct, "RGWProcess::m_tp", "tp_rgw_process", num_threads),
      req_throttle(cct, "rgw_ops", num_threads * 2),
      rest(pe->rest),
      conf(conf),
      sock_fd(-1),
      uri_prefix(pe->uri_prefix),
      req_wq(this, g_conf()->rgw_op_thread_timeout,
	     g_conf()->rgw_op_thread_suicide_timeout, &m_tp) {
  }
  
  virtual ~RGWProcess() = default;

  virtual void run() = 0;
  virtual void handle_request(RGWRequest *req) = 0;

  void pause() {
    m_tp.pause();
  }

  void unpause_with_new_config(RGWRados* const store,
                               rgw_auth_registry_ptr_t auth_registry) {
    this->store = store;
    this->auth_registry = std::move(auth_registry);
    m_tp.unpause();
  }

  void close_fd() {
    if (sock_fd >= 0) {
      ::close(sock_fd);
      sock_fd = -1;
    }
  }
}; /* RGWProcess */

class RGWFCGXProcess : public RGWProcess {
  int max_connections;
public:

  /* have a bit more connections than threads so that requests are
   * still accepted even if we're still processing older requests */
  RGWFCGXProcess(CephContext* const cct,
                 RGWProcessEnv* const pe,
                 const int num_threads,
                 RGWFrontendConfig* const conf)
    : RGWProcess(cct, pe, num_threads, conf),
      max_connections(num_threads + (num_threads >> 3)) {
  }

  void run() override;
  void handle_request(RGWRequest* req) override;
};

class RGWProcessControlThread : public Thread {
  RGWProcess *pprocess;
public:
  explicit RGWProcessControlThread(RGWProcess *_pprocess) : pprocess(_pprocess) {}

  void *entry() override {
    pprocess->run();
    return NULL;
  }
};

class RGWLoadGenProcess : public RGWProcess {
  RGWAccessKey access_key;
public:
  RGWLoadGenProcess(CephContext* cct, RGWProcessEnv* pe, int num_threads,
		  RGWFrontendConfig* _conf) :
  RGWProcess(cct, pe, num_threads, _conf) {}
  void run() override;
  void checkpoint();
  void handle_request(RGWRequest* req) override;
  void gen_request(const string& method, const string& resource,
		  int content_length, std::atomic<bool>* fail_flag);

  void set_access_key(RGWAccessKey& key) { access_key = key; }
};
/* process stream request */
extern int process_request(RGWRados* store,
                           RGWREST* rest,
                           RGWRequest* req,
                           const std::string& frontend_prefix,
                           const rgw_auth_registry_t& auth_registry,
                           RGWRestfulIO* client_io,
                           OpsLogSocket* olog,
                           optional_yield y,
                           rgw::dmclock::Scheduler *scheduler,
                           int* http_ret = nullptr);

extern int rgw_process_authenticated(RGWHandler_REST* handler,
                                     RGWOp*& op,
                                     RGWRequest* req,
                                     req_state* s,
                                     bool skip_retarget = false);

#if defined(def_dout_subsys)
#undef def_dout_subsys
#undef dout_subsys
#endif
#undef dout_context

#endif /* RGW_PROCESS_H */
