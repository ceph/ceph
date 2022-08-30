// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef RGW_PROCESS_H
#define RGW_PROCESS_H

#include "rgw_common.h"
#include "rgw_acl.h"
#include "rgw_auth_registry.h"
#include "rgw_user.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_ratelimit.h"
#include "include/ceph_assert.h"

#include "common/WorkQueue.h"
#include "common/Throttle.h"

#include <atomic>

#define dout_context g_ceph_context

extern void signal_shutdown();

namespace rgw::dmclock {
  class Scheduler;
}
namespace rgw::lua {
  class Background;
}

struct RGWProcessEnv {
  rgw::sal::Store* store;
  RGWREST *rest;
  OpsLogSink *olog;
  int port;
  std::string uri_prefix;
  std::shared_ptr<rgw::auth::StrategyRegistry> auth_registry;
  //maybe there is a better place to store the rate limit data structure
  ActiveRateLimiter* ratelimiting;
  rgw::lua::Background* lua_background;
};

class RGWFrontendConfig;
class RGWRequest;

class RGWProcess {
  std::deque<RGWRequest*> m_req_queue;
protected:
  CephContext *cct;
  rgw::sal::Store* store;
  rgw_auth_registry_ptr_t auth_registry;
  OpsLogSink* olog;
  ThreadPool m_tp;
  Throttle req_throttle;
  RGWREST* rest;
  RGWFrontendConfig* conf;
  int sock_fd;
  std::string uri_prefix;
  rgw::lua::Background* lua_background;
  std::unique_ptr<rgw::sal::LuaManager> lua_manager;

  struct RGWWQ : public DoutPrefixProvider, public ThreadPool::WorkQueue<RGWRequest> {
    RGWProcess* process;
    RGWWQ(RGWProcess* p, ceph::timespan timeout, ceph::timespan suicide_timeout,
	  ThreadPool* tp)
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

  CephContext *get_cct() const override { return process->cct; }
  unsigned get_subsys() const { return ceph_subsys_rgw; }
  std::ostream& gen_prefix(std::ostream& out) const { return out << "rgw request work queue: ";}

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
      lua_background(pe->lua_background),
      lua_manager(store->get_lua_manager()),
      req_wq(this,
	     ceph::make_timespan(g_conf()->rgw_op_thread_timeout),
	     ceph::make_timespan(g_conf()->rgw_op_thread_suicide_timeout),
	     &m_tp) {
  }
  
  virtual ~RGWProcess() = default;

  virtual void run() = 0;
  virtual void handle_request(const DoutPrefixProvider *dpp, RGWRequest *req) = 0;

  void pause() {
    m_tp.pause();
  }

  void unpause_with_new_config(rgw::sal::Store* const store,
                               rgw_auth_registry_ptr_t auth_registry) {
    this->store = store;
    this->auth_registry = std::move(auth_registry);
    lua_manager = store->get_lua_manager();
    m_tp.unpause();
  }

  void close_fd() {
    if (sock_fd >= 0) {
      ::close(sock_fd);
      sock_fd = -1;
    }
  }
}; /* RGWProcess */

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
  void handle_request(const DoutPrefixProvider *dpp, RGWRequest* req) override;
  void gen_request(const std::string& method, const std::string& resource,
		  int content_length, std::atomic<bool>* fail_flag);

  void set_access_key(RGWAccessKey& key) { access_key = key; }
};
/* process stream request */
extern int process_request(rgw::sal::Store* store,
                           RGWREST* rest,
                           RGWRequest* req,
                           const std::string& frontend_prefix,
                           const rgw_auth_registry_t& auth_registry,
                           RGWRestfulIO* client_io,
                           OpsLogSink* olog,
                           optional_yield y,
                           rgw::dmclock::Scheduler *scheduler,
                           std::string* user,
                           ceph::coarse_real_clock::duration* latency,
                           std::shared_ptr<RateLimiter> ratelimit,
                           rgw::lua::Background* lua_background,
                           std::unique_ptr<rgw::sal::LuaManager>& lua_manager,
                           int* http_ret = nullptr);

extern int rgw_process_authenticated(RGWHandler_REST* handler,
                                     RGWOp*& op,
                                     RGWRequest* req,
                                     req_state* s,
				                             optional_yield y,
                                     rgw::sal::Store* store,
                                     bool skip_retarget = false);

#undef dout_context

#endif /* RGW_PROCESS_H */
