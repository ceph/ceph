// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_common.h"
#include "rgw_acl.h"
#include "rgw_user.h"
#include "rgw_rest.h"
#include "include/ceph_assert.h"

#include "common/WorkQueue.h"
#include "common/Throttle.h"

#include <atomic>

#define dout_context g_ceph_context


namespace rgw::dmclock {
  class Scheduler;
}

struct RGWProcessEnv;
class RGWFrontendConfig;
class RGWRequest;

class RGWProcess {
  std::deque<RGWRequest*> m_req_queue;
protected:
  CephContext *cct;
  RGWProcessEnv& env;
  ThreadPool m_tp;
  Throttle req_throttle;
  RGWFrontendConfig* conf;
  int sock_fd;
  std::string uri_prefix;

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
             RGWProcessEnv& env,
             const int num_threads,
             std::string uri_prefix,
             RGWFrontendConfig* const conf)
    : cct(cct), env(env),
      m_tp(cct, "RGWProcess::m_tp", "tp_rgw_process", num_threads),
      req_throttle(cct, "rgw_ops", num_threads * 2),
      conf(conf),
      sock_fd(-1),
      uri_prefix(std::move(uri_prefix)),
      req_wq(this,
	     ceph::make_timespan(g_conf()->rgw_op_thread_timeout),
	     ceph::make_timespan(g_conf()->rgw_op_thread_suicide_timeout),
	     &m_tp) {
  }

  virtual ~RGWProcess() = default;

  const RGWProcessEnv& get_env() const { return env; }

  virtual void run() = 0;
  virtual void handle_request(const DoutPrefixProvider *dpp, RGWRequest *req) = 0;

  void pause() {
    m_tp.pause();
  }

  void unpause_with_new_config() {
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
  RGWLoadGenProcess(CephContext* cct, RGWProcessEnv& env, int num_threads,
                    std::string uri_prefix, RGWFrontendConfig* _conf)
    : RGWProcess(cct, env, num_threads, std::move(uri_prefix), _conf) {}
  void run() override;
  void checkpoint();
  void handle_request(const DoutPrefixProvider *dpp, RGWRequest* req) override;
  void gen_request(const std::string& method, const std::string& resource,
		  int content_length, std::atomic<bool>* fail_flag);

  void set_access_key(RGWAccessKey& key) { access_key = key; }
};
/* process stream request */
extern int process_request(const RGWProcessEnv& penv,
                           RGWRequest* req,
                           const std::string& frontend_prefix,
                           RGWRestfulIO* client_io,
                           optional_yield y,
                           rgw::dmclock::Scheduler *scheduler,
                           std::string* user,
                           ceph::coarse_real_clock::duration* latency,
                           int* http_ret = nullptr);

extern int rgw_process_authenticated(RGWHandler_REST* handler,
                                     RGWOp*& op,
                                     RGWRequest* req,
                                     req_state* s,
				                             optional_yield y,
                                     rgw::sal::Driver* driver,
                                     bool skip_retarget = false);

#undef dout_context
