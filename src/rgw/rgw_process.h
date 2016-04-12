// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_PROCESS_H
#define RGW_PROCESS_H

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_acl.h"
#include "rgw_user.h"
#include "rgw_op.h"
#include "rgw_rest.h"

#include "include/assert.h"

#include "common/WorkQueue.h"
#include "common/Throttle.h"

#if !defined(dout_subsys)
#define dout_subsys ceph_subsys_rgw
#define def_dout_subsys
#endif

#define SOCKET_BACKLOG 1024

extern void signal_shutdown();

struct RGWProcessEnv {
  RGWRados *store;
  RGWREST *rest;
  OpsLogSocket *olog;
  int port;
};

class RGWFrontendConfig;

class RGWProcess {
  deque<RGWRequest*> m_req_queue;
protected:
  CephContext *cct;
  RGWRados* store;
  OpsLogSocket* olog;
  ThreadPool m_tp;
  Throttle req_throttle;
  RGWREST* rest;
  RGWFrontendConfig* conf;
  int sock_fd;

  struct RGWWQ : public ThreadPool::WorkQueue<RGWRequest> {
    RGWProcess* process;
    RGWWQ(RGWProcess* p, time_t timeout, time_t suicide_timeout, ThreadPool* tp)
      : ThreadPool::WorkQueue<RGWRequest>("RGWWQ", timeout, suicide_timeout,
					  tp), process(p) {}

    bool _enqueue(RGWRequest* req) {
      process->m_req_queue.push_back(req);
      perfcounter->inc(l_rgw_qlen);
      dout(20) << "enqueued request req=" << hex << req << dec << dendl;
      _dump_queue();
      return true;
    }

    void _dequeue(RGWRequest* req) {
      assert(0);
    }

    bool _empty() {
      return process->m_req_queue.empty();
    }

    RGWRequest* _dequeue() {
      if (process->m_req_queue.empty())
	return NULL;
      RGWRequest *req = process->m_req_queue.front();
      process->m_req_queue.pop_front();
      dout(20) << "dequeued request req=" << hex << req << dec << dendl;
      _dump_queue();
      perfcounter->inc(l_rgw_qlen, -1);
      return req;
    }

    using ThreadPool::WorkQueue<RGWRequest>::_process;

    void _process(RGWRequest *req, ThreadPool::TPHandle &) override  {
      perfcounter->inc(l_rgw_qactive);
      process->handle_request(req);
      process->req_throttle.put(1);
      perfcounter->inc(l_rgw_qactive, -1);
    }

    void _dump_queue();

    void _clear() {
      assert(process->m_req_queue.empty());
    }
  } req_wq;

public:
  RGWProcess(CephContext* cct, RGWProcessEnv* pe, int num_threads,
	    RGWFrontendConfig* _conf)
    : cct(cct), store(pe->store), olog(pe->olog),
      m_tp(cct, "RGWProcess::m_tp", "tp_rgw_process", num_threads),
      req_throttle(cct, "rgw_ops", num_threads * 2),
      rest(pe->rest), conf(_conf), sock_fd(-1),
      req_wq(this, g_conf->rgw_op_thread_timeout,
	     g_conf->rgw_op_thread_suicide_timeout, &m_tp) {}
  
  virtual ~RGWProcess() {}

  virtual void run() = 0;
  virtual void handle_request(RGWRequest *req) = 0;

  void pause() {
    m_tp.pause();
  }

  void unpause_with_new_config(RGWRados *store) {
    this->store = store;
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
  RGWFCGXProcess(CephContext* cct, RGWProcessEnv* pe, int num_threads,
		 RGWFrontendConfig* _conf)
    : RGWProcess(cct, pe, num_threads, _conf),
      max_connections(num_threads + (num_threads >> 3))
    {}

  void run();
  void handle_request(RGWRequest* req);
};

class RGWProcessControlThread : public Thread {
  RGWProcess *pprocess;
public:
  RGWProcessControlThread(RGWProcess *_pprocess) : pprocess(_pprocess) {}

  void *entry() {
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
  void run();
  void checkpoint();
  void handle_request(RGWRequest* req);
  void gen_request(const string& method, const string& resource,
		  int content_length, atomic_t* fail_flag);

  void set_access_key(RGWAccessKey& key) { access_key = key; }
};

/* process stream request */
int process_request(RGWRados* store, RGWREST* rest, RGWRequest* req,
		    RGWStreamIO* client_io, OpsLogSocket* olog);

#if defined(def_dout_subsys)
#undef def_dout_subsys
#undef dout_subsys
#endif

#endif /* RGW_PROCESS_H */
