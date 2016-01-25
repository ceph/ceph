// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_LIB_FRONTEND_H
#define RGW_LIB_FRONTEND_H

#include <boost/container/flat_map.hpp>

#include "rgw_lib.h"
#include "rgw_file.h"

namespace rgw {

  class RGWLibProcess : public RGWProcess {
    RGWAccessKey access_key;
  public:
    RGWLibProcess(CephContext* cct, RGWProcessEnv* pe, int num_threads,
		  RGWFrontendConfig* _conf) :
      RGWProcess(cct, pe, num_threads, _conf) {}

    void run();
    void checkpoint();

    void enqueue_req(RGWLibRequest* req) {

      lsubdout(g_ceph_context, rgw, 10)
	<< __func__ << " enqueue request req="
	<< hex << req << dec << dendl;

      req_throttle.get(1);
      req_wq.queue(req);
    } /* enqueue_req */

    /* "regular" requests */
    void handle_request(RGWRequest* req); // async handler, deletes req
    int process_request(RGWLibRequest* req);
    int process_request(RGWLibRequest* req, RGWLibIO* io);
    void set_access_key(RGWAccessKey& key) { access_key = key; }

    /* requests w/continue semantics */
    int start_request(RGWLibContinuedReq* req);
    int finish_request(RGWLibContinuedReq* req);
  }; /* RGWLibProcess */

  class RGWLibFrontend : public RGWProcessFrontend {
  public:
    RGWLibFrontend(RGWProcessEnv& pe, RGWFrontendConfig *_conf)
      : RGWProcessFrontend(pe, _conf) {}
		
    int init();

    inline void enqueue_req(RGWLibRequest* req) {
      static_cast<RGWLibProcess*>(pprocess)->enqueue_req(req); // async
    }

    inline int execute_req(RGWLibRequest* req) {
      return static_cast<RGWLibProcess*>(pprocess)->process_request(req); // !async
    }

    inline int start_req(RGWLibContinuedReq* req) {
      return static_cast<RGWLibProcess*>(pprocess)->start_request(req);
    }

    inline int finish_req(RGWLibContinuedReq* req) {
      return static_cast<RGWLibProcess*>(pprocess)->finish_request(req);
    }

  }; /* RGWLibFrontend */

} /* namespace rgw */

#endif /* RGW_LIB_FRONTEND_H */
