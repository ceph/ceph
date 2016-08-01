// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_LIB_FRONTEND_H
#define RGW_LIB_FRONTEND_H

#include <boost/container/flat_map.hpp>

#include <boost/container/flat_map.hpp>

#include "rgw_lib.h"
#include "rgw_file.h"

namespace rgw {

  class RGWLibProcess : public RGWProcess {
    RGWAccessKey access_key;
    std::mutex mtx;
    int gen;
    bool shutdown;

    typedef flat_map<RGWLibFS*, RGWLibFS*> FSMAP;
    FSMAP mounted_fs;

    using lock_guard = std::lock_guard<std::mutex>;
    using unique_lock = std::unique_lock<std::mutex>;

  public:
    RGWLibProcess(CephContext* cct, RGWProcessEnv* pe, int num_threads,
		  RGWFrontendConfig* _conf) :
      RGWProcess(cct, pe, num_threads, _conf), gen(0), shutdown(false) {}

    void run();
    void checkpoint();

    void register_fs(RGWLibFS* fs) {
      lock_guard guard(mtx);
      mounted_fs.insert(FSMAP::value_type(fs, fs));
      ++gen;
    }

    void unregister_fs(RGWLibFS* fs) {
      lock_guard guard(mtx);
      FSMAP::iterator it = mounted_fs.find(fs);
      if (it != mounted_fs.end()) {
	mounted_fs.erase(it);
	++gen;
      }
    }

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

    RGWLibProcess* get_process() {
      return static_cast<RGWLibProcess*>(pprocess);
    }

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
