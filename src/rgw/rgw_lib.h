// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef RGW_LIB_H
#define RGW_LIB_H

#include "include/unordered_map.h"
#include "rgw_common.h"
#include "rgw_client_io.h"
#include "rgw_rest.h"
#include "rgw_request.h"
#include "rgw_frontend.h"
#include "rgw_process.h"
#include "rgw_rest_s3.h" // RGW_Auth_S3


class RGWLibFrontendConfig;
class RGWLibFrontend;
class OpsLogSocket;

class RGWLib {
  RGWFrontendConfig* fec;
  RGWLibFrontend* fe;
  OpsLogSocket* olog;
  RGWREST rest; // XXX needed for RGWProcessEnv
  RGWProcessEnv env;
  RGWRados* store;

public:
  RGWLib() {}
  ~RGWLib() {}

  RGWRados* get_store() { return store; }

  RGWLibFrontend* get_fe() { return fe; }

  int init();
  int init(vector<const char *>& args);
  int stop();
};

/* request interface */

class RGWLibIO : public RGWClientIO
{
  RGWUserInfo user_info;
public:
  RGWLibIO() {}
  RGWLibIO(const RGWUserInfo &_user_info)
    : user_info(_user_info) {}

  virtual void init_env(CephContext *cct) {}

  const RGWUserInfo& get_user() {
    return user_info;
  }

  int set_uid(RGWRados* store, const rgw_user& uid);

  int write_data(const char *buf, int len);
  int read_data(char *buf, int len);
  int send_status(int status, const char *status_name);
  int send_100_continue();
  int complete_header();
  int send_content_length(uint64_t len);

  int complete_request() { /* XXX */
    return 0;
  };

}; /* RGWLibIO */

/* XXX */
class RGWRESTMgr_Lib : public RGWRESTMgr {
public:
  RGWRESTMgr_Lib() {}
  virtual ~RGWRESTMgr_Lib() {}
}; /* RGWRESTMgr_Lib */

/* XXX */
class RGWHandler_Lib : public RGWHandler {
  friend class RGWRESTMgr_Lib;
public:

  virtual int authorize();

  RGWHandler_Lib() {}
  virtual ~RGWHandler_Lib() {}
  static int init_from_header(struct req_state *s);
}; /* RGWHandler_Lib */

class RGWLibRequest : public RGWRequest,
		      public RGWHandler_Lib {
public:
  CephContext* cct;
  RGWUserInfo* user;

  /* unambiguiously return req_state */
  inline struct req_state* get_state() { return this->RGWRequest::s; }

  RGWLibRequest(CephContext* _cct, RGWUserInfo* _user)
    :  RGWRequest(0), cct(_cct), user(_user)
    {}

  RGWUserInfo* get_user() { return user; }

  /* descendant equivalent of *REST*::init_from_header(...):
   * prepare request for execute()--should mean, fixup URI-alikes
   * and any other expected stat vars in local req_state, for
   * now */
  virtual int header_init() = 0;

  /* descendant initializer responsible to call RGWOp::init()--which
   * descendants are required to inherit */
  virtual int op_init() = 0;

  int init(const RGWEnv& rgw_env, RGWObjectCtx* rados_ctx,
	  RGWLibIO* io, struct req_state* _s) {

    RGWRequest::init_state(_s);
    RGWHandler::init(rados_ctx->store, _s, io);

    /* fixup _s->req */
    _s->req = this;

    log_init();

    get_state()->obj_ctx = rados_ctx;
    get_state()->req_id = store->unique_id(id);
    get_state()->trans_id = store->unique_trans_id(id);

    log_format(_s, "initializing for trans_id = %s",
	      get_state()->trans_id.c_str());

    int ret = header_init();
    if (ret == 0) {
      ret = init_from_header(_s);
    }
    return ret;
  }

  virtual bool only_bucket() = 0;

  virtual int read_permissions(RGWOp *op);

  virtual int postauth_init() { return 0; }

}; /* RGWLibRequest */

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
      << __func__ << " enqueue request req=" << hex << req << dec << dendl;

    req_throttle.get(1);
    req_wq.queue(req);
  } /* enqueue_req */

  void handle_request(RGWRequest* req); // async handler, deletes req
  int process_request(RGWLibRequest* req);
  int process_request(RGWLibRequest* req, RGWLibIO* io);
  void set_access_key(RGWAccessKey& key) { access_key = key; }
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

}; /* RGWLibFrontend */

#endif /* RGW_LIB_H */
