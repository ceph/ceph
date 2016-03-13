// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef RGW_LIB_H
#define RGW_LIB_H

#include <mutex>
#include "include/unordered_map.h"
#include "rgw_common.h"
#include "rgw_client_io.h"
#include "rgw_rest.h"
#include "rgw_request.h"
#include "rgw_frontend.h"
#include "rgw_process.h"
#include "rgw_rest_s3.h" // RGW_Auth_S3

class OpsLogSocket;

namespace rgw {

  class RGWLibFrontendConfig;
  class RGWLibFrontend;

  class RGWLib {
    RGWFrontendConfig* fec;
    RGWLibFrontend* fe;
    OpsLogSocket* olog;
    RGWREST rest; // XXX needed for RGWProcessEnv
    RGWRados* store;

  public:
    RGWLib() : fec(nullptr), fe(nullptr), olog(nullptr), store(nullptr)
      {}
    ~RGWLib() {}

    RGWRados* get_store() { return store; }

    RGWLibFrontend* get_fe() { return fe; }

    int init();
    int init(vector<const char *>& args);
    int stop();
  };

  extern RGWLib rgwlib;

/* request interface */

  class RGWLibIO : public RGWClientIO
  {
    RGWUserInfo user_info;
  public:
    RGWLibIO() {
      get_env().set("HTTP_HOST", "");
    }
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

  virtual int postauth_init() { return 0; }

    /* descendant equivalent of *REST*::init_from_header(...):
     * prepare request for execute()--should mean, fixup URI-alikes
     * and any other expected stat vars in local req_state, for
     * now */
    virtual int header_init() = 0;

    /* descendant initializer responsible to call RGWOp::init()--which
     * descendants are required to inherit */
    virtual int op_init() = 0;

    using RGWHandler::init;

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

  }; /* RGWLibRequest */

  class RGWLibContinuedReq : public RGWLibRequest {
    RGWLibIO io_ctx;
    struct req_state rstate;
    RGWObjectCtx rados_ctx;
  public:

    RGWLibContinuedReq(CephContext* _cct, RGWUserInfo* _user)
      :  RGWLibRequest(_cct, _user), io_ctx(),
	 rstate(_cct, &io_ctx.get_env(), _user), rados_ctx(rgwlib.get_store(),
							   &rstate)
      {
	io_ctx.init(_cct);

	RGWRequest::init_state(&rstate);
	RGWHandler::init(rados_ctx.store, &rstate, &io_ctx);

	/* fixup _s->req */
	get_state()->req = this;

	log_init();

	get_state()->obj_ctx = &rados_ctx;
	get_state()->req_id = store->unique_id(id);
	get_state()->trans_id = store->unique_trans_id(id);

	log_format(get_state(), "initializing for trans_id = %s",
		   get_state()->trans_id.c_str());
      }

    inline RGWRados* get_store() { return store; }

    virtual int execute() final { abort(); }
    virtual int exec_start() = 0;
    virtual int exec_continue() = 0;
    virtual int exec_finish() = 0;

  }; /* RGWLibContinuedReq */

} /* namespace rgw */

#endif /* RGW_LIB_H */
