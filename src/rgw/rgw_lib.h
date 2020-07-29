// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef RGW_LIB_H
#define RGW_LIB_H

#include <mutex>
#include "include/unordered_map.h"
#include "global/global_init.h"
#include "rgw_common.h"
#include "rgw_client_io.h"
#include "rgw_rest.h"
#include "rgw_request.h"
#include "rgw_frontend.h"
#include "rgw_process.h"
#include "rgw_rest_s3.h" // RGW_Auth_S3
#include "rgw_ldap.h"
#include "services/svc_zone_utils.h"
#include "include/ceph_assert.h"

class OpsLogSocket;

namespace rgw {

  class RGWLibFrontend;

  class RGWLib {
    RGWFrontendConfig* fec;
    RGWLibFrontend* fe;
    OpsLogSocket* olog;
    rgw::LDAPHelper* ldh{nullptr};
    RGWREST rest; // XXX needed for RGWProcessEnv
    rgw::sal::RGWRadosStore* store;
    boost::intrusive_ptr<CephContext> cct;

  public:
    RGWLib() : fec(nullptr), fe(nullptr), olog(nullptr), store(nullptr)
      {}
    ~RGWLib() {}

    rgw::sal::RGWRadosStore* get_store() { return store; }

    RGWLibFrontend* get_fe() { return fe; }

    rgw::LDAPHelper* get_ldh() { return ldh; }

    int init();
    int init(vector<const char *>& args);
    int stop();
  };

  extern RGWLib rgwlib;

/* request interface */

  class RGWLibIO : public rgw::io::BasicClient,
                   public rgw::io::Accounter
  {
    RGWUserInfo user_info;
    RGWEnv env;
  public:
    RGWLibIO() {
      get_env().set("HTTP_HOST", "");
    }
    explicit RGWLibIO(const RGWUserInfo &_user_info)
      : user_info(_user_info) {}

    int init_env(CephContext *cct) override {
      env.init(cct);
      return 0;
    }

    const RGWUserInfo& get_user() {
      return user_info;
    }

    int set_uid(rgw::sal::RGWRadosStore* store, const rgw_user& uid);

    int write_data(const char *buf, int len);
    int read_data(char *buf, int len);
    int send_status(int status, const char *status_name);
    int send_100_continue();
    int complete_header();
    int send_content_length(uint64_t len);

    RGWEnv& get_env() noexcept override {
      return env;
    }

    size_t complete_request() override { /* XXX */
      return 0;
    };

    void set_account(bool) override {
      return;
    }

    uint64_t get_bytes_sent() const override {
      return 0;
    }

    uint64_t get_bytes_received() const override {
      return 0;
    }

  }; /* RGWLibIO */

/* XXX */
  class RGWRESTMgr_Lib : public RGWRESTMgr {
  public:
    RGWRESTMgr_Lib() {}
    ~RGWRESTMgr_Lib() override {}
  }; /* RGWRESTMgr_Lib */

/* XXX */
  class RGWHandler_Lib : public RGWHandler {
    friend class RGWRESTMgr_Lib;
  public:

    int authorize(const DoutPrefixProvider *dpp) override;

    RGWHandler_Lib() {}
    ~RGWHandler_Lib() override {}
    static int init_from_header(rgw::sal::RGWRadosStore *store,
				struct req_state *s);
  }; /* RGWHandler_Lib */

  class RGWLibRequest : public RGWRequest,
			public RGWHandler_Lib {
  public:
    CephContext* cct;
    rgw::sal::RGWUser* user;
    boost::optional<RGWSysObjectCtx> sysobj_ctx;

    /* unambiguiously return req_state */
    inline struct req_state* get_state() { return this->RGWRequest::s; }

    RGWLibRequest(CephContext* _cct, rgw::sal::RGWUser* _user)
      :  RGWRequest(rgwlib.get_store()->getRados()->get_new_req_id()), cct(_cct),
	 user(_user)
      {}

    rgw::sal::RGWUser* get_user() { return user; }

  int postauth_init() override { return 0; }

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
      RGWHandler::init(rados_ctx->get_store(), _s, io);

      sysobj_ctx.emplace(store->svc()->sysobj);

      get_state()->obj_ctx = rados_ctx;
      get_state()->sysobj_ctx = &(sysobj_ctx.get());
      get_state()->req_id = store->svc()->zone_utils->unique_id(id);
      get_state()->trans_id = store->svc()->zone_utils->unique_trans_id(id);

      ldpp_dout(_s, 2) << "initializing for trans_id = "
	  << get_state()->trans_id.c_str() << dendl;

      int ret = header_init();
      if (ret == 0) {
	ret = init_from_header(rados_ctx->get_store(), _s);
      }
      return ret;
    }

    virtual bool only_bucket() = 0;

    int read_permissions(RGWOp *op) override;

  }; /* RGWLibRequest */

  class RGWLibContinuedReq : public RGWLibRequest {
    RGWLibIO io_ctx;
    struct req_state rstate;
    RGWObjectCtx rados_ctx;
  public:

    RGWLibContinuedReq(CephContext* _cct, rgw::sal::RGWUser* _user)
      :  RGWLibRequest(_cct, _user), io_ctx(),
	 rstate(_cct, &io_ctx.get_env(), _user, id),
	 rados_ctx(rgwlib.get_store(), &rstate)
      {
	io_ctx.init(_cct);

	RGWRequest::init_state(&rstate);
	RGWHandler::init(rados_ctx.get_store(), &rstate, &io_ctx);

	sysobj_ctx.emplace(store->svc()->sysobj);

	get_state()->obj_ctx = &rados_ctx;
	get_state()->sysobj_ctx = &(sysobj_ctx.get());
	get_state()->req_id = store->svc()->zone_utils->unique_id(id);
	get_state()->trans_id = store->svc()->zone_utils->unique_trans_id(id);

	ldpp_dout(get_state(), 2) << "initializing for trans_id = "
	    << get_state()->trans_id.c_str() << dendl;
      }

    inline rgw::sal::RGWRadosStore* get_store() { return store; }

    virtual int execute() final { ceph_abort(); }
    virtual int exec_start() = 0;
    virtual int exec_continue() = 0;
    virtual int exec_finish() = 0;

  }; /* RGWLibContinuedReq */

} /* namespace rgw */

#endif /* RGW_LIB_H */
