// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <mutex>
#include <optional>
#include "rgw_common.h"
#include "rgw_client_io.h"
#include "rgw_rest.h"
#include "rgw_request.h"
#include "rgw_ldap.h"
#include "include/ceph_assert.h"
#include "rgw_main.h"

class OpsLogSink;

namespace rgw {

  class RGWLibFrontend;

  class RGWLib : public DoutPrefixProvider {
    boost::intrusive_ptr<CephContext> cct;
    AppMain main;
    RGWLibFrontend* fe;

  public:
    RGWLib() : main(this), fe(nullptr)
      {}
    ~RGWLib() {}

    rgw::sal::Driver* get_driver() { return main.get_driver(); }

    RGWLibFrontend* get_fe() { return fe; }

    rgw::LDAPHelper* get_ldh() { return main.get_ldh(); }
    CephContext *get_cct() const override { return cct.get(); }
    unsigned get_subsys() const { return ceph_subsys_rgw; }
    std::ostream& gen_prefix(std::ostream& out) const { return out << "lib rgw: "; }

    void set_fe(RGWLibFrontend* fe);

    int init();
    int init(std::vector<const char *>& args);
    int stop();
  };

  extern RGWLib* g_rgwlib;

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

    int set_uid(rgw::sal::Driver* driver, const rgw_user& uid);

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

  class RGWRESTMgr_Lib : public RGWRESTMgr {
  public:
    RGWRESTMgr_Lib() {}
    ~RGWRESTMgr_Lib() override {}
  }; /* RGWRESTMgr_Lib */

  class RGWHandler_Lib : public RGWHandler {
    friend class RGWRESTMgr_Lib;
  public:

    int authorize(const DoutPrefixProvider *dpp, optional_yield y) override;

    RGWHandler_Lib() {}
    ~RGWHandler_Lib() override {}
    static int init_from_header(rgw::sal::Driver* driver,
				req_state *s);
  }; /* RGWHandler_Lib */

  class RGWLibRequest : public RGWRequest,
			public RGWHandler_Lib {
  private:
    std::unique_ptr<rgw::sal::User> tuser; // Don't use this.  It's empty except during init.
  public:
    CephContext* cct;

    /* unambiguously return req_state */
    inline req_state* get_state() { return this->RGWRequest::s; }

    RGWLibRequest(CephContext* _cct, std::unique_ptr<rgw::sal::User> _user)
      :  RGWRequest(g_rgwlib->get_driver()->get_new_req_id()),
	 tuser(std::move(_user)), cct(_cct)
      {}

  int postauth_init(optional_yield) override { return 0; }

    /* descendant equivalent of *REST*::init_from_header(...):
     * prepare request for execute()--should mean, fixup URI-alikes
     * and any other expected stat vars in local req_state, for
     * now */
    virtual int header_init() = 0;

    /* descendant initializer responsible to call RGWOp::init()--which
     * descendants are required to inherit */
    virtual int op_init() = 0;

    using RGWHandler::init;

    int init(const RGWEnv& rgw_env, rgw::sal::Driver* _driver,
	     RGWLibIO* io, req_state* _s) {

      RGWRequest::init_state(_s);
      RGWHandler::init(_driver, _s, io);

      get_state()->req_id = driver->zone_unique_id(id);
      get_state()->trans_id = driver->zone_unique_trans_id(id);
      get_state()->bucket_tenant = tuser->get_tenant();
      get_state()->set_user(tuser);

      ldpp_dout(_s, 2) << "initializing for trans_id = "
	  << get_state()->trans_id.c_str() << dendl;

      int ret = header_init();
      if (ret == 0) {
	ret = init_from_header(driver, _s);
      }
      return ret;
    }

    virtual bool only_bucket() = 0;

    int read_permissions(RGWOp *op, optional_yield y) override;

  }; /* RGWLibRequest */

  class RGWLibContinuedReq : public RGWLibRequest {
    RGWLibIO io_ctx;
    req_state rstate;
  public:

    RGWLibContinuedReq(CephContext* _cct, const RGWProcessEnv& penv,
		       std::unique_ptr<rgw::sal::User> _user)
      :  RGWLibRequest(_cct, std::move(_user)), io_ctx(),
	 rstate(_cct, penv, &io_ctx.get_env(), id)
      {
	io_ctx.init(_cct);

	RGWRequest::init_state(&rstate);
	RGWHandler::init(g_rgwlib->get_driver(), &rstate, &io_ctx);

	get_state()->req_id = driver->zone_unique_id(id);
	get_state()->trans_id = driver->zone_unique_trans_id(id);

	ldpp_dout(get_state(), 2) << "initializing for trans_id = "
	    << get_state()->trans_id.c_str() << dendl;
      }

    inline rgw::sal::Driver* get_driver() { return driver; }
    inline RGWLibIO& get_io() { return io_ctx; }

    virtual int execute() final { ceph_abort(); }
    virtual int exec_start() = 0;
    virtual int exec_continue() = 0;
    virtual int exec_finish() = 0;

  }; /* RGWLibContinuedReq */

} /* namespace rgw */
