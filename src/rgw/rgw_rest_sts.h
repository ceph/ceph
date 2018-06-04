#ifndef CEPH_RGW_REST_STS_H
#define CEPH_RGW_REST_STS_H

#include "rgw_sts.h"

class RGWREST_STS : public RGWRESTOp {
protected:
  STS::STSService sts;
public:
  RGWREST_STS() = default;
  int verify_permission() override;
  void send_response() override;
};

class RGWSTSAssumeRole : public RGWREST_STS {
protected:
  string duration;
  string externalId;
  string policy;
  string roleArn;
  string roleSessionName;
  string serialNumber;
  string tokenCode;
public:
  RGWSTSAssumeRole() = default;
  void execute() override;
  int get_params();
  const char* name() const override { return "assume_role"; }
  RGWOpType get_type() override { return RGW_STS_ASSUME_ROLE; }
};

class RGWHandler_REST_STS : public RGWHandler_REST {
  const rgw::auth::StrategyRegistry& auth_registry;
  RGWOp *op_post() override;
public:
  static int init_from_header(struct req_state *s, int default_formatter, bool configurable_format);

  RGWHandler_REST_STS(const rgw::auth::StrategyRegistry& auth_registry)
    : RGWHandler_REST(),
      auth_registry(auth_registry) {}
  ~RGWHandler_REST_STS() override = default;

  int init(RGWRados *store,
           struct req_state *s,
           rgw::io::BasicClient *cio) override;
  int authorize() override {
    return RGW_Auth_S3::authorize(store, auth_registry, s);
  }
  int postauth_init() override { return 0; }
};

class RGWRESTMgr_STS : public RGWRESTMgr {
public:
  RGWRESTMgr_STS() = default;
  ~RGWRESTMgr_STS() override = default;
  
  RGWRESTMgr *get_resource_mgr(struct req_state* const s,
                               const std::string& uri,
                               std::string* const out_uri) override {
    return this;
  }

  RGWHandler_REST* get_handler(struct req_state*,
                               const rgw::auth::StrategyRegistry&,
                               const std::string&) override;
};

#endif /* CEPH_RGW_REST_STS_H */

