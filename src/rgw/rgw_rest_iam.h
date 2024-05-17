// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <concepts>

#include "rgw_auth.h"
#include "rgw_auth_filters.h"
#include "rgw_rest.h"
#include "rgw_role.h"
#include "rgw_sal.h"
#include "rgw_xml.h"


class DoutPrefixProvider;
namespace rgw { class SiteConfig; }
struct RGWUserInfo;
struct RGWGroupInfo;

bool validate_iam_policy_name(const std::string& name, std::string& err);
bool validate_iam_policy_arn(const std::string& arn, std::string& err);
bool validate_iam_user_name(const std::string& name, std::string& err);
bool validate_iam_role_name(const std::string& name, std::string& err);
bool validate_iam_group_name(const std::string& name, std::string& err);
bool validate_iam_path(const std::string& path, std::string& err);

std::string iam_user_arn(const RGWUserInfo& info);
std::string iam_group_arn(const RGWGroupInfo& info);

int forward_iam_request_to_master(const DoutPrefixProvider* dpp,
                                  const rgw::SiteConfig& site,
                                  const RGWUserInfo& user,
                                  bufferlist& indata,
                                  RGWXMLDecoder::XMLParser& parser,
                                  req_info& req, optional_yield y);

/// Perform an atomic read-modify-write operation on the given user metadata.
/// Racing writes are detected here as ECANCELED errors, where we reload the
/// updated user metadata and retry the operation.
template <std::invocable<> F>
int retry_raced_user_write(const DoutPrefixProvider* dpp, optional_yield y,
                           rgw::sal::User* u, const F& f)
{
  int r = f();
  for (int i = 0; i < 10 && r == -ECANCELED; ++i) {
    u->get_version_tracker().clear();
    r = u->load_user(dpp, y);
    if (r >= 0) {
      r = f();
    }
  }
  return r;
}

/// Perform an atomic read-modify-write operation on the given group metadata.
/// Racing writes are detected here as ECANCELED errors, where we reload the
/// updated group metadata and retry the operation.
template <std::invocable<> F>
int retry_raced_group_write(const DoutPrefixProvider* dpp, optional_yield y,
                            rgw::sal::Driver* driver, RGWGroupInfo& info,
                            rgw::sal::Attrs& attrs, RGWObjVersionTracker& objv,
                            const F& f)
{
  int r = f();
  for (int i = 0; i < 10 && r == -ECANCELED; ++i) {
    objv.clear();
    r = driver->load_group_by_id(dpp, y, info.id, info, attrs, objv);
    if (r >= 0) {
      r = f();
    }
  }
  return r;
}

/// Perform an atomic read-modify-write operation on the given role metadata.
/// Racing writes are detected here as ECANCELED errors, where we reload the
/// updated group metadata and retry the operation.
template <std::invocable<> F>
int retry_raced_role_write(const DoutPrefixProvider* dpp, optional_yield y,
                           rgw::sal::RGWRole* role, const F& f)
{
  int r = f();
  for (int i = 0; i < 10 && r == -ECANCELED; ++i) {
    role->get_objv_tracker().clear();
    r = role->get_by_id(dpp, y);
    if (r >= 0) {
      r = f();
    }
  }
  return r;
}

class RGWHandler_REST_IAM : public RGWHandler_REST {
  const rgw::auth::StrategyRegistry& auth_registry;
  bufferlist bl_post_body;
  RGWOp *op_post() override;

public:

  static bool action_exists(const req_state* s);

  RGWHandler_REST_IAM(const rgw::auth::StrategyRegistry& auth_registry,
		      bufferlist& bl_post_body)
    : RGWHandler_REST(),
      auth_registry(auth_registry),
      bl_post_body(bl_post_body) {}
  ~RGWHandler_REST_IAM() override = default;

  int init(rgw::sal::Driver* driver,
           req_state *s,
           rgw::io::BasicClient *cio) override;
  int authorize(const DoutPrefixProvider* dpp, optional_yield y) override;
  int postauth_init(optional_yield y) override { return 0; }
};

class RGWRESTMgr_IAM : public RGWRESTMgr {
public:
  RGWRESTMgr_IAM() = default;
  ~RGWRESTMgr_IAM() override = default;

  RGWRESTMgr *get_resource_mgr(req_state* const s,
                               const std::string& uri,
                               std::string* const out_uri) override {
    return this;
  }

  RGWHandler_REST* get_handler(rgw::sal::Driver* driver,
			       req_state*,
                               const rgw::auth::StrategyRegistry&,
                               const std::string&) override;
};
