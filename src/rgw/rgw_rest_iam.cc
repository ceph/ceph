// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <boost/tokenizer.hpp>

#include "rgw_auth_s3.h"
#include "rgw_rest_iam.h"

#include "rgw_rest_role.h"
#include "rgw_rest_user_policy.h"
#include "rgw_rest_oidc_provider.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

using op_generator = RGWOp*(*)(const bufferlist&);
static const std::unordered_map<std::string_view, op_generator> op_generators = {
  {"CreateRole", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWCreateRole(bl_post_body);}},
  {"DeleteRole", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWDeleteRole(bl_post_body);}},
  {"GetRole", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWGetRole;}},
  {"UpdateAssumeRolePolicy", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWModifyRoleTrustPolicy(bl_post_body);}},
  {"ListRoles", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWListRoles;}},
  {"PutRolePolicy", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWPutRolePolicy(bl_post_body);}},
  {"GetRolePolicy", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWGetRolePolicy;}},
  {"ListRolePolicies", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWListRolePolicies;}},
  {"DeleteRolePolicy", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWDeleteRolePolicy(bl_post_body);}},
  {"PutUserPolicy", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWPutUserPolicy;}},
  {"GetUserPolicy", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWGetUserPolicy;}},
  {"ListUserPolicies", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWListUserPolicies;}},
  {"DeleteUserPolicy", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWDeleteUserPolicy;}},
  {"CreateOpenIDConnectProvider", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWCreateOIDCProvider;}},
  {"ListOpenIDConnectProviders", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWListOIDCProviders;}},
  {"GetOpenIDConnectProvider", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWGetOIDCProvider;}},
  {"DeleteOpenIDConnectProvider", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWDeleteOIDCProvider;}},
  {"TagRole", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWTagRole(bl_post_body);}},
  {"ListRoleTags", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWListRoleTags;}},
  {"UntagRole", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWUntagRole(bl_post_body);}},
  {"UpdateRole", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWUpdateRole(bl_post_body);}}
};

bool RGWHandler_REST_IAM::action_exists(const req_state* s) 
{
  if (s->info.args.exists("Action")) {
    const std::string action_name = s->info.args.get("Action");
    return op_generators.contains(action_name);
  }
  return false;
}

RGWOp *RGWHandler_REST_IAM::op_post()
{
  if (s->info.args.exists("Action")) {
    const std::string action_name = s->info.args.get("Action");
    const auto action_it = op_generators.find(action_name);
    if (action_it != op_generators.end()) {
      return action_it->second(bl_post_body);
    }
    ldpp_dout(s, 10) << "unknown action '" << action_name << "' for IAM handler" << dendl;
  } else {
    ldpp_dout(s, 10) << "missing action argument in IAM handler" << dendl;
  }
  return nullptr;
}

int RGWHandler_REST_IAM::init(rgw::sal::Driver* driver,
                              req_state *s,
                              rgw::io::BasicClient *cio)
{
  s->dialect = "iam";
  s->prot_flags = RGW_REST_IAM;

  return RGWHandler_REST::init(driver, s, cio);
}

int RGWHandler_REST_IAM::authorize(const DoutPrefixProvider* dpp, optional_yield y)
{
  return RGW_Auth_S3::authorize(dpp, driver, auth_registry, s, y);
}

RGWHandler_REST*
RGWRESTMgr_IAM::get_handler(rgw::sal::Driver* driver,
			    req_state* const s,
			    const rgw::auth::StrategyRegistry& auth_registry,
			    const std::string& frontend_prefix)
{
  bufferlist bl;
  return new RGWHandler_REST_IAM(auth_registry, bl);
}
