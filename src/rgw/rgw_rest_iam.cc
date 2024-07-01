// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <boost/tokenizer.hpp>

#include "rgw_rest.h"
#include "rgw_rest_iam.h"

#include "rgw_request.h"
#include "rgw_process.h"

#include "rgw_rest_role.h"
#include "rgw_rest_user_policy.h"
#include "rgw_rest_oidc_provider.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

void RGWHandler_REST_IAM::rgw_iam_parse_input()
{
  std::string post_body = bl_post_body.to_str();
  if (post_body.size() > 0) {
    ldpp_dout(s, 10) << "Content of POST: " << post_body << dendl;

    if (post_body.find("Action") != string::npos) {
      boost::char_separator<char> sep("&");
      boost::tokenizer<boost::char_separator<char>> tokens(post_body, sep);
      for (const auto& t : tokens) {
        auto pos = t.find("=");
        if (pos != string::npos) {
          constexpr bool in_query = true; // replace '+' with ' '
          s->info.args.append(t.substr(0,pos),
                              url_decode(t.substr(pos+1, t.size() -1), in_query));
        }
      }
    }
  }
  auto payload_hash = rgw::auth::s3::calc_v4_payload_hash(post_body);
  s->info.args.append("PayloadHash", payload_hash);
}

RGWOp *RGWHandler_REST_IAM::op_post()
{
  rgw_iam_parse_input();

  if (s->info.args.exists("Action")) {
    string action = s->info.args.get("Action");
    if (action.compare("CreateRole") == 0)
      return new RGWCreateRole(this->bl_post_body);
    if (action.compare("DeleteRole") == 0)
      return new RGWDeleteRole(this->bl_post_body);
    if (action.compare("GetRole") == 0)
      return new RGWGetRole;
    if (action.compare("UpdateAssumeRolePolicy") == 0)
      return new RGWModifyRole(this->bl_post_body);
    if (action.compare("ListRoles") == 0)
      return new RGWListRoles;
    if (action.compare("PutRolePolicy") == 0)
      return new RGWPutRolePolicy(this->bl_post_body);
    if (action.compare("GetRolePolicy") == 0)
      return new RGWGetRolePolicy;
    if (action.compare("ListRolePolicies") == 0)
      return new RGWListRolePolicies;
    if (action.compare("DeleteRolePolicy") == 0)
      return new RGWDeleteRolePolicy(this->bl_post_body);
    if (action.compare("PutUserPolicy") == 0)
      return new RGWPutUserPolicy;
    if (action.compare("GetUserPolicy") == 0)
      return new RGWGetUserPolicy;
    if (action.compare("ListUserPolicies") == 0)
      return new RGWListUserPolicies;
    if (action.compare("DeleteUserPolicy") == 0)
      return new RGWDeleteUserPolicy;
    if (action.compare("CreateOpenIDConnectProvider") == 0)
      return new RGWCreateOIDCProvider;
    if (action.compare("ListOpenIDConnectProviders") == 0)
      return new RGWListOIDCProviders;
    if (action.compare("GetOpenIDConnectProvider") == 0)
      return new RGWGetOIDCProvider;
    if (action.compare("DeleteOpenIDConnectProvider") == 0)
      return new RGWDeleteOIDCProvider;
    if (action.compare("TagRole") == 0)
      return new RGWTagRole(this->bl_post_body);
    if (action.compare("ListRoleTags") == 0)
      return new RGWListRoleTags;
    if (action.compare("UntagRole") == 0)
      return new RGWUntagRole(this->bl_post_body);
  }

  return nullptr;
}

int RGWHandler_REST_IAM::init(rgw::sal::Store* store,
                              struct req_state *s,
                              rgw::io::BasicClient *cio)
{
  s->dialect = "iam";

  if (int ret = RGWHandler_REST_IAM::init_from_header(s, RGW_FORMAT_XML, true); ret < 0) {
    ldpp_dout(s, 10) << "init_from_header returned err=" << ret <<  dendl;
    return ret;
  }

  return RGWHandler_REST::init(store, s, cio);
}

int RGWHandler_REST_IAM::authorize(const DoutPrefixProvider* dpp, optional_yield y)
{
  return RGW_Auth_S3::authorize(dpp, store, auth_registry, s, y);
}

int RGWHandler_REST_IAM::init_from_header(struct req_state* s,
                                          int default_formatter,
                                          bool configurable_format)
{
  string req;
  string first;

  s->prot_flags = RGW_REST_IAM;

  const char *p, *req_name;
  if (req_name = s->relative_uri.c_str(); *req_name == '?') {
    p = req_name;
  } else {
    p = s->info.request_params.c_str();
  }

  s->info.args.set(p);
  s->info.args.parse(s);

  /* must be called after the args parsing */
  if (int ret = allocate_formatter(s, default_formatter, configurable_format); ret < 0)
    return ret;

  if (*req_name != '/')
    return 0;

  req_name++;

  if (!*req_name)
    return 0;

  req = req_name;
  int pos = req.find('/');
  if (pos >= 0) {
    first = req.substr(0, pos);
  } else {
    first = req;
  }

  return 0;
}

RGWHandler_REST*
RGWRESTMgr_IAM::get_handler(rgw::sal::Store* store,
			    struct req_state* const s,
			    const rgw::auth::StrategyRegistry& auth_registry,
			    const std::string& frontend_prefix)
{
  bufferlist bl;
  return new RGWHandler_REST_IAM(auth_registry, bl);
}
