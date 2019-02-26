// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/algorithm/string/predicate.hpp>
#include <boost/format.hpp>
#include <boost/optional.hpp>
#include <boost/utility/in_place_factory.hpp>
#include <boost/tokenizer.hpp>

#include "ceph_ver.h"

#include "common/Formatter.h"
#include "common/utf8.h"
#include "common/ceph_json.h"

#include "rgw_rest.h"
#include "rgw_auth.h"
#include "rgw_auth_registry.h"
#include "rgw_rest_sts.h"
#include "rgw_auth_s3.h"

#include "rgw_formats.h"
#include "rgw_client_io.h"

#include "rgw_request.h"
#include "rgw_process.h"
#include "rgw_iam_policy.h"
#include "rgw_iam_policy_keywords.h"

#include "rgw_sts.h"

#include <array>
#include <sstream>
#include <memory>

#include <boost/utility/string_ref.hpp>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

namespace rgw {
namespace auth {
namespace sts {

bool
WebTokenEngine::is_applicable(const std::string& token) const noexcept
{
  return ! token.empty();
}

boost::optional<WebTokenEngine::token_t>
WebTokenEngine::get_from_idp(const DoutPrefixProvider* dpp, const std::string& token) const
{
  //Access token conforming to OAuth2.0
  if (! cct->_conf->rgw_sts_token_introspection_url.empty()) {
    bufferlist introspect_resp;
    RGWHTTPTransceiver introspect_req(cct, "POST", cct->_conf->rgw_sts_token_introspection_url, &introspect_resp);
    //Headers
    introspect_req.append_header("Content-Type", "application/x-www-form-urlencoded");
    string base64_creds = "Basic " + rgw::to_base64(cct->_conf->rgw_sts_client_id + ":" + cct->_conf->rgw_sts_client_secret);
    introspect_req.append_header("Authorization", base64_creds);
    // POST data
    string post_data = "token=" + token;
    introspect_req.set_post_data(post_data);
    introspect_req.set_send_length(post_data.length());

    int res = introspect_req.process();
    if (res < 0) {
      ldpp_dout(dpp, 10) << "HTTP request res: " << res << dendl;
      throw -EINVAL;
    }
    //Debug only
    ldpp_dout(dpp, 20) << "HTTP status: " << introspect_req.get_http_status() << dendl;
    ldpp_dout(dpp, 20) << "JSON Response is: " << introspect_resp.c_str() << dendl;

    JSONParser parser;
    WebTokenEngine::token_t token;
    if (!parser.parse(introspect_resp.c_str(), introspect_resp.length())) {
      ldpp_dout(dpp, 2) << "Malformed json" << dendl;
      throw -EINVAL;
    } else {
      bool is_active;
      JSONDecoder::decode_json("active", is_active, &parser);
      if (! is_active) {
        ldpp_dout(dpp, 0) << "Active state is false"  << dendl;
        throw -ERR_INVALID_IDENTITY_TOKEN;
      }
      JSONDecoder::decode_json("iss", token.iss, &parser);
      JSONDecoder::decode_json("aud", token.aud, &parser);
      JSONDecoder::decode_json("sub", token.sub, &parser);
      JSONDecoder::decode_json("user_name", token.user_name, &parser);
    }
    return token;
  }
  return boost::none;
}

WebTokenEngine::result_t
WebTokenEngine::authenticate( const DoutPrefixProvider* dpp,
                              const std::string& token,
                              const req_state* const s) const
{
  boost::optional<WebTokenEngine::token_t> t;

  if (! is_applicable(token)) {
    return result_t::deny();
  }

  try {
    t = get_from_idp(dpp, token);
  } catch(...) {
    return result_t::deny(-EACCES);
  }

  if (t) {
    auto apl = apl_factory->create_apl_web_identity(cct, s, *t);
    return result_t::grant(std::move(apl));
  }
  return result_t::deny(-EACCES);
}

}; /* namespace sts */
}; /* namespace auth */
}; /* namespace rgw */

int RGWREST_STS::verify_permission()
{
  STS::STSService _sts(s->cct, store, s->user->user_id, s->auth.identity.get());
  sts = std::move(_sts);

  string rArn = s->info.args.get("RoleArn");
  const auto& [ret, role] = sts.getRoleInfo(rArn);
  if (ret < 0) {
    return ret;
  }
  string policy = role.get_assume_role_policy();
  buffer::list bl = buffer::list::static_from_string(policy);

  //Parse the policy
  //TODO - This step should be part of Role Creation
  try {
    const rgw::IAM::Policy p(s->cct, s->user->user_id.tenant, bl);
    //Check if the input role arn is there as one of the Principals in the policy,
    // If yes, then return 0, else -EPERM
    auto p_res = p.eval_principal(s->env, *s->auth.identity);
    if (p_res == rgw::IAM::Effect::Deny) {
      return -EPERM;
    }
    auto c_res = p.eval_conditions(s->env);
    if (c_res == rgw::IAM::Effect::Deny) {
      return -EPERM;
    }
  } catch (rgw::IAM::PolicyParseException& e) {
    ldout(s->cct, 20) << "failed to parse policy: " << e.what() << dendl;
    return -EPERM;
  }

  return 0;
}

void RGWREST_STS::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}

int RGWSTSGetSessionToken::verify_permission()
{
  rgw::IAM::Partition partition = rgw::IAM::Partition::aws;
  rgw::IAM::Service service = rgw::IAM::Service::s3;
  if (!verify_user_permission(this,
                              s,
                              rgw::IAM::ARN(partition, service, "", s->user->user_id.tenant, ""),
                              rgw::IAM::stsGetSessionToken)) {
    return -EACCES;
  }

  return 0;
}

int RGWSTSGetSessionToken::get_params()
{
  duration = s->info.args.get("DurationSeconds");
  serialNumber = s->info.args.get("SerialNumber");
  tokenCode = s->info.args.get("TokenCode");

  if (! duration.empty()) {
    uint64_t duration_in_secs = stoull(duration);
    if (duration_in_secs < STS::GetSessionTokenRequest::getMinDuration() ||
            duration_in_secs > s->cct->_conf->rgw_sts_max_session_duration)
      return -EINVAL;
  }

  return 0;
}

void RGWSTSGetSessionToken::execute()
{
  if (op_ret = get_params(); op_ret < 0) {
    return;
  }

  STS::STSService sts(s->cct, store, s->user->user_id, s->auth.identity.get());

  STS::GetSessionTokenRequest req(duration, serialNumber, tokenCode);
  const auto& [ret, creds] = sts.getSessionToken(req);
  op_ret = std::move(ret);
  //Dump the output
  if (op_ret == 0) {
    s->formatter->open_object_section("GetSessionTokenResponse");
    s->formatter->open_object_section("GetSessionTokenResult");
    s->formatter->open_object_section("Credentials");
    creds.dump(s->formatter);
    s->formatter->close_section();
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGWSTSAssumeRoleWithWebIdentity::get_params()
{
  duration = s->info.args.get("DurationSeconds");
  providerId = s->info.args.get("ProviderId");
  policy = s->info.args.get("Policy");
  roleArn = s->info.args.get("RoleArn");
  roleSessionName = s->info.args.get("RoleSessionName");
  iss = s->info.args.get("provider_id");
  sub = s->info.args.get("sub");
  aud = s->info.args.get("aud");

  if (roleArn.empty() || roleSessionName.empty() || sub.empty() || aud.empty()) {
    ldout(s->cct, 20) << "ERROR: one of role arn or role session name or token is empty" << dendl;
    return -EINVAL;
  }

  if (! policy.empty()) {
    bufferlist bl = bufferlist::static_from_string(policy);
    try {
      const rgw::IAM::Policy p(s->cct, s->user->user_id.tenant, bl);
    }
    catch (rgw::IAM::PolicyParseException& e) {
      ldout(s->cct, 20) << "failed to parse policy: " << e.what() << "policy" << policy << dendl;
      return -ERR_MALFORMED_DOC;
    }
  }

  return 0;
}

void RGWSTSAssumeRoleWithWebIdentity::execute()
{
  if (op_ret = get_params(); op_ret < 0) {
    return;
  }

  STS::AssumeRoleWithWebIdentityRequest req(duration, providerId, policy, roleArn,
                        roleSessionName, iss, sub, aud);
  STS::AssumeRoleWithWebIdentityResponse response = sts.assumeRoleWithWebIdentity(req);
  op_ret = std::move(response.assumeRoleResp.retCode);

  //Dump the output
  if (op_ret == 0) {
    s->formatter->open_object_section("AssumeRoleWithWebIdentityResponse");
    s->formatter->open_object_section("AssumeRoleWithWebIdentityResult");
    encode_json("SubjectFromWebIdentityToken", response.sub , s->formatter);
    encode_json("Audience", response.aud , s->formatter);
    s->formatter->open_object_section("AssumedRoleUser");
    response.assumeRoleResp.user.dump(s->formatter);
    s->formatter->close_section();
    s->formatter->open_object_section("Credentials");
    response.assumeRoleResp.creds.dump(s->formatter);
    s->formatter->close_section();
    encode_json("Provider", response.providerId , s->formatter);
    encode_json("PackedPolicySize", response.assumeRoleResp.packedPolicySize , s->formatter);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGWSTSAssumeRole::get_params()
{
  duration = s->info.args.get("DurationSeconds");
  externalId = s->info.args.get("ExternalId");
  policy = s->info.args.get("Policy");
  roleArn = s->info.args.get("RoleArn");
  roleSessionName = s->info.args.get("RoleSessionName");
  serialNumber = s->info.args.get("SerialNumber");
  tokenCode = s->info.args.get("TokenCode");

  if (roleArn.empty() || roleSessionName.empty()) {
    ldout(s->cct, 20) << "ERROR: one of role arn or role session name is empty" << dendl;
    return -EINVAL;
  }

  if (! policy.empty()) {
    bufferlist bl = bufferlist::static_from_string(policy);
    try {
      const rgw::IAM::Policy p(s->cct, s->user->user_id.tenant, bl);
    }
    catch (rgw::IAM::PolicyParseException& e) {
      ldout(s->cct, 20) << "failed to parse policy: " << e.what() << "policy" << policy << dendl;
      return -ERR_MALFORMED_DOC;
    }
  }

  return 0;
}

void RGWSTSAssumeRole::execute()
{
  if (op_ret = get_params(); op_ret < 0) {
    return;
  }

  STS::AssumeRoleRequest req(duration, externalId, policy, roleArn,
                        roleSessionName, serialNumber, tokenCode);
  STS::AssumeRoleResponse response = sts.assumeRole(req);
  op_ret = std::move(response.retCode);
  //Dump the output
  if (op_ret == 0) {
    s->formatter->open_object_section("AssumeRoleResponse");
    s->formatter->open_object_section("AssumeRoleResult");
    s->formatter->open_object_section("Credentials");
    response.creds.dump(s->formatter);
    s->formatter->close_section();
    s->formatter->open_object_section("AssumedRoleUser");
    response.user.dump(s->formatter);
    s->formatter->close_section();
    encode_json("PackedPolicySize", response.packedPolicySize , s->formatter);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGW_Auth_STS::authorize(const DoutPrefixProvider *dpp,
                            RGWRados *store,
                            const rgw::auth::StrategyRegistry& auth_registry,
                            struct req_state *s)
{
    return rgw::auth::Strategy::apply(dpp, auth_registry.get_sts(), s);
}

void RGWHandler_REST_STS::rgw_sts_parse_input()
{
  const auto max_size = s->cct->_conf->rgw_max_put_param_size;

  int ret = 0;
  bufferlist data;
  std::tie(ret, data) = rgw_rest_read_all_input(s, max_size, false);
  string post_body = data.to_str();
  if (data.length() > 0) {
    ldout(s->cct, 10) << "Content of POST: " << post_body << dendl;

    if (post_body.find("Action") != string::npos) {
      boost::char_separator<char> sep("&");
      boost::tokenizer<boost::char_separator<char>> tokens(post_body, sep);
      for (const auto& t : tokens) {
        auto pos = t.find("=");
        if (pos != string::npos) {
           std::string key = t.substr(0, pos);
           std::string value = t.substr(pos + 1, t.size() - 1);
           if (key == "RoleArn") {
            value = url_decode(value);
           }
           ldout(s->cct, 10) << "Key: " << key << "Value: " << value << dendl;
           s->info.args.append(key, value);
         }
       }
    }
  }
  auto payload_hash = rgw::auth::s3::calc_v4_payload_hash(post_body);
  s->info.args.append("PayloadHash", payload_hash);
}

RGWOp *RGWHandler_REST_STS::op_post()
{
  rgw_sts_parse_input();

  if (s->info.args.exists("Action"))    {
    string action = s->info.args.get("Action");
    if (action == "AssumeRole") {
      return new RGWSTSAssumeRole;
    } else if (action == "GetSessionToken") {
      return new RGWSTSGetSessionToken;
    } else if (action == "AssumeRoleWithWebIdentity") {
      return new RGWSTSAssumeRoleWithWebIdentity;
    }
  }

  return nullptr;
}

int RGWHandler_REST_STS::init(RGWRados *store,
                              struct req_state *s,
                              rgw::io::BasicClient *cio)
{
  s->dialect = "sts";

  if (int ret = RGWHandler_REST_STS::init_from_header(s, RGW_FORMAT_XML, true); ret < 0) {
    ldout(s->cct, 10) << "init_from_header returned err=" << ret <<  dendl;
    return ret;
  }

  return RGWHandler_REST::init(store, s, cio);
}

int RGWHandler_REST_STS::authorize(const DoutPrefixProvider* dpp)
{
  if (s->info.args.exists("Action") && s->info.args.get("Action") == "AssumeRoleWithWebIdentity") {
    return RGW_Auth_STS::authorize(dpp, store, auth_registry, s);
  }
  return RGW_Auth_S3::authorize(dpp, store, auth_registry, s);
}

int RGWHandler_REST_STS::init_from_header(struct req_state* s,
                                          int default_formatter,
                                          bool configurable_format)
{
  string req;
  string first;

  s->prot_flags |= RGW_REST_STS;

  const char *p, *req_name;
  if (req_name = s->relative_uri.c_str(); *req_name == '?') {
    p = req_name;
  } else {
    p = s->info.request_params.c_str();
  }

  s->info.args.set(p);
  s->info.args.parse();

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
RGWRESTMgr_STS::get_handler(struct req_state* const s,
                              const rgw::auth::StrategyRegistry& auth_registry,
                              const std::string& frontend_prefix)
{
  return new RGWHandler_REST_STS(auth_registry);
}
