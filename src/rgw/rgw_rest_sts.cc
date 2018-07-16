#include <boost/algorithm/string/predicate.hpp>
#include <boost/format.hpp>
#include <boost/optional.hpp>
#include <boost/utility/in_place_factory.hpp>

#include "include/assert.h"
#include "ceph_ver.h"

#include "common/Formatter.h"
#include "common/utf8.h"
#include "common/ceph_json.h"

#include "rgw_rest.h"
#include "rgw_auth.h"
#include "rgw_auth_s3.h"
#include "rgw_rest_sts.h"
#include "rgw_formats.h"
#include "rgw_client_io.h"

#include "rgw_request.h"
#include "rgw_process.h"
#include "rgw_iam_policy.h"
#include "rgw_iam_policy_keywords.h"

#include "rgw_rest_sts.h"

#include <array>
#include <sstream>
#include <memory>

#include <boost/utility/string_ref.hpp>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

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
    auto res = p.eval_principal(s->env, *s->auth.identity);
    if (res == rgw::IAM::Effect::Deny) {
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
  const auto& [ret, assumedRoleUser, creds, packedPolicySize] = sts.assumeRole(req);
  op_ret = std::move(ret);
  //Dump the output
  if (op_ret == 0) {
    s->formatter->open_object_section("AssumeRole");
    assumedRoleUser.dump(s->formatter);
    creds.dump(s->formatter);
    encode_json("PackedPolicySize", packedPolicySize , s->formatter);
    s->formatter->close_section();
  }
}

RGWOp *RGWHandler_REST_STS::op_post()
{
  if (s->info.args.exists("Action"))    {
    if (string action = s->info.args.get("Action"); action == "AssumeRole") {
	  return new RGWSTSAssumeRole;
    }
  }
  return nullptr;
}

int RGWHandler_REST_STS::init(RGWRados *store,
                              struct req_state *s,
                              rgw::io::BasicClient *cio)
{
  s->dialect = "sts";

  if (int ret = RGWHandler_REST_STS::init_from_header(s, RGW_FORMAT_JSON, true); ret < 0) {
    ldout(s->cct, 10) << "init_from_header returned err=" << ret <<  dendl;
    return ret;
  }

  return RGWHandler_REST::init(store, s, cio);
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
