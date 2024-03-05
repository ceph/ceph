// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>
#include <regex>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"

#include "include/types.h"
#include "rgw_string.h"

#include "rgw_common.h"
#include "rgw_iam_managed_policy.h"
#include "rgw_op.h"
#include "rgw_process_env.h"
#include "rgw_rest.h"
#include "rgw_rest_iam.h"
#include "rgw_rest_user_policy.h"
#include "rgw_sal.h"
#include "services/svc_zone.h"

#define dout_subsys ceph_subsys_rgw

RGWRestUserPolicy::RGWRestUserPolicy(uint64_t action, uint32_t perm)
  : action(action), perm(perm)
{
}

void RGWRestUserPolicy::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}

int RGWRestUserPolicy::get_params()
{
  user_name = s->info.args.get("UserName");
  if (!validate_iam_user_name(user_name, s->err.message)) {
    return -EINVAL;
  }
  return 0;
}

int RGWRestUserPolicy::init_processing(optional_yield y)
{
  int r = get_params();
  if (r < 0) {
    return r;
  }

  if (const auto* id = std::get_if<rgw_account_id>(&s->owner.id); id) {
    account_id = *id;

    // look up account user by UserName
    const std::string& tenant = s->auth.identity->get_tenant();
    r = driver->load_account_user_by_name(this, y, account_id,
                                          tenant, user_name, &user);

    if (r == -ENOENT) {
      s->err.message = "No such UserName in the account";
      return -ERR_NO_SUCH_ENTITY;
    }
    if (r >= 0) {
      // user ARN includes account id, path, and display name
      const RGWUserInfo& info = user->get_info();
      const std::string resource = string_cat_reserve(info.path, info.display_name);
      user_arn = rgw::ARN{resource, "user", account_id, true};
    }
  } else {
    // interpret UserName as a uid with optional tenant
    const auto uid = rgw_user{user_name};
    // user ARN includes tenant and user id
    user_arn = rgw::ARN{uid.id, "user", uid.tenant};

    user = driver->get_user(uid);
    r = user->load_user(this, y);
    if (r == -ENOENT) {
      s->err.message = "No such UserName in the tenant";
      return -ERR_NO_SUCH_ENTITY;
    }
  }

  return r;
}

int RGWRestUserPolicy::check_caps(const RGWUserCaps& caps)
{
  return caps.check_cap("user-policy", perm);
}

int RGWRestUserPolicy::verify_permission(optional_yield y)
{
  if (s->auth.identity->is_anonymous()) {
    return -EACCES;
  }

  // admin caps are required for non-account users
  if (check_caps(s->user->get_caps()) == 0) {
    return 0;
  }

  if (! verify_user_permission(this, s, user_arn, action)) {
    return -EACCES;
  }
  return 0;
}


RGWPutUserPolicy::RGWPutUserPolicy(const ceph::bufferlist& post_body)
  : RGWRestUserPolicy(rgw::IAM::iamPutUserPolicy, RGW_CAP_WRITE),
    post_body(post_body)
{
}

int RGWPutUserPolicy::get_params()
{
  policy_name = s->info.args.get("PolicyName");
  if (!validate_iam_policy_name(policy_name, s->err.message)) {
    return -EINVAL;
  }

  policy = s->info.args.get("PolicyDocument");
  if (policy.empty()) {
    s->err.message = "Missing required element PolicyDocument";
    return -EINVAL;
  }

  return RGWRestUserPolicy::get_params();
}

int RGWPutUserPolicy::forward_to_master(optional_yield y, const rgw::SiteConfig& site)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
    return -EINVAL;
  }

  s->info.args.remove("UserName");
  s->info.args.remove("PolicyName");
  s->info.args.remove("PolicyDocument");
  s->info.args.remove("Action");
  s->info.args.remove("Version");

  int r = forward_iam_request_to_master(this, site, s->user->get_info(),
                                        post_body, parser, s->info, y);
  if (r < 0) {
    ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << r << dendl;
    return r;
  }
  return 0;
}

void RGWPutUserPolicy::execute(optional_yield y)
{
  // validate the policy document
  try {
    // non-account identity policy is restricted to the current tenant
    const std::string* policy_tenant = account_id.empty() ?
        &s->user->get_tenant() : nullptr;

    const rgw::IAM::Policy p(
      s->cct, policy_tenant, policy,
      s->cct->_conf.get_val<bool>("rgw_policy_reject_invalid_principals"));
  } catch (const rgw::IAM::PolicyParseException& e) {
    ldpp_dout(this, 5) << "failed to parse policy: " << e.what() << dendl;
    s->err.message = e.what();
    op_ret = -ERR_MALFORMED_DOC;
    return;
  }

  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    op_ret = forward_to_master(y, site);
    if (op_ret) {
      return;
    }
  }

  op_ret = retry_raced_user_write(this, y, user.get(),
      [this, y] {
        rgw::sal::Attrs& attrs = user->get_attrs();
        std::map<std::string, std::string> policies;
        if (auto it = attrs.find(RGW_ATTR_USER_POLICY); it != attrs.end()) try {
          decode(policies, it->second);
        } catch (const buffer::error& err) {
          ldpp_dout(this, 0) << "ERROR: failed to decode user policies" << dendl;
          return -EIO;
        }

        policies[policy_name] = policy;

        constexpr unsigned int USER_POLICIES_MAX_NUM = 100;
        const unsigned int max_num = s->cct->_conf->rgw_user_policies_max_num < 0 ?
          USER_POLICIES_MAX_NUM : s->cct->_conf->rgw_user_policies_max_num;
        if (policies.size() > max_num) {
          ldpp_dout(this, 4) << "IAM user policies has reached the num config: "
                             << max_num << ", cant add another" << dendl;
          s->err.message =
              "The number of IAM user policies should not exceed allowed limit "
              "of " +
              std::to_string(max_num) + " policies.";
          return -ERR_LIMIT_EXCEEDED;
        }

        bufferlist bl;
        encode(policies, bl);
        attrs[RGW_ATTR_USER_POLICY] = std::move(bl);

        return user->store_user(s, y, false);
      });

  if (op_ret == 0) {
    s->formatter->open_object_section_in_ns("PutUserPolicyResponse", RGW_REST_IAM_XMLNS);
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}


RGWGetUserPolicy::RGWGetUserPolicy()
  : RGWRestUserPolicy(rgw::IAM::iamGetUserPolicy, RGW_CAP_READ)
{
}

int RGWGetUserPolicy::get_params()
{
  policy_name = s->info.args.get("PolicyName");
  if (!validate_iam_policy_name(policy_name, s->err.message)) {
    return -EINVAL;
  }

  return RGWRestUserPolicy::get_params();
}

void RGWGetUserPolicy::execute(optional_yield y)
{
  std::map<std::string, std::string> policies;
  if (auto it = user->get_attrs().find(RGW_ATTR_USER_POLICY); it != user->get_attrs().end()) {
    try {
      decode(policies, it->second);
    } catch (buffer::error& err) {
      ldpp_dout(this, 0) << "ERROR: failed to decode user policies" << dendl;
      op_ret = -EIO;
      return;
    }
  }

  auto policy = policies.find(policy_name);
  if (policy == policies.end()) {
    s->err.message = "No such PolicyName on the user";
    op_ret = -ERR_NO_SUCH_ENTITY;
    return;
  }

  s->formatter->open_object_section_in_ns("GetUserPolicyResponse", RGW_REST_IAM_XMLNS);
  s->formatter->open_object_section("ResponseMetadata");
  s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->close_section();
  s->formatter->open_object_section("GetUserPolicyResult");
  encode_json("PolicyName", policy_name , s->formatter);
  encode_json("UserName", user_name, s->formatter);
  encode_json("PolicyDocument", policy->second, s->formatter);
  s->formatter->close_section();
  s->formatter->close_section();
}


RGWListUserPolicies::RGWListUserPolicies()
  : RGWRestUserPolicy(rgw::IAM::iamListUserPolicies, RGW_CAP_READ)
{
}

int RGWListUserPolicies::get_params()
{
  marker = s->info.args.get("Marker");

  int r = s->info.args.get_int("MaxItems", &max_items, max_items);
  if (r < 0 || max_items > 1000) {
    s->err.message = "Invalid value for MaxItems";
    return -EINVAL;
  }

  return RGWRestUserPolicy::get_params();
}

void RGWListUserPolicies::execute(optional_yield y)
{
  std::map<std::string, std::string> policies;
  if (auto it = user->get_attrs().find(RGW_ATTR_USER_POLICY); it != user->get_attrs().end()) {
    try {
      decode(policies, it->second);
    } catch (buffer::error& err) {
      ldpp_dout(this, 0) << "ERROR: failed to decode user policies" << dendl;
      op_ret = -EIO;
      return;
    }
  }

  s->formatter->open_object_section_in_ns("ListUserPoliciesResponse", RGW_REST_IAM_XMLNS);
  s->formatter->open_object_section("ResponseMetadata");
  s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->close_section();
  s->formatter->open_object_section("ListUserPoliciesResult");
  s->formatter->open_array_section("PolicyNames");
  auto policy = policies.lower_bound(marker);
  for (; policy != policies.end() && max_items > 0; ++policy, --max_items) {
    s->formatter->dump_string("member", policy->first);
  }
  s->formatter->close_section(); // PolicyNames
  const bool is_truncated = (policy != policies.end());
  encode_json("IsTruncated", is_truncated, s->formatter);
  if (is_truncated) {
    encode_json("Marker", policy->first, s->formatter);
  }
  s->formatter->close_section(); // ListUserPoliciesResult
  s->formatter->close_section(); // ListUserPoliciesResponse
}


RGWDeleteUserPolicy::RGWDeleteUserPolicy(const ceph::bufferlist& post_body)
  : RGWRestUserPolicy(rgw::IAM::iamDeleteUserPolicy, RGW_CAP_WRITE),
    post_body(post_body)
{
}

int RGWDeleteUserPolicy::get_params()
{
  policy_name = s->info.args.get("PolicyName");
  if (!validate_iam_policy_name(policy_name, s->err.message)) {
    return -EINVAL;
  }

  return RGWRestUserPolicy::get_params();
}

int RGWDeleteUserPolicy::forward_to_master(optional_yield y, const rgw::SiteConfig& site)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
    return -EINVAL;
  }

  s->info.args.remove("UserName");
  s->info.args.remove("PolicyName");
  s->info.args.remove("Action");
  s->info.args.remove("Version");

  int r = forward_iam_request_to_master(this, site, s->user->get_info(),
                                        post_body, parser, s->info, y);
  if (r < 0) {
    ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << r << dendl;
    return r;
  }
  return 0;
}

void RGWDeleteUserPolicy::execute(optional_yield y)
{
  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    op_ret = forward_to_master(y, site);
    if (op_ret) {
      return;
    }
  }

  op_ret = retry_raced_user_write(this, y, user.get(),
      [this, y, &site] {
        rgw::sal::Attrs& attrs = user->get_attrs();
        std::map<std::string, std::string> policies;
        if (auto it = attrs.find(RGW_ATTR_USER_POLICY); it != attrs.end()) try {
          decode(policies, it->second);
        } catch (const buffer::error& err) {
          ldpp_dout(this, 0) << "ERROR: failed to decode user policies" << dendl;
          return -EIO;
        }

        auto policy = policies.find(policy_name);
        if (policy == policies.end()) {
          if (!site.is_meta_master()) {
            return 0; // delete succeeded on the master
          }
          s->err.message = "No such PolicyName on the user";
          return -ERR_NO_SUCH_ENTITY;
        }
        policies.erase(policy);

        bufferlist bl;
        encode(policies, bl);
        attrs[RGW_ATTR_USER_POLICY] = std::move(bl);

        return user->store_user(s, y, false);
      });

  if (op_ret < 0) {
    return;
  }

  s->formatter->open_object_section_in_ns("DeleteUserPoliciesResponse", RGW_REST_IAM_XMLNS);
  s->formatter->open_object_section("ResponseMetadata");
  s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->close_section();
  s->formatter->close_section();
}


class RGWAttachUserPolicy_IAM : public RGWRestUserPolicy {
  bufferlist post_body;
  std::string policy_arn;

  int get_params() override;
  int forward_to_master(optional_yield y, const rgw::SiteConfig& site);
 public:
  explicit RGWAttachUserPolicy_IAM(const ceph::bufferlist& post_body)
    : RGWRestUserPolicy(rgw::IAM::iamAttachUserPolicy, RGW_CAP_WRITE),
      post_body(post_body) {}

  void execute(optional_yield y) override;
  const char* name() const override { return "attach_user_policy"; }
  RGWOpType get_type() override { return RGW_OP_ATTACH_USER_POLICY; }
};

int RGWAttachUserPolicy_IAM::get_params()
{
  policy_arn = s->info.args.get("PolicyArn");
  if (!validate_iam_policy_arn(policy_arn, s->err.message)) {
    return -EINVAL;
  }

  return RGWRestUserPolicy::get_params();
}

int RGWAttachUserPolicy_IAM::forward_to_master(optional_yield y, const rgw::SiteConfig& site)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
    return -EINVAL;
  }

  s->info.args.remove("UserName");
  s->info.args.remove("PolicyArn");
  s->info.args.remove("Action");
  s->info.args.remove("Version");

  int r = forward_iam_request_to_master(this, site, s->user->get_info(),
                                        post_body, parser, s->info, y);
  if (r < 0) {
    ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << r << dendl;
    return r;
  }
  return 0;
}

void RGWAttachUserPolicy_IAM::execute(optional_yield y)
{
  // validate the policy arn
  try {
    const auto p = rgw::IAM::get_managed_policy(s->cct, policy_arn);
    if (!p) {
      op_ret = ERR_NO_SUCH_ENTITY;
      s->err.message = "The requested PolicyArn is not recognized";
      return;
    }
  } catch (const rgw::IAM::PolicyParseException& e) {
    ldpp_dout(this, 5) << "failed to parse policy: " << e.what() << dendl;
    s->err.message = e.what();
    op_ret = -ERR_MALFORMED_DOC;
    return;
  }

  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    op_ret = forward_to_master(y, site);
    if (op_ret) {
      return;
    }
  }

  op_ret = retry_raced_user_write(this, y, user.get(),
      [this, y] {
        rgw::sal::Attrs& attrs = user->get_attrs();
        rgw::IAM::ManagedPolicies policies;
        if (auto it = attrs.find(RGW_ATTR_MANAGED_POLICY); it != attrs.end()) try {
          decode(policies, it->second);
        } catch (buffer::error& err) {
          ldpp_dout(this, 0) << "ERROR: failed to decode user policies" << dendl;
          return -EIO;
        }
        policies.arns.insert(policy_arn);

        bufferlist bl;
        encode(policies, bl);
        attrs[RGW_ATTR_MANAGED_POLICY] = std::move(bl);

        return user->store_user(this, y, false);
      });

  if (op_ret == 0) {
    s->formatter->open_object_section_in_ns("AttachUserPolicyResponse", RGW_REST_IAM_XMLNS);
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}


class RGWRestAttachedUserPolicy : public RGWRestUserPolicy {
 public:
  using RGWRestUserPolicy::RGWRestUserPolicy;
  int init_processing(optional_yield y) override;
};

int RGWRestAttachedUserPolicy::init_processing(optional_yield y)
{
  // managed policy is only supported for account users. adding them to
  // non-account roles would give blanket permissions to all buckets
  if (!s->auth.identity->get_account()) {
    s->err.message = "Managed policies are only supported for account users";
    return -ERR_METHOD_NOT_ALLOWED;
  }

  return RGWRestUserPolicy::init_processing(y);
}

class RGWDetachUserPolicy_IAM : public RGWRestAttachedUserPolicy {
  bufferlist post_body;
  std::string policy_arn;

  int get_params() override;
  int forward_to_master(optional_yield y, const rgw::SiteConfig& site);
 public:
  explicit RGWDetachUserPolicy_IAM(const bufferlist& post_body)
    : RGWRestAttachedUserPolicy(rgw::IAM::iamDetachUserPolicy, RGW_CAP_WRITE),
      post_body(post_body) {}

  void execute(optional_yield y) override;
  const char* name() const override { return "detach_user_policy"; }
  RGWOpType get_type() override { return RGW_OP_DETACH_USER_POLICY; }
};

int RGWDetachUserPolicy_IAM::get_params()
{
  policy_arn = s->info.args.get("PolicyArn");
  if (!validate_iam_policy_arn(policy_arn, s->err.message)) {
    return -EINVAL;
  }

  return RGWRestAttachedUserPolicy::get_params();
}

int RGWDetachUserPolicy_IAM::forward_to_master(optional_yield y, const rgw::SiteConfig& site)
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
    return -EINVAL;
  }

  s->info.args.remove("UserName");
  s->info.args.remove("PolicyArn");
  s->info.args.remove("Action");
  s->info.args.remove("Version");

  int r = forward_iam_request_to_master(this, site, s->user->get_info(),
                                        post_body, parser, s->info, y);
  if (r < 0) {
    ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << r << dendl;
    return r;
  }
  return 0;
}

void RGWDetachUserPolicy_IAM::execute(optional_yield y)
{
  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    op_ret = forward_to_master(y, site);
    if (op_ret) {
      return;
    }
  }

  op_ret = retry_raced_user_write(this, y, user.get(),
      [this, y, &site] {
        rgw::sal::Attrs& attrs = user->get_attrs();
        rgw::IAM::ManagedPolicies policies;
        if (auto it = attrs.find(RGW_ATTR_MANAGED_POLICY); it != attrs.end()) try {
          decode(policies, it->second);
        } catch (const buffer::error& err) {
          ldpp_dout(this, 0) << "ERROR: failed to decode user policies" << dendl;
          return -EIO;
        }

        auto i = policies.arns.find(policy_arn);
        if (i == policies.arns.end()) {
          if (!site.is_meta_master()) {
            return 0; // delete succeeded on the master
          }
          s->err.message = "No such PolicyArn on the user";
          return ERR_NO_SUCH_ENTITY;
        }
        policies.arns.erase(i);

        bufferlist bl;
        encode(policies, bl);
        attrs[RGW_ATTR_MANAGED_POLICY] = std::move(bl);

        return user->store_user(this, y, false);
      });

  if (op_ret == 0) {
    s->formatter->open_object_section_in_ns("DetachUserPolicyResponse", RGW_REST_IAM_XMLNS);
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}


class RGWListAttachedUserPolicies_IAM : public RGWRestAttachedUserPolicy {
  std::string marker;
  int max_items = 100;
  int get_params() override;
 public:
  RGWListAttachedUserPolicies_IAM()
   : RGWRestAttachedUserPolicy(rgw::IAM::iamListAttachedUserPolicies, RGW_CAP_READ)
  {}
  void execute(optional_yield y) override;
  const char* name() const override { return "list_attached_user_policies"; }
  RGWOpType get_type() override { return RGW_OP_LIST_ATTACHED_USER_POLICIES; }
};

int RGWListAttachedUserPolicies_IAM::get_params()
{
  marker = s->info.args.get("Marker");

  int r = s->info.args.get_int("MaxItems", &max_items, max_items);
  if (r < 0 || max_items > 1000) {
    s->err.message = "Invalid value for MaxItems";
    return -EINVAL;
  }

  return RGWRestAttachedUserPolicy::get_params();
}

void RGWListAttachedUserPolicies_IAM::execute(optional_yield y)
{
  rgw::IAM::ManagedPolicies policies;
  const auto& attrs = user->get_attrs();
  if (auto it = attrs.find(RGW_ATTR_MANAGED_POLICY); it != attrs.end()) {
    try {
      decode(policies, it->second);
    } catch (buffer::error& err) {
      ldpp_dout(this, 0) << "ERROR: failed to decode user policies" << dendl;
      op_ret = -EIO;
      return;
    }
  }

  s->formatter->open_object_section_in_ns("ListAttachedUserPoliciesResponse", RGW_REST_IAM_XMLNS);
  s->formatter->open_object_section("ResponseMetadata");
  s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->close_section();
  s->formatter->open_object_section("ListAttachedUserPoliciesResult");
  s->formatter->open_array_section("AttachedPolicies");
  auto policy = policies.arns.lower_bound(marker);
  for (; policy != policies.arns.end() && max_items > 0; ++policy, --max_items) {
    s->formatter->open_object_section("member");
    std::string_view arn = *policy;
    if (auto p = arn.find('/'); p != arn.npos) {
      s->formatter->dump_string("PolicyName", arn.substr(p + 1));
    }
    s->formatter->dump_string("PolicyArn", arn);
    s->formatter->close_section(); // member
  }
  s->formatter->close_section(); // AttachedPolicies
  const bool is_truncated = (policy != policies.arns.end());
  encode_json("IsTruncated", is_truncated, s->formatter);
  if (is_truncated) {
    encode_json("Marker", *policy, s->formatter);
  }
  s->formatter->close_section(); // ListAttachedUserPoliciesResult
  s->formatter->close_section(); // ListAttachedUserPoliciesResponse
}


RGWOp* make_iam_attach_user_policy_op(const ceph::bufferlist& post_body) {
  return new RGWAttachUserPolicy_IAM(post_body);
}

RGWOp* make_iam_detach_user_policy_op(const ceph::bufferlist& post_body) {
  return new RGWDetachUserPolicy_IAM(post_body);
}

RGWOp* make_iam_list_attached_user_policies_op(const ceph::bufferlist& unused) {
  return new RGWListAttachedUserPolicies_IAM();
}
