// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <algorithm>
#include <boost/tokenizer.hpp>
#include <optional>
#include <regex>
#include "include/function2.hpp"
#include "rgw_iam_policy.h"
#include "rgw_rest_pubsub.h"
#include "rgw_pubsub_push.h"
#include "rgw_pubsub.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_arn.h"
#include "rgw_auth_s3.h"
#include "rgw_notify.h"
#include "services/svc_zone.h"
#include "common/dout.h"
#include "rgw_url.h"
#include "rgw_process_env.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

bool verify_transport_security(CephContext *cct, const RGWEnv& env) {
  const auto is_secure = rgw_transport_is_secure(cct, env);
  if (!is_secure && g_conf().get_val<bool>("rgw_allow_notification_secrets_in_cleartext")) {
    ldout(cct, 0) << "WARNING: bypassing endpoint validation, allows sending secrets over insecure transport" << dendl;
    return true;
  }
  return is_secure;
}

// make sure that endpoint is a valid URL
// make sure that if user/password are passed inside URL, it is over secure connection
// update rgw_pubsub_dest to indicate that a password is stored in the URL
bool validate_and_update_endpoint_secret(rgw_pubsub_dest& dest, CephContext *cct,
                                         const req_info& ri, std::string& message)
{
  if (dest.push_endpoint.empty()) {
    return true;
  }
  std::string user;
  std::string password;
  if (!rgw::parse_url_userinfo(dest.push_endpoint, user, password)) {
    message = "Malformed URL for push-endpoint";
    return false;
  }

  const auto& args=ri.args;
  auto topic_user_name=args.get_optional("user-name");
  auto topic_password=args.get_optional("password");

  // check if username/password was already supplied via topic attributes
  // and if also provided as part of the endpoint URL issue a warning
  if (topic_user_name.has_value()) {
    if (!user.empty()) {
      message = "Username provided via both topic attributes and endpoint URL: using topic attributes";
    }
    user = topic_user_name.get();
  }
  if (topic_password.has_value()) {
    if (!password.empty()) {
      message = "Password provided via both topic attributes and endpoint URL: using topic attributes";
    }
    password = topic_password.get();
  }

  // this should be verified inside parse_url()
  ceph_assert(user.empty() == password.empty());
  if (!user.empty()) {
    dest.stored_secret = true;
    if (!verify_transport_security(cct, *ri.env)) {
      message = "Topic contains secrets that must be transmitted over a secure transport";
      return false;
    }
  }
  return true;
}

bool validate_topic_name(const std::string& name, std::string& message)
{
  constexpr size_t max_topic_name_length = 256;
  if (name.size() > max_topic_name_length) {
    message = "Name cannot be longer than 256 characters";
    return false;
  }

  std::regex pattern("[A-Za-z0-9_-]+");
  if (!std::regex_match(name, pattern)) {
    message = "Name must be made up of only uppercase and lowercase "
        "ASCII letters, numbers, underscores, and hyphens";
    return false;
  }
  return true;
}

auto validate_topic_arn(const std::string& str, std::string& message)
  -> boost::optional<rgw::ARN>
{
  if (str.empty()) {
    message = "Missing required element TopicArn";
    return boost::none;
  }
  auto arn = rgw::ARN::parse(str);
  if (!arn || arn->resource.empty()) {
    message = "Invalid value for TopicArn";
    return boost::none;
  }
  return arn;
}

const std::string& get_account_or_tenant(const rgw_owner& owner)
{
  return std::visit(fu2::overload(
      [] (const rgw_user& u) -> const std::string& { return u.tenant; },
      [] (const rgw_account_id& a) -> const std::string& { return a; }
      ), owner);
}

bool topic_has_endpoint_secret(const rgw_pubsub_topic& topic) {
    return topic.dest.stored_secret;
}

bool topics_has_endpoint_secret(const rgw_pubsub_topics& topics) {
    for (const auto& topic : topics.topics) {
        if (topic_has_endpoint_secret(topic.second)) return true;
    }
    return false;
}

static bool topic_needs_queue(const rgw_pubsub_dest& dest)
{
  return !dest.push_endpoint.empty() && dest.persistent;
}

auto get_policy_from_text(req_state* const s, const std::string& policy_text)
  -> boost::optional<rgw::IAM::Policy>
{
  try {
    return rgw::IAM::Policy(
        s->cct, nullptr, policy_text,
        s->cct->_conf.get_val<bool>("rgw_policy_reject_invalid_principals"));
  } catch (rgw::IAM::PolicyParseException& e) {
    ldout(s->cct, 1) << "failed to parse policy: '" << policy_text
                     << "' with error: " << e.what() << dendl;
    s->err.message = e.what();
    return boost::none;
  }
}

using rgw::IAM::Effect;
using rgw::IAM::Policy;

bool verify_topic_permission(const DoutPrefixProvider* dpp, req_state* s,
                             const rgw_owner& owner, const rgw::ARN& arn,
                             const boost::optional<Policy>& policy,
                             uint64_t op)
{
  if (s->auth.identity->get_account()) {
    const bool account_root = (s->auth.identity->get_identity_type() == TYPE_ROOT);
    if (!s->auth.identity->is_owner_of(owner)) {
      ldpp_dout(dpp, 4) << "cross-account request for resource owner "
          << owner << " != " << s->owner.id << dendl;
      // cross-account requests evaluate the identity-based policies separately
      // from the resource-based policies and require Allow from both
      const auto identity_res = evaluate_iam_policies(
          dpp, s->env, *s->auth.identity, account_root, op, arn,
          {}, s->iam_identity_policies, s->session_policies);
      if (identity_res == Effect::Deny) {
        return false;
      }
      const auto resource_res = evaluate_iam_policies(
          dpp, s->env, *s->auth.identity, false, op, arn,
          policy, {}, {});
      return identity_res == Effect::Allow && resource_res == Effect::Allow;
    } else {
      // require an Allow from either identity- or resource-based policy
      return Effect::Allow == evaluate_iam_policies(
          dpp, s->env, *s->auth.identity, account_root, op, arn,
          policy, s->iam_identity_policies, s->session_policies);
    }
  }

  constexpr bool account_root = false;
  const auto effect = evaluate_iam_policies(
      dpp, s->env, *s->auth.identity, account_root, op, arn,
      policy, s->iam_identity_policies, s->session_policies);
  if (effect == Effect::Deny) {
    return false;
  }
  if (effect == Effect::Allow) {
    return true;
  }

  if (s->auth.identity->is_owner_of(owner)) {
    ldpp_dout(dpp, 10) << __func__ << ": granted to resource owner" << dendl;
    return true;
  }

  if (!policy) {
    if (op == rgw::IAM::snsPublish &&
        !s->cct->_conf->rgw_topic_require_publish_policy) {
      return true;
    }

    if (std::visit([] (const auto& o) { return o.empty(); }, owner)) {
      // if we don't know the original user and there is no policy
      // we will not reject the request.
      // this is for compatibility with versions that did not store the user in the topic
      return true;
    }
  }

  s->err.message = "Topic was created by another user.";
  return false;
}

// parse topic policy if present and evaluate permissions
bool verify_topic_permission(const DoutPrefixProvider* dpp, req_state* s,
                             const rgw_pubsub_topic& topic,
                             const rgw::ARN& arn, uint64_t op)
{
  boost::optional<Policy> policy;
  if (!topic.policy_text.empty()) {
    policy = get_policy_from_text(s, topic.policy_text);
    if (!policy) {
      return false;
    }
  }

  return verify_topic_permission(dpp, s, topic.owner, arn, policy, op);
}

// command (AWS compliant): 
// POST
// Action=CreateTopic&Name=<topic-name>[&OpaqueData=data][&push-endpoint=<endpoint>[&persistent][&<arg1>=<value1>]]
class RGWPSCreateTopicOp : public RGWOp {
  private:
  bufferlist bl_post_body;
  std::string topic_name;
  rgw::ARN topic_arn;
  std::optional<rgw_pubsub_topic> topic;
  rgw_pubsub_dest dest;
  std::string opaque_data;
  std::string policy_text;

  int get_params() {
    topic_name = s->info.args.get("Name");
    if (!validate_topic_name(topic_name, s->err.message)) {
      return -EINVAL;
    }

    opaque_data = s->info.args.get("OpaqueData");

    dest.push_endpoint = s->info.args.get("push-endpoint");
    s->info.args.get_bool("persistent", &dest.persistent, false);
    s->info.args.get_int("time_to_live", reinterpret_cast<int *>(&dest.time_to_live), rgw::notify::DEFAULT_GLOBAL_VALUE);
    s->info.args.get_int("max_retries", reinterpret_cast<int *>(&dest.max_retries), rgw::notify::DEFAULT_GLOBAL_VALUE);
    s->info.args.get_int("retry_sleep_duration", reinterpret_cast<int *>(&dest.retry_sleep_duration), rgw::notify::DEFAULT_GLOBAL_VALUE);

    if (!validate_and_update_endpoint_secret(dest, s->cct, s->info, s->err.message)) {
      return -EINVAL;
    }
    // Store topic Policy.
    policy_text = s->info.args.get("Policy");
    if (!policy_text.empty() && !get_policy_from_text(s, policy_text)) {
      return -ERR_MALFORMED_DOC;
    }

    // Remove the args that are parsed, so the push_endpoint_args only contains
    // necessary one's which is parsed after this if. but only if master zone,
    // else we do not remove as request is forwarded to master.
    if (driver->is_meta_master()) {
      s->info.args.remove("OpaqueData");
      s->info.args.remove("push-endpoint");
      s->info.args.remove("persistent");
      s->info.args.remove("time_to_live");
      s->info.args.remove("max_retries");
      s->info.args.remove("retry_sleep_duration");
      s->info.args.remove("Policy");
    }
    for (const auto& param : s->info.args.get_params()) {
      if (param.first == "Action" || param.first == "Name" || param.first == "PayloadHash") {
        continue;
      }
      dest.push_endpoint_args.append(param.first+"="+param.second+"&");
    }

    if (!dest.push_endpoint_args.empty()) {
      // remove last separator
      dest.push_endpoint_args.pop_back();
    }

    // dest object only stores endpoint info
    dest.arn_topic = topic_name;
    // the topic ARN will be sent in the reply
    topic_arn = rgw::ARN{rgw::Partition::aws, rgw::Service::sns,
        driver->get_zone()->get_zonegroup().get_name(),
        get_account_or_tenant(s->owner.id), topic_name};
    return 0;
  }

 public:
  explicit RGWPSCreateTopicOp(bufferlist bl_post_body)
    : bl_post_body(std::move(bl_post_body)) {}

  int init_processing(optional_yield y) override {
    int ret = get_params();
    if (ret < 0) {
      return ret;
    }
    ret = RGWOp::init_processing(y);
    if (ret < 0) {
      return ret;
    }

      // account users require the notification_v2 format to index the topic metadata
    if (s->auth.identity->get_account() &&
        !rgw::all_zonegroups_support(*s->penv.site, rgw::zone_features::notification_v2)) {
      s->err.message = "The 'notification_v2' zone feature must be enabled "
          "to create topics in an account";
      return -EINVAL;
    }

    // try to load existing topic for owner and policy
    const RGWPubSub ps(driver, get_account_or_tenant(s->owner.id), *s->penv.site);
    rgw_pubsub_topic result;
    ret = ps.get_topic(this, topic_name, result, y, nullptr);
    if (ret == -ENOENT) {
      // topic not present
    } else if (ret < 0) {
      ldpp_dout(this, 1) << "failed to read topic '" << topic_name
          << "', with error:" << ret << dendl;
      return ret;
    } else {
      topic = std::move(result);
    }
    return 0;
  }

  int verify_permission(optional_yield y) override {
    if (topic) {
      // consult topic policy for overwrite permission
      if (!verify_topic_permission(this, s, *topic, topic_arn,
                                   rgw::IAM::snsCreateTopic)) {
        return -ERR_AUTHORIZATION;
      }
    } else {
      // if no topic policy exists, just check identity policies for denies
      // account users require an Allow, non-account users just check for Deny
      const bool mandatory_policy{s->auth.identity->get_account()};
      if (!verify_user_permission(this, s, topic_arn,
                                  rgw::IAM::snsCreateTopic,
                                  mandatory_policy)) {
        return -ERR_AUTHORIZATION;
      }
    }
    return 0;
  }

  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute(optional_yield) override;

  const char* name() const override { return "pubsub_topic_create"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_TOPIC_CREATE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  void send_response() override {
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/xml");

    if (op_ret < 0) {
      return;
    }

    const auto f = s->formatter;
    f->open_object_section_in_ns("CreateTopicResponse", RGW_REST_SNS_XMLNS);
    f->open_object_section("CreateTopicResult");
    encode_xml("TopicArn", topic_arn.to_string(), f);
    f->close_section(); // CreateTopicResult
    f->open_object_section("ResponseMetadata");
    encode_xml("RequestId", s->req_id, f); 
    f->close_section(); // ResponseMetadata
    f->close_section(); // CreateTopicResponse
    rgw_flush_formatter_and_reset(s, f);
  }
};

void RGWPSCreateTopicOp::execute(optional_yield y) {
  // master request will replicate the topic creation.
  if (!driver->is_meta_master()) {
    op_ret = rgw_forward_request_to_master(
        this, *s->penv.site, s->owner.id, &bl_post_body, nullptr, s->info, y);
    if (op_ret < 0) {
      ldpp_dout(this, 4)
          << "CreateTopic forward_request_to_master returned ret = " << op_ret
          << dendl;
      return;
    }
  }

  // don't add a persistent queue if we already have one
  const bool already_persistent = topic && topic_needs_queue(topic->dest);
  if (!already_persistent && topic_needs_queue(dest)) {
    // initialize the persistent queue's location, using ':' as the namespace
    // delimiter because its inclusion in a TopicName would break ARNs
    dest.persistent_queue = string_cat_reserve(
        get_account_or_tenant(s->owner.id), ":", topic_name);

    op_ret = driver->add_persistent_topic(this, y, dest.persistent_queue);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "CreateTopic Action failed to create queue for "
                            "persistent topics. error:"
                         << op_ret << dendl;
      return;
    }
  } else if (already_persistent) {  // redundant call to CreateTopic
    dest.persistent_queue = topic->dest.persistent_queue;
  }
  const RGWPubSub ps(driver, get_account_or_tenant(s->owner.id), *s->penv.site);
  op_ret = ps.create_topic(this, topic_name, dest, topic_arn.to_string(),
                           opaque_data, s->owner.id, policy_text, y);
  if (op_ret < 0) {
    ldpp_dout(this, 4) << "failed to create topic '" << topic_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldpp_dout(this, 20) << "successfully created topic '" << topic_name << "'" << dendl;
}

// command (AWS compliant): 
// POST 
// Action=ListTopics
class RGWPSListTopicsOp : public RGWOp {
private:
  rgw_pubsub_topics result;
  std::string next_token;

public:
  int verify_permission(optional_yield) override {
    // check account permissions up front
    if (s->auth.identity->get_account() &&
        !verify_user_permission(this, s, {}, rgw::IAM::snsListTopics)) {
      return -ERR_AUTHORIZATION;
    }

    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute(optional_yield) override;

  const char* name() const override { return "pubsub_topics_list"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_TOPICS_LIST; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

  void send_response() override {
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/xml");

    if (op_ret < 0) {
      return;
    }

    const auto f = s->formatter;
    f->open_object_section_in_ns("ListTopicsResponse", RGW_REST_SNS_XMLNS);
    f->open_object_section("ListTopicsResult");
    encode_xml("Topics", result, f); 
    f->close_section(); // ListTopicsResult
    f->open_object_section("ResponseMetadata");
    encode_xml("RequestId", s->req_id, f); 
    f->close_section(); // ResponseMetadata
    if (!next_token.empty()) {
      encode_xml("NextToken", next_token, f);
    }
    f->close_section(); // ListTopicsResponse
    rgw_flush_formatter_and_reset(s, f);
  }
};

void RGWPSListTopicsOp::execute(optional_yield y) {
  const std::string start_token = s->info.args.get("NextToken");

  const RGWPubSub ps(driver, get_account_or_tenant(s->owner.id), *s->penv.site);
  constexpr int max_items = 100;
  op_ret = ps.get_topics(this, start_token, max_items, result, next_token, y);
  // if there are no topics it is not considered an error
  op_ret = op_ret == -ENOENT ? 0 : op_ret;
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "failed to get topics, ret=" << op_ret << dendl;
    return;
  }
  if (topics_has_endpoint_secret(result) && !verify_transport_security(s->cct, *(s->info.env))) {
    s->err.message = "Topic contains secrets that must be transmitted over a secure transport";
    op_ret = -EPERM;
    return;
  }

  ldpp_dout(this, 20) << "successfully got topics" << dendl;

  // non-account users filter out topics they aren't permitted to see
  if (s->auth.identity->get_account()) {
    return;
  }
  for (auto it = result.topics.cbegin(); it != result.topics.cend();) {
    const auto arn = rgw::ARN::parse(it->second.arn);
    if (!arn || !verify_topic_permission(this, s, it->second, *arn,
                                         rgw::IAM::snsGetTopicAttributes)) {
      result.topics.erase(it++);
    } else {
      ++it;
    }
  }
}

// command (extension to AWS): 
// POST
// Action=GetTopic&TopicArn=<topic-arn>
class RGWPSGetTopicOp : public RGWOp {
 private:
  rgw::ARN topic_arn;
  std::string topic_name;
  rgw_pubsub_topic result;
  
  int get_params() {
    auto arn = validate_topic_arn(s->info.args.get("TopicArn"), s->err.message);
    if (!arn) {
      return -EINVAL;
    }
    topic_arn = std::move(*arn);
    topic_name = topic_arn.resource;
    return 0;
  }

 public:
  int init_processing(optional_yield y) override {
    int ret = get_params();
    if (ret < 0) {
      return ret;
    }
    const RGWPubSub ps(driver, get_account_or_tenant(s->owner.id), *s->penv.site);
    ret = ps.get_topic(this, topic_name, result, y, nullptr);
    if (ret < 0) {
      ldpp_dout(this, 4) << "failed to get topic '" << topic_name << "', ret=" << ret << dendl;
      if (ret == -ENOENT) {
        s->err.message = "No such TopicArn";
        return -ERR_NOT_FOUND; // return NotFound instead of NoSuchKey
      }
      return ret;
    }
    if (topic_has_endpoint_secret(result) && !verify_transport_security(s->cct, *(s->info.env))) {
      s->err.message = "Topic contains secrets that must be transmitted over a secure transport";
      return -EPERM;
    }
    return RGWOp::init_processing(y);
  }

  int verify_permission(optional_yield y) override {
    if (!verify_topic_permission(this, s, result, topic_arn,
                                 rgw::IAM::snsGetTopicAttributes)) {
      return -ERR_AUTHORIZATION;
    }
    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute(optional_yield y) override;

  const char* name() const override { return "pubsub_topic_get"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_TOPIC_GET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

  void send_response() override {
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/xml");

    if (op_ret < 0) {
      return;
    }

    const auto f = s->formatter;
    f->open_object_section("GetTopicResponse");
    f->open_object_section("GetTopicResult");
    encode_xml("Topic", result, f); 
    f->close_section();
    f->open_object_section("ResponseMetadata");
    encode_xml("RequestId", s->req_id, f); 
    f->close_section();
    f->close_section();
    rgw_flush_formatter_and_reset(s, f);
  }
};

void RGWPSGetTopicOp::execute(optional_yield y) {
  ldpp_dout(this, 4) << "successfully got topic '" << topic_name << "'" << dendl;
}

// command (AWS compliant): 
// POST
// Action=GetTopicAttributes&TopicArn=<topic-arn>
class RGWPSGetTopicAttributesOp : public RGWOp {
 private:
  rgw::ARN topic_arn;
  std::string topic_name;
  rgw_pubsub_topic result;
  
  int get_params() {
    auto arn = validate_topic_arn(s->info.args.get("TopicArn"), s->err.message);
    if (!arn) {
      return -EINVAL;
    }
    topic_arn = std::move(*arn);
    topic_name = topic_arn.resource;
    return 0;
  }

 public:
  int init_processing(optional_yield y) override {
    int ret = get_params();
    if (ret < 0) {
      return ret;
    }
    const RGWPubSub ps(driver, get_account_or_tenant(s->owner.id), *s->penv.site);
    ret = ps.get_topic(this, topic_name, result, y, nullptr);
    if (ret < 0) {
      ldpp_dout(this, 4) << "failed to get topic '" << topic_name << "', ret=" << ret << dendl;
      if (ret == -ENOENT) {
        s->err.message = "No such TopicArn";
        return -ERR_NOT_FOUND; // return NotFound instead of NoSuchKey
      }
      return ret;
    }
    if (topic_has_endpoint_secret(result) && !verify_transport_security(s->cct, *(s->info.env))) {
      s->err.message = "Topic contains secrets that must be transmitted over a secure transport";
      return -EPERM;
    }
    return 0;
  }

  int verify_permission(optional_yield y) override {
    if (!verify_topic_permission(this, s, result, topic_arn,
                                 rgw::IAM::snsGetTopicAttributes)) {
      return -ERR_AUTHORIZATION;
    }
    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute(optional_yield y) override;

  const char* name() const override { return "pubsub_topic_get"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_TOPIC_GET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

  void send_response() override {
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/xml");

    if (op_ret < 0) {
      return;
    }

    const auto f = s->formatter;
    f->open_object_section_in_ns("GetTopicAttributesResponse", RGW_REST_SNS_XMLNS);
    f->open_object_section("GetTopicAttributesResult");
    result.dump_xml_as_attributes(f);
    f->close_section(); // GetTopicAttributesResult
    f->open_object_section("ResponseMetadata");
    encode_xml("RequestId", s->req_id, f); 
    f->close_section(); // ResponseMetadata
    f->close_section(); // GetTopicAttributesResponse
    rgw_flush_formatter_and_reset(s, f);
  }
};

void RGWPSGetTopicAttributesOp::execute(optional_yield y) {
  ldpp_dout(this, 4) << "successfully got topic '" << topic_name << "'" << dendl;
}

// command (AWS compliant):
// POST
// Action=SetTopicAttributes&TopicArn=<topic-arn>&AttributeName=<attribute-name>&AttributeValue=<attribute-value>
class RGWPSSetTopicAttributesOp : public RGWOp {
 private:
  bufferlist bl_post_body;
  rgw::ARN topic_arn;
  std::string topic_name;
  rgw_pubsub_topic result;
  std::string opaque_data;
  std::string policy_text;
  rgw_pubsub_dest dest;
  rgw_owner topic_owner;
  std::string attribute_name;

  int get_params() {
    auto arn = validate_topic_arn(s->info.args.get("TopicArn"), s->err.message);
    if (!arn) {
      return -EINVAL;
    }
    topic_arn = std::move(*arn);
    topic_name = topic_arn.resource;

    attribute_name = s->info.args.get("AttributeName");
    if (attribute_name.empty()) {
      s->err.message = "Missing required element AttributeName";
      return -EINVAL;
    }
    return 0;
  }

  int map_attributes(const rgw_pubsub_topic& topic) {
    // update the default values that is stored in topic currently.
    opaque_data = topic.opaque_data;
    policy_text = topic.policy_text;
    dest = topic.dest;

    if (attribute_name == "OpaqueData") {
      opaque_data = s->info.args.get("AttributeValue");
    } else if (attribute_name == "persistent") {
      s->info.args.get_bool("AttributeValue", &dest.persistent, false);
    } else if (attribute_name == "time_to_live") {
      s->info.args.get_int("AttributeValue",
                           reinterpret_cast<int*>(&dest.time_to_live),
                           rgw::notify::DEFAULT_GLOBAL_VALUE);
    } else if (attribute_name == "max_retries") {
      s->info.args.get_int("AttributeValue",
                           reinterpret_cast<int*>(&dest.max_retries),
                           rgw::notify::DEFAULT_GLOBAL_VALUE);
    } else if (attribute_name == "retry_sleep_duration") {
      s->info.args.get_int("AttributeValue",
                           reinterpret_cast<int*>(&dest.retry_sleep_duration),
                           rgw::notify::DEFAULT_GLOBAL_VALUE);
    } else if (attribute_name == "push-endpoint") {
      dest.push_endpoint = s->info.args.get("AttributeValue");
      if (!validate_and_update_endpoint_secret(dest, s->cct, s->info, s->err.message)) {
        return -EINVAL;
      }
    } else if (attribute_name == "Policy") {
      policy_text = s->info.args.get("AttributeValue");
      if (!policy_text.empty() && !get_policy_from_text(s, policy_text)) {
        return -ERR_MALFORMED_DOC;
      }
    } else {
      // replace the push_endpoint_args if passed in SetAttribute.
      const auto replace_str = [&](const std::string& param,
                                   const std::string& val) {
        auto& push_endpoint_args = dest.push_endpoint_args;
        const std::string replaced_str = param + "=" + val;
        const auto pos = push_endpoint_args.find(param);
        if (pos == std::string::npos) {
          dest.push_endpoint_args.append("&" + replaced_str);
          return;
        }
        auto end_pos = dest.push_endpoint_args.find("&", pos);
        end_pos = end_pos == std::string::npos ? push_endpoint_args.length()
                                               : end_pos;
        push_endpoint_args.replace(pos, end_pos - pos, replaced_str);
      };
      static constexpr std::initializer_list<const char*> args = {
          "verify-ssl",    "use-ssl",         "ca-location", "amqp-ack-level",
          "amqp-exchange", "kafka-ack-level", "mechanism",   "cloudevents",
          "user-name",     "password"};
      if (std::find(args.begin(), args.end(), attribute_name) != args.end()) {
        replace_str(attribute_name, s->info.args.get("AttributeValue"));
        return 0;
      }
      s->err.message = fmt::format("Invalid value for AttributeName '{}'",
                                   attribute_name);
      return -EINVAL;
    }
    return 0;
  }

 public:
  explicit RGWPSSetTopicAttributesOp(bufferlist bl_post_body)
    : bl_post_body(std::move(bl_post_body)) {}

  int init_processing(optional_yield y) override {
    int ret = get_params();
    if (ret < 0) {
      return ret;
    }

    const RGWPubSub ps(driver, get_account_or_tenant(s->owner.id), *s->penv.site);
    ret = ps.get_topic(this, topic_name, result, y, nullptr);
    if (ret < 0) {
      ldpp_dout(this, 4) << "failed to get topic '" << topic_name
                         << "', ret=" << ret << dendl;
      if (ret == -ENOENT) {
        s->err.message = "No such TopicArn";
        return -ERR_NOT_FOUND; // return NotFound instead of NoSuchKey
      }
      return ret;
    }
    topic_owner = result.owner;

    ret = map_attributes(result);
    if (ret < 0) {
      return ret;
    }

    return RGWOp::init_processing(y);
  }

  int verify_permission(optional_yield y) override {
    if (!verify_topic_permission(this, s, result, topic_arn,
                                 rgw::IAM::snsSetTopicAttributes)) {
      return -ERR_AUTHORIZATION;
    }
    return 0;
  }

  void pre_exec() override { rgw_bucket_object_pre_exec(s); }
  void execute(optional_yield) override;

  const char* name() const override { return "pubsub_topic_set"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_TOPIC_SET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  void send_response() override {
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/xml");

    if (op_ret < 0) {
      return;
    }

    const auto f = s->formatter;
    f->open_object_section_in_ns("SetTopicAttributesResponse", RGW_REST_SNS_XMLNS);
    f->open_object_section("ResponseMetadata");
    encode_xml("RequestId", s->req_id, f);
    f->close_section();  // ResponseMetadata
    f->close_section();  // SetTopicAttributesResponse
    rgw_flush_formatter_and_reset(s, f);
  }
};

void RGWPSSetTopicAttributesOp::execute(optional_yield y) {
  if (!driver->is_meta_master()) {
    op_ret = rgw_forward_request_to_master(
        this, *s->penv.site, s->owner.id, &bl_post_body, nullptr, s->info, y);
    if (op_ret < 0) {
      ldpp_dout(this, 4)
          << "SetTopicAttributes forward_request_to_master returned ret = "
          << op_ret << dendl;
      return;
    }
  }
  // don't add a persistent queue if we already have one
  const bool already_persistent = topic_needs_queue(result.dest);
  if (!already_persistent && topic_needs_queue(dest)) {
    // initialize the persistent queue's location, using ':' as the namespace
    // delimiter because its inclusion in a TopicName would break ARNs
    dest.persistent_queue = string_cat_reserve(
        get_account_or_tenant(s->owner.id), ":", topic_name);

    op_ret = driver->add_persistent_topic(this, y, dest.persistent_queue);
    if (op_ret < 0) {
      ldpp_dout(this, 4)
          << "SetTopicAttributes Action failed to create queue for "
             "persistent topics. error:"
          << op_ret << dendl;
      return;
    }
  } else if (already_persistent && !topic_needs_queue(dest)) {
    // changing the persistent topic to non-persistent.
    op_ret = driver->remove_persistent_topic(this, y, result.dest.persistent_queue);
    if (op_ret != -ENOENT && op_ret < 0) {
      ldpp_dout(this, 4) << "SetTopicAttributes Action failed to remove queue "
                            "for persistent topics. error:"
                         << op_ret << dendl;
      return;
    }
  }
  const RGWPubSub ps(driver, get_account_or_tenant(s->owner.id), *s->penv.site);
  op_ret = ps.create_topic(this, topic_name, dest, topic_arn.to_string(),
                           opaque_data, topic_owner, policy_text, y);
  if (op_ret < 0) {
    ldpp_dout(this, 4) << "failed to SetAttributes for topic '" << topic_name
                       << "', ret=" << op_ret << dendl;
    return;
  }
  ldpp_dout(this, 20) << "successfully set the attributes for topic '"
                      << topic_name << "'" << dendl;
}

// command (AWS compliant): 
// POST
// Action=DeleteTopic&TopicArn=<topic-arn>
class RGWPSDeleteTopicOp : public RGWOp {
  private:
  bufferlist bl_post_body;
  rgw::ARN topic_arn;
  std::string topic_name;
  std::optional<rgw_pubsub_topic> topic;
  
  int get_params() {
    auto arn = validate_topic_arn(s->info.args.get("TopicArn"), s->err.message);
    if (!arn) {
      return -EINVAL;
    }
    topic_arn = std::move(*arn);
    topic_name = topic_arn.resource;
    return 0;
  }

 public:
  explicit RGWPSDeleteTopicOp(bufferlist bl_post_body)
    : bl_post_body(std::move(bl_post_body)) {}

  int init_processing(optional_yield y) override {
    int ret = get_params();
    if (ret < 0) {
      return ret;
    }

    const RGWPubSub ps(driver, get_account_or_tenant(s->owner.id), *s->penv.site);
    rgw_pubsub_topic result;
    ret = ps.get_topic(this, topic_name, result, y, nullptr);
    if (ret == -ENOENT) {
      // leave topic empty
    } else if (ret < 0) {
      ldpp_dout(this, 4) << "failed to get topic '" << topic_name
                         << "', ret=" << ret << dendl;
      return ret;
    } else {
      topic = std::move(result);
    }

    return RGWOp::init_processing(y);
  }

  int verify_permission(optional_yield y) override {
    if (topic) {
      // consult topic policy for delete permission
      if (!verify_topic_permission(this, s, *topic, topic_arn,
                                   rgw::IAM::snsDeleteTopic)) {
        return -ERR_AUTHORIZATION;
      }
    } else {
      // if no topic policy exists, just check identity policies
      // account users require an Allow, non-account users just check for Deny
      const bool mandatory_policy = !!s->auth.identity->get_account();
      if (!verify_user_permission(this, s, topic_arn,
                                  rgw::IAM::snsDeleteTopic,
                                  mandatory_policy)) {
        return -ERR_AUTHORIZATION;
      }
    }
    return 0;
  }

  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute(optional_yield y) override;

  const char* name() const override { return "pubsub_topic_delete"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_TOPIC_DELETE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }

  void send_response() override {
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/xml");

    if (op_ret < 0) {
      return;
    }

    const auto f = s->formatter;
    f->open_object_section_in_ns("DeleteTopicResponse", RGW_REST_SNS_XMLNS);
    f->open_object_section("ResponseMetadata");
    encode_xml("RequestId", s->req_id, f); 
    f->close_section(); // ResponseMetadata
    f->close_section(); // DeleteTopicResponse
    rgw_flush_formatter_and_reset(s, f);
  }
};

void RGWPSDeleteTopicOp::execute(optional_yield y) {
  if (!driver->is_meta_master()) {
    op_ret = rgw_forward_request_to_master(
        this, *s->penv.site, s->owner.id, &bl_post_body, nullptr, s->info, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1)
          << "DeleteTopic forward_request_to_master returned ret = " << op_ret
          << dendl;
      return;
    }
  }

  if (!topic) {
    return;
  }

  const RGWPubSub ps(driver, get_account_or_tenant(s->owner.id), *s->penv.site);
  op_ret = ps.remove_topic(this, topic_name, y);
  if (op_ret < 0 && op_ret != -ENOENT) {
    ldpp_dout(this, 4) << "failed to remove topic '" << topic_name << ", ret=" << op_ret << dendl;
    return;
  }
  ldpp_dout(this, 4) << "successfully removed topic '" << topic_name << "'" << dendl;

  if (op_ret == -ENOENT) {
    // its not an error if no topics exist, just a no-op
    op_ret = 0;
  }
}

using op_generator = RGWOp*(*)(bufferlist);
static const std::unordered_map<std::string, op_generator> op_generators = {
    {"CreateTopic", [](bufferlist bl) -> RGWOp* { return new RGWPSCreateTopicOp(std::move(bl)); }},
    {"DeleteTopic", [](bufferlist bl) -> RGWOp* { return new RGWPSDeleteTopicOp(std::move(bl)); }},
    {"ListTopics", [](bufferlist bl) -> RGWOp* { return new RGWPSListTopicsOp; }},
    {"GetTopic", [](bufferlist bl) -> RGWOp* { return new RGWPSGetTopicOp; }},
    {"GetTopicAttributes",
     [](bufferlist bl) -> RGWOp* { return new RGWPSGetTopicAttributesOp; }},
    {"SetTopicAttributes",
     [](bufferlist bl) -> RGWOp* { return new RGWPSSetTopicAttributesOp(std::move(bl)); }}};

bool RGWHandler_REST_PSTopic_AWS::action_exists(const req_info& info)
{
  if (info.args.exists("Action")) {
    const std::string action_name = info.args.get("Action");
    return op_generators.contains(action_name);
  }
  return false;
}
bool RGWHandler_REST_PSTopic_AWS::action_exists(const req_state* s)
{
  return action_exists(s->info);
}

RGWOp *RGWHandler_REST_PSTopic_AWS::op_post()
{
  s->dialect = "sns";
  s->prot_flags = RGW_REST_STS;

  if (s->info.args.exists("Action")) {
    const std::string action_name = s->info.args.get("Action");
    const auto action_it = op_generators.find(action_name);
    if (action_it != op_generators.end()) {
      return action_it->second(std::move(bl_post_body));
    }
    ldpp_dout(s, 10) << "unknown action '" << action_name << "' for Topic handler" << dendl;
  } else {
    ldpp_dout(s, 10) << "missing action argument in Topic handler" << dendl;
  }
  return nullptr;
}

int RGWHandler_REST_PSTopic_AWS::authorize(const DoutPrefixProvider* dpp, optional_yield y) {
  const auto rc = RGW_Auth_S3::authorize(dpp, driver, auth_registry, s, y);
  if (rc < 0) {
    return rc;
  }
  if (s->auth.identity->is_anonymous()) {
    ldpp_dout(dpp, 1) << "anonymous user not allowed in topic operations" << dendl;
    return -ERR_INVALID_REQUEST;
  }
  return 0;
}

int remove_notification_by_topic(const DoutPrefixProvider *dpp, const std::string& topic_name, const RGWPubSub::Bucket& b, optional_yield y, const RGWPubSub& ps) {
  int op_ret = b.remove_notification(dpp, topic_name, y);
  if (op_ret < 0) {
    ldpp_dout(dpp, 1) << "failed to remove notification of topic '" << topic_name << "', ret=" << op_ret << dendl;
  }
  op_ret = ps.remove_topic(dpp, topic_name, y);
  if (op_ret < 0) {
    ldpp_dout(dpp, 1) << "failed to remove auto-generated topic '" << topic_name << "', ret=" << op_ret << dendl;
  }
  return op_ret;
}

int delete_all_notifications(const DoutPrefixProvider *dpp, const rgw_pubsub_bucket_topics& bucket_topics, const RGWPubSub::Bucket& b, optional_yield y, const RGWPubSub& ps) {
  // delete all notifications of on a bucket
  for (const auto& topic : bucket_topics.topics) {
    const auto op_ret = remove_notification_by_topic(dpp, topic.first, b, y, ps);
    if (op_ret < 0) {
      return op_ret;
    }
  }
  return 0;
}

// command (S3 compliant): PUT /<bucket name>?notification
// a "notification" and a subscription will be auto-generated
// actual configuration is XML encoded in the body of the message
class RGWPSCreateNotifOp : public RGWDefaultResponseOp {
  bufferlist data;
  rgw_pubsub_s3_notifications configurations;
  std::map<rgw::ARN, rgw_pubsub_topic> topics;

  int verify_params() override {
    bool exists;
    const auto no_value = s->info.args.get("notification", &exists);
    if (!exists) {
      s->err.message = "Missing required parameter 'notification'";
      return -EINVAL;
    } 
    if (no_value.length() > 0) {
      s->err.message = "Parameter 'notification' should not have any value";
      return -EINVAL;
    }
    if (s->bucket_name.empty()) {
      s->err.message = "Missing required bucket name";
      return -EINVAL;
    }
    return 0;
  }

  int get_params_from_body(rgw_pubsub_s3_notifications& configurations) {
    const auto max_size = s->cct->_conf->rgw_max_put_param_size;
    int r;
    std::tie(r, data) = read_all_input(s, max_size, false);

    if (r < 0) {
      ldpp_dout(this, 4) << "failed to read XML payload" << dendl;
      return r;
    }
    if (data.length() == 0) {
      ldpp_dout(this, 4) << "XML payload missing" << dendl;
      return -EINVAL;
    }

    RGWXMLDecoder::XMLParser parser;

    if (!parser.init()){
      ldpp_dout(this, 4) << "failed to initialize XML parser" << dendl;
      return -EINVAL;
    }
    if (!parser.parse(data.c_str(), data.length(), 1)) {
      ldpp_dout(this, 4) << "failed to parse XML payload" << dendl;
      return -ERR_MALFORMED_XML;
    }
    try {
      // NotificationConfigurations is mandatory
      // It can be empty which means we delete all the notifications
      RGWXMLDecoder::decode_xml("NotificationConfiguration", configurations, &parser, true);
    } catch (const RGWXMLDecoder::err& err) {
      s->err.message = err.what();
      return -ERR_MALFORMED_XML;
    }
    return 0;
  }
public:
  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;

  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }

  const char* name() const override { return "pubsub_notification_create_s3"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_NOTIF_CREATE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

  void execute(optional_yield) override;
  void execute_v2(optional_yield);
};

int RGWPSCreateNotifOp::init_processing(optional_yield y)
{
  int ret = verify_params();
  if (ret < 0) {
    return ret;
  }

  ret = get_params_from_body(configurations);
  if (ret < 0) {
    return ret;
  }


  for (const auto& c : configurations.list) {
    const auto& notif_name = c.id;
    if (notif_name.empty()) {
      s->err.message = "Missing required element Id";
      return -EINVAL;
    }
    if (c.topic_arn.empty()) {
      s->err.message = "Missing required element Topic";
      return -EINVAL;
    }

    const auto arn = rgw::ARN::parse(c.topic_arn);
    if (!arn || arn->resource.empty()) {
      s->err.message = "Invalid Topic ARN";
      return -EINVAL;
    }
    const auto& topic_name = arn->resource;

    if (std::find(c.events.begin(), c.events.end(), rgw::notify::UnknownEvent) != c.events.end()) {
      s->err.message = "Unknown Event type: " + notif_name;
      return -EINVAL;
    }

    // load topic metadata if we haven't already
    auto insert = topics.emplace(std::piecewise_construct,
                                 std::forward_as_tuple(*arn),
                                 std::forward_as_tuple());
    if (insert.second) {
      rgw_pubsub_topic& topic_info = insert.first->second;
      const RGWPubSub ps(driver, arn->account, *s->penv.site);
      ret = ps.get_topic(this, topic_name, topic_info, y, nullptr);
      if (ret < 0) {
        ldpp_dout(this, 4) << "failed to get topic '" << topic_name << "', ret=" << ret << dendl;
        return ret;
      }
    }
  }

  return RGWOp::init_processing(y);
}

int RGWPSCreateNotifOp::verify_permission(optional_yield y) {
  // require s3:PutBucketNotification permission for the bucket
  if (!verify_bucket_permission(this, s, rgw::IAM::s3PutBucketNotification)) {
    return -EACCES;
  }

  // require sns:Publish permission for each topic
  for (const auto& [arn, topic] : topics) {
    if (!verify_topic_permission(this, s, topic, arn, rgw::IAM::snsPublish)) {
      return -EACCES;
    }
  }
  return 0;
}

void RGWPSCreateNotifOp::execute(optional_yield y) {
  if (!driver->is_meta_master()) {
    op_ret = rgw_forward_request_to_master(
        this, *s->penv.site, s->owner.id, &data, nullptr, s->info, y);
    if (op_ret < 0) {
      ldpp_dout(this, 4) << "CreateBucketNotification "
                            "forward_request_to_master returned ret = "
                         << op_ret << dendl;
      return;
    }
  }

  if (rgw::all_zonegroups_support(*s->penv.site, rgw::zone_features::notification_v2)) {
    return execute_v2(y);
  }

  const RGWPubSub ps(driver, get_account_or_tenant(s->owner.id), *s->penv.site);
  const RGWPubSub::Bucket b(ps, s->bucket.get());

  if(configurations.list.empty()) {
    // get all topics on a bucket
    rgw_pubsub_bucket_topics bucket_topics;
    op_ret = b.get_topics(this, bucket_topics, y);
    if (op_ret < 0) {
      ldpp_dout(this, 4) << "failed to get list of topics from bucket '" << s->bucket_name << "', ret=" << op_ret << dendl;
      return;
    }

    op_ret = delete_all_notifications(this, bucket_topics, b, y, ps);
    return;
  }

  for (const auto& c : configurations.list) {
    const auto& notif_name = c.id;

    const auto arn = rgw::ARN::parse(c.topic_arn);
    if (!arn) { // already validated above
      continue;
    }
    const auto& topic_name = arn->resource;

    auto t = topics.find(*arn);
    if (t == topics.end()) {
      continue;
    }
    auto& topic_info = t->second;

    // make sure that full topic configuration match
    // TODO: use ARN match function
    
    // create unique topic name. this has 2 reasons:
    // (1) topics cannot be shared between different S3 notifications because they hold the filter information
    // (2) make topic cleanup easier, when notification is removed
    const auto unique_topic_name = topic_to_unique(topic_name, notif_name);
    // generate the internal topic. destination is stored here for the "push-only" case
    // when no subscription exists
    // ARN is cached to make the "GET" method faster
    op_ret = ps.create_topic(this, unique_topic_name, topic_info.dest,
                             topic_info.arn, topic_info.opaque_data,
                             s->owner.id, topic_info.policy_text, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "failed to auto-generate unique topic '" << unique_topic_name << 
        "', ret=" << op_ret << dendl;
      return;
    }
    ldpp_dout(this, 20) << "successfully auto-generated unique topic '" << unique_topic_name << "'" << dendl;
    // generate the notification
    rgw::notify::EventTypeList events;
    op_ret = b.create_notification(this, unique_topic_name, c.events, std::make_optional(c.filter), notif_name, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "failed to auto-generate notification for unique topic '" << unique_topic_name <<
        "', ret=" << op_ret << dendl;
      // rollback generated topic (ignore return value)
      ps.remove_topic(this, unique_topic_name, y);
      return;
    }
    ldpp_dout(this, 20) << "successfully auto-generated notification for unique topic '" << unique_topic_name << "'" << dendl;
  }
}

void RGWPSCreateNotifOp::execute_v2(optional_yield y) {
  if (const auto ret = driver->stat_topics_v1(s->bucket_tenant, y, this); ret != -ENOENT) {
    ldpp_dout(this, 1) << "WARNING: " << (ret == 0 ? "topic migration in process" : "cannot determine topic migration status. ret = " + std::to_string(ret))
      << ". please try again later" << dendl; 
    op_ret = -ERR_SERVICE_UNAVAILABLE;
    return;
  }
  op_ret = retry_raced_bucket_write(this, s->bucket.get(), [this, y] {
    if (configurations.list.empty()) {
      return remove_notification_v2(this, driver, s->bucket.get(),
                                    /*delete all notif=true*/"", y);
    }
    rgw_pubsub_bucket_topics bucket_topics;
    int ret = get_bucket_notifications(this, s->bucket.get(), bucket_topics);
    if (ret < 0) {
      ldpp_dout(this, 1)
            << "failed to load existing bucket notification on bucket: "
              << s->bucket << ", ret = " << ret << dendl;
      return ret;
    }
    for (const auto &c : configurations.list) {
      const auto &notif_name = c.id;

      const auto arn = rgw::ARN::parse(c.topic_arn);
      if (!arn) { // already validated above
        continue;
      }
      const auto &topic_name = arn->resource;

      auto t = topics.find(*arn);
      if (t == topics.end()) {
        continue;
      }
      auto &topic_info = t->second;

      auto &topic_filter =
        bucket_topics.topics[topic_to_unique(topic_name, notif_name)];
      topic_filter.topic = topic_info;
      topic_filter.events = c.events;
      topic_filter.s3_id = notif_name;
      topic_filter.s3_filter = c.filter;
    }
    // finally store all the bucket notifications as attr.
    bufferlist bl;
    bucket_topics.encode(bl);
    rgw::sal::Attrs &attrs = s->bucket->get_attrs();
    attrs[RGW_ATTR_BUCKET_NOTIFICATION] = std::move(bl);
    return s->bucket->merge_and_store_attrs(this, attrs, y);
  }, y);

  if (op_ret < 0) {
    ldpp_dout(this, 4)
        << "Failed to store RGW_ATTR_BUCKET_NOTIFICATION on bucket="
        << s->bucket->get_name() << " returned err= " << op_ret << dendl;
    return;
  }
  for (const auto& [_, topic] : topics) {
    const auto ret = driver->update_bucket_topic_mapping(
        topic,
        rgw_make_bucket_entry_name(s->bucket->get_tenant(), s->bucket->get_name()),
        /*add_mapping=*/true, y, this);
    if (ret < 0) {
      ldpp_dout(this, 4) << "Failed to remove topic mapping on bucket="
                         << s->bucket->get_name() << " ret= " << ret << dendl;
      // error should be reported ??
      // op_ret = ret;
    }
  }
  ldpp_dout(this, 20) << "successfully created bucket notification for bucket: "
                      << s->bucket->get_name() << dendl;
}

// command (extension to S3): DELETE /bucket?notification[=<notification-id>]
class RGWPSDeleteNotifOp : public RGWDefaultResponseOp {
  std::string notif_name;
  int get_params() {
    bool exists;
    notif_name = s->info.args.get("notification", &exists);
    if (!exists) {
      s->err.message = "Missing required parameter 'notification'";
      return -EINVAL;
    } 
    if (s->bucket_name.empty()) {
      s->err.message = "Missing required bucket name";
      return -EINVAL;
    }
    return 0;
  }
  void execute_v2(optional_yield y);

 public:
  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override;

  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  
  const char* name() const override { return "pubsub_notification_delete_s3"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_NOTIF_DELETE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }

  void execute(optional_yield y) override;
};

int RGWPSDeleteNotifOp::init_processing(optional_yield y)
{
  int ret = get_params();
  if (ret < 0) {
    return ret;
  }
  return RGWOp::init_processing(y);
}

int RGWPSDeleteNotifOp::verify_permission(optional_yield y) {
  if (!verify_bucket_permission(this, s, rgw::IAM::s3PutBucketNotification)) {
    return -EACCES;
  }

  return 0;
}

void RGWPSDeleteNotifOp::execute(optional_yield y) {
  if (!driver->is_meta_master()) {
    bufferlist indata;
    op_ret = rgw_forward_request_to_master(
        this, *s->penv.site, s->owner.id, &indata, nullptr, s->info, y);
    if (op_ret < 0) {
      ldpp_dout(this, 4) << "DeleteBucketNotification "
                            "forward_request_to_master returned error ret= "
                         << op_ret << dendl;
      return;
    }
  }

  if (rgw::all_zonegroups_support(*s->penv.site, rgw::zone_features::notification_v2)) {
    return execute_v2(y);
  }

  const RGWPubSub ps(driver, get_account_or_tenant(s->owner.id), *s->penv.site);
  const RGWPubSub::Bucket b(ps, s->bucket.get());

  // get all topics on a bucket
  rgw_pubsub_bucket_topics bucket_topics;
  op_ret = b.get_topics(this, bucket_topics, y);
  if (op_ret < 0) {
    ldpp_dout(this, 4) << "failed to get list of topics from bucket '" << s->bucket_name << "', ret=" << op_ret << dendl;
    return;
  }

  if (!notif_name.empty()) {
    // delete a specific notification
    const auto unique_topic = find_unique_topic(bucket_topics, notif_name);
    if (unique_topic) {
      const auto unique_topic_name = unique_topic->topic.name;
      op_ret = remove_notification_by_topic(this, unique_topic_name, b, y, ps);
      return;
    }
    // notification to be removed is not found - considered success
    ldpp_dout(this, 20) << "notification '" << notif_name << "' already removed" << dendl;
    return;
  }

  op_ret = delete_all_notifications(this, bucket_topics, b, y, ps);
}

void RGWPSDeleteNotifOp::execute_v2(optional_yield y) {
  if (const auto ret = driver->stat_topics_v1(s->bucket_tenant, y, this); ret != -ENOENT) {
    ldpp_dout(this, 4) << "WARNING: " << (ret == 0 ? "topic migration in process" : "cannot determine topic migration status. ret = " + std::to_string(ret))
      << ". please try again later" << dendl; 
    op_ret = -ERR_SERVICE_UNAVAILABLE;
    return;
  }

  op_ret = remove_notification_v2(this, driver, s->bucket.get(), notif_name, y);
}

// command (S3 compliant): GET /bucket?notification[=<notification-id>]
class RGWPSListNotifsOp : public RGWOp {
  rgw_pubsub_s3_notifications notifications;

  int get_params(std::string& notif_name) const {
    bool exists;
    notif_name = s->info.args.get("notification", &exists);
    if (!exists) {
      s->err.message = "Missing required parameter 'notification'";
      return -EINVAL;
    } 
    if (s->bucket_name.empty()) {
      s->err.message = "Missing required bucket name";
      return -EINVAL;
    }
    return 0;
  }

public:
  int verify_permission(optional_yield y) override;

  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }

  const char* name() const override { return "pubsub_notifications_get_s3"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_NOTIF_LIST; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

  void execute(optional_yield y) override;
  void send_response() override {
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/xml");

    if (op_ret < 0) {
      return;
    }
    notifications.dump_xml(s->formatter);
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
};

void RGWPSListNotifsOp::execute(optional_yield y) {
  std::string notif_name;
  op_ret = get_params(notif_name);
  if (op_ret < 0) {
    return;
  }

  std::unique_ptr<rgw::sal::Bucket> bucket;
  op_ret = driver->load_bucket(this, rgw_bucket(s->bucket_tenant, s->bucket_name),
                               &bucket, y);
  if (op_ret < 0) {
    ldpp_dout(this, 4) << "failed to get bucket '" <<
      (s->bucket_tenant.empty() ? s->bucket_name : s->bucket_tenant + ":" + s->bucket_name) << 
      "' info, ret = " << op_ret << dendl;
    return;
  }

  // get all topics on a bucket
  rgw_pubsub_bucket_topics bucket_topics;
  if (rgw::all_zonegroups_support(*s->penv.site, rgw::zone_features::notification_v2) &&
      driver->stat_topics_v1(s->bucket_tenant, y, this) == -ENOENT) {
    op_ret = get_bucket_notifications(this, bucket.get(), bucket_topics);
  } else {
    const RGWPubSub ps(driver, get_account_or_tenant(s->owner.id), *s->penv.site);
    const RGWPubSub::Bucket b(ps, bucket.get());
    op_ret = b.get_topics(this, bucket_topics, y);
  }
  if (op_ret < 0) {
    ldpp_dout(this, 4) << "failed to get list of topics from bucket '"
                       << s->bucket_name << "', ret=" << op_ret << dendl;
    return;
  }
  if (!notif_name.empty()) {
    // get info of a specific notification
    const auto unique_topic = find_unique_topic(bucket_topics, notif_name);
    if (unique_topic) {
      notifications.list.emplace_back(*unique_topic);
      return;
    }
    op_ret = -ENOENT;
    ldpp_dout(this, 4) << "failed to get notification info for '" << notif_name << "', ret=" << op_ret << dendl;
    return;
  }
  // loop through all topics of the bucket
  for (const auto& topic : bucket_topics.topics) {
    if (topic.second.s3_id.empty()) {
        // not an s3 notification
        continue;
    }
    notifications.list.emplace_back(topic.second);
  }
}

int RGWPSListNotifsOp::verify_permission(optional_yield y) {
  if (!verify_bucket_permission(this, s, rgw::IAM::s3GetBucketNotification)) {
    return -EACCES;
  }

  return 0;
}

RGWOp* RGWHandler_REST_PSNotifs_S3::op_get() {
  return new RGWPSListNotifsOp();
}

RGWOp* RGWHandler_REST_PSNotifs_S3::op_put() {
  return new RGWPSCreateNotifOp();
}

RGWOp* RGWHandler_REST_PSNotifs_S3::op_delete() {
  return new RGWPSDeleteNotifOp();
}

RGWOp* RGWHandler_REST_PSNotifs_S3::create_get_op() {
    return new RGWPSListNotifsOp();
}

RGWOp* RGWHandler_REST_PSNotifs_S3::create_put_op() {
  return new RGWPSCreateNotifOp();
}

RGWOp* RGWHandler_REST_PSNotifs_S3::create_delete_op() {
  return new RGWPSDeleteNotifOp();
}

