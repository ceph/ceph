// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <algorithm>
#include <boost/tokenizer.hpp>
#include <optional>
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

static const char* AWS_SNS_NS("https://sns.amazonaws.com/doc/2010-03-31/");

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
bool validate_and_update_endpoint_secret(rgw_pubsub_dest& dest, CephContext *cct, const RGWEnv& env) {
  if (dest.push_endpoint.empty()) {
      return true;
  }
  std::string user;
  std::string password;
  if (!rgw::parse_url_userinfo(dest.push_endpoint, user, password)) {
    ldout(cct, 1) << "endpoint validation error: malformed endpoint URL:" << dest.push_endpoint << dendl;
    return false;
  }
  // this should be verified inside parse_url()
  ceph_assert(user.empty() == password.empty());
  if (!user.empty()) {
      dest.stored_secret = true;
      if (!verify_transport_security(cct, env)) {
        ldout(cct, 1) << "endpoint validation error: sending secrets over insecure transport" << dendl;
        return false;
      }
  }
  return true;
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

std::optional<rgw::IAM::Policy> get_policy_from_text(req_state* const s,
                                                     std::string& policy_text) {
  const auto bl = bufferlist::static_from_string(policy_text);
  try {
    return rgw::IAM::Policy(
        s->cct, s->owner.id.tenant, bl,
        s->cct->_conf.get_val<bool>("rgw_policy_reject_invalid_principals"));
  } catch (rgw::IAM::PolicyParseException& e) {
    ldout(s->cct, 1) << "failed to parse policy: '" << policy_text
                     << "' with error: " << e.what() << dendl;
    s->err.message = e.what();
    return std::nullopt;
  }
}

int verify_topic_owner_or_policy(req_state* const s,
                                 const rgw_pubsub_topic& topic,
                                 const std::string& zonegroup_name,
                                 const uint64_t op) {
  if (topic.user == s->owner.id) {
    return 0;
  }
  // no policy set.
  if (topic.policy_text.empty()) {
    // if rgw_topic_require_publish_policy is "false" dont validate "publish" policies
    if (op == rgw::IAM::snsPublish && !s->cct->_conf->rgw_topic_require_publish_policy) {
      return 0;
    }
    if (topic.user.empty()) {
      // if we don't know the original user and there is no policy
      // we will not reject the request.
      // this is for compatibility with versions that did not store the user in the topic
      return 0;
    }
    s->err.message = "Topic was created by another user.";
    return -EACCES;
  }
  // bufferlist::static_from_string wants non const string
  std::string policy_text(topic.policy_text);
  const auto p = get_policy_from_text(s, policy_text);
  rgw::IAM::PolicyPrincipal princ_type = rgw::IAM::PolicyPrincipal::Other;
  const rgw::ARN arn(rgw::Partition::aws, rgw::Service::sns, zonegroup_name,
                     s->user->get_tenant(), topic.name);
  if (!p || p->eval(s->env, *s->auth.identity, op, arn, princ_type) !=
                rgw::IAM::Effect::Allow) {
    ldout(s->cct, 1) << "topic policy failed validation, topic policy: " << p
                     << dendl;
    return -EACCES;
  }
  return 0;
}

// command (AWS compliant): 
// POST
// Action=CreateTopic&Name=<topic-name>[&OpaqueData=data][&push-endpoint=<endpoint>[&persistent][&<arg1>=<value1>]]
class RGWPSCreateTopicOp : public RGWOp {
  private:
  std::string topic_name;
  rgw_pubsub_dest dest;
  std::string topic_arn;
  std::string opaque_data;
  std::string policy_text;

  int get_params() {
    topic_name = s->info.args.get("Name");
    if (topic_name.empty()) {
      ldpp_dout(this, 1) << "CreateTopic Action 'Name' argument is missing" << dendl;
      return -EINVAL;
    }

    opaque_data = s->info.args.get("OpaqueData");

    dest.push_endpoint = s->info.args.get("push-endpoint");
    s->info.args.get_bool("persistent", &dest.persistent, false);
    s->info.args.get_int("time_to_live", reinterpret_cast<int *>(&dest.time_to_live), rgw::notify::DEFAULT_GLOBAL_VALUE);
    s->info.args.get_int("max_retries", reinterpret_cast<int *>(&dest.max_retries), rgw::notify::DEFAULT_GLOBAL_VALUE);
    s->info.args.get_int("retry_sleep_duration", reinterpret_cast<int *>(&dest.retry_sleep_duration), rgw::notify::DEFAULT_GLOBAL_VALUE);

    if (!validate_and_update_endpoint_secret(dest, s->cct, *(s->info.env))) {
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
    const rgw::ARN arn(rgw::Partition::aws, rgw::Service::sns, 
        driver->get_zone()->get_zonegroup().get_name(),
        s->user->get_tenant(), topic_name);
    topic_arn = arn.to_string();
    return 0;
  }

  public:
   int verify_permission(optional_yield y) override {
    auto ret = get_params();
    if (ret < 0) {
      return ret;
    }

    const RGWPubSub ps(driver, s->owner.id.tenant,
                       &s->penv.site->get_period()->get_map().zonegroups);
    rgw_pubsub_topic result;
    ret = ps.get_topic(this, topic_name, result, y);
    if (ret == -ENOENT) {
      // topic not present
      return 0;
    }
    if (ret == 0) {
      ret = verify_topic_owner_or_policy(
          s, result, driver->get_zone()->get_zonegroup().get_name(),
          rgw::IAM::snsCreateTopic);
      if (ret == 0)
      {
        return 0;
      }

      ldpp_dout(this, 1) << "no permission to modify topic '" << topic_name
                         << "', topic already exist." << dendl;
      return -EACCES;
    }
    ldpp_dout(this, 1) << "failed to read topic '" << topic_name
                       << "', with error:" << ret << dendl;
    return ret;
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
    f->open_object_section_in_ns("CreateTopicResponse", AWS_SNS_NS);
    f->open_object_section("CreateTopicResult");
    encode_xml("TopicArn", topic_arn, f); 
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
  bufferlist indata;
  if (!driver->is_meta_master()) {
    op_ret = rgw_forward_request_to_master(
        this, *s->penv.site, s->user->get_id(), &indata, nullptr, s->info, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1)
          << "CreateTopic forward_request_to_master returned ret = " << op_ret
          << dendl;
      return;
    }
  }
  if (!dest.push_endpoint.empty() && dest.persistent) {
    op_ret = rgw::notify::add_persistent_topic(topic_name, s->yield);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "CreateTopic Action failed to create queue for "
                            "persistent topics. error:"
                         << op_ret << dendl;
      return;
    }
  }
  const RGWPubSub ps(driver, s->owner.id.tenant,
                     &s->penv.site->get_period()->get_map().zonegroups);
  op_ret = ps.create_topic(this, topic_name, dest, topic_arn, opaque_data,
                           s->owner.id, policy_text, y);
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "failed to create topic '" << topic_name << "', ret=" << op_ret << dendl;
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

public:
  int verify_permission(optional_yield) override {
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
    f->open_object_section_in_ns("ListTopicsResponse", AWS_SNS_NS);
    f->open_object_section("ListTopicsResult");
    encode_xml("Topics", result, f); 
    f->close_section(); // ListTopicsResult
    f->open_object_section("ResponseMetadata");
    encode_xml("RequestId", s->req_id, f); 
    f->close_section(); // ResponseMetadat
    f->close_section(); // ListTopicsResponse
    rgw_flush_formatter_and_reset(s, f);
  }
};

void RGWPSListTopicsOp::execute(optional_yield y) {
  const RGWPubSub ps(driver, s->owner.id.tenant,
                     &s->penv.site->get_period()->get_map().zonegroups);
  op_ret = ps.get_topics(this, result, y);
  // if there are no topics it is not considered an error
  op_ret = op_ret == -ENOENT ? 0 : op_ret;
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "failed to get topics, ret=" << op_ret << dendl;
    return;
  }
  if (topics_has_endpoint_secret(result) && !verify_transport_security(s->cct, *(s->info.env))) {
    ldpp_dout(this, 1) << "topics contain secrets and cannot be sent over insecure transport" << dendl;
    op_ret = -EPERM;
    return;
  }
  for (auto it = result.topics.cbegin(); it != result.topics.cend();) {
    if (verify_topic_owner_or_policy(
            s, it->second, driver->get_zone()->get_zonegroup().get_name(),
            rgw::IAM::snsGetTopicAttributes) != 0) {
      result.topics.erase(it++);
    } else {
      ++it;
    }
  }
  ldpp_dout(this, 20) << "successfully got topics" << dendl;
}

// command (extension to AWS): 
// POST
// Action=GetTopic&TopicArn=<topic-arn>
class RGWPSGetTopicOp : public RGWOp {
  private:
  std::string topic_name;
  rgw_pubsub_topic result;
  
  int get_params() {
    const auto topic_arn = rgw::ARN::parse((s->info.args.get("TopicArn")));

    if (!topic_arn || topic_arn->resource.empty()) {
        ldpp_dout(this, 1) << "GetTopic Action 'TopicArn' argument is missing or invalid" << dendl;
        return -EINVAL;
    }

    topic_name = topic_arn->resource;
    return 0;
  }

  public:
  int verify_permission(optional_yield y) override {
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
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  const RGWPubSub ps(driver, s->owner.id.tenant,
                     &s->penv.site->get_period()->get_map().zonegroups);
  op_ret = ps.get_topic(this, topic_name, result, y);
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "failed to get topic '" << topic_name << "', ret=" << op_ret << dendl;
    return;
  }
  if (topic_has_endpoint_secret(result) && !verify_transport_security(s->cct, *(s->info.env))) {
    ldpp_dout(this, 1) << "topic '" << topic_name << "' contain secret and cannot be sent over insecure transport" << dendl;
    op_ret = -EPERM;
    return;
  }
  op_ret = verify_topic_owner_or_policy(
      s, result, driver->get_zone()->get_zonegroup().get_name(),
      rgw::IAM::snsGetTopicAttributes);
  if (op_ret != 0) {
    ldpp_dout(this, 1) << "no permission to get topic '" << topic_name
                       << "'" << dendl;
    return;
  }
  ldpp_dout(this, 1) << "successfully got topic '" << topic_name << "'" << dendl;
}

// command (AWS compliant): 
// POST
// Action=GetTopicAttributes&TopicArn=<topic-arn>
class RGWPSGetTopicAttributesOp : public RGWOp {
  private:
  std::string topic_name;
  rgw_pubsub_topic result;
  
  int get_params() {
    const auto topic_arn = rgw::ARN::parse((s->info.args.get("TopicArn")));

    if (!topic_arn || topic_arn->resource.empty()) {
        ldpp_dout(this, 1) << "GetTopicAttribute Action 'TopicArn' argument is missing or invalid" << dendl;
        return -EINVAL;
    }

    topic_name = topic_arn->resource;
    return 0;
  }

  public:
  int verify_permission(optional_yield y) override {
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
    f->open_object_section_in_ns("GetTopicAttributesResponse", AWS_SNS_NS);
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
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  const RGWPubSub ps(driver, s->owner.id.tenant,
                     &s->penv.site->get_period()->get_map().zonegroups);
  op_ret = ps.get_topic(this, topic_name, result, y);
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "failed to get topic '" << topic_name << "', ret=" << op_ret << dendl;
    return;
  }
  if (topic_has_endpoint_secret(result) && !verify_transport_security(s->cct, *(s->info.env))) {
    ldpp_dout(this, 1) << "topic '" << topic_name << "' contain secret and cannot be sent over insecure transport" << dendl;
    op_ret = -EPERM;
    return;
  }
  op_ret = verify_topic_owner_or_policy(
      s, result, driver->get_zone()->get_zonegroup().get_name(),
      rgw::IAM::snsGetTopicAttributes);
  if (op_ret != 0) {
    ldpp_dout(this, 1) << "no permission to get topic '" << topic_name
                       << "'" << dendl;
    return;
  }
  ldpp_dout(this, 1) << "successfully got topic '" << topic_name << "'" << dendl;
}

// command (AWS compliant):
// POST
// Action=SetTopicAttributes&TopicArn=<topic-arn>&AttributeName=<attribute-name>&AttributeValue=<attribute-value>
class RGWPSSetTopicAttributesOp : public RGWOp {
 private:
  std::string topic_name;
  std::string topic_arn;
  std::string opaque_data;
  std::string policy_text;
  rgw_pubsub_dest dest;
  rgw_user topic_owner;
  std::string attribute_name;

  int get_params() {
    const auto arn = rgw::ARN::parse((s->info.args.get("TopicArn")));

    if (!arn || arn->resource.empty()) {
      ldpp_dout(this, 1) << "SetTopicAttribute Action 'TopicArn' argument is "
                            "missing or invalid"
                         << dendl;
      return -EINVAL;
    }
    topic_arn = arn->to_string();
    topic_name = arn->resource;
    attribute_name = s->info.args.get("AttributeName");
    if (attribute_name.empty()) {
      ldpp_dout(this, 1)
          << "SetTopicAttribute Action 'AttributeName' argument is "
             "missing or invalid"
          << dendl;
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
      if (!validate_and_update_endpoint_secret(dest, s->cct, *(s->info.env))) {
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
      const std::unordered_set<std::string> push_endpoint_args = {
          "verify-ssl",    "use-ssl",         "ca-location", "amqp-ack-level",
          "amqp-exchange", "kafka-ack-level", "mechanism",   "cloudevents"};
      if (push_endpoint_args.count(attribute_name) == 1) {
        replace_str(attribute_name, s->info.args.get("AttributeValue"));
        return 0;
      }
      ldpp_dout(this, 1)
          << "SetTopicAttribute Action 'AttributeName' argument is "
             "invalid: 'AttributeName' = "
          << attribute_name << dendl;
      return -EINVAL;
    }
    return 0;
  }

 public:
  int verify_permission(optional_yield y) override {
    auto ret = get_params();
    if (ret < 0) {
      return ret;
    }
    rgw_pubsub_topic result;
    const RGWPubSub ps(driver, s->owner.id.tenant,
                       &s->penv.site->get_period()->get_map().zonegroups);
    ret = ps.get_topic(this, topic_name, result, y);
    if (ret < 0) {
      ldpp_dout(this, 1) << "failed to get topic '" << topic_name
                         << "', ret=" << ret << dendl;
      return ret;
    }
    topic_owner = result.user;
    ret = verify_topic_owner_or_policy(
        s, result, driver->get_zone()->get_zonegroup().get_name(),
        rgw::IAM::snsSetTopicAttributes);
    if (ret != 0) {
      ldpp_dout(this, 1) << "no permission to set attributes for topic '" << topic_name
                         << "'" << dendl;
      return ret;
    }

    return map_attributes(result);
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
    f->open_object_section_in_ns("SetTopicAttributesResponse", AWS_SNS_NS);
    f->open_object_section("ResponseMetadata");
    encode_xml("RequestId", s->req_id, f);
    f->close_section();  // ResponseMetadata
    f->close_section();  // SetTopicAttributesResponse
    rgw_flush_formatter_and_reset(s, f);
  }
};

void RGWPSSetTopicAttributesOp::execute(optional_yield y) {
  if (!driver->is_meta_master()) {
    bufferlist indata;
    op_ret = rgw_forward_request_to_master(
        this, *s->penv.site, s->user->get_id(), &indata, nullptr, s->info, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1)
          << "SetTopicAttributes forward_request_to_master returned ret = "
          << op_ret << dendl;
      return;
    }
  }
  if (!dest.push_endpoint.empty() && dest.persistent) {
    op_ret = rgw::notify::add_persistent_topic(topic_name, s->yield);
    if (op_ret < 0) {
      ldpp_dout(this, 1)
          << "SetTopicAttributes Action failed to create queue for "
             "persistent topics. error:"
          << op_ret << dendl;
      return;
    }
  } else {  // changing the persistent topic to non-persistent.
    op_ret = rgw::notify::remove_persistent_topic(topic_name, s->yield);
    if (op_ret != -ENOENT && op_ret < 0) {
      ldpp_dout(this, 1) << "SetTopicAttributes Action failed to remove queue "
                            "for persistent topics. error:"
                         << op_ret << dendl;
      return;
    }
  }
  const RGWPubSub ps(driver, s->owner.id.tenant,
                     &s->penv.site->get_period()->get_map().zonegroups);
  op_ret = ps.create_topic(this, topic_name, dest, topic_arn, opaque_data,
                           topic_owner, policy_text, y);
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "failed to SetAttributes for topic '" << topic_name
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
  std::string topic_name;
  
  int get_params() {
    const auto topic_arn = rgw::ARN::parse((s->info.args.get("TopicArn")));

    if (!topic_arn || topic_arn->resource.empty()) {
      ldpp_dout(this, 1) << "DeleteTopic Action 'TopicArn' argument is missing or invalid" << dendl;
      return -EINVAL;
    }

    topic_name = topic_arn->resource;
    return 0;
  }

  public:
  int verify_permission(optional_yield) override {
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
    f->open_object_section_in_ns("DeleteTopicResponse", AWS_SNS_NS);
    f->open_object_section("ResponseMetadata");
    encode_xml("RequestId", s->req_id, f); 
    f->close_section(); // ResponseMetadata
    f->close_section(); // DeleteTopicResponse
    rgw_flush_formatter_and_reset(s, f);
  }
};

void RGWPSDeleteTopicOp::execute(optional_yield y) {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  if (!driver->is_meta_master()) {
    bufferlist indata;
    op_ret = rgw_forward_request_to_master(
        this, *s->penv.site, s->user->get_id(), &indata, nullptr, s->info, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1)
          << "DeleteTopic forward_request_to_master returned ret = " << op_ret
          << dendl;
      return;
    }
  }
  const RGWPubSub ps(driver, s->owner.id.tenant,
                     &s->penv.site->get_period()->get_map().zonegroups);

  rgw_pubsub_topic result;
  op_ret = ps.get_topic(this, topic_name, result, y);
  if (op_ret == 0) {
    op_ret = verify_topic_owner_or_policy(
        s, result, driver->get_zone()->get_zonegroup().get_name(),
        rgw::IAM::snsDeleteTopic);
    if (op_ret != 0) {
      ldpp_dout(this, 1) << "no permission to remove topic '" << topic_name
                         << "'" << dendl;
      return;
    }
  } else {
    ldpp_dout(this, 1) << "failed to fetch topic '" << topic_name
                       << "' with error: " << op_ret << dendl;
    if (op_ret == -ENOENT) {
      // its not an error if no topics exist, just a no-op
      op_ret = 0;
    }
    return;
  }
  // upon deletion it is not known if topic is persistent or not
  // will try to delete the persistent topic anyway
  op_ret = rgw::notify::remove_persistent_topic(topic_name, s->yield);
  if (op_ret != -ENOENT && op_ret < 0) {
    ldpp_dout(this, 1) << "DeleteTopic Action failed to remove queue for "
                          "persistent topics. error:"
                       << op_ret << dendl;
    return;
  }
  op_ret = ps.remove_topic(this, topic_name, y);
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "failed to remove topic '" << topic_name << ", ret=" << op_ret << dendl;
    return;
  }
  ldpp_dout(this, 1) << "successfully removed topic '" << topic_name << "'" << dendl;
}

using op_generator = RGWOp*(*)();
static const std::unordered_map<std::string, op_generator> op_generators = {
    {"CreateTopic", []() -> RGWOp* { return new RGWPSCreateTopicOp; }},
    {"DeleteTopic", []() -> RGWOp* { return new RGWPSDeleteTopicOp; }},
    {"ListTopics", []() -> RGWOp* { return new RGWPSListTopicsOp; }},
    {"GetTopic", []() -> RGWOp* { return new RGWPSGetTopicOp; }},
    {"GetTopicAttributes",
     []() -> RGWOp* { return new RGWPSGetTopicAttributesOp; }},
    {"SetTopicAttributes",
     []() -> RGWOp* { return new RGWPSSetTopicAttributesOp; }}};

bool RGWHandler_REST_PSTopic_AWS::action_exists(const req_state* s) 
{
  if (s->info.args.exists("Action")) {
    const std::string action_name = s->info.args.get("Action");
    return op_generators.contains(action_name);
  }
  return false;
}
bool RGWHandler_REST_PSTopic_AWS::action_exists(const req_info& info) {
  if (info.args.exists("Action")) {
    const std::string action_name = info.args.get("Action");
    return op_generators.contains(action_name);
  }
  return false;
}

RGWOp *RGWHandler_REST_PSTopic_AWS::op_post()
{
  s->dialect = "sns";
  s->prot_flags = RGW_REST_STS;

  if (s->info.args.exists("Action")) {
    const std::string action_name = s->info.args.get("Action");
    const auto action_it = op_generators.find(action_name);
    if (action_it != op_generators.end()) {
      return action_it->second();
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

namespace {
// return a unique topic by prefexing with the notification name: <notification>_<topic>
std::string topic_to_unique(const std::string& topic, const std::string& notification) {
  return notification + "_" + topic;
}

// extract the topic from a unique topic of the form: <notification>_<topic>
[[maybe_unused]] std::string unique_to_topic(const std::string& unique_topic, const std::string& notification) {
  if (unique_topic.find(notification + "_") == std::string::npos) {
    return "";
  }
  return unique_topic.substr(notification.length() + 1);
}

// from list of bucket topics, find the one that was auto-generated by a notification
auto find_unique_topic(const rgw_pubsub_bucket_topics& bucket_topics, const std::string& notif_name) {
    auto it = std::find_if(bucket_topics.topics.begin(), bucket_topics.topics.end(), [&](const auto& val) { return notif_name == val.second.s3_id; });
    return it != bucket_topics.topics.end() ?
        std::optional<std::reference_wrapper<const rgw_pubsub_topic_filter>>(it->second):
        std::nullopt;
}
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
  int verify_params() override {
    bool exists;
    const auto no_value = s->info.args.get("notification", &exists);
    if (!exists) {
      ldpp_dout(this, 1) << "missing required param 'notification'" << dendl;
      return -EINVAL;
    } 
    if (no_value.length() > 0) {
      ldpp_dout(this, 1) << "param 'notification' should not have any value" << dendl;
      return -EINVAL;
    }
    if (s->bucket_name.empty()) {
      ldpp_dout(this, 1) << "request must be on a bucket" << dendl;
      return -EINVAL;
    }
    return 0;
  }

  int get_params_from_body(rgw_pubsub_s3_notifications& configurations) {
    const auto max_size = s->cct->_conf->rgw_max_put_param_size;
    int r;
    bufferlist data;
    std::tie(r, data) = read_all_input(s, max_size, false);

    if (r < 0) {
      ldpp_dout(this, 1) << "failed to read XML payload" << dendl;
      return r;
    }
    if (data.length() == 0) {
      ldpp_dout(this, 1) << "XML payload missing" << dendl;
      return -EINVAL;
    }

    RGWXMLDecoder::XMLParser parser;

    if (!parser.init()){
      ldpp_dout(this, 1) << "failed to initialize XML parser" << dendl;
      return -EINVAL;
    }
    if (!parser.parse(data.c_str(), data.length(), 1)) {
      ldpp_dout(this, 1) << "failed to parse XML payload" << dendl;
      return -ERR_MALFORMED_XML;
    }
    try {
      // NotificationConfigurations is mandatory
      // It can be empty which means we delete all the notifications
      RGWXMLDecoder::decode_xml("NotificationConfiguration", configurations, &parser, true);
    } catch (RGWXMLDecoder::err& err) {
      ldpp_dout(this, 1) << "failed to parse XML payload. error: " << err << dendl;
      return -ERR_MALFORMED_XML;
    }
    return 0;
  }
public:
  int verify_permission(optional_yield y) override;

  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }

  const char* name() const override { return "pubsub_notification_create_s3"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_NOTIF_CREATE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }


  void execute(optional_yield) override;
};

void RGWPSCreateNotifOp::execute(optional_yield y) {
  op_ret = verify_params();
  if (op_ret < 0) {
    return;
  }

  rgw_pubsub_s3_notifications configurations;
  op_ret = get_params_from_body(configurations);
  if (op_ret < 0) {
    return;
  }

  std::unique_ptr<rgw::sal::Bucket> bucket;
  op_ret = driver->load_bucket(this, rgw_bucket(s->bucket_tenant, s->bucket_name),
                               &bucket, y);
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "failed to get bucket '" << 
      (s->bucket_tenant.empty() ? s->bucket_name : s->bucket_tenant + ":" + s->bucket_name) << 
      "' info, ret = " << op_ret << dendl;
    return;
  }

  const RGWPubSub ps(driver, s->owner.id.tenant);
  const RGWPubSub::Bucket b(ps, bucket.get());

  if(configurations.list.empty()) {
    // get all topics on a bucket
    rgw_pubsub_bucket_topics bucket_topics;
    op_ret = b.get_topics(this, bucket_topics, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "failed to get list of topics from bucket '" << s->bucket_name << "', ret=" << op_ret << dendl;
      return;
    }

    op_ret = delete_all_notifications(this, bucket_topics, b, y, ps);
    return;
  }

  for (const auto& c : configurations.list) {
    const auto& notif_name = c.id;
    if (notif_name.empty()) {
      ldpp_dout(this, 1) << "missing notification id" << dendl;
      op_ret = -EINVAL;
      return;
    }
    if (c.topic_arn.empty()) {
      ldpp_dout(this, 1) << "missing topic ARN in notification: '" << notif_name << "'" << dendl;
      op_ret = -EINVAL;
      return;
    }

    const auto arn = rgw::ARN::parse(c.topic_arn);
    if (!arn || arn->resource.empty()) {
      ldpp_dout(this, 1) << "topic ARN has invalid format: '" << c.topic_arn << "' in notification: '" << notif_name << "'" << dendl;
      op_ret = -EINVAL;
      return;
    }

    if (std::find(c.events.begin(), c.events.end(), rgw::notify::UnknownEvent) != c.events.end()) {
      ldpp_dout(this, 1) << "unknown event type in notification: '" << notif_name << "'" << dendl;
      op_ret = -EINVAL;
      return;
    }

    const auto topic_name = arn->resource;

    // get topic information. destination information is stored in the topic
    rgw_pubsub_topic topic_info;  
    op_ret = ps.get_topic(this, topic_name, topic_info, y);
    if (op_ret < 0) {
      ldpp_dout(this, 1) << "failed to get topic '" << topic_name << "', ret=" << op_ret << dendl;
      return;
    }
    op_ret = verify_topic_owner_or_policy(
        s, topic_info, driver->get_zone()->get_zonegroup().get_name(),
        rgw::IAM::snsPublish);
    if (op_ret != 0) {
      ldpp_dout(this, 1) << "no permission to create notification for topic '"
                         << topic_name << "'" << dendl;
      return;
    }
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

int RGWPSCreateNotifOp::verify_permission(optional_yield y) {
  if (!verify_bucket_permission(this, s, rgw::IAM::s3PutBucketNotification)) {
    return -EACCES;
  }

  return 0;
}

// command (extension to S3): DELETE /bucket?notification[=<notification-id>]
class RGWPSDeleteNotifOp : public RGWDefaultResponseOp {
  int get_params(std::string& notif_name) const {
    bool exists;
    notif_name = s->info.args.get("notification", &exists);
    if (!exists) {
      ldpp_dout(this, 1) << "missing required param 'notification'" << dendl;
      return -EINVAL;
    } 
    if (s->bucket_name.empty()) {
      ldpp_dout(this, 1) << "request must be on a bucket" << dendl;
      return -EINVAL;
    }
    return 0;
  }

public:
  int verify_permission(optional_yield y) override;

  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  
  const char* name() const override { return "pubsub_notification_delete_s3"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_NOTIF_DELETE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }

  void execute(optional_yield y) override;
};

void RGWPSDeleteNotifOp::execute(optional_yield y) {
  std::string notif_name;
  op_ret = get_params(notif_name);
  if (op_ret < 0) {
    return;
  }

  std::unique_ptr<rgw::sal::Bucket> bucket;
  op_ret = driver->load_bucket(this, rgw_bucket(s->bucket_tenant, s->bucket_name),
                               &bucket, y);
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "failed to get bucket '" << 
      (s->bucket_tenant.empty() ? s->bucket_name : s->bucket_tenant + ":" + s->bucket_name) << 
      "' info, ret = " << op_ret << dendl;
    return;
  }

  const RGWPubSub ps(driver, s->owner.id.tenant);
  const RGWPubSub::Bucket b(ps, bucket.get());

  // get all topics on a bucket
  rgw_pubsub_bucket_topics bucket_topics;
  op_ret = b.get_topics(this, bucket_topics, y);
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "failed to get list of topics from bucket '" << s->bucket_name << "', ret=" << op_ret << dendl;
    return;
  }

  if (!notif_name.empty()) {
    // delete a specific notification
    const auto unique_topic = find_unique_topic(bucket_topics, notif_name);
    if (unique_topic) {
      const auto unique_topic_name = unique_topic->get().topic.name;
      op_ret = remove_notification_by_topic(this, unique_topic_name, b, y, ps);
      return;
    }
    // notification to be removed is not found - considered success
    ldpp_dout(this, 20) << "notification '" << notif_name << "' already removed" << dendl;
    return;
  }

  op_ret = delete_all_notifications(this, bucket_topics, b, y, ps);
}

int RGWPSDeleteNotifOp::verify_permission(optional_yield y) {
  if (!verify_bucket_permission(this, s, rgw::IAM::s3PutBucketNotification)) {
    return -EACCES;
  }

  return 0;
}

// command (S3 compliant): GET /bucket?notification[=<notification-id>]
class RGWPSListNotifsOp : public RGWOp {
  rgw_pubsub_s3_notifications notifications;

  int get_params(std::string& notif_name) const {
    bool exists;
    notif_name = s->info.args.get("notification", &exists);
    if (!exists) {
      ldpp_dout(this, 1) << "missing required param 'notification'" << dendl;
      return -EINVAL;
    } 
    if (s->bucket_name.empty()) {
      ldpp_dout(this, 1) << "request must be on a bucket" << dendl;
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
    ldpp_dout(this, 1) << "failed to get bucket '" << 
      (s->bucket_tenant.empty() ? s->bucket_name : s->bucket_tenant + ":" + s->bucket_name) << 
      "' info, ret = " << op_ret << dendl;
    return;
  }

  const RGWPubSub ps(driver, s->owner.id.tenant);
  const RGWPubSub::Bucket b(ps, bucket.get());
  
  // get all topics on a bucket
  rgw_pubsub_bucket_topics bucket_topics;
  op_ret = b.get_topics(this, bucket_topics, y);
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "failed to get list of topics from bucket '" << s->bucket_name << "', ret=" << op_ret << dendl;
    return;
  }
  if (!notif_name.empty()) {
    // get info of a specific notification
    const auto unique_topic = find_unique_topic(bucket_topics, notif_name);
    if (unique_topic) {
      notifications.list.emplace_back(unique_topic->get());
      return;
    }
    op_ret = -ENOENT;
    ldpp_dout(this, 1) << "failed to get notification info for '" << notif_name << "', ret=" << op_ret << dendl;
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

