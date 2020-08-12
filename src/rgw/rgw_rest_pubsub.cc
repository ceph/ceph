// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <algorithm>
#include <boost/tokenizer.hpp>
#include <optional>
#include "rgw_rest_pubsub_common.h"
#include "rgw_rest_pubsub.h"
#include "rgw_pubsub_push.h"
#include "rgw_pubsub.h"
#include "rgw_sync_module_pubsub.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_arn.h"
#include "rgw_auth_s3.h"
#include "services/svc_zone.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw


// command (AWS compliant): 
// POST
// Action=CreateTopic&Name=<topic-name>[&push-endpoint=<endpoint>[&<arg1>=<value1>]]
class RGWPSCreateTopic_ObjStore_AWS : public RGWPSCreateTopicOp {
public:
  int get_params() override {
    topic_name = s->info.args.get("Name");
    if (topic_name.empty()) {
      ldout(s->cct, 1) << "CreateTopic Action 'Name' argument is missing" << dendl;
      return -EINVAL;
    }

    opaque_data = s->info.args.get("OpaqueData");

    dest.push_endpoint = s->info.args.get("push-endpoint");

    if (!validate_and_update_endpoint_secret(dest, s->cct, *(s->info.env))) {
      return -EINVAL;
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
    // bucket to store events/records will be set only when subscription is created
    dest.bucket_name = "";
    dest.oid_prefix = "";
    dest.arn_topic = topic_name;
    // the topic ARN will be sent in the reply
    const rgw::ARN arn(rgw::Partition::aws, rgw::Service::sns, 
        store->svc()->zone->get_zonegroup().get_name(),
        s->user->get_tenant(), topic_name);
    topic_arn = arn.to_string();
    return 0;
  }

  void send_response(const Span& parent_span = nullptr) override {
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/xml");

    if (op_ret < 0) {
      return;
    }

    const auto f = s->formatter;
    f->open_object_section_in_ns("CreateTopicResponse", "https://sns.amazonaws.com/doc/2010-03-31/");
    f->open_object_section("CreateTopicResult");
    encode_xml("TopicArn", topic_arn, f); 
    f->close_section();
    f->open_object_section("ResponseMetadata");
    encode_xml("RequestId", s->req_id, f); 
    f->close_section();
    f->close_section();
    rgw_flush_formatter_and_reset(s, f);
  }
};

// command (AWS compliant): 
// POST 
// Action=ListTopics
class RGWPSListTopics_ObjStore_AWS : public RGWPSListTopicsOp {
public:
  void send_response(const Span& parent_span = nullptr) override {
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/xml");

    if (op_ret < 0) {
      return;
    }

    const auto f = s->formatter;
    f->open_object_section_in_ns("ListTopicsResponse", "https://sns.amazonaws.com/doc/2010-03-31/");
    f->open_object_section("ListTopicsResult");
    encode_xml("Topics", result, f); 
    f->close_section();
    f->open_object_section("ResponseMetadata");
    encode_xml("RequestId", s->req_id, f); 
    f->close_section();
    f->close_section();
    rgw_flush_formatter_and_reset(s, f);
  }
};

// command (extension to AWS): 
// POST
// Action=GetTopic&TopicArn=<topic-arn>
class RGWPSGetTopic_ObjStore_AWS : public RGWPSGetTopicOp {
public:
  int get_params() override {
    const auto topic_arn = rgw::ARN::parse((s->info.args.get("TopicArn")));

    if (!topic_arn || topic_arn->resource.empty()) {
        ldout(s->cct, 1) << "GetTopic Action 'TopicArn' argument is missing or invalid" << dendl;
        return -EINVAL;
    }

    topic_name = topic_arn->resource;
    return 0;
  }

  void send_response(const Span& parent_span = nullptr) override {
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
    encode_xml("Topic", result.topic, f); 
    f->close_section();
    f->open_object_section("ResponseMetadata");
    encode_xml("RequestId", s->req_id, f); 
    f->close_section();
    f->close_section();
    rgw_flush_formatter_and_reset(s, f);
  }
};

// command (AWS compliant): 
// POST
// Action=DeleteTopic&TopicArn=<topic-arn>
class RGWPSDeleteTopic_ObjStore_AWS : public RGWPSDeleteTopicOp {
public:
  int get_params() override {
    const auto topic_arn = rgw::ARN::parse((s->info.args.get("TopicArn")));

    if (!topic_arn || topic_arn->resource.empty()) {
      ldout(s->cct, 1) << "DeleteTopic Action 'TopicArn' argument is missing or invalid" << dendl;
      return -EINVAL;
    }

    topic_name = topic_arn->resource;
    return 0;
  }
  
  void send_response(const Span& parent_span = nullptr) override {
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/xml");

    if (op_ret < 0) {
      return;
    }

    const auto f = s->formatter;
    f->open_object_section_in_ns("DeleteTopicResponse", "https://sns.amazonaws.com/doc/2010-03-31/");
    f->open_object_section("ResponseMetadata");
    encode_xml("RequestId", s->req_id, f); 
    f->close_section();
    f->close_section();
    rgw_flush_formatter_and_reset(s, f);
  }
};

namespace {
// utility classes and functions for handling parameters with the following format:
// Attributes.entry.{N}.{key|value}={VALUE}
// N - any unsigned number
// VALUE - url encoded string

// and Attribute is holding key and value
// ctor and set are done according to the "type" argument
// if type is not "key" or "value" its a no-op
class Attribute {
  std::string key;
  std::string value;
public:
  Attribute(const std::string& type, const std::string& key_or_value) {
    set(type, key_or_value);
  }
  void set(const std::string& type, const std::string& key_or_value) {
    if (type == "key") {
      key = key_or_value;
    } else if (type == "value") {
      value = key_or_value;
    }
  }
  const std::string& get_key() const { return key; }
  const std::string& get_value() const { return value; }
};

using AttributeMap = std::map<unsigned, Attribute>;

// aggregate the attributes into a map
// the key and value are associated by the index (N)
// no assumptions are made on the order in which these parameters are added
void update_attribute_map(const std::string& input, AttributeMap& map) {
  const boost::char_separator<char> sep(".");
  const boost::tokenizer tokens(input, sep);
  auto token = tokens.begin();
  if (*token != "Attributes") {
      return;
  }
  ++token;

  if (*token != "entry") {
      return;
  }
  ++token;

  unsigned idx;
  try {
    idx = std::stoul(*token);
  } catch (const std::invalid_argument&) {
    return;
  }
  ++token;

  std::string key_or_value = "";
  // get the rest of the string regardless of dots
  // this is to allow dots in the value
  while (token != tokens.end()) {
    key_or_value.append(*token+".");
    ++token;
  }
  // remove last separator
  key_or_value.pop_back();

  auto pos = key_or_value.find("=");
  if (pos != string::npos) {
    const auto key_or_value_lhs = key_or_value.substr(0, pos);
    const auto key_or_value_rhs = url_decode(key_or_value.substr(pos + 1, key_or_value.size() - 1));
    const auto map_it = map.find(idx);
    if (map_it == map.end()) {
      // new entry
      map.emplace(std::make_pair(idx, Attribute(key_or_value_lhs, key_or_value_rhs)));
    } else {
      // existing entry
      map_it->second.set(key_or_value_lhs, key_or_value_rhs);
    }
  }
}
}

void RGWHandler_REST_PSTopic_AWS::rgw_topic_parse_input() {
  if (post_body.size() > 0) {
    ldout(s->cct, 10) << "Content of POST: " << post_body << dendl;

    if (post_body.find("Action") != string::npos) {
      const boost::char_separator<char> sep("&");
      const boost::tokenizer<boost::char_separator<char>> tokens(post_body, sep);
      AttributeMap map;
      for (const auto& t : tokens) {
        auto pos = t.find("=");
        if (pos != string::npos) {
          const auto key = t.substr(0, pos);
          if (key == "Action") {
            s->info.args.append(key, t.substr(pos + 1, t.size() - 1));
          } else if (key == "Name" || key == "TopicArn") {
            const auto value = url_decode(t.substr(pos + 1, t.size() - 1));
            s->info.args.append(key, value);
          } else {
            update_attribute_map(t, map);
          }
        }
      }
      // update the regular args with the content of the attribute map
      for (const auto& attr : map) {
          s->info.args.append(attr.second.get_key(), attr.second.get_value());
      }
    }
    const auto payload_hash = rgw::auth::s3::calc_v4_payload_hash(post_body);
    s->info.args.append("PayloadHash", payload_hash);
  }
}

RGWOp* RGWHandler_REST_PSTopic_AWS::op_post() {
  rgw_topic_parse_input();

  if (s->info.args.exists("Action")) {
    const auto action = s->info.args.get("Action");
    if (action.compare("CreateTopic") == 0)
      return new RGWPSCreateTopic_ObjStore_AWS();
    if (action.compare("DeleteTopic") == 0)
      return new RGWPSDeleteTopic_ObjStore_AWS;
    if (action.compare("ListTopics") == 0)
      return new RGWPSListTopics_ObjStore_AWS();
    if (action.compare("GetTopic") == 0)
      return new RGWPSGetTopic_ObjStore_AWS();
  }

  return nullptr;
}

int RGWHandler_REST_PSTopic_AWS::authorize(const DoutPrefixProvider* dpp) {
  /*if (s->info.args.exists("Action") && s->info.args.get("Action").find("Topic") != std::string::npos) {
      // TODO: some topic specific authorization
      return 0;
  }*/
  return RGW_Auth_S3::authorize(dpp, store, auth_registry, s);
}


namespace {
// return a unique topic by prefexing with the notification name: <notification>_<topic>
std::string topic_to_unique(const std::string& topic, const std::string& notification) {
  return notification + "_" + topic;
}

// extract the topic from a unique topic of the form: <notification>_<topic>
[[maybe_unused]] std::string unique_to_topic(const std::string& unique_topic, const std::string& notification) {
  if (unique_topic.find(notification + "_") == string::npos) {
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

// command (S3 compliant): PUT /<bucket name>?notification
// a "notification" and a subscription will be auto-generated
// actual configuration is XML encoded in the body of the message
class RGWPSCreateNotif_ObjStore_S3 : public RGWPSCreateNotifOp {
  rgw_pubsub_s3_notifications configurations;

  int get_params_from_body() {
    const auto max_size = s->cct->_conf->rgw_max_put_param_size;
    int r;
    bufferlist data;
    std::tie(r, data) = rgw_rest_read_all_input(s, max_size, false);

    if (r < 0) {
      ldout(s->cct, 1) << "failed to read XML payload" << dendl;
      return r;
    }
    if (data.length() == 0) {
      ldout(s->cct, 1) << "XML payload missing" << dendl;
      return -EINVAL;
    }

    RGWXMLDecoder::XMLParser parser;

    if (!parser.init()){
      ldout(s->cct, 1) << "failed to initialize XML parser" << dendl;
      return -EINVAL;
    }
    if (!parser.parse(data.c_str(), data.length(), 1)) {
      ldout(s->cct, 1) << "failed to parse XML payload" << dendl;
      return -ERR_MALFORMED_XML;
    }
    try {
      // NotificationConfigurations is mandatory
      RGWXMLDecoder::decode_xml("NotificationConfiguration", configurations, &parser, true);
    } catch (RGWXMLDecoder::err& err) {
      ldout(s->cct, 1) << "failed to parse XML payload. error: " << err << dendl;
      return -ERR_MALFORMED_XML;
    }
    return 0;
  }

  int get_params() override {
    bool exists;
    const auto no_value = s->info.args.get("notification", &exists);
    if (!exists) {
      ldout(s->cct, 1) << "missing required param 'notification'" << dendl;
      return -EINVAL;
    } 
    if (no_value.length() > 0) {
      ldout(s->cct, 1) << "param 'notification' should not have any value" << dendl;
      return -EINVAL;
    }
    if (s->bucket_name.empty()) {
      ldout(s->cct, 1) << "request must be on a bucket" << dendl;
      return -EINVAL;
    }
    bucket_name = s->bucket_name;
    return 0;
  }

public:
  const char* name() const override { return "pubsub_notification_create_s3"; }
  void execute(const Span& parent_span = nullptr) override;
};

void RGWPSCreateNotif_ObjStore_S3::execute(const Span& parent_span) {
  op_ret = get_params_from_body();
  if (op_ret < 0) {
    return;
  }

  ups.emplace(store, s->owner.get_id());
  auto b = ups->get_bucket(bucket_info.bucket);
  ceph_assert(b);
  std::string data_bucket_prefix = "";
  std::string data_oid_prefix = "";
  bool push_only = true;
  if (store->getRados()->get_sync_module()) {
    const auto psmodule = dynamic_cast<RGWPSSyncModuleInstance*>(store->getRados()->get_sync_module().get());
    if (psmodule) {
      const auto& conf = psmodule->get_effective_conf();
      data_bucket_prefix = conf["data_bucket_prefix"];
      data_oid_prefix = conf["data_oid_prefix"];
      // TODO: allow "push-only" on PS zone as well
      push_only = false;
    }
  }

  for (const auto& c : configurations.list) {
    const auto& notif_name = c.id;
    if (notif_name.empty()) {
      ldout(s->cct, 1) << "missing notification id" << dendl;
      op_ret = -EINVAL;
      return;
    }
    if (c.topic_arn.empty()) {
      ldout(s->cct, 1) << "missing topic ARN in notification: '" << notif_name << "'" << dendl;
      op_ret = -EINVAL;
      return;
    }

    const auto arn = rgw::ARN::parse(c.topic_arn);
    if (!arn || arn->resource.empty()) {
      ldout(s->cct, 1) << "topic ARN has invalid format: '" << c.topic_arn << "' in notification: '" << notif_name << "'" << dendl;
      op_ret = -EINVAL;
      return;
    }

    if (std::find(c.events.begin(), c.events.end(), rgw::notify::UnknownEvent) != c.events.end()) {
      ldout(s->cct, 1) << "unknown event type in notification: '" << notif_name << "'" << dendl;
      op_ret = -EINVAL;
      return;
    }

    const auto topic_name = arn->resource;

    // get topic information. destination information is stored in the topic
    rgw_pubsub_topic topic_info;  
    op_ret = ups->get_topic(topic_name, &topic_info);
    if (op_ret < 0) {
      ldout(s->cct, 1) << "failed to get topic '" << topic_name << "', ret=" << op_ret << dendl;
      return;
    }
    // make sure that full topic configuration match
    // TODO: use ARN match function
    
    // create unique topic name. this has 2 reasons:
    // (1) topics cannot be shared between different S3 notifications because they hold the filter information
    // (2) make topic clneaup easier, when notification is removed
    const auto unique_topic_name = topic_to_unique(topic_name, notif_name);
    // generate the internal topic. destination is stored here for the "push-only" case
    // when no subscription exists
    // ARN is cached to make the "GET" method faster
    op_ret = ups->create_topic(unique_topic_name, topic_info.dest, topic_info.arn, topic_info.opaque_data);
    if (op_ret < 0) {
      ldout(s->cct, 1) << "failed to auto-generate unique topic '" << unique_topic_name << 
        "', ret=" << op_ret << dendl;
      return;
    }
    ldout(s->cct, 20) << "successfully auto-generated unique topic '" << unique_topic_name << "'" << dendl;
    // generate the notification
    rgw::notify::EventTypeList events;
    op_ret = b->create_notification(unique_topic_name, c.events, std::make_optional(c.filter), notif_name);
    if (op_ret < 0) {
      ldout(s->cct, 1) << "failed to auto-generate notification for unique topic '" << unique_topic_name <<
        "', ret=" << op_ret << dendl;
      // rollback generated topic (ignore return value)
      ups->remove_topic(unique_topic_name);
      return;
    }
    ldout(s->cct, 20) << "successfully auto-generated notification for unique topic '" << unique_topic_name << "'" << dendl;
  
    if (!push_only) {
      // generate the subscription with destination information from the original topic
      rgw_pubsub_sub_dest dest = topic_info.dest;
      dest.bucket_name = data_bucket_prefix + s->owner.get_id().to_str() + "-" + unique_topic_name;
      dest.oid_prefix = data_oid_prefix + notif_name + "/";
      auto sub = ups->get_sub(notif_name);
      op_ret = sub->subscribe(unique_topic_name, dest, notif_name);
      if (op_ret < 0) {
        ldout(s->cct, 1) << "failed to auto-generate subscription '" << notif_name << "', ret=" << op_ret << dendl;
        // rollback generated notification (ignore return value)
        b->remove_notification(unique_topic_name);
        // rollback generated topic (ignore return value)
        ups->remove_topic(unique_topic_name);
        return;
      }
      ldout(s->cct, 20) << "successfully auto-generated subscription '" << notif_name << "'" << dendl;
    }
  }
}

// command (extension to S3): DELETE /bucket?notification[=<notification-id>]
class RGWPSDeleteNotif_ObjStore_S3 : public RGWPSDeleteNotifOp {
private:
  std::string notif_name;

  int get_params() override {
    bool exists;
    notif_name = s->info.args.get("notification", &exists);
    if (!exists) {
      ldout(s->cct, 1) << "missing required param 'notification'" << dendl;
      return -EINVAL;
    } 
    if (s->bucket_name.empty()) {
      ldout(s->cct, 1) << "request must be on a bucket" << dendl;
      return -EINVAL;
    }
    bucket_name = s->bucket_name;
    return 0;
  }

  void remove_notification_by_topic(const std::string& topic_name, const RGWUserPubSub::BucketRef& b) {
    op_ret = b->remove_notification(topic_name);
    if (op_ret < 0) {
      ldout(s->cct, 1) << "failed to remove notification of topic '" << topic_name << "', ret=" << op_ret << dendl;
    }
    op_ret = ups->remove_topic(topic_name);
    if (op_ret < 0) {
      ldout(s->cct, 1) << "failed to remove auto-generated topic '" << topic_name << "', ret=" << op_ret << dendl;
    }
  }

public:
  void execute(const Span& parent_span = nullptr) override;
  const char* name() const override { return "pubsub_notification_delete_s3"; }
};

void RGWPSDeleteNotif_ObjStore_S3::execute(const Span& parent_span) {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  ups.emplace(store, s->owner.get_id());
  auto b = ups->get_bucket(bucket_info.bucket);
  ceph_assert(b);

  // get all topics on a bucket
  rgw_pubsub_bucket_topics bucket_topics;
  op_ret = b->get_topics(&bucket_topics);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to get list of topics from bucket '" << bucket_info.bucket.name << "', ret=" << op_ret << dendl;
    return;
  }

  if (!notif_name.empty()) {
    // delete a specific notification
    const auto unique_topic = find_unique_topic(bucket_topics, notif_name);
    if (unique_topic) {
      // remove the auto generated subscription according to notification name (if exist)
      const auto unique_topic_name = unique_topic->get().topic.name;
      auto sub = ups->get_sub(notif_name);
      op_ret = sub->unsubscribe(unique_topic_name);
      if (op_ret < 0 && op_ret != -ENOENT) {
        ldout(s->cct, 1) << "failed to remove auto-generated subscription '" << notif_name << "', ret=" << op_ret << dendl;
        return;
      }
      remove_notification_by_topic(unique_topic_name, b);
      return;
    }
    // notification to be removed is not found - considered success
    ldout(s->cct, 20) << "notification '" << notif_name << "' already removed" << dendl;
    return;
  }

  // delete all notification of on a bucket
  for (const auto& topic : bucket_topics.topics) {
    // remove the auto generated subscription of the topic (if exist)
    rgw_pubsub_topic_subs topic_subs;
    op_ret = ups->get_topic(topic.first, &topic_subs);
    for (const auto& topic_sub_name : topic_subs.subs) {
      auto sub = ups->get_sub(topic_sub_name);
      rgw_pubsub_sub_config sub_conf;
      op_ret = sub->get_conf(&sub_conf);
      if (op_ret < 0) {
        ldout(s->cct, 1) << "failed to get subscription '" << topic_sub_name << "' info, ret=" << op_ret << dendl;
        return;
      }
      if (!sub_conf.s3_id.empty()) {
        // S3 notification, has autogenerated subscription
        const auto& sub_topic_name = sub_conf.topic;
        op_ret = sub->unsubscribe(sub_topic_name);
        if (op_ret < 0) {
          ldout(s->cct, 1) << "failed to remove auto-generated subscription '" << topic_sub_name << "', ret=" << op_ret << dendl;
          return;
        }
      }
    }
    remove_notification_by_topic(topic.first, b);
  }
}

// command (S3 compliant): GET /bucket?notification[=<notification-id>]
class RGWPSListNotifs_ObjStore_S3 : public RGWPSListNotifsOp {
private:
  std::string notif_name;
  rgw_pubsub_s3_notifications notifications;

  int get_params() override {
    bool exists;
    notif_name = s->info.args.get("notification", &exists);
    if (!exists) {
      ldout(s->cct, 1) << "missing required param 'notification'" << dendl;
      return -EINVAL;
    } 
    if (s->bucket_name.empty()) {
      ldout(s->cct, 1) << "request must be on a bucket" << dendl;
      return -EINVAL;
    }
    bucket_name = s->bucket_name;
    return 0;
  }

public:
  void execute(const Span& parent_span = nullptr) override;
  void send_response(const Span& parent_span = nullptr) override {
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
  const char* name() const override { return "pubsub_notifications_get_s3"; }
};

void RGWPSListNotifs_ObjStore_S3::execute(const Span& parent_span) {
  ups.emplace(store, s->owner.get_id());
  auto b = ups->get_bucket(bucket_info.bucket);
  ceph_assert(b);
  
  // get all topics on a bucket
  rgw_pubsub_bucket_topics bucket_topics;
  op_ret = b->get_topics(&bucket_topics);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to get list of topics from bucket '" << bucket_info.bucket.name << "', ret=" << op_ret << dendl;
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
    ldout(s->cct, 1) << "failed to get notification info for '" << notif_name << "', ret=" << op_ret << dendl;
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

RGWOp* RGWHandler_REST_PSNotifs_S3::op_get() {
  return new RGWPSListNotifs_ObjStore_S3();
}

RGWOp* RGWHandler_REST_PSNotifs_S3::op_put() {
  return new RGWPSCreateNotif_ObjStore_S3();
}

RGWOp* RGWHandler_REST_PSNotifs_S3::op_delete() {
  return new RGWPSDeleteNotif_ObjStore_S3();
}

RGWOp* RGWHandler_REST_PSNotifs_S3::create_get_op() {
    return new RGWPSListNotifs_ObjStore_S3();
}

RGWOp* RGWHandler_REST_PSNotifs_S3::create_put_op() {
  return new RGWPSCreateNotif_ObjStore_S3();
}

RGWOp* RGWHandler_REST_PSNotifs_S3::create_delete_op() {
  return new RGWPSDeleteNotif_ObjStore_S3();
}

