// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <algorithm>
#include "rgw_base_pubsub_rest.h"
#include "rgw_pubsub_rest.h"
#include "rgw_pubsub_push.h"
#include "rgw_pubsub.h"
#include "rgw_sync_module_pubsub.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_arn.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

namespace {
// conversion functions between S3 and GCP style event names
std::string s3_to_gcp_event(const std::string& event) {
  if (event == "s3:ObjectCreated:*") {
    return "OBJECT_CREATE";
  }
  if (event == "s3:ObjectRemoved:*") {
    return "OBJECT_DELETE";
  }
  return "UNKNOWN_EVENT";
}
std::string gcp_to_s3_event(const std::string& event) {
  if (event == "OBJECT_CREATE") {
    return "s3:ObjectCreated:";
  }
  if (event == "OBJECT_DELETE") {
    return "s3:ObjectRemoved:";
  }
  return "UNKNOWN_EVENT";
}

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
      // TopicConfigurations is mandatory
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
  void execute() override;
};

void RGWPSCreateNotif_ObjStore_S3::execute() {
  op_ret = get_params_from_body();
  if (op_ret < 0) {
    return;
  }

  ups = std::make_unique<RGWUserPubSub>(store, s->owner.get_id());
  auto b = ups->get_bucket(bucket_info.bucket);
  ceph_assert(b);
  std::string data_bucket_prefix = "";
  std::string data_oid_prefix = "";
  if (store->get_sync_module()) {
    const auto psmodule = dynamic_cast<RGWPSSyncModuleInstance*>(store->get_sync_module().get());
    if (psmodule) {
        const auto& conf = psmodule->get_effective_conf();
        data_bucket_prefix = conf["data_bucket_prefix"];
        data_oid_prefix = conf["data_oid_prefix"];
    }
  }

  for (const auto& c : configurations.list) {
    const auto& sub_name = c.id;
    if (sub_name.empty()) {
      ldout(s->cct, 1) << "missing notification id" << dendl;
      op_ret = -EINVAL;
      return;
    }
    if (c.topic_arn.empty()) {
      ldout(s->cct, 1) << "missing topic ARN" << dendl;
      op_ret = -EINVAL;
      return;
    }

    const auto arn = rgw::ARN::parse(c.topic_arn);
    if (!arn || arn->resource.empty()) {
      ldout(s->cct, 1) << "topic ARN has invalid format:" << c.topic_arn << dendl;
      op_ret = -EINVAL;
      return;
    }

    const auto topic_name = arn->resource;

    // get topic information. destination information is stored in the topic
    rgw_pubsub_topic_subs topic_info;  
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
    const auto unique_topic_name = topic_to_unique(topic_name, sub_name);
    // generate the internal topic, no need to store destination info in thr unique topic
    // ARN is cached to make the "GET" method faster
    op_ret = ups->create_topic(unique_topic_name, rgw_pubsub_sub_dest(), topic_info.topic.arn);
    if (op_ret < 0) {
      ldout(s->cct, 1) << "failed to auto-generate topic '" << unique_topic_name << 
        "', ret=" << op_ret << dendl;
      return;
    }
    // generate the notification
    EventTypeList events;
    std::transform(c.events.begin(), c.events.end(), std::inserter(events, events.begin()), s3_to_gcp_event);
    op_ret = b->create_notification(unique_topic_name, events);
    if (op_ret < 0) {
      ldout(s->cct, 1) << "failed to auto-generate notification on topic '" << unique_topic_name <<
        "', ret=" << op_ret << dendl;
      // rollback generated topic (ignore return value)
      ups->remove_topic(unique_topic_name);
      return;
    }
   
    // generate the subscription with destination information from the original topic
    rgw_pubsub_sub_dest dest = topic_info.topic.dest;
    dest.bucket_name = data_bucket_prefix + s->owner.get_id().to_str() + "-" + unique_topic_name;
    dest.oid_prefix = data_oid_prefix + sub_name + "/";
    auto sub = ups->get_sub(sub_name);
    op_ret = sub->subscribe(unique_topic_name, dest, sub_name);
    if (op_ret < 0) {
      ldout(s->cct, 1) << "failed to auto-generate subscription '" << sub_name << "', ret=" << op_ret << dendl;
      // rollback generated notification (ignore return value)
      b->remove_notification(unique_topic_name);
      // rollback generated topic (ignore return value)
      ups->remove_topic(unique_topic_name);
      return;
    }
    ldout(s->cct, 20) << "successfully auto-generated subscription '" << sub_name << "'" << dendl;
  }
}

// command (extension to S3): DELETE /bucket?notification[=<notification-id>]
class RGWPSDeleteNotif_ObjStore_S3 : public RGWPSDeleteNotifOp {
private:
  std::string sub_name;

  int get_params() override {
    bool exists;
    sub_name = s->info.args.get("notification", &exists);
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

  void delete_notification(const std::string& _sub_name, const RGWUserPubSub::BucketRef& b, bool must_delete) {
    auto sub = ups->get_sub(_sub_name);
    rgw_pubsub_sub_config sub_conf;
    op_ret = sub->get_conf(&sub_conf);
    if (op_ret < 0) {
      ldout(s->cct, 1) << "failed to get notification info, ret=" << op_ret << dendl;
      return;
    }
    if (sub_conf.s3_id.empty()) {
      if (must_delete) {
        op_ret = -ENOENT;
        ldout(s->cct, 1) << "notification does not have an ID, ret=" << op_ret << dendl;
      }
      return;
    }
    const auto& sub_topic_name = sub_conf.topic;
    op_ret = sub->unsubscribe(sub_topic_name);
    if (op_ret < 0) {
      ldout(s->cct, 1) << "failed to remove auto-generated subscription, ret=" << op_ret << dendl;
      return;
    }
    op_ret = b->remove_notification(sub_topic_name);
    if (op_ret < 0) {
      ldout(s->cct, 1) << "failed to remove auto-generated notification, ret=" << op_ret << dendl;
    }
    op_ret = ups->remove_topic(sub_topic_name);
    if (op_ret < 0) {
      ldout(s->cct, 1) << "failed to remove auto-generated topic, ret=" << op_ret << dendl;
    }
    return;
  }

public:
  void execute() override;
  const char* name() const override { return "pubsub_notification_delete_s3"; }
};

void RGWPSDeleteNotif_ObjStore_S3::execute() {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  ups = std::make_unique<RGWUserPubSub>(store, s->owner.get_id());
  auto b = ups->get_bucket(bucket_info.bucket);
  ceph_assert(b);

  if (!sub_name.empty()) {
    // delete a specific notification
    delete_notification(sub_name, b, true);
    return;
  }

  // delete all notifications on a bucket
  rgw_pubsub_bucket_topics bucket_topics;
  b->get_topics(&bucket_topics);
  // loop through all topics of the bucket
  for (const auto& topic : bucket_topics.topics) {
    // for each topic get all subscriptions
    rgw_pubsub_topic_subs topic_subs;
    ups->get_topic(topic.first, &topic_subs);
    // loop through all subscriptions
    for (const auto& topic_sub_name : topic_subs.subs) {
      delete_notification(topic_sub_name, b, false);
    }
  }
}

// command (S3 compliant): GET /bucket?notification[=<notification-id>]
class RGWPSListNotifs_ObjStore_S3 : public RGWPSListNotifsOp {
private:
  std::string sub_name;
  rgw_pubsub_s3_notifications notifications;

  int get_params() override {
    bool exists;
    sub_name = s->info.args.get("notification", &exists);
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

  void add_notification_to_list(const rgw_pubsub_sub_config& sub_conf, 
      const EventTypeList& events,
      const std::string& topic_arn);  

public:
  void execute() override;
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
  const char* name() const override { return "pubsub_notifications_get_s3"; }
};

void RGWPSListNotifs_ObjStore_S3::add_notification_to_list(const rgw_pubsub_sub_config& sub_conf, 
    const EventTypeList& events, 
    const std::string& topic_arn) { 
    rgw_pubsub_s3_notification notification;
    notification.id = sub_conf.s3_id;
    notification.topic_arn = topic_arn,
    std::transform(events.begin(), events.end(), std::back_inserter(notification.events), gcp_to_s3_event);
    notifications.list.push_back(notification);
}

void RGWPSListNotifs_ObjStore_S3::execute() {
  ups = std::make_unique<RGWUserPubSub>(store, s->owner.get_id());
  auto b = ups->get_bucket(bucket_info.bucket);
  ceph_assert(b);
  if (!sub_name.empty()) {
    // get info of a specific notification
    auto sub = ups->get_sub(sub_name);
    rgw_pubsub_sub_config sub_conf;
    op_ret = sub->get_conf(&sub_conf);
    if (op_ret < 0) {
      ldout(s->cct, 1) << "failed to get notification info, ret=" << op_ret << dendl;
      return;
    }
    if (sub_conf.s3_id.empty()) {
      op_ret = -ENOENT;
      ldout(s->cct, 1) << "notification does not have an ID, ret=" << op_ret << dendl;
      return;
    }
    rgw_pubsub_bucket_topics bucket_topics;
    op_ret = b->get_topics(&bucket_topics);
    if (op_ret < 0) {
      ldout(s->cct, 1) << "failed to get notification info, ret=" << op_ret << dendl;
      return;
    }
    const auto topic_it = bucket_topics.topics.find(sub_conf.topic);
    if (topic_it == bucket_topics.topics.end()) {
      op_ret = -ENOENT;
      ldout(s->cct, 1) << "notification does not have topic information, ret=" << op_ret << dendl;
      return;
    }
    add_notification_to_list(sub_conf, topic_it->second.events, topic_it->second.topic.arn);
    return;
  }
  // get info on all s3 notifications of the bucket
  rgw_pubsub_bucket_topics bucket_topics;
  b->get_topics(&bucket_topics);
  // loop through all topics of the bucket
  for (const auto& topic : bucket_topics.topics) {
    // for each topic get all subscriptions
    rgw_pubsub_topic_subs topic_subs;
    ups->get_topic(topic.first, &topic_subs);
    const auto& events = topic.second.events;
    const auto& topic_arn = topic.second.topic.arn;
    // loop through all subscriptions
    for (const auto& topic_sub_name : topic_subs.subs) {
      // get info of a specific notification
      auto sub = ups->get_sub(topic_sub_name);
      rgw_pubsub_sub_config sub_conf;
      op_ret = sub->get_conf(&sub_conf);
      if (op_ret < 0) {
        ldout(s->cct, 1) << "failed to get notification info, ret=" << op_ret << dendl;
        return;
      }
      if (sub_conf.s3_id.empty()) {
        // not an s3 notification
        continue;
      }
      add_notification_to_list(sub_conf, events, topic_arn);
    }
  }
}

// s3 compliant notification handler factory
class RGWHandler_REST_PSNotifs_S3 : public RGWHandler_REST_S3 {
protected:
  int init_permissions(RGWOp* op) override {
    return 0;
  }

  int read_permissions(RGWOp* op) override {
    return 0;
  }
  bool supports_quota() override {
    return false;
  }
  RGWOp *op_get() override {
    return new RGWPSListNotifs_ObjStore_S3();
  }
  RGWOp *op_put() override {
    return new RGWPSCreateNotif_ObjStore_S3();
  }
  RGWOp *op_delete() override {
    return new RGWPSDeleteNotif_ObjStore_S3();
  }
public:
  explicit RGWHandler_REST_PSNotifs_S3(const rgw::auth::StrategyRegistry& auth_registry) : RGWHandler_REST_S3(auth_registry) {}
  virtual ~RGWHandler_REST_PSNotifs_S3() = default;
};

// factory for S3 compliant PubSub REST handlers 
RGWHandler_REST* RGWRESTMgr_PubSub_S3::_get_handler(req_state* s,
                                                     const rgw::auth::StrategyRegistry& auth_registry,
                                                     const std::string& frontend_prefix) {
  if (RGWHandler_REST_S3::init_from_header(s, RGW_FORMAT_XML, true) < 0) {
    return nullptr;
  }
  
  // s3 compliant PubSub API: <bucket name>?notification
  if (s->info.args.exists("notification")) {
      return new RGWHandler_REST_PSNotifs_S3(auth_registry);
  }

  return nullptr;
}

