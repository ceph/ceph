// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <algorithm>
#include "rgw_sync_module_pubsub.h"
#include "rgw_sync_module_pubsub_rest.h"
#include "rgw_pubsub.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_arn.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

class RGWPSCreateTopicOp : public RGWDefaultResponseOp {
protected:
  std::unique_ptr<RGWUserPubSub> ups;
  string topic_name;
  string bucket_name;

public:
  RGWPSCreateTopicOp() {}

  int verify_permission() override {
    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute() override;

  const char* name() const override { return "pubsub_topic_create"; }
  virtual RGWOpType get_type() override { return RGW_OP_PUBSUB_TOPIC_CREATE; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  virtual int get_params() = 0;
};

void RGWPSCreateTopicOp::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  ups = make_unique<RGWUserPubSub>(store, s->owner.get_id());
  op_ret = ups->create_topic(topic_name);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to create topic, ret=" << op_ret << dendl;
    return;
  }
}

// create a topic
// command: PUT /topics/<topic-name>
class RGWPSCreateTopic_ObjStore_S3 : public RGWPSCreateTopicOp {
public:
  explicit RGWPSCreateTopic_ObjStore_S3() {}

  int get_params() override {
    topic_name = s->object.name;
    return 0;
  }
};

class RGWPSListTopicsOp : public RGWOp {
protected:
  std::unique_ptr<RGWUserPubSub> ups;
  rgw_pubsub_user_topics result;


public:
  RGWPSListTopicsOp() {}

  int verify_permission() override {
    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute() override;

  const char* name() const override { return "pubsub_topics_list"; }
  virtual RGWOpType get_type() override { return RGW_OP_PUBSUB_TOPICS_LIST; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

void RGWPSListTopicsOp::execute()
{
  ups = make_unique<RGWUserPubSub>(store, s->owner.get_id());
  op_ret = ups->get_user_topics(&result);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to get topics, ret=" << op_ret << dendl;
    return;
  }

}

// list all topics
// command: GET /topics
class RGWPSListTopics_ObjStore_S3 : public RGWPSListTopicsOp {
public:
  explicit RGWPSListTopics_ObjStore_S3() {}

  void send_response() override {
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/json");

    if (op_ret < 0) {
      return;
    }

    encode_json("result", result, s->formatter);
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
};

class RGWPSGetTopicOp : public RGWOp {
protected:
  string topic_name;
  std::unique_ptr<RGWUserPubSub> ups;
  rgw_pubsub_topic_subs result;

public:
  RGWPSGetTopicOp() {}

  int verify_permission() override {
    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute() override;

  const char* name() const override { return "pubsub_topic_get"; }
  virtual RGWOpType get_type() override { return RGW_OP_PUBSUB_TOPIC_GET; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
  virtual int get_params() = 0;
};

void RGWPSGetTopicOp::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ups = make_unique<RGWUserPubSub>(store, s->owner.get_id());
  op_ret = ups->get_topic(topic_name, &result);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to get topic, ret=" << op_ret << dendl;
    return;
  }
}

// get topic information (including subscriptions)
// command:  GET /topics/<topic-name>
class RGWPSGetTopic_ObjStore_S3 : public RGWPSGetTopicOp {
public:
  explicit RGWPSGetTopic_ObjStore_S3() {}

  int get_params() override {
    topic_name = s->object.name;
    return 0;
  }

  void send_response() override {
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/json");

    if (op_ret < 0) {
      return;
    }

    encode_json("result", result, s->formatter);
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
};

class RGWPSDeleteTopicOp : public RGWDefaultResponseOp {
protected:
  string topic_name;
  std::unique_ptr<RGWUserPubSub> ups;

public:
  RGWPSDeleteTopicOp() {}

  int verify_permission() override {
    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute() override;

  const char* name() const override { return "pubsub_topic_delete"; }
  virtual RGWOpType get_type() override { return RGW_OP_PUBSUB_TOPIC_DELETE; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
  virtual int get_params() = 0;
};

void RGWPSDeleteTopicOp::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  ups = make_unique<RGWUserPubSub>(store, s->owner.get_id());
  op_ret = ups->remove_topic(topic_name);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to remove topic, ret=" << op_ret << dendl;
    return;
  }
}

// delete a topic
// command: DELETE /topics/<topic-name>
class RGWPSDeleteTopic_ObjStore_S3 : public RGWPSDeleteTopicOp {
public:
  explicit RGWPSDeleteTopic_ObjStore_S3() {}

  int get_params() override {
    topic_name = s->object.name;
    return 0;
  }
};

// topics handler factory
class RGWHandler_REST_PSTopic_S3 : public RGWHandler_REST_S3 {
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
    if (s->init_state.url_bucket.empty()) {
      return nullptr;
    }
    if (s->object.empty()) {
      return new RGWPSListTopics_ObjStore_S3();
    }
    return new RGWPSGetTopic_ObjStore_S3();
  }
  RGWOp *op_put() override {
    if (!s->object.empty()) {
      return new RGWPSCreateTopic_ObjStore_S3();
    }
    return nullptr;
  }
  RGWOp *op_delete() override {
    if (!s->object.empty()) {
      return new RGWPSDeleteTopic_ObjStore_S3();
    }
    return nullptr;
  }
public:
  explicit RGWHandler_REST_PSTopic_S3(const rgw::auth::StrategyRegistry& auth_registry) : RGWHandler_REST_S3(auth_registry) {}
  virtual ~RGWHandler_REST_PSTopic_S3() {}
};

class RGWPSCreateSubOp : public RGWDefaultResponseOp {
protected:
  string sub_name;
  string topic_name;
  std::unique_ptr<RGWUserPubSub> ups;
  rgw_pubsub_sub_dest dest;

public:
  RGWPSCreateSubOp() {}

  int verify_permission() override {
    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute() override;

  const char* name() const override { return "pubsub_subscription_create"; }
  virtual RGWOpType get_type() override { return RGW_OP_PUBSUB_SUB_CREATE; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  virtual int get_params() = 0;
};

void RGWPSCreateSubOp::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ups = make_unique<RGWUserPubSub>(store, s->owner.get_id());
  auto sub = ups->get_sub(sub_name);
  op_ret = sub->subscribe(topic_name, dest);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to create subscription, ret=" << op_ret << dendl;
    return;
  }
}

// create a subscription
// command: PUT /subscriptions/<sub-name>?topic=<topic-name>[&push-endpoint=<endpoint>[&<arg1>=<value1>]...
class RGWPSCreateSub_ObjStore_S3 : public RGWPSCreateSubOp {
public:
  explicit RGWPSCreateSub_ObjStore_S3() {}

  int get_params() override {
    sub_name = s->object.name;

    bool exists;

    topic_name = s->info.args.get("topic", &exists);
    if (!exists) {
      ldout(s->cct, 1) << "missing required param 'topic' for request" << dendl;
      return -EINVAL;
    }

    auto psmodule = static_cast<RGWPSSyncModuleInstance *>(store->get_sync_module().get());
    auto conf = psmodule->get_effective_conf();

    dest.push_endpoint = s->info.args.get("push-endpoint");
    dest.bucket_name = string(conf["data_bucket_prefix"]) + s->owner.get_id().to_str() + "-" + topic_name;
    dest.oid_prefix = string(conf["data_oid_prefix"]) + sub_name + "/";
    dest.push_endpoint_args = s->info.args.get_str();

    return 0;
  }
};

class RGWPSGetSubOp : public RGWOp {
protected:
  string sub_name;
  std::unique_ptr<RGWUserPubSub> ups;
  rgw_pubsub_sub_config result;

public:
  RGWPSGetSubOp() {}

  int verify_permission() override {
    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute() override;

  const char* name() const override { return "pubsub_subscription_get"; }
  virtual RGWOpType get_type() override { return RGW_OP_PUBSUB_SUB_GET; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
  virtual int get_params() = 0;
};

void RGWPSGetSubOp::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ups = make_unique<RGWUserPubSub>(store, s->owner.get_id());
  auto sub = ups->get_sub(sub_name);
  op_ret = sub->get_conf(&result);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to get subscription, ret=" << op_ret << dendl;
    return;
  }
}

// get subscription configuration
// command: GET /subscriptions/<sub-name>
class RGWPSGetSub_ObjStore_S3 : public RGWPSGetSubOp {
public:
  explicit RGWPSGetSub_ObjStore_S3() {}

  int get_params() override {
    sub_name = s->object.name;
    return 0;
  }

  void send_response() override {
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/json");

    if (op_ret < 0) {
      return;
    }

    {
      Formatter::ObjectSection section(*s->formatter, "result");
      encode_json("topic", result.topic, s->formatter);
      encode_json("push_endpoint", result.dest.push_endpoint, s->formatter);
      encode_json("args", result.dest.push_endpoint_args, s->formatter);
    }
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
};

class RGWPSDeleteSubOp : public RGWDefaultResponseOp {
protected:
  string sub_name;
  string topic_name;
  std::unique_ptr<RGWUserPubSub> ups;

public:
  RGWPSDeleteSubOp() {}

  int verify_permission() override {
    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute() override;

  const char* name() const override { return "pubsub_subscription_delete"; }
  virtual RGWOpType get_type() override { return RGW_OP_PUBSUB_SUB_DELETE; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
  virtual int get_params() = 0;
};

void RGWPSDeleteSubOp::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ups = make_unique<RGWUserPubSub>(store, s->owner.get_id());
  auto sub = ups->get_sub(sub_name);
  op_ret = sub->unsubscribe(topic_name);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to remove subscription, ret=" << op_ret << dendl;
    return;
  }
}

// delete subscription
// Command: DELETE /subscriptions/<sub-name>
class RGWPSDeleteSub_ObjStore_S3 : public RGWPSDeleteSubOp {
public:
  explicit RGWPSDeleteSub_ObjStore_S3() {}

  int get_params() override {
    sub_name = s->object.name;
    topic_name = s->info.args.get("topic");
    return 0;
  }
};

class RGWPSAckSubEventOp : public RGWDefaultResponseOp {
protected:
  std::string sub_name;
  std::string event_id;
  std::unique_ptr<RGWUserPubSub> ups;

public:
  RGWPSAckSubEventOp() {}

  int verify_permission() override {
    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute() override;

  const char* name() const override { return "pubsub_subscription_ack"; }
  virtual RGWOpType get_type() override { return RGW_OP_PUBSUB_SUB_ACK; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
  virtual int get_params() = 0;
};

void RGWPSAckSubEventOp::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ups = make_unique<RGWUserPubSub>(store, s->owner.get_id());
  auto sub = ups->get_sub_with_events(sub_name);
  op_ret = sub->remove_event(event_id);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to ack event, ret=" << op_ret << dendl;
    return;
  }
}

// acking of an event
// command POST /subscriptions/<sub-name>?ack&event-id=<event-id>
class RGWPSAckSubEvent_ObjStore_S3 : public RGWPSAckSubEventOp {
public:
  explicit RGWPSAckSubEvent_ObjStore_S3() {}

  int get_params() override {
    sub_name = s->object.name;

    bool exists;

    event_id = s->info.args.get("event-id", &exists);
    if (!exists) {
      ldout(s->cct, 1) << "missing required param 'event-id' for request" << dendl;
      return -EINVAL;
    }
    return 0;
  }
};

class RGWPSPullSubEventsOp : public RGWOp {
protected:
  int max_entries{0};
  std::string sub_name;
  std::string marker;
  std::unique_ptr<RGWUserPubSub> ups;
  RGWUserPubSub::SubRef sub; 
  //RGWUserPubSub::SubWithEvents<rgw_pubsub_event>::list_events_result result;

public:
  RGWPSPullSubEventsOp() {}

  int verify_permission() override {
    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute() override;

  const char* name() const override { return "pubsub_subscription_pull"; }
  virtual RGWOpType get_type() override { return RGW_OP_PUBSUB_SUB_PULL; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
  virtual int get_params() = 0;
};

void RGWPSPullSubEventsOp::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ups = make_unique<RGWUserPubSub>(store, s->owner.get_id());
  sub = ups->get_sub_with_events(sub_name);
  if (!sub) {
    op_ret = -ENOENT;
    ldout(s->cct, 1) << "failed to get subscription, ret=" << op_ret << dendl;
    return;
  }
  op_ret = sub->list_events(marker, max_entries);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to get subscription events, ret=" << op_ret << dendl;
    return;
  }
}

// fetching events from a subscription
// command: GET /subscriptions/<sub-name>?events[&max-entries=<max-entries>][&marker=<marker>]
// dpending on whether the subscription was created via s3 compliant API or not
// the matching events will be returned
class RGWPSPullSubEvents_ObjStore_S3 : public RGWPSPullSubEventsOp {
public:
  explicit RGWPSPullSubEvents_ObjStore_S3() {}

  int get_params() override {
    sub_name = s->object.name;
    marker = s->info.args.get("marker");
    const int ret = s->info.args.get_int("max-entries", &max_entries, 
        RGWUserPubSub::Sub::DEFAULT_MAX_EVENTS);
    if (ret < 0) {
      ldout(s->cct, 1) << "failed to parse 'max-entries' param" << dendl;
      return -EINVAL;
    }
    return 0;
  }

  void send_response() override {
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/json");

    if (op_ret < 0) {
      return;
    }

    encode_json("result", *sub, s->formatter);
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
};

class RGWHandler_REST_PSSub_S3 : public RGWHandler_REST_S3 {
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
    if (s->object.empty()) {
      return nullptr;
    }
    if (s->info.args.exists("events")) {
      return new RGWPSPullSubEvents_ObjStore_S3();
    }
    return new RGWPSGetSub_ObjStore_S3();
  }
  RGWOp *op_put() override {
    if (!s->object.empty()) {
      return new RGWPSCreateSub_ObjStore_S3();
    }
    return nullptr;
  }
  RGWOp *op_delete() override {
    if (!s->object.empty()) {
      return new RGWPSDeleteSub_ObjStore_S3();
    }
    return nullptr;
  }
  RGWOp *op_post() override {
    if (s->info.args.exists("ack")) {
      return new RGWPSAckSubEvent_ObjStore_S3();
    }
    return nullptr;
  }
public:
  explicit RGWHandler_REST_PSSub_S3(const rgw::auth::StrategyRegistry& auth_registry) : RGWHandler_REST_S3(auth_registry) {}
  virtual ~RGWHandler_REST_PSSub_S3() {}
};


static int notif_bucket_path(const string& path, string *bucket_name)
{
  if (path.empty()) {
    return -EINVAL;
  }
  size_t pos = path.find('/');
  if (pos  == string::npos) {
    return -EINVAL;
  }
  if (pos >= path.size()) {
    return -EINVAL;
  }

  string type = path.substr(0, pos);
  if (type != "bucket") {
    return -EINVAL;
  }

  *bucket_name = path.substr(pos + 1);
  return 0;
}

class RGWPSCreateNotifOp : public RGWDefaultResponseOp {
protected:
  std::unique_ptr<RGWUserPubSub> ups;
  string bucket_name;
  RGWBucketInfo bucket_info;

  virtual int get_params() = 0;

public:
  RGWPSCreateNotifOp() = default;

  int verify_permission() override {
    int ret = get_params();
    if (ret < 0) {
      return ret;
    }

    const auto& id = s->owner.get_id();

    ret = store->get_bucket_info(*s->sysobj_ctx, id.tenant, bucket_name,
                                 bucket_info, nullptr, nullptr);
    if (ret < 0) {
      return ret;
    }

    if (bucket_info.owner != id) {
      ldout(s->cct, 1) << "user doesn't own bucket, cannot create notification" << dendl;
      return -EPERM;
    }
    return 0;
  }

  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }

  RGWOpType get_type() override { return RGW_OP_PUBSUB_NOTIF_CREATE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};


// ceph specific notification creation
// command: PUT /notification/bucket/<bucket name>?topic=<topic name>
// ("topic" has to be created beforehand)
class RGWPSCreateNotif_ObjStore_Ceph : public RGWPSCreateNotifOp {
private:
  string topic_name;
  std::set<string, ltstr_nocase> events;

  int get_params() override {
    bool exists;
    topic_name = s->info.args.get("topic", &exists);
    if (!exists) {
      ldout(s->cct, 1) << "param 'topic' not provided" << dendl;
      return -EINVAL;
    }

    string events_str = s->info.args.get("events", &exists);
    if (exists) {
      get_str_set(events_str, ",", events);
    }
    return notif_bucket_path(s->object.name, &bucket_name);
  }

public:
  RGWPSCreateNotif_ObjStore_Ceph() = default;

  const char* name() const override { return "pubsub_notification_create_gcp"; }

  void execute() override;

};

void RGWPSCreateNotif_ObjStore_Ceph::execute()
{
  ups = make_unique<RGWUserPubSub>(store, s->owner.get_id());

  auto b = ups->get_bucket(bucket_info.bucket);
  op_ret = b->create_notification(topic_name, events);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to create notification, ret=" << op_ret << dendl;
    return;
  }
}

// s3 compliant notification creation
// command: PUT /<bucket name>?notification
// a "topic", a "notification" and a subscription will be auto-generated
// actual configuration is XML encoded in the body of the message, with following schema example:
// <NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
//   <TopicConfiguration>
//     <Filter>
//       <S3Key>
//         <FilterRule>
//           <Name>suffix</Name>
//           <Value>jpg</Value>
//         </FilterRule>
//       </S3Key>
//     </Filter>
//     <Id>notification1</Id>
//     <Topic>arn:aws:sns:::<endpoint-type>:<endpoint-name>:topic1</Topic>
//     <Event>s3:ObjectCreated:*</Event>
//     <Event>s3:ObjectRemoved:*</Event>
//   </TopicConfiguration>
// </NotificationConfiguration>
class RGWPSCreateNotif_ObjStore_S3 : public RGWPSCreateNotifOp {

  struct TopicConfiguration {
    std::string id;
    std::string endpoint_type;
    std::string endpoint_id;
    std::string topic;
    std::list<std::string> events;

    bool decode_xml(XMLObj *obj) {
      const auto throw_if_missing = true;
      RGWXMLDecoder::decode_xml("Id", id, obj, throw_if_missing);
      
      std::string str_arn;
      RGWXMLDecoder::decode_xml("Topic", str_arn, obj, throw_if_missing);

      // parse ARN. allow wildcards 
      const auto arn = rgw::ARN::parse(str_arn, true);
      if (arn == boost::none || arn->resource.empty()) {
        throw RGWXMLDecoder::err("topic ARN parsing failed. ARN = '" + str_arn + "'");
      }

      // partition and service are expected to be "aws" and "sns"
      // but there is no need to validate ARN them
      
      const auto arn_resource = rgw::ARNResource::parse(arn->resource);
      if (arn_resource == boost::none || arn_resource->resource.empty()) {
        throw RGWXMLDecoder::err("topic ARN resource parsing failed. ARNResource = '" + arn->resource + "'");
      }

      if (!arn_resource->resource_type.empty()) {
        // endpoint exists
        endpoint_type = arn_resource->resource_type;
        endpoint_id = arn_resource->resource;
        if (arn_resource->qualifier.empty()) {
          throw RGWXMLDecoder::err("topic ARN resource parsing failed. missing qualifier for endpoint");
        }
        topic = arn_resource->qualifier;
      } else {
        // only topic
        topic = arn_resource->resource;
      }

      do_decode_xml_obj(events, "Event", obj);
      if (events.empty()) {
        // if no events are provided, we assume all events
        events.push_back("s3:ObjectCreated:*");
        events.push_back("s3:ObjectRemoved:*");
      }
      return true;
    }
  };

  struct NotificationConfiguration {
    std::list<TopicConfiguration> list;
    bool decode_xml(XMLObj *obj) {
      do_decode_xml_obj(list, "TopicConfiguration", obj);
      if (list.empty()) {
        throw RGWXMLDecoder::err("at least one 'TopicConfiguration' must exist");
      }
      return true;
    }
  } configurations;

  static std::string s3_to_gcp_event(const std::string& event) {
    if (event == "s3:ObjectCreated:*") {
      return "OBJECT_CREATE";
    }
    if (event == "s3:ObjectRemoved:*") {
      return "OBJECT_DELETE";
    }
    return "";
  }

  int get_params_from_body() {
    const auto max_size = s->cct->_conf->rgw_max_put_param_size;
    int r;
    bufferlist data;
    std::tie(r, data) = rgw_rest_read_all_input(s, max_size, false);

    if (r < 0) {
      ldout(s->cct, 1) << "failed to read notification parameters from payload" << dendl;
      return r;
    }
    if (data.length() == 0) {
      ldout(s->cct, 1) << "payload missing for notification" << dendl;
      return -EINVAL;
    }

    RGWXMLDecoder::XMLParser parser;

    if (!parser.init()){
      ldout(s->cct, 1) << "failed to initialize XML parser" << dendl;
      return -EINVAL;
    }
    if (!parser.parse(data.c_str(), data.length(), 1)) {
      ldout(s->cct, 1) << "failed to parse XML payload of notification" << dendl;
      return -ERR_MALFORMED_XML;
    }
    try {
      // TopicConfigurations is mandatory
      RGWXMLDecoder::decode_xml("NotificationConfiguration", configurations, &parser, true);
    } catch (RGWXMLDecoder::err& err) {
      ldout(s->cct, 1) << "failed to parse XML payload of notification. error: " << err << dendl;
      return -ERR_MALFORMED_XML;
    }
    return 0;
  }

  int get_params() override {
    bool exists;
    const auto no_value = s->info.args.get("notification", &exists);
    if (!exists) {
      ldout(s->cct, 20) << "param 'notification' not provided" << dendl;
      return -EINVAL;
    } 
    if (no_value.length() > 0) {
      ldout(s->cct, 1) << "param 'notification' should not be set with value" << dendl;
      return -EINVAL;
    }
    if (s->bucket_name.empty()) {
      ldout(s->cct, 1) << "notification must be set on a bucket" << dendl;
      return -EINVAL;
    }
    bucket_name = s->bucket_name;
    return 0;
  }

public:
  RGWPSCreateNotif_ObjStore_S3() = default;

  const char* name() const override { return "pubsub_notification_create_s3"; }

  void execute() override;
};

void RGWPSCreateNotif_ObjStore_S3::execute()
{
  op_ret = get_params_from_body();
  if (op_ret < 0) {
    return;
  }

  ups = make_unique<RGWUserPubSub>(store, s->owner.get_id());
  auto b = ups->get_bucket(bucket_info.bucket);
  const auto psmodule = static_cast<RGWPSSyncModuleInstance*>(store->get_sync_module().get());
  const auto& conf = psmodule->get_effective_conf();

  for (const auto& c : configurations.list) {
    const auto& topic_name = c.topic;
    const auto& sub_name = c.id;
    if (topic_name.empty()) {
      ldout(s->cct, 1) << "missing topic information" << dendl;
      op_ret = -EINVAL;
      return;
    }
    if (sub_name.empty()) {
      ldout(s->cct, 1) << "missing subscription information" << dendl;
      op_ret = -EINVAL;
      return;
    }
    // get endpoint configuration according to type
    // no endpoint (pull mode): arn:s3:sns:::<topic>
    // TODO: HTTP/S endpoint:         arn:s3:sns:::webhook:<endpoint-name>:<topic>
    // TODO: AMQP endpoint:           arn:s3:sns:::amqp:<endpoint-name>:<topic>
    if (!c.endpoint_type.empty()) {
      ldout(s->cct, 1) << "endpoint type '" << c.endpoint_type << 
        "' not supported" << dendl;
      op_ret = -EINVAL;
      return;
    }
    // generate the topic
    op_ret = ups->create_topic(topic_name);
    if (op_ret < 0) {
      ldout(s->cct, 1) << "failed to auto-generate topic '" << topic_name << 
        "' for notification, ret=" << op_ret << dendl;
      return;
    }
    // generate the notification
    std::set<std::string, ltstr_nocase> events;
    std::transform(c.events.begin(), c.events.end(), std::inserter(events, events.begin()), s3_to_gcp_event);
    ceph_assert(b);
    op_ret = b->create_notification(topic_name, events);
    if (op_ret < 0) {
      ldout(s->cct, 1) << "failed to auto-generate notification on topic '" << topic_name <<
        "', ret=" << op_ret << dendl;
      // rollback generated topic
      ups->remove_topic(topic_name);
      return;
    }
    
    rgw_pubsub_sub_dest dest;
    dest.bucket_name = string(conf["data_bucket_prefix"]) + s->owner.get_id().to_str() + "-" + topic_name;
    dest.oid_prefix = string(conf["data_oid_prefix"]) + sub_name + "/";
    auto sub = ups->get_sub(sub_name);
    op_ret = sub->subscribe(topic_name, dest, c.id);
    if (op_ret < 0) {
      ldout(s->cct, 1) << "failed to auto-generate subscription '" << sub_name << "', ret=" << op_ret << dendl;
      // rollback generated topic
      ups->remove_topic(topic_name);
      return;
    }
  }
}

class RGWPSDeleteNotifOp : public RGWDefaultResponseOp {
protected:
  std::unique_ptr<RGWUserPubSub> ups;
  string topic_name;
  string bucket_name;
  RGWBucketInfo bucket_info;

public:
  RGWPSDeleteNotifOp() {}

  int verify_permission() override {
    int ret = get_params();
    if (ret < 0) {
      return ret;
    }

    ret = store->get_bucket_info(*s->sysobj_ctx, s->owner.get_id().tenant, bucket_name,
                                 bucket_info, nullptr, nullptr);
    if (ret < 0) {
      return ret;
    }

    if (bucket_info.owner != s->owner.get_id()) {
      ldout(s->cct, 1) << "user doesn't own bucket, cannot create topic" << dendl;
      return -EPERM;
    }
    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute() override;

  const char* name() const override { return "pubsub_notification_delete"; }
  virtual RGWOpType get_type() override { return RGW_OP_PUBSUB_NOTIF_DELETE; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
  virtual int get_params() = 0;
};

void RGWPSDeleteNotifOp::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  ups = make_unique<RGWUserPubSub>(store, s->owner.get_id());
  auto b = ups->get_bucket(bucket_info.bucket);
  op_ret = b->remove_notification(topic_name);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to remove notification, ret=" << op_ret << dendl;
    return;
  }
}

class RGWPSDeleteNotif_ObjStore_Ceph : public RGWPSDeleteNotifOp {
public:
  explicit RGWPSDeleteNotif_ObjStore_Ceph() {}

  int get_params() override {
    bool exists;
    topic_name = s->info.args.get("topic", &exists);
    if (!exists) {
      ldout(s->cct, 1) << "param 'topic' not provided" << dendl;
      return -EINVAL;
    }
    return notif_bucket_path(s->object.name, &bucket_name);
  }
};

class RGWPSListNotifsOp : public RGWOp {
protected:
  std::string bucket_name;
  RGWBucketInfo bucket_info;
  std::unique_ptr<RGWUserPubSub> ups;
  rgw_pubsub_bucket_topics result;


public:
  RGWPSListNotifsOp() {}

  int verify_permission() override {
    int ret = get_params();
    if (ret < 0) {
      return ret;
    }

    ret = store->get_bucket_info(*s->sysobj_ctx, s->owner.get_id().tenant, bucket_name,
                                 bucket_info, nullptr, nullptr);
    if (ret < 0) {
      return ret;
    }

    if (bucket_info.owner != s->owner.get_id()) {
      ldout(s->cct, 1) << "user doesn't own bucket, cannot create topic" << dendl;
      return -EPERM;
    }

    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute() override;

  const char* name() const override { return "pubsub_notifications_list"; }
  virtual RGWOpType get_type() override { return RGW_OP_PUBSUB_NOTIF_LIST; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
  virtual int get_params() = 0;
};

void RGWPSListNotifsOp::execute()
{
  ups = make_unique<RGWUserPubSub>(store, s->owner.get_id());
  auto b = ups->get_bucket(bucket_info.bucket);
  op_ret = b->get_topics(&result);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to get topics, ret=" << op_ret << dendl;
    return;
  }

}

class RGWPSListNotifs_ObjStore_Ceph : public RGWPSListNotifsOp {
public:
  explicit RGWPSListNotifs_ObjStore_Ceph() {}

  int get_params() override {
    return notif_bucket_path(s->object.name, &bucket_name);
  }

  void send_response() override {
    if (op_ret) {
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/json");

    if (op_ret < 0) {
      return;
    }

    encode_json("result", result, s->formatter);
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
};

// ceph specific notification handler factory
class RGWHandler_REST_PSNotifs_Ceph : public RGWHandler_REST_S3 {
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
    if (s->object.empty()) {
      return nullptr;
    }
    return new RGWPSListNotifs_ObjStore_Ceph();
  }
  RGWOp *op_put() override {
    if (!s->object.empty()) {
      return new RGWPSCreateNotif_ObjStore_Ceph();
    }
    return nullptr;
  }
  RGWOp *op_delete() override {
    if (!s->object.empty()) {
      return new RGWPSDeleteNotif_ObjStore_Ceph();
    }
    return nullptr;
  }
public:
  explicit RGWHandler_REST_PSNotifs_Ceph(const rgw::auth::StrategyRegistry& auth_registry) : RGWHandler_REST_S3(auth_registry) {}
  virtual ~RGWHandler_REST_PSNotifs_Ceph() {}
};

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
    return nullptr;
  }
  RGWOp *op_put() override {
    return new RGWPSCreateNotif_ObjStore_S3();
  }
  RGWOp *op_delete() override {
    return nullptr;
  }
public:
  explicit RGWHandler_REST_PSNotifs_S3(const rgw::auth::StrategyRegistry& auth_registry) : RGWHandler_REST_S3(auth_registry) {}
  virtual ~RGWHandler_REST_PSNotifs_S3() {}
};

// factory for PubSub REST handlers 
RGWHandler_REST* RGWRESTMgr_PubSub_S3::get_handler(struct req_state* const s,
                                                     const rgw::auth::StrategyRegistry& auth_registry,
                                                     const std::string& frontend_prefix)
{
  int ret =
    RGWHandler_REST_S3::init_from_header(s,
					RGW_FORMAT_JSON, true);
  if (ret < 0) {
    return nullptr;
  }

  RGWHandler_REST *handler = nullptr;

  // ceph specific PubSub API: topics/subscriptions/notification are reserved bucket names
  if (s->init_state.url_bucket == "topics") {
    handler = new RGWHandler_REST_PSTopic_S3(auth_registry);
  } else if (s->init_state.url_bucket == "subscriptions") {
    handler = new RGWHandler_REST_PSSub_S3(auth_registry);
  } else if (s->init_state.url_bucket == "notifications") {
    handler = new RGWHandler_REST_PSNotifs_Ceph(auth_registry);
  } else {
    // s3 compliant PubSub API: uses: <bucket name>?notification
    handler = new RGWHandler_REST_PSNotifs_S3(auth_registry);
  }

  ldout(s->cct, 20) << __func__ << " handler=" << (handler ? typeid(*handler).name() : "<null>") << dendl;
  return handler;
}

