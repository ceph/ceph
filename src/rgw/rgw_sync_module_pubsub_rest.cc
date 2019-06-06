// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <algorithm>
#include "rgw_sync_module_pubsub.h"
#include "rgw_pubsub_push.h"
#include "rgw_sync_module_pubsub_rest.h"
#include "rgw_pubsub.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_arn.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

// create a topic
class RGWPSCreateTopicOp : public RGWDefaultResponseOp {
protected:
  std::unique_ptr<RGWUserPubSub> ups;
  std::string topic_name;
  rgw_pubsub_sub_dest dest;
  std::string topic_arn;
  
  virtual int get_params() = 0;

public:
  int verify_permission() override {
    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute() override;

  const char* name() const override { return "pubsub_topic_create"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_TOPIC_CREATE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

void RGWPSCreateTopicOp::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  ups = std::make_unique<RGWUserPubSub>(store, s->owner.get_id());
  op_ret = ups->create_topic(topic_name, dest, topic_arn);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to create topic '" << topic_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 20) << "successfully created topic '" << topic_name << "'" << dendl;
}

// command: PUT /topics/<topic-name>[&push-endpoint=<endpoint>[&<arg1>=<value1>]]
class RGWPSCreateTopic_ObjStore_S3 : public RGWPSCreateTopicOp {
public:
  int get_params() override {
    
    topic_name = s->object.name;

    dest.push_endpoint = s->info.args.get("push-endpoint");
    dest.push_endpoint_args = s->info.args.get_str();
    // dest object only stores endpoint info
    // bucket to store events/records will be set only when subscription is created
    dest.bucket_name = "";
    dest.oid_prefix = "";
    dest.arn_topic = topic_name;
    // the topic ARN will be sent in the reply
    const rgw::ARN arn(rgw::Partition::aws, rgw::Service::sns, 
        store->svc()->zone->get_zonegroup().get_name(),
        s->user->user_id.tenant, topic_name);
    topic_arn = arn.to_string();
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
      encode_json("arn", topic_arn, s->formatter);
    }
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
};

// list all topics
class RGWPSListTopicsOp : public RGWOp {
protected:
  std::unique_ptr<RGWUserPubSub> ups;
  rgw_pubsub_user_topics result;

public:
  int verify_permission() override {
    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute() override;

  const char* name() const override { return "pubsub_topics_list"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_TOPICS_LIST; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

void RGWPSListTopicsOp::execute()
{
  ups = std::make_unique<RGWUserPubSub>(store, s->owner.get_id());
  op_ret = ups->get_user_topics(&result);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to get topics, ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 20) << "successfully got topics" << dendl;
}

// command: GET /topics
class RGWPSListTopics_ObjStore_S3 : public RGWPSListTopicsOp {
public:
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

// get topic information
class RGWPSGetTopicOp : public RGWOp {
protected:
  std::string topic_name;
  std::unique_ptr<RGWUserPubSub> ups;
  rgw_pubsub_topic_subs result;
  
  virtual int get_params() = 0;

public:
  int verify_permission() override {
    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute() override;

  const char* name() const override { return "pubsub_topic_get"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_TOPIC_GET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

void RGWPSGetTopicOp::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ups = std::make_unique<RGWUserPubSub>(store, s->owner.get_id());
  op_ret = ups->get_topic(topic_name, &result);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to get topic '" << topic_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 1) << "successfully got topic '" << topic_name << "'" << dendl;
}

// command: GET /topics/<topic-name>
class RGWPSGetTopic_ObjStore_S3 : public RGWPSGetTopicOp {
public:
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

// delete a topic
class RGWPSDeleteTopicOp : public RGWDefaultResponseOp {
protected:
  string topic_name;
  std::unique_ptr<RGWUserPubSub> ups;
  
  virtual int get_params() = 0;

public:
  int verify_permission() override {
    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute() override;

  const char* name() const override { return "pubsub_topic_delete"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_TOPIC_DELETE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
};

void RGWPSDeleteTopicOp::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  ups = std::make_unique<RGWUserPubSub>(store, s->owner.get_id());
  op_ret = ups->remove_topic(topic_name);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to remove topic '" << topic_name << ", ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 1) << "successfully removed topic '" << topic_name << "'" << dendl;
}

// command: DELETE /topics/<topic-name>
class RGWPSDeleteTopic_ObjStore_S3 : public RGWPSDeleteTopicOp {
public:
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
  virtual ~RGWHandler_REST_PSTopic_S3() = default;
};

// create a subscription
class RGWPSCreateSubOp : public RGWDefaultResponseOp {
protected:
  std::string sub_name;
  std::string topic_name;
  std::unique_ptr<RGWUserPubSub> ups;
  rgw_pubsub_sub_dest dest;
  
  virtual int get_params() = 0;

public:
  int verify_permission() override {
    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute() override;

  const char* name() const override { return "pubsub_subscription_create"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_SUB_CREATE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

void RGWPSCreateSubOp::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ups = std::make_unique<RGWUserPubSub>(store, s->owner.get_id());
  auto sub = ups->get_sub(sub_name);
  op_ret = sub->subscribe(topic_name, dest);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to create subscription '" << sub_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 20) << "successfully created subscription '" << sub_name << "'" << dendl;
}

// command: PUT /subscriptions/<sub-name>?topic=<topic-name>[&push-endpoint=<endpoint>[&<arg1>=<value1>]]...
class RGWPSCreateSub_ObjStore_S3 : public RGWPSCreateSubOp {
public:
  int get_params() override {
    sub_name = s->object.name;

    bool exists;
    topic_name = s->info.args.get("topic", &exists);
    if (!exists) {
      ldout(s->cct, 1) << "missing required param 'topic'" << dendl;
      return -EINVAL;
    }

    const auto psmodule = static_cast<RGWPSSyncModuleInstance*>(store->getRados()->get_sync_module().get());
    const auto& conf = psmodule->get_effective_conf();

    dest.push_endpoint = s->info.args.get("push-endpoint");
    dest.bucket_name = string(conf["data_bucket_prefix"]) + s->owner.get_id().to_str() + "-" + topic_name;
    dest.oid_prefix = string(conf["data_oid_prefix"]) + sub_name + "/";
    dest.push_endpoint_args = s->info.args.get_str();
    dest.arn_topic = topic_name;

    return 0;
  }
};

// get subscription information (including push-endpoint if exist)
class RGWPSGetSubOp : public RGWOp {
protected:
  std::string sub_name;
  std::unique_ptr<RGWUserPubSub> ups;
  rgw_pubsub_sub_config result;
  
  virtual int get_params() = 0;

public:
  int verify_permission() override {
    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute() override;

  const char* name() const override { return "pubsub_subscription_get"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_SUB_GET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

void RGWPSGetSubOp::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ups = std::make_unique<RGWUserPubSub>(store, s->owner.get_id());
  auto sub = ups->get_sub(sub_name);
  op_ret = sub->get_conf(&result);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to get subscription '" << sub_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 20) << "successfully got subscription '" << sub_name << "'" << dendl;
}

// command: GET /subscriptions/<sub-name>
class RGWPSGetSub_ObjStore_S3 : public RGWPSGetSubOp {
public:
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

    encode_json("result", result, s->formatter);
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
};

// delete subscription
class RGWPSDeleteSubOp : public RGWDefaultResponseOp {
protected:
  std::string sub_name;
  std::string topic_name;
  std::unique_ptr<RGWUserPubSub> ups;
  
  virtual int get_params() = 0;

public:
  int verify_permission() override {
    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute() override;

  const char* name() const override { return "pubsub_subscription_delete"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_SUB_DELETE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
};

void RGWPSDeleteSubOp::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ups = std::make_unique<RGWUserPubSub>(store, s->owner.get_id());
  auto sub = ups->get_sub(sub_name);
  op_ret = sub->unsubscribe(topic_name);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to remove subscription '" << sub_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 20) << "successfully removed subscription '" << sub_name << "'" << dendl;
}

// command: DELETE /subscriptions/<sub-name>
class RGWPSDeleteSub_ObjStore_S3 : public RGWPSDeleteSubOp {
public:
  int get_params() override {
    sub_name = s->object.name;
    topic_name = s->info.args.get("topic");
    return 0;
  }
};

// acking of an event
class RGWPSAckSubEventOp : public RGWDefaultResponseOp {
protected:
  std::string sub_name;
  std::string event_id;
  std::unique_ptr<RGWUserPubSub> ups;
  
  virtual int get_params() = 0;

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
  RGWOpType get_type() override { return RGW_OP_PUBSUB_SUB_ACK; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

void RGWPSAckSubEventOp::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ups = std::make_unique<RGWUserPubSub>(store, s->owner.get_id());
  auto sub = ups->get_sub_with_events(sub_name);
  op_ret = sub->remove_event(event_id);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to ack event on subscription '" << sub_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 20) << "successfully acked event on subscription '" << sub_name << "'" << dendl;
}

// command: POST /subscriptions/<sub-name>?ack&event-id=<event-id>
class RGWPSAckSubEvent_ObjStore_S3 : public RGWPSAckSubEventOp {
public:
  explicit RGWPSAckSubEvent_ObjStore_S3() {}

  int get_params() override {
    sub_name = s->object.name;

    bool exists;

    event_id = s->info.args.get("event-id", &exists);
    if (!exists) {
      ldout(s->cct, 1) << "missing required param 'event-id'" << dendl;
      return -EINVAL;
    }
    return 0;
  }
};

// fetching events from a subscription
// dpending on whether the subscription was created via s3 compliant API or not
// the matching events will be returned
class RGWPSPullSubEventsOp : public RGWOp {
protected:
  int max_entries{0};
  std::string sub_name;
  std::string marker;
  std::unique_ptr<RGWUserPubSub> ups;
  RGWUserPubSub::SubRef sub; 

  virtual int get_params() = 0;

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
  RGWOpType get_type() override { return RGW_OP_PUBSUB_SUB_PULL; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

void RGWPSPullSubEventsOp::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ups = std::make_unique<RGWUserPubSub>(store, s->owner.get_id());
  sub = ups->get_sub_with_events(sub_name);
  if (!sub) {
    op_ret = -ENOENT;
    ldout(s->cct, 1) << "failed to get subscription '" << sub_name << "' for events, ret=" << op_ret << dendl;
    return;
  }
  op_ret = sub->list_events(marker, max_entries);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to get events from subscription '" << sub_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 20) << "successfully got events from subscription '" << sub_name << "'" << dendl;
}

// command: GET /subscriptions/<sub-name>?events[&max-entries=<max-entries>][&marker=<marker>]
class RGWPSPullSubEvents_ObjStore_S3 : public RGWPSPullSubEventsOp {
public:
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

// subscriptions handler factory
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
  virtual ~RGWHandler_REST_PSSub_S3() = default;
};

namespace {
// extract bucket name from ceph specific notification command, with the format:
// /notifications/<bucket-name>
int notif_bucket_path(const string& path, std::string& bucket_name) {
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

  bucket_name = path.substr(pos + 1);
  return 0;
}
}

// notification creation
class RGWPSCreateNotifOp : public RGWDefaultResponseOp {
protected:
  std::unique_ptr<RGWUserPubSub> ups;
  string bucket_name;
  RGWBucketInfo bucket_info;

  virtual int get_params() = 0;

public:
  int verify_permission() override {
    int ret = get_params();
    if (ret < 0) {
      return ret;
    }

    const auto& id = s->owner.get_id();

    ret = store->getRados()->get_bucket_info(*s->sysobj_ctx, id.tenant, bucket_name,
                                 bucket_info, nullptr, null_yield, nullptr);
    if (ret < 0) {
      ldout(s->cct, 1) << "failed to get bucket info, cannot verify ownership" << dendl;
      return ret;
    }

    if (bucket_info.owner != id) {
      ldout(s->cct, 1) << "user doesn't own bucket, not allowed to create notification" << dendl;
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


// command (ceph specific): PUT /notification/bucket/<bucket name>?topic=<topic name>
class RGWPSCreateNotif_ObjStore_Ceph : public RGWPSCreateNotifOp {
private:
  std::string topic_name;
  std::set<std::string, ltstr_nocase> events;

  int get_params() override {
    bool exists;
    topic_name = s->info.args.get("topic", &exists);
    if (!exists) {
      ldout(s->cct, 1) << "missing required param 'topic'" << dendl;
      return -EINVAL;
    }

    string events_str = s->info.args.get("events", &exists);
    if (exists) {
      get_str_set(events_str, ",", events);
    }
    return notif_bucket_path(s->object.name, bucket_name);
  }

public:
  const char* name() const override { return "pubsub_notification_create"; }
  void execute() override;
};

void RGWPSCreateNotif_ObjStore_Ceph::execute()
{
  ups = std::make_unique<RGWUserPubSub>(store, s->owner.get_id());

  auto b = ups->get_bucket(bucket_info.bucket);
  op_ret = b->create_notification(topic_name, events);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to create notification for topic '" << topic_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 20) << "successfully created notification for topic '" << topic_name << "'" << dendl;
}

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

  ups = make_unique<RGWUserPubSub>(store, s->owner.get_id());
  auto b = ups->get_bucket(bucket_info.bucket);
  ceph_assert(b);
  const auto psmodule = static_cast<RGWPSSyncModuleInstance*>(store->getRados()->get_sync_module().get());
  const auto& conf = psmodule->get_effective_conf();

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
    // generate the internal topic, no need to store destination info
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
    
    rgw_pubsub_sub_dest dest = topic_info.topic.dest;
    dest.bucket_name = string(conf["data_bucket_prefix"]) + s->owner.get_id().to_str() + "-" + unique_topic_name;
    dest.oid_prefix = string(conf["data_oid_prefix"]) + sub_name + "/";
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

// delete a notification
class RGWPSDeleteNotifOp : public RGWDefaultResponseOp {
protected:
  std::unique_ptr<RGWUserPubSub> ups;
  std::string bucket_name;
  RGWBucketInfo bucket_info;
  
  virtual int get_params() = 0;

public:
  int verify_permission() override {
    int ret = get_params();
    if (ret < 0) {
      return ret;
    }

    ret = store->getRados()->get_bucket_info(*s->sysobj_ctx, s->owner.get_id().tenant, bucket_name,
                                 bucket_info, nullptr, null_yield, nullptr);
    if (ret < 0) {
      return ret;
    }

    if (bucket_info.owner != s->owner.get_id()) {
      ldout(s->cct, 1) << "user doesn't own bucket, cannot remove notification" << dendl;
      return -EPERM;
    }
    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  
  RGWOpType get_type() override { return RGW_OP_PUBSUB_NOTIF_DELETE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
};

// command:  DELETE /notifications/bucket/<bucket>?topic=<topic-name>
class RGWPSDeleteNotif_ObjStore_Ceph : public RGWPSDeleteNotifOp {
private:
  std::string topic_name;

  int get_params() override {
    bool exists;
    topic_name = s->info.args.get("topic", &exists);
    if (!exists) {
      ldout(s->cct, 1) << "missing required param 'topic'" << dendl;
      return -EINVAL;
    }
    return notif_bucket_path(s->object.name, bucket_name);
  }

public:
  void execute() override;
  const char* name() const override { return "pubsub_notification_delete"; }
};

void RGWPSDeleteNotif_ObjStore_Ceph::execute() {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  ups = make_unique<RGWUserPubSub>(store, s->owner.get_id());
  auto b = ups->get_bucket(bucket_info.bucket);
  op_ret = b->remove_notification(topic_name);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to remove notification from topic '" << topic_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 20) << "successfully removed notification from topic '" << topic_name << "'" << dendl;
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

// get topics/notifications on a bucket
class RGWPSListNotifsOp : public RGWOp {
protected:
  std::string bucket_name;
  RGWBucketInfo bucket_info;
  std::unique_ptr<RGWUserPubSub> ups;

  virtual int get_params() = 0;

public:
  int verify_permission() override {
    int ret = get_params();
    if (ret < 0) {
      return ret;
    }

    ret = store->getRados()->get_bucket_info(*s->sysobj_ctx, s->owner.get_id().tenant, bucket_name,
                                 bucket_info, nullptr, null_yield, nullptr);
    if (ret < 0) {
      return ret;
    }

    if (bucket_info.owner != s->owner.get_id()) {
      ldout(s->cct, 1) << "user doesn't own bucket, cannot get topic list" << dendl;
      return -EPERM;
    }

    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }

  RGWOpType get_type() override { return RGW_OP_PUBSUB_NOTIF_LIST; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

// command: GET /notifications/bucket/<bucket>
class RGWPSListNotifs_ObjStore_Ceph : public RGWPSListNotifsOp {
private:
  rgw_pubsub_bucket_topics result;

  int get_params() override {
    return notif_bucket_path(s->object.name, bucket_name);
  }

public:
  void execute() override;
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
  const char* name() const override { return "pubsub_notifications_list"; }
};

void RGWPSListNotifs_ObjStore_Ceph::execute()
{
  ups = std::make_unique<RGWUserPubSub>(store, s->owner.get_id());
  auto b = ups->get_bucket(bucket_info.bucket);
  op_ret = b->get_topics(&result);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to get topics, ret=" << op_ret << dendl;
    return;
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
  virtual ~RGWHandler_REST_PSNotifs_Ceph() = default;
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

// factory for PubSub REST handlers 
RGWHandler_REST* RGWRESTMgr_PubSub_S3::get_handler(struct req_state* const s,
                                                     const rgw::auth::StrategyRegistry& auth_registry,
                                                     const std::string& frontend_prefix)
{
  if (RGWHandler_REST_S3::init_from_header(s, RGW_FORMAT_JSON, true) < 0) {
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
    // S3 compatible answers are XML formatted, this is not necessarily in the header of the request
    if (RGWHandler_REST_S3::reallocate_formatter(s, RGW_FORMAT_XML) < 0) {
      return nullptr;
    }
    // s3 compliant PubSub API: uses: <bucket name>?notification
    handler = new RGWHandler_REST_PSNotifs_S3(auth_registry);
  }

  ldout(s->cct, 20) << __func__ << " handler=" << (handler ? typeid(*handler).name() : "<null>") << dendl;
  return handler;
}

