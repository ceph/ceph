// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <algorithm>
#include "rgw_rest_pubsub_common.h"
#include "rgw_rest_pubsub.h"
#include "rgw_sync_module_pubsub.h"
#include "rgw_pubsub_push.h"
#include "rgw_sync_module_pubsub_rest.h"
#include "rgw_pubsub.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_arn.h"
#include "rgw_zone.h"
#include "services/svc_zone.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

// command: PUT /topics/<topic-name>[&push-endpoint=<endpoint>[&<arg1>=<value1>]]
class RGWPSCreateTopic_ObjStore : public RGWPSCreateTopicOp {
public:
  int get_params() override {
    
    topic_name = s->object->get_name();

    opaque_data = s->info.args.get("OpaqueData");
    dest.push_endpoint = s->info.args.get("push-endpoint");
    
    if (!validate_and_update_endpoint_secret(dest, s->cct, *(s->info.env))) {
      return -EINVAL;
    }
    dest.push_endpoint_args = s->info.args.get_str();
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

// command: GET /topics
class RGWPSListTopics_ObjStore : public RGWPSListTopicsOp {
public:
  void send_response(const Span& parent_span = nullptr) override {
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

// command: GET /topics/<topic-name>
class RGWPSGetTopic_ObjStore : public RGWPSGetTopicOp {
public:
  int get_params() override {
    topic_name = s->object->get_name();
    return 0;
  }

  void send_response(const Span& parent_span = nullptr) override {
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

// command: DELETE /topics/<topic-name>
class RGWPSDeleteTopic_ObjStore : public RGWPSDeleteTopicOp {
public:
  int get_params() override {
    topic_name = s->object->get_name();
    return 0;
  }
};

// ceph specifc topics handler factory
class RGWHandler_REST_PSTopic : public RGWHandler_REST_S3 {
protected:
  int init_permissions(RGWOp* op, const Span& parent_span = nullptr) override {
    return 0;
  }

  int read_permissions(RGWOp* op, const Span& parent_span = nullptr) override {
    return 0;
  }

  bool supports_quota() override {
    return false;
  }

  RGWOp *op_get() override {
    if (s->init_state.url_bucket.empty()) {
      return nullptr;
    }
    if (s->object->empty()) {
      return new RGWPSListTopics_ObjStore();
    }
    return new RGWPSGetTopic_ObjStore();
  }
  RGWOp *op_put() override {
    if (!s->object->empty()) {
      return new RGWPSCreateTopic_ObjStore();
    }
    return nullptr;
  }
  RGWOp *op_delete() override {
    if (!s->object->empty()) {
      return new RGWPSDeleteTopic_ObjStore();
    }
    return nullptr;
  }
public:
  explicit RGWHandler_REST_PSTopic(const rgw::auth::StrategyRegistry& auth_registry) : RGWHandler_REST_S3(auth_registry) {}
  virtual ~RGWHandler_REST_PSTopic() = default;
};

// command: PUT /subscriptions/<sub-name>?topic=<topic-name>[&push-endpoint=<endpoint>[&<arg1>=<value1>]]...
class RGWPSCreateSub_ObjStore : public RGWPSCreateSubOp {
public:
  int get_params() override {
    sub_name = s->object->get_name();

    bool exists;
    topic_name = s->info.args.get("topic", &exists);
    if (!exists) {
      ldout(s->cct, 1) << "missing required param 'topic'" << dendl;
      return -EINVAL;
    }

    const auto psmodule = static_cast<RGWPSSyncModuleInstance*>(store->getRados()->get_sync_module().get());
    const auto& conf = psmodule->get_effective_conf();

    dest.push_endpoint = s->info.args.get("push-endpoint");
    if (!validate_and_update_endpoint_secret(dest, s->cct, *(s->info.env))) {
      return -EINVAL;
    }
    dest.push_endpoint_args = s->info.args.get_str();
    dest.bucket_name = string(conf["data_bucket_prefix"]) + s->owner.get_id().to_str() + "-" + topic_name;
    dest.oid_prefix = string(conf["data_oid_prefix"]) + sub_name + "/";
    dest.arn_topic = topic_name;

    return 0;
  }
};

// command: GET /subscriptions/<sub-name>
class RGWPSGetSub_ObjStore : public RGWPSGetSubOp {
public:
  int get_params() override {
    sub_name = s->object->get_name();
    return 0;
  }
  void send_response(const Span& parent_span = nullptr) override {
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

// command: DELETE /subscriptions/<sub-name>
class RGWPSDeleteSub_ObjStore : public RGWPSDeleteSubOp {
public:
  int get_params() override {
    sub_name = s->object->get_name();
    topic_name = s->info.args.get("topic");
    return 0;
  }
};

// command: POST /subscriptions/<sub-name>?ack&event-id=<event-id>
class RGWPSAckSubEvent_ObjStore : public RGWPSAckSubEventOp {
public:
  explicit RGWPSAckSubEvent_ObjStore() {}

  int get_params() override {
    sub_name = s->object->get_name();

    bool exists;

    event_id = s->info.args.get("event-id", &exists);
    if (!exists) {
      ldout(s->cct, 1) << "missing required param 'event-id'" << dendl;
      return -EINVAL;
    }
    return 0;
  }
};

// command: GET /subscriptions/<sub-name>?events[&max-entries=<max-entries>][&marker=<marker>]
class RGWPSPullSubEvents_ObjStore : public RGWPSPullSubEventsOp {
public:
  int get_params() override {
    sub_name = s->object->get_name();
    marker = s->info.args.get("marker");
    const int ret = s->info.args.get_int("max-entries", &max_entries, 
        RGWUserPubSub::Sub::DEFAULT_MAX_EVENTS);
    if (ret < 0) {
      ldout(s->cct, 1) << "failed to parse 'max-entries' param" << dendl;
      return -EINVAL;
    }
    return 0;
  }

  void send_response(const Span& parent_span = nullptr) override {
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
class RGWHandler_REST_PSSub : public RGWHandler_REST_S3 {
protected:
  int init_permissions(RGWOp* op, const Span& parent_span = nullptr) override {
    return 0;
  }

  int read_permissions(RGWOp* op, const Span& parent_span = nullptr) override {
    return 0;
  }
  bool supports_quota() override {
    return false;
  }
  RGWOp *op_get() override {
    if (s->object->empty()) {
      return nullptr;
    }
    if (s->info.args.exists("events")) {
      return new RGWPSPullSubEvents_ObjStore();
    }
    return new RGWPSGetSub_ObjStore();
  }
  RGWOp *op_put() override {
    if (!s->object->empty()) {
      return new RGWPSCreateSub_ObjStore();
    }
    return nullptr;
  }
  RGWOp *op_delete() override {
    if (!s->object->empty()) {
      return new RGWPSDeleteSub_ObjStore();
    }
    return nullptr;
  }
  RGWOp *op_post() override {
    if (s->info.args.exists("ack")) {
      return new RGWPSAckSubEvent_ObjStore();
    }
    return nullptr;
  }
public:
  explicit RGWHandler_REST_PSSub(const rgw::auth::StrategyRegistry& auth_registry) : RGWHandler_REST_S3(auth_registry) {}
  virtual ~RGWHandler_REST_PSSub() = default;
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

// command (ceph specific): PUT /notification/bucket/<bucket name>?topic=<topic name>
class RGWPSCreateNotif_ObjStore : public RGWPSCreateNotifOp {
private:
  std::string topic_name;
  rgw::notify::EventTypeList events;

  int get_params() override {
    bool exists;
    topic_name = s->info.args.get("topic", &exists);
    if (!exists) {
      ldout(s->cct, 1) << "missing required param 'topic'" << dendl;
      return -EINVAL;
    }

    std::string events_str = s->info.args.get("events", &exists);
    if (!exists) {
      // if no events are provided, we notify on all of them
      events_str = "OBJECT_CREATE,OBJECT_DELETE,DELETE_MARKER_CREATE";
    }
    rgw::notify::from_string_list(events_str, events);
    if (std::find(events.begin(), events.end(), rgw::notify::UnknownEvent) != events.end()) {
      ldout(s->cct, 1) << "invalid event type in list: " << events_str << dendl;
      return -EINVAL;
    }
    return notif_bucket_path(s->object->get_name(), bucket_name);
  }

public:
  const char* name() const override { return "pubsub_notification_create"; }
  void execute(const Span& parent_span = nullptr) override;
};

void RGWPSCreateNotif_ObjStore::execute(const Span& parent_span)
{
  ups.emplace(store, s->owner.get_id());

  auto b = ups->get_bucket(bucket_info.bucket);
  op_ret = b->create_notification(topic_name, events);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to create notification for topic '" << topic_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 20) << "successfully created notification for topic '" << topic_name << "'" << dendl;
}

// command: DELETE /notifications/bucket/<bucket>?topic=<topic-name>
class RGWPSDeleteNotif_ObjStore : public RGWPSDeleteNotifOp {
private:
  std::string topic_name;

  int get_params() override {
    bool exists;
    topic_name = s->info.args.get("topic", &exists);
    if (!exists) {
      ldout(s->cct, 1) << "missing required param 'topic'" << dendl;
      return -EINVAL;
    }
    return notif_bucket_path(s->object->get_name(), bucket_name);
  }

public:
  void execute(const Span& parent_span = nullptr) override;
  const char* name() const override { return "pubsub_notification_delete"; }
};

void RGWPSDeleteNotif_ObjStore::execute(const Span& parent_span) {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  ups.emplace(store, s->owner.get_id());
  auto b = ups->get_bucket(bucket_info.bucket);
  op_ret = b->remove_notification(topic_name);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to remove notification from topic '" << topic_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 20) << "successfully removed notification from topic '" << topic_name << "'" << dendl;
}

// command: GET /notifications/bucket/<bucket>
class RGWPSListNotifs_ObjStore : public RGWPSListNotifsOp {
private:
  rgw_pubsub_bucket_topics result;

  int get_params() override {
    return notif_bucket_path(s->object->get_name(), bucket_name);
  }

public:
  void execute(const Span& parent_span = nullptr) override;
  void send_response(const Span& parent_span = nullptr) override {
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

void RGWPSListNotifs_ObjStore::execute(const Span& parent_span)
{
  ups.emplace(store, s->owner.get_id());
  auto b = ups->get_bucket(bucket_info.bucket);
  op_ret = b->get_topics(&result);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to get topics, ret=" << op_ret << dendl;
    return;
  }
}

// ceph specific notification handler factory
class RGWHandler_REST_PSNotifs : public RGWHandler_REST_S3 {
protected:
  int init_permissions(RGWOp* op, const Span& parent_span = nullptr) override {
    return 0;
  }

  int read_permissions(RGWOp* op, const Span& parent_span = nullptr) override {
    return 0;
  }
  bool supports_quota() override {
    return false;
  }
  RGWOp *op_get() override {
    if (s->object->empty()) {
      return nullptr;
    }
    return new RGWPSListNotifs_ObjStore();
  }
  RGWOp *op_put() override {
    if (!s->object->empty()) {
      return new RGWPSCreateNotif_ObjStore();
    }
    return nullptr;
  }
  RGWOp *op_delete() override {
    if (!s->object->empty()) {
      return new RGWPSDeleteNotif_ObjStore();
    }
    return nullptr;
  }
public:
  explicit RGWHandler_REST_PSNotifs(const rgw::auth::StrategyRegistry& auth_registry) : RGWHandler_REST_S3(auth_registry) {}
  virtual ~RGWHandler_REST_PSNotifs() = default;
};

// factory for ceph specific PubSub REST handlers 
RGWHandler_REST* RGWRESTMgr_PubSub::get_handler(rgw::sal::RGWRadosStore *store,
						struct req_state* const s,
						const rgw::auth::StrategyRegistry& auth_registry,
						const std::string& frontend_prefix)
{
  if (RGWHandler_REST_S3::init_from_header(store, s, RGW_FORMAT_JSON, true) < 0) {
    return nullptr;
  }
 
  RGWHandler_REST* handler{nullptr};

  // ceph specific PubSub API: topics/subscriptions/notification are reserved bucket names
  // this API is available only on RGW that belong to a pubsub zone
  if (s->init_state.url_bucket == "topics") {
    handler = new RGWHandler_REST_PSTopic(auth_registry);
  } else if (s->init_state.url_bucket == "subscriptions") {
    handler = new RGWHandler_REST_PSSub(auth_registry);
  } else if (s->init_state.url_bucket == "notifications") {
    handler = new RGWHandler_REST_PSNotifs(auth_registry);
  } else if (s->info.args.exists("notification")) {
    const int ret = RGWHandler_REST::allocate_formatter(s, RGW_FORMAT_XML, true);
    if (ret == 0) {
        handler = new RGWHandler_REST_PSNotifs_S3(auth_registry);
    }
  }
  
  ldout(s->cct, 20) << __func__ << " handler=" << (handler ? typeid(*handler).name() : "<null>") << dendl;

  return handler;
}

