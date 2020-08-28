// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once
#include <string>
#include <optional>
#include "rgw_op.h"
#include "rgw_pubsub.h"

// make sure that endpoint is a valid URL
// make sure that if user/password are passed inside URL, it is over secure connection
// update rgw_pubsub_sub_dest to indicate that a password is stored in the URL
bool validate_and_update_endpoint_secret(rgw_pubsub_sub_dest& dest, CephContext *cct, const RGWEnv& env);

// create a topic
class RGWPSCreateTopicOp : public RGWDefaultResponseOp {
protected:
  std::optional<RGWUserPubSub> ups;
  std::string topic_name;
  rgw_pubsub_sub_dest dest;
  std::string topic_arn;
  std::string opaque_data;
  
  virtual int get_params() = 0;

public:
  int verify_permission(const Span& parent_span = nullptr) override {
    return 0;
  }
  void pre_exec(const Span& parent_span = nullptr) override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute(const Span& parent_span = nullptr) override;

  const char* name() const override { return "pubsub_topic_create"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_TOPIC_CREATE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

// list all topics
class RGWPSListTopicsOp : public RGWOp {
protected:
  std::optional<RGWUserPubSub> ups;
  rgw_pubsub_user_topics result;

public:
  int verify_permission(const Span& parent_span = nullptr) override {
    return 0;
  }
  void pre_exec(const Span& parent_span = nullptr) override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute(const Span& parent_span = nullptr) override;

  const char* name() const override { return "pubsub_topics_list"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_TOPICS_LIST; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

// get topic information
class RGWPSGetTopicOp : public RGWOp {
protected:
  std::string topic_name;
  std::optional<RGWUserPubSub> ups;
  rgw_pubsub_topic_subs result;
  
  virtual int get_params() = 0;

public:
  int verify_permission(const Span& parent_span = nullptr) override {
    return 0;
  }
  void pre_exec(const Span& parent_span = nullptr) override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute(const Span& parent_span = nullptr) override;

  const char* name() const override { return "pubsub_topic_get"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_TOPIC_GET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

// delete a topic
class RGWPSDeleteTopicOp : public RGWDefaultResponseOp {
protected:
  string topic_name;
  std::optional<RGWUserPubSub> ups;
  
  virtual int get_params() = 0;

public:
  int verify_permission(const Span& parent_span = nullptr) override {
    return 0;
  }
  void pre_exec(const Span& parent_span = nullptr) override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute(const Span& parent_span = nullptr) override;

  const char* name() const override { return "pubsub_topic_delete"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_TOPIC_DELETE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
};

// create a subscription
class RGWPSCreateSubOp : public RGWDefaultResponseOp {
protected:
  std::string sub_name;
  std::string topic_name;
  std::optional<RGWUserPubSub> ups;
  rgw_pubsub_sub_dest dest;
  
  virtual int get_params() = 0;

public:
  int verify_permission(const Span& parent_span = nullptr) override {
    return 0;
  }
  void pre_exec(const Span& parent_span = nullptr) override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute(const Span& parent_span = nullptr) override;

  const char* name() const override { return "pubsub_subscription_create"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_SUB_CREATE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

// get subscription information (including push-endpoint if exist)
class RGWPSGetSubOp : public RGWOp {
protected:
  std::string sub_name;
  std::optional<RGWUserPubSub> ups;
  rgw_pubsub_sub_config result;
  
  virtual int get_params() = 0;

public:
  int verify_permission(const Span& parent_span = nullptr) override {
    return 0;
  }
  void pre_exec(const Span& parent_span = nullptr) override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute(const Span& parent_span = nullptr) override;

  const char* name() const override { return "pubsub_subscription_get"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_SUB_GET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

// delete subscription
class RGWPSDeleteSubOp : public RGWDefaultResponseOp {
protected:
  std::string sub_name;
  std::string topic_name;
  std::optional<RGWUserPubSub> ups;
  
  virtual int get_params() = 0;

public:
  int verify_permission(const Span& parent_span = nullptr) override {
    return 0;
  }
  void pre_exec(const Span& parent_span = nullptr) override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute(const Span& parent_span = nullptr) override;

  const char* name() const override { return "pubsub_subscription_delete"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_SUB_DELETE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
};

// acking of an event
class RGWPSAckSubEventOp : public RGWDefaultResponseOp {
protected:
  std::string sub_name;
  std::string event_id;
  std::optional<RGWUserPubSub> ups;
  
  virtual int get_params() = 0;

public:
  RGWPSAckSubEventOp() {}

  int verify_permission(const Span& parent_span = nullptr) override {
    return 0;
  }
  void pre_exec(const Span& parent_span = nullptr) override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute(const Span& parent_span = nullptr) override;

  const char* name() const override { return "pubsub_subscription_ack"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_SUB_ACK; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

// fetching events from a subscription
// dpending on whether the subscription was created via s3 compliant API or not
// the matching events will be returned
class RGWPSPullSubEventsOp : public RGWOp {
protected:
  int max_entries{0};
  std::string sub_name;
  std::string marker;
  std::optional<RGWUserPubSub> ups;
  RGWUserPubSub::SubRef sub; 

  virtual int get_params() = 0;

public:
  RGWPSPullSubEventsOp() {}

  int verify_permission(const Span& parent_span = nullptr) override {
    return 0;
  }
  void pre_exec(const Span& parent_span = nullptr) override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute(const Span& parent_span = nullptr) override;

  const char* name() const override { return "pubsub_subscription_pull"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_SUB_PULL; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

// notification creation
class RGWPSCreateNotifOp : public RGWDefaultResponseOp {
protected:
  std::optional<RGWUserPubSub> ups;
  string bucket_name;
  RGWBucketInfo bucket_info;

  virtual int get_params() = 0;

public:
  int verify_permission(const Span& parent_span = nullptr) override;

  void pre_exec(const Span& parent_span = nullptr) override {
    rgw_bucket_object_pre_exec(s);
  }

  RGWOpType get_type() override { return RGW_OP_PUBSUB_NOTIF_CREATE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

// delete a notification
class RGWPSDeleteNotifOp : public RGWDefaultResponseOp {
protected:
  std::optional<RGWUserPubSub> ups;
  std::string bucket_name;
  RGWBucketInfo bucket_info;
  
  virtual int get_params() = 0;

public:
  int verify_permission(const Span& parent_span = nullptr) override;

  void pre_exec(const Span& parent_span = nullptr) override {
    rgw_bucket_object_pre_exec(s);
  }
  
  RGWOpType get_type() override { return RGW_OP_PUBSUB_NOTIF_DELETE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }
};

// get topics/notifications on a bucket
class RGWPSListNotifsOp : public RGWOp {
protected:
  std::string bucket_name;
  RGWBucketInfo bucket_info;
  std::optional<RGWUserPubSub> ups;

  virtual int get_params() = 0;

public:
  int verify_permission(const Span& parent_span = nullptr) override;

  void pre_exec(const Span& parent_span = nullptr) override {
    rgw_bucket_object_pre_exec(s);
  }

  RGWOpType get_type() override { return RGW_OP_PUBSUB_NOTIF_LIST; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

