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
  std::optional<RGWPubSub> ps;
  std::string topic_name;
  rgw_pubsub_sub_dest dest;
  std::string topic_arn;
  std::string opaque_data;
  
  virtual int get_params() = 0;

public:
  int verify_permission(optional_yield) override {
    return 0;
  }
  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }
  void execute(optional_yield) override;

  const char* name() const override { return "pubsub_topic_create"; }
  RGWOpType get_type() override { return RGW_OP_PUBSUB_TOPIC_CREATE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

// list all topics
class RGWPSListTopicsOp : public RGWOp {
protected:
  std::optional<RGWPubSub> ps;
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
};

// get topic information
class RGWPSGetTopicOp : public RGWOp {
protected:
  std::string topic_name;
  std::optional<RGWPubSub> ps;
  rgw_pubsub_topic_subs result;
  
  virtual int get_params() = 0;

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
};

// delete a topic
class RGWPSDeleteTopicOp : public RGWDefaultResponseOp {
protected:
  std::string topic_name;
  std::optional<RGWPubSub> ps;
  
  virtual int get_params() = 0;

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
};

// notification creation
class RGWPSCreateNotifOp : public RGWDefaultResponseOp {
protected:
  std::optional<RGWPubSub> ps;
  std::string bucket_name;
  RGWBucketInfo bucket_info;

  virtual int get_params() = 0;

public:
  int verify_permission(optional_yield y) override;

  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }

  RGWOpType get_type() override { return RGW_OP_PUBSUB_NOTIF_CREATE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }
};

// delete a notification
class RGWPSDeleteNotifOp : public RGWDefaultResponseOp {
protected:
  std::optional<RGWPubSub> ps;
  std::string bucket_name;
  RGWBucketInfo bucket_info;
  
  virtual int get_params() = 0;

public:
  int verify_permission(optional_yield y) override;

  void pre_exec() override {
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
  std::optional<RGWPubSub> ps;

  virtual int get_params() = 0;

public:
  int verify_permission(optional_yield y) override;

  void pre_exec() override {
    rgw_bucket_object_pre_exec(s);
  }

  RGWOpType get_type() override { return RGW_OP_PUBSUB_NOTIF_LIST; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};
