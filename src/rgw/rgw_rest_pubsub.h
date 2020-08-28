// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include "rgw_rest_s3.h"

// s3 compliant notification handler factory
class RGWHandler_REST_PSNotifs_S3 : public RGWHandler_REST_S3 {
protected:
  int init_permissions(RGWOp* op, const Span& parent_span = nullptr) override {return 0;}
  int read_permissions(RGWOp* op, const Span& parent_span = nullptr) override {return 0;}
  bool supports_quota() override {return false;}
  RGWOp* op_get() override;
  RGWOp* op_put() override;
  RGWOp* op_delete() override;
public:
  using RGWHandler_REST_S3::RGWHandler_REST_S3;
  virtual ~RGWHandler_REST_PSNotifs_S3() = default;
  // following are used to generate the operations when invoked by another REST handler
  static RGWOp* create_get_op();
  static RGWOp* create_put_op();
  static RGWOp* create_delete_op();
};

// AWS compliant topics handler factory
class RGWHandler_REST_PSTopic_AWS : public RGWHandler_REST {
  const rgw::auth::StrategyRegistry& auth_registry;
  const std::string& post_body;
  void rgw_topic_parse_input();
  //static int init_from_header(struct req_state *s, int default_formatter, bool configurable_format);
protected:
  RGWOp* op_post() override;
public:
  RGWHandler_REST_PSTopic_AWS(const rgw::auth::StrategyRegistry& _auth_registry, const std::string& _post_body) : 
      auth_registry(_auth_registry),
      post_body(_post_body) {}
  virtual ~RGWHandler_REST_PSTopic_AWS() = default;
  int postauth_init() override { return 0; }
  int authorize(const DoutPrefixProvider* dpp) override;
};

