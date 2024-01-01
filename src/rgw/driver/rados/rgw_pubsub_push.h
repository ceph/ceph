// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
#pragma once

#include <string>
#include <memory>
#include <stdexcept>
#include "include/buffer_fwd.h"
#include "include/common_fwd.h"
#include "common/async/yield_context.h"

// TODO the env should be used as a template parameter to differentiate the source that triggers the pushes
class RGWDataSyncEnv;
class RGWHTTPArgs;
struct rgw_pubsub_s3_event;

// endpoint base class all endpoint  - types should derive from it
class RGWPubSubEndpoint {
public:
  RGWPubSubEndpoint() = default;
  // endpoint should not be copied
  RGWPubSubEndpoint(const RGWPubSubEndpoint&) = delete;
  const RGWPubSubEndpoint& operator=(const RGWPubSubEndpoint&) = delete;

  // factory method for the actual notification endpoint
  // derived class specific arguments are passed in http args format
  // may throw a configuration_error if creation fails
  static RGWPubSubEndpoint* create(const std::string& endpoint, const std::string& topic, const RGWHTTPArgs& args, CephContext *cct=nullptr);
 
  // this method is used in order to send notification to the endpoint
  virtual int send(CephContext* cct, const rgw_pubsub_s3_event& event, optional_yield y) = 0;

  // this method is used in order to verify connectivity with the endpoint
  virtual int is_alive(CephContext* cct, optional_yield y) = 0;

  // present as string
  virtual std::string to_str() const { return ""; }
  
  virtual ~RGWPubSubEndpoint() = default;
  
  // exception object for configuration error
  struct configuration_error : public std::logic_error {
    configuration_error(const std::string& what_arg) : 
      std::logic_error("pubsub endpoint configuration error: " + what_arg) {}
  };
};

