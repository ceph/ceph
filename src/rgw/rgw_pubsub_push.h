// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <string>
#include <memory>
#include <stdexcept>
#include "include/buffer_fwd.h"

// TODO the env should be used as a template parameter to differentiate
// synchronization driven pushes to (when running on the pubsub zone) to direct rados driven pushes
// when running on the main zone
class RGWDataSyncEnv;
class RGWCoroutine;
class RGWHTTPArgs;
struct rgw_pubsub_event;

// endpoint base class all endpoint  - types should derive from it
class RGWPubSubEndpoint {
public:
  RGWPubSubEndpoint() = default;
  // endpoint should not be copied
  RGWPubSubEndpoint(const RGWPubSubEndpoint&) = delete;
  const RGWPubSubEndpoint& operator=(const RGWPubSubEndpoint&) = delete;

  typedef std::unique_ptr<RGWPubSubEndpoint> Ptr;

  // factory method for the actual notification endpoint
  // derived class specific arguments are passed in http args format
  // may throw a configuration_error if creation fails
  static Ptr create(const std::string& endpoint, const std::string& topic, const RGWHTTPArgs& args);
 
  // this method is used in order to send notification and wait for completion 
  // in async manner via a coroutine
  virtual RGWCoroutine* send_to_completion_async(const rgw_pubsub_event& event, RGWDataSyncEnv* env) = 0;

  // present as string
  virtual std::string to_str() const { return ""; }
  
  virtual ~RGWPubSubEndpoint() = default;
  
  // exception object for configuration error
  struct configuration_error : public std::logic_error {
    configuration_error(const std::string& what_arg) : 
      std::logic_error("pubsub endpoint configuration error: " + what_arg) {}
  };
};

