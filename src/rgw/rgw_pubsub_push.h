// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <string>
#include <map>
#include <memory>
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
protected:
  // should be set in derived class
  std::string last_error;
public:
  RGWPubSubEndpoint() = default;
  // endpoint should not be copied
  RGWPubSubEndpoint(const RGWPubSubEndpoint&) = delete;
  const RGWPubSubEndpoint& operator=(const RGWPubSubEndpoint&) = delete;

  typedef std::unique_ptr<RGWPubSubEndpoint> Ptr;

  // factory method for the actual notification endpoint
  // derived class specific arguments are passed in http args format
  static Ptr create(const std::string& endpoint, const std::string& topic, const RGWHTTPArgs& args);
 
  // this method is used in order to send notification and wait for completion 
  // in async manner via a coroutine
  virtual RGWCoroutine* send_to_completion_async(const rgw_pubsub_event& event, RGWDataSyncEnv* env) = 0;

  // return the last error
  const std::string get_last_error() const { return last_error; }
  
  // return whether an error exist
  bool has_error() const { return last_error.length() > 0; }

  // present as string
  virtual std::string to_str() const { return ""; }
  
  virtual ~RGWPubSubEndpoint() = default;
};

