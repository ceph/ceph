// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_pubsub_push.h"
#include <string>
#include <sstream>
#include <algorithm>
#include "include/buffer_fwd.h"
#include "common/Formatter.h"
#include "rgw_common.h"
#include "rgw_data_sync.h"
#include "rgw_pubsub.h"
#include "rgw_amqp.h"
#include <boost/asio/yield.hpp>

#include <iostream>

std::string json_format_pubsub_event(const rgw_pubsub_event& event) {
  std::stringstream ss;
  JSONFormatter f(false);
  encode_json("event", event, &f);
  f.flush(ss);
  return ss.str();
}

class RGWPubSubHTTPEndpoint : public RGWPubSubEndpoint {
private:
  const std::string endpoint;
  std::string str_ack_level;
  typedef unsigned ack_level_t;
  ack_level_t ack_level;
  static const ack_level_t ACK_LEVEL_ANY = 0;
  static const ack_level_t ACK_LEVEL_NON_ERROR = 1;

  // PostCR implements async execution of RGWPostHTTPData via coroutine
  class PostCR : public RGWPostHTTPData, public RGWSimpleCoroutine {
  private:
    RGWDataSyncEnv* const sync_env;
    bufferlist read_bl;
    const ack_level_t ack_level;

  public:
    PostCR(const std::string& _post_data,
        RGWDataSyncEnv* _sync_env,
        const std::string& endpoint,
        ack_level_t _ack_level) :
      RGWPostHTTPData(_sync_env->cct, "POST", endpoint, &read_bl, false),
      RGWSimpleCoroutine(_sync_env->cct), 
      sync_env(_sync_env),
      ack_level (_ack_level) {
      // ctor also set the data to send
      set_post_data(_post_data);
      set_send_length(_post_data.length());
    }

    // send message to endpoint
    int send_request() override {
      init_new_io(this);
      const auto rc = sync_env->http_manager->add_request(this);
      // return zero on sucess, rc o/w
      return std::max(rc, 0);
    }

    // wait for reply
    int request_complete() override {
      if (ack_level == ACK_LEVEL_ANY) {
        return 0;
      } else if (ack_level == ACK_LEVEL_NON_ERROR) {
        // TODO check result code to be non-error
      } else {
        // TODO: check that result code == ack_level
      }
      return -1;
    }
  };

public:
  RGWPubSubHTTPEndpoint(const std::string& _endpoint, 
      const RGWHTTPArgs& args) :
    endpoint(_endpoint) {
      bool exists;
      str_ack_level = args.get("http-ack-level", &exists);
      if (!exists || str_ack_level == "any") {
        // "any" is default
        ack_level = ACK_LEVEL_ANY;
      } else if (str_ack_level == "non-error") {
        ack_level = ACK_LEVEL_NON_ERROR;
      } else {
        ack_level = std::atoi(str_ack_level.c_str());
        if (ack_level < 100 || ack_level >= 600) {
          throw configuration_error("HTTP: invalid http-ack-level " + str_ack_level);
        }
      }
    }

  RGWCoroutine* send_to_completion_async(const rgw_pubsub_event& event, RGWDataSyncEnv* env) override {
    return new PostCR(json_format_pubsub_event(event), env, endpoint, ack_level);
  }

  std::string to_str() const override {
    std::string str("HTTP Endpoint");
    str += "\nURI: " + endpoint;
    str += "\nAck Level: " + str_ack_level;
    return str;

  }
};

class RGWPubSubAMQPEndpoint : public RGWPubSubEndpoint {
  private:
    enum ack_level_t {
      ACK_LEVEL_NONE,
      ACK_LEVEL_BROKER,
      ACK_LEVEL_ROUTEABLE
    };
    const std::string endpoint;
    const std::string topic;
    const amqp::connection_t& conn;
    ack_level_t ack_level;
    std::string str_ack_level;

    static std::string get_exchange(const RGWHTTPArgs& args) {
      bool exists;
      const auto exchange = args.get("amqp-exchange", &exists);
      if (!exists) {
        throw configuration_error("AMQP: missing amqp-exchange");
      }
      return exchange;
    }

  // NoAckPublishCR implements async amqp publishing via coroutine
  // This coroutine ends when it send the message and does not wait for an ack
  class NoAckPublishCR : public RGWCoroutine {
  private:
    RGWDataSyncEnv* const sync_env;
    const std::string topic;
    const amqp::connection_t& conn;
    std::string message;

  public:
    NoAckPublishCR(RGWDataSyncEnv* _sync_env,
              const std::string& _topic,
              const amqp::connection_t& _conn,
              const std::string& _message) :
      RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
      topic(_topic), conn(_conn), message(_message) {}

    // send message to endpoint, without waiting for reply
    int operate() override {
      reenter(this) {
        const auto rc = amqp::publish(conn, topic, message);
        if (rc < 0) {
          std::cout << "publish error: " << amqp::status_to_string(rc) << std::endl;;
          return set_cr_error(rc);
        }
        return set_cr_done();
      }
      return 0;
    }
  };

  public:
    RGWPubSubAMQPEndpoint(const std::string& _endpoint,
        const std::string& _topic,
        const RGWHTTPArgs& args) : 
          endpoint(_endpoint), 
          topic(_topic), 
          conn(amqp::connect(endpoint, get_exchange(args))) {
      bool exists;
      // get ack level
      str_ack_level = args.get("amqp-ack-level", &exists);
      if (!exists || str_ack_level == "broker") {
        // "broker" is default
        ack_level = ACK_LEVEL_BROKER;
      } else if (str_ack_level == "none") {
        ack_level = ACK_LEVEL_NONE;
      } else if (str_ack_level == "routable") {
        ack_level = ACK_LEVEL_ROUTEABLE;
      } else {
        throw configuration_error("HTTP: invalid amqp-ack-level " + str_ack_level);
      }
    }

    RGWCoroutine* send_to_completion_async(const rgw_pubsub_event& event, RGWDataSyncEnv* env) override {
      if (ack_level == ACK_LEVEL_NONE) {
        return new NoAckPublishCR(env, topic, conn, json_format_pubsub_event(event));
      } else {
        // TODO implement the couroutines that wait for the ack
        return nullptr;
      }
    }

    std::string to_str() const override {
      std::string str("AMQP(0.9.1) Endpoint");
      str += "\nURI: " + endpoint;
      str += "\nTopic: " + topic;
      str += "\nAck Level: " + str_ack_level;
      return str;
    }
};

RGWPubSubEndpoint::Ptr RGWPubSubEndpoint::create(const std::string& endpoint, 
    const std::string& topic, 
    const RGWHTTPArgs& args) {
  //fetch the schema from the endpoint
  const auto pos = endpoint.find(':');
  if (pos == std::string::npos) {
    throw configuration_error("malformed endpoint " + endpoint);
    return nullptr;
  }
  const auto& schema = endpoint.substr(0,pos);
  if (schema == "http") {
    return Ptr(new RGWPubSubHTTPEndpoint(endpoint, args));
  } else if (schema == "https") {
    throw configuration_error("https not supported");
    return nullptr;
  } else if (schema == "amqp") {
    bool exists;
    std::string version = args.get("amqp-version", &exists);
    if (!exists) {
      version = "0-9-1";
    }
    if (version == "0-9-1") {
      return Ptr(new RGWPubSubAMQPEndpoint(endpoint, topic, args));
    } else if (version == "1-0") {
      throw configuration_error("amqp v1.0 not supported");
      return nullptr;
    } else {
      throw configuration_error("unknown amqp version " + version);
      return nullptr;
    }
  } else if (schema == "amqps") {
    throw configuration_error("amqps not supported");
    return nullptr;
  }

  throw configuration_error("unknown schema " + schema);
  return nullptr;
}

