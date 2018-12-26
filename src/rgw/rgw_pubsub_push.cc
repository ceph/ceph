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
  unsigned ack_level;
  static const unsigned ACK_LEVEL_ANY = 0;
  static const unsigned ACK_LEVEL_NON_ERROR = 1;

  // PostCR implements async execution of RGWPostHTTPData via coroutine
  class PostCR : public RGWPostHTTPData, public RGWSimpleCoroutine {
  private:
    RGWDataSyncEnv* const sync_env;
    bufferlist read_bl;

  public:
    PostCR(const std::string& _post_data,
        RGWDataSyncEnv* _sync_env,
        const std::string& endpoint) :
      RGWPostHTTPData(_sync_env->cct, "POST", endpoint, &read_bl, false),
      RGWSimpleCoroutine(_sync_env->cct), sync_env(_sync_env) {
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
      // TODO: check result and return accordingly
      // this should be dependent with the configured ack level
      return 0;
    }
  };

public:
  RGWPubSubHTTPEndpoint(const std::string& _endpoint, 
      const RGWHTTPArgs& args) :
    endpoint(_endpoint) {
      bool exists;
      str_ack_level = args.get("http-ack-level", &exists);
      if (!exists || str_ack_level == "any") {
        ack_level = ACK_LEVEL_ANY;
      } else if (str_ack_level == "non-error") {
        ack_level = ACK_LEVEL_NON_ERROR;
      } else {
        ack_level = std::atoi(str_ack_level.c_str());
        if (ack_level < 100 || ack_level >= 600) {
          last_error = "configuration: invalid ack level.";
        }
      }
    }

  RGWCoroutine* send_to_completion_async(const rgw_pubsub_event& event, RGWDataSyncEnv* env) override {
    if (has_error()) return nullptr;
    return new PostCR(json_format_pubsub_event(event), env, endpoint);
  }

  std::string to_str() const override {
    std::string str("HTTP Endpoint");
    str += "\nURI: " + endpoint;
    str += "\nAck Level: " + str_ack_level;
    str += "\nLast Error: " + last_error;
    return str;

  }
};

class RGWPubSubAMQPEndpoint : public RGWPubSubEndpoint {
  private:
    enum AckLevel {
      ACK_LEVEL_NONE,
      ACK_LEVEL_BROKER,
      ACK_LEVEL_ROUTEABLE
    };
    const std::string endpoint;
    const std::string topic;
    std::string exchange;
    amqp_connection_state_t conn;
    AckLevel ack_level;
    std::string str_ack_level;

  // PublishCR implements async amqp publishing via coroutine
  class PublishCR : public RGWSimpleCoroutine {
  private:
    RGWDataSyncEnv* const sync_env;
    const std::string topic;
    const std::string exchange;
    amqp_connection_state_t conn;
    std::string message;

  public:
    PublishCR(RGWDataSyncEnv* _sync_env,
              const std::string _topic,
              const std::string _exchange,
              amqp_connection_state_t _conn,
              const std::string& _message) :
      RGWSimpleCoroutine(_sync_env->cct), sync_env(_sync_env),
      topic(_topic), exchange(_exchange),
      conn(_conn), message(_message) { }

    // send message to endpoint
    int send_request() override {
      const auto rc = amqp::publish(conn, exchange, topic, message);
      // return zero on sucess, -1 o/w
      return rc ? 0 : -1;
    }

    // wait for reply
    int request_complete() override {
      // TODO: check result and return accordingly
      // this should be dependent with hthe configured ack level
      return 0;
    }
  };

  public:
    RGWPubSubAMQPEndpoint(const std::string& _endpoint,
        const std::string& _topic,
        const RGWHTTPArgs& args) :
      endpoint(_endpoint), topic(_topic) {
        bool exists;
        // get exchange
        exchange = args.get("amqp-exchange", &exists);
        if (!exists) {
          last_error = "configuration: missing exchange.";
        }
        // get ack level
        str_ack_level = args.get("amqp-ack-level", &exists);
        if (!exists || str_ack_level == "broker") {
          ack_level = ACK_LEVEL_BROKER;
        } else if (str_ack_level == "none") {
          ack_level = ACK_LEVEL_NONE;
        } else if (str_ack_level == "routable") {
          ack_level = ACK_LEVEL_ROUTEABLE;
        } else {
          last_error += "configuration: invalid ack level.";
        }
        // connect to broker, if connection already exists it will be reused
        conn = amqp::connect(endpoint, exchange);
        if (!conn) {
          last_error += "configuration: failed to connect to amqp broker.";
        }
      }

    RGWCoroutine* send_to_completion_async(const rgw_pubsub_event& event, RGWDataSyncEnv* env) override {
      if (has_error()) return nullptr;
      return new PublishCR(env, topic, exchange, conn, json_format_pubsub_event(event));
    }

    std::string to_str() const override {
      std::string str("AMQP(0.9.1) Endpoint");
      str += "\nURI: " + endpoint;
      str += "\nTopic: " + topic;
      str += "\nAck Level: " + str_ack_level;
      str += "\nLast Error: " + last_error;
      return str;
    }
};

RGWPubSubEndpoint::Ptr RGWPubSubEndpoint::create(const std::string& endpoint, 
    const std::string& topic, 
    const RGWHTTPArgs& args) {
  //fetch the schema from the endpoint
  const auto pos = endpoint.find(':');
  if (pos == std::string::npos) {
    // unknowm schema
    return nullptr;
  }
  const auto& schema = endpoint.substr(0,pos);
  if (schema == "http") {
    return Ptr(new RGWPubSubHTTPEndpoint(endpoint, args));
  } else if (schema == "https") {
    // not supported yet
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
      // not supported yet
      return nullptr;
    } else {
      // unknown version
      return nullptr;
    }
  } else if (schema == "amqps") {
    // not supported yet
    return nullptr;
  }

  // unknown schema
  return nullptr;
}

