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
#include "acconfig.h"
#ifdef WITH_RADOSGW_AMQP_ENDPOINT
#include "rgw_amqp.h"
#endif
#include <boost/asio/yield.hpp>
#include <boost/algorithm/string.hpp>
#include <functional>
#include "rgw_perf_counters.h"

using namespace rgw;

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
  ack_level_t ack_level; // TODO: not used for now
  bool verify_ssl;
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
        ack_level_t _ack_level,
        bool verify_ssl) :
      RGWPostHTTPData(_sync_env->cct, "POST", endpoint, &read_bl, verify_ssl),
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
      if (rc < 0) {
        return rc;
      }
      if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_pending);
      return 0;
    }

    // wait for reply
    int request_complete() override {
      if (perfcounter) perfcounter->dec(l_rgw_pubsub_push_pending);
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

      auto str_verify_ssl = args.get("verify-ssl", &exists);
      boost::algorithm::to_lower(str_verify_ssl);
      // verify server certificate by default
      if (!exists || str_verify_ssl == "true") {
        verify_ssl = true;
      } else if (str_verify_ssl == "false") {
        verify_ssl = false;
      } else {
          throw configuration_error("HTTP: verify-ssl must be true/false, not: " + str_verify_ssl);
      }
    }

  RGWCoroutine* send_to_completion_async(const rgw_pubsub_event& event, RGWDataSyncEnv* env) override {
    return new PostCR(json_format_pubsub_event(event), env, endpoint, ack_level, verify_ssl);
  }

  std::string to_str() const override {
    std::string str("HTTP Endpoint");
    str += "\nURI: " + endpoint;
    str += "\nAck Level: " + str_ack_level;
    str += (verify_ssl ? "\nverify SSL" : "\ndon't verify SSL");
    return str;

  }
};

#ifdef WITH_RADOSGW_AMQP_ENDPOINT
class RGWPubSubAMQPEndpoint : public RGWPubSubEndpoint {
  private:
    enum ack_level_t {
      ACK_LEVEL_NONE,
      ACK_LEVEL_BROKER,
      ACK_LEVEL_ROUTEABLE
    };
    const std::string endpoint;
    const std::string topic;
    amqp::connection_ptr_t conn;
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
    amqp::connection_ptr_t conn;
    const std::string message;

  public:
    NoAckPublishCR(RGWDataSyncEnv* _sync_env,
              const std::string& _topic,
              amqp::connection_ptr_t& _conn,
              const std::string& _message) :
      RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
      topic(_topic), conn(_conn), message(_message) {}

    // send message to endpoint, without waiting for reply
    int operate() override {
      reenter(this) {
        const auto rc = amqp::publish(conn, topic, message);
        if (rc < 0) {
          return set_cr_error(rc);
        }
        return set_cr_done();
      }
      return 0;
    }
  };

  // AckPublishCR implements async amqp publishing via coroutine
  // This coroutine ends when an ack is received from the borker 
  // note that it does not wait for an ack fron the end client
  class AckPublishCR : public RGWCoroutine, public RGWIOProvider {
  private:
    RGWDataSyncEnv* const sync_env;
    const std::string topic;
    amqp::connection_ptr_t conn;
    const std::string message;
    const ack_level_t ack_level; // TODO not used for now

  public:
    AckPublishCR(RGWDataSyncEnv* _sync_env,
              const std::string& _topic,
              amqp::connection_ptr_t& _conn,
              const std::string& _message,
              ack_level_t _ack_level) :
      RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
      topic(_topic), conn(_conn), message(_message), ack_level(_ack_level) {}

    // send message to endpoint, waiting for reply
    int operate() override {
      reenter(this) {
        yield {
          init_new_io(this);
          const auto rc = amqp::publish_with_confirm(conn, 
              topic,
              message,
              std::bind(&AckPublishCR::request_complete, this, std::placeholders::_1));
          if (rc < 0) {
            // failed to publish, does not wait for reply
            return set_cr_error(rc);
          }
          // mark as blocked on the amqp answer
          if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_pending);
          io_block();
          return 0;
        }
        return set_cr_done();
      }
      return 0;
    }

    // callback invoked from the amqp manager thread when ack/nack is received
    void request_complete(int status) {
      ceph_assert(!is_done());
      if (status != 0) {
        // server replied with a nack
        set_cr_error(status);
      }
      io_complete();
      if (perfcounter) perfcounter->dec(l_rgw_pubsub_push_pending);
    }
   
    // TODO: why are these mandatory in RGWIOProvider?
    void set_io_user_info(void *_user_info) override {
    }

    void *get_io_user_info() override {
      return nullptr;
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
        // TODO: currently broker and routable are the same - this will require different flags
        // but the same mechanism
        return new AckPublishCR(env, topic, conn, json_format_pubsub_event(event), ack_level);
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

static const std::string AMQP_0_9_1("0-9-1");
static const std::string AMQP_1_0("1-0");
#endif	// ifdef WITH_RADOSGW_AMQP_ENDPOINT

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
  if (schema == "http" || schema == "https") {
    return Ptr(new RGWPubSubHTTPEndpoint(endpoint, args));
#ifdef WITH_RADOSGW_AMQP_ENDPOINT
  } else if (schema == "amqp") {
    bool exists;
    std::string version = args.get("amqp-version", &exists);
    if (!exists) {
      version = AMQP_0_9_1;
    }
    if (version == AMQP_0_9_1) {
      return Ptr(new RGWPubSubAMQPEndpoint(endpoint, topic, args));
    } else if (version == AMQP_1_0) {
      throw configuration_error("amqp v1.0 not supported");
      return nullptr;
    } else {
      throw configuration_error("unknown amqp version " + version);
      return nullptr;
    }
  } else if (schema == "amqps") {
    throw configuration_error("amqps not supported");
    return nullptr;
#endif
  }

  throw configuration_error("unknown schema " + schema);
  return nullptr;
}

