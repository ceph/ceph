// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_pubsub_push.h"
#include <string>
#include <sstream>
#include <algorithm>
#include <curl/curl.h>
#include "common/Formatter.h"
#include "common/iso_8601.h"
#include "common/async/completion.h"
#include "common/async/yield_waiter.h"
#include "common/async/waiter.h"
#include "rgw_asio_thread.h"
#include "rgw_common.h"
#include "rgw_data_sync.h"
#include "rgw_pubsub.h"
#include "acconfig.h"
#ifdef WITH_RADOSGW_AMQP_ENDPOINT
#include "rgw_amqp.h"
#endif
#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
#include "rgw_kafka.h"
#endif
//#include <boost/asio/yield.hpp>
#include <boost/algorithm/string.hpp>
#include <functional>
#include "rgw_perf_counters.h"

#define dout_subsys ceph_subsys_rgw_notification

using namespace rgw;

template<typename EventType>
std::string json_format_pubsub_event(const EventType& event) {
  std::stringstream ss;
  JSONFormatter f(false);
  {
    Formatter::ObjectSection s(f, EventType::json_type_plural);
    {
      Formatter::ArraySection s(f, EventType::json_type_plural);
      encode_json("", event, &f);
    }
  }
  f.flush(ss);
  return ss.str();
}
  
bool get_bool(const RGWHTTPArgs& args, const std::string& name, bool default_value) {
  bool value;
  bool exists;
  if (args.get_bool(name.c_str(), &value, &exists) == -EINVAL) {
    throw RGWPubSubEndpoint::configuration_error("invalid boolean value for " + name);
  }
  if (!exists) {
    return default_value;
  }
  return value;
}

static std::unique_ptr<RGWHTTPManager> s_http_manager;
static std::shared_mutex s_http_manager_mutex;

class RGWPubSubHTTPEndpoint : public RGWPubSubEndpoint {
private:
  CephContext* const cct;
  const std::string endpoint;
  typedef unsigned ack_level_t;
  ack_level_t ack_level; // TODO: not used for now
  const bool verify_ssl;
  const bool cloudevents;
  static const ack_level_t ACK_LEVEL_ANY = 0;
  static const ack_level_t ACK_LEVEL_NON_ERROR = 1;

public:
  RGWPubSubHTTPEndpoint(const std::string& _endpoint, const RGWHTTPArgs& args, CephContext* _cct) : 
    cct(_cct), endpoint(_endpoint), verify_ssl(get_bool(args, "verify-ssl", true)), cloudevents(get_bool(args, "cloudevents", false)) 
  {
    bool exists;
    const auto& str_ack_level = args.get("http-ack-level", &exists);
    if (!exists || str_ack_level == "any") {
      // "any" is default
      ack_level = ACK_LEVEL_ANY;
    } else if (str_ack_level == "non-error") {
      ack_level = ACK_LEVEL_NON_ERROR;
    } else {
      ack_level = std::atoi(str_ack_level.c_str());
      if (ack_level < 100 || ack_level >= 600) {
        throw configuration_error("HTTP/S: invalid http-ack-level: " + str_ack_level);
      }
    }
  }

  int send(const DoutPrefixProvider* dpp, const rgw_pubsub_s3_event& event,
           optional_yield y) override {
    std::shared_lock lock(s_http_manager_mutex);
    if (!s_http_manager) {
      ldout(cct, 1) << "ERROR: send failed. http endpoint manager not running" << dendl;
      return -ESRCH;
    }
    bufferlist read_bl;
    RGWPostHTTPData request(cct, "POST", endpoint, &read_bl, verify_ssl);
    const auto post_data = json_format_pubsub_event(event);
    if (cloudevents) {
      // following: https://github.com/cloudevents/spec/blob/v1.0.1/http-protocol-binding.md
      // using "Binary Content Mode"
      request.append_header("ce-specversion", "1.0");
      request.append_header("ce-type", "com.amazonaws." + event.eventName);
      request.append_header("ce-time", to_iso_8601(event.eventTime)); 
      // default output of iso8601 is also RFC3339 compatible
      request.append_header("ce-id", event.x_amz_request_id + "." + event.x_amz_id_2);
      request.append_header("ce-source", event.eventSource + "." + event.awsRegion + "." + event.bucket_name);
      request.append_header("ce-subject", event.object_key);
    }
    request.set_post_data(post_data);
    request.set_send_length(post_data.length());
    request.append_header("Content-Type", "application/json");
    if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_pending);
    auto rc = s_http_manager->add_request(&request);
    if (rc == 0) {
      rc = request.wait(dpp, y);
    }
    if (perfcounter) perfcounter->dec(l_rgw_pubsub_push_pending);
    // TODO: use read_bl to process return code and handle according to ack level
    return rc;
  }

  std::string to_str() const override {
    std::string str("HTTP/S Endpoint");
    str += "\nURI: " + endpoint;
    str += (verify_ssl ? "\nverify SSL" : "\ndon't verify SSL");
    return str;
  }
};

#ifdef WITH_RADOSGW_AMQP_ENDPOINT
class RGWPubSubAMQPEndpoint : public RGWPubSubEndpoint {
private:
  enum class ack_level_t {
    None,
    Broker,
    Routable
  };
  const std::string endpoint;
  const std::string topic;
  const std::string exchange;
  ack_level_t ack_level;
  amqp::connection_id_t conn_id;

  bool get_verify_ssl(const RGWHTTPArgs& args) {
    bool exists;
    auto str_verify_ssl = args.get("verify-ssl", &exists);
    if (!exists) {
      // verify server certificate by default
      return true;
    }
    boost::algorithm::to_lower(str_verify_ssl);
    if (str_verify_ssl == "true") {
      return true;
    }
    if (str_verify_ssl == "false") {
      return false;
    }
    throw configuration_error("'verify-ssl' must be true/false, not: " + str_verify_ssl);
  }

  std::string get_exchange(const RGWHTTPArgs& args) {
    bool exists;
    const auto exchange = args.get("amqp-exchange", &exists);
    if (!exists) {
      throw configuration_error("AMQP: missing amqp-exchange");
    }
    return exchange;
  }

  ack_level_t get_ack_level(const RGWHTTPArgs& args) {
    bool exists;
    const auto& str_ack_level = args.get("amqp-ack-level", &exists);
    if (!exists || str_ack_level == "broker") {
      // "broker" is default
      return ack_level_t::Broker;
    }
    if (str_ack_level == "none") {
      return ack_level_t::None;
    }
    if (str_ack_level == "routable") {
      return ack_level_t::Routable;
    }
    throw configuration_error("AMQP: invalid amqp-ack-level: " + str_ack_level);
  }
  
public:
  RGWPubSubAMQPEndpoint(const std::string& _endpoint,
      const std::string& _topic,
      const RGWHTTPArgs& args) : 
        endpoint(_endpoint), 
        topic(_topic),
        exchange(get_exchange(args)),
        ack_level(get_ack_level(args)) {
    if (!amqp::connect(conn_id, endpoint, exchange, (ack_level == ack_level_t::Broker), get_verify_ssl(args), args.get_optional("ca-location"))) {
      throw configuration_error("AMQP: failed to create connection to: " + endpoint);
    }
  }

  int send(const DoutPrefixProvider* dpp, const rgw_pubsub_s3_event& event, optional_yield y) override {
    if (ack_level == ack_level_t::None) {
      return amqp::publish(conn_id, topic, json_format_pubsub_event(event));
    } else {
      // TODO: currently broker and routable are the same - this will require different flags but the same mechanism
      if (y) {
        auto& yield = y.get_yield_context();
        ceph::async::yield_waiter<int> w;
        boost::asio::defer(yield.get_executor(),[&w, &event, this]() {
          const auto rc = amqp::publish_with_confirm(
              conn_id, topic, json_format_pubsub_event(event),
              [&w](int r) {w.complete(boost::system::error_code{}, r);});
          if (rc < 0) {
            // failed to publish, does not wait for reply
            w.complete(boost::system::error_code{}, rc);
          }
        });
        return w.async_wait(yield);
      }
      ceph::async::waiter<int> w;
      const auto rc = amqp::publish_with_confirm(
            conn_id, topic, json_format_pubsub_event(event),
            [&w](int r) {w(r);});
      if (rc < 0) {
        // failed to publish, does not wait for reply
        return rc;
      }
      return w.wait();
    }
  }

  std::string to_str() const override {
    std::string str("AMQP(0.9.1) Endpoint");
    str += "\nURI: " + endpoint;
    str += "\nTopic: " + topic;
    str += "\nExchange: " + exchange;
    return str;
  }
};

static const std::string AMQP_0_9_1("0-9-1");
static const std::string AMQP_1_0("1-0");
static const std::string AMQP_SCHEMA("amqp");
#endif	// ifdef WITH_RADOSGW_AMQP_ENDPOINT

#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
class RGWPubSubKafkaEndpoint : public RGWPubSubEndpoint {
private:
  enum class ack_level_t {
    None,
    Broker,
  };
  const std::string topic;
  const ack_level_t ack_level;
  kafka::connection_id_t conn_id;

  ack_level_t get_ack_level(const RGWHTTPArgs& args) {
    bool exists;
    const auto& str_ack_level = args.get("kafka-ack-level", &exists);
    if (!exists || str_ack_level == "broker") {
      // "broker" is default
      return ack_level_t::Broker;
    }
    if (str_ack_level == "none") {
      return ack_level_t::None;
    }
    throw configuration_error("Kafka: invalid kafka-ack-level: " + str_ack_level);
  }

public:
  RGWPubSubKafkaEndpoint(const std::string& _endpoint,
      const std::string& _topic,
      const RGWHTTPArgs& args) : 
        topic(_topic),
        ack_level(get_ack_level(args)) {
   if (!kafka::connect(
           conn_id, _endpoint, get_bool(args, "use-ssl", false),
           get_bool(args, "verify-ssl", true), args.get_optional("ca-location"),
           args.get_optional("mechanism"), args.get_optional("user-name"),
           args.get_optional("password"), args.get_optional("kafka-brokers"))) {
     throw configuration_error("Kafka: failed to create connection to: " +
                               _endpoint);
   }
 }

  int send(const DoutPrefixProvider* dpp, const rgw_pubsub_s3_event& event,
           optional_yield y) override {
    if (ack_level == ack_level_t::None) {
      return kafka::publish(conn_id, topic, json_format_pubsub_event(event));
    } else {
      if (y) {
        auto& yield = y.get_yield_context();
        ceph::async::yield_waiter<int> w;
        boost::asio::defer(yield.get_executor(),[&w, &event, this]() {
          const auto rc = kafka::publish_with_confirm(
              conn_id, topic, json_format_pubsub_event(event),
              [&w](int r) {w.complete(boost::system::error_code{}, r);});
          if (rc < 0) {
            // failed to publish, does not wait for reply
            w.complete(boost::system::error_code{}, rc);
          }
        });
        return w.async_wait(yield);
      }
      ceph::async::waiter<int> w;
      const auto rc = kafka::publish_with_confirm(
            conn_id, topic, json_format_pubsub_event(event),
            [&w](int r) {w(r);});
      if (rc < 0) {
        // failed to publish, does not wait for reply
        return rc;
      }
      return w.wait();
    }
  }

  std::string to_str() const override {
    std::string str("Kafka Endpoint");
    str += "\nBroker: " + to_string(conn_id);
    str += "\nTopic: " + topic;
    return str;
  }
};

static const std::string KAFKA_SCHEMA("kafka");
#endif	// ifdef WITH_RADOSGW_KAFKA_ENDPOINT

static const std::string WEBHOOK_SCHEMA("webhook");
static const std::string UNKNOWN_SCHEMA("unknown");
static const std::string NO_SCHEMA("");

const std::string& get_schema(const std::string& endpoint) {
  if (endpoint.empty()) {
    return NO_SCHEMA; 
  }
  const auto pos = endpoint.find(':');
  if (pos == std::string::npos) {
    return UNKNOWN_SCHEMA;
  }
  const auto& schema = endpoint.substr(0,pos);
  if (schema == "http" || schema == "https") {
    return WEBHOOK_SCHEMA;
#ifdef WITH_RADOSGW_AMQP_ENDPOINT
  } else if (schema == "amqp" || schema == "amqps") {
    return AMQP_SCHEMA;
#endif
#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
  } else if (schema == "kafka") {
    return KAFKA_SCHEMA;
#endif
  }
  return UNKNOWN_SCHEMA;
}

RGWPubSubEndpoint::Ptr RGWPubSubEndpoint::create(const std::string& endpoint, 
    const std::string& topic, 
    const RGWHTTPArgs& args,
    CephContext* cct) {
  const auto& schema = get_schema(endpoint);
  if (schema == WEBHOOK_SCHEMA) {
    return std::make_unique<RGWPubSubHTTPEndpoint>(endpoint, args, cct);
#ifdef WITH_RADOSGW_AMQP_ENDPOINT
  } else if (schema == AMQP_SCHEMA) {
    bool exists;
    std::string version = args.get("amqp-version", &exists);
    if (!exists) {
      version = AMQP_0_9_1;
    }
    if (version == AMQP_0_9_1) {
      return std::make_unique<RGWPubSubAMQPEndpoint>(endpoint, topic, args);
    } else if (version == AMQP_1_0) {
      throw configuration_error("AMQP: v1.0 not supported");
      return nullptr;
    } else {
      throw configuration_error("AMQP: unknown version: " + version);
      return nullptr;
    }
#endif
#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
  } else if (schema == KAFKA_SCHEMA) {
      return std::make_unique<RGWPubSubKafkaEndpoint>(endpoint, topic, args);
#endif
  }

  throw configuration_error("unknown schema in: " + endpoint);
  return nullptr;
}

bool init_http_manager(CephContext* cct) {
  std::unique_lock lock(s_http_manager_mutex);
  if (s_http_manager) return false;
  s_http_manager = std::make_unique<RGWHTTPManager>(cct);
  return (s_http_manager->start() == 0);
}

void shutdown_http_manager() {
  std::unique_lock lock(s_http_manager_mutex);
  if (s_http_manager) {
    s_http_manager->stop();
    s_http_manager.reset();
  }
}

bool RGWPubSubEndpoint::init_all(CephContext* cct) {
#ifdef WITH_RADOSGW_AMQP_ENDPOINT
  if (!amqp::init(cct)) {
    ldout(cct, 1) << "ERROR: failed to init amqp endpoint manager" << dendl;
    return false;
  }
#endif
#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
  if (!kafka::init(cct)) {
    ldout(cct, 1) << "ERROR: failed to init kafka endpoint manager" << dendl;
    return false;
  }
#endif
  if (!init_http_manager(cct)) {
    ldout(cct, 1) << "ERROR: failed to init http endpoint manager" << dendl;
    return false;
  }
  return true;
}

void RGWPubSubEndpoint::shutdown_all() {
#ifdef WITH_RADOSGW_AMQP_ENDPOINT
  amqp::shutdown();
#endif
#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
  kafka::shutdown();
#endif
  shutdown_http_manager();
}
