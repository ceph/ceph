// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_pubsub_push.h"
#include <string>
#include <sstream>
#include <algorithm>
#include "include/buffer_fwd.h"
#include "common/Formatter.h"
#include "common/async/completion.h"
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
#include <boost/asio/yield.hpp>
#include <boost/algorithm/string.hpp>
#include <functional>
#include "rgw_perf_counters.h"

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
    const RGWHTTPArgs& args) : endpoint(_endpoint) {
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
        throw configuration_error("HTTP/S: invalid http-ack-level: " + str_ack_level);
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
        throw configuration_error("HTTP/S: verify-ssl must be true/false, not: " + str_verify_ssl);
    }
  }

  RGWCoroutine* send_to_completion_async(const rgw_pubsub_event& event, RGWDataSyncEnv* env) override {
    return new PostCR(json_format_pubsub_event(event), env, endpoint, ack_level, verify_ssl);
  }

  RGWCoroutine* send_to_completion_async(const rgw_pubsub_s3_record& record, RGWDataSyncEnv* env) override {
    return new PostCR(json_format_pubsub_event(record), env, endpoint, ack_level, verify_ssl);
  }

  int send_to_completion_async(CephContext* cct, const rgw_pubsub_s3_record& record, optional_yield y) override {
    bufferlist read_bl;
    RGWPostHTTPData request(cct, "POST", endpoint, &read_bl, verify_ssl);
    const auto post_data = json_format_pubsub_event(record);
    request.set_post_data(post_data);
    request.set_send_length(post_data.length());
    if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_pending);
    const auto rc = RGWHTTP::process(&request, y);
    if (perfcounter) perfcounter->dec(l_rgw_pubsub_push_pending);
    // TODO: use read_bl to process return code and handle according to ack level
    return rc;
  }

  std::string to_str() const override {
    std::string str("HTTP/S Endpoint");
    str += "\nURI: " + endpoint;
    str += "\nAck Level: " + str_ack_level;
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
  CephContext* const cct;
  const std::string endpoint;
  const std::string topic;
  const std::string exchange;
  ack_level_t ack_level;
  amqp::connection_ptr_t conn;

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

  // NoAckPublishCR implements async amqp publishing via coroutine
  // This coroutine ends when it send the message and does not wait for an ack
  class NoAckPublishCR : public RGWCoroutine {
  private:
    const std::string topic;
    amqp::connection_ptr_t conn;
    const std::string message;

  public:
    NoAckPublishCR(CephContext* cct,
              const std::string& _topic,
              amqp::connection_ptr_t& _conn,
              const std::string& _message) :
      RGWCoroutine(cct),
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
    const std::string topic;
    amqp::connection_ptr_t conn;
    const std::string message;

  public:
    AckPublishCR(CephContext* cct,
              const std::string& _topic,
              amqp::connection_ptr_t& _conn,
              const std::string& _message) :
      RGWCoroutine(cct),
      topic(_topic), conn(_conn), message(_message) {}

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
      const RGWHTTPArgs& args,
      CephContext* _cct) : 
        cct(_cct),
        endpoint(_endpoint), 
        topic(_topic),
        exchange(get_exchange(args)),
        ack_level(get_ack_level(args)),
        conn(amqp::connect(endpoint, exchange, (ack_level == ack_level_t::Broker))) {
    if (!conn) { 
      throw configuration_error("AMQP: failed to create connection to: " + endpoint);
    }
  }

  RGWCoroutine* send_to_completion_async(const rgw_pubsub_event& event, RGWDataSyncEnv* env) override {
    ceph_assert(conn);
    if (ack_level == ack_level_t::None) {
      return new NoAckPublishCR(cct, topic, conn, json_format_pubsub_event(event));
    } else {
      return new AckPublishCR(cct, topic, conn, json_format_pubsub_event(event));
    }
  }
  
  RGWCoroutine* send_to_completion_async(const rgw_pubsub_s3_record& record, RGWDataSyncEnv* env) override {
    ceph_assert(conn);
    if (ack_level == ack_level_t::None) {
      return new NoAckPublishCR(cct, topic, conn, json_format_pubsub_event(record));
    } else {
      return new AckPublishCR(cct, topic, conn, json_format_pubsub_event(record));
    }
  }

  // this allows waiting untill "finish()" is called from a different thread
  // waiting could be blocking the waiting thread or yielding, depending
  // with compilation flag support and whether the optional_yield is set
  class Waiter {
    using Signature = void(boost::system::error_code);
    using Completion = ceph::async::Completion<Signature>;
    std::unique_ptr<Completion> completion = nullptr;
    int ret;

    mutable std::atomic<bool> done = false;
    mutable std::mutex lock;
    mutable std::condition_variable cond;

    template <typename ExecutionContext, typename CompletionToken>
    auto async_wait(ExecutionContext& ctx, CompletionToken&& token) {
      boost::asio::async_completion<CompletionToken, Signature> init(token);
      auto& handler = init.completion_handler;
      {
        std::unique_lock l{lock};
        completion = Completion::create(ctx.get_executor(), std::move(handler));
      }
      return init.result.get();
    }

  public:
    int wait(optional_yield y) {
      if (done) {
        return ret;
      }
#ifdef HAVE_BOOST_CONTEXT
      if (y) {
        auto& io_ctx = y.get_io_context();
        auto& yield_ctx = y.get_yield_context();
        boost::system::error_code ec;
        async_wait(io_ctx, yield_ctx[ec]);
        return -ec.value();
      }
#endif
      std::unique_lock l(lock);
      cond.wait(l, [this]{return (done==true);});
      return ret;
    }

    void finish(int r) {
      std::unique_lock l{lock};
      ret = r;
      done = true;
      if (completion) {
        boost::system::error_code ec(-ret, boost::system::system_category());
        Completion::post(std::move(completion), ec);
      } else {
        cond.notify_all();
      }
    }
  };

  int send_to_completion_async(CephContext* cct, const rgw_pubsub_s3_record& record, optional_yield y) override {
    ceph_assert(conn);
    if (ack_level == ack_level_t::None) {
      return amqp::publish(conn, topic, json_format_pubsub_event(record));
    } else {
      // TODO: currently broker and routable are the same - this will require different flags but the same mechanism
      // note: dynamic allocation of Waiter is needed when this is invoked from a beast coroutine
      auto w = std::unique_ptr<Waiter>(new Waiter);
      const auto rc = amqp::publish_with_confirm(conn, 
        topic,
        json_format_pubsub_event(record),
        std::bind(&Waiter::finish, w.get(), std::placeholders::_1));
      if (rc < 0) {
        // failed to publish, does not wait for reply
        return rc;
      }
      return w->wait(y);
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
  CephContext* const cct;
  const std::string topic;
  kafka::connection_ptr_t conn;
  const ack_level_t ack_level;

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

  bool get_use_ssl(const RGWHTTPArgs& args) {
    bool exists;
    auto str_use_ssl = args.get("use-ssl", &exists);
    if (!exists) {
      // by default ssl not used
      return false;
    }
    boost::algorithm::to_lower(str_use_ssl);
    if (str_use_ssl == "true") {
      return true;
    }
    if (str_use_ssl == "false") {
      return false;
    }
    throw configuration_error("'use-ssl' must be true/false, not: " + str_use_ssl);
  }

  ack_level_t get_ack_level(const RGWHTTPArgs& args) {
    bool exists;
    // get ack level
    const auto str_ack_level = args.get("kafka-ack-level", &exists);
    if (!exists || str_ack_level == "broker") {
      // "broker" is default
      return ack_level_t::Broker;
    }
    if (str_ack_level == "none") {
      return ack_level_t::None;
    }
    throw configuration_error("Kafka: invalid kafka-ack-level: " + str_ack_level);
  }

  // NoAckPublishCR implements async kafka publishing via coroutine
  // This coroutine ends when it send the message and does not wait for an ack
  class NoAckPublishCR : public RGWCoroutine {
  private:
    const std::string topic;
    kafka::connection_ptr_t conn;
    const std::string message;

  public:
    NoAckPublishCR(CephContext* cct,
              const std::string& _topic,
              kafka::connection_ptr_t& _conn,
              const std::string& _message) :
      RGWCoroutine(cct),
      topic(_topic), conn(_conn), message(_message) {}

    // send message to endpoint, without waiting for reply
    int operate() override {
      reenter(this) {
        const auto rc = kafka::publish(conn, topic, message);
        if (rc < 0) {
          return set_cr_error(rc);
        }
        return set_cr_done();
      }
      return 0;
    }
  };

  // AckPublishCR implements async kafka publishing via coroutine
  // This coroutine ends when an ack is received from the borker 
  // note that it does not wait for an ack fron the end client
  class AckPublishCR : public RGWCoroutine, public RGWIOProvider {
  private:
    const std::string topic;
    kafka::connection_ptr_t conn;
    const std::string message;

  public:
    AckPublishCR(CephContext* cct,
              const std::string& _topic,
              kafka::connection_ptr_t& _conn,
              const std::string& _message) :
      RGWCoroutine(cct),
      topic(_topic), conn(_conn), message(_message) {}

    // send message to endpoint, waiting for reply
    int operate() override {
      reenter(this) {
        yield {
          init_new_io(this);
          const auto rc = kafka::publish_with_confirm(conn, 
              topic,
              message,
              std::bind(&AckPublishCR::request_complete, this, std::placeholders::_1));
          if (rc < 0) {
            // failed to publish, does not wait for reply
            return set_cr_error(rc);
          }
          // mark as blocked on the kafka answer
          if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_pending);
          io_block();
          return 0;
        }
        return set_cr_done();
      }
      return 0;
    }

    // callback invoked from the kafka manager thread when ack/nack is received
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
  RGWPubSubKafkaEndpoint(const std::string& _endpoint,
      const std::string& _topic,
      const RGWHTTPArgs& args,
      CephContext* _cct) : 
        cct(_cct),
        topic(_topic),
        conn(kafka::connect(_endpoint, get_use_ssl(args), get_verify_ssl(args), args.get_optional("ca-location"))) ,
        ack_level(get_ack_level(args)) {
    if (!conn) { 
      throw configuration_error("Kafka: failed to create connection to: " + _endpoint);
    }
  }

  RGWCoroutine* send_to_completion_async(const rgw_pubsub_event& event, RGWDataSyncEnv* env) override {
    ceph_assert(conn);
    if (ack_level == ack_level_t::None) {
      return new NoAckPublishCR(cct, topic, conn, json_format_pubsub_event(event));
    } else {
      return new AckPublishCR(cct, topic, conn, json_format_pubsub_event(event));
    }
  }
  
  RGWCoroutine* send_to_completion_async(const rgw_pubsub_s3_record& record, RGWDataSyncEnv* env) override {
    ceph_assert(conn);
    if (ack_level == ack_level_t::None) {
      return new NoAckPublishCR(cct, topic, conn, json_format_pubsub_event(record));
    } else {
      return new AckPublishCR(cct, topic, conn, json_format_pubsub_event(record));
    }
  }

  // this allows waiting untill "finish()" is called from a different thread
  // waiting could be blocking the waiting thread or yielding, depending
  // with compilation flag support and whether the optional_yield is set
  class Waiter {
    using Signature = void(boost::system::error_code);
    using Completion = ceph::async::Completion<Signature>;
    std::unique_ptr<Completion> completion = nullptr;
    int ret;

    mutable std::atomic<bool> done = false;
    mutable std::mutex lock;
    mutable std::condition_variable cond;

    template <typename ExecutionContext, typename CompletionToken>
    auto async_wait(ExecutionContext& ctx, CompletionToken&& token) {
      boost::asio::async_completion<CompletionToken, Signature> init(token);
      auto& handler = init.completion_handler;
      {
        std::unique_lock l{lock};
        completion = Completion::create(ctx.get_executor(), std::move(handler));
      }
      return init.result.get();
    }

  public:
    int wait(optional_yield y) {
      if (done) {
        return ret;
      }
#ifdef HAVE_BOOST_CONTEXT
      if (y) {
        auto& io_ctx = y.get_io_context();
        auto& yield_ctx = y.get_yield_context();
        boost::system::error_code ec;
        async_wait(io_ctx, yield_ctx[ec]);
        return -ec.value();
      }
#endif
      std::unique_lock l(lock);
      cond.wait(l, [this]{return (done==true);});
      return ret;
    }

    void finish(int r) {
      std::unique_lock l{lock};
      ret = r;
      done = true;
      if (completion) {
        boost::system::error_code ec(-ret, boost::system::system_category());
        Completion::post(std::move(completion), ec);
      } else {
        cond.notify_all();
      }
    }
  };

  int send_to_completion_async(CephContext* cct, const rgw_pubsub_s3_record& record, optional_yield y) override {
    ceph_assert(conn);
    if (ack_level == ack_level_t::None) {
      return kafka::publish(conn, topic, json_format_pubsub_event(record));
    } else {
      // note: dynamic allocation of Waiter is needed when this is invoked from a beast coroutine
      auto w = std::unique_ptr<Waiter>(new Waiter);
      const auto rc = kafka::publish_with_confirm(conn, 
        topic,
        json_format_pubsub_event(record),
        std::bind(&Waiter::finish, w.get(), std::placeholders::_1));
      if (rc < 0) {
        // failed to publish, does not wait for reply
        return rc;
      }
      return w->wait(y);
    }
  }

  std::string to_str() const override {
    std::string str("Kafka Endpoint");
    str += kafka::to_string(conn);
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
  } else if (schema == "amqp") {
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
    return Ptr(new RGWPubSubHTTPEndpoint(endpoint, args));
#ifdef WITH_RADOSGW_AMQP_ENDPOINT
  } else if (schema == AMQP_SCHEMA) {
    bool exists;
    std::string version = args.get("amqp-version", &exists);
    if (!exists) {
      version = AMQP_0_9_1;
    }
    if (version == AMQP_0_9_1) {
      return Ptr(new RGWPubSubAMQPEndpoint(endpoint, topic, args, cct));
    } else if (version == AMQP_1_0) {
      throw configuration_error("AMQP: v1.0 not supported");
      return nullptr;
    } else {
      throw configuration_error("AMQP: unknown version: " + version);
      return nullptr;
    }
  } else if (schema == "amqps") {
    throw configuration_error("AMQP: ssl not supported");
    return nullptr;
#endif
#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
  } else if (schema == KAFKA_SCHEMA) {
      return Ptr(new RGWPubSubKafkaEndpoint(endpoint, topic, args, cct));
#endif
  }

  throw configuration_error("unknown schema in: " + endpoint);
  return nullptr;
}

