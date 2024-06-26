// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_kafka.h"
#include "rgw_url.h"
#include <librdkafka/rdkafka.h>
#include "include/ceph_assert.h"
#include <sstream>
#include <cstring>
#include <unordered_map>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <boost/lockfree/queue.hpp>
#include "common/dout.h"

#define dout_subsys ceph_subsys_rgw_notification

// comparison operator between topic pointer and name
bool operator==(const rd_kafka_topic_t* rkt, const std::string& name) {
    return name == std::string_view(rd_kafka_topic_name(rkt)); 
}

// this is the inverse of rd_kafka_errno2err
// see: https://github.com/confluentinc/librdkafka/blob/master/src/rdkafka.c
inline int rd_kafka_err2errno(rd_kafka_resp_err_t err) {
  if (err == 0) return 0;
  switch (err) {
    case RD_KAFKA_RESP_ERR__INVALID_ARG:
    return EINVAL;
  case RD_KAFKA_RESP_ERR__CONFLICT:
    return EBUSY;
  case RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC:
    return ENOENT;
  case RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION:
    return ESRCH;
  case RD_KAFKA_RESP_ERR__TIMED_OUT:
  case RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:
    return ETIMEDOUT;
  case RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE:
    return EMSGSIZE;
  case RD_KAFKA_RESP_ERR__QUEUE_FULL:
    return ENOBUFS;                                                                                                                                                                                                                           
  default:
    return EIO;
  }
}

namespace rgw::kafka {

enum Status {
  STATUS_CONNECTION_CLOSED = -0x1002,
  STATUS_CONNECTION_IDLE   = -0x1006,
  STATUS_CONF_ALLOC_FAILED = -0x2001,
};

// convert int status to string - both RGW and librdkafka values
inline std::string status_to_string(int s) {
  switch (s) {
    case STATUS_CONNECTION_CLOSED:
      return "Kafka connection closed";
    case STATUS_CONF_ALLOC_FAILED:
      return "Kafka configuration allocation failed";
    case STATUS_CONNECTION_IDLE:
      return "Kafka connection idle";
    default:
      return std::string(rd_kafka_err2str(static_cast<rd_kafka_resp_err_t>(s)));
  }
}

// convert int status to errno - both RGW and librdkafka values
inline int status_to_errno(int s) {
  if (s == 0) return 0;
  switch (s) {
    case STATUS_CONNECTION_CLOSED:
      return -EIO;
    case STATUS_CONF_ALLOC_FAILED:
      return -ENOMEM;
    case STATUS_CONNECTION_IDLE:
      return -EIO;
    default:
      return -rd_kafka_err2errno(static_cast<rd_kafka_resp_err_t>(s));
  }
}

// struct for holding the callback and its tag in the callback list
struct reply_callback_with_tag_t {
  uint64_t tag;
  reply_callback_t cb;
  
  reply_callback_with_tag_t(uint64_t _tag, reply_callback_t _cb) : tag(_tag), cb(_cb) {}
  
  bool operator==(uint64_t rhs) {
    return tag == rhs;
  }
};

typedef std::vector<reply_callback_with_tag_t> CallbackList;

struct connection_t {
  rd_kafka_t* producer = nullptr;
  std::vector<rd_kafka_topic_t*> topics;
  uint64_t delivery_tag = 1;
  int status = 0;
  CephContext* const cct;
  CallbackList callbacks;
  const std::string broker;
  const bool use_ssl;
  const bool verify_ssl; // TODO currently ignored, not supported in librdkafka v0.11.6
  const boost::optional<std::string> ca_location;
  const std::string user;
  const std::string password;
  const boost::optional<std::string> mechanism;
  utime_t timestamp = ceph_clock_now();

  // cleanup of all internal connection resource
  // the object can still remain, and internal connection
  // resources created again on successful reconnection
  void destroy() {
    if (!producer) {
      // no producer, nothing to destroy
      return;
    }
    // wait for 500ms to try and handle pending callbacks
    rd_kafka_flush(producer, 500);
    // destroy all topics
    std::for_each(topics.begin(), topics.end(), [](auto topic) {rd_kafka_topic_destroy(topic);});
    topics.clear();
    // destroy producer
    rd_kafka_destroy(producer);
    producer = nullptr;
    // fire all remaining callbacks (if not fired by rd_kafka_flush)
    std::for_each(callbacks.begin(), callbacks.end(), [this](auto& cb_tag) {
        ldout(cct, 1) << "Kafka destroy: invoking callback with tag: "
                       << cb_tag.tag << " for: " << broker
                       << " with status: " << status_to_string(status) << dendl;
        cb_tag.cb(status_to_errno(status));
      });
    callbacks.clear();
    delivery_tag = 1;
    ldout(cct, 20) << "Kafka destroy: complete for: " << broker << dendl;
  }

  // ctor for setting immutable values
  connection_t(CephContext* _cct, const std::string& _broker, bool _use_ssl, bool _verify_ssl, 
          const boost::optional<const std::string&>& _ca_location,
          const std::string& _user, const std::string& _password, const boost::optional<const std::string&>& _mechanism) :
      cct(_cct), broker(_broker), use_ssl(_use_ssl), verify_ssl(_verify_ssl), ca_location(_ca_location), user(_user), password(_password), mechanism(_mechanism) {}                                                                                                                                                        

  // dtor also destroys the internals
  ~connection_t() {
    destroy();
  }
};

void message_callback(rd_kafka_t* rk, const rd_kafka_message_t* rkmessage, void* opaque) {
  ceph_assert(opaque);

  const auto conn = reinterpret_cast<connection_t*>(opaque);
  const auto result = rkmessage->err;

  if (rkmessage->err == 0) {
      ldout(conn->cct, 20) << "Kafka run: ack received with result=" << 
        rd_kafka_err2str(result) << dendl;
  } else {
      ldout(conn->cct, 1) << "Kafka run: nack received with result=" << 
        rd_kafka_err2str(result) << dendl;
  }

  if (!rkmessage->_private) {
    ldout(conn->cct, 20) << "Kafka run: n/ack received without a callback" << dendl;
    return;  
  }

  const auto tag = reinterpret_cast<uint64_t*>(rkmessage->_private);
  const auto& callbacks_end = conn->callbacks.end();
  const auto& callbacks_begin = conn->callbacks.begin();
  const auto tag_it = std::find(callbacks_begin, callbacks_end, *tag);
  if (tag_it != callbacks_end) {
      ldout(conn->cct, 20) << "Kafka run: n/ack received, invoking callback with tag=" << 
          *tag << dendl;
      tag_it->cb(-rd_kafka_err2errno(result));
      conn->callbacks.erase(tag_it);
  } else {
    // TODO add counter for acks with no callback
    ldout(conn->cct, 10) << "Kafka run: unsolicited n/ack received with tag=" << 
        *tag << dendl;
  }
  delete tag;
  // rkmessage is destroyed automatically by librdkafka
}

void log_callback(const rd_kafka_t* rk, int level, const char *fac, const char *buf) {
  ceph_assert(rd_kafka_opaque(rk));
  
  const auto conn = reinterpret_cast<connection_t*>(rd_kafka_opaque(rk));
  if (level <= 3)
    ldout(conn->cct, 1) << "RDKAFKA-" << level << "-" << fac << ": " << rd_kafka_name(rk) << ": " << buf << dendl;
  else if (level <= 5)
    ldout(conn->cct, 2) << "RDKAFKA-" << level << "-" << fac << ": " << rd_kafka_name(rk) << ": " << buf << dendl;
  else if (level <= 6)
    ldout(conn->cct, 10) << "RDKAFKA-" << level << "-" << fac << ": " << rd_kafka_name(rk) << ": " << buf << dendl;
  else
    ldout(conn->cct, 20) << "RDKAFKA-" << level << "-" << fac << ": " << rd_kafka_name(rk) << ": " << buf << dendl;
}

void poll_err_callback(rd_kafka_t *rk, int err, const char *reason, void *opaque) {
  const auto conn = reinterpret_cast<connection_t*>(rd_kafka_opaque(rk));
  ldout(conn->cct, 10) << "Kafka run: poll error(" << err << "): " << reason << dendl;
}

using connection_t_ptr = std::unique_ptr<connection_t>;

// utility function to create a producer, when the connection object already exists
bool new_producer(connection_t* conn) {
  // reset all status codes
  conn->status = 0;
  ceph_assert(!conn->producer);

  auto kafka_conf_deleter = [](rd_kafka_conf_t* conf) {rd_kafka_conf_destroy(conf);};

  std::unique_ptr<rd_kafka_conf_t, decltype(kafka_conf_deleter)> conf(rd_kafka_conf_new(), kafka_conf_deleter);
  if (!conf) {
    ldout(conn->cct, 1) << "Kafka connect: failed to allocate configuration" << dendl;
    conn->status = STATUS_CONF_ALLOC_FAILED;
    return false;
  }

  char errstr[512] = {0};

  // set message timeout
  // according to documentation, value of zero will expire the message based on retries.
  // however, testing with librdkafka v1.6.1 did not expire the message in that case. hence, a value of zero is changed to 1ms
  constexpr std::uint64_t min_message_timeout = 1;
  const auto message_timeout = std::max(min_message_timeout, conn->cct->_conf->rgw_kafka_message_timeout);
  if (rd_kafka_conf_set(conf.get(), "message.timeout.ms", 
        std::to_string(message_timeout).c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;

  // get list of brokers based on the bootstrap broker
  if (rd_kafka_conf_set(conf.get(), "bootstrap.servers", conn->broker.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;

  if (conn->use_ssl) {
    if (!conn->user.empty()) {
      // use SSL+SASL
      if (rd_kafka_conf_set(conf.get(), "security.protocol", "SASL_SSL", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
              rd_kafka_conf_set(conf.get(), "sasl.username", conn->user.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
              rd_kafka_conf_set(conf.get(), "sasl.password", conn->password.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;
      ldout(conn->cct, 20) << "Kafka connect: successfully configured SSL+SASL security" << dendl;

      if (conn->mechanism) {
        if (rd_kafka_conf_set(conf.get(), "sasl.mechanism", conn->mechanism->c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;
        ldout(conn->cct, 20) << "Kafka connect: successfully configured SASL mechanism" << dendl;
      } else {
        if (rd_kafka_conf_set(conf.get(), "sasl.mechanism", "PLAIN", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;
        ldout(conn->cct, 20) << "Kafka connect: using default SASL mechanism" << dendl;
      }

    } else {
      // use only SSL
      if (rd_kafka_conf_set(conf.get(), "security.protocol", "SSL", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;
      ldout(conn->cct, 20) << "Kafka connect: successfully configured SSL security" << dendl;
    }
    if (conn->ca_location) {
      if (rd_kafka_conf_set(conf.get(), "ssl.ca.location", conn->ca_location->c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;
      ldout(conn->cct, 20) << "Kafka connect: successfully configured CA location" << dendl;
    } else {
      ldout(conn->cct, 20) << "Kafka connect: using default CA location" << dendl;
    }
    // Note: when librdkafka.1.0 is available the following line could be uncommented instead of the callback setting call
    // if (rd_kafka_conf_set(conn->conf, "enable.ssl.certificate.verification", "0", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;

    ldout(conn->cct, 20) << "Kafka connect: successfully configured security" << dendl;
  } else if (!conn->user.empty()) {
      // use SASL+PLAINTEXT
      if (rd_kafka_conf_set(conf.get(), "security.protocol", "SASL_PLAINTEXT", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
              rd_kafka_conf_set(conf.get(), "sasl.username", conn->user.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
              rd_kafka_conf_set(conf.get(), "sasl.password", conn->password.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;
      ldout(conn->cct, 20) << "Kafka connect: successfully configured SASL_PLAINTEXT" << dendl;

      if (conn->mechanism) {
        if (rd_kafka_conf_set(conf.get(), "sasl.mechanism", conn->mechanism->c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;
        ldout(conn->cct, 20) << "Kafka connect: successfully configured SASL mechanism" << dendl;
      } else {
        if (rd_kafka_conf_set(conf.get(), "sasl.mechanism", "PLAIN", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;
        ldout(conn->cct, 20) << "Kafka connect: using default SASL mechanism" << dendl;
      }
  }

  // set the global callback for delivery success/fail
  rd_kafka_conf_set_dr_msg_cb(conf.get(), message_callback);
  // set the global opaque pointer to be the connection itself
  rd_kafka_conf_set_opaque(conf.get(), conn);
  // redirect kafka logs to RGW
  rd_kafka_conf_set_log_cb(conf.get(), log_callback);
  // define poll callback to allow reconnect
  rd_kafka_conf_set_error_cb(conf.get(), poll_err_callback);
  // create the producer and move conf ownership to it
  conn->producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf.release(), errstr, sizeof(errstr));
  if (!conn->producer) {
    conn->status = rd_kafka_last_error();
    ldout(conn->cct, 1) << "Kafka connect: failed to create producer: " << errstr << dendl;
    return false;
  }
  ldout(conn->cct, 20) << "Kafka connect: successfully created new producer" << dendl;
  {
    // set log level of producer
    const auto log_level = conn->cct->_conf->subsys.get_log_level(ceph_subsys_rgw);
    if (log_level <= 1)
      rd_kafka_set_log_level(conn->producer, 3);
    else if (log_level <= 2)
      rd_kafka_set_log_level(conn->producer, 5);
    else if (log_level <= 10)
      rd_kafka_set_log_level(conn->producer, 6);
    else
      rd_kafka_set_log_level(conn->producer, 7);
  }

  return true;

conf_error:
  conn->status = rd_kafka_last_error();
  ldout(conn->cct, 1) << "Kafka connect: configuration failed: " << errstr << dendl;
  return false;
}

// struct used for holding messages in the message queue
struct message_wrapper_t {
  std::string conn_name; 
  std::string topic;
  std::string message;
  const reply_callback_t cb;
  
  message_wrapper_t(const std::string& _conn_name,
      const std::string& _topic,
      const std::string& _message,
      reply_callback_t _cb) : conn_name(_conn_name), topic(_topic), message(_message), cb(_cb) {}
};

typedef std::unordered_map<std::string, connection_t_ptr> ConnectionList;
typedef boost::lockfree::queue<message_wrapper_t*, boost::lockfree::fixed_sized<true>> MessageQueue;

class Manager {
public:
  const size_t max_connections;
  const size_t max_inflight;
  const size_t max_queue;
private:
  std::atomic<size_t> connection_count;
  bool stopped;
  int read_timeout_ms;
  ConnectionList connections;
  MessageQueue messages;
  std::atomic<size_t> queued;
  std::atomic<size_t> dequeued;
  CephContext* const cct;
  mutable std::mutex connections_lock;
  std::thread runner;

  // TODO use rd_kafka_produce_batch for better performance
  void publish_internal(message_wrapper_t* message) {
    const std::unique_ptr<message_wrapper_t> msg_deleter(message);
    const auto conn_it = connections.find(message->conn_name);
    if (conn_it == connections.end()) {
      ldout(cct, 1) << "Kafka publish: connection was deleted while message was in the queue" << dendl;
      if (message->cb) {
        message->cb(status_to_errno(STATUS_CONNECTION_CLOSED));
      }
      return;
    }
    auto& conn = conn_it->second;

    conn->timestamp = ceph_clock_now(); 

    ceph_assert(conn->producer);
    if (conn->status != 0) {
      // connection had an issue while message was in the queue
      // TODO add error stats
      ldout(conn->cct, 1) << "Kafka publish: producer was closed while message was in the queue. with status: " << status_to_string(conn->status) << dendl;
      if (message->cb) {
        message->cb(status_to_errno(conn->status));
      }
      return;
    }

    // create a new topic unless it was already created
    auto topic_it = std::find(conn->topics.begin(), conn->topics.end(), message->topic);
    rd_kafka_topic_t* topic = nullptr;
    if (topic_it == conn->topics.end()) {
      topic = rd_kafka_topic_new(conn->producer, message->topic.c_str(), nullptr);
      if (!topic) {
        const auto err = rd_kafka_last_error();
        ldout(conn->cct, 1) << "Kafka publish: failed to create topic: " << message->topic << " error: " 
          << rd_kafka_err2str(err) << "(" << err << ")" << dendl;
        if (message->cb) {
          message->cb(-rd_kafka_err2errno(err));
        }
        return;
      }
      // TODO use the topics list as an LRU cache
      conn->topics.push_back(topic);
      ldout(conn->cct, 20) << "Kafka publish: successfully created topic: " << message->topic << dendl;
    } else {
        topic = *topic_it;
        ldout(conn->cct, 20) << "Kafka publish: reused existing topic: " << message->topic << dendl;
    }

    const auto tag = (message->cb == nullptr ? nullptr : new uint64_t(conn->delivery_tag++));
    const auto rc = rd_kafka_produce(
            topic,
            // TODO: non builtin partitioning
            RD_KAFKA_PARTITION_UA,
            // make a copy of the payload
            // so it is safe to pass the pointer from the string
            RD_KAFKA_MSG_F_COPY,
            message->message.data(),
            message->message.length(),
            // optional key and its length
            nullptr, 
            0,
            // opaque data: tag, used in the global callback
            // in order to invoke the real callback
            // null if no callback exists
            tag);
    if (rc == -1) {
      const auto err = rd_kafka_last_error();
      ldout(conn->cct, 1) << "Kafka publish: failed to produce: " << rd_kafka_err2str(err) << dendl;
      // immediatly invoke callback on error if needed
      if (message->cb) {
        message->cb(-rd_kafka_err2errno(err));
      }
      delete tag;
      return;
    }
   
    if (tag) {
      auto const q_len = conn->callbacks.size();
      if (q_len < max_inflight) {
        ldout(conn->cct, 20)
            << "Kafka publish (with callback, tag=" << *tag
            << "): OK. Queue has: " << q_len + 1 << " callbacks" << dendl;
        conn->callbacks.emplace_back(*tag, message->cb);
      } else {
        // immediately invoke callback with error - this is not a connection error
        ldout(conn->cct, 1) << "Kafka publish (with callback): failed with error: callback queue full" << dendl;
        message->cb(-EBUSY);
        // tag will be deleted when the global callback is invoked
      }
    } else {
        ldout(conn->cct, 20) << "Kafka publish (no callback): OK" << dendl;
    }
    // coverity[leaked_storage:SUPPRESS]
  }

  void run() noexcept {
    while (!stopped) {

      // publish all messages in the queue
      auto reply_count = 0U;
      const auto send_count = messages.consume_all([this](auto message){this->publish_internal(message);});
      dequeued += send_count;
      ConnectionList::iterator conn_it;
      ConnectionList::const_iterator end_it;
      {
        // thread safe access to the connection list
        // once the iterators are fetched they are guaranteed to remain valid
        std::lock_guard lock(connections_lock);
        conn_it = connections.begin();
        end_it = connections.end();
      }

      const auto read_timeout = cct->_conf->rgw_kafka_sleep_timeout;
      // loop over all connections to read acks
      for (;conn_it != end_it;) {
        
        auto& conn = conn_it->second;

        // Checking the connection idleness
        if(conn->timestamp.sec() + conn->cct->_conf->rgw_kafka_connection_idle < ceph_clock_now()) {
          ldout(conn->cct, 20) << "kafka run: deleting a connection that was idle for: " << 
            conn->cct->_conf->rgw_kafka_connection_idle << " seconds. last activity was at: " << conn->timestamp << dendl;
          std::lock_guard lock(connections_lock);
          conn->status = STATUS_CONNECTION_IDLE;
          conn_it = connections.erase(conn_it);
          --connection_count; \
          continue;
        }

        reply_count += rd_kafka_poll(conn->producer, read_timeout);

        // just increment the iterator
        ++conn_it;
      }
      // sleep if no messages were received or published across all connection
      if (send_count == 0 && reply_count == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(read_timeout*3));
        // TODO: add exponential backoff to sleep time
      }
    }
  }

  // used in the dtor for message cleanup
  static void delete_message(const message_wrapper_t* message) {
    delete message;
  }

public:
  Manager(size_t _max_connections,
      size_t _max_inflight,
      size_t _max_queue, 
      CephContext* _cct) : 
    max_connections(_max_connections),
    max_inflight(_max_inflight),
    max_queue(_max_queue),
    connection_count(0),
    stopped(false),
    connections(_max_connections),
    messages(max_queue),
    queued(0),
    dequeued(0),
    cct(_cct),
    runner(&Manager::run, this) {
      // The hashmap has "max connections" as the initial number of buckets, 
      // and allows for 10 collisions per bucket before rehash.
      // This is to prevent rehashing so that iterators are not invalidated 
      // when a new connection is added.
      connections.max_load_factor(10.0);
      // give the runner thread a name for easier debugging
      const char* thread_name = "kafka_manager";
      if (const auto rc = ceph_pthread_setname(runner.native_handle(), thread_name); rc != 0) {
        ldout(cct, 1) << "ERROR: failed to set kafka manager thread name to: " << thread_name
          << ". error: " << rc << dendl;
      }
  }

  // non copyable
  Manager(const Manager&) = delete;
  const Manager& operator=(const Manager&) = delete;

  // stop the main thread
  void stop() {
    stopped = true;
  }

  // connect to a broker, or reuse an existing connection if already connected
  bool connect(std::string& broker,
          const std::string& url, 
          bool use_ssl,
          bool verify_ssl,
          boost::optional<const std::string&> ca_location,
          boost::optional<const std::string&> mechanism,
          boost::optional<const std::string&> topic_user_name,
          boost::optional<const std::string&> topic_password) {
    if (stopped) {
      ldout(cct, 1) << "Kafka connect: manager is stopped" << dendl;
      return false;
    }

    std::string user;
    std::string password;
    if (!parse_url_authority(url, broker, user, password)) {
      // TODO: increment counter
      ldout(cct, 1) << "Kafka connect: URL parsing failed" << dendl;
      return false;
    }

    // check if username/password was already supplied via topic attributes
    // and if also provided as part of the endpoint URL issue a warning
    if (topic_user_name.has_value()) {
      if (!user.empty()) {
        ldout(cct, 5) << "Kafka connect: username provided via both topic attributes and endpoint URL: using topic attributes" << dendl;
      }
      user = topic_user_name.get();
    }
    if (topic_password.has_value()) {
      if (!password.empty()) {
        ldout(cct, 5) << "Kafka connect: password provided via both topic attributes and endpoint URL: using topic attributes" << dendl;
      }
      password = topic_password.get();
    }

    // this should be validated by the regex in parse_url()
    ceph_assert(user.empty() == password.empty());

    if (!user.empty() && !use_ssl && !g_conf().get_val<bool>("rgw_allow_notification_secrets_in_cleartext")) {
      ldout(cct, 1) << "Kafka connect: user/password are only allowed over secure connection" << dendl;
      return false;
    }

    std::lock_guard lock(connections_lock);
    const auto it = connections.find(broker);
    // note that ssl vs. non-ssl connection to the same host are two separate connections
    if (it != connections.end()) {
      // connection found - return even if non-ok
      ldout(cct, 20) << "Kafka connect: connection found" << dendl;
      return it->second.get();
    }

    // connection not found, creating a new one
    if (connection_count >= max_connections) {
      // TODO: increment counter
      ldout(cct, 1) << "Kafka connect: max connections exceeded" << dendl;
      return false;
    }

    auto conn = std::make_unique<connection_t>(cct, broker, use_ssl, verify_ssl, ca_location, user, password, mechanism);
    if (!new_producer(conn.get())) {
      ldout(cct, 10) << "Kafka connect: producer creation failed in new connection" << dendl;
      return false;
    }
    ++connection_count;
    connections.emplace(broker, std::move(conn));

    ldout(cct, 10) << "Kafka connect: new connection is created. Total connections: " << connection_count << dendl;
    return true;
  }

  // TODO publish with confirm is needed in "none" case as well, cb should be invoked publish is ok (no ack)
  int publish(const std::string& conn_name, 
    const std::string& topic,
    const std::string& message) {
    if (stopped) {
      return -ESRCH;
    }
    auto message_wrapper = std::make_unique<message_wrapper_t>(conn_name, topic, message, nullptr);
    if (messages.push(message_wrapper.get())) {
      std::ignore = message_wrapper.release();
      ++queued;
      return 0;
    }
    return -EBUSY;
  }
  
  int publish_with_confirm(const std::string& conn_name, 
    const std::string& topic,
    const std::string& message,
    reply_callback_t cb) {
    if (stopped) {
      return -ESRCH;
    }
    auto message_wrapper = std::make_unique<message_wrapper_t>(conn_name, topic, message, cb);
    if (messages.push(message_wrapper.get())) {
      std::ignore = message_wrapper.release();
      ++queued;
      return 0;
    }
    return -EBUSY;
  }

  // dtor wait for thread to stop
  // then connection are cleaned-up
  ~Manager() {
    stopped = true;
    runner.join();
    messages.consume_all(delete_message);
    std::for_each(connections.begin(), connections.end(), [](auto& conn_pair) {
        conn_pair.second->status = STATUS_CONNECTION_CLOSED;
      });
  }

  // get the number of connections
  size_t get_connection_count() const {
    return connection_count;
  }
  
  // get the number of in-flight messages
  size_t get_inflight() const {
    size_t sum = 0;
    std::lock_guard lock(connections_lock);
    std::for_each(connections.begin(), connections.end(), [&sum](auto& conn_pair) {
        sum += conn_pair.second->callbacks.size();
      });
    return sum;
  }

  // running counter of the queued messages
  size_t get_queued() const {
    return queued;
  }

  // running counter of the dequeued messages
  size_t get_dequeued() const {
    return dequeued;
  }
};

// singleton manager
// note that the manager itself is not a singleton, and multiple instances may co-exist
static Manager* s_manager = nullptr;
static std::shared_mutex s_manager_mutex;

static const size_t MAX_CONNECTIONS_DEFAULT = 256;
static const size_t MAX_INFLIGHT_DEFAULT = 8192; 
static const size_t MAX_QUEUE_DEFAULT = 8192;

bool init(CephContext* cct) {
  std::unique_lock lock(s_manager_mutex);
  if (s_manager) {
    return false;
  }
  // TODO: take conf from CephContext
  s_manager = new Manager(MAX_CONNECTIONS_DEFAULT, MAX_INFLIGHT_DEFAULT, MAX_QUEUE_DEFAULT, cct);
  return true;
}

void shutdown() {
  std::unique_lock lock(s_manager_mutex);
  delete s_manager;
  s_manager = nullptr;
}

bool connect(std::string& broker, const std::string& url, bool use_ssl, bool verify_ssl,
        boost::optional<const std::string&> ca_location,
        boost::optional<const std::string&> mechanism,
        boost::optional<const std::string&> user_name,
        boost::optional<const std::string&> password) {
  std::shared_lock lock(s_manager_mutex);
  if (!s_manager) return false;
  return s_manager->connect(broker, url, use_ssl, verify_ssl, ca_location, mechanism, user_name, password);
}

int publish(const std::string& conn_name,
    const std::string& topic,
    const std::string& message) {
  std::shared_lock lock(s_manager_mutex);
  if (!s_manager) return -ESRCH;
  return s_manager->publish(conn_name, topic, message);
}

int publish_with_confirm(const std::string& conn_name,
    const std::string& topic,
    const std::string& message,
    reply_callback_t cb) {
  std::shared_lock lock(s_manager_mutex);
  if (!s_manager) return -ESRCH;
  return s_manager->publish_with_confirm(conn_name, topic, message, cb);
}

size_t get_connection_count() {
  std::shared_lock lock(s_manager_mutex);
  if (!s_manager) return 0;
  return s_manager->get_connection_count();
}
  
size_t get_inflight() {
  std::shared_lock lock(s_manager_mutex);
  if (!s_manager) return 0;
  return s_manager->get_inflight();
}

size_t get_queued() {
  std::shared_lock lock(s_manager_mutex);
  if (!s_manager) return 0;
  return s_manager->get_queued();
}

size_t get_dequeued() {
  std::shared_lock lock(s_manager_mutex);
  if (!s_manager) return 0;
  return s_manager->get_dequeued();
}

size_t get_max_connections() {
  std::shared_lock lock(s_manager_mutex);
  if (!s_manager) return MAX_CONNECTIONS_DEFAULT;
  return s_manager->max_connections;
}

size_t get_max_inflight() {
  std::shared_lock lock(s_manager_mutex);
  if (!s_manager) return MAX_INFLIGHT_DEFAULT;
  return s_manager->max_inflight;
}

size_t get_max_queue() {
  std::shared_lock lock(s_manager_mutex);
  if (!s_manager) return MAX_QUEUE_DEFAULT;
  return s_manager->max_queue;
}

} // namespace kafka

