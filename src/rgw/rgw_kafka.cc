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

#define dout_subsys ceph_subsys_rgw

// TODO investigation, not necessarily issues:
// (1) in case of single threaded writer context use spsc_queue
// (2) check performance of emptying queue to local list, and go over the list and publish
// (3) use std::shared_mutex (c++17) or equivalent for the connections lock

// cmparisson operator between topic pointer and name
bool operator==(const rd_kafka_topic_t* rkt, const std::string& name) {
    return name == std::string_view(rd_kafka_topic_name(rkt)); 
}

namespace rgw::kafka {

// status codes for publishing
static const int STATUS_CONNECTION_CLOSED =      -0x1002;
static const int STATUS_QUEUE_FULL =             -0x1003;
static const int STATUS_MAX_INFLIGHT =           -0x1004;
static const int STATUS_MANAGER_STOPPED =        -0x1005;
static const int STATUS_CONNECTION_IDLE =        -0x1006;
// status code for connection opening
static const int STATUS_CONF_ALLOC_FAILED      = -0x2001;
static const int STATUS_CONF_REPLCACE          = -0x2002;

static const int STATUS_OK =                     0x0;

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

// struct for holding the connection state object as well as list of topics
// it is used inside an intrusive ref counted pointer (boost::intrusive_ptr)
// since references to deleted objects may still exist in the calling code
struct connection_t {
  rd_kafka_t* producer = nullptr;
  rd_kafka_conf_t* temp_conf = nullptr;
  std::vector<rd_kafka_topic_t*> topics;
  uint64_t delivery_tag = 1;
  int status = STATUS_OK;
  CephContext* const cct;
  CallbackList callbacks;
  const std::string broker;
  const bool use_ssl;
  const bool verify_ssl; // TODO currently iognored, not supported in librdkafka v0.11.6
  const boost::optional<std::string> ca_location;
  const std::string user;
  const std::string password;
  const boost::optional<std::string> mechanism;
  utime_t timestamp = ceph_clock_now();

  // cleanup of all internal connection resource
  // the object can still remain, and internal connection
  // resources created again on successful reconnection
  void destroy(int s) {
    status = s;
    // destroy temporary conf (if connection was never established)
    if (temp_conf) {
        rd_kafka_conf_destroy(temp_conf);
        return;
    }
    if (!is_ok()) {
      // no producer, nothing to destroy
      return;
    }
    // wait for all remaining acks/nacks
    rd_kafka_flush(producer, 5*1000 /* wait for max 5 seconds */);
    // destroy all topics
    std::for_each(topics.begin(), topics.end(), [](auto topic) {rd_kafka_topic_destroy(topic);});
    // destroy producer
    rd_kafka_destroy(producer);
    producer = nullptr;
    // fire all remaining callbacks (if not fired by rd_kafka_flush)
    std::for_each(callbacks.begin(), callbacks.end(), [this](auto& cb_tag) {
        cb_tag.cb(status);
        ldout(cct, 20) << "Kafka destroy: invoking callback with tag="
                       << cb_tag.tag << " for: " << broker
                       << " with status: " << status << dendl;
      });
    callbacks.clear();
    delivery_tag = 1;
    ldout(cct, 20) << "Kafka destroy: complete for: " << broker << dendl;
  }

  bool is_ok() const {
    return (producer != nullptr);
  }

  // ctor for setting immutable values
  connection_t(CephContext* _cct, const std::string& _broker, bool _use_ssl, bool _verify_ssl, 
          const boost::optional<const std::string&>& _ca_location,
          const std::string& _user, const std::string& _password, const boost::optional<const std::string&>& _mechanism) :
      cct(_cct), broker(_broker), use_ssl(_use_ssl), verify_ssl(_verify_ssl), ca_location(_ca_location), user(_user), password(_password), mechanism(_mechanism) {}                                                                                                                                                        

  // dtor also destroys the internals
  ~connection_t() {
    destroy(status);
  }
};

// convert int status to string - including RGW specific values
std::string status_to_string(int s) {
  switch (s) {
    case STATUS_OK:
        return "STATUS_OK";
    case STATUS_CONNECTION_CLOSED:
      return "RGW_KAFKA_STATUS_CONNECTION_CLOSED";
    case STATUS_QUEUE_FULL:
      return "RGW_KAFKA_STATUS_QUEUE_FULL";
    case STATUS_MAX_INFLIGHT:
      return "RGW_KAFKA_STATUS_MAX_INFLIGHT";
    case STATUS_MANAGER_STOPPED:
      return "RGW_KAFKA_STATUS_MANAGER_STOPPED";
    case STATUS_CONF_ALLOC_FAILED:
      return "RGW_KAFKA_STATUS_CONF_ALLOC_FAILED";
    case STATUS_CONF_REPLCACE:
      return "RGW_KAFKA_STATUS_CONF_REPLCACE";
    case STATUS_CONNECTION_IDLE:
      return "RGW_KAFKA_STATUS_CONNECTION_IDLE";
  }
  return std::string(rd_kafka_err2str((rd_kafka_resp_err_t)s));
}

void message_callback(rd_kafka_t* rk, const rd_kafka_message_t* rkmessage, void* opaque) {
  ceph_assert(opaque);

  const auto conn = reinterpret_cast<connection_t*>(opaque);
  const auto result = rkmessage->err;

  if (!rkmessage->_private) {
    ldout(conn->cct, 20) << "Kafka run: n/ack received, (no callback) with result=" << result << dendl;
    return;  
  }

  const auto tag = reinterpret_cast<uint64_t*>(rkmessage->_private);
  const auto& callbacks_end = conn->callbacks.end();
  const auto& callbacks_begin = conn->callbacks.begin();
  const auto tag_it = std::find(callbacks_begin, callbacks_end, *tag);
  if (tag_it != callbacks_end) {
      ldout(conn->cct, 20) << "Kafka run: n/ack received, invoking callback with tag=" << 
          *tag << " and result=" << rd_kafka_err2str(result) << dendl;
      tag_it->cb(result);
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
  conn->status = STATUS_OK; 
  char errstr[512] = {0};

  conn->temp_conf = rd_kafka_conf_new();
  if (!conn->temp_conf) {
    conn->status = STATUS_CONF_ALLOC_FAILED;
    return false;
  }

  // get list of brokers based on the bootsrap broker
  if (rd_kafka_conf_set(conn->temp_conf, "bootstrap.servers", conn->broker.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;

  if (conn->use_ssl) {
    if (!conn->user.empty()) {
      // use SSL+SASL
      if (rd_kafka_conf_set(conn->temp_conf, "security.protocol", "SASL_SSL", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
              rd_kafka_conf_set(conn->temp_conf, "sasl.username", conn->user.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
              rd_kafka_conf_set(conn->temp_conf, "sasl.password", conn->password.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;
      ldout(conn->cct, 20) << "Kafka connect: successfully configured SSL+SASL security" << dendl;

      if (conn->mechanism) {
        if (rd_kafka_conf_set(conn->temp_conf, "sasl.mechanism", conn->mechanism->c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;
        ldout(conn->cct, 20) << "Kafka connect: successfully configured SASL mechanism" << dendl;
      } else {
        if (rd_kafka_conf_set(conn->temp_conf, "sasl.mechanism", "PLAIN", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;
        ldout(conn->cct, 20) << "Kafka connect: using default SASL mechanism" << dendl;
      }

    } else {
      // use only SSL
      if (rd_kafka_conf_set(conn->temp_conf, "security.protocol", "SSL", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;
      ldout(conn->cct, 20) << "Kafka connect: successfully configured SSL security" << dendl;
    }
    if (conn->ca_location) {
      if (rd_kafka_conf_set(conn->temp_conf, "ssl.ca.location", conn->ca_location->c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;
      ldout(conn->cct, 20) << "Kafka connect: successfully configured CA location" << dendl;
    } else {
      ldout(conn->cct, 20) << "Kafka connect: using default CA location" << dendl;
    }
    // Note: when librdkafka.1.0 is available the following line could be uncommented instead of the callback setting call
    // if (rd_kafka_conf_set(conn->temp_conf, "enable.ssl.certificate.verification", "0", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;

    ldout(conn->cct, 20) << "Kafka connect: successfully configured security" << dendl;
  } else if (!conn->user.empty()) {
      // use SASL+PLAINTEXT
      if (rd_kafka_conf_set(conn->temp_conf, "security.protocol", "SASL_PLAINTEXT", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
              rd_kafka_conf_set(conn->temp_conf, "sasl.username", conn->user.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
              rd_kafka_conf_set(conn->temp_conf, "sasl.password", conn->password.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;
      ldout(conn->cct, 20) << "Kafka connect: successfully configured SASL_PLAINTEXT" << dendl;

      if (conn->mechanism) {
        if (rd_kafka_conf_set(conn->temp_conf, "sasl.mechanism", conn->mechanism->c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;
        ldout(conn->cct, 20) << "Kafka connect: successfully configured SASL mechanism" << dendl;
      } else {
        if (rd_kafka_conf_set(conn->temp_conf, "sasl.mechanism", "PLAIN", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;
        ldout(conn->cct, 20) << "Kafka connect: using default SASL mechanism" << dendl;
      }
  }

  // set the global callback for delivery success/fail
  rd_kafka_conf_set_dr_msg_cb(conn->temp_conf, message_callback);

  // set the global opaque pointer to be the connection itself
  rd_kafka_conf_set_opaque(conn->temp_conf, conn);

  // redirect kafka logs to RGW
  rd_kafka_conf_set_log_cb(conn->temp_conf, log_callback);
  // define poll callback to allow reconnect
  rd_kafka_conf_set_error_cb(conn->temp_conf, poll_err_callback);
  // create the producer
  if (conn->producer) {
    ldout(conn->cct, 5) << "Kafka connect: producer already exists. detroying the existing before creating a new one" << dendl;
    conn->destroy(STATUS_CONF_REPLCACE);
  }
  conn->producer = rd_kafka_new(RD_KAFKA_PRODUCER, conn->temp_conf, errstr, sizeof(errstr));
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

  // conf ownership passed to producer
  conn->temp_conf = nullptr;
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
  const size_t max_idle_time;
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
      ldout(cct, 1) << "Kafka publish: connection was deleted while message was in the queue. error: " << STATUS_CONNECTION_CLOSED << dendl;
      if (message->cb) {
        message->cb(STATUS_CONNECTION_CLOSED);
      }
      return;
    }
    auto& conn = conn_it->second;

    conn->timestamp = ceph_clock_now(); 

    if (!conn->is_ok()) {
      // connection had an issue while message was in the queue
      // TODO add error stats
      ldout(conn->cct, 1) << "Kafka publish: producer was closed while message was in the queue. error: " << status_to_string(conn->status) << dendl;
      if (message->cb) {
        message->cb(conn->status);
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
        ldout(conn->cct, 1) << "Kafka publish: failed to create topic: " << message->topic << " error: " << status_to_string(err) << dendl;
        if (message->cb) {
          message->cb(err);
        }
        conn->destroy(err);
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
      ldout(conn->cct, 10) << "Kafka publish: failed to produce: " << rd_kafka_err2str(err) << dendl;
      // TODO: dont error on full queue, and don't destroy connection, retry instead
      // immediatly invoke callback on error if needed
      if (message->cb) {
        message->cb(err);
      }
      conn->destroy(err);
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
        message->cb(STATUS_MAX_INFLIGHT);
        // tag will be deleted when the global callback is invoked
      }
    } else {
        ldout(conn->cct, 20) << "Kafka publish (no callback): OK" << dendl;
    }
  }

  // the managers thread:
  // (1) empty the queue of messages to be published
  // (2) loop over all connections and read acks
  // (3) manages deleted connections
  // (4) TODO reconnect on connection errors
  // (5) TODO cleanup timedout callbacks
  void run() noexcept {
    while (!stopped) {

      // publish all messages in the queue
      auto reply_count = 0U;
      const auto send_count = messages.consume_all(std::bind(&Manager::publish_internal, this, std::placeholders::_1));
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
      // loop over all connections to read acks
      for (;conn_it != end_it;) {
        
        auto& conn = conn_it->second;

        // Checking the connection idlesness
        if(conn->timestamp.sec() + max_idle_time < ceph_clock_now()) {
          ldout(conn->cct, 20) << "kafka run: deleting a connection due to idle behaviour: " << ceph_clock_now() << dendl;
          std::lock_guard lock(connections_lock);
          conn->status = STATUS_CONNECTION_IDLE;
          conn_it = connections.erase(conn_it);
          --connection_count; \
          continue;
        }

        // try to reconnect the connection if it has an error
        if (!conn->is_ok()) {
          ldout(conn->cct, 10) << "Kafka run: connection status is: " << status_to_string(conn->status) << dendl;
          const auto& broker = conn_it->first;
          ldout(conn->cct, 20) << "Kafka run: retry connection" << dendl;
          if (new_producer(conn.get()) == false) {
            ldout(conn->cct, 10) << "Kafka run: connection (" << broker << ") retry failed" << dendl;
            // TODO: add error counter for failed retries
            // TODO: add exponential backoff for retries
          } else {
            ldout(conn->cct, 10) << "Kafka run: connection (" << broker << ") retry successfull" << dendl;
          }
          ++conn_it;
          continue;
        }

        reply_count += rd_kafka_poll(conn->producer, read_timeout_ms);

        // just increment the iterator
        ++conn_it;
      }
      // if no messages were received or published
      // across all connection, sleep for 100ms
      if (send_count == 0 && reply_count == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
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
      int _read_timeout_ms,
      CephContext* _cct) : 
    max_connections(_max_connections),
    max_inflight(_max_inflight),
    max_queue(_max_queue),
    max_idle_time(30),
    connection_count(0),
    stopped(false),
    read_timeout_ms(_read_timeout_ms),
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
      const auto rc = ceph_pthread_setname(runner.native_handle(), "kafka_manager");
      ceph_assert(rc==0);
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
          boost::optional<const std::string&> mechanism) {
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

    // this should be validated by the regex in parse_url()
    ceph_assert(user.empty() == password.empty());

    if (!user.empty() && !use_ssl && !g_conf().get_val<bool>("rgw_allow_notification_secrets_in_cleartext")) {
      ldout(cct, 1) << "Kafka connect: user/password are only allowed over secure connection" << dendl;
      return false;
    }

    std::lock_guard lock(connections_lock);
    const auto it = connections.find(broker);
    // note that ssl vs. non-ssl connection to the same host are two separate conenctions
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
    // create_connection must always return a connection object
    // even if error occurred during creation. 
    // in such a case the creation will be retried in the main thread
    ++connection_count;
    ldout(cct, 10) << "Kafka connect: new connection is created. Total connections: " << connection_count << dendl;
    auto conn = connections.emplace(broker, std::make_unique<connection_t>(cct, broker, use_ssl, verify_ssl, ca_location, user, password, mechanism)).first->second.get();
    if (!new_producer(conn)) {
      ldout(cct, 10) << "Kafka connect: new connection is created. But producer creation failed. will retry" << dendl;
    }
    return true;
  }

  // TODO publish with confirm is needed in "none" case as well, cb should be invoked publish is ok (no ack)
  int publish(const std::string& conn_name, 
    const std::string& topic,
    const std::string& message) {
    if (stopped) {
      return STATUS_MANAGER_STOPPED;
    }
    if (messages.push(new message_wrapper_t(conn_name, topic, message, nullptr))) {
      ++queued;
      return STATUS_OK;
    }
    return STATUS_QUEUE_FULL;
  }
  
  int publish_with_confirm(const std::string& conn_name, 
    const std::string& topic,
    const std::string& message,
    reply_callback_t cb) {
    if (stopped) {
      return STATUS_MANAGER_STOPPED;
    }
    if (messages.push(new message_wrapper_t(conn_name, topic, message, cb))) {
      ++queued;
      return STATUS_OK;
    }
    return STATUS_QUEUE_FULL;
  }

  // dtor wait for thread to stop
  // then connection are cleaned-up
  ~Manager() {
    stopped = true;
    runner.join();
    messages.consume_all(delete_message);
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
// TODO make the pointer atomic in allocation and deallocation to avoid race conditions
static Manager* s_manager = nullptr;

static const size_t MAX_CONNECTIONS_DEFAULT = 256;
static const size_t MAX_INFLIGHT_DEFAULT = 8192; 
static const size_t MAX_QUEUE_DEFAULT = 8192;
static const int READ_TIMEOUT_MS_DEFAULT = 500;

bool init(CephContext* cct) {
  if (s_manager) {
    return false;
  }
  // TODO: take conf from CephContext
  s_manager = new Manager(MAX_CONNECTIONS_DEFAULT, MAX_INFLIGHT_DEFAULT, MAX_QUEUE_DEFAULT, READ_TIMEOUT_MS_DEFAULT, cct);
  return true;
}

void shutdown() {
  delete s_manager;
  s_manager = nullptr;
}

bool connect(std::string& broker, const std::string& url, bool use_ssl, bool verify_ssl,
        boost::optional<const std::string&> ca_location,
        boost::optional<const std::string&> mechanism) {
  if (!s_manager) return false;
  return s_manager->connect(broker, url, use_ssl, verify_ssl, ca_location, mechanism);
}

int publish(const std::string& conn_name,
    const std::string& topic,
    const std::string& message) {
  if (!s_manager) return STATUS_MANAGER_STOPPED;
  return s_manager->publish(conn_name, topic, message);
}

int publish_with_confirm(const std::string& conn_name,
    const std::string& topic,
    const std::string& message,
    reply_callback_t cb) {
  if (!s_manager) return STATUS_MANAGER_STOPPED;
  return s_manager->publish_with_confirm(conn_name, topic, message, cb);
}

size_t get_connection_count() {
  if (!s_manager) return 0;
  return s_manager->get_connection_count();
}
  
size_t get_inflight() {
  if (!s_manager) return 0;
  return s_manager->get_inflight();
}

size_t get_queued() {
  if (!s_manager) return 0;
  return s_manager->get_queued();
}

size_t get_dequeued() {
  if (!s_manager) return 0;
  return s_manager->get_dequeued();
}

size_t get_max_connections() {
  if (!s_manager) return MAX_CONNECTIONS_DEFAULT;
  return s_manager->max_connections;
}

size_t get_max_inflight() {
  if (!s_manager) return MAX_INFLIGHT_DEFAULT;
  return s_manager->max_inflight;
}

size_t get_max_queue() {
  if (!s_manager) return MAX_QUEUE_DEFAULT;
  return s_manager->max_queue;
}

} // namespace kafka

