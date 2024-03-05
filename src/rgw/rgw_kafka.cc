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
// TODO: use the actual error code (when conn exists) instead of STATUS_CONNECTION_CLOSED when replying to client
static const int STATUS_CONNECTION_CLOSED =      -0x1002;
static const int STATUS_QUEUE_FULL =             -0x1003;
static const int STATUS_MAX_INFLIGHT =           -0x1004;
static const int STATUS_MANAGER_STOPPED =        -0x1005;
// status code for connection opening
static const int STATUS_CONF_ALLOC_FAILED      = -0x2001;

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
  mutable std::atomic<int> ref_count = 0;
  CephContext* const cct;
  CallbackList callbacks;
  const std::string broker;
  const bool use_ssl;
  const bool verify_ssl; // TODO currently iognored, not supported in librdkafka v0.11.6
  const boost::optional<std::string> ca_location;
  const std::string user;
  const std::string password;
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
    // wait for all remaining acks/nacks
    rd_kafka_flush(producer, 5*1000 /* wait for max 5 seconds */);
    // destroy all topics
    std::for_each(topics.begin(), topics.end(), [](auto topic) {rd_kafka_topic_destroy(topic);});
    // destroy producer
    rd_kafka_destroy(producer);
    // fire all remaining callbacks (if not fired by rd_kafka_flush)
    std::for_each(callbacks.begin(), callbacks.end(), [this](auto& cb_tag) {
        cb_tag.cb(status);
        ldout(cct, 20) << "Kafka destroy: invoking callback with tag=" << cb_tag.tag << dendl;
      });
    callbacks.clear();
    delivery_tag = 1;
  }

  bool is_ok() const {
    return (producer != nullptr);
  }

  // ctor for setting immutable values
  connection_t(CephContext* _cct, const std::string& _broker, bool _use_ssl, bool _verify_ssl, 
          const boost::optional<const std::string&>& _ca_location,
          const std::string& _user, const std::string& _password) :
      cct(_cct), broker(_broker), use_ssl(_use_ssl), verify_ssl(_verify_ssl), ca_location(_ca_location), user(_user), password(_password) {}

  // dtor also destroys the internals
  ~connection_t() {
    destroy(STATUS_CONNECTION_CLOSED);
  }

  friend void intrusive_ptr_add_ref(const connection_t* p);
  friend void intrusive_ptr_release(const connection_t* p);
};

std::string to_string(const connection_ptr_t& conn) {
    std::string str;
    str += "\nBroker: " + conn->broker; 
    str += conn->use_ssl ? "\nUse SSL" : ""; 
    str += conn->ca_location ? "\nCA Location: " + *(conn->ca_location) : "";
    return str;
}
// these are required interfaces so that connection_t could be used inside boost::intrusive_ptr
void intrusive_ptr_add_ref(const connection_t* p) {
  ++p->ref_count;
}
void intrusive_ptr_release(const connection_t* p) {
  if (--p->ref_count == 0) {
    delete p;
  }
}

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

// utility function to create a connection, when the connection object already exists
connection_ptr_t& create_connection(connection_ptr_t& conn) {
  // pointer must be valid and not marked for deletion
  ceph_assert(conn);
  
  // reset all status codes
  conn->status = STATUS_OK; 
  char errstr[512] = {0};

  conn->temp_conf = rd_kafka_conf_new();
  if (!conn->temp_conf) {
    conn->status = STATUS_CONF_ALLOC_FAILED;
    return conn;
  }

  // set message timeout
  // according to documentation, value of zero will expire the message based on retries.
  // however, testing with librdkafka v1.6.1 did not expire the message in that case. hence, a value of zero is changed to 1ms
  constexpr std::uint64_t min_message_timeout = 1;
  const auto message_timeout = std::max(min_message_timeout, conn->cct->_conf->rgw_kafka_message_timeout);
  if (rd_kafka_conf_set(conn->temp_conf, "message.timeout.ms", 
        std::to_string(message_timeout).c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;
  // get list of brokers based on the bootstrap broker
  if (rd_kafka_conf_set(conn->temp_conf, "bootstrap.servers", conn->broker.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;

  if (conn->use_ssl) {
    if (!conn->user.empty()) {
      // use SSL+SASL
      if (rd_kafka_conf_set(conn->temp_conf, "security.protocol", "SASL_SSL", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
              rd_kafka_conf_set(conn->temp_conf, "sasl.mechanism", "PLAIN", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
              rd_kafka_conf_set(conn->temp_conf, "sasl.username", conn->user.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
              rd_kafka_conf_set(conn->temp_conf, "sasl.password", conn->password.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) goto conf_error;
      ldout(conn->cct, 20) << "Kafka connect: successfully configured SSL+SASL security" << dendl;
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
  }

  // set the global callback for delivery success/fail
  rd_kafka_conf_set_dr_msg_cb(conn->temp_conf, message_callback);

  // set the global opaque pointer to be the connection itself
  rd_kafka_conf_set_opaque(conn->temp_conf, conn.get());

  // create the producer
  conn->producer = rd_kafka_new(RD_KAFKA_PRODUCER, conn->temp_conf, errstr, sizeof(errstr));
  if (!conn->producer) {
    conn->status = rd_kafka_last_error();
    ldout(conn->cct, 1) << "Kafka connect: failed to create producer: " << errstr << dendl;
    return conn;
  }
  ldout(conn->cct, 20) << "Kafka connect: successfully created new producer" << dendl;

  // conf ownership passed to producer
  conn->temp_conf = nullptr;
  return conn;

conf_error:
  conn->status = rd_kafka_last_error();
  ldout(conn->cct, 1) << "Kafka connect: configuration failed: " << errstr << dendl;
  return conn;
}

// utility function to create a new connection
connection_ptr_t create_new_connection(const std::string& broker, CephContext* cct,
        bool use_ssl,
        bool verify_ssl,
        boost::optional<const std::string&> ca_location, 
        const std::string& user, 
        const std::string& password) { 
  // create connection state
  connection_ptr_t conn(new connection_t(cct, broker, use_ssl, verify_ssl, ca_location, user, password));
  return create_connection(conn);
}

/// struct used for holding messages in the message queue
struct message_wrapper_t {
  connection_ptr_t conn; 
  std::string topic;
  std::string message;
  reply_callback_t cb;
  
  message_wrapper_t(connection_ptr_t& _conn,
      const std::string& _topic,
      const std::string& _message,
      reply_callback_t _cb) : conn(_conn), topic(_topic), message(_message), cb(_cb) {}
};

typedef std::unordered_map<std::string, connection_ptr_t> ConnectionList;
typedef boost::lockfree::queue<message_wrapper_t*, boost::lockfree::fixed_sized<true>> MessageQueue;

// macros used inside a loop where an iterator is either incremented or erased
#define INCREMENT_AND_CONTINUE(IT) \
          ++IT; \
          continue;

#define ERASE_AND_CONTINUE(IT,CONTAINER) \
          IT=CONTAINER.erase(IT); \
          --connection_count; \
          continue;

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
    const std::unique_ptr<message_wrapper_t> msg_owner(message);
    auto& conn = message->conn;

    conn->timestamp = ceph_clock_now(); 

    if (!conn->is_ok()) {
      // connection had an issue while message was in the queue
      // TODO add error stats
      ldout(conn->cct, 1) << "Kafka publish: connection had an issue while message was in the queue. error: " << status_to_string(conn->status) << dendl;
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
        ldout(conn->cct, 20) << "Kafka publish (with callback, tag=" << *tag << "): OK. Queue has: " << q_len << " callbacks" << dendl;
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
        if(conn->timestamp.sec() + conn->cct->_conf->rgw_kafka_connection_idle < ceph_clock_now()) {
          ldout(conn->cct, 20) << "Time for deleting a connection due to idle behaviour: " << ceph_clock_now() << dendl;
          ERASE_AND_CONTINUE(conn_it, connections);
        }

        // try to reconnect the connection if it has an error
        if (!conn->is_ok()) {
          ldout(conn->cct, 10) << "Kafka run: connection status is: " << status_to_string(conn->status) << dendl;
          const auto& broker = conn_it->first;
          ldout(conn->cct, 20) << "Kafka run: retry connection" << dendl;
          if (create_connection(conn)->is_ok() == false) {
            ldout(conn->cct, 10) << "Kafka run: connection (" << broker << ") retry failed" << dendl;
            // TODO: add error counter for failed retries
            // TODO: add exponential backoff for retries
          } else {
            ldout(conn->cct, 10) << "Kafka run: connection (" << broker << ") retry successfull" << dendl;
          }
          INCREMENT_AND_CONTINUE(conn_it);
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
  connection_ptr_t connect(const std::string& url, 
          bool use_ssl,
          bool verify_ssl,
          boost::optional<const std::string&> ca_location) {
    if (stopped) {
      // TODO: increment counter
      ldout(cct, 1) << "Kafka connect: manager is stopped" << dendl;
      return nullptr;
    }

    std::string broker;
	std::string user;
	std::string password;
    if (!parse_url_authority(url, broker, user, password)) {
      // TODO: increment counter
      ldout(cct, 1) << "Kafka connect: URL parsing failed" << dendl;
      return nullptr;
    }

    // this should be validated by the regex in parse_url()
    ceph_assert(user.empty() == password.empty());

	if (!user.empty() && !use_ssl) {
      ldout(cct, 1) << "Kafka connect: user/password are only allowed over secure connection" << dendl;
      return nullptr;
	}

    std::lock_guard lock(connections_lock);
    const auto it = connections.find(broker);
    // note that ssl vs. non-ssl connection to the same host are two separate conenctions
    if (it != connections.end()) {
      // connection found - return even if non-ok
      ldout(cct, 20) << "Kafka connect: connection found" << dendl;
      return it->second;
    }

    // connection not found, creating a new one
    if (connection_count >= max_connections) {
      // TODO: increment counter
      ldout(cct, 1) << "Kafka connect: max connections exceeded" << dendl;
      return nullptr;
    }
    const auto conn = create_new_connection(broker, cct, use_ssl, verify_ssl, ca_location, user, password);
    // create_new_connection must always return a connection object
    // even if error occurred during creation. 
    // in such a case the creation will be retried in the main thread
    ceph_assert(conn);
    ++connection_count;
    ldout(cct, 10) << "Kafka connect: new connection is created. Total connections: " << connection_count << dendl;
    return connections.emplace(broker, conn).first->second;
  }

  // TODO publish with confirm is needed in "none" case as well, cb should be invoked publish is ok (no ack)
  int publish(connection_ptr_t& conn, 
    const std::string& topic,
    const std::string& message) {
    if (stopped) {
      return STATUS_MANAGER_STOPPED;
    }
    if (!conn || !conn->is_ok()) {
      return STATUS_CONNECTION_CLOSED;
    }
    if (messages.push(new message_wrapper_t(conn, topic, message, nullptr))) {
      ++queued;
      return STATUS_OK;
    }
    return STATUS_QUEUE_FULL;
  }
  
  int publish_with_confirm(connection_ptr_t& conn, 
    const std::string& topic,
    const std::string& message,
    reply_callback_t cb) {
    if (stopped) {
      return STATUS_MANAGER_STOPPED;
    }
    if (!conn || !conn->is_ok()) {
      return STATUS_CONNECTION_CLOSED;
    }
    if (messages.push(new message_wrapper_t(conn, topic, message, cb))) {
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

connection_ptr_t connect(const std::string& url, bool use_ssl, bool verify_ssl,
        boost::optional<const std::string&> ca_location) {
  if (!s_manager) return nullptr;
  return s_manager->connect(url, use_ssl, verify_ssl, ca_location);
}

int publish(connection_ptr_t& conn, 
    const std::string& topic,
    const std::string& message) {
  if (!s_manager) return STATUS_MANAGER_STOPPED;
  return s_manager->publish(conn, topic, message);
}

int publish_with_confirm(connection_ptr_t& conn, 
    const std::string& topic,
    const std::string& message,
    reply_callback_t cb) {
  if (!s_manager) return STATUS_MANAGER_STOPPED;
  return s_manager->publish_with_confirm(conn, topic, message, cb);
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

