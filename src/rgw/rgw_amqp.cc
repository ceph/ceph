// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_amqp.h"
#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>
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
// (2) support multiple channels
// (3) check performance of emptying queue to local list, and go over the list and publish
// (4) use std::shared_mutex (c++17) or equivalent for the connections lock

namespace rgw::amqp {

// RGW AMQP status codes for publishing
static const int RGW_AMQP_STATUS_BROKER_NACK =            -0x1001;
static const int RGW_AMQP_STATUS_CONNECTION_CLOSED =      -0x1002;
static const int RGW_AMQP_STATUS_QUEUE_FULL =             -0x1003;
static const int RGW_AMQP_STATUS_MAX_INFLIGHT =           -0x1004;
static const int RGW_AMQP_STATUS_MANAGER_STOPPED =        -0x1005;
// RGW AMQP status code for connection opening
static const int RGW_AMQP_STATUS_CONN_ALLOC_FAILED =      -0x2001;
static const int RGW_AMQP_STATUS_SOCKET_ALLOC_FAILED =    -0x2002;
static const int RGW_AMQP_STATUS_SOCKET_OPEN_FAILED =     -0x2003;
static const int RGW_AMQP_STATUS_LOGIN_FAILED =           -0x2004;
static const int RGW_AMQP_STATUS_CHANNEL_OPEN_FAILED =    -0x2005;
static const int RGW_AMQP_STATUS_VERIFY_EXCHANGE_FAILED = -0x2006;
static const int RGW_AMQP_STATUS_Q_DECLARE_FAILED =       -0x2007;
static const int RGW_AMQP_STATUS_CONFIRM_DECLARE_FAILED = -0x2008;
static const int RGW_AMQP_STATUS_CONSUME_DECLARE_FAILED = -0x2009;

static const int RGW_AMQP_RESPONSE_SOCKET_ERROR =         -0x3008;
static const int RGW_AMQP_NO_REPLY_CODE =                 0x0;

// key class for the connection list
struct connection_id_t {
  const std::string host;
  const int port;
  const std::string vhost;
  // constructed from amqp_connection_info struct
  connection_id_t(const amqp_connection_info& info) 
    : host(info.host), port(info.port), vhost(info.vhost) {}

  // equality operator and hasher functor are needed 
  // so that connection_id_t could be used as key in unordered_map
  bool operator==(const connection_id_t& other) const {
    return host == other.host && port == other.port && vhost == other.vhost;
  }
  
  struct hasher {
    std::size_t operator()(const connection_id_t& k) const {
       return ((std::hash<std::string>()(k.host)
             ^ (std::hash<int>()(k.port) << 1)) >> 1)
             ^ (std::hash<std::string>()(k.vhost) << 1); 
    }
  };
};

std::string to_string(const connection_id_t& id) {
    return id.host+":"+std::to_string(id.port)+id.vhost;
}

// connection_t state cleaner
// could be used for automatic cleanup when getting out of scope
class ConnectionCleaner {
  private:
    amqp_connection_state_t conn;
  public:
    ConnectionCleaner(amqp_connection_state_t _conn) : conn(_conn) {}
    ~ConnectionCleaner() {
      if (conn) {
        amqp_destroy_connection(conn);
      }
    }
    // call reset() if cleanup is not needed anymore
    void reset() {
      conn = nullptr;
    }
};

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

// struct for holding the connection state object as well as the exchange
// it is used inside an intrusive ref counted pointer (boost::intrusive_ptr)
// since references to deleted objects may still exist in the calling code
struct connection_t {
  amqp_connection_state_t state;
  std::string exchange;
  std::string user;
  std::string password;
  amqp_bytes_t reply_to_queue;
  bool marked_for_deletion;
  uint64_t delivery_tag;
  int status;
  int reply_type;
  int reply_code;
  mutable std::atomic<int> ref_count;
  CephContext* cct;
  CallbackList callbacks;
  ceph::coarse_real_clock::time_point next_reconnect;
  bool mandatory;

  // default ctor
  connection_t() :
    state(nullptr),
    reply_to_queue(amqp_empty_bytes),
    marked_for_deletion(false),
    delivery_tag(1),
    status(AMQP_STATUS_OK),
    reply_type(AMQP_RESPONSE_NORMAL),
    reply_code(RGW_AMQP_NO_REPLY_CODE),
    ref_count(0),
    cct(nullptr),
    next_reconnect(ceph::coarse_real_clock::now()),
    mandatory(false)
  {}

  // cleanup of all internal connection resource
  // the object can still remain, and internal connection
  // resources created again on successful reconnection
  void destroy(int s) {
    status = s;
    ConnectionCleaner clean_state(state);
    state = nullptr;
    amqp_bytes_free(reply_to_queue);
    reply_to_queue = amqp_empty_bytes;
    // fire all remaining callbacks
    std::for_each(callbacks.begin(), callbacks.end(), [this](auto& cb_tag) {
        cb_tag.cb(status);
        ldout(cct, 20) << "AMQP destroy: invoking callback with tag=" << cb_tag.tag << dendl;
      });
    callbacks.clear();
    delivery_tag = 1;
  }

  bool is_ok() const {
    return (state != nullptr && !marked_for_deletion);
  }

  // dtor also destroys the internals
  ~connection_t() {
    destroy(RGW_AMQP_STATUS_CONNECTION_CLOSED);
  }

  friend void intrusive_ptr_add_ref(const connection_t* p);
  friend void intrusive_ptr_release(const connection_t* p);
};

// these are required interfaces so that connection_t could be used inside boost::intrusive_ptr
void intrusive_ptr_add_ref(const connection_t* p) {
  ++p->ref_count;
}
void intrusive_ptr_release(const connection_t* p) {
  if (--p->ref_count == 0) {
    delete p;
  }
}

// convert connection info to string
std::string to_string(const amqp_connection_info& info) {
  std::stringstream ss;
  ss << "connection info:" <<
        "\nHost: " << info.host <<
        "\nPort: " << info.port <<
        "\nUser: " << info.user << 
        "\nPassword: " << info.password <<
        "\nvhost: " << info.vhost <<
        "\nSSL support: " << info.ssl << std::endl;
  return ss.str();
}

// convert reply to error code
int reply_to_code(const amqp_rpc_reply_t& reply) {
  switch (reply.reply_type) {
    case AMQP_RESPONSE_NONE:
    case AMQP_RESPONSE_NORMAL:
      return RGW_AMQP_NO_REPLY_CODE;
    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
      return reply.library_error;
    case AMQP_RESPONSE_SERVER_EXCEPTION: 
      if (reply.reply.decoded) {
        const amqp_connection_close_t* m = (amqp_connection_close_t*)reply.reply.decoded;
        return m->reply_code;
      }
      return reply.reply.id;
  }
  return RGW_AMQP_NO_REPLY_CODE;
}

// convert reply to string
std::string to_string(const amqp_rpc_reply_t& reply) {
  std::stringstream ss;
  switch (reply.reply_type) {
    case AMQP_RESPONSE_NORMAL:
      return "";
    case AMQP_RESPONSE_NONE:
      return "missing RPC reply type";
    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
      return amqp_error_string2(reply.library_error);
    case AMQP_RESPONSE_SERVER_EXCEPTION: 
      {
        switch (reply.reply.id) {
          case AMQP_CONNECTION_CLOSE_METHOD:
            ss << "server connection error: ";
            break;
          case AMQP_CHANNEL_CLOSE_METHOD:
            ss << "server channel error: ";
            break;
          default:
            ss << "server unknown error: ";
            break;
        }
        if (reply.reply.decoded) {
          amqp_connection_close_t* m = (amqp_connection_close_t*)reply.reply.decoded;
          ss << m->reply_code << " text: " << std::string((char*)m->reply_text.bytes, m->reply_text.len);
        }
        return ss.str();
      }
    default:
      ss << "unknown error, method id: " << reply.reply.id;
      return ss.str();
  }
}

// convert status enum to string
std::string to_string(amqp_status_enum s) {
  switch (s) {
    case AMQP_STATUS_OK:
      return "AMQP_STATUS_OK";
    case AMQP_STATUS_NO_MEMORY:
      return "AMQP_STATUS_NO_MEMORY";
    case AMQP_STATUS_BAD_AMQP_DATA:
      return "AMQP_STATUS_BAD_AMQP_DATA";
    case AMQP_STATUS_UNKNOWN_CLASS:
      return "AMQP_STATUS_UNKNOWN_CLASS";
    case AMQP_STATUS_UNKNOWN_METHOD:
      return "AMQP_STATUS_UNKNOWN_METHOD";
    case AMQP_STATUS_HOSTNAME_RESOLUTION_FAILED:
      return "AMQP_STATUS_HOSTNAME_RESOLUTION_FAILED";
    case AMQP_STATUS_INCOMPATIBLE_AMQP_VERSION:
      return "AMQP_STATUS_INCOMPATIBLE_AMQP_VERSION";
    case AMQP_STATUS_CONNECTION_CLOSED:
      return "AMQP_STATUS_CONNECTION_CLOSED";
    case AMQP_STATUS_BAD_URL:
      return "AMQP_STATUS_BAD_URL";
    case AMQP_STATUS_SOCKET_ERROR:
      return "AMQP_STATUS_SOCKET_ERROR";
    case AMQP_STATUS_INVALID_PARAMETER:
      return "AMQP_STATUS_INVALID_PARAMETER"; 
    case AMQP_STATUS_TABLE_TOO_BIG:
      return "AMQP_STATUS_TABLE_TOO_BIG";
    case AMQP_STATUS_WRONG_METHOD:
      return "AMQP_STATUS_WRONG_METHOD";
    case AMQP_STATUS_TIMEOUT:
      return "AMQP_STATUS_TIMEOUT";
    case AMQP_STATUS_TIMER_FAILURE:
      return "AMQP_STATUS_TIMER_FAILURE";
    case AMQP_STATUS_HEARTBEAT_TIMEOUT:
      return "AMQP_STATUS_HEARTBEAT_TIMEOUT";
    case AMQP_STATUS_UNEXPECTED_STATE:
      return "AMQP_STATUS_UNEXPECTED_STATE";
    case AMQP_STATUS_SOCKET_CLOSED:
      return "AMQP_STATUS_SOCKET_CLOSED";
    case AMQP_STATUS_SOCKET_INUSE:
      return "AMQP_STATUS_SOCKET_INUSE";
    case AMQP_STATUS_BROKER_UNSUPPORTED_SASL_METHOD:
      return "AMQP_STATUS_BROKER_UNSUPPORTED_SASL_METHOD";
#if AMQP_VERSION >= AMQP_VERSION_CODE(0, 8, 0, 0)
    case AMQP_STATUS_UNSUPPORTED:
      return "AMQP_STATUS_UNSUPPORTED";
#endif
    case _AMQP_STATUS_NEXT_VALUE:
      return "AMQP_STATUS_INTERNAL"; 
    case AMQP_STATUS_TCP_ERROR:
        return "AMQP_STATUS_TCP_ERROR";
    case AMQP_STATUS_TCP_SOCKETLIB_INIT_ERROR:
      return "AMQP_STATUS_TCP_SOCKETLIB_INIT_ERROR";
    case _AMQP_STATUS_TCP_NEXT_VALUE:
      return "AMQP_STATUS_INTERNAL"; 
    case AMQP_STATUS_SSL_ERROR:
      return "AMQP_STATUS_SSL_ERROR";
    case AMQP_STATUS_SSL_HOSTNAME_VERIFY_FAILED:
      return "AMQP_STATUS_SSL_HOSTNAME_VERIFY_FAILED";
    case AMQP_STATUS_SSL_PEER_VERIFY_FAILED:
      return "AMQP_STATUS_SSL_PEER_VERIFY_FAILED";
    case AMQP_STATUS_SSL_CONNECTION_FAILED:
      return "AMQP_STATUS_SSL_CONNECTION_FAILED";
    case _AMQP_STATUS_SSL_NEXT_VALUE:
      return "AMQP_STATUS_INTERNAL"; 
  }
  return "AMQP_STATUS_UNKNOWN";
}

// TODO: add status_to_string on the connection object to prinf full status

// convert int status to string - including RGW specific values
std::string status_to_string(int s) {
  switch (s) {
    case RGW_AMQP_STATUS_BROKER_NACK:
      return "RGW_AMQP_STATUS_BROKER_NACK";
    case RGW_AMQP_STATUS_CONNECTION_CLOSED:
      return "RGW_AMQP_STATUS_CONNECTION_CLOSED";
    case RGW_AMQP_STATUS_QUEUE_FULL:
      return "RGW_AMQP_STATUS_QUEUE_FULL";
    case RGW_AMQP_STATUS_MAX_INFLIGHT:
      return "RGW_AMQP_STATUS_MAX_INFLIGHT";
    case RGW_AMQP_STATUS_MANAGER_STOPPED:
      return "RGW_AMQP_STATUS_MANAGER_STOPPED";
    case RGW_AMQP_STATUS_CONN_ALLOC_FAILED:
      return "RGW_AMQP_STATUS_CONN_ALLOC_FAILED";
    case RGW_AMQP_STATUS_SOCKET_ALLOC_FAILED:
      return "RGW_AMQP_STATUS_SOCKET_ALLOC_FAILED";
    case RGW_AMQP_STATUS_SOCKET_OPEN_FAILED:
      return "RGW_AMQP_STATUS_SOCKET_OPEN_FAILED";
    case RGW_AMQP_STATUS_LOGIN_FAILED:
      return "RGW_AMQP_STATUS_LOGIN_FAILED";
    case RGW_AMQP_STATUS_CHANNEL_OPEN_FAILED:
      return "RGW_AMQP_STATUS_CHANNEL_OPEN_FAILED";
    case RGW_AMQP_STATUS_VERIFY_EXCHANGE_FAILED:
      return "RGW_AMQP_STATUS_VERIFY_EXCHANGE_FAILED";
    case RGW_AMQP_STATUS_Q_DECLARE_FAILED:
      return "RGW_AMQP_STATUS_Q_DECLARE_FAILED";
    case RGW_AMQP_STATUS_CONFIRM_DECLARE_FAILED:
      return "RGW_AMQP_STATUS_CONFIRM_DECLARE_FAILED";
    case RGW_AMQP_STATUS_CONSUME_DECLARE_FAILED:
      return "RGW_AMQP_STATUS_CONSUME_DECLARE_FAILED";
  }
  return to_string((amqp_status_enum)s);
}

// check the result from calls and return if error (=null)
#define RETURN_ON_ERROR(C, S, OK) \
  if (!OK) { \
    C->status = S; \
    return C; \
  }

// in case of RPC calls, getting the RPC reply and return if an error is detected
#define RETURN_ON_REPLY_ERROR(C, ST, S) { \
      const auto reply = amqp_get_rpc_reply(ST); \
      if (reply.reply_type != AMQP_RESPONSE_NORMAL) { \
        C->status = S; \
        C->reply_type = reply.reply_type; \
        C->reply_code = reply_to_code(reply); \
        return C; \
      } \
    }

static const amqp_channel_t CHANNEL_ID = 1;
static const amqp_channel_t CONFIRMING_CHANNEL_ID = 2;

// utility function to create a connection, when the connection object already exists
connection_ptr_t& create_connection(connection_ptr_t& conn, const amqp_connection_info& info) {
  // pointer must be valid and not marked for deletion
  ceph_assert(conn && !conn->marked_for_deletion);
  
  // reset all status codes
  conn->status = AMQP_STATUS_OK; 
  conn->reply_type = AMQP_RESPONSE_NORMAL;
  conn->reply_code = RGW_AMQP_NO_REPLY_CODE;

  auto state = amqp_new_connection();
  if (!state) {
    conn->status = RGW_AMQP_STATUS_CONN_ALLOC_FAILED;
    return conn;
  }
  // make sure that the connection state is cleaned up in case of error
  ConnectionCleaner state_guard(state);

  // create and open socket
  auto socket = amqp_tcp_socket_new(state);
  if (!socket) {
    conn->status = RGW_AMQP_STATUS_SOCKET_ALLOC_FAILED;
    return conn;
  }
  const auto s = amqp_socket_open(socket, info.host, info.port);
  if (s < 0) {
    conn->status = RGW_AMQP_STATUS_SOCKET_OPEN_FAILED;
    conn->reply_type = RGW_AMQP_RESPONSE_SOCKET_ERROR;
    conn->reply_code = s;
    return conn;
  }

  // login to broker
  const auto reply = amqp_login(state,
      info.vhost, 
      AMQP_DEFAULT_MAX_CHANNELS,
      AMQP_DEFAULT_FRAME_SIZE,
      0,                        // no heartbeat TODO: add conf
      AMQP_SASL_METHOD_PLAIN,   // TODO: add other types of security
      info.user,
      info.password);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    conn->status = RGW_AMQP_STATUS_LOGIN_FAILED;
    conn->reply_type = reply.reply_type;
    conn->reply_code = reply_to_code(reply);
    return conn;
  }

  // open channels
  {
    const auto ok = amqp_channel_open(state, CHANNEL_ID);
    RETURN_ON_ERROR(conn, RGW_AMQP_STATUS_CHANNEL_OPEN_FAILED, ok);
    RETURN_ON_REPLY_ERROR(conn, state, RGW_AMQP_STATUS_CHANNEL_OPEN_FAILED);
  }
  {
    const auto ok = amqp_channel_open(state, CONFIRMING_CHANNEL_ID);
    RETURN_ON_ERROR(conn, RGW_AMQP_STATUS_CHANNEL_OPEN_FAILED, ok);
    RETURN_ON_REPLY_ERROR(conn, state, RGW_AMQP_STATUS_CHANNEL_OPEN_FAILED);
  }
  {
    const auto ok = amqp_confirm_select(state, CONFIRMING_CHANNEL_ID);
    RETURN_ON_ERROR(conn, RGW_AMQP_STATUS_CONFIRM_DECLARE_FAILED, ok);
    RETURN_ON_REPLY_ERROR(conn, state, RGW_AMQP_STATUS_CONFIRM_DECLARE_FAILED);
  }

  // verify that the topic exchange is there
  // TODO: make this step optional
  {
    const auto ok = amqp_exchange_declare(state, 
      CHANNEL_ID,
      amqp_cstring_bytes(conn->exchange.c_str()),
      amqp_cstring_bytes("topic"),
      1, // passive - exchange must already exist on broker
      1, // durable
      0, // dont auto-delete
      0, // not internal
      amqp_empty_table);
    RETURN_ON_ERROR(conn, RGW_AMQP_STATUS_VERIFY_EXCHANGE_FAILED, ok);
    RETURN_ON_REPLY_ERROR(conn, state, RGW_AMQP_STATUS_VERIFY_EXCHANGE_FAILED);
  }
  {
    // create queue for confirmations
    const auto queue_ok = amqp_queue_declare(state, 
        CHANNEL_ID,         // use the regular channel for this call
        amqp_empty_bytes,   // let broker allocate queue name
        0,                  // not passive - create the queue 
        0,                  // not durable 
        1,                  // exclusive 
        1,                  // auto-delete
        amqp_empty_table    // not args TODO add args from conf: TTL, max length etc.
        );
    RETURN_ON_ERROR(conn, RGW_AMQP_STATUS_Q_DECLARE_FAILED, queue_ok);
    RETURN_ON_REPLY_ERROR(conn, state, RGW_AMQP_STATUS_Q_DECLARE_FAILED);

    // define consumption for connection
    const auto consume_ok = amqp_basic_consume(state, 
        CONFIRMING_CHANNEL_ID, 
        queue_ok->queue,
        amqp_empty_bytes, // broker will generate consumer tag
        1,                // messages sent from client are never routed back
        1,                // client does not ack thr acks
        1,                // exclusive access to queue
        amqp_empty_table  // no parameters
        );

    RETURN_ON_ERROR(conn, RGW_AMQP_STATUS_CONSUME_DECLARE_FAILED, consume_ok);
    RETURN_ON_REPLY_ERROR(conn, state, RGW_AMQP_STATUS_CONSUME_DECLARE_FAILED);
    // broker generated consumer_tag could be used to cancel sending of n/acks from broker - not needed
    
    state_guard.reset();
    conn->state = state;
    conn->reply_to_queue = amqp_bytes_malloc_dup(queue_ok->queue);
    return conn;
  }
}

// utility function to create a new connection
connection_ptr_t create_new_connection(const amqp_connection_info& info, 
    const std::string& exchange, bool mandatory_delivery, CephContext* cct) { 
  // create connection state
  connection_ptr_t conn = new connection_t;
  conn->exchange = exchange;
  conn->user.assign(info.user);
  conn->password.assign(info.password);
  conn->mandatory = mandatory_delivery;
  conn->cct = cct;
  return create_connection(conn, info);
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


typedef std::unordered_map<connection_id_t, connection_ptr_t, connection_id_t::hasher> ConnectionList;
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
  struct timeval read_timeout;
  ConnectionList connections;
  MessageQueue messages;
  std::atomic<size_t> queued;
  std::atomic<size_t> dequeued;
  CephContext* const cct;
  mutable std::mutex connections_lock;
  std::thread runner;
  const ceph::coarse_real_clock::duration idle_time;
  const ceph::coarse_real_clock::duration reconnect_time;

  void publish_internal(message_wrapper_t* message) {
    const std::unique_ptr<message_wrapper_t> msg_owner(message);
    auto& conn = message->conn;

    if (!conn->is_ok()) {
      // connection had an issue while message was in the queue
      // TODO add error stats
      ldout(conn->cct, 1) << "AMQP publish: connection had an issue while message was in the queue" << dendl;
      if (message->cb) {
        message->cb(RGW_AMQP_STATUS_CONNECTION_CLOSED);
      }
      return;
    }

    if (message->cb == nullptr) {
      // TODO add error stats
      const auto rc = amqp_basic_publish(conn->state,
        CHANNEL_ID,
        amqp_cstring_bytes(conn->exchange.c_str()),
        amqp_cstring_bytes(message->topic.c_str()),
        0, // does not have to be routable
        0, // not immediate
        nullptr, // no properties needed
        amqp_cstring_bytes(message->message.c_str()));
      if (rc == AMQP_STATUS_OK) {
        ldout(conn->cct, 20) << "AMQP publish (no callback): OK" << dendl;
        return;
      }
      ldout(conn->cct, 1) << "AMQP publish (no callback): failed with error " << status_to_string(rc) << dendl;
      // an error occurred, close connection
      // it will be retied by the main loop
      conn->destroy(rc);
      return;
    }

    amqp_basic_properties_t props;
    props._flags = 
      AMQP_BASIC_DELIVERY_MODE_FLAG | 
      AMQP_BASIC_REPLY_TO_FLAG;
    props.delivery_mode = 2; // persistent delivery TODO take from conf
    props.reply_to = conn->reply_to_queue;

    const auto rc = amqp_basic_publish(conn->state,
      CONFIRMING_CHANNEL_ID,
      amqp_cstring_bytes(conn->exchange.c_str()),
      amqp_cstring_bytes(message->topic.c_str()),
      conn->mandatory,
      0, // not immediate
      &props,
      amqp_cstring_bytes(message->message.c_str()));

    if (rc == AMQP_STATUS_OK) {
      auto const q_len = conn->callbacks.size();
      if (q_len < max_inflight) {
        ldout(conn->cct, 20) << "AMQP publish (with callback, tag=" << conn->delivery_tag << "): OK. Queue has: " << q_len << " callbacks" << dendl;
        conn->callbacks.emplace_back(conn->delivery_tag++, message->cb);
      } else {
        // immediately invoke callback with error
        ldout(conn->cct, 1) << "AMQP publish (with callback): failed with error: callback queue full" << dendl;
        message->cb(RGW_AMQP_STATUS_MAX_INFLIGHT);
      }
    } else {
      // an error occurred, close connection
      // it will be retied by the main loop
      ldout(conn->cct, 1) << "AMQP publish (with callback): failed with error: " << status_to_string(rc) << dendl;
      conn->destroy(rc);
      // immediately invoke callback with error
      message->cb(rc);
    }
  }

  // the managers thread:
  // (1) empty the queue of messages to be published
  // (2) loop over all connections and read acks
  // (3) manages deleted connections
  // (4) TODO reconnect on connection errors
  // (5) TODO cleanup timedout callbacks
  void run() {
    amqp_frame_t frame;
    while (!stopped) {

      // publish all messages in the queue
      const auto count = messages.consume_all(std::bind(&Manager::publish_internal, this, std::placeholders::_1));
      dequeued += count;
      ConnectionList::iterator conn_it;
      ConnectionList::const_iterator end_it;
      {
        // thread safe access to the connection list
        // once the iterators are fetched they are guaranteed to remain valid
        std::lock_guard lock(connections_lock);
        conn_it = connections.begin();
        end_it = connections.end();
      }
      auto incoming_message = false;
      // loop over all connections to read acks
      for (;conn_it != end_it;) {
        
        auto& conn = conn_it->second;
        // delete the connection if marked for deletion
        if (conn->marked_for_deletion) {
          ldout(conn->cct, 10) << "AMQP run: connection is deleted" << dendl;
          conn->destroy(RGW_AMQP_STATUS_CONNECTION_CLOSED);
          std::lock_guard lock(connections_lock);
          // erase is safe - does not invalidate any other iterator
          // lock so no insertion happens at the same time
          ERASE_AND_CONTINUE(conn_it, connections);
        }

        // try to reconnect the connection if it has an error
        if (!conn->is_ok()) {
          const auto now = ceph::coarse_real_clock::now();
          if (now >= conn->next_reconnect) {
            // pointers are used temporarily inside the amqp_connection_info object
            // as read-only values, hence the assignment, and const_cast are safe here
            amqp_connection_info info;
            info.host = const_cast<char*>(conn_it->first.host.c_str());
            info.port = conn_it->first.port;
            info.vhost = const_cast<char*>(conn_it->first.vhost.c_str());
            info.user = const_cast<char*>(conn->user.c_str());
            info.password = const_cast<char*>(conn->password.c_str());
            ldout(conn->cct, 20) << "AMQP run: retry connection" << dendl;
            if (create_connection(conn, info)->is_ok() == false) {
              ldout(conn->cct, 10) << "AMQP run: connection (" << to_string(conn_it->first) << ") retry failed. error: " <<
                status_to_string(conn->status) << " (" << conn->reply_code << ")"  << dendl;
              // TODO: add error counter for failed retries
              // TODO: add exponential backoff for retries
              conn->next_reconnect = now + reconnect_time;
            } else {
              ldout(conn->cct, 10) << "AMQP run: connection (" << to_string(conn_it->first) << ") retry successfull" << dendl;
            }
          }
          INCREMENT_AND_CONTINUE(conn_it);
        }

        const auto rc = amqp_simple_wait_frame_noblock(conn->state, &frame, &read_timeout);

        if (rc == AMQP_STATUS_TIMEOUT) {
          // TODO mark connection as idle
          INCREMENT_AND_CONTINUE(conn_it);
        }
       
        // this is just to prevent spinning idle, does not indicate that a message
        // was successfully processed or not
        incoming_message = true;

        // check if error occurred that require reopening the connection
        if (rc != AMQP_STATUS_OK) {
          // an error occurred, close connection
          // it will be retied by the main loop
          ldout(conn->cct, 1) << "AMQP run: connection read error: " << status_to_string(rc) << dendl;
          conn->destroy(rc);
          INCREMENT_AND_CONTINUE(conn_it);
        }

        if (frame.frame_type != AMQP_FRAME_METHOD) {
          ldout(conn->cct, 10) << "AMQP run: ignoring non n/ack messages. frame type: " 
            << unsigned(frame.frame_type) << dendl;
          // handler is for publish confirmation only - handle only method frames
          INCREMENT_AND_CONTINUE(conn_it);
        }

        uint64_t tag;
        bool multiple;
        int result;

        switch (frame.payload.method.id) {
          case AMQP_BASIC_ACK_METHOD: 
            {
              result = AMQP_STATUS_OK;
              const auto ack = (amqp_basic_ack_t*)frame.payload.method.decoded;
              ceph_assert(ack);
              tag = ack->delivery_tag;
              multiple = ack->multiple;
              break;
            }
          case AMQP_BASIC_NACK_METHOD:
            {
              result = RGW_AMQP_STATUS_BROKER_NACK;
              const auto nack = (amqp_basic_nack_t*)frame.payload.method.decoded;
              ceph_assert(nack);
              tag = nack->delivery_tag;
              multiple = nack->multiple;
              break;
            }
          case AMQP_BASIC_REJECT_METHOD:                                                   
            {                                                                              
              result = RGW_AMQP_STATUS_BROKER_NACK;                                        
              const auto reject = (amqp_basic_reject_t*)frame.payload.method.decoded;      
              tag = reject->delivery_tag;                                                  
              multiple = false;                                                            
              break;
            }
          case AMQP_CONNECTION_CLOSE_METHOD:
            // TODO on channel close, no need to reopen the connection
          case AMQP_CHANNEL_CLOSE_METHOD:
            {
              // other side closed the connection, no need to continue
              ldout(conn->cct, 10) << "AMQP run: connection was closed by broker" << dendl;
              conn->destroy(rc);
              INCREMENT_AND_CONTINUE(conn_it);
            }
          case AMQP_BASIC_RETURN_METHOD:
            // message was not delivered, returned to sender
            ldout(conn->cct, 10) << "AMQP run: message was not routable" << dendl;
            INCREMENT_AND_CONTINUE(conn_it);
            break;
          default:
            // unexpected method
            ldout(conn->cct, 10) << "AMQP run: unexpected message" << dendl;
            INCREMENT_AND_CONTINUE(conn_it);
        }

        const auto& callbacks_end = conn->callbacks.end();
        const auto& callbacks_begin = conn->callbacks.begin();
        const auto tag_it = std::find(callbacks_begin, callbacks_end, tag);
        if (tag_it != callbacks_end) {
          if (multiple) {
            // n/ack all up to (and including) the tag
            ldout(conn->cct, 20) << "AMQP run: multiple n/acks received with tag=" << tag << " and result=" << result << dendl;
            auto it = callbacks_begin;
            while (it->tag <= tag && it != conn->callbacks.end()) {
              ldout(conn->cct, 20) << "AMQP run: invoking callback with tag=" << it->tag << dendl;
              it->cb(result);
              it = conn->callbacks.erase(it);
            }
          } else {
            // n/ack a specific tag
            ldout(conn->cct, 20) << "AMQP run: n/ack received, invoking callback with tag=" << tag << " and result=" << result << dendl;
            tag_it->cb(result);
            conn->callbacks.erase(tag_it);
          }
        } else {
          ldout(conn->cct, 10) << "AMQP run: unsolicited n/ack received with tag=" << tag << dendl;
        }
        // just increment the iterator
        ++conn_it;
      }
      // if no messages were received or published, sleep for 100ms
      if (count == 0 && !incoming_message) {
        std::this_thread::sleep_for(idle_time);
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
      long _usec_timeout,
      unsigned reconnect_time_ms,
      unsigned idle_time_ms,
      CephContext* _cct) : 
    max_connections(_max_connections),
    max_inflight(_max_inflight),
    max_queue(_max_queue),
    connection_count(0),
    stopped(false),
    read_timeout{0, _usec_timeout},
    connections(_max_connections),
    messages(max_queue),
    queued(0),
    dequeued(0),
    cct(_cct),
    runner(&Manager::run, this),
    idle_time(std::chrono::milliseconds(idle_time_ms)),
    reconnect_time(std::chrono::milliseconds(reconnect_time_ms)) {
      // The hashmap has "max connections" as the initial number of buckets, 
      // and allows for 10 collisions per bucket before rehash.
      // This is to prevent rehashing so that iterators are not invalidated 
      // when a new connection is added.
      connections.max_load_factor(10.0);
      // give the runner thread a name for easier debugging
      const auto rc = ceph_pthread_setname(runner.native_handle(), "amqp_manager");
      ceph_assert(rc==0);
  }

  // non copyable
  Manager(const Manager&) = delete;
  const Manager& operator=(const Manager&) = delete;

  // stop the main thread
  void stop() {
    stopped = true;
  }

  // disconnect from a broker
  bool disconnect(connection_ptr_t& conn) {
    if (!conn || stopped) {
      return false;
    }
    conn->marked_for_deletion = true;
    return true;
  }

  // connect to a broker, or reuse an existing connection if already connected
  connection_ptr_t connect(const std::string& url, const std::string& exchange, bool mandatory_delivery) {
    if (stopped) {
      ldout(cct, 1) << "AMQP connect: manager is stopped" << dendl;
      return nullptr;
    }

    struct amqp_connection_info info;
    // cache the URL so that parsing could happen in-place
    std::vector<char> url_cache(url.c_str(), url.c_str()+url.size()+1);
    if (AMQP_STATUS_OK != amqp_parse_url(url_cache.data(), &info)) {
      ldout(cct, 1) << "AMQP connect: URL parsing failed" << dendl;
      return nullptr;
    }

    const connection_id_t id(info);
    std::lock_guard lock(connections_lock);
    const auto it = connections.find(id);
    if (it != connections.end()) {
      if (it->second->marked_for_deletion) {
        ldout(cct, 1) << "AMQP connect: endpoint marked for deletion" << dendl;
        return nullptr;
      } else if (it->second->exchange != exchange) {
        ldout(cct, 1) << "AMQP connect: exchange mismatch" << dendl;
        return nullptr;
      }
      // connection found - return even if non-ok
      ldout(cct, 20) << "AMQP connect: connection found" << dendl;
      return it->second;
    }

    // connection not found, creating a new one
    if (connection_count >= max_connections) {
      ldout(cct, 1) << "AMQP connect: max connections exceeded" << dendl;
      return nullptr;
    }
    const auto conn = create_new_connection(info, exchange, mandatory_delivery, cct);
    if (!conn->is_ok()) {
      ldout(cct, 10) << "AMQP connect: connection (" << to_string(id) << ") creation failed. error:" <<
              status_to_string(conn->status) << "(" << conn->reply_code << ")" << dendl;
    }
    // create_new_connection must always return a connection object
    // even if error occurred during creation. 
    // in such a case the creation will be retried in the main thread
    ceph_assert(conn);
    ++connection_count;
    ldout(cct, 10) << "AMQP connect: new connection is created. Total connections: " << connection_count << dendl;
    ldout(cct, 10) << "AMQP connect: new connection status is: " << status_to_string(conn->status) << dendl;
    return connections.emplace(id, conn).first->second;
  }

  // TODO publish with confirm is needed in "none" case as well, cb should be invoked publish is ok (no ack)
  int publish(connection_ptr_t& conn, 
    const std::string& topic,
    const std::string& message) {
    if (stopped) {
      ldout(cct, 1) << "AMQP publish: manager is not running" << dendl;
      return RGW_AMQP_STATUS_MANAGER_STOPPED;
    }
    if (!conn || !conn->is_ok()) {
      ldout(cct, 1) << "AMQP publish: no connection" << dendl;
      return RGW_AMQP_STATUS_CONNECTION_CLOSED;
    }
    if (messages.push(new message_wrapper_t(conn, topic, message, nullptr))) {
      ++queued;
      return AMQP_STATUS_OK;
    }
    ldout(cct, 1) << "AMQP publish: queue is full" << dendl;
    return RGW_AMQP_STATUS_QUEUE_FULL;
  }
  
  int publish_with_confirm(connection_ptr_t& conn, 
    const std::string& topic,
    const std::string& message,
    reply_callback_t cb) {
    if (stopped) {
      ldout(cct, 1) << "AMQP publish_with_confirm: manager is not running" << dendl;
      return RGW_AMQP_STATUS_MANAGER_STOPPED;
    }
    if (!conn || !conn->is_ok()) {
      ldout(cct, 1) << "AMQP publish_with_confirm: no connection" << dendl;
      return RGW_AMQP_STATUS_CONNECTION_CLOSED;
    }
    if (messages.push(new message_wrapper_t(conn, topic, message, cb))) {
      ++queued;
      return AMQP_STATUS_OK;
    }
    ldout(cct, 1) << "AMQP publish_with_confirm: queue is full" << dendl;
    return RGW_AMQP_STATUS_QUEUE_FULL;
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
static const long READ_TIMEOUT_USEC = 100;
static const unsigned IDLE_TIME_MS = 100;
static const unsigned RECONNECT_TIME_MS = 100;

bool init(CephContext* cct) {
  if (s_manager) {
    return false;
  }
  // TODO: take conf from CephContext
  s_manager = new Manager(MAX_CONNECTIONS_DEFAULT, MAX_INFLIGHT_DEFAULT, MAX_QUEUE_DEFAULT, 
      READ_TIMEOUT_USEC, IDLE_TIME_MS, RECONNECT_TIME_MS, cct);
  return true;
}

void shutdown() {
  delete s_manager;
  s_manager = nullptr;
}

connection_ptr_t connect(const std::string& url, const std::string& exchange, bool mandatory_delivery) {
  if (!s_manager) return nullptr;
  return s_manager->connect(url, exchange, mandatory_delivery);
}

int publish(connection_ptr_t& conn, 
    const std::string& topic,
    const std::string& message) {
  if (!s_manager) return RGW_AMQP_STATUS_MANAGER_STOPPED;
  return s_manager->publish(conn, topic, message);
}

int publish_with_confirm(connection_ptr_t& conn, 
    const std::string& topic,
    const std::string& message,
    reply_callback_t cb) {
  if (!s_manager) return RGW_AMQP_STATUS_MANAGER_STOPPED;
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

bool disconnect(connection_ptr_t& conn) {
  if (!s_manager) return false;
  return s_manager->disconnect(conn);
}

} // namespace amqp

