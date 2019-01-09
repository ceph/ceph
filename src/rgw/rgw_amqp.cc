// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

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

namespace rgw::amqp {

// key class for the connection list
class ConnectionID {
private:
  std::string host;
  int port;
  std::string vhost;
public:
  // constructed from amqp_connection_info struct
  ConnectionID(const amqp_connection_info& info) 
    : host(info.host), port(info.port), vhost(info.vhost) {}

  // equality operator and hasher functor are needed 
  // so that ConnectionID could be used as key in unordered_map
  bool operator==(const ConnectionID& other) const {
    return host == other.host && port == other.port && vhost == other.vhost;
  }
  struct hasher {
    std::size_t operator()(const ConnectionID& k) const {
       return ((std::hash<std::string>()(k.host)
             ^ (std::hash<int>()(k.port) << 1)) >> 1)
             ^ (std::hash<std::string>()(k.vhost) << 1); 
    }
  };
};

// struct for holding the connection state object
// as well as the exchange
struct connection_t {
  amqp_connection_state_t const state;
  const std::string exchange;
  const amqp_bytes_t reply_to_queue;
  
  connection_t(amqp_connection_state_t _state, 
      const std::string _exchange,
      const amqp_bytes_t& _reply_to_queue) :
    state(_state), exchange(_exchange), reply_to_queue(amqp_bytes_malloc_dup(_reply_to_queue))
  {}
};

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

// convert error reply to string
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
      return "AMQP_STATUS_SOCKET_ERROR: " + std::to_string(errno);
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
        return "AMQP_STATUS_TCP_ERROR: " + std::to_string(errno);
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

static const int INTERNAL_AMQP_STATUS_PENDING_CONFIRMATION = -99;
static const int AMQP_STATUS_BROKER_NACK = -999;

// convert int status to string
std::string status_to_string(int s) {
  if (s == INTERNAL_AMQP_STATUS_PENDING_CONFIRMATION) {
    return "INTERNAL_AMQP_STATUS_PENDING_CONFIRMATION";
  } else if (s == AMQP_STATUS_BROKER_NACK) {
    return "AMQP_STATUS_BROKER_NACK";
  }
  return to_string((amqp_status_enum)s);
}

// in case of RPC calls, getting ther RPC reply and throwing if an error detected
void throw_on_reply_error(amqp_connection_state_t conn, const std::string& prefix_text) {
    const auto reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
      throw(connection_error(prefix_text + to_string(reply)));
    }
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

// TODO: support multiple channels
static const amqp_channel_t CHANNEL_ID = 1;
static const amqp_channel_t CONFIRMING_CHANNEL_ID = 2;

// utility function to create a connection
// TODO: retry in case an error is recoverable
connection_t create_connection(const amqp_connection_info& info, const std::string& exchange) {
  // create connection state
  auto conn = amqp_new_connection();
  ConnectionCleaner guard(conn);
  if (!conn) {
    throw (connection_error("failed to allocate connection"));
  }

  // create and open socket
  auto socket = amqp_tcp_socket_new(conn);
  if (!socket) {
    throw(connection_error("failed to allocate socket"));
  }
  const auto s = amqp_socket_open(socket, info.host, info.port);
  if (s < 0) {
    throw(connection_error("failed to connect to broker: " + to_string((amqp_status_enum)s)));
  }

  // login to broker
  const auto reply = amqp_login(conn,
      info.vhost, 
      AMQP_DEFAULT_MAX_CHANNELS,
      AMQP_DEFAULT_FRAME_SIZE,
      0,                        // no heartbeat TODO: add conf
      AMQP_SASL_METHOD_PLAIN,   // TODO: add other types of security
      info.user,
      info.password);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    throw(connection_error("failed to login to broker: " + to_string(reply)));
  }

  // open channels
  {
    const auto ok = amqp_channel_open(conn, CHANNEL_ID);
    if (!ok) {
      throw(connection_error("failed to open channel (client): " + std::to_string(CHANNEL_ID)));
    }
    throw_on_reply_error(conn, "failed to open channel (broker): ");
  }
  {
    const auto ok = amqp_channel_open(conn, CONFIRMING_CHANNEL_ID);
    if (!ok) {
      throw(connection_error("failed to open channel (client): " + std::to_string(CONFIRMING_CHANNEL_ID)));
    }
    throw_on_reply_error(conn, "failed to open channel (broker): ");
  }
  {
    const auto ok = amqp_confirm_select(conn, CONFIRMING_CHANNEL_ID);
    if (!ok) {
      throw(connection_error("failed to set channel to 'confirm' mode (client): " + std::to_string(CONFIRMING_CHANNEL_ID)));
    }
    throw_on_reply_error(conn, "failed to set channel to 'confirm' mode (broker): ");
  }

  // verify that the topic exchange is there
  {
    const auto ok = amqp_exchange_declare(conn, 
      CHANNEL_ID,
      amqp_cstring_bytes(exchange.c_str()),
      amqp_cstring_bytes("topic"),
      1, // passive - exchange must already exist on broker
      1, // durable
      0, // dont auto-delete
      0, // not internal
      amqp_empty_table);
    if (!ok) {
      throw(connection_error("could not query exchange"));
    }
    throw_on_reply_error(conn, "exchange not found: ");
  }
  {
    // create queue for confirmations
    const auto ok  = amqp_queue_declare(conn, 
        CHANNEL_ID,         // use the regular channel for this call
        amqp_empty_bytes,   // let broker allocate queue name
        0,                  // not passive - create the queue 
        0,                  // not durable 
        1,                  // exclusive 
        1,                  // auto-delete
        amqp_empty_table    // not args TODO add args from conf: TTL, max length etc.
        );
    if (!ok) {
      throw(connection_error("failed to create queue for incomming confimations (client)"));
    }
    const auto reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
      throw(connection_error("failed to create queue for incomming confimations (broker):" + to_string(reply)));
    }

    guard.reset();
    return connection_t(conn, exchange, ok->queue);
  }
}

typedef std::unordered_map<ConnectionID, connection_t, ConnectionID::hasher> ConnectionList;
typedef std::unordered_map<uint64_t, reply_callback_t> CallbackList;

// Thread Safety: libamqp is not thread-safe within the connection object
// so, this should not be a separate thread, has to run in the same thread
// of the callers of connect() and publish(). This would require:
// (1) non-blocking wait with timeout
// (2) exponential backoff - skipping idle connections
class Manager : public std::thread {
private:
  ConnectionList connections;
  CallbackList callbacks;
  bool stopped;
  const unsigned max_connections;
  unsigned connection_number;
  uint64_t delivery_tag;

  void run() {
    // define consumption for all connection
    for (const auto& conn : connections) {
      amqp_basic_consume(conn.second.state, 
          CONFIRMING_CHANNEL_ID, 
          conn.second.reply_to_queue, 
          amqp_empty_bytes, 
          0,
          1,
          0,
          amqp_empty_table);
      // TODO: error handling here should involve retries
    }
    amqp_frame_t frame;
    while (!stopped) {
      // loop over all connections
      for (const auto& conn : connections) {
        amqp_simple_wait_frame(conn.second.state, &frame);

        if (frame.frame_type != AMQP_FRAME_METHOD) {
          // handler is for publish confirmation only - ignore all other messages
          // TODO: add a counter
          continue;
        }

        uint64_t tag;
        bool multiple;
        int result;

        if (frame.payload.method.id == AMQP_BASIC_ACK_METHOD) {
          result = AMQP_STATUS_OK;
          const auto ack = (amqp_basic_ack_t*)frame.payload.method.decoded;
          ceph_assert(ack);
          tag = ack->delivery_tag;
          multiple = ack->multiple;
        } else if (frame.payload.method.id == AMQP_BASIC_NACK_METHOD) {
          result = AMQP_STATUS_BROKER_NACK;
          const auto nack = (amqp_basic_nack_t*)frame.payload.method.decoded;
          ceph_assert(nack);
          tag = nack->delivery_tag;
          multiple = nack->multiple;
        } else {
          // unexpected method
          // TODO: add a counter
          continue;
        }
      
        // TODO: handle "multiple"
        ceph_assert(!multiple); // multiple n/acks in one reply not supported. going to cause a leak if received
        const auto it = callbacks.find(tag);
        if (it != callbacks.end()) {
          it->second(result);
          callbacks.erase(it);
        } else {
          // TODO add counter for acks with no callback
        }
      }
      // if no message was received, sleep for 10ms
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

public:
  Manager(unsigned _max_connections) : 
    std::thread(&Manager::run, this),
    max_connections(_max_connections), delivery_tag(0) {}

  // non copyable
  Manager(const Manager&) = delete;
  const Manager& operator=(const Manager&) = delete;

  // stop the main thread
  void stop() {
    stopped = true;
  }

  // disconnect from a broker
  bool disconnect(const std::string& url) {
    // TODO
    return true;
  }

  // connect to a broker, or reuse an existing connection if already connected
  connection_t& connect(const std::string& url, const std::string& exchange) {
    if (stopped) {
      throw(connection_error("manager is stopped"));
    }

    struct amqp_connection_info info;
    // cache the URL so that parsing could happen in-place
    std::vector<char> url_cache(url.c_str(), url.c_str()+url.size()+1);
    if (AMQP_STATUS_OK != amqp_parse_url(url_cache.data(), &info)) {
      throw(connection_error("failed to parse URL: " + url));
    }

    ConnectionID id(info);
    auto it = connections.find(id);
    if (it != connections.end()) {
      if (it->second.exchange != exchange) {
        throw(connection_error("exchange mismatch: " + it->second.exchange + " != " + exchange));
      }
      // connection found
      ceph_assert(it->second.state);
      return it->second;
    }

    // connection not found, creating a new one
    if (connection_number >= max_connections) {
      throw(connection_error("max connections reached: " + std::to_string(max_connections)));
    }

    const auto conn = create_connection(info, exchange);
    // if creation fails, create_connection() must throw an exception
    ceph_assert(conn.state != nullptr);
    ++connection_number;
    return connections.emplace(id, conn).first->second;
  }

  int publish_with_confirm(const connection_t& conn, 
    const std::string& topic,
    const std::string& message,
    reply_callback_t cb) {
    ceph_assert(conn.state);


    amqp_basic_properties_t props;
    props._flags = 
      AMQP_BASIC_DELIVERY_MODE_FLAG | 
      AMQP_BASIC_REPLY_TO_FLAG;
    props.delivery_mode = 2; // persistent delivery TODO take from conf
    props.reply_to = conn.reply_to_queue;

    const auto rc = amqp_basic_publish(conn.state,
      CONFIRMING_CHANNEL_ID,
      amqp_cstring_bytes(conn.exchange.c_str()),
      amqp_cstring_bytes(topic.c_str()),
      1, // mandatory, TODO: take from conf
      0, // not immediate
      &props,
      amqp_cstring_bytes(message.c_str()));

    if (rc == AMQP_STATUS_OK) {
      ++delivery_tag;
      if (callbacks.insert(std::make_pair(delivery_tag, cb)).second == false) {
        // callback with that message id is already pending confirmation
        return INTERNAL_AMQP_STATUS_PENDING_CONFIRMATION;
      }
    }
    return rc;
  }

  // dtor wait for thread to stop
  // then connection are cleaned-up
  ~Manager() {
    stopped = true;
    join();
    for (const auto& conn : connections) {
        amqp_destroy_connection(conn.second.state);
        amqp_bytes_free(conn.second.reply_to_queue);
    }
  }

  // get the number of connections
  unsigned get_connection_number() const {
    return connection_number;
  }
};

// singleton manager
// note that the manager itself is not a singleton, and multiple instances may co-exist
Manager s_manager(256);

const connection_t& connect(const std::string& url, const std::string& exchange) {
  return s_manager.connect(url, exchange);
}

int publish(const connection_t& conn, 
    const std::string& topic,
    const std::string& message) {
  ceph_assert(conn.state);
  return amqp_basic_publish(conn.state,
      CHANNEL_ID,
      amqp_cstring_bytes(conn.exchange.c_str()),
      amqp_cstring_bytes(topic.c_str()),
      1, // mandatory, TODO: take from conf
      0, // not immediate
      nullptr,
      amqp_cstring_bytes(message.c_str()));
}

int publish_with_confirm(const connection_t& conn, 
    const std::string& topic,
    const std::string& message,
    reply_callback_t cb) {
  return s_manager.publish_with_confirm(conn, topic, message, cb);
}

unsigned get_connection_number() {
  return s_manager.get_connection_number();
}

} // namespace amqp

