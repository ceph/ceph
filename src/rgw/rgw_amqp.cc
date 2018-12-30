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
#include <atomic>

namespace amqp {

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
  
  connection_t(amqp_connection_state_t _state, const std::string _exchange) :
    state(_state), exchange(_exchange) {} 
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
        amqp_connection_close_t* m = (amqp_connection_close_t*)reply.reply.decoded;
        ss << m->reply_code << " text: " << std::string((char*)m->reply_text.bytes, m->reply_text.len);
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
    case AMQP_STATUS_UNSUPPORTED:
      return "AMQP_STATUS_UNSUPPORTED";
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

// convert int status to string
std::string status_to_string(int s) {
  return to_string((amqp_status_enum)s);
}

// amqp connection guard
// could be used for automatic cleanup when getting out of scope
class ConnectionGuard {
  private:
    amqp_connection_state_t conn;
  public:
    ConnectionGuard(amqp_connection_state_t _conn) : conn(_conn) {}
    ~ConnectionGuard() {
      if (conn) {
        amqp_destroy_connection(conn);
      }
    }
    // call disable if cleanup is not needed anymore
    void reset() {
      conn = nullptr;
    }
};

// TODO: support multiple channels
static const amqp_channel_t CHANNEL_ID = 1;

static connection_t NO_CONNECTION(nullptr, "");

// utility function to create a connection
connection_t create_connection(const amqp_connection_info& info, const std::string& exchange) {
  // create connection state
  auto conn = amqp_new_connection();
  ConnectionGuard guard(conn);
  if (!conn) {
    throw (connection_error("failed to allocate connection"));
    return NO_CONNECTION;
  }

  // create and open socket
  auto socket = amqp_tcp_socket_new(conn);
  if (!socket) {
    throw(connection_error("failed to allocate socket"));
    return NO_CONNECTION;
  }
  const auto s = amqp_socket_open(socket, info.host, info.port);
  if (s < 0) {
    throw(connection_error("failed to connect to broker: " + to_string((amqp_status_enum)s)));
    return NO_CONNECTION;
    // TODO: retry 
  }

  // login to broker
  const auto reply = amqp_login(conn,
      info.vhost, 
      AMQP_DEFAULT_MAX_CHANNELS,
      AMQP_DEFAULT_FRAME_SIZE,
      0, // no heartbeat TODO: add conf
      AMQP_SASL_METHOD_PLAIN, // TODO: add other types of security
      info.user,
      info.password);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    throw(connection_error("failed to login to broker: " + to_string(reply)));
    return NO_CONNECTION;
  }

  // open channel
  {
    const auto ok = amqp_channel_open(conn, CHANNEL_ID);
    if (!ok) {
      throw(connection_error("failed to open channel: " + std::to_string(CHANNEL_ID)));
      return NO_CONNECTION;
    }
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
      throw(connection_error("exchange not found"));
      return NO_CONNECTION;
    }
    const auto reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
      throw(connection_error("failed to declare exchange: " + to_string(reply)));
      return NO_CONNECTION;
      // TODO: retry 
    }
  }

  guard.reset();
  return connection_t(conn, exchange);
}

typedef std::unordered_map<ConnectionID, connection_t, ConnectionID::hasher> ConnectionList;

class Manager : public std::thread {
  private:
    ConnectionList connections;
    std::atomic<bool> stopped;
    const unsigned max_connections;
    std::atomic<unsigned> connection_number;

    void run() {
      while (!stopped) {
        // TODO: add reply handling
        std::this_thread::sleep_for(std::chrono::milliseconds(10000));
      }
    }

  public:
    Manager(unsigned _max_connections) : 
      std::thread(&Manager::run, this),
      max_connections(_max_connections) {}

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
        return NO_CONNECTION;
      }
      struct amqp_connection_info info;
      // cache the URL so that parsing could happen in-place
      std::vector<char> url_cache(url.c_str(), url.c_str()+url.size()+1);
      if (AMQP_STATUS_OK != amqp_parse_url(url_cache.data(), &info)) {
        throw(connection_error("failed to parse URL: " + url));
        return NO_CONNECTION;
      }
      ConnectionID id(info);
      auto it = connections.find(id);
      if (it != connections.end()) {
        if (it->second.exchange != exchange) {
          throw(connection_error("exchange mismatch: " + it->second.exchange + " != " + exchange));
          return NO_CONNECTION;
        }
        // connection found
        ceph_assert(it->second.state);
        return it->second;
      }

      // connection not found, creating a new one
      if (connection_number >= max_connections) {
        throw(connection_error("max connections reached: " + std::to_string(max_connections)));
        return NO_CONNECTION;
      }

      auto conn = create_connection(info, exchange);
      if (conn.state != nullptr) {
        ++connection_number;
        return connections.emplace(id, conn).first->second;
      }
      return NO_CONNECTION;
    }

    // dtor cleans all connections
    ~Manager() {
      stopped = true;
      join();
      for (auto conn : connections) {
        ConnectionGuard guard(conn.second.state);
      }
    }
};

// singleton manager for caching connections
Manager s_manager(256);

const connection_t& connect(const std::string& url, const std::string& exchange) {
  return s_manager.connect(url, exchange);
}

// publish a message over a connection that was already created
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

} // namespace amqp

