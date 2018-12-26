// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

//#include "rgw_amqp.h"
#include <amqp.h>
#include <amqp_tcp_socket.h>

#include <sstream>
#include <cstring>
#include <map>
#include <string>
#include <vector>

// comparisson operator needed so that amqp_connection_info could
// be used as a key in std::map
bool operator<(const amqp_connection_info& lhs, const amqp_connection_info& rhs) {
  // check hostname
  if (std::strcmp(lhs.host, rhs.host) < 0) return true;
  if (std::strcmp(lhs.host, rhs.host) > 0) return false;
  // same hostname, check port
  if (lhs.port < rhs.port) return true;
  if (lhs.port > rhs.port) return false;
  // same port, check vhost
  if (std::strcmp(lhs.vhost, rhs.vhost) < 0) return true;
  if (std::strcmp(lhs.vhost, rhs.vhost) > 0) return false;
  // both are equal (ignoring user/password and ssl support)
  return false;
}

namespace amqp {

// convert connection ifo to string
std::string to_str(const amqp_connection_info& info) {
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
std::string to_str(const amqp_rpc_reply_t& reply) {
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

// TODO: why would you need multiple channels?
static const amqp_channel_t CHANNEL_ID = 1;

// utility function to create a connection
amqp_connection_state_t create_connection(const amqp_connection_info& info, const std::string& exchange) {
  // create connection
  auto conn = amqp_new_connection();
  ConnectionGuard guard(conn);
  if (!conn) {
    // failed to allocate connection
    return nullptr;
  }

  // create and open socket
  auto socket = amqp_tcp_socket_new(conn);
  if (!socket) {
    // failed to allocate socket
    return nullptr;
  }
  auto rc = amqp_socket_open(socket, info.host, info.port);
  if (rc < 0) {
    // failed to open socket to broker
    // TODO: retry
    return nullptr;
  }

  // login to broker
  auto reply = amqp_login(conn,
      info.vhost, 
      AMQP_DEFAULT_MAX_CHANNELS,
      AMQP_DEFAULT_FRAME_SIZE,
      0, // no heartbeat TODO: add conf
      AMQP_SASL_METHOD_PLAIN, // TODO: add other types of security
      info.user,
      info.password);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    // TODO: report the specific error and retry
    return nullptr;
  }

  // open channel
  amqp_channel_open(conn, CHANNEL_ID);
  if (amqp_get_rpc_reply(conn).reply_type != AMQP_RESPONSE_NORMAL) {
    // TODO: report the specific error and retry
    return nullptr;
  }

  // verify that the topic exchange is there
  amqp_exchange_declare(conn, 
      CHANNEL_ID,
      amqp_cstring_bytes(exchange.c_str()),
      amqp_cstring_bytes("topic"),
      0, // TODO: should be passive - exchange must already exist on broker
      1, // durable
      0, // dont auto-delete
      0, // not internal
      amqp_empty_table);

  if (amqp_get_rpc_reply(conn).reply_type != AMQP_RESPONSE_NORMAL) {
    // TODO: report the specific error and retry
    return nullptr;
  }

  guard.reset();
  return conn;
}

typedef std::map<amqp_connection_info, amqp_connection_state_t> ConnectionList;

class Manager {
  private:
    ConnectionList connections;

  public:
    // TODO: add max connections to ctor
    Manager() = default;
    // non copyable
    Manager(const Manager&) = delete;
    const Manager& operator=(const Manager&) = delete;

    bool disconnect(const std::string& url) {
      // TODO
      return true;
    }

    amqp_connection_state_t connect(const std::string& url, const std::string& exchange) {
      struct amqp_connection_info info;
      // cache the URL so that parsing could happen in-place
      std::vector<char> url_cache(url.c_str(), url.c_str()+url.size()+1);
      // override with parameters
      if (AMQP_STATUS_OK != amqp_parse_url(url_cache.data(), &info)) {
        // invalid input
        // TODO report error
        return nullptr;
      }
      auto it = connections.find(info);
      if (it != connections.end()) {
        // connection is found, return it
        // TODO: validate exchange in list
        return it->second;
      }

      // connection not found, should create it
      auto conn = create_connection(info, exchange);
      if (conn != nullptr) {
        connections[info] = conn;
      }

      return conn;
    }

    // dtor cleans all connections
    ~Manager() {
      for (auto conn : connections) {
        ConnectionGuard guard(conn.second);
      }
    }
};

// singleton manager for caching connections
Manager s_manager;

amqp_connection_state_t connect(const std::string& url, const std::string& exchange) {
  return s_manager.connect(url, exchange);
}

// publish a message over a connection that was already created
bool publish(amqp_connection_state_t conn, 
    const std::string& exchange,
    const std::string& topic,
    const std::string& message) {
  auto rc = amqp_basic_publish(conn, 
      CHANNEL_ID,
      amqp_cstring_bytes(exchange.c_str()),
      amqp_cstring_bytes(topic.c_str()),
      1, // mandatory, TODO: take from conf
      0, // not immediate
      nullptr,
      amqp_cstring_bytes(message.c_str()));
  // TODO: this is in the case of ack-status "none"
  return rc == AMQP_STATUS_OK;
}

} // namespace amqp

