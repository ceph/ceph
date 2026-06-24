#pragma once

#include <mutex>

#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core/flat_static_buffer.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

namespace rgw::asio {

using tcp = boost::asio::ip::tcp;

static constexpr size_t parse_buffer_size = 65536;
using parse_buffer = boost::beast::flat_static_buffer<parse_buffer_size>;

// timeout support requires that connections are reference-counted, because the
// timeout_handler can outlive the coroutine
struct Connection : boost::intrusive::list_base_hook<>,
                    boost::intrusive_ref_counter<Connection>
{
  tcp::socket socket;
  parse_buffer buffer;

  explicit Connection(tcp::socket&& socket) noexcept
      : socket(std::move(socket)) {}

  void close(boost::system::error_code& ec) {
    socket.close(ec);
  }

  tcp::socket& get_socket() { return socket; }
};

class ConnectionList {
  using List = boost::intrusive::list<Connection>;
  List connections;
  std::mutex mutex;

  void remove(Connection& c) {
    std::lock_guard lock{mutex};
    if (c.is_linked()) {
      connections.erase(List::s_iterator_to(c));
    }
  }
 public:
  class Guard {
    ConnectionList *list;
    Connection *conn;
   public:
    Guard(ConnectionList *list, Connection *conn) : list(list), conn(conn) {}
    ~Guard() { list->remove(*conn); }
  };
  [[nodiscard]] Guard add(Connection& conn) {
    std::lock_guard lock{mutex};
    connections.push_back(conn);
    return Guard{this, &conn};
  }
  void close(boost::system::error_code& ec) {
    std::lock_guard lock{mutex};
    for (auto& conn : connections) {
      conn.socket.close(ec);
    }
    connections.clear();
  }
};

} // namespace rgw::asio
