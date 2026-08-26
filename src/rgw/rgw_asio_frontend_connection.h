#pragma once

#include <mutex>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core/flat_static_buffer.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
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
  // executor of the handle_connection coroutine's strand. used so that close()
  // from outside the strand (e.g. frontend pause) serializes with coroutine
  // operations on the socket instead of racing the reactor
  boost::asio::any_io_executor strand;
  Connection(tcp::socket&& socket,
             boost::asio::any_io_executor strand) noexcept
      : socket(std::move(socket)), strand(std::move(strand)) {}

  void close() {
    boost::asio::dispatch(strand, [c = boost::intrusive_ptr<Connection>{this}] {
      boost::system::error_code ec;
      c->socket.close(ec);
    });
  }

  tcp::socket& get_socket() { return socket; }
};

class ConnectionList {
  using List = boost::intrusive::list<Connection>;
  List connections;
  mutable std::mutex mutex;

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
  size_t size() const {
    std::lock_guard lock{mutex};
    return connections.size();
  }
  void close() {
    std::lock_guard lock{mutex};
    for (auto& conn : connections) {
      // dispatch operations to the connection's strand so it serializes with
      // the handle_connection coroutine.
      boost::asio::dispatch(
	  conn.strand, [c = boost::intrusive_ptr<Connection>{&conn}] {
	    boost::system::error_code ec;
	    // cancel pending reactor operations which delivers operation_aborted
	    // to completion handlers and then shutdown the transport so any
	    // subsequent I/O attempts fail immediately.
	    c->socket.cancel(ec);
	    c->socket.shutdown(tcp::socket::shutdown_both, ec);
	  });
    }
  }
};
} // namespace rgw::asio
