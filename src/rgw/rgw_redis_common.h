#pragma once

#include <boost/asio/detached.hpp>
#include <iostream>
#include <string>

#include "boost/redis/connection.hpp"
#include "common/async/blocked_completion.h"
#include "common/async/yield_context.h"
#include "common/dout.h"

namespace rgw {
namespace redis {

using boost::redis::config;
using boost::redis::connection;

struct initiate_exec {
  connection* conn;

  using executor_type = boost::redis::connection::executor_type;
  executor_type get_executor() const noexcept { return conn->get_executor(); }

  template <typename Handler, typename Response>
  void operator()(Handler handler, const boost::redis::request& req,
                  Response& resp) {
    auto h = boost::asio::consign(std::move(handler), conn);
    return boost::asio::dispatch(
        get_executor(), [c = conn, &req, &resp, h = std::move(h)]() mutable {
          return c->async_exec(req, resp, std::move(h));
        });
  }
};

template <typename Response, typename CompletionToken>
auto async_exec(connection* conn, const boost::redis::request& req,
                Response& resp, CompletionToken&& token) {
  return boost::asio::async_initiate<
      CompletionToken, void(boost::system::error_code, std::size_t)>(
      initiate_exec{conn}, token, req, resp);
}

template <typename T>
void redis_exec(connection* conn, boost::system::error_code& ec,
                boost::redis::request& req, boost::redis::response<T>& resp,
                optional_yield y) {
  if (y) {
    auto yield = y.get_yield_context();
    async_exec(conn, req, resp, yield[ec]);
  } else {
    async_exec(conn, req, resp, ceph::async::use_blocked[ec]);
  }
}

template <typename T>
int doRedisFunc(connection* conn, boost::redis::request& req,
                boost::redis::response<T>& resp, optional_yield y) {
  boost::system::error_code ec;
  redis_exec(conn, ec, req, resp, y);

  if (ec) {
    std::cerr << "EC Message: " << ec.message() << std::endl;
    return ec.value();
  }
  return std::get<0>(resp).value();
}

int loadLuaFunctions(boost::asio::io_context& io, connection* conn, config* cfg,
                     optional_yield y);

}  // namespace redis
}  // namespace rgw