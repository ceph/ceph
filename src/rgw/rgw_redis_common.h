#pragma once

#include <boost/asio/detached.hpp>
#include <iostream>
#include <memory>
#include <string>

#include "boost/redis/connection.hpp"
#include "common/async/blocked_completion.h"
#include "common/async/yield_context.h"
#include "common/dout.h"

namespace rgw {
namespace redis {

using boost::redis::config;
using boost::redis::connection;
using RedisResponseMap =
    boost::redis::response<std::map<std::string, std::string>>;

struct RedisResponse {
  const int errorCode;
  const std::string errorMessage;
  const std::string data;

  RedisResponse(const int ec, const std::string& msg)
      : errorCode(ec), errorMessage(msg) {}

  RedisResponse(const RedisResponseMap& resp)
      : errorCode(std::stoi(std::get<0>(resp).value().at("errorCode"))),
        errorMessage(std::get<0>(resp).value().at("errorMessage")),
        data(std::get<0>(resp).value().at("data")) {}
};

// <<<<<<< HEAD
// =======
// BOOST_DESCRIBE_STRUCT(RedisWriteResponse, (), (errorCode, errorMessage))

// inline void boost_redis_from_bulk(RedisWriteResponse& resp, std::string_view
// sv,
//                            boost::system::error_code& ec) {
//   resp = boost::json::value_to<RedisWriteResponse>(boost::json::parse(sv));
// }

// struct RedisReadResponse {
//   int errorCode;
//   std::string errorMessage;
//   int elementCount;
//   std::vector<std::string> data;
// };

// BOOST_DESCRIBE_STRUCT(RedisReadResponse, (),
//                       (errorCode, errorMessage, elementCount, data))

// inline void boost_redis_from_bulk(RedisReadResponse& resp, std::string_view
// sv,
//                            boost::system::error_code& ec) {
//   resp = boost::json::value_to<RedisReadResponse>(boost::json::parse(sv));
// }

// >>>>>>> 94026556cbb (rgw/redis: fix linking issues)

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

template <typename T, typename... Ts>
void redis_exec(connection* conn, boost::system::error_code& ec,
                boost::redis::request& req,
                boost::redis::response<T, Ts...>& resp, optional_yield y) {
  if (y) {
    auto yield = y.get_yield_context();
    async_exec(conn, req, resp, yield[ec]);
  } else {
    async_exec(conn, req, resp, ceph::async::use_blocked[ec]);
  }
}

RedisResponse do_redis_func(connection* conn, boost::redis::request& req,
                            RedisResponseMap& resp, std::string func_name,
                            optional_yield y);

int load_lua_rgwlib(boost::asio::io_context& io, connection* conn, config* cfg,
                    optional_yield y);

}  // namespace redis
}  // namespace rgw
