#pragma once

#include "client.h"

#include <mutex>
#include <optional>

#include <boost/asio/execution/context.hpp>
#include <boost/asio/query.hpp>
#include <boost/asio/strand.hpp>

namespace rgw::curl::detail {

// a service that owns the shared Client instance over the lifetime of its
// execution context
class shared_client_service : public boost::asio::execution_context::service {
  std::once_flag init_flag;
  std::optional<Client> client;

  void shutdown() override
  {
    client.reset();
  }

 public:
  using key_type = service;
  static inline boost::asio::execution_context::id id;

  explicit shared_client_service(boost::asio::execution_context& ctx)
      : boost::asio::execution_context::service(ctx) {}

  Client& get_client(const boost::asio::execution::executor auto& ex)
  {
    std::call_once(init_flag, [this, &ex] {
      client.emplace(boost::asio::make_strand(ex));
    });
    return *client;
  }
};

// get or create a shared Client instance from the underlying execution context
Client& get_shared_client(const boost::asio::execution::executor auto& ex)
{
  auto& ctx = boost::asio::query(ex, boost::asio::execution::context);
  auto& svc = boost::asio::use_service<shared_client_service>(ctx);
  return svc.get_client(ex);
}

} // namespace rgw::curl::detail
