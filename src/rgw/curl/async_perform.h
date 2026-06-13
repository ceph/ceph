#pragma once

#include <concepts>
#include <boost/asio/async_result.hpp>
#include <boost/asio/compose.hpp>
#include <boost/asio/execution_context.hpp>
#include <boost/asio/execution/executor.hpp>
#include <boost/system/error_code.hpp>

#include "shared_client.h"

namespace rgw::curl {

/// \brief Perform an asynchronous http request with libcurl.
///
/// Send the http request asynchronously, as if by curl_easy_perform(), and
/// invoke the handler on completion. The caller must keep the easy handle
/// alive in the meantime.
///
/// The operation can be canceled by binding a cancellation slot to the
/// given CompletionToken.
///
/// This function uses a shared Client instance associated with the given
/// execution context. This Client is wrapped by a strand executor, making it
/// safe to call async_perform() from any thread.
template <boost::asio::execution::executor Executor,
          boost::asio::completion_token_for<void(error_code)> CompletionToken>
auto async_perform(const Executor& ex, void* easy, CompletionToken&& token);

/// \overload
template <typename ExecutionContext,
          boost::asio::completion_token_for<void(error_code)> CompletionToken>
    requires std::convertible_to<ExecutionContext&, boost::asio::execution_context&>
auto async_perform(ExecutionContext& ctx, void* easy, CompletionToken&& token)
{
  return async_perform(ctx.get_executor(), easy,
                       std::forward<CompletionToken>(token));
}

namespace detail {

struct async_perform_impl {
  void* easy = nullptr;
  Client& client;
  enum class State {
    dispatch,
    perform,
    complete
  } state = State::dispatch;

  void operator()(auto& self, error_code ec = {})
  {
    BOOST_ASIO_HANDLER_LOCATION((__FILE__, __LINE__, "async_perform_impl"));

    // handle cancellation at any state
    if (self.cancelled() != boost::asio::cancellation_type::none) {
      self.complete(boost::asio::error::operation_aborted);
      return;
    }

    switch (state) {
      case State::dispatch:
        state = State::perform;
        // switch to the client's strand executor before calling async_perform()
        boost::asio::dispatch(client.get_executor(), std::move(self));
        break;

      case State::perform:
        state = State::complete;
        client.async_perform(easy, std::move(self));
        break;

      case State::complete:
        self.complete(ec);
        break;
    }
  }
};

} // namespace detail

template <boost::asio::execution::executor Executor,
          boost::asio::completion_token_for<void(error_code)> CompletionToken>
auto async_perform(const Executor& ex, void* easy, CompletionToken&& token)
{
  return boost::asio::async_compose<CompletionToken, void(error_code)>(
      detail::async_perform_impl{easy, detail::get_shared_client(ex)},
      token, ex);
}

} // namespace rgw::curl
