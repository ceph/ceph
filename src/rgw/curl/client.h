#pragma once

#include <memory>
#include <utility>

#include <boost/asio/any_completion_handler.hpp>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/system/error_category.hpp>
#include <boost/system/error_code.hpp>

struct curl_slist;

namespace rgw::curl {

using error_code = boost::system::error_code;

/// Error category corresponding to CURLcode.
boost::system::error_category& easy_category();

/// Error category corresponding to CURLMcode.
boost::system::error_category& multi_category();


struct easy_deleter { void operator()(void* p); };
/// A unique_ptr wrapper for CURL easy handles.
using easy_ptr = std::unique_ptr<void, easy_deleter>;

/// Create an easy handle with curl_easy_init(). May fail with
/// CURLE_OUT_OF_MEMORY.
easy_ptr easy_init(error_code& ec);
/// \overload
easy_ptr easy_init(); // throws on error


struct multi_deleter { void operator()(void* p); };
/// A unique_ptr wrapper for CURLM multi handles.
using multi_ptr = std::unique_ptr<void, multi_deleter>;

/// Create a multi handle with curl_multi_init(). May fail with
/// CURLE_OUT_OF_MEMORY.
multi_ptr multi_init(error_code& ec);
/// \overload
multi_ptr multi_init(); // throws on error


struct slist_deleter { void operator()(curl_slist* p); };
/// A unique_ptr wrapper for curl string lists.
using slist_ptr = std::unique_ptr<curl_slist, slist_deleter>;

/// Allocate a new slist entry to hold a copy of the given string and update
/// the slist_ptr to point at the new head. May fail with CURLE_OUT_OF_MEMORY.
void slist_append(slist_ptr& p, const char* str, error_code& ec);
/// \overload
void slist_append(slist_ptr& p, const char* str); // throws on error


/// \brief Asynchronous libcurl HTTP client.
///
/// An asynchronous multiplexing libcurl client that runs exclusively on the
/// given asio io executor. Each instance of Client manages its own curl multi
/// handle and connection pool.
///
/// This class is not thread-safe, so a strand executor should be used in
/// multi-threaded contexts, and all member functions must be called within
/// that strand.
class Client {
 public:
  using executor_type = boost::asio::any_io_executor;

  /// Create and configure a curl multi handle.
  explicit Client(executor_type ex);

  /// Cancel any pending requests and close the curl handles.
  ~Client();

  /// Return the io executor.
  executor_type get_executor() const noexcept;

  /// \brief Perform an asynchronous http request with libcurl.
  ///
  /// Send the http request asynchronously, as if by curl_easy_perform(), and
  /// invoke the handler on completion. The caller must keep the easy handle
  /// alive in the meantime.
  ///
  /// The operation can be canceled by binding a cancellation slot to the
  /// given CompletionToken.
  template <boost::asio::completion_token_for<void(error_code)> CompletionToken>
  auto async_perform(void* easy, CompletionToken&& token)
  {
    return boost::asio::async_initiate<CompletionToken, void(error_code)>(
        [this, easy] (handler_type handler) {
          async_perform_impl(std::move(handler), easy);
        }, token);
  }

  /// Cancel all pending requests with boost::asio::error::operation_aborted.
  void cancel();

 private:
  class Impl;
  boost::intrusive_ptr<Impl> impl;

  using handler_type = boost::asio::any_completion_handler<void(error_code)>;
  void async_perform_impl(handler_type handler, void* easy);
};

} // namespace rgw::curl
