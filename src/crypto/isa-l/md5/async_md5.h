// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License version 2.1, as published by
 * the Free Software Foundation.  See file COPYING.
 *
 */

#pragma once

#include <md5_mb.h>
#include <optional>
#include <string>

#include <boost/system/error_code.hpp>
#include <boost/asio/any_completion_handler.hpp>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

namespace ceph::async_md5 {

using boost::system::error_code;

/// Error category for enum HASH_CTX_ERROR.
boost::system::error_category& hash_category();

/// Represents the state of a hash digest over one or more calls to
/// Batch::async_hash(). Once the final call with last=true completes,
/// the hex-encoded digest can be retreived from as_hex(). After that, the
/// digest can be reused to calculate the hash of a new buffer sequence.
class Digest {
 public:
  Digest();

  Digest(const Digest&) = delete;
  Digest(Digest&&) = delete;

  /// Return the digest as a hex-encoded string.
  std::string as_hex() const;

 private:
  friend class Batch;
  MD5_HASH_CTX ctx;
  boost::asio::any_completion_handler<void(error_code)> handler;
  using WorkExecutor = boost::asio::any_io_executor;
  std::optional<WorkExecutor> work;
};

/// An asynchronous hash manager that uses batching to take advantage of
/// vectorized hash calculations. The best supported implementation of
/// SSE/AVX/AVX2/AVX512 is selected at runtime.
///
/// Hash requests submitted via async_hash() may be delayed until a full batch
/// is ready to process. The exact batch size is between 8 and 32, but depends
/// on the implementation.
///
/// An optional batch_timeout can be specified in the constructor to force the
/// processing of an incomplete batch instead of waiting indefinitely. Member
/// function async_flush() can also be called to force the processessing of an
/// incomplete batch.
class Batch {
 public:
  using executor_type = boost::asio::any_io_executor;
  executor_type get_executor() const noexcept { return ex; }

  /// Construct the manager to run on the given executor.
  Batch(const executor_type& ex,
        std::chrono::nanoseconds batch_timeout);

  /// Issue an asynchronous request to hash the input bytes and update the
  /// digest. Pass last=true to indicate that the digest should be finalized.
  /// After completion, the hex-encoded digest can be read via Digest::as_hex().
  template <boost::asio::completion_token_for<void(error_code)> CompletionToken>
  auto async_hash(Digest& digest, std::string_view input,
                  bool last, CompletionToken&& token)
  {
    BOOST_ASIO_HANDLER_LOCATION((__FILE__, __LINE__, __func__));
    return boost::asio::async_initiate<CompletionToken, void(error_code)>(
        initiate_async_hash{this}, token, std::ref(digest), input, last);
  }

  /// Issue an asynchronous request to flush all pending requests without
  /// waiting for a full batch.
  template <boost::asio::completion_token_for<void()> CompletionToken>
  auto async_flush(CompletionToken&& token)
  {
    BOOST_ASIO_HANDLER_LOCATION((__FILE__, __LINE__, __func__));
    return boost::asio::async_initiate<CompletionToken, void()>(
        initiate_async_flush{this}, token);
  }

 private:
  MD5_HASH_CTX_MGR mgr;
  executor_type ex;
  boost::asio::steady_timer timer;
  uint32_t pending_count = 0;
  std::chrono::nanoseconds batch_timeout;

  void complete(MD5_HASH_CTX* ctx, MD5_HASH_CTX* submit_ctx);

  void flush(MD5_HASH_CTX* submit_ctx);

  void init_async_hash(boost::asio::any_completion_handler<void(error_code)> handler,
                       Digest& digest, std::string_view input, bool last);

  struct initiate_async_hash {
    Batch* self;

    template <typename Handler>
    void operator()(Handler&& handler,
                    std::reference_wrapper<Digest> digest,
                    std::string_view input, bool last)
    {
      // maintain work on the default executor
      digest.get().work.emplace(boost::asio::prefer(
          self->ex, boost::asio::execution::outstanding_work.tracked));
      BOOST_ASIO_HANDLER_LOCATION((__FILE__, __LINE__, "initiate_async_hash"));
      boost::asio::dispatch(self->ex,
        [self=self, h=std::move(handler), digest, input, last] () mutable {
          self->init_async_hash(std::move(h), digest, input, last);
        });
    }
  };

  struct initiate_async_flush {
    Batch* self;

    template <typename Handler>
    void operator()(Handler&& handler)
    {
      BOOST_ASIO_HANDLER_LOCATION((__FILE__, __LINE__, "initiate_async_flush"));
      boost::asio::dispatch(self->ex,
        [self=self, h=std::move(handler)] () mutable {
          self->flush(nullptr);
          boost::asio::post(boost::asio::bind_executor(self->ex, std::move(h)));
        });
    }
  };
};

} // namespace ceph::async_md5
