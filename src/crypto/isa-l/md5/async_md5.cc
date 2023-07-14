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

#include "async_md5.h"

#include <bit>
#include <concepts>
#include <span>
#include <boost/asio/append.hpp>
#include <boost/asio/dispatch.hpp>

namespace ceph::async_md5 {

boost::system::error_category& hash_category()
{
  static struct category : boost::system::error_category {
    const char* name() const noexcept override { return "isal-hash"; }
    std::string message(int code) const override {
      switch (code) {
        case HASH_CTX_ERROR_NONE: return "none";
        case HASH_CTX_ERROR_INVALID_FLAGS: return "invalid flags";
        case HASH_CTX_ERROR_ALREADY_PROCESSING: return "already processing";
        case HASH_CTX_ERROR_ALREADY_COMPLETED: return "already completed";
        default: return "unknown";
      }
    }
  } instance;
  return instance;
}


Digest::Digest()
{
  hash_ctx_init(&ctx);
  ctx.user_data = this;
}

Digest& digest_from_hash_ctx(MD5_HASH_CTX* ctx)
{
  return *reinterpret_cast<Digest*>(ctx->user_data);
}

// copy the given byte as two hex characters to output
template <std::output_iterator<char> Iterator>
Iterator hex_encode(Iterator out, unsigned char b)
{
  static constexpr char hexdigits[16] = {
    '0', '1', '2', '3', '4', '5', '6', '7',
    '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
  };
  *out++ = hexdigits[b >> 4]; // high 4 bits
  *out++ = hexdigits[b & 0xf]; // low 4 bits
  return out;
}

static std::string to_hex(std::span<const MD5_WORD_T> digest)
{
  auto result = std::string(32, '\0');
  std::string::iterator out = result.begin();

  for (const MD5_WORD_T w : digest) {
    if constexpr (std::endian::native == std::endian::big) {
      out = hex_encode(out, w >> 24);
      out = hex_encode(out, w >> 16);
      out = hex_encode(out, w >> 8);
      out = hex_encode(out, w >> 0);
    } else {
      out = hex_encode(out, w >> 0);
      out = hex_encode(out, w >> 8);
      out = hex_encode(out, w >> 16);
      out = hex_encode(out, w >> 24);
    }
  }
  return result;
}

std::string Digest::as_hex() const
{
  return to_hex(hash_ctx_digest(&ctx));
}


Batch::Batch(const executor_type& ex, std::chrono::nanoseconds batch_timeout)
    : ex(ex), timer(ex), batch_timeout(batch_timeout)
{
  ::md5_ctx_mgr_init(&mgr);
}

void Batch::complete(MD5_HASH_CTX* ctx, MD5_HASH_CTX* submit_ctx)
{
  --pending_count;

  error_code ec;
  if (int err = hash_ctx_error(ctx);
      err != HASH_CTX_ERROR_NONE) {
    ec.assign(err, hash_category());
  }
  auto& digest = digest_from_hash_ctx(ctx);
  auto h = boost::asio::append(std::move(digest.handler), ec);
  digest.work.reset();

  if (ctx == submit_ctx) {
    // if we're completing the ctx inside of its own call to async_hash(), we
    // have to post() the completion instead
    boost::asio::post(boost::asio::bind_executor(get_executor(), std::move(h)));
  } else {
    boost::asio::dispatch(std::move(h));
  }
}

void Batch::flush(MD5_HASH_CTX* submit_ctx)
{
  BOOST_ASIO_HANDLER_LOCATION((__FILE__, __LINE__, __func__));
  while (MD5_HASH_CTX* ctx = ::md5_ctx_mgr_flush(&mgr)) {
    complete(ctx, submit_ctx);
  }
}

void Batch::init_async_hash(
    boost::asio::any_completion_handler<void(error_code)> handler,
    Digest& digest, std::string_view input, bool last)
{
  BOOST_ASIO_HANDLER_LOCATION((__FILE__, __LINE__, __func__));
  digest.handler = std::move(handler);

  int flag = HASH_UPDATE;
  if (hash_ctx_status(&digest.ctx) == HASH_CTX_STS_COMPLETE) {
    flag |= HASH_FIRST;
  }
  if (last) {
    flag |= HASH_LAST;
  }

  ++pending_count;

  // submit the ctx and maybe complete a batch
  auto submit_ctx = &digest.ctx;
  auto ctx = ::md5_ctx_mgr_submit(&mgr, submit_ctx, input.data(), input.size(),
                                  static_cast<HASH_CTX_FLAG>(flag));
  if (ctx) {
    // complete() may end up destroying ctx, so read the error first
    const auto result = hash_ctx_error(ctx);

    complete(ctx, submit_ctx);

    if (result == HASH_CTX_ERROR_NONE) {
      // on a successful completion, flush results for the rest of the batch
      flush(submit_ctx);
    }
  } else if (pending_count == 1 &&
             batch_timeout.count()) {
    // if we just started a new batch, arm the timeout
    timer.expires_after(batch_timeout);
    timer.async_wait([this] (error_code ec) {
          if (!ec) {
            flush(nullptr);
          }
        });
  }
}

} // namespace ceph::async_md5
