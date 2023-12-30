// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <algorithm>
#include <string>

#include <boost/asio/any_completion_handler.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/execution/outstanding_work.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/prefer.hpp>

#include "common/async/yield_context.h"

#include "rgw_putobj.h"

namespace rgw::putobj {

/// Finalize a digest and return it as a hex-encoded string.
template <typename Digest>
std::string finalize(Digest& digest)
{
  unsigned char buf[Digest::digest_size];
  digest.Final(buf);

  std::string hex;
  hex.resize(Digest::digest_size * 2);

  auto out = hex.begin();
  std::for_each(std::begin(buf), std::end(buf),
      [&out] (unsigned char c) {
        constexpr auto table = std::string_view{"0123456789abcdef"};
        *out++ = table[c >> 4]; // high 4 bits
        *out++ = table[c & 0xf]; // low 4 bits
      });
  return hex;
}

/// A streaming data processor that performs inline hashing of incoming
/// bytes before forwarding them to the wrapped processor. When the processor
/// is flushed by calling process() with an empty buffer, the final sum is
/// copied to the given output string.
template <typename Digest>
class HashPipe : public putobj::Pipe {
  std::string& output;
  Digest digest;

 public:
  template <typename ...DigestArgs>
  HashPipe(sal::DataProcessor* next, std::string& output, DigestArgs&& ...args)
    : Pipe(next), output(output), digest(std::forward<DigestArgs>(args)...)
  {}

  int process(bufferlist&& data, uint64_t logical_offset) override
  {
    if (data.length() == 0) {
      // flush the pipe and finalize the digest
      output = finalize(digest);
    } else {
      // hash each buffer segment
      for (const auto& ptr : data.buffers()) {
        digest.Update(reinterpret_cast<const unsigned char*>(ptr.c_str()),
                      ptr.length());
      }
    }

    return Pipe::process(std::move(data), logical_offset);
  }
};

/// A streaming data processor that offloads the hashing of incoming bytes to
/// another executor while forwarding them to the wrapped processor.
///
/// Because the hashing is asynchronous with respect to process(), the data from
/// later process() calls may need to be buffered until the previous hashes
/// complete. The total amount of buffered memory is limited to the given
/// window_size.
///
/// When the processor is flushed by calling process() with an empty buffer,
/// the final sum is copied to the given output string.
///
/// This class is not thread-safe. All members functions must be called from
/// within the yield_context's strand executor.
template <typename Digest>
class AsyncHashPipe : public putobj::Pipe {
 public:
  template <typename ...DigestArgs>
  AsyncHashPipe(sal::DataProcessor* next,
                spawn::yield_context yield,
                boost::asio::any_io_executor hash_executor,
                size_t window_size,
                std::string& output,
                DigestArgs&& ...args)
    : Pipe(next), yield(yield), hash_executor(hash_executor),
      window_size(window_size), output(output),
      digest(std::forward<DigestArgs>(args)...)
  {}

  ~AsyncHashPipe()
  {
    // stop submitting new hash requests and await pending completion
    pending.clear();
    if (outstanding) {
      async_wait(yield);
    }
  }

  int process(bufferlist&& data, uint64_t logical_offset) override
  {
    if (data.length() == 0) {
      // wait for all pending hashes to complete
      while (outstanding) {
        async_wait(yield);
      }

      // finalize the digest
      output = finalize(digest);

      return Pipe::process(std::move(data), logical_offset);
    }

    // block if the window is full
    while (pending.length() + outstanding > 0 &&
           pending.length() + outstanding + data.length() > window_size) {
      async_wait(yield);
    }
    pending.append(data);

    if (!outstanding) {
      submit(std::move(pending));
    }

    return Pipe::process(std::move(data), logical_offset);
  }

 private:
  spawn::yield_context yield;
  boost::asio::any_io_executor hash_executor;
  const size_t window_size;
  std::string& output;
  Digest digest;

  // list of buffer segments pending submission
  bufferlist pending;
  // number of submitted bytes pending completion
  size_t outstanding = 0;
  // completion handler for async_wait(), if any
  boost::asio::any_completion_handler<void()> waiter;

  // return the coroutine's associated strand executor
  static auto get_strand_executor(spawn::yield_context yield)
  {
    boost::asio::async_completion<spawn::yield_context, void()> init(yield);
    return get_associated_executor(init.completion_handler);
  }

  // submit buffers for async hashing
  void submit(bufferlist&& bl)
  {
    ceph_assert(!outstanding);
    outstanding = bl.length();

    // bind a completion handler to the coroutine's strand executor
    auto handler = boost::asio::bind_executor(
        boost::asio::prefer(get_strand_executor(yield),
                            boost::asio::execution::outstanding_work.tracked),
        [this] { complete(); });

    // offload the hashing to the requested hash_executor
    boost::asio::post(hash_executor,
        [&d = digest, bl = std::move(bl), h = std::move(handler)] () mutable {
          // hash each buffer segment
          for (const auto& ptr : bl.buffers()) {
            d.Update(reinterpret_cast<const unsigned char*>(ptr.c_str()),
                     ptr.length());
          }
          // dispatch the completion back to its executor
          boost::asio::dispatch(std::move(h));
        });
  }

  // hash completion callback
  void complete()
  {
    ceph_assert(outstanding);
    outstanding = 0;

    if (pending.length()) {
      // submit pending buffers
      submit(std::move(pending));
    }

    if (waiter) { // wake if blocked
      boost::asio::dispatch(std::move(waiter));
    }
  }

  // capture the completion handler in `waiter`
  template <typename CompletionToken>
  auto async_wait(CompletionToken&& token)
  {
    return boost::asio::async_initiate<CompletionToken, void()>(
        [this] (auto&& handler) {
          waiter = std::move(handler);
        }, token);
  }
};

} // namespace rgw::putobj
