// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 * Licensed under the GNU Lesser General Public License version 2.1.
 * See file COPYING.
 */

#ifndef CEPH_RGW_FDB_EXPERIMENTAL_ASIO_COMPLETION_H
 #define CEPH_RGW_FDB_EXPERIMENTAL_ASIO_COMPLETION_H

#include <concepts>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <stop_token>
#include <tuple>
#include <utility>

#include <boost/asio/bind_allocator.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/post.hpp>
#include <boost/system/system_error.hpp>

namespace ceph::libfdb::experimental::detail {

template <boost::asio::cancellation_type Accepted =
           boost::asio::cancellation_type::terminal | boost::asio::cancellation_type::partial>
struct stop_on_cancellation final
{
 std::stop_source source;

 void operator()(const boost::asio::cancellation_type type) const noexcept
 {
  if (boost::asio::cancellation_type::none != (type & Accepted)) {
   source.request_stop();
  }
 }

};

inline std::exception_ptr operation_aborted_exception() noexcept
try
{
 throw boost::system::system_error {boost::asio::error::operation_aborted};
}
catch (...)
{
 return std::current_exception();
}

// Retain ownership for enqueue failure, but drop that copy before delivery can
// release operation storage. The queued owner keeps its mutex alive.
//
// Project the mutex from its owner and keep the callable compressed with it.
// On the measured Boost/glibc setup, an extra mutex-pointer capture moved this
// allocation to another size class and prevented reuse with FDB request tasks.
// That cross-thread allocator traffic regressed ready reads under concurrency.
template <auto Mutex, typename ExecutorT, typename AllocatorT, typename StateT,
          std::copy_constructible FnT>
void post_completion(const ExecutorT& executor, const AllocatorT& allocator,
                     std::shared_ptr<StateT> owner, FnT complete) noexcept
{
 std::unique_lock posting {(*owner).*Mutex};
 {
  auto retained = std::tuple {std::move(owner), std::move(complete)};
  try {
   boost::asio::post(executor, boost::asio::bind_allocator(allocator,
     [state = retained]() mutable {
      auto& [owner, fn] = state;
      { std::lock_guard posted {(*owner).*Mutex}; }
      std::invoke(fn, std::move(owner), std::exception_ptr {});
     }));
  } catch (...) {
   posting.unlock();
   try {
    auto& [owner, fn] = retained;
    std::invoke(fn, std::move(owner), std::current_exception());
   } catch (...) {
    std::terminate();
   }
   return;
  }
 }
 posting.unlock();
}

} // namespace ceph::libfdb::experimental::detail

#endif
