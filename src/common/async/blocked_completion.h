// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#ifndef CEPH_COMMON_ASYNC_BLOCKED_COMPLETION_H
#define CEPH_COMMON_ASYNC_BLOCKED_COMPLETION_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <type_traits>

#include <boost/asio/async_result.hpp>

#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>

namespace ceph::async {

namespace bs = boost::system;

class use_blocked_t {
  use_blocked_t(bs::error_code* ec) : ec(ec) {}
public:
  use_blocked_t() = default;

  use_blocked_t operator [](bs::error_code& _ec) const {
    return use_blocked_t(&_ec);
  }

  bs::error_code* ec = nullptr;
};

inline constexpr use_blocked_t use_blocked;

namespace detail {

template<typename... Ts>
struct blocked_handler
{
  blocked_handler(use_blocked_t b) noexcept : ec(b.ec) {}

  void operator ()(Ts... values) noexcept {
    std::scoped_lock l(*m);
    *ec = bs::error_code{};
    *value = std::forward_as_tuple(std::move(values)...);
    *done = true;
    cv->notify_one();
  }

  void operator ()(bs::error_code ec, Ts... values) noexcept {
    std::scoped_lock l(*m);
    *this->ec = ec;
    *value = std::forward_as_tuple(std::move(values)...);
    *done = true;
    cv->notify_one();
  }

  bs::error_code* ec;
  std::optional<std::tuple<Ts...>>* value = nullptr;
  std::mutex* m = nullptr;
  std::condition_variable* cv = nullptr;
  bool* done = nullptr;
};

template<typename T>
struct blocked_handler<T>
{
  blocked_handler(use_blocked_t b) noexcept : ec(b.ec) {}

  void operator ()(T value) noexcept {
    std::scoped_lock l(*m);
    *ec = bs::error_code();
    *this->value = std::move(value);
    *done = true;
    cv->notify_one();
  }

  void operator ()(bs::error_code ec, T value) noexcept {
    std::scoped_lock l(*m);
    *this->ec = ec;
    *this->value = std::move(value);
    *done = true;
    cv->notify_one();
  }

  //private:
  bs::error_code* ec;
  std::optional<T>* value;
  std::mutex* m = nullptr;
  std::condition_variable* cv = nullptr;
  bool* done = nullptr;
};

template<>
struct blocked_handler<void>
{
  blocked_handler(use_blocked_t b) noexcept : ec(b.ec) {}

  void operator ()() noexcept {
    std::scoped_lock l(*m);
    *ec = bs::error_code{};
    *done = true;
    cv->notify_one();
  }

  void operator ()(bs::error_code ec) noexcept {
    std::scoped_lock l(*m);
    *this->ec = ec;
    *done = true;
    cv->notify_one();
  }

  bs::error_code* ec;
  std::mutex* m = nullptr;
  std::condition_variable* cv = nullptr;
  bool* done = nullptr;
};


template<typename... Ts>
class blocked_result
{
public:
  using completion_handler_type = blocked_handler<Ts...>;
  using return_type = std::tuple<Ts...>;

  explicit blocked_result(completion_handler_type& h) noexcept {
    out_ec = h.ec;
    if (!out_ec) h.ec = &ec;
    h.value = &value;
    h.m = &m;
    h.cv = &cv;
    h.done = &done;
  }

  return_type get() {
    std::unique_lock l(m);
    cv.wait(l, [this]() { return done; });
    if (!out_ec && ec) throw bs::system_error(ec);
    return std::move(*value);
  }

private:
  bs::error_code* out_ec;
  bs::error_code ec;
  std::optional<return_type> value;
  std::mutex m;
  std::condition_variable cv;
  bool done = false;
};

template<typename T>
class blocked_result<T>
{
public:
  using completion_handler_type = blocked_handler<T>;
  using return_type = T;

  explicit blocked_result(completion_handler_type& h) noexcept {
    out_ec = h.ec;
    if (!out_ec) h.ec = &ec;
    h.value = &value;
    h.m = &m;
    h.cv = &cv;
    h.done = &done;
  }

  return_type get() {
    std::unique_lock l(m);
    cv.wait(l, [this]() { return done; });
    if (!out_ec && ec) throw bs::system_error(ec);
    return std::move(*value);
  }

private:
  bs::error_code* out_ec;
  bs::error_code ec;
  std::optional<return_type> value;
  std::mutex m;
  std::condition_variable cv;
  bool done = false;
};

template<>
class blocked_result<void>
{
public:
  using completion_handler_type = blocked_handler<void>;
  using return_type = void;

  explicit blocked_result(completion_handler_type& h) noexcept {
    out_ec = h.ec;
    if (!out_ec) h.ec = &ec;
    h.m = &m;
    h.cv = &cv;
    h.done = &done;
  }

  void get() {
    std::unique_lock l(m);
    cv.wait(l, [this]() { return done; });
    if (!out_ec && ec) throw bs::system_error(ec);
  }

private:
  bs::error_code* out_ec;
  bs::error_code ec;
  std::mutex m;
  std::condition_variable cv;
  bool done = false;
};
} // namespace detail
} // namespace ceph::async


namespace boost::asio {
template<typename ReturnType>
class async_result<ceph::async::use_blocked_t, ReturnType()>
  : public ceph::async::detail::blocked_result<void>
{
public:
  explicit async_result(typename ceph::async::detail::blocked_result<void>
			::completion_handler_type& h)
    : ceph::async::detail::blocked_result<void>(h) {}
};

template<typename ReturnType, typename... Args>
class async_result<ceph::async::use_blocked_t, ReturnType(Args...)>
  : public ceph::async::detail::blocked_result<std::decay_t<Args>...>
{
public:
  explicit async_result(
    typename ceph::async::detail::blocked_result<std::decay_t<Args>...>::completion_handler_type& h)
    : ceph::async::detail::blocked_result<std::decay_t<Args>...>(h) {}
};

template<typename ReturnType>
class async_result<ceph::async::use_blocked_t,
		   ReturnType(boost::system::error_code)>
  : public ceph::async::detail::blocked_result<void>
{
public:
  explicit async_result(
    typename ceph::async::detail::blocked_result<void>::completion_handler_type& h)
    : ceph::async::detail::blocked_result<void>(h) {}
};

template<typename ReturnType, typename... Args>
class async_result<ceph::async::use_blocked_t,
		   ReturnType(boost::system::error_code, Args...)>
  : public ceph::async::detail::blocked_result<std::decay_t<Args>...>
{
public:
  explicit async_result(
    typename ceph::async::detail::blocked_result<std::decay_t<Args>...>::completion_handler_type& h)
    : ceph::async::detail::blocked_result<std::decay_t<Args>...>(h) {}
};
}

#endif // !CEPH_COMMON_ASYNC_BLOCKED_COMPLETION_H
