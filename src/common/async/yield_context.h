// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <variant>

#include <boost/range/begin.hpp>
#include <boost/range/end.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/system/error_code.hpp>

#include "common/async/blocked_completion.h"

#include <spawn/spawn.hpp>

/// optional-like wrapper for a spawn::yield_context and its associated
/// boost::asio::io_context. operations that take an optional_yield argument
/// will, when passed a non-empty yield context, suspend this coroutine instead
/// of the blocking the thread of execution
class optional_yield {
  static std::function<void()> checker;

  using cy = std::pair<boost::asio::io_context*, spawn::yield_context&>;
  using cye = std::pair<boost::asio::io_context*, spawn::yield_context>;

  std::variant<cy, cye, ceph::async::use_blocked_t> v;
  /// construct with a valid io and yield_context
  optional_yield(spawn::yield_context&& y, boost::asio::io_context& c) noexcept
    : v(cye(&c, std::move(y))) {}
 public:
  /// construct with a valid io and yield_context
  optional_yield(boost::asio::io_context& c, spawn::yield_context& y) noexcept
    : v(cy(&c, y)) {}

  /// type tag to construct an empty object
  struct empty_t {
    optional_yield operator[](boost::system::error_code& ec) const noexcept {
      auto y = optional_yield(empty_t{});
      std::get<ceph::async::use_blocked_t>(y.v) = ceph::async::use_blocked[ec];
      return y;
    }
  };
  optional_yield(empty_t) noexcept
    : v(ceph::async::use_blocked) {}


  optional_yield(const optional_yield&) = default;
  optional_yield& operator =(const optional_yield& rhs) {
    // Hideous way to get around types with references having no copy
    // assignment operators.
    v.~variant<cy, cye, ceph::async::use_blocked_t>();
    new (&v) std::variant<cy, cye, ceph::async::use_blocked_t>(rhs.v);
    return *this;
  }
  optional_yield(optional_yield&&) = default;
  optional_yield& operator =(optional_yield&& rhs) {
    v.~variant<cy, cye, ceph::async::use_blocked_t>();
    new (&v) std::variant<cy, cye, ceph::async::use_blocked_t>(std::move(rhs.v));
    return *this;
  }

  /// implicit conversion to bool, returns true if non-empty
  operator bool() const noexcept {
    return (std::holds_alternative<cy>(v) || std::holds_alternative<cye>(v));
  }

  /// Bind an error code to the completion
  optional_yield operator[](boost::system::error_code& ec) const noexcept {
    if (*this) {
      return optional_yield(get_yield_context()[ec],
			    get_io_context());
    } else {
      return empty_t{}[ec];
    }
  }

  /// return a reference to the associated io_context. only valid if non-empty
  boost::asio::io_context& get_io_context() const noexcept {
    if (std::holds_alternative<cy>(v)) {
      return *std::get<cy>(v).first;
    } else if (std::holds_alternative<cye>(v)) {
      return *std::get<cye>(v).first;
    } else {
      std::terminate();
    }
  }

  /// return a reference to the yield_context. only valid if non-empty
  spawn::yield_context& get_yield_context() noexcept {
    if (std::holds_alternative<cy>(v)) {
      return std::get<cy>(v).second;
    } else if (std::holds_alternative<cye>(v)) {
      return std::get<cye>(v).second;
    } else {
      std::terminate();
    }
  }

  /// return a reference to the yield_context. only valid if non-empty
  const spawn::yield_context& get_yield_context() const noexcept {
    if (std::holds_alternative<cy>(v)) {
      return std::get<cy>(v).second;
    } else if (std::holds_alternative<cye>(v)) {
      return std::get<cye>(v).second;
    } else {
      std::terminate();
    }
  };

  /// Get blocking continuation, only valid if empty.
  ceph::async::use_blocked_t& get_blocked() noexcept {
    return std::get<ceph::async::use_blocked_t>(v);
  }

  // Set the checker for yield contexts.
  //
  // WARNING: This must be called at initialization before
  // before optional_yields completions are queued.
  static void set_checker(std::function<void()> f) {
    checker = std::move(f);
  }

  /// Call the checker for yield contexts
  static void check() {
    if (checker)
      checker();
  }
};

// type tag object to construct an empty optional_yield
inline constexpr optional_yield::empty_t null_yield{};

namespace boost::asio {
template<typename ReturnType, typename... Args>
class async_result<optional_yield, ReturnType(Args...)>
{
  using yield_result = async_result<spawn::yield_context, void(Args...)>;
  using blocked_result = async_result<ceph::async::use_blocked_t,
				       ReturnType(Args...)>;

  using retvar = std::variant<yield_result, blocked_result>;
  retvar r;

public:
  class completion_handler_type {
    friend async_result;
    using yield_handler =
      typename async_result<spawn::yield_context,
			    void(Args...)>::completion_handler_type;
    using blocked_handler =
      typename async_result<ceph::async::use_blocked_t,
			    void(Args...)>::completion_handler_type;

    using v_type = std::variant<yield_handler, blocked_handler>;
    v_type v;

  public:
    completion_handler_type(optional_yield y)
      : v(y ?
	  v_type(yield_handler(y.get_yield_context())) :
	  v_type(blocked_handler(y.get_blocked()))) {}

    template<typename... Args2>
    void operator ()(Args2&&... args) noexcept {
      if (std::holds_alternative<
	    typename async_result<ceph::async::use_blocked_t,
	                          void(Args...)>::completion_handler_type>(v)) {
	optional_yield::check();
      }
      std::visit([&](auto& h) {
	h(std::forward<Args2>(args)...);
      }, v);
    }
  };
  using return_type = typename async_result<spawn::yield_context,
					    void(Args...)>::return_type;
private:
  static retvar build(completion_handler_type& h) {
    using yh = typename completion_handler_type::yield_handler;
    using bh = typename completion_handler_type::blocked_handler;
    if (std::holds_alternative<yh>(h.v)) {
      return retvar(std::in_place_type<yield_result>, std::get<yh>(h.v));
    } else {
      return retvar(std::in_place_type<blocked_result>, std::get<bh>(h.v));
    }
  }

public:

  explicit async_result(completion_handler_type& h)
    : r(build(h)) {}

  return_type get() {
    return std::visit([](auto&& v) { return v.get(); }, r);
  }
};

template<typename ReturnType, typename... Args>
class async_result<optional_yield::empty_t, ReturnType(Args...)>
  : public async_result<optional_yield, ReturnType(Args...)>
{
public:
  class completion_handler_type
    : public async_result<optional_yield,
			  ReturnType(Args...)>::completion_handler_type
  {
  public:
    completion_handler_type(const optional_yield::empty_t&)
      : async_result<optional_yield,
		     ReturnType(Args...)>::completion_handler_type(null_yield)
      {}
  };


  explicit async_result(completion_handler_type& h)
    : async_result<optional_yield, ReturnType(Args...)>(h) {}
};
}
