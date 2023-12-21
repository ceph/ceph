// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_WAITER_H
#define CEPH_COMMON_WAITER_H

#include <condition_variable>
#include <tuple>

#include <boost/asio/async_result.hpp>

#include "include/ceph_assert.h"
#include "include/function2.hpp"

#include "common/ceph_mutex.h"

namespace ceph::async {
namespace detail {
// For safety reasons (avoiding undefined behavior around sequence
// points) std::reference_wrapper disallows move construction. This
// harms us in cases where we want to pass a reference in to something
// that unavoidably moves.
//
// It should not be used generally.
template<typename T>
class rvalue_reference_wrapper {
public:
  // types
  using type = T;

  rvalue_reference_wrapper(T& r) noexcept
    : p(std::addressof(r)) {}

  // We write our semantics to match those of reference collapsing. If
  // we're treated as an lvalue, collapse to one.

  rvalue_reference_wrapper(const rvalue_reference_wrapper&) noexcept = default;
  rvalue_reference_wrapper(rvalue_reference_wrapper&&) noexcept = default;

  // assignment
  rvalue_reference_wrapper& operator=(
    const rvalue_reference_wrapper& x) noexcept = default;
  rvalue_reference_wrapper& operator=(
    rvalue_reference_wrapper&& x) noexcept = default;

  operator T& () const noexcept {
    return *p;
  }
  T& get() const noexcept {
    return *p;
  }

  operator T&& () noexcept {
    return std::move(*p);
  }
  T&& get() noexcept {
    return std::move(*p);
  }

  template<typename... Args>
  auto operator()(Args&&... args) const {
    return (*p)(std::forward<Args>(args)...);
  }

  template<typename... Args>
  auto operator()(Args&&... args) {
    return std::move(*p)(std::forward<Args>(args)...);
  }

private:
  T* p;
};

class base {
protected:
  ceph::mutex lock = ceph::make_mutex("ceph::async::detail::base::lock");
  ceph::condition_variable cond;
  bool has_value = false;

  ~base() = default;

  auto wait_base() {
    std::unique_lock l(lock);
    cond.wait(l, [this](){ return has_value; });
    return l;
  }

  auto exec_base() {
    std::unique_lock l(lock);
    // There's no really good way to handle being called twice
    // without being reset.
    ceph_assert(!has_value);
    has_value = true;
    cond.notify_one();
    return l;
  }
};
}

// waiter is a replacement for C_SafeCond and friends. It is the
// moral equivalent of a future but plays well with a world of
// callbacks.
template<typename ...S>
class waiter;

template<>
class waiter<> final : public detail::base {
public:
  void wait() {
    wait_base();
    has_value = false;
  }

  void operator()() {
    exec_base();
  }

  auto ref() {
    return detail::rvalue_reference_wrapper(*this);
  }


  operator fu2::unique_function<void() &&>() {
    return fu2::unique_function<void() &&>(ref());
  }
};

template<typename Ret>
class waiter<Ret> final : public detail::base {
  std::aligned_storage_t<sizeof(Ret)> ret;

public:
  Ret wait() {
    auto l = wait_base();
    auto r = reinterpret_cast<Ret*>(&ret);
    auto t = std::move(*r);
    r->~Ret();
    has_value = false;
    return t;
  }

  void operator()(Ret&& _ret) {
    auto l = exec_base();
    auto r = reinterpret_cast<Ret*>(&ret);
    *r = std::move(_ret);
  }

  void operator()(const Ret& _ret) {
    auto l = exec_base();
    auto r = reinterpret_cast<Ret*>(&ret);
    *r = std::move(_ret);
  }

  auto ref() {
    return detail::rvalue_reference_wrapper(*this);
  }

  operator fu2::unique_function<void(Ret) &&>() {
    return fu2::unique_function<void(Ret) &&>(ref());
  }

  ~waiter() {
    if (has_value)
      reinterpret_cast<Ret*>(&ret)->~Ret();
  }
};

template<typename ...Ret>
class waiter final : public detail::base {
  std::tuple<Ret...> ret;

public:
  std::tuple<Ret...> wait() {
    using std::tuple;
    auto l = wait_base();
    return std::move(ret);
    auto r = reinterpret_cast<std::tuple<Ret...>*>(&ret);
    auto t = std::move(*r);
    r->~tuple<Ret...>();
    has_value = false;
    return t;
  }

  void operator()(Ret&&... _ret) {
    auto l = exec_base();
    auto r = reinterpret_cast<std::tuple<Ret...>*>(&ret);
    *r = std::forward_as_tuple(_ret...);
  }

  void operator()(const Ret&... _ret) {
    auto l = exec_base();
    auto r = reinterpret_cast<std::tuple<Ret...>*>(&ret);
    *r = std::forward_as_tuple(_ret...);
  }

  auto ref() {
    return detail::rvalue_reference_wrapper(*this);
  }

  operator fu2::unique_function<void(Ret...) &&>() {
    return fu2::unique_function<void(Ret...) &&>(ref());
  }

  ~waiter() {
    using std::tuple;
    if (has_value)
      reinterpret_cast<tuple<Ret...>*>(&ret)->~tuple<Ret...>();
  }
};
}

#endif // CEPH_COMMON_WAITER_H
