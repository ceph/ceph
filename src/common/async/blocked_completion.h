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

#include <condition_variable>
#include <mutex>
#include <optional>

#include <boost/asio/async_result.hpp>
#include <boost/asio/redirect_error.hpp>

#include <boost/system/error_code.hpp>

#include <common/async/concepts.h>

namespace ceph::async {

namespace bs = boost::system;

class use_blocked_t {
public:
  use_blocked_t() = default;

  auto operator [](bs::error_code& ec) const {
    return boost::asio::redirect_error(use_blocked_t{}, ec);
  }
};

inline constexpr use_blocked_t use_blocked;

namespace detail {
// Obnoxiously repetitive, but it cuts down on the amount of
// copying/moving/splicing/concatenating of tuples I need to do.
template<typename ...Ts>
struct blocked_handler
{
  blocked_handler(std::optional<std::tuple<Ts...>>* vals, std::mutex* m,
		  std::condition_variable* cv, bool* done)
    : vals(vals), m(m), cv(cv), done(done) {
  }

  template<typename ...Args>
  void operator ()(Args&& ...args) noexcept {
    static_assert(sizeof...(Ts) == sizeof...(Args));
    std::scoped_lock l(*m);
    *vals = std::tuple<Ts...>(std::forward<Args>(args)...);
    *done = true;
    cv->notify_one();
  }

  //private:
  std::optional<std::tuple<Ts...>>* vals;
  std::mutex* m = nullptr;
  std::condition_variable* cv = nullptr;
  bool* done = nullptr;
};

template<boost::asio::disposition D, typename ...Ts>
struct blocked_handler<D, Ts...>
{
  blocked_handler(D* dispo, std::optional<std::tuple<Ts...>>* vals,
		  std::mutex* m, std::condition_variable* cv, bool* done)
    : dispo(dispo), vals(vals), m(m), cv(cv), done(done) {
  }

  template<typename Arg0, typename... Args>
  void operator ()(Arg0&& arg0, Args&& ...args) noexcept {
    static_assert(sizeof...(Ts) == sizeof...(Args));
    std::scoped_lock l(*m);
    *dispo = std::move(arg0);
    *vals = std::tuple<Ts...>(std::forward<Args>(args)...);
    *done = true;
    cv->notify_one();
  }

  //private:
  D* dispo;
  std::optional<std::tuple<Ts...>>* vals;
  std::mutex* m = nullptr;
  std::condition_variable* cv = nullptr;
  bool* done = nullptr;
};

template<typename T>
struct blocked_handler<T>
{
  blocked_handler(std::optional<T>* val, std::mutex* m,
		  std::condition_variable* cv, bool* done)
    : val(val), m(m), cv(cv), done(done) {}

  template<typename Arg>
  void operator ()(Arg&& arg) noexcept {
    std::scoped_lock l(*m);
    *val = std::forward<Arg>(arg);
    *done = true;
    cv->notify_one();
  }

  //private:
  std::optional<T>* val;
  std::mutex* m = nullptr;
  std::condition_variable* cv = nullptr;
  bool* done = nullptr;
};

template<boost::asio::disposition D, typename T>
struct blocked_handler<D, T>
{
  blocked_handler(D* dispo, std::optional<T>* val, std::mutex* m,
		  std::condition_variable* cv, bool* done)
    : dispo(dispo), val(val), m(m), cv(cv), done(done) {}

  template<typename Arg0, typename Arg>
  void operator ()(Arg0&& arg0, Arg&& arg) noexcept {
    std::scoped_lock l(*m);
    *dispo = std::move(arg0);
    *val = std::move(arg);
    *done = true;
    cv->notify_one();
  }

  //private:
  D* dispo;
  std::optional<T>* val;
  std::mutex* m = nullptr;
  std::condition_variable* cv = nullptr;
  bool* done = nullptr;
};

template<>
struct blocked_handler<void>
{
  blocked_handler(std::mutex* m, std::condition_variable* cv, bool* done)
    : m(m), cv(cv), done(done) {}

  void operator ()() noexcept {
    std::scoped_lock l(*m);
    *done = true;
    cv->notify_one();
  }

  std::mutex* m = nullptr;
  std::condition_variable* cv = nullptr;
  bool* done = nullptr;
};

template<boost::asio::disposition D>
struct blocked_handler<D>
{
  blocked_handler(D* dispo, std::mutex* m,
		  std::condition_variable* cv, bool* done)
    : dispo(dispo), m(m), cv(cv), done(done) {}

  template<typename Arg0>
  void operator ()(Arg0&& arg0) noexcept {
    std::scoped_lock l(*m);
    *dispo = std::move(arg0);
    *done = true;
    cv->notify_one();
  }

  //private:
  D* dispo;
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

  template<typename Initiation, typename... Args>
  static return_type initiate(Initiation&& init,
			      use_blocked_t,
			      Args&& ...args) {
    using ttype = std::tuple<Ts...>;
    std::optional<ttype> vals;
    std::mutex m;
    std::condition_variable cv;
    bool done = false;
    static_assert(std::tuple_size_v<ttype> > 1);

    std::move(init)(completion_handler_type(&vals, &m, &cv, &done),
		    std::forward<Args>(args)...);

    std::unique_lock l(m);
    cv.wait(l, [&done]() { return done; });
    return std::move(*vals);
  }

  blocked_result(const blocked_result&) = delete;
  blocked_result& operator =(const blocked_result&) = delete;
  blocked_result(blocked_result&&) = delete;
  blocked_result& operator =(blocked_result&&) = delete;
};

template<boost::asio::disposition D, typename... Ts>
class blocked_result<D, Ts...>
{
public:
  using completion_handler_type = blocked_handler<D, Ts...>;
  using return_type = std::tuple<Ts...>;

  template<typename Initiation, typename... Args>
  static return_type initiate(Initiation&& init,
			      use_blocked_t,
			      Args&& ...args) {
    using ttype = std::tuple<Ts...>;
    std::optional<ttype> vals;
    D dispo;
    std::mutex m;
    std::condition_variable cv;
    bool done = false;
    static_assert(std::tuple_size_v<ttype> > 1);

    std::move(init)(completion_handler_type(&dispo, &vals, &m, &cv, &done),
		    std::forward<Args>(args)...);

    std::unique_lock l(m);
    cv.wait(l, [&done]() { return done; });
    if (dispo != boost::asio::no_error) {
      boost::asio::disposition_traits<D>::throw_exception(dispo);
    }
    return std::move(*vals);
  }

  blocked_result(const blocked_result&) = delete;
  blocked_result& operator =(const blocked_result&) = delete;
  blocked_result(blocked_result&&) = delete;
  blocked_result& operator =(blocked_result&&) = delete;
};

template<typename T>
class blocked_result<T>
{
public:
  using completion_handler_type = blocked_handler<T>;
  using return_type = T;

  template<typename Initiation, typename... Args>
  static return_type initiate(Initiation&& init,
			      use_blocked_t,
			      Args&& ...args) {
    std::optional<T> val;
    std::mutex m;
    std::condition_variable cv;
    bool done = false;

    std::move(init)(completion_handler_type(&val, &m, &cv, &done),
		    std::forward<Args>(args)...);

    std::unique_lock l(m);
    cv.wait(l, [&done]() { return done; });
    return std::move(*val);
  }

  blocked_result(const blocked_result&) = delete;
  blocked_result& operator =(const blocked_result&) = delete;
  blocked_result(blocked_result&&) = delete;
  blocked_result& operator =(blocked_result&&) = delete;
};

template<boost::asio::disposition D, typename T>
class blocked_result<D, T>
{
public:
  using completion_handler_type = blocked_handler<D, T>;
  using return_type = T;

  template<typename Initiation, typename... Args>
  static return_type initiate(Initiation&& init,
			      use_blocked_t,
			      Args&& ...args) {
    D dispo;
    std::optional<T> val;
    std::mutex m;
    std::condition_variable cv;
    bool done = false;

    std::move(init)(completion_handler_type(&dispo, &val, &m, &cv, &done),
		    std::forward<Args>(args)...);

    std::unique_lock l(m);
    cv.wait(l, [&done]() { return done; });
    if (dispo != boost::asio::no_error) {
      boost::asio::disposition_traits<D>::throw_exception(dispo);
    }
    return std::move(*val);
  }

  blocked_result(const blocked_result&) = delete;
  blocked_result& operator =(const blocked_result&) = delete;
  blocked_result(blocked_result&&) = delete;
  blocked_result& operator =(blocked_result&&) = delete;
};

template<>
class blocked_result<void>
{
public:
  using completion_handler_type = blocked_handler<void>;
  using return_type = void;

  template<typename Initiation, typename... Args>
  static return_type initiate(Initiation&& init,
			      use_blocked_t,
			      Args&& ...args) {
    std::mutex m;
    std::condition_variable cv;
    bool done = false;

    std::move(init)(completion_handler_type(&m, &cv, &done),
		    std::forward<Args>(args)...);

    std::unique_lock l(m);
    cv.wait(l, [&done]() { return done; });
    return;
  }

  blocked_result(const blocked_result&) = delete;
  blocked_result& operator =(const blocked_result&) = delete;
  blocked_result(blocked_result&&) = delete;
  blocked_result& operator =(blocked_result&&) = delete;
};


template<boost::asio::disposition D>
class blocked_result<D>
{
public:
  using completion_handler_type = blocked_handler<D>;
  using return_type = void;

  template<typename Initiation, typename... Args>
  static return_type initiate(Initiation&& init,
			      use_blocked_t,
			      Args&& ...args) {
    D dispo;
    std::mutex m;
    std::condition_variable cv;
    bool done = false;

    std::move(init)(completion_handler_type(&dispo, &m, &cv, &done),
		    std::forward<Args>(args)...);

    std::unique_lock l(m);
    cv.wait(l, [&done]() { return done; });
    if (dispo != boost::asio::no_error) {
      boost::asio::disposition_traits<D>::throw_exception(dispo);
    }
    return;
  }

  blocked_result(const blocked_result&) = delete;
  blocked_result& operator =(const blocked_result&) = delete;
  blocked_result(blocked_result&&) = delete;
  blocked_result& operator =(blocked_result&&) = delete;
};

} // namespace detail
} // namespace ceph::async


namespace boost::asio {
template<typename R>
class async_result<ceph::async::use_blocked_t, R()>
  : public ceph::async::detail::blocked_result<void>
{
};

template<typename R, disposition D>
class async_result<ceph::async::use_blocked_t, R(D)>
  : public ceph::async::detail::blocked_result<D>
{
};

template<typename R, typename Arg>
class async_result<ceph::async::use_blocked_t, R(Arg)>
  : public ceph::async::detail::blocked_result<Arg>
{
};

template<typename R, disposition D, typename Arg>
class async_result<ceph::async::use_blocked_t, R(D, Arg)>
  : public ceph::async::detail::blocked_result<D, Arg>
{
};

template<typename R, typename... Args>
class async_result<ceph::async::use_blocked_t, R(Args...)>
  : public ceph::async::detail::blocked_result<Args...>
{
};

template<typename R, disposition D, typename... Args>
class async_result<ceph::async::use_blocked_t, R(D, Args...)>
  : public ceph::async::detail::blocked_result<D, Args...>
{
};
}

#endif // !CEPH_COMMON_ASYNC_BLOCKED_COMPLETION_H
