// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp
/*
 * Ceph - scalable distributed file system
 */

// Copyright: 2025 Contributors to the Ceph Project
// Based on boost/asio/redirect_error.hpp and
// boost/asio/impl/redirect_error.hpp which are
// Copyright (c) 2003-2024 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once

#include <type_traits>

#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/default_completion_token.hpp>
#include <boost/asio/disposition.hpp>
#include <boost/asio/handler_continuation_hook.hpp>

/// \file common/async/redirect_error.h
///
/// \brief `redirect_error` that knows about `dispositions`.
///
/// Asio has a very useful concept called a `disposition` that
/// generalizes the notion of an error code. Unfortunately
/// `redirect_error` doesn't know about dispositions, making it less
/// useful.

namespace ceph::async {

/// A @ref completion_token adapter used to specify that an error
/// produced by an asynchronous operation is captured to a specified
/// variable. The variable must be of a `disposition` type.
/**
 * The redirect_error_t class is used to indicate that any disposition produced
 * by an asynchronous operation is captured to a specified variable.
 */
template<typename CompletionToken,
         boost::asio::disposition Disposition>
class redirect_error_t {
public: // Sigh
  CompletionToken token;
  Disposition& disposition;

  template<typename CT>
  redirect_error_t(CT&& token, Disposition& disposition)
    : token(std::forward<CT>(token)), disposition(disposition) {}
};

/// A function object type that adapts a @ref completion_token to capture
/// disposition values to a variable.
/**
 * May also be used directly as a completion token, in which case it adapts the
 * asynchronous operation's default completion token (or boost::asio::deferred
 * if no default is available).
 */
template<boost::asio::disposition Disposition>
class partial_redirect_error {
public:
  Disposition& disposition;

  /// Constructor that specifies the variable used to capture disposition values.
  explicit partial_redirect_error(Disposition& disposition)
    : disposition(disposition) {}

  /// Adapt a @ref completion_token to specify that the completion handler
  /// should capture disposition values to a variable.
  template<typename CompletionToken>
  [[nodiscard]] constexpr inline auto
  operator ()(CompletionToken&& token) const {
    return redirect_error_t<std::decay_t<CompletionToken>, Disposition>{
      std::forward<CompletionToken>(token), disposition};
  }
};

/// Create a partial completion token adapter that captures disposition values
/// to a variable.
template<boost::asio::disposition Disposition>
[[nodiscard]] inline auto redirect_error(Disposition& d)
{
  return partial_redirect_error<std::decay_t<Disposition>>{d};
}

/// Adapt a @ref completion_token to capture disposition values to a variable.
template<typename CompletionToken, boost::asio::disposition Disposition>
[[nodiscard]] inline auto redirect_error(CompletionToken&& token,
                                         Disposition& d)
{
  return redirect_error_t<std::decay_t<CompletionToken>,
                          std::decay_t<Disposition>>{
    std::forward<CompletionToken>(token), d};
}

namespace detail {
template<typename Handler, boost::asio::disposition Disposition>
class redirect_error_handler {
public:
  Disposition& disposition;
  // Essentially a call-once function, invoked as an rvalue.
  Handler handler;

  using result_type = void;
  template<typename CompletionToken>
  redirect_error_handler(
    redirect_error_t<std::decay_t<CompletionToken>,
                     std::decay_t<Disposition>> re)
    : disposition(re.disposition), handler(std::move(re.token)) {}

  template<typename RedirectedHandler>
  redirect_error_handler(Disposition &disposition, RedirectedHandler&& handler)
    : disposition(disposition),
      handler(std::forward<RedirectedHandler>(handler)) {}


  void operator ()() {
    std::move(handler)();
  }

  template<typename Arg0, typename ...Args>
  std::enable_if_t<!std::is_same_v<std::decay_t<Arg0>,
				   Disposition>>
  operator ()(Arg0&& arg0, Args ...args) {
    std::move(handler)(std::forward<Arg0>(arg0), std::forward<Args>(args)...);
  }

  template<typename... Args>
  void operator ()(const Disposition& d, Args&& ...args) {
    disposition = d;
    std::move(handler)(std::forward<Args>(args)...);
  }
};

template<boost::asio::disposition Disposition, typename Handler>
inline bool asio_handler_is_continuation(
  redirect_error_handler<Disposition, Handler>* this_handler)
{
  using boost::asio::asio_handler_is_continuation;
  return asio_handler_is_continuation(&this_handler->handler);
}

template<typename Signature>
struct redirect_error_signature
{
  using type = Signature;
};

template<typename R, boost::asio::disposition Disposition, typename... Args>
struct redirect_error_signature<R(Disposition, Args...)>
{
  typedef R type(Args...);
};

template<typename R, boost::asio::disposition Disposition, typename... Args>
struct redirect_error_signature<R(const Disposition&, Args...)>
{
  typedef R type(Args...);
};

template<typename R, boost::asio::disposition Disposition, typename... Args>
struct redirect_error_signature<R(Disposition, Args...) &>
{
  typedef R type(Args...) &;
};

template<typename R, boost::asio::disposition Disposition, typename... Args>
struct redirect_error_signature<R(const Disposition&, Args...) &>
{
  typedef R type(Args...) &;
};

template<typename R, boost::asio::disposition Disposition, typename... Args>
struct redirect_error_signature<R(Disposition, Args...) &&>
{
  typedef R type(Args...) &&;
};

template<typename R, boost::asio::disposition Disposition, typename... Args>
struct redirect_error_signature<R(const Disposition&, Args...) &&>
{
  typedef R type(Args...) &&;
};

template<typename R, boost::asio::disposition Disposition, typename... Args>
struct redirect_error_signature<R(Disposition, Args...) noexcept>
{
  typedef R type(Args...) & noexcept;
};

template<typename R, boost::asio::disposition Disposition, typename... Args>
struct redirect_error_signature<R(const Disposition&, Args...) noexcept>
{
  typedef R type(Args...) & noexcept;
};

template<typename R, boost::asio::disposition Disposition, typename... Args>
struct redirect_error_signature<R(Disposition, Args...) & noexcept>
{
  typedef R type(Args...) & noexcept;
};

template <typename R, boost::asio::disposition Disposition, typename... Args>
struct redirect_error_signature<R(const Disposition&, Args...) & noexcept>
{
  typedef R type(Args...) & noexcept;
};

template<typename R, boost::asio::disposition Disposition, typename... Args>
struct redirect_error_signature<R(Disposition, Args...) && noexcept>
{
  typedef R type(Args...) && noexcept;
};

template <typename R, boost::asio::disposition Disposition, typename... Args>
struct redirect_error_signature<R(const Disposition&, Args...) && noexcept>
{
  typedef R type(Args...) && noexcept;
};

template<typename Initiation, typename = void>
class initiation_base : public Initiation
{
public:
  template<typename I>
  explicit initiation_base(I&& initiation)
    : Initiation(std::forward<I>(initiation)) {}
};

template<typename Initiation>
class initiation_base<Initiation,
		      std::enable_if_t<!std::is_class_v<Initiation>>>
{
public:
  template<typename I>
  explicit initiation_base(I&& initiation)
    : initiation(std::forward<I>(initiation)) {}

  template<typename... Args>
  void operator()(Args&&... args) const
  {
    initiation(std::forward<Args>(args)...);
  }

private:
  Initiation initiation;
};
} // namespace detail
} // namespace ceph::async

namespace boost::asio {

template<boost::asio::disposition Disposition, typename CompletionToken,
	 typename Signature>
struct async_result<::ceph::async::redirect_error_t<CompletionToken,
						    Disposition>, Signature>
  : async_result<CompletionToken,
		 typename ::ceph::async::detail::redirect_error_signature<
		   Signature>::type>
{
  template<typename Initiation>
    struct init_wrapper : ::ceph::async::detail::initiation_base<Initiation>
  {
    using ::ceph::async::detail::initiation_base<Initiation>::initiation_base;

    template<typename Handler, typename... Args>
    void operator ()(Handler&& handler, Disposition* d, Args&&... args) &&
    {
      static_cast<Initiation&&>(*this)(
	::ceph::async::detail::redirect_error_handler<
	decay_t<Handler>, Disposition>(
	  *d, std::forward<Handler>(handler)),
	std::forward<Args>(args)...);
    }

    template<typename Handler, typename... Args>
    void operator ()(Handler&& handler, Disposition* d, Args&&... args) const &
    {
      static_cast<const Initiation&>(*this)(
	::ceph::async::detail::redirect_error_handler<
	decay_t<Handler>, Disposition>(
	  *d, std::forward<Handler>(handler)),
	std::forward<Args>(args)...);
    }
  };

  template<typename Initiation, typename RawCompletionToken, typename... Args>
  static auto initiate(Initiation&& initiation,
		       RawCompletionToken&& token, Args&&... args)
  {
    return async_initiate<
      std::conditional_t<
        std::is_const_v<remove_reference_t<RawCompletionToken>>,
          const CompletionToken, CompletionToken>,
      typename ::ceph::async::detail::redirect_error_signature<Signature>::type>(
	init_wrapper<std::decay_t<Initiation>>(
	  std::forward<Initiation>(initiation)),
	token.token, &token.disposition, std::forward<Args>(args)...);
  }
};

template<template<typename, typename> class Associator, typename Handler,
	 typename DefaultCandidate, typename Disposition>
struct associator<Associator,
		  ::ceph::async::detail::redirect_error_handler<Handler,
								Disposition>,
		  DefaultCandidate>
  : Associator<Handler, DefaultCandidate>
{
  static auto get(const ::ceph::async::detail::redirect_error_handler<
		  Handler, Disposition>& h) noexcept
  {
    return Associator<Handler, DefaultCandidate>::get(h.handler);
  }

  static auto get(const ::ceph::async::detail::redirect_error_handler<
		    Handler, Disposition>& h,
		  const DefaultCandidate& c) noexcept
  {
    return Associator<Handler, DefaultCandidate>::get(h.handler, c);
  }
};

template<boost::asio::disposition Disposition, typename... Signatures>
struct async_result<::ceph::async::partial_redirect_error<Disposition>,
		     Signatures...>
{
  template <typename Initiation, typename RawCompletionToken, typename... Args>
    static auto initiate(Initiation&& initiation,
			 RawCompletionToken&& token, Args&&... args)
  {
    return async_initiate<Signatures...>(
      std::forward<Initiation>(initiation),
      ::ceph::async::redirect_error_t<
      default_completion_token_t<associated_executor_t<Initiation>>,
      Disposition>(
	default_completion_token_t<associated_executor_t<Initiation>>{},
	token.disposition), std::forward<Args>(args)...);
  }
};
} // namespace boost::asio
