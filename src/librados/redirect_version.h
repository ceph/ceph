// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include <boost/asio/associator.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/system/error_code.hpp>
#include "include/types.h"

namespace librados {

template <typename CompletionToken>
struct redirect_version_t {
  [[no_unique_address]] CompletionToken token;
  version_t* ver = nullptr;
};

/// A completion token adapter that modifies the completion signature by
/// removing the version_t argument, optionally redirecting its value into
/// the given pointer.
template <typename CompletionToken>
auto redirect_version(CompletionToken&& token, version_t* ver = nullptr)
    -> redirect_version_t<CompletionToken> {
  return {std::forward<CompletionToken>(token), ver};
}

namespace detail {

template <typename Handler>
struct redirect_version_handler {
  [[no_unique_address]] Handler handler;
  version_t* ver = nullptr;

  void operator()(boost::system::error_code ec, version_t v) {
    if (ver) {
      *ver = v;
    }
    std::move(handler)(ec);
  }
  void operator()(boost::system::error_code ec, version_t v, bufferlist bl) {
    if (ver) {
      *ver = v;
    }
    std::move(handler)(ec, std::move(bl));
  }
};

template <typename Signature>
struct redirect_version_signature {};
template <>
struct redirect_version_signature<void(boost::system::error_code, version_t)> {
  using type = void(boost::system::error_code);
};
template <>
struct redirect_version_signature<void(boost::system::error_code, version_t, bufferlist)> {
  using type = void(boost::system::error_code, bufferlist);
};
template <typename Signature>
using redirect_version_signature_t = typename redirect_version_signature<Signature>::type;

} // namespace detail

} // namespace librados

namespace boost::asio {

template <template <typename, typename> class Associator,
    typename Handler, typename DefaultCandidate>
struct associator<Associator,
    librados::detail::redirect_version_handler<Handler>, DefaultCandidate>
  : Associator<Handler, DefaultCandidate>
{
  static auto get(const librados::detail::redirect_version_handler<Handler>& h) noexcept {
    return Associator<Handler, DefaultCandidate>::get(h.handler);
  }
  static auto get(const librados::detail::redirect_version_handler<Handler>& h,
                  const DefaultCandidate& c) noexcept {
    return Associator<Handler, DefaultCandidate>::get(h.handler, c);
  }
};

template <typename CompletionToken, typename Signature>
struct async_result<librados::redirect_version_t<CompletionToken>, Signature>
  : async_result<CompletionToken,
      librados::detail::redirect_version_signature_t<Signature>>
{
  template <typename Initiation>
  struct init_wrapper {
    [[no_unique_address]] Initiation init;

    template <typename Handler, typename... Args>
    void operator()(Handler&& handler, version_t* ver, Args&&... args) && {
      std::move(init)(
          librados::detail::redirect_version_handler<decay_t<Handler>>{
              std::forward<Handler>(handler), ver},
          std::forward<Args>(args)...);
    }

    template <typename Handler, typename... Args>
    void operator()(Handler&& handler, version_t* ver, Args&&... args) const & {
      init(
          librados::detail::redirect_version_handler<decay_t<Handler>>{
              std::forward<Handler>(handler), ver},
          std::forward<Args>(args)...);
    }
  };

  template <typename Initiation, typename RawCompletionToken, typename... Args>
  static auto initiate(Initiation&& init,
                       RawCompletionToken&& token,
                       Args&&... args) {
    return async_initiate<
      conditional_t<is_const<remove_reference_t<RawCompletionToken>>::value,
          const CompletionToken, CompletionToken>,
      librados::detail::redirect_version_signature_t<Signature>>(
        init_wrapper<decay_t<Initiation>>{std::forward<Initiation>(init)},
        token.token, token.ver, std::forward<Args>(args)...);
  }
};

} // namespace boost::asio
