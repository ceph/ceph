// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp
/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <boost/asio/async_result.hpp>

#include "common/async/blocked_completion.h"
#include "common/async/concepts.h"
#include "common/async/redirect_error.h"
#include "common/async/yield_context.h"

#include "common/dout.h"

#include "rgw_asio_thread.h"

namespace rgw {
namespace detail {
/// CompletionToken wrapping an optional_yield and a DoutPrefixProvider*
///
/// \note I thought briefly of whether I could justify asserting on
/// blocking in an asio thread. Unconditionally, since I didn't have a
/// CephContext* either.
///
/// \note Obviously, I could not.
///
/// \note ;(
struct maybe_completer {
  const DoutPrefixProvider* dpp;
  optional_yield y;
};
} // namespace detail

/// Adapt `optional_yield` as a completion token for Asio operations
///
/// Rather than `use_blocked`, use this as a completion token when
/// calling to neorados or similar functions expecting one.
///
/// \warning Errors from calls made using this adaptor will throw.
///
/// For example, with neorados:
///
/// \code{.cpp}
/// try {
///   rados.execute(o, ioc, std::move(op), maybe_yield(dpp, y));
/// } catch (sys::system_error& e) {
///   // …
/// }
///
/// \endcode
///
/// \param[in] dpp Logging for `maybe_warn_about_blocking()`
/// \param[in] y The yield_context, maybe. Maybe not.
///
/// \note I was originally going to write an `async_result`
/// specialization for optional_yield.
inline detail::maybe_completer
maybe_yield(const DoutPrefixProvider* dpp, optional_yield y)
{
  return {dpp, y};
}

/// Adapt `optional_yield` as a completion token for Asio operations,
/// with error diversion
///
/// Rather than `use_blocked`, use this as a completion token when
/// calling to neorados or similar functions expecting one.
///
/// \note This version diverts the error rather than throwing. The
/// type of the error (the `disposition` in Asio parlance) depends on
/// the operation. Basic neorados operations use
/// `boost::system::error_code`. Coroutines use `std::exception_ptr`.
///
/// An example for neorados,
/// \code{.cpp}
///
/// boost::system::error_code ec;
/// rados.execute(o, ioc, std::move(op), maybe_yield(dpp, y, ec));
///
/// if (ec) {
///   …
/// }
/// \endcode
///
/// \param[in] dpp Logging for `maybe_warn_about_blocking()`
/// \param[in] y The yield_context, maybe. Maybe not.
/// \param[in] disposition A disposition to redirect
///
/// \note I was going to add an overload to optional_yield for
/// `operator []` to give ergonomic error redirection. Then I
/// remembered to add maybe warn about blocking and discovered it
/// required a DoutPrefixProvider*.
template<ceph::async::disposition Disposition>
inline auto
maybe_yield(const DoutPrefixProvider* dpp, optional_yield y, Disposition& disposition)
{
  return ceph::async::redirect_error(maybe_yield(dpp, y), disposition);
}
} // namespace rgw

namespace boost::asio {
template <typename Signature>
struct async_result<rgw::detail::maybe_completer, Signature> {
  template <typename Initiation, typename RawCompletionToken, typename... Args>
  static auto
  initiate(Initiation&& initiation, RawCompletionToken&& token, Args&&... args)
  {
    if (token.y) {
      return async_result<yield_context, Signature>::initiate(
          std::forward<Initiation>(initiation), token.y.get_yield_context(),
          std::forward<Args>(args)...);
    } else {
      maybe_warn_about_blocking(token.dpp);
      return async_result<ceph::async::use_blocked_t, Signature>::initiate(
          std::forward<Initiation>(initiation), ceph::async::use_blocked,
          std::forward<Args>(args)...);
    }
  }
};
} // namespace boost::asio
