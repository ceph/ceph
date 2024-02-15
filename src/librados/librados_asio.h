// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef LIBRADOS_ASIO_H
#define LIBRADOS_ASIO_H

#include "include/rados/librados.hpp"
#include "common/async/completion.h"

/// Defines asynchronous librados operations that satisfy all of the
/// "Requirements on asynchronous operations" imposed by the C++ Networking TS
/// in section 13.2.7. Many of the type and variable names below are taken
/// directly from those requirements.
///
/// The current draft of the Networking TS (as of 2017-11-27) is available here:
/// http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2017/n4711.pdf
///
/// The boost::asio documentation duplicates these requirements here:
/// http://www.boost.org/doc/libs/1_66_0/doc/html/boost_asio/reference/asynchronous_operations.html

namespace librados {

namespace detail {

#ifndef _WIN32
constexpr auto err_category = boost::system::system_category;
#else
// librados uses "errno.h" error codes. On Windows,
// boost::system::system_category refers to errors from winerror.h.
// That being considered, we'll use boost::system::generic_category.
constexpr auto err_category = boost::system::generic_category;
#endif

/// unique_ptr with custom deleter for AioCompletion
struct AioCompletionDeleter {
  void operator()(AioCompletion *c) { c->release(); }
};
using unique_aio_completion_ptr =
    std::unique_ptr<AioCompletion, AioCompletionDeleter>;

/// Invokes the given completion handler. When the type of Result is not void,
/// storage is provided for it and that result is passed as an additional
/// argument to the handler.
template <typename Result>
struct Invoker {
  using Signature = void(boost::system::error_code, Result);
  Result result;
  template <typename Completion>
  void dispatch(Completion&& completion, boost::system::error_code ec) {
    ceph::async::dispatch(std::move(completion), ec, std::move(result));
  }
};
// specialization for Result=void
template <>
struct Invoker<void> {
  using Signature = void(boost::system::error_code);
  template <typename Completion>
  void dispatch(Completion&& completion, boost::system::error_code ec) {
    ceph::async::dispatch(std::move(completion), ec);
  }
};

template <typename Result>
struct AsyncOp : Invoker<Result> {
  unique_aio_completion_ptr aio_completion;

  using Signature = typename Invoker<Result>::Signature;
  using Completion = ceph::async::Completion<Signature, AsyncOp<Result>>;

  static void aio_dispatch(completion_t cb, void *arg) {
    // reclaim ownership of the completion
    auto p = std::unique_ptr<Completion>{static_cast<Completion*>(arg)};
    // move result out of Completion memory being freed
    auto op = std::move(p->user_data);
    const int ret = op.aio_completion->get_return_value();
    boost::system::error_code ec;
    if (ret < 0) {
      ec.assign(-ret, librados::detail::err_category());
    }
    op.dispatch(std::move(p), ec);
  }

  template <typename Executor1, typename CompletionHandler>
  static auto create(const Executor1& ex1, CompletionHandler&& handler) {
    auto p = Completion::create(ex1, std::move(handler));
    p->user_data.aio_completion.reset(
        Rados::aio_create_completion(p.get(), aio_dispatch));
    return p;
  }
};

} // namespace detail


/// Calls IoCtx::aio_read() and arranges for the AioCompletion to call a
/// given handler with signature (boost::system::error_code, bufferlist).
template <typename ExecutionContext, typename CompletionToken>
auto async_read(ExecutionContext& ctx, IoCtx& io, const std::string& oid,
                size_t len, uint64_t off, CompletionToken&& token)
{
  using Op = detail::AsyncOp<bufferlist>;
  using Signature = typename Op::Signature;
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [] (auto handler, auto ex, IoCtx& io, const std::string& oid,
          size_t len, uint64_t off) {
        auto p = Op::create(ex, std::move(handler));
        auto& op = p->user_data;

        int ret = io.aio_read(oid, op.aio_completion.get(), &op.result, len, off);
        if (ret < 0) {
          auto ec = boost::system::error_code{-ret, librados::detail::err_category()};
          ceph::async::post(std::move(p), ec, bufferlist{});
        } else {
          p.release(); // release ownership until completion
        }
      }, token, ctx.get_executor(), io, oid, len, off);
}

/// Calls IoCtx::aio_write() and arranges for the AioCompletion to call a
/// given handler with signature (boost::system::error_code).
template <typename ExecutionContext, typename CompletionToken>
auto async_write(ExecutionContext& ctx, IoCtx& io, const std::string& oid,
                 bufferlist &bl, size_t len, uint64_t off,
                 CompletionToken&& token)
{
  using Op = detail::AsyncOp<void>;
  using Signature = typename Op::Signature;
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [] (auto handler, auto ex, IoCtx& io, const std::string& oid,
          bufferlist &bl, size_t len, uint64_t off) {
        auto p = Op::create(ex, std::move(handler));
        auto& op = p->user_data;

        int ret = io.aio_write(oid, op.aio_completion.get(), bl, len, off);
        if (ret < 0) {
          auto ec = boost::system::error_code{-ret, librados::detail::err_category()};
          ceph::async::post(std::move(p), ec);
        } else {
          p.release(); // release ownership until completion
        }
      }, token, ctx.get_executor(), io, oid, bl, len, off);
}

/// Calls IoCtx::aio_operate() and arranges for the AioCompletion to call a
/// given handler with signature (boost::system::error_code, bufferlist).
template <typename ExecutionContext, typename CompletionToken>
auto async_operate(ExecutionContext& ctx, IoCtx& io, const std::string& oid,
                   ObjectReadOperation *read_op, int flags,
                   const jspan_context* trace_ctx, CompletionToken&& token)
{
  using Op = detail::AsyncOp<bufferlist>;
  using Signature = typename Op::Signature;
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [] (auto handler, auto ex, IoCtx& io, const std::string& oid,
          ObjectReadOperation *read_op, int flags) {
        auto p = Op::create(ex, std::move(handler));
        auto& op = p->user_data;

        int ret = io.aio_operate(oid, op.aio_completion.get(), read_op,
                                 flags, &op.result);
        if (ret < 0) {
          auto ec = boost::system::error_code{-ret, librados::detail::err_category()};
          ceph::async::post(std::move(p), ec, bufferlist{});
        } else {
          p.release(); // release ownership until completion
        }
      }, token, ctx.get_executor(), io, oid, read_op, flags);
}

/// Calls IoCtx::aio_operate() and arranges for the AioCompletion to call a
/// given handler with signature (boost::system::error_code).
template <typename ExecutionContext, typename CompletionToken>
auto async_operate(ExecutionContext& ctx, IoCtx& io, const std::string& oid,
                   ObjectWriteOperation *write_op, int flags,
                   const jspan_context* trace_ctx, CompletionToken &&token)
{
  using Op = detail::AsyncOp<void>;
  using Signature = typename Op::Signature;
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [] (auto handler, auto ex, IoCtx& io, const std::string& oid,
          ObjectWriteOperation *write_op, int flags,
          const jspan_context* trace_ctx) {
        auto p = Op::create(ex, std::move(handler));
        auto& op = p->user_data;

        int ret = io.aio_operate(oid, op.aio_completion.get(), write_op, flags, trace_ctx);
        if (ret < 0) {
          auto ec = boost::system::error_code{-ret, librados::detail::err_category()};
          ceph::async::post(std::move(p), ec);
        } else {
          p.release(); // release ownership until completion
        }
      }, token, ctx.get_executor(), io, oid, write_op, flags, trace_ctx);
}

/// Calls IoCtx::aio_notify() and arranges for the AioCompletion to call a
/// given handler with signature (boost::system::error_code, bufferlist).
template <typename ExecutionContext, typename CompletionToken>
auto async_notify(ExecutionContext& ctx, IoCtx& io, const std::string& oid,
                  bufferlist& bl, uint64_t timeout_ms, CompletionToken &&token)
{
  using Op = detail::AsyncOp<bufferlist>;
  using Signature = typename Op::Signature;
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [] (auto handler, auto ex, IoCtx& io, const std::string& oid,
          bufferlist& bl, uint64_t timeout_ms) {
        auto p = Op::create(ex, std::move(handler));
        auto& op = p->user_data;

        int ret = io.aio_notify(oid, op.aio_completion.get(),
                                bl, timeout_ms, &op.result);
        if (ret < 0) {
          auto ec = boost::system::error_code{-ret, librados::detail::err_category()};
          ceph::async::post(std::move(p), ec, bufferlist{});
        } else {
          p.release(); // release ownership until completion
        }
      }, token, ctx.get_executor(), io, oid, bl, timeout_ms);
}

} // namespace librados

#endif // LIBRADOS_ASIO_H
