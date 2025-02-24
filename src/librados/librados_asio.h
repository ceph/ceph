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

#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/execution/executor.hpp>

#include "include/rados/librados.hpp"
#include "common/async/completion.h"
#include "librados/AioCompletionImpl.h"

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
  using Signature = void(boost::system::error_code, version_t, Result);
  Result result;
  template <typename Completion>
  void dispatch(Completion&& completion, boost::system::error_code ec, version_t ver) {
    ceph::async::dispatch(std::move(completion), ec, ver, std::move(result));
  }
};
// specialization for Result=void
template <>
struct Invoker<void> {
  using Signature = void(boost::system::error_code, version_t);
  template <typename Completion>
  void dispatch(Completion&& completion, boost::system::error_code ec, version_t ver) {
    ceph::async::dispatch(std::move(completion), ec, ver);
  }
};

template <typename Result>
struct AsyncOp : Invoker<Result> {
  unique_aio_completion_ptr aio_completion;
  boost::asio::cancellation_slot slot;

  using Signature = typename Invoker<Result>::Signature;
  using Completion = ceph::async::Completion<Signature, AsyncOp<Result>>;

  static void aio_dispatch(completion_t cb, void *arg) {
    // reclaim ownership of the completion
    auto p = std::unique_ptr<Completion>{static_cast<Completion*>(arg)};
    // move result out of Completion memory being freed
    auto op = std::move(p->user_data);
    op.slot.clear(); // clear our cancellation handler
    // access AioCompletionImpl directly to avoid locking
    const librados::AioCompletionImpl* pc = op.aio_completion->pc;
    const int ret = pc->rval;
    const version_t ver = pc->objver;
    boost::system::error_code ec;
    if (ret < 0) {
      ec.assign(-ret, librados::detail::err_category());
    }
    op.dispatch(std::move(p), ec, ver);
  }

  struct op_cancellation {
    AioCompletion* completion = nullptr;
    bool is_read = false;

    void operator()(boost::asio::cancellation_type type) {
      if (completion == nullptr) {
        return; // no AioCompletion attached
      } else if (type == boost::asio::cancellation_type::none) {
        return; // no cancellation requested
      } else if (is_read) {
        // read operations produce no side effects, so can satisfy the
        // requirements of 'total' cancellation. the weaker requirements
        // of 'partial' and 'terminal' are also satisfied
        completion->cancel();
      } else if (type == boost::asio::cancellation_type::terminal) {
        // write operations only support 'terminal' cancellation because we
        // can't guarantee that no osd has succeeded (or will succeed) in
        // applying the write
        completion->cancel();
      }
    }
  };

  template <typename Executor1, typename CompletionHandler>
  static auto create(const Executor1& ex1, bool is_read,
                     CompletionHandler&& handler) {
    op_cancellation* cancel_handler = nullptr;
    auto slot = boost::asio::get_associated_cancellation_slot(handler);
    if (slot.is_connected()) {
      cancel_handler = &slot.template emplace<op_cancellation>();
    }

    auto p = Completion::create(ex1, std::move(handler));
    p->user_data.aio_completion.reset(
        Rados::aio_create_completion(p.get(), aio_dispatch));
    if (cancel_handler) {
      cancel_handler->completion = p->user_data.aio_completion.get();
      cancel_handler->is_read = is_read;
      p->user_data.slot = std::move(slot);
    }
    return p;
  }
};

} // namespace detail


/// Calls IoCtx::aio_read() and arranges for the AioCompletion to call a
/// given handler with signature (error_code, version_t, bufferlist).
///
/// The given IoCtx reference is not required to remain valid, but some IoCtx
/// instance must preserve its underlying implementation until completion.
template <boost::asio::execution::executor IoExecutor, typename CompletionToken>
auto async_read(IoExecutor ex, IoCtx& io, const std::string& oid,
                size_t len, uint64_t off, CompletionToken&& token)
{
  using Op = detail::AsyncOp<bufferlist>;
  using Signature = typename Op::Signature;
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [] (auto handler, IoExecutor ex, const IoCtx& i,
          const std::string& oid, size_t len, uint64_t off) {
        constexpr bool is_read = true;
        auto p = Op::create(ex, is_read, std::move(handler));
        auto& op = p->user_data;

        IoCtx& io = const_cast<IoCtx&>(i);
        int ret = io.aio_read(oid, op.aio_completion.get(), &op.result, len, off);
        if (ret < 0) {
          auto ec = boost::system::error_code{-ret, librados::detail::err_category()};
          ceph::async::post(std::move(p), ec, 0, bufferlist{});
        } else {
          p.release(); // release ownership until completion
        }
      }, token, ex, io, oid, len, off);
}

/// Calls IoCtx::aio_write() and arranges for the AioCompletion to call a
/// given handler with signature (error_code, version_t).
///
/// The given IoCtx reference is not required to remain valid, but some IoCtx
/// instance must preserve its underlying implementation until completion.
template <boost::asio::execution::executor IoExecutor, typename CompletionToken>
auto async_write(IoExecutor ex, IoCtx& io, const std::string& oid,
                 const bufferlist &bl, size_t len, uint64_t off,
                 CompletionToken&& token)
{
  using Op = detail::AsyncOp<void>;
  using Signature = typename Op::Signature;
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [] (auto handler, IoExecutor ex, const IoCtx& i, const std::string& oid,
          const bufferlist &bl, size_t len, uint64_t off) {
        constexpr bool is_read = false;
        auto p = Op::create(ex, is_read, std::move(handler));
        auto& op = p->user_data;

        IoCtx& io = const_cast<IoCtx&>(i);
        int ret = io.aio_write(oid, op.aio_completion.get(), bl, len, off);
        if (ret < 0) {
          auto ec = boost::system::error_code{-ret, librados::detail::err_category()};
          ceph::async::post(std::move(p), ec, 0);
        } else {
          p.release(); // release ownership until completion
        }
      }, token, ex, io, oid, bl, len, off);
}

/// Calls IoCtx::aio_operate() and arranges for the AioCompletion to call a
/// given handler with signature (error_code, version_t, bufferlist).
///
/// The given IoCtx reference is not required to remain valid, but some IoCtx
/// instance must preserve its underlying implementation until completion.
template <boost::asio::execution::executor IoExecutor, typename CompletionToken>
auto async_operate(IoExecutor ex, IoCtx& io, const std::string& oid,
                   ObjectReadOperation read_op, int flags,
                   const jspan_context* trace_ctx, CompletionToken&& token)
{
  using Op = detail::AsyncOp<bufferlist>;
  using Signature = typename Op::Signature;
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [] (auto handler, IoExecutor ex, const IoCtx& i, const std::string& oid,
          ObjectReadOperation read_op, int flags) {
        constexpr bool is_read = true;
        auto p = Op::create(ex, is_read, std::move(handler));
        auto& op = p->user_data;

        auto& io = const_cast<IoCtx&>(i);
        int ret = io.aio_operate(oid, op.aio_completion.get(), &read_op,
                                 flags, &op.result);
        if (ret < 0) {
          auto ec = boost::system::error_code{-ret, librados::detail::err_category()};
          ceph::async::post(std::move(p), ec, 0, bufferlist{});
        } else {
          p.release(); // release ownership until completion
        }
      }, token, ex, io, oid, std::move(read_op), flags);
}

/// Calls IoCtx::aio_operate() and arranges for the AioCompletion to call a
/// given handler with signature (error_code, version_t).
///
/// The given IoCtx reference is not required to remain valid, but some IoCtx
/// instance must preserve its underlying implementation until completion.
template <boost::asio::execution::executor IoExecutor, typename CompletionToken>
auto async_operate(IoExecutor ex, IoCtx& io, const std::string& oid,
                   ObjectWriteOperation write_op, int flags,
                   const jspan_context* trace_ctx, CompletionToken &&token)
{
  using Op = detail::AsyncOp<void>;
  using Signature = typename Op::Signature;
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [] (auto handler, IoExecutor ex, const IoCtx& i, const std::string& oid,
          ObjectWriteOperation write_op, int flags,
          const jspan_context* trace_ctx) {
        constexpr bool is_read = false;
        auto p = Op::create(ex, is_read, std::move(handler));
        auto& op = p->user_data;

        auto& io = const_cast<IoCtx&>(i);
        int ret = io.aio_operate(oid, op.aio_completion.get(), &write_op, flags, trace_ctx);
        if (ret < 0) {
          auto ec = boost::system::error_code{-ret, librados::detail::err_category()};
          ceph::async::post(std::move(p), ec, 0);
        } else {
          p.release(); // release ownership until completion
        }
      }, token, ex, io, oid, std::move(write_op), flags, trace_ctx);
}

/// Calls IoCtx::aio_notify() and arranges for the AioCompletion to call a
/// given handler with signature (error_code, version_t, bufferlist).
///
/// The given IoCtx reference is not required to remain valid, but some IoCtx
/// instance must preserve its underlying implementation until completion.
template <boost::asio::execution::executor IoExecutor, typename CompletionToken>
auto async_notify(IoExecutor ex, IoCtx& io, const std::string& oid,
                  bufferlist& bl, uint64_t timeout_ms, CompletionToken &&token)
{
  using Op = detail::AsyncOp<bufferlist>;
  using Signature = typename Op::Signature;
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [] (auto handler, IoExecutor ex, const IoCtx& i, const std::string& oid,
          const bufferlist& b, uint64_t timeout_ms) {
        constexpr bool is_read = false;
        auto p = Op::create(ex, is_read, std::move(handler));
        auto& op = p->user_data;

        IoCtx& io = const_cast<IoCtx&>(i);
        bufferlist& bl = const_cast<bufferlist&>(b);
        int ret = io.aio_notify(oid, op.aio_completion.get(),
                                bl, timeout_ms, &op.result);
        if (ret < 0) {
          auto ec = boost::system::error_code{-ret, librados::detail::err_category()};
          ceph::async::post(std::move(p), ec, 0, bufferlist{});
        } else {
          p.release(); // release ownership until completion
        }
      }, token, ex, io, oid, bl, timeout_ms);
}

} // namespace librados

#endif // LIBRADOS_ASIO_H
