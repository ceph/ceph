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

#include <memory>
#include <boost/asio.hpp>
#include "include/rados/librados.hpp"

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

/// unique_ptr with custom deleter for AioCompletion
struct release_completion {
  void operator()(AioCompletion *c) { c->release(); }
};
using unique_completion_ptr =
    std::unique_ptr<AioCompletion, release_completion>;

/// Invokes the given completion handler. When the type of Result is not void,
/// storage is provided for it and that result is passed as an additional
/// argument to the handler.
template <typename Result>
struct invoker {
  Result result;
  template <typename CompletionHandler>
  void invoke(CompletionHandler& completion_handler,
              boost::system::error_code ec) {
    completion_handler(ec, std::move(result));
  }
};
// specialization for Result=void
template <>
struct invoker<void> {
  template <typename CompletionHandler>
  void invoke(CompletionHandler& completion_handler,
              boost::system::error_code ec) {
    completion_handler(ec);
  }
};

/// The function object that eventually gets dispatched back to its associated
/// executor to invoke the completion_handler with our bound error_code and
/// Result arguments.
/// Inherits from invoker for empty base optimization when Result=void.
template <typename CompletionHandler, typename Result, typename Executor2>
struct bound_completion_handler : public invoker<Result> {
  CompletionHandler completion_handler; //< upcall handler from CompletionToken
  boost::system::error_code ec;
  Executor2 ex2; //< associated completion handler executor

  bound_completion_handler(CompletionHandler& completion_handler, Executor2 ex2)
    : completion_handler(completion_handler), ex2(ex2)
  {
    // static check for CompletionHandler concept (must be CopyConstructible and
    // callable with no arguments)
    using namespace boost::asio;
    BOOST_ASIO_COMPLETION_HANDLER_CHECK(bound_completion_handler, *this) type_check;
  }

  /// Invoke the completion handler with our bound arguments
  void operator()() {
    this->invoke(completion_handler, ec);
  }

  /// Delegate to CompletionHandler's associated allocator
  using allocator_type = boost::asio::associated_allocator_t<CompletionHandler>;
  allocator_type get_allocator() const noexcept {
    return boost::asio::get_associated_allocator(completion_handler);
  }

  /// Use our associated completion handler executor
  using executor_type = Executor2;
  executor_type get_executor() const noexcept {
    return ex2;
  }
};

/// Operation state needed to invoke the handler on completion. This state must
/// be allocated so that its address can be passed through the AioCompletion
/// callback. This memory is managed by the CompletionHandler's associated
/// allocator according to "Allocation of intermediate storage" requirements.
template <typename CompletionHandler, typename Result, typename Executor1>
struct op_state {
  /// completion handler executor, which delegates to CompletionHandler's
  /// associated executor or defaults to the io executor
  using Executor2 = boost::asio::associated_executor_t<CompletionHandler, Executor1>;

  /// maintain outstanding work on the io executor
  boost::asio::executor_work_guard<Executor1> work1;
  /// maintain outstanding work on the completion handler executor
  boost::asio::executor_work_guard<Executor2> work2;

  /// the function object that invokes the completion handler
  bound_completion_handler<CompletionHandler, Result, Executor2> f;
  unique_completion_ptr completion; //< the AioCompletion

  op_state(CompletionHandler& completion_handler, Executor1 ex1,
           unique_completion_ptr&& completion)
    : work1(ex1),
      work2(boost::asio::get_associated_executor(completion_handler, ex1)),
      f(completion_handler, work2.get_executor()),
      completion(std::move(completion))
  {}

  using Handler = CompletionHandler; // the following macro wants a Handler type

  /// Defines a scoped 'op_state::ptr' type that uses CompletionHandler's
  /// associated allocator to manage its memory
  BOOST_ASIO_DEFINE_HANDLER_PTR(op_state);
};

/// Handler allocators require that their memory is released before the handler
/// itself is invoked. Return a moved copy of the bound completion handler after
/// destroying/deallocating the op_state.
template <typename StatePtr>
auto release_handler(StatePtr&& p) -> decltype(p.p->f)
{
  // move the completion handler out of the memory being released
  auto f = std::move(p.p->f);
  // return the memory to the moved handler's associated allocator
  p.h = std::addressof(f.completion_handler);
  p.reset();
  return f;
}

/// AioCompletion callback function, executed in the librados finisher thread.
template <typename State, typename StatePtr = typename State::ptr>
inline void aio_op_dispatch(completion_t cb, void *arg)
{
  auto op = static_cast<State*>(arg);
  const int ret = op->completion->get_return_value();
  // maintain work until the completion handler is dispatched. these would
  // otherwise be destroyed with op_state in release_handler()
  auto work1 = std::move(op->work1);
  auto work2 = std::move(op->work2);
  // return the memory to the handler allocator
  auto f = release_handler<StatePtr>({nullptr, op, op});
  if (ret < 0) {
    // assign the bound error code
    f.ec.assign(-ret, boost::system::system_category());
  }
  // dispatch the completion handler using its associated allocator/executor
  auto alloc2 = boost::asio::get_associated_allocator(f);
  f.ex2.dispatch(std::move(f), alloc2);
}

/// Create an AioCompletion and return it as a unique_ptr.
template <typename State>
inline unique_completion_ptr make_completion(void *op)
{
  auto cb = aio_op_dispatch<State>;
  return unique_completion_ptr{Rados::aio_create_completion(op, nullptr, cb)};
}

/// Allocate op state using the CompletionHandler's associated allocator.
template <typename Result, typename Executor1, typename CompletionHandler,
          typename State = op_state<CompletionHandler, Result, Executor1>,
          typename StatePtr = typename State::ptr>
StatePtr make_op_state(Executor1&& ex1, CompletionHandler& handler)
{
  // allocate a block of memory with StatePtr::allocate()
  StatePtr p = {std::addressof(handler), StatePtr::allocate(handler), 0};
  // create an AioCompletion to call aio_op_dispatch() with this pointer
  auto completion = make_completion<State>(p.v);
  // construct the op_state in place
  p.p = new (p.v) State(handler, ex1, std::move(completion));
  return p;
}

} // namespace detail


/// Calls IoCtx::aio_read() and arranges for the AioCompletion to call a
/// given handler with signature (boost::system::error_code, bufferlist).
template <typename ExecutionContext, typename CompletionToken,
          typename Signature = void(boost::system::error_code, bufferlist)>
BOOST_ASIO_INITFN_RESULT_TYPE(CompletionToken, Signature)
async_read(ExecutionContext& ctx, IoCtx& io, const std::string& oid,
           size_t len, uint64_t off, CompletionToken&& token)
{
  boost::asio::async_completion<CompletionToken, Signature> init(token);
  auto p = detail::make_op_state<bufferlist>(ctx.get_executor(),
                                             init.completion_handler);

  int ret = io.aio_read(oid, p.p->completion.get(),
                        &p.p->f.result, len, off);
  if (ret < 0) {
    // post the completion after releasing the handler-allocated memory
    p.p->f.ec.assign(-ret, boost::system::system_category());
    boost::asio::post(detail::release_handler(std::move(p)));
  } else {
    p.v = p.p = nullptr; // release ownership until completion
  }
  return init.result.get();
}

/// Calls IoCtx::aio_write() and arranges for the AioCompletion to call a
/// given handler with signature (boost::system::error_code).
template <typename ExecutionContext, typename CompletionToken,
          typename Signature = void(boost::system::error_code)>
BOOST_ASIO_INITFN_RESULT_TYPE(CompletionToken, Signature)
async_write(ExecutionContext& ctx, IoCtx& io, const std::string& oid,
            bufferlist &bl, size_t len, uint64_t off,
            CompletionToken&& token)
{
  boost::asio::async_completion<CompletionToken, Signature> init(token);
  auto p = detail::make_op_state<void>(ctx.get_executor(),
                                       init.completion_handler);

  int ret = io.aio_write(oid, p.p->completion.get(), bl, len, off);
  if (ret < 0) {
    p.p->f.ec.assign(-ret, boost::system::system_category());
    boost::asio::post(detail::release_handler(std::move(p)));
  } else {
    p.v = p.p = nullptr; // release ownership until completion
  }
  return init.result.get();
}

/// Calls IoCtx::aio_operate() and arranges for the AioCompletion to call a
/// given handler with signature (boost::system::error_code, bufferlist).
template <typename ExecutionContext, typename CompletionToken,
          typename Signature = void(boost::system::error_code, bufferlist)>
BOOST_ASIO_INITFN_RESULT_TYPE(CompletionToken, Signature)
async_operate(ExecutionContext& ctx, IoCtx& io, const std::string& oid,
              ObjectReadOperation *op, int flags,
              CompletionToken&& token)
{
  boost::asio::async_completion<CompletionToken, Signature> init(token);
  auto p = detail::make_op_state<bufferlist>(ctx.get_executor(),
                                             init.completion_handler);

  int ret = io.aio_operate(oid, p.p->completion.get(), op,
                           flags, &p.p->f.result);
  if (ret < 0) {
    p.p->f.ec.assign(-ret, boost::system::system_category());
    boost::asio::post(detail::release_handler(std::move(p)));
  } else {
    p.v = p.p = nullptr; // release ownership until completion
  }
  return init.result.get();
}

/// Calls IoCtx::aio_operate() and arranges for the AioCompletion to call a
/// given handler with signature (boost::system::error_code).
template <typename ExecutionContext, typename CompletionToken,
          typename Signature = void(boost::system::error_code)>
BOOST_ASIO_INITFN_RESULT_TYPE(CompletionToken, Signature)
async_operate(ExecutionContext& ctx, IoCtx& io, const std::string& oid,
              ObjectWriteOperation *op, int flags,
              CompletionToken &&token)
{
  boost::asio::async_completion<CompletionToken, Signature> init(token);
  auto p = detail::make_op_state<void>(ctx.get_executor(),
                                       init.completion_handler);

  int ret = io.aio_operate(oid, p.p->completion.get(), op, flags);
  if (ret < 0) {
    p.p->f.ec.assign(-ret, boost::system::system_category());
    boost::asio::post(detail::release_handler(std::move(p)));
  } else {
    p.v = p.p = nullptr; // release ownership until completion
  }
  return init.result.get();
}

} // namespace librados

#endif // LIBRADOS_ASIO_H
