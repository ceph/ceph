// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#ifndef CEPH_ASYNC_COMPLETION_H
#define CEPH_ASYNC_COMPLETION_H

#include <memory>

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/defer.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/post.hpp>

#include "bind_handler.h"
#include "forward_handler.h"

namespace ceph::async {

/**
 * Abstract completion handler interface for use with boost::asio.
 *
 * Memory management is performed using the Handler's 'associated allocator',
 * which carries the additional requirement that its memory be released before
 * the Handler is invoked. This allows memory allocated for one asynchronous
 * operation to be reused in its continuation. Because of this requirement, any
 * calls to invoke the completion must first release ownership of it. To enforce
 * this, the static functions defer()/dispatch()/post() take the completion by
 * rvalue-reference to std::unique_ptr<Completion>, i.e. std::move(completion).
 *
 * Handlers may also have an 'associated executor', so the calls to defer(),
 * dispatch(), and post() are forwarded to that executor. If there is no
 * associated executor (which is generally the case unless one was bound with
 * boost::asio::bind_executor()), the executor passed to Completion::create()
 * is used as a default.
 *
 * Example use:
 *
 *   // declare a Completion type with Signature = void(int, string)
 *   using MyCompletion = ceph::async::Completion<void(int, string)>;
 *
 *   // create a completion with the given callback:
 *   std::unique_ptr<MyCompletion> c;
 *   c = MyCompletion::create(ex, [] (int a, const string& b) {});
 *
 *   // bind arguments to the callback and post to its associated executor:
 *   MyCompletion::post(std::move(c), 5, "hello");
 *
 *
 * Additional user data may be stored along with the Completion to take
 * advantage of the handler allocator optimization. This is accomplished by
 * specifying its type in the template parameter T. For example, the type
 * Completion<void(), int> contains a public member variable 'int user_data'.
 * Any additional arguments to Completion::create() will be forwarded to type
 * T's constructor.
 *
 * If the AsBase<T> type tag is used, as in Completion<void(), AsBase<T>>,
 * the Completion will inherit from T instead of declaring it as a member
 * variable.
 *
 * When invoking the completion handler via defer(), dispatch(), or post(),
 * care must be taken when passing arguments that refer to user data, because
 * its memory is destroyed prior to invocation. In such cases, the user data
 * should be moved/copied out of the Completion first.
 */
template <typename Signature, typename T = void>
class Completion;


/// type tag for UserData
template <typename T> struct AsBase {};

namespace detail {

/// optional user data to be stored with the Completion
template <typename T>
struct UserData {
  T user_data;
  template <typename ...Args>
  UserData(Args&& ...args)
    : user_data(std::forward<Args>(args)...)
  {}
};
// AsBase specialization inherits from T
template <typename T>
struct UserData<AsBase<T>> : public T {
  template <typename ...Args>
  UserData(Args&& ...args)
    : T(std::forward<Args>(args)...)
  {}
};
// void specialization
template <>
class UserData<void> {};

} // namespace detail


// template specialization to pull the Signature's args apart
template <typename T, typename ...Args>
class Completion<void(Args...), T> : public detail::UserData<T> {
 protected:
  // internal interfaces for type-erasure on the Handler/Executor. uses
  // tuple<Args...> to provide perfect forwarding because you can't make
  // virtual function templates
  virtual void destroy_defer(std::tuple<Args...>&& args) = 0;
  virtual void destroy_dispatch(std::tuple<Args...>&& args) = 0;
  virtual void destroy_post(std::tuple<Args...>&& args) = 0;
  virtual void destroy() = 0;

  // constructor is protected, use create(). any constructor arguments are
  // forwarded to UserData
  template <typename ...TArgs>
  Completion(TArgs&& ...args)
    : detail::UserData<T>(std::forward<TArgs>(args)...)
  {}
 public:
  virtual ~Completion() = default;

  // use the virtual destroy() interface on delete. this allows the derived
  // class to manage its memory using Handler allocators, without having to use
  // a custom Deleter for std::unique_ptr<>
  static void operator delete(void *p) {
    static_cast<Completion*>(p)->destroy();
  }

  /// completion factory function that uses the handler's associated allocator.
  /// any additional arguments are forwared to T's constructor
  template <typename Executor1, typename Handler, typename ...TArgs>
  static std::unique_ptr<Completion>
  create(const Executor1& ex1, Handler&& handler, TArgs&& ...args);

  /// take ownership of the completion, bind any arguments to the completion
  /// handler, then defer() it on its associated executor
  template <typename ...Args2>
  static void defer(std::unique_ptr<Completion>&& c, Args2&&...args);

  /// take ownership of the completion, bind any arguments to the completion
  /// handler, then dispatch() it on its associated executor
  template <typename ...Args2>
  static void dispatch(std::unique_ptr<Completion>&& c, Args2&&...args);

  /// take ownership of the completion, bind any arguments to the completion
  /// handler, then post() it to its associated executor
  template <typename ...Args2>
  static void post(std::unique_ptr<Completion>&& c, Args2&&...args);
};

namespace detail {

// concrete Completion that knows how to invoke the completion handler. this
// observes all of the 'Requirements on asynchronous operations' specified by
// the C++ Networking TS
template <typename Executor1, typename Handler, typename T, typename ...Args>
class CompletionImpl final : public Completion<void(Args...), T> {
  // use Handler's associated executor (or Executor1 by default) for callbacks
  using Executor2 = boost::asio::associated_executor_t<Handler, Executor1>;
  // maintain work on both executors
  using Work1 = boost::asio::executor_work_guard<Executor1>;
  using Work2 = boost::asio::executor_work_guard<Executor2>;
  std::pair<Work1, Work2> work;
  Handler handler;

  // use Handler's associated allocator
  using Alloc2 = boost::asio::associated_allocator_t<Handler>;
  using Traits2 = std::allocator_traits<Alloc2>;
  using RebindAlloc2 = typename Traits2::template rebind_alloc<CompletionImpl>;
  using RebindTraits2 = std::allocator_traits<RebindAlloc2>;

  // placement new for the handler allocator
  static void* operator new(size_t, RebindAlloc2 alloc2) {
    return RebindTraits2::allocate(alloc2, 1);
  }
  // placement delete for when the constructor throws during placement new
  static void operator delete(void *p, RebindAlloc2 alloc2) {
    RebindTraits2::deallocate(alloc2, static_cast<CompletionImpl*>(p), 1);
  }

  static auto bind_and_forward(const Executor2& ex, Handler&& h,
                               std::tuple<Args...>&& args) {
    return forward_handler(CompletionHandler{
        boost::asio::bind_executor(ex, std::move(h)), std::move(args)});
  }

  void destroy_defer(std::tuple<Args...>&& args) override {
    auto w = std::move(work);
    auto ex2 = w.second.get_executor();
    RebindAlloc2 alloc2 = boost::asio::get_associated_allocator(handler);
    auto f = bind_and_forward(ex2, std::move(handler), std::move(args));
    RebindTraits2::destroy(alloc2, this);
    RebindTraits2::deallocate(alloc2, this, 1);
    boost::asio::defer(boost::asio::bind_executor(ex2, std::move(f)));
  }
  void destroy_dispatch(std::tuple<Args...>&& args) override {
    auto w = std::move(work);
    auto ex2 = w.second.get_executor();
    RebindAlloc2 alloc2 = boost::asio::get_associated_allocator(handler);
    auto f = bind_and_forward(ex2, std::move(handler), std::move(args));
    RebindTraits2::destroy(alloc2, this);
    RebindTraits2::deallocate(alloc2, this, 1);
    boost::asio::dispatch(std::move(f));
  }
  void destroy_post(std::tuple<Args...>&& args) override {
    auto w = std::move(work);
    auto ex2 = w.second.get_executor();
    RebindAlloc2 alloc2 = boost::asio::get_associated_allocator(handler);
    auto f = bind_and_forward(ex2, std::move(handler), std::move(args));
    RebindTraits2::destroy(alloc2, this);
    RebindTraits2::deallocate(alloc2, this, 1);
    boost::asio::post(std::move(f));
  }
  void destroy() override {
    RebindAlloc2 alloc2 = boost::asio::get_associated_allocator(handler);
    RebindTraits2::destroy(alloc2, this);
    RebindTraits2::deallocate(alloc2, this, 1);
  }

  // constructor is private, use create(). extra constructor arguments are
  // forwarded to UserData
  template <typename ...TArgs>
  CompletionImpl(const Executor1& ex1, Handler&& handler, TArgs&& ...args)
    : Completion<void(Args...), T>(std::forward<TArgs>(args)...),
      work(ex1, boost::asio::make_work_guard(handler, ex1)),
      handler(std::move(handler))
  {}

 public:
  template <typename ...TArgs>
  static auto create(const Executor1& ex, Handler&& handler, TArgs&& ...args) {
    auto alloc2 = boost::asio::get_associated_allocator(handler);
    using Ptr = std::unique_ptr<CompletionImpl>;
    return Ptr{new (alloc2) CompletionImpl(ex, std::move(handler),
                                           std::forward<TArgs>(args)...)};
  }

  static void operator delete(void *p) {
    static_cast<CompletionImpl*>(p)->destroy();
  }
};

} // namespace detail


template <typename T, typename ...Args>
template <typename Executor1, typename Handler, typename ...TArgs>
std::unique_ptr<Completion<void(Args...), T>>
Completion<void(Args...), T>::create(const Executor1& ex,
                                     Handler&& handler, TArgs&& ...args)
{
  using Impl = detail::CompletionImpl<Executor1, Handler, T, Args...>;
  return Impl::create(ex, std::forward<Handler>(handler),
                      std::forward<TArgs>(args)...);
}

template <typename T, typename ...Args>
template <typename ...Args2>
void Completion<void(Args...), T>::defer(std::unique_ptr<Completion>&& ptr,
                                         Args2&& ...args)
{
  auto c = ptr.release();
  c->destroy_defer(std::make_tuple(std::forward<Args2>(args)...));
}

template <typename T, typename ...Args>
template <typename ...Args2>
void Completion<void(Args...), T>::dispatch(std::unique_ptr<Completion>&& ptr,
                                            Args2&& ...args)
{
  auto c = ptr.release();
  c->destroy_dispatch(std::make_tuple(std::forward<Args2>(args)...));
}

template <typename T, typename ...Args>
template <typename ...Args2>
void Completion<void(Args...), T>::post(std::unique_ptr<Completion>&& ptr,
                                        Args2&& ...args)
{
  auto c = ptr.release();
  c->destroy_post(std::make_tuple(std::forward<Args2>(args)...));
}


/// completion factory function that uses the handler's associated allocator.
/// any additional arguments are forwared to T's constructor
template <typename Signature, typename T, typename Executor1,
          typename Handler, typename ...TArgs>
std::unique_ptr<Completion<Signature, T>>
create_completion(const Executor1& ex, Handler&& handler, TArgs&& ...args)
{
  return Completion<Signature, T>::create(ex, std::forward<Handler>(handler),
                                          std::forward<TArgs>(args)...);
}

/// take ownership of the completion, bind any arguments to the completion
/// handler, then defer() it on its associated executor
template <typename Signature, typename T, typename ...Args>
void defer(std::unique_ptr<Completion<Signature, T>>&& ptr, Args&& ...args)
{
  Completion<Signature, T>::defer(std::move(ptr), std::forward<Args>(args)...);
}

/// take ownership of the completion, bind any arguments to the completion
/// handler, then dispatch() it on its associated executor
template <typename Signature, typename T, typename ...Args>
void dispatch(std::unique_ptr<Completion<Signature, T>>&& ptr, Args&& ...args)
{
  Completion<Signature, T>::dispatch(std::move(ptr), std::forward<Args>(args)...);
}

/// take ownership of the completion, bind any arguments to the completion
/// handler, then post() it to its associated executor
template <typename Signature, typename T, typename ...Args>
void post(std::unique_ptr<Completion<Signature, T>>&& ptr, Args&& ...args)
{
  Completion<Signature, T>::post(std::move(ptr), std::forward<Args>(args)...);
}

} // namespace ceph::async

#endif // CEPH_ASYNC_COMPLETION_H
