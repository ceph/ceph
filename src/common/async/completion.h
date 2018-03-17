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

#include "bind_handler.h"

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
      noexcept(std::is_nothrow_constructible_v<T, Args...>)
    : user_data(std::forward<Args>(args)...)
  {}
};
// AsBase specialization inherits from T
template <typename T>
struct UserData<AsBase<T>> : public T {
  template <typename ...Args>
  UserData(Args&& ...args)
      noexcept(std::is_nothrow_constructible_v<T, Args...>)
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
      noexcept(std::is_nothrow_constructible_v<detail::UserData<T>, TArgs...>)
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
 protected:
  // use Handler's associated executor (or Executor1 by default) for callbacks
  using Executor2 = boost::asio::associated_executor_t<Handler, Executor1>;
  // maintain work on both executors
  boost::asio::executor_work_guard<Executor1> work1;
  boost::asio::executor_work_guard<Executor2> work2;
  Handler handler;

  // use Handler's associated allocator
  using Alloc2 = boost::asio::associated_allocator_t<Handler>;
  using Traits2 = std::allocator_traits<Alloc2>;
  using RebindAlloc2 = typename Traits2::template rebind_alloc<CompletionImpl>;
  using RebindTraits2 = std::allocator_traits<RebindAlloc2>;

  // placement new for Handler&
  static void* operator new(size_t, Handler& handler) {
    RebindAlloc2 alloc2 = boost::asio::get_associated_allocator(handler);
    return RebindTraits2::allocate(alloc2, 1);
  }
  // placement delete for when the constructor throws during placement new
  static void operator delete(void *p, Handler& handler) {
    RebindAlloc2 alloc2 = boost::asio::get_associated_allocator(handler);
    RebindTraits2::deallocate(alloc2, static_cast<CompletionImpl*>(p), 1);
  }

  // move the handler out, and use it to destroy this completion
  Handler release_handler() {
    auto f = std::move(handler);
    RebindAlloc2 alloc2 = boost::asio::get_associated_allocator(f);
    RebindTraits2::destroy(alloc2, this);
    RebindTraits2::deallocate(alloc2, this, 1);
    return f;
  }
  void destroy_defer(std::tuple<Args...>&& args) override {
    auto w1 = std::move(work1);
    auto w2 = std::move(work2);
    auto b = CompletionHandler{release_handler(), std::move(args)};
    auto ex2 = w2.get_executor();
    auto alloc2 = boost::asio::get_associated_allocator(b);
    ex2.defer(std::move(b), alloc2);
  }
  void destroy_dispatch(std::tuple<Args...>&& args) override {
    auto w1 = std::move(work1);
    auto w2 = std::move(work2);
    auto b = CompletionHandler{release_handler(), std::move(args)};
    auto ex2 = w2.get_executor();
    auto alloc2 = boost::asio::get_associated_allocator(b);
    ex2.dispatch(std::move(b), alloc2);
  }
  void destroy_post(std::tuple<Args...>&& args) override {
    auto w1 = std::move(work1);
    auto w2 = std::move(work2);
    auto b = CompletionHandler{release_handler(), std::move(args)};
    auto ex2 = w2.get_executor();
    auto alloc2 = boost::asio::get_associated_allocator(b);
    ex2.post(std::move(b), alloc2);
  }
  void destroy() override {
    RebindAlloc2 alloc2 = boost::asio::get_associated_allocator(handler);
    RebindTraits2::destroy(alloc2, this);
    RebindTraits2::deallocate(alloc2, this, 1);
  }

  // constructor is protected, use create(). extra constructor arguments are
  // forwarded to UserData
  template <typename ...TArgs>
  CompletionImpl(const Executor1& ex1, Handler&& handler, TArgs&& ...args)
      noexcept(std::is_nothrow_constructible_v<UserData<T>, TArgs...> &&
               std::is_nothrow_move_constructible_v<Handler>)
    : Completion<void(Args...), T>(std::forward<TArgs>(args)...),
      work1(ex1),
      work2(boost::asio::get_associated_executor(handler, ex1)),
      handler(std::move(handler))
  {}

 public:
  static void operator delete(void *p) {
    static_cast<CompletionImpl*>(p)->destroy();
  }

  template <typename ...TArgs>
  static std::unique_ptr<CompletionImpl>
  create(const Executor1& ex, Handler&& handler, TArgs&& ...args)
  {
    using Ptr = std::unique_ptr<CompletionImpl>;
    // if the CompletionImpl constructor throws, we'll need the handler to
    // deallocate this memory. that requires a copy of the handler, unless we
    // can prove that it can't throw
    if constexpr (std::is_nothrow_constructible_v<UserData<T>, TArgs...> &&
                  std::is_nothrow_move_constructible_v<Handler>) {
      return Ptr{new (handler) CompletionImpl(ex, std::move(handler),
                                              std::forward<TArgs>(args)...)};
    } else {
      Handler h = handler;
      return Ptr{new (h) CompletionImpl(ex, std::move(handler),
                                        std::forward<TArgs>(args)...)};
    }
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
  c->destroy_defer(std::forward_as_tuple(std::forward<Args2>(args)...));
}

template <typename T, typename ...Args>
template <typename ...Args2>
void Completion<void(Args...), T>::dispatch(std::unique_ptr<Completion>&& ptr,
                                            Args2&& ...args)
{
  auto c = ptr.release();
  c->destroy_dispatch(std::forward_as_tuple(std::forward<Args2>(args)...));
}

template <typename T, typename ...Args>
template <typename ...Args2>
void Completion<void(Args...), T>::post(std::unique_ptr<Completion>&& ptr,
                                        Args2&& ...args)
{
  auto c = ptr.release();
  c->destroy_post(std::forward_as_tuple(std::forward<Args2>(args)...));
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
/// handler, then defer() it on its associated executor
template <typename Signature, typename T, typename ...Args>
void dispatch(std::unique_ptr<Completion<Signature, T>>&& ptr, Args&& ...args)
{
  Completion<Signature, T>::dispatch(std::move(ptr), std::forward<Args>(args)...);
}

/// take ownership of the completion, bind any arguments to the completion
/// handler, then defer() it on its associated executor
template <typename Signature, typename T, typename ...Args>
void post(std::unique_ptr<Completion<Signature, T>>&& ptr, Args&& ...args)
{
  Completion<Signature, T>::post(std::move(ptr), std::forward<Args>(args)...);
}

} // namespace ceph::async

#endif // CEPH_ASYNC_COMPLETION_H
