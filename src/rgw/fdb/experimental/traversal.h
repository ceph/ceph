// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 * Licensed under the GNU Lesser General Public License version 2.1.
 * See file COPYING.
 */

#ifndef CEPH_RGW_FDB_EXPERIMENTAL_TRAVERSAL_H
 #define CEPH_RGW_FDB_EXPERIMENTAL_TRAVERSAL_H

#if defined(CEPH_LIBFDB_EXPERIMENTAL_NVIDIA_STDEXEC) && defined(CEPH_LIBFDB_EXPERIMENTAL_ASIO)
 #error "libfdb experimental concurrency requires exactly one backend"
#elif defined(CEPH_LIBFDB_EXPERIMENTAL_NVIDIA_STDEXEC)
 #include "stdexec.h"
#elif defined(CEPH_LIBFDB_EXPERIMENTAL_ASIO)
 #include "asio.h"
#else
 #error "enable LIBFDB_EXPERIMENTAL_CONCURRENCY=ASIO or NVIDIA_STDEXEC and link rgw_fdb_execution"
#endif

#include <algorithm>
#include <array>
#include <concepts>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <ranges>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>

#if defined(CEPH_LIBFDB_EXPERIMENTAL_NVIDIA_STDEXEC)
 #include <exec/repeat_n.hpp>
 #include <exec/repeat_until.hpp>
 #include <exec/trampoline_scheduler.hpp>
 #include <exec/variant_sender.hpp>
#elif defined(CEPH_LIBFDB_EXPERIMENTAL_ASIO)
 #include <atomic>
 #include <mutex>

 #include <boost/asio/co_composed.hpp>
 #include <boost/asio/consign.hpp>
 #include <boost/asio/experimental/parallel_group.hpp>
 #include <boost/asio/require.hpp>
 #include <boost/asio/system_executor.hpp>
#endif

namespace ceph::libfdb::experimental {

struct scan_options final
{
 read_mode mode = read_mode::serializable;

 bool prefetch = false;
};

namespace detail {

template <typename SourceT>
struct traversal_state final
{
 struct batch final
 {
  transaction_handle txn;

  std::optional<::ceph::libfdb::detail::query_window> window;
 };

 SourceT source;

 ::ceph::libfdb::detail::query_cursor cursor;

 scan_options options;

 std::array<batch, 2> batches;
 std::size_t current = 0;

 std::size_t count = 0;

 public:
 template <query::expression SelectionT>
 traversal_state(SourceT source, SelectionT selection, const scan_options options)
  : source(std::move(source)),
    cursor([&] {
     if constexpr (std::same_as<SourceT, transaction_handle>) {
      return ::ceph::libfdb::detail::query_cursor {std::move(selection)};
     }
     return ::ceph::libfdb::detail::make_managed_query_cursor(std::move(selection));
    }()),
    options(options)
 {}

 public:
 void prepare(const std::size_t index)
 {
  if constexpr (std::same_as<SourceT, transaction_handle>) {
   batches[index].txn = source;
  } else {
   batches[index].txn = source.make();
  }
 }

 auto read_batch(const std::size_t index)
 {
  return make_future_operation(scan_request<::ceph::libfdb::detail::query_cursor&> {
   {}, batches[index].txn, cursor, options.mode
  });
 }

 auto replay(const std::size_t index, const std::exception_ptr& error)
 {
  retry_if<> policy;
  const auto native = ::ceph::libfdb::detail::retryable_error(error, batches[index].txn, policy);
  if (not native) {
   std::rethrow_exception(error);
  }

  return ::ceph::libfdb::experimental::reset_for_replay(batches[index].txn,
    std::error_code(*native, ::ceph::libfdb::fdb_error_category()));
 }

 bool advance()
 {
  auto& batch = batches[current];
  count += std::size(batch.window->result_pairs);
  batch.window.reset();
  batch.txn.reset();
  current = 1 - current;

  return batches[current].window.has_value();
 }

};

#if defined(CEPH_LIBFDB_EXPERIMENTAL_NVIDIA_STDEXEC)

template <typename SourceT>
auto read_traversal_batch(traversal_state<SourceT>& traversal, const std::size_t index)
{
 return stdexec::let_value(stdexec::just(), [&traversal, index] {
  traversal.prepare(index);
  auto batch_read = stdexec::let_value(stdexec::just(), [&traversal, index] {
   return traversal.read_batch(index) | stdexec::then([&traversal, index](auto window) {
    traversal.batches[index].window.emplace(std::move(window));
    return true;
   });
  });
  if constexpr (std::same_as<SourceT, transaction_handle>) {
   return std::move(batch_read) | stdexec::then([](bool) {});
  } else {
   return ::exec::repeat_until(std::move(batch_read) |
     stdexec::let_error([&traversal, index](const std::exception_ptr error) {
      return traversal.replay(index, error) | stdexec::then([] { return false; });
     }));
  }
 });
}

template <typename SourceT, typename FnT>
auto consume_traversal_batch(traversal_state<SourceT>& traversal, const std::size_t index, FnT& fn)
{
 auto consume = ::ceph::libfdb::detail::raw_row_callback(fn);
 if constexpr (std::is_void_v<std::invoke_result_t<decltype(consume), const FDBKeyValue&>>) {
  return stdexec::just() | stdexec::then([&traversal, index, consume] {
   std::ranges::for_each(traversal.batches[index].window->result_pairs, consume);
  });
 } else {
  return stdexec::let_value(
    stdexec::just(std::ranges::begin(traversal.batches[index].window->result_pairs)),
    [&traversal, index, consume](auto& row) {
     auto step = stdexec::let_value(stdexec::just(), [&row, consume]() -> decltype(auto) {
      return consume(*row);
     }) | stdexec::then([&row](auto&&...) { ++row; });
     auto loop = ::exec::repeat_n(std::move(step),
       std::size(traversal.batches[index].window->result_pairs));
     using result_t = ::exec::variant_sender<decltype(stdexec::just()), decltype(loop)>;

     // An empty batch succeeds even when the callback has only error or stopped channels.
     if (std::empty(traversal.batches[index].window->result_pairs)) {
      return result_t {stdexec::just()};
     }
     return result_t {std::move(loop)};
    });
 }
}

template <typename SourceT, typename FnT>
auto traversal_round(traversal_state<SourceT>& traversal, FnT& fn)
{
 auto next_read = [&traversal] {
  return ::exec::repeat_n(read_traversal_batch(traversal, 1 - traversal.current),
                          traversal.cursor ? 1 : 0);
 };
 auto consumer = consume_traversal_batch(traversal, traversal.current, fn);
 using parallel_t = decltype(stdexec::when_all(next_read(), std::move(consumer)));
 using serial_t = decltype(stdexec::let_value(std::move(consumer), next_read));
 using result_t = ::exec::variant_sender<parallel_t, serial_t>;

 if (traversal.options.prefetch) {
  return result_t {stdexec::when_all(next_read(), std::move(consumer))};
 }
 return result_t {stdexec::let_value(std::move(consumer), std::move(next_read))};
}

template <typename SourceT, typename FnT, typename ReceiverT>
class traversal_operation final
{
 traversal_state<SourceT> traversal;
 FnT fn;

 ReceiverT receiver;

 struct step_receiver final
 {
  using receiver_concept = stdexec::receiver_tag;

  traversal_operation& parent;

  void set_value() noexcept { parent.step_completed(); }

  template <typename ErrorT>
  void set_error(ErrorT&& error) noexcept
  {
   stdexec::set_error(std::move(parent.receiver), std::forward<ErrorT>(error));
  }

  void set_stopped() noexcept { stdexec::set_stopped(std::move(parent.receiver)); }

  [[nodiscard]] decltype(auto) get_env() const noexcept { return stdexec::get_env(parent.receiver); }

 };

 static constexpr auto first_step = [](traversal_operation& parent) {
  return read_traversal_batch(parent.traversal, 0);
 };
 static constexpr auto yield_step = [](traversal_operation&) {
  return stdexec::schedule(::exec::trampoline_scheduler {});
 };
 static constexpr auto next_step = [](traversal_operation& parent) {
  return traversal_round(parent.traversal, parent.fn);
 };

 template <auto Factory>
 struct connected_step final
 {
  using sender_t = std::invoke_result_t<decltype(Factory), traversal_operation&>;

  stdexec::connect_result_t<sender_t, step_receiver> state;

  explicit connected_step(traversal_operation& parent)
   : state(stdexec::connect(Factory(parent), step_receiver {parent}))
  {}

  void start() noexcept { stdexec::start(state); }
 };

 std::variant<std::monostate, connected_step<first_step>, connected_step<yield_step>,
              connected_step<next_step>> active;

 template <auto Factory>
 void start_step() noexcept
 {
  if (stdexec::get_stop_token(stdexec::get_env(receiver)).stop_requested()) {
   stdexec::set_stopped(std::move(receiver));
   return;
  }
  try {
   active.template emplace<connected_step<Factory>>(*this).start();
  } catch (...) {
   stdexec::set_error(std::move(receiver), std::current_exception());
  }
 }

 void step_completed() noexcept
 {
  if (std::holds_alternative<connected_step<first_step>>(active)) {
   start_step<yield_step>();
   return;
  }
  if (std::holds_alternative<connected_step<yield_step>>(active)) {
   start_step<next_step>();
   return;
  }
  if (traversal.advance()) {
   start_step<yield_step>();
   return;
  }
  stdexec::set_value(std::move(receiver), std::size_t {traversal.count});
 }

 public:
 using operation_state_concept = stdexec::operation_state_tag;

 traversal_operation(traversal_state<SourceT> state, FnT callback, ReceiverT completion)
  : traversal(std::move(state)), fn(std::move(callback)), receiver(std::move(completion))
 {}

 void start() & noexcept
 {
  if (stdexec::get_stop_token(stdexec::get_env(receiver)).stop_requested()) {
   stdexec::set_stopped(std::move(receiver));
   return;
  }
  if (not traversal.cursor) {
   stdexec::set_value(std::move(receiver), std::size_t {0});
   return;
  }
  start_step<first_step>();
 }

};

template <typename SourceT, typename FnT>
class traversal_sender final
{
 traversal_state<SourceT> traversal;
 FnT fn;

 public:
 using sender_concept = stdexec::sender_tag;

 template <typename SelfT, typename ...EnvTs>
 static consteval auto get_completion_signatures()
 {
  using bytes_t = ::ceph::libfdb::detail::byte_view;
  using callback_t = std::invoke_result_t<FnT&, bytes_t, bytes_t>;
  using completions_t = stdexec::completion_signatures<stdexec::set_value_t(std::size_t),
    stdexec::set_error_t(std::exception_ptr), stdexec::set_stopped_t()>;

  if constexpr (std::is_void_v<callback_t>) {
   return completions_t {};
  } else if constexpr (sizeof...(EnvTs) == 0 and stdexec::dependent_sender<callback_t>) {
   return stdexec::get_completion_signatures<callback_t>();
  } else {
   return ::exec::transform_completion_signatures(
     stdexec::get_completion_signatures<callback_t, EnvTs...>(),
     []<typename ...>() { return stdexec::completion_signatures<stdexec::set_value_t(std::size_t)> {}; },
     []<typename ErrorT>() {
      return stdexec::completion_signatures<stdexec::set_error_t(std::decay_t<ErrorT>)> {};
     },
     ::exec::keep_completion<stdexec::set_stopped_t> {},
     completions_t {});
  }
 }

 traversal_sender(traversal_state<SourceT> state, FnT callback)
  : traversal(std::move(state)), fn(std::move(callback))
 {}

 template <stdexec::receiver ReceiverT>
 auto connect(ReceiverT receiver) &&
 {
  return traversal_operation<SourceT, FnT, ReceiverT> {
   std::move(traversal), std::move(fn), std::move(receiver)
  };
 }

 [[nodiscard]] stdexec::env<> get_env() const noexcept { return {}; }

};

template <typename SourceT, query::expression SelectionT, typename FnT>
auto make_traversal_operation(SourceT source, SelectionT selection, const scan_options options, FnT fn)
{
 return operation {traversal_sender {
  traversal_state {std::move(source), std::move(selection), options}, std::move(fn)
 }};
}

#elif defined(CEPH_LIBFDB_EXPERIMENTAL_ASIO)

using traversal_step_signature = void(std::exception_ptr, std::optional<std::tuple<>>);

template <typename HandlerT, typename ValueT>
struct traversal_step_completion final
{
 HandlerT handler;

 std::mutex mutex;
 bool initiated = false;
 bool completed = false;
 std::exception_ptr error;
 std::optional<ValueT> value;

 explicit traversal_step_completion(HandlerT&& completion)
  : handler(std::move(completion))
 {}

 static void finish(std::shared_ptr<traversal_step_completion> completion,
                    const bool initiation, std::exception_ptr error,
                    std::optional<ValueT> value = {})
 {
  std::unique_lock lock {completion->mutex};
  if (initiation) {
   completion->initiated = true;
   if (error) {
    completion->completed = true;
    completion->error = std::move(error);
    completion->value.reset();
   }
  } else {
   completion->completed = true;
   completion->error = std::move(error);
   if (value) {
    completion->value.emplace(std::move(*value));
   } else {
    completion->value.reset();
   }
  }
  if (not completion->initiated or not completion->completed) {
   completion.reset();
   lock.unlock();
   return;
  }
  auto handler = std::move(completion->handler);
  error = std::move(completion->error);
  auto result = std::move(completion->value);
  lock.unlock();
  completion.reset();
  std::move(handler)(std::move(error), std::move(result));
 }

};

template <typename OperationT, typename HandlerT>
auto guard_traversal_operation(OperationT work, const HandlerT& parent)
{
 using value_t = asio_value_t<OperationT>;
 using signature_t = void(std::exception_ptr, std::optional<value_t>);
 auto token = boost::asio::deferred;
 return operation {boost::asio::async_initiate<decltype(token), signature_t>(
   [executor = boost::asio::get_associated_executor(parent),
    allocator = boost::asio::get_associated_allocator(parent)](auto handler, OperationT work) {
    using completion_t = traversal_step_completion<decltype(handler), value_t>;
    std::shared_ptr<completion_t> completion;
    try {
     const auto slot = boost::asio::get_associated_cancellation_slot(handler);
     completion = std::allocate_shared<completion_t>(allocator, std::move(handler));
     std::move(work).native_handle()(boost::asio::bind_allocator(allocator,
       boost::asio::bind_executor(executor, boost::asio::bind_cancellation_slot(slot,
        [completion](std::exception_ptr error, std::optional<value_t> value) mutable {
         completion_t::finish(std::move(completion), false, std::move(error), std::move(value));
        }))));
    } catch (...) {
     // Initiation failure still counts as completion, so the group joins its other child.
     if (not completion) {
      std::move(handler)(std::current_exception(), std::optional<value_t> {});
      return;
     }
     completion_t::finish(std::move(completion), true, std::current_exception());
     return;
    }
    completion_t::finish(std::move(completion), true, {});
   }, token, std::move(work))};
}

template <typename HandlerT>
struct traversal_join_completion final
{
 HandlerT handler;

 std::mutex mutex;
 std::exception_ptr error;
 unsigned pending_stages = 2;

 // Written only by the initiating thread, before starting either child.
 bool launched = false;

 explicit traversal_join_completion(HandlerT&& completion)
  : handler(std::move(completion))
 {}

 static void complete(std::shared_ptr<traversal_join_completion> completion,
                       const bool initiation, std::exception_ptr failure)
 {
  std::unique_lock lock {completion->mutex};
  // Without children, failed initiation also completes the group's stage.
  completion->pending_stages -= initiation and failure and not completion->launched ? 2 : 1;
  if (failure and not completion->error) {
   completion->error = std::move(failure);
  }
  if (0 == completion->pending_stages) {
   lock.unlock();
   deliver(std::move(completion));
  } else {
   completion.reset();
   lock.unlock();
  }
 }

 static void deliver(std::shared_ptr<traversal_join_completion> completion) noexcept
 {
  const auto executor = boost::asio::get_associated_executor(completion->handler);
  const auto allocator = boost::asio::get_associated_allocator(completion->handler);
  post_completion<&traversal_join_completion::mutex>(executor, allocator, std::move(completion),
    [](auto completion, std::exception_ptr failure) {
     const auto error = failure ? std::move(failure) : completion->error;
     auto handler = std::move(completion->handler);
     completion.reset();
     std::move(handler)(error, error ? std::nullopt : std::optional {std::tuple<> {}});
    });
 }

};

template <typename ReadT, typename ConsumeT>
auto join_traversal_steps(ReadT next_read, ConsumeT consumer)
{
 auto token = boost::asio::deferred;
 return operation {boost::asio::async_initiate<decltype(token), traversal_step_signature>(
   [](auto handler, ReadT next_read, ConsumeT consumer) {
    const auto allocator = boost::asio::get_associated_allocator(handler);
    const auto slot = boost::asio::get_associated_cancellation_slot(handler);
    auto first = guard_traversal_operation(std::move(next_read), handler);
    auto second = guard_traversal_operation(std::move(consumer), handler);
    using completion_t = traversal_join_completion<decltype(handler)>;
    auto completion = std::allocate_shared<completion_t>(allocator, std::move(handler));
    auto track_start = [completion = completion.get()](auto work) {
     auto token = boost::asio::deferred;
     return boost::asio::async_initiate<decltype(token), traversal_step_signature>(
       [completion](auto handler, auto work) {
        completion->launched = true;
        std::move(work)(std::move(handler));
       }, token, std::move(work).native_handle());
    };
    // Only enqueueing runs inline; coroutine and application work resumes on its executor.
    const auto inline_executor = boost::asio::require(boost::asio::system_executor {},
      boost::asio::execution::blocking.always);
    std::exception_ptr initiation_error;
    try {
     boost::asio::experimental::make_parallel_group(track_start(std::move(first)),
       track_start(std::move(second))).async_wait(
       boost::asio::experimental::wait_for_one_error {},
       boost::asio::bind_executor(inline_executor, boost::asio::bind_cancellation_slot(slot,
        [completion](auto order, std::exception_ptr read_error, auto,
                      std::exception_ptr consume_error, auto) mutable noexcept {
         const std::array errors {read_error, consume_error};
         const auto failed = std::ranges::find_if(order,
           [&errors](auto index) { return bool(errors[index]); });
         auto error = std::ranges::end(order) == failed ? std::exception_ptr {} : errors[*failed];
         completion_t::complete(std::move(completion), false, std::move(error));
        })));
    } catch (...) {
     initiation_error = std::current_exception();
    }
    // parallel_group installs its parent cancellation handler after starting children.
    // A failure there must still await those children before releasing borrowed state.
    completion_t::complete(std::move(completion), true, std::move(initiation_error));
   }, token, std::move(next_read), std::move(consumer))};
}

template <typename SourceT>
auto read_traversal_batch(traversal_state<SourceT>& traversal, const std::size_t index)
{
 auto token = boost::asio::deferred;
 return operation {boost::asio::async_initiate<decltype(token), traversal_step_signature>(
   boost::asio::co_composed<traversal_step_signature>(
    [](auto state, traversal_state<SourceT>& traversal, const std::size_t index) -> void {
     state.reset_cancellation_state(boost::asio::enable_partial_cancellation());
     state.throw_if_cancelled(true);
     try {
      traversal.prepare(index);
      for (;;) {
       std::exception_ptr error;
       try {
        auto window = co_await guard_traversal_operation(traversal.read_batch(index),
          state.handler()).native_handle();
        traversal.batches[index].window.emplace(std::move(*window));
       } catch (...) {
        error = std::current_exception();
       }
       if (not error) {
        break;
       }
       if constexpr (std::same_as<SourceT, transaction_handle>) {
        std::rethrow_exception(error);
       } else {
        co_await guard_traversal_operation(traversal.replay(index, error),
          state.handler()).native_handle();
       }
      }
      co_return {std::exception_ptr {}, std::tuple<> {}};
     } catch (...) {
      co_return {std::current_exception(), std::nullopt};
     }
    }), token, std::ref(traversal), index)};
}

template <typename SourceT, typename FnT>
auto consume_traversal_batch(traversal_state<SourceT>& traversal, const std::size_t index, FnT& fn)
{
 auto token = boost::asio::deferred;
 return operation {boost::asio::async_initiate<decltype(token), traversal_step_signature>(
   boost::asio::co_composed<traversal_step_signature>(
    [](auto state, traversal_state<SourceT>& traversal, const std::size_t index, FnT& fn) -> void {
     state.reset_cancellation_state(boost::asio::enable_partial_cancellation());
     state.throw_if_cancelled(true);
     try {
      auto consume = ::ceph::libfdb::detail::raw_row_callback(fn);
      if constexpr (std::is_void_v<std::invoke_result_t<decltype(consume), const FDBKeyValue&>>) {
       std::ranges::for_each(traversal.batches[index].window->result_pairs, consume);
      } else {
       for (const auto& pair : traversal.batches[index].window->result_pairs) {
        co_await guard_traversal_operation(consume(pair),
          state.handler()).native_handle();
       }
      }
      co_return {std::exception_ptr {}, std::tuple<> {}};
     } catch (...) {
      co_return {std::current_exception(), std::nullopt};
     }
    }), token, std::ref(traversal), index, std::ref(fn))};
}

template <typename SourceT, query::expression SelectionT, typename FnT>
auto make_traversal_operation(SourceT source, SelectionT selection, const scan_options options, FnT fn)
{
 using signature_t = void(std::exception_ptr, std::optional<std::size_t>);
 auto initiate = boost::asio::co_composed<signature_t>(
    [](auto state, traversal_state<SourceT> traversal, FnT fn) -> void {
     state.reset_cancellation_state(boost::asio::enable_partial_cancellation());
     state.throw_if_cancelled(true);
     try {
      co_await boost::asio::post(boost::asio::deferred);
      if (not traversal.cursor) {
       co_return {std::exception_ptr {}, std::size_t {0}};
      }
      co_await guard_traversal_operation(read_traversal_batch(traversal, 0),
        state.handler()).native_handle();
      do {
       const auto current = traversal.current;
       const auto next = 1 - current;
       if (traversal.options.prefetch and traversal.cursor) {
        co_await guard_traversal_operation(join_traversal_steps(
          read_traversal_batch(traversal, next), consume_traversal_batch(traversal, current, fn)),
          state.handler()).native_handle();
        continue;
       }
       co_await guard_traversal_operation(consume_traversal_batch(traversal, current, fn),
         state.handler()).native_handle();
       if (traversal.cursor) {
        co_await guard_traversal_operation(read_traversal_batch(traversal, next),
          state.handler()).native_handle();
       }
      } while (traversal.advance());
      co_return {std::exception_ptr {}, traversal.count};
     } catch (...) {
      co_return {std::current_exception(), std::nullopt};
     }
    });
 auto token = boost::asio::deferred;
 return operation {boost::asio::async_initiate<decltype(token), signature_t>(
   [initiate](auto handler, traversal_state<SourceT> traversal, FnT fn) mutable {
    // Keep run() alive even when a failed post resumes a child on the FDB thread.
    auto work = boost::asio::make_work_guard(boost::asio::get_associated_executor(handler));
    auto completion = boost::asio::consign(std::move(handler), std::move(work));
    boost::asio::async_initiate<decltype(completion), signature_t>(std::move(initiate), completion,
      std::move(traversal), std::move(fn));
   }, token, traversal_state {std::move(source), std::move(selection), options}, std::move(fn))};
}

#endif

} // namespace detail

// The operation owns its callback. Returned native work completes before its row's bytes expire.
template <query::expression SelectionT, typename FnT>
[[nodiscard]] auto for_each(raw_t, transaction_handle txn, SelectionT selection,
                             const scan_options options, FnT&& fn)
{
 return detail::make_traversal_operation(std::move(txn), std::move(selection), options,
                                          std::forward<FnT>(fn));
}

template <query::expression SelectionT, typename FnT>
[[nodiscard]] auto for_each(raw_t tag, transaction_handle txn, SelectionT selection, FnT&& fn)
{
 return for_each(tag, std::move(txn), std::move(selection), scan_options {}, std::forward<FnT>(fn));
}

template <query::expression SelectionT, typename FnT>
[[nodiscard]] auto for_each(raw_t, database_handle dbh, transaction_options txn_options,
                             SelectionT selection, const scan_options options, FnT&& fn)
{
 return detail::make_traversal_operation(
   ::ceph::libfdb::detail::transaction_source {std::move(dbh), std::move(txn_options)},
   std::move(selection), options, std::forward<FnT>(fn));
}

template <query::expression SelectionT, typename FnT>
[[nodiscard]] auto for_each(raw_t tag, database_handle dbh, SelectionT selection,
                             const scan_options options, FnT&& fn)
{
 return for_each(tag, std::move(dbh), transaction_options {}, std::move(selection), options,
                  std::forward<FnT>(fn));
}

template <query::expression SelectionT, typename FnT>
[[nodiscard]] auto for_each(raw_t tag, database_handle dbh, transaction_options options,
                             SelectionT selection, FnT&& fn)
{
 return for_each(tag, std::move(dbh), std::move(options), std::move(selection), scan_options {},
                  std::forward<FnT>(fn));
}

template <query::expression SelectionT, typename FnT>
[[nodiscard]] auto for_each(raw_t tag, database_handle dbh, SelectionT selection, FnT&& fn)
{
 return for_each(tag, std::move(dbh), std::move(selection), scan_options {}, std::forward<FnT>(fn));
}

} // namespace ceph::libfdb::experimental

#endif
