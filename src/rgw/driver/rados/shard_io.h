// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <map>
#include <string>
#include <vector>

#include <boost/asio/any_completion_handler.hpp>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/associator.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/co_composed.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/system/error_code.hpp>

#include "librados/librados_asio.h"
#include "common/dout.h"

/// Concurrent io algorithms for data sharded across rados objects.
namespace rgwrados::shard_io {

using boost::system::error_code;

/// Classify the result of a single shard operation.
enum class Result { Success, Retry, Error };

namespace detail {

struct RevertibleWriteHandler;
struct RevertHandler;
struct WriteHandler;
struct ReadHandler;

} // namespace detail

/// Interface for async_writes() that can initiate write operations and
/// their inverse. The latter is used to restore the original state on
/// error or cancellation.
///
/// \see RadosRevertibleWriter
class RevertibleWriter : public DoutPrefixPipe {
 public:
  using executor_type = boost::asio::any_io_executor;
  executor_type get_executor() const { return ex; }

  RevertibleWriter(const DoutPrefixProvider& dpp, executor_type ex)
    : DoutPrefixPipe(dpp), ex(std::move(ex)) {}

  virtual ~RevertibleWriter() {}

  /// Initiate an async write operation for the given shard object.
  virtual void write(int shard, const std::string& object,
                     detail::RevertibleWriteHandler&& handler) = 0;

  /// Classify the result of a completed shard write operation.
  virtual Result on_complete(int shard, error_code ec) {
    return ec ? Result::Error : Result::Success;
  }

  /// Initiate an async operation to revert the side effects of write().
  /// Revert operations do not call on_complete() on completion.
  virtual void revert(int shard, const std::string& object,
                      detail::RevertHandler&& handler) = 0;

  void add_prefix(std::ostream& out) const override {
    out << "shard writer: ";
  }

 private:
  executor_type ex;
};

/// Interface for async_writes() that can initiate write operations.
///
/// \see RadosWriter
class Writer : public DoutPrefixPipe {
 public:
  using executor_type = boost::asio::any_io_executor;
  executor_type get_executor() const { return ex; }

  Writer(const DoutPrefixProvider& dpp, executor_type ex)
    : DoutPrefixPipe(dpp), ex(std::move(ex)) {}

  virtual ~Writer() {}

  /// Initiate an async write operation for the given shard object.
  virtual void write(int shard, const std::string& object,
                     detail::WriteHandler&& handler) = 0;

  /// Classify the result of a completed shard write operation. Returning
  /// Retry will trigger another write() and its corresponding on_complete().
  virtual Result on_complete(int shard, error_code ec) {
    return ec ? Result::Error : Result::Success;
  }

  void add_prefix(std::ostream& out) const override {
    out << "shard writer: ";
  }

 private:
  executor_type ex;
};

/// Interface for async_reads() that can initiate read operations.
///
/// \see RadosReader
class Reader : public DoutPrefixPipe {
 public:
  using executor_type = boost::asio::any_io_executor;
  executor_type get_executor() const { return ex; }

  Reader(const DoutPrefixProvider& dpp, executor_type ex)
    : DoutPrefixPipe(dpp), ex(std::move(ex)) {}

  virtual ~Reader() {}

  /// Initiate an async read operation for the given shard object.
  virtual void read(int shard, const std::string& object,
                    detail::ReadHandler&& handler) = 0;

  /// Classify the result of a completed shard read operation. Returning
  /// Retry will trigger another read() and its corresponding on_complete().
  virtual Result on_complete(int shard, error_code ec) {
    return ec ? Result::Error : Result::Success;
  }

  void add_prefix(std::ostream& out) const override {
    out << "shard reader: ";
  }

 private:
  executor_type ex;
};

namespace detail {

struct Shard : boost::intrusive::list_base_hook<> {
  boost::asio::cancellation_signal signal;
  std::map<int, std::string>::const_iterator iter;
  int id() const { return iter->first; }
  const std::string& object() const { return iter->second; }
};
using ShardList = boost::intrusive::list<Shard>;

inline auto make_shards(const std::map<int, std::string>& objects)
  -> std::vector<Shard>
{
  // allocate the shards and assign an 'objects' iterator to each
  auto shards = std::vector<Shard>(objects.size());
  auto s = shards.begin();
  for (auto o = objects.begin(); o != objects.end(); ++o, ++s) {
    s->iter = o;
  }
  return shards;
}

// handler that wakes the caller of async_wait()
using WaitHandler = boost::asio::any_completion_handler<void()>;

// cancellation handler that forwards signals to all outstanding requests
struct WaitCancellation {
  ShardList& sending;
  ShardList& outstanding;
  ShardList* completed;
  error_code& failure;
  bool& terminal;

  WaitCancellation(ShardList& sending, ShardList& outstanding,
                   ShardList* completed, error_code& failure, bool& terminal)
    : sending(sending), outstanding(outstanding), completed(completed),
      failure(failure), terminal(terminal) {}

  void operator()(boost::asio::cancellation_type type) {
    if (type != boost::asio::cancellation_type::none) {
      if (!!(type & boost::asio::cancellation_type::terminal)) {
        terminal = true;
      }
      if (!failure) {
        failure = make_error_code(boost::asio::error::operation_aborted);
        if (completed) {
          // for RevertibleWriter, schedule completed shards for reverts
          sending = std::move(*completed);
        }
      }
      auto i = outstanding.begin();
      while (i != outstanding.end()) {
        // emit() may dispatch the completion that removes i from outstanding
        auto& shard = *i++;
        shard.signal.emit(type);
      }
    }
  }
};

// suspend the calling coroutine and capture a WaitHandler to resume it
template <typename CompletionToken>
auto async_wait(WaitHandler& wakeup, ShardList& sending, ShardList& outstanding,
                ShardList* completed, error_code& failure, bool& terminal,
                CompletionToken&& token)
{
  return boost::asio::async_initiate<CompletionToken, void()>(
      [&, completed] (auto handler) {
        wakeup = std::move(handler);
        auto slot = boost::asio::get_associated_cancellation_slot(wakeup);
        if (slot.is_connected()) {
          slot.template emplace<WaitCancellation>(
              sending, outstanding, completed, failure, terminal);
        }
      }, token);
}

inline void maybe_complete(WaitHandler& wakeup, ShardList& sending,
                           ShardList& outstanding, bool terminal)
{
  const bool ready_to_send = !sending.empty() && !terminal;
  const bool done_waiting = outstanding.empty();
  if (wakeup && (ready_to_send || done_waiting)) {
    auto slot = boost::asio::get_associated_cancellation_slot(wakeup);
    slot.clear(); // remove async_wait()'s cancellation handler
    boost::asio::dispatch(std::move(wakeup));
  }
}

struct RevertibleWriteHandler {
  RevertibleWriter& writer;
  ShardList& sending;
  ShardList& outstanding;
  ShardList& completed;
  WaitHandler& waiter;
  error_code& failure;
  bool& terminal;
  Shard& shard;

  using executor_type = boost::asio::any_io_executor;
  executor_type get_executor() const { return writer.get_executor(); }

  using cancellation_slot_type = boost::asio::cancellation_slot;
  cancellation_slot_type get_cancellation_slot() const noexcept {
    return shard.signal.slot();
  }

  void operator()(error_code ec, version_t = {}) {
    outstanding.erase(outstanding.iterator_to(shard));

    const auto result = writer.on_complete(shard.id(), ec);
    switch (result) {
      case Result::Retry:
        ldpp_dout(&writer, 20) << "write on '" << shard.object()
            << "' needs retry: " << ec.message() << dendl;
        if (!failure) {
          // reschedule the shard for sending
          sending.push_back(shard);
        }
        break;
      case Result::Success:
        if (!failure) {
          // track as 'completed' in case reverts are necessary
          completed.push_back(shard);
        } else if (!terminal) {
          // add directly to 'sending' for revert
          sending.push_back(shard);
        }
        break;
      case Result::Error:
        ldpp_dout(&writer, 4) << "write on '" << shard.object()
            << "' failed: " << ec.message() << dendl;
        if (!failure) {
          failure = ec;
          // schedule completed shards for reverts
          sending = std::move(completed);
        }
        break;
    }

    maybe_complete(waiter, sending, outstanding, terminal);
  }
}; // struct RevertibleWriteHandler

struct RevertHandler {
  RevertibleWriter& writer;
  ShardList& sending;
  ShardList& outstanding;
  WaitHandler& waiter;
  bool& terminal;
  Shard& shard;

  using executor_type = boost::asio::any_io_executor;
  executor_type get_executor() const { return writer.get_executor(); }

  using cancellation_slot_type = boost::asio::cancellation_slot;
  cancellation_slot_type get_cancellation_slot() const noexcept {
    return shard.signal.slot();
  }

  void operator()(error_code ec, version_t = {}) {
    outstanding.erase(outstanding.iterator_to(shard));

    if (ec) {
      ldpp_dout(&writer, 4) << "revert on '" << shard.object()
          << "' failed: " << ec.message() << dendl;
    }

    maybe_complete(waiter, sending, outstanding, terminal);
  }
}; // struct RevertHandler

} // namespace detail

/// Try to apply concurrent write operations to the shard objects. Upon error
/// or partial/total cancellation, revert any operations that succeeded.
template <boost::asio::completion_token_for<void(error_code)> CompletionToken>
auto async_writes(RevertibleWriter& writer,
                  const std::map<int, std::string>& objects,
                  size_t max_concurrent,
                  CompletionToken&& token)
{
  return boost::asio::async_initiate<CompletionToken, void(error_code)>(
      boost::asio::co_composed<void(error_code)>(
          [] (auto state, RevertibleWriter& writer,
              const std::map<int, std::string>& objects,
              size_t max_concurrent) -> void
          {
            state.reset_cancellation_state(
                boost::asio::enable_total_cancellation());

            // initialize the shards array and schedule each for sending
            auto shards = detail::make_shards(objects);
            detail::ShardList sending{shards.begin(), shards.end()};
            detail::ShardList outstanding;
            detail::ShardList completed;
            detail::WaitHandler waiter;
            error_code failure;
            bool terminal = false;

            for (;;) {
              while (!sending.empty() && !terminal) {
                if (outstanding.size() >= max_concurrent) {
                  // wait for the next completion before sending another
                  co_await detail::async_wait(
                      waiter, sending, outstanding, &completed,
                      failure, terminal, boost::asio::deferred);

                  if (!!state.cancelled()) {
                    // once partial/total cancellation is requested, only
                    // terminal cancellation can stop the reverts
                    state.reset_cancellation_state(
                        boost::asio::enable_terminal_cancellation());
                  }
                  continue; // recheck loop conditions
                }

                // prepare and send the next shard object operation
                detail::Shard& shard = sending.front();
                sending.pop_front();
                outstanding.push_back(shard);

                if (failure) {
                  writer.revert(shard.id(), shard.object(),
                                detail::RevertHandler{
                                    writer, sending, outstanding,
                                    waiter, terminal, shard});
                } else {
                  writer.write(shard.id(), shard.object(),
                               detail::RevertibleWriteHandler{
                                   writer, sending, outstanding, completed,
                                   waiter, failure, terminal, shard});
                }
              } // while (!sending.empty() && !terminal)

              if (outstanding.empty()) {
                // nothing left to send or receive, we're done
                co_return failure;
              }

              // wait for outstanding completions
              co_await detail::async_wait(
                  waiter, sending, outstanding, &completed,
                  failure, terminal, boost::asio::deferred);

              if (!!state.cancelled()) {
                state.reset_cancellation_state(
                    boost::asio::enable_terminal_cancellation());
              }
            } // for (;;)
            // unreachable
          }, writer),
      token, std::ref(writer), objects, max_concurrent);
} // async_writes(RevertibleWriter)

namespace detail {

struct WriteHandler {
  Writer& writer;
  ShardList& sending;
  ShardList& outstanding;
  WaitHandler& waiter;
  error_code& failure;
  bool& terminal;
  Shard& shard;

  using executor_type = boost::asio::any_io_executor;
  executor_type get_executor() const { return writer.get_executor(); }

  using cancellation_slot_type = boost::asio::cancellation_slot;
  cancellation_slot_type get_cancellation_slot() const noexcept {
    return shard.signal.slot();
  }

  void operator()(error_code ec, version_t = {}) {
    outstanding.erase(outstanding.iterator_to(shard));

    const auto result = writer.on_complete(shard.id(), ec);
    switch (result) {
      case Result::Retry:
        ldpp_dout(&writer, 20) << "write on '" << shard.object()
            << "' needs retry: " << ec.message() << dendl;
        // reschedule the shard for sending
        sending.push_back(shard);
        break;
      case Result::Success:
        break;
      case Result::Error:
        ldpp_dout(&writer, 4) << "write on '" << shard.object()
            << "' failed: " << ec.message() << dendl;
        if (!failure) {
          failure = ec;
        }
        break;
    }

    maybe_complete(waiter, sending, outstanding, terminal);
  }
}; // struct WriteHandler

} // namespace detail

/// Make an effort to apply a write operation to all shard objects. On error,
/// finish the operations and retries on other shards before returning.
template <boost::asio::completion_token_for<void(error_code)> CompletionToken>
auto async_writes(Writer& writer,
                  const std::map<int, std::string>& objects,
                  size_t max_concurrent,
                  CompletionToken&& token)
{
  return boost::asio::async_initiate<CompletionToken, void(error_code)>(
      boost::asio::co_composed<void(error_code)>(
          [] (auto state, Writer& writer,
              const std::map<int, std::string>& objects,
              size_t max_concurrent) -> void
          {
            state.reset_cancellation_state(
                boost::asio::enable_terminal_cancellation());

            // initialize the shards array and schedule each for sending
            auto shards = detail::make_shards(objects);
            detail::ShardList sending{shards.begin(), shards.end()};
            detail::ShardList outstanding;
            detail::WaitHandler waiter;
            error_code failure;
            bool terminal = false;

            for (;;) {
              while (!sending.empty() && !terminal) {
                if (outstanding.size() >= max_concurrent) {
                  // wait for the next completion before sending another
                  co_await detail::async_wait(
                      waiter, sending, outstanding, nullptr,
                      failure, terminal, boost::asio::deferred);
                  continue; // recheck loop conditions
                }

                // prepare and send the next shard object operation
                detail::Shard& shard = sending.front();
                sending.pop_front();
                outstanding.push_back(shard);

                writer.write(shard.id(), shard.object(),
                    detail::WriteHandler{writer, sending, outstanding,
                                         waiter, failure, terminal, shard});
              } // while (!sending.empty() && !terminal)

              if (outstanding.empty()) {
                // nothing left to send or receive, we're done
                co_return failure;
              }

              // await the next completion (it may schedule a retry)
              co_await detail::async_wait(
                  waiter, sending, outstanding, nullptr,
                  failure, terminal, boost::asio::deferred);
            } // for (;;)
            // unreachable
          }, writer),
      token, std::ref(writer), objects, max_concurrent);
} // async_writes(Writer)

namespace detail {

struct ReadHandler {
  Reader& reader;
  ShardList& sending;
  ShardList& outstanding;
  WaitHandler& waiter;
  error_code& failure;
  bool& terminal;
  Shard& shard;

  using executor_type = boost::asio::any_io_executor;
  executor_type get_executor() const { return reader.get_executor(); }

  using cancellation_slot_type = boost::asio::cancellation_slot;
  cancellation_slot_type get_cancellation_slot() const noexcept {
    return shard.signal.slot();
  }

  void operator()(error_code ec, version_t = {}, bufferlist = {}) {
    outstanding.erase(outstanding.iterator_to(shard));

    bool need_cancel = false;

    const auto result = reader.on_complete(shard.id(), ec);
    switch (result) {
      case Result::Retry:
        // reschedule the shard for sending
        sending.push_back(shard);
        ldpp_dout(&reader, 20) << "read on '" << shard.object()
            << "' needs retry: " << ec.message() << dendl;
        break;
      case Result::Success:
        break;
      case Result::Error:
        ldpp_dout(&reader, 4) << "read on '" << shard.object()
            << "' failed: " << ec.message() << dendl;
        if (!failure) {
          failure = ec;
          // trigger cancellations after our call to maybe_complete(). one of
          // the cancellations may trigger completion and cause async_reads()
          // to co_return. our call to maybe_complete() would then access
          // variables that were destroyed with async_reads()'s stack
          need_cancel = !outstanding.empty();
        }
        break;
    }

    maybe_complete(waiter, sending, outstanding, terminal);

    if (need_cancel) {
      // cancel outstanding requests
      auto i = outstanding.begin();
      while (i != outstanding.end()) {
        // emit() may recurse and remove i
        auto& s = *i++;
        s.signal.emit(boost::asio::cancellation_type::terminal);
      }
    }
  }
}; // struct ReadHandler

} // namespace detail

/// Issue concurrent read operations to all shard objects, aborting on error.
template <boost::asio::completion_token_for<void(error_code)> CompletionToken>
auto async_reads(Reader& reader,
                 const std::map<int, std::string>& objects,
                 size_t max_concurrent,
                 CompletionToken&& token)
{
  return boost::asio::async_initiate<CompletionToken, void(error_code)>(
      boost::asio::co_composed<void(error_code)>(
          [] (auto state, Reader& reader,
              const std::map<int, std::string>& objects,
              size_t max_concurrent) -> void
          {
            state.reset_cancellation_state(
                boost::asio::enable_terminal_cancellation());

            // initialize the shards array and schedule each for sending
            auto shards = detail::make_shards(objects);
            detail::ShardList sending{shards.begin(), shards.end()};
            detail::ShardList outstanding;
            detail::WaitHandler waiter;
            error_code failure;
            bool terminal = false;

            for (;;) {
              while (!sending.empty() && !failure) {
                if (outstanding.size() >= max_concurrent) {
                  // wait for the next completion before sending another
                  co_await detail::async_wait(
                      waiter, sending, outstanding, nullptr,
                      failure, terminal, boost::asio::deferred);
                  continue; // recheck loop conditions
                }

                // prepare and send the next shard object operation
                detail::Shard& shard = sending.front();
                sending.pop_front();
                outstanding.push_back(shard);

                reader.read(shard.id(), shard.object(),
                    detail::ReadHandler{reader, sending, outstanding,
                                        waiter, failure, terminal, shard});
              } // while (!sending.empty() && !failure)

              if (outstanding.empty()) {
                // nothing left to send or receive, we're done
                co_return failure;
              }

              // await the next completion (it may schedule a retry)
              co_await detail::async_wait(
                  waiter, sending, outstanding, nullptr,
                  failure, terminal, boost::asio::deferred);
            } // for (;;)
            // unreachable
          }, reader),
      token, std::ref(reader), objects, max_concurrent);
} // async_reads(Reader)


/// Interface for async_writes() that can prepare rados write operations
/// and their inverse.
class RadosRevertibleWriter : public RevertibleWriter {
 public:
  RadosRevertibleWriter(const DoutPrefixProvider& dpp, executor_type ex,
                        librados::IoCtx& ioctx)
    : RevertibleWriter(dpp, std::move(ex)), ioctx(ioctx)
  {}

  /// Prepare a rados write operation for the given shard.
  virtual void prepare_write(int shard, librados::ObjectWriteOperation& op) = 0;

  /// Prepare a rados operation to revert the side effects of prepare_write().
  /// Revert operations do not call on_complete() on completion.
  virtual void prepare_revert(int shard, librados::ObjectWriteOperation& op) = 0;

 private:
  // implement write() and revert() in terms of prepare_write(),
  // prepare_revert() and librados::async_operate()
  void write(int shard, const std::string& object,
            detail::RevertibleWriteHandler&& handler) final {
    librados::ObjectWriteOperation op;
    prepare_write(shard, op);

    constexpr int flags = 0;
    constexpr jspan_context* trace = nullptr;
    librados::async_operate(get_executor(), ioctx, object, std::move(op),
                            flags, trace, std::move(handler));
  }

  void revert(int shard, const std::string& object,
              detail::RevertHandler&& handler) final {
    librados::ObjectWriteOperation op;
    prepare_revert(shard, op);

    constexpr int flags = 0;
    constexpr jspan_context* trace = nullptr;
    librados::async_operate(get_executor(), ioctx, object, std::move(op),
                            flags, trace, std::move(handler));
  }

  librados::IoCtx& ioctx;
};

/// Interface for async_writes() that can prepare rados write operations.
class RadosWriter : public Writer {
 public:
  RadosWriter(const DoutPrefixProvider& dpp, executor_type ex,
              librados::IoCtx& ioctx)
    : Writer(dpp, std::move(ex)), ioctx(ioctx)
  {}

  /// Prepare a rados write operation for the given shard.
  virtual void prepare_write(int shard, librados::ObjectWriteOperation& op) = 0;

 private:
  // implement write() in terms of prepare_write() and librados::async_operate()
  void write(int shard, const std::string& object,
            detail::WriteHandler&& handler) final {
    librados::ObjectWriteOperation op;
    prepare_write(shard, op);

    constexpr int flags = 0;
    constexpr jspan_context* trace = nullptr;
    librados::async_operate(get_executor(), ioctx, object, std::move(op),
                            flags, trace, std::move(handler));
  }

  librados::IoCtx& ioctx;
};

/// Interface for async_reads() that can prepare rados read operations.
class RadosReader : public Reader {
 public:
  RadosReader(const DoutPrefixProvider& dpp, executor_type ex,
              librados::IoCtx& ioctx)
    : Reader(dpp, std::move(ex)), ioctx(ioctx)
  {}

  /// Prepare a rados read operation for the given shard.
  virtual void prepare_read(int shard, librados::ObjectReadOperation& op) = 0;

 private:
  // implement read() in terms of prepare_read() and librados::async_operate()
  void read(int shard, const std::string& object,
            detail::ReadHandler&& handler) final {
    librados::ObjectReadOperation op;
    prepare_read(shard, op);

    constexpr int flags = 0;
    constexpr jspan_context* trace = nullptr;
    librados::async_operate(get_executor(), ioctx, object, std::move(op),
                            flags, trace, std::move(handler));
  }

  librados::IoCtx& ioctx;
};

} // namespace rgwrados::shard_io
