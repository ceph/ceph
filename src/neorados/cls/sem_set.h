// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 IBM
 *
 * See file COPYING for license information.
 *
 */

#pragma once

/// \file neodrados/cls/sem_set.h
///
/// \brief NeoRADOS interface to semaphore set class
///
/// The `sem_set` object class stores a set of strings with associated
/// semaphores in OMAP.

#include <cstdint>
#include <initializer_list>
#include <string>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/asio/experimental/co_composed.hpp>

#include <boost/system/error_code.hpp>
#include <boost/system/errc.hpp>

#include "include/buffer.h"

#include "include/neorados/RADOS.hpp"

#include "common/ceph_context.h"

#include "cls/sem_set/ops.h"

#include "common.h"

namespace neorados::cls::sem_set {

/// \brief The maximum number of keys per op
///
/// Get the maximum number of keys that can be sent in a single
/// increment or decrement operation
///
/// \param entries Entries to push
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline auto max_entries(RADOS& rados) {
  using namespace std::literals;
  return rados.cct()->_conf.get_val<std::uint64_t>(
    "osd_max_omap_entries_per_request"sv);
}

/// \brief Increment semaphore
///
/// Append a call to a write operation that increments the semaphore
/// on a key.
///
/// \param key Key to increment
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline auto increment(std::string key)
{
  namespace ss = ::cls::sem_set;
  buffer::list in;
  ss::increment call{std::move(key)};
  encode(call, in);
  return ClsWriteOp{[in = std::move(in)](WriteOp& op) {
    op.exec(ss::CLASS, ss::INCREMENT, in);
  }};
}

/// \brief Increment semaphores
///
/// Append a call to a write operation that increments the semaphores
/// on a set of keys.
///
/// \param keys Keys to increment
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline auto increment(std::initializer_list<std::string> keys)
{
  namespace ss = ::cls::sem_set;
  buffer::list in;
  ss::increment call{keys};
  encode(call, in);
  return ClsWriteOp{[in = std::move(in)](WriteOp& op) {
    op.exec(ss::CLASS, ss::INCREMENT, in);
  }};
}

/// \brief Increment semaphores
///
/// Append a call to a write operation that increments the semaphores
/// on a set of keys.
///
/// \param keys Keys to increment
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
template<std::input_iterator I>
[[nodiscard]] inline auto increment(I begin, I end)
  requires std::is_convertible_v<typename I::value_type, std::string>
{
  namespace ss = ::cls::sem_set;
  buffer::list in;
  ss::increment call{begin, end};
  encode(call, in);
  return ClsWriteOp{[in = std::move(in)](WriteOp& op) {
    op.exec(ss::CLASS, ss::INCREMENT, in);
  }};
}

/// \brief Decrement semaphore
///
/// Append a call to a write operation that decrements the semaphore
/// on a key.
///
/// \param key Key to decrement
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline auto decrement(std::string key)
{
  namespace ss = ::cls::sem_set;
  buffer::list in;
  ss::decrement call{std::move(key)};
  encode(call, in);
  return ClsWriteOp{[in = std::move(in)](WriteOp& op) {
    op.exec(ss::CLASS, ss::DECREMENT, in);
  }};
}

/// \brief Decrement semaphores
///
/// Append a call to a write operation that decrements the semaphores
/// on a set of keys.
///
/// \param keys Keys to decrement
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline auto decrement(std::initializer_list<std::string> keys)
{
  namespace ss = ::cls::sem_set;
  buffer::list in;
  ss::decrement call{keys};
  encode(call, in);
  return ClsWriteOp{[in = std::move(in)](WriteOp& op) {
    op.exec(ss::CLASS, ss::DECREMENT, in);
  }};
}

/// \brief decrement semaphores
///
/// Append a call to a write operation that decrements the semaphores
/// on a set of keys.
///
/// \param keys Keys to decrement
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
template<std::input_iterator I>
[[nodiscard]] inline auto decrement(I begin, I end)
  requires std::is_convertible_v<typename I::value_type, std::string>
{
  namespace ss = ::cls::sem_set;
  buffer::list in;
  ss::decrement call{begin, end};
  encode(call, in);
  return ClsWriteOp{[in = std::move(in)](WriteOp& op) {
    op.exec(ss::CLASS, ss::DECREMENT, in);
  }};
}

/// \brief List keys and semaphores
///
/// Append a call to a read operation that lists keys and semaphores
///
/// \note This *appends* to the `entries`, it does not *clear* it.
///
/// \param count Number of entries to fetch
/// \param cursor Where to start
/// \param entries Unordered list to which entries are appended
/// \param new_cursor Where to start for the next iteration, empty if completed
///
/// \return The ClsReadOp to be passed to WriteOp::exec
[[nodiscard]] inline auto list(
  std::uint64_t count,
  std::optional<std::string> cursor,
  std::unordered_map<std::string, std::uint64_t>* const entries,
  std::optional<std::string>* const new_cursor)
{
  namespace ss = ::cls::sem_set;
  using boost::system::error_code;
  buffer::list in;
  ::cls::sem_set::list_op call;
  call.count = count;
  call.cursor = std::move(cursor);

  encode(call, in);
  return ClsReadOp{[entries, new_cursor,
		    in = std::move(in)](ReadOp& op) {
    op.exec(ss::CLASS, ss::LIST, in,
	    [entries, new_cursor](error_code ec, const buffer::list& bl) {
	      ss::list_ret ret;
	      if (!ec) {
		auto iter = bl.cbegin();
		decode(ret, iter);
		if (entries) {
		  entries->reserve(entries->size() + ret.kvs.size());
		  entries->merge(std::move(ret.kvs));
		}
		if (new_cursor) {
		  *new_cursor = std::move(ret.cursor);
		}
	      }
	    });
  }};
}

/// \brief List keys and semaphores
///
/// Append a call to a read operation that lists keys and semaphores
///
/// \param count Number of entries to fetch
/// \param cursor Where to start
/// \param out Output iterator
/// \param new_cursor Where to start for the next iteration, empty if completed
///
/// \return The ClsReadOp to be passed to WriteOp::exec
template<std::output_iterator<std::pair<std::string, std::uint64_t>> I>
[[nodiscard]] inline auto list(std::uint64_t count,
			       std::optional<std::string> cursor,
			       I output,
			       std::optional<std::string>* const new_cursor)
{
  namespace ss = ::cls::sem_set;
  using boost::system::error_code;
  buffer::list in;
  ::cls::sem_set::list_op call;
  call.count = count;
  call.cursor = std::move(cursor);

  encode(call, in);
  return ClsReadOp{[output, new_cursor,
		    in = std::move(in)](ReadOp& op) {
    op.exec(ss::CLASS, ss::LIST, in,
	    [output, new_cursor](error_code ec, const buffer::list& bl) {
	      ss::list_ret ret;
	      if (!ec) {
		auto iter = bl.cbegin();
		decode(ret, iter);
		std::move(ret.kvs.begin(), ret.kvs.end(), output);
		if (new_cursor) {
		  *new_cursor = std::move(ret.cursor);
		}
	      }
	    });
  }};
}
} // namespace neorados::cls::sem_set
