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

#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>

#include "include/buffer.h"

#include "include/neorados/RADOS.hpp"

#include "cls/sem_set/ops.h"

namespace neorados::cls::sem_set {
using namespace std::literals;

/// \brief The maximum number of keys per op
///
using ::cls::sem_set::max_keys;

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
  namespace buffer = ::ceph::buffer;
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
[[nodiscard]] inline auto increment(boost::container::flat_set<std::string> keys)
{
  namespace ss = ::cls::sem_set;
  namespace buffer = ::ceph::buffer;
  buffer::list in;
  ss::increment call{std::move(keys)};
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
  requires std::is_convertible_v<typename std::iterator_traits<I>::value_type,
				 std::string>
{
  namespace ss = ::cls::sem_set;
  namespace buffer = ::ceph::buffer;
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
/// \param grace Don't decrement anything decremented more recently.
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline auto
decrement(std::string key, ceph::timespan grace = 0ns)
{
  namespace ss = ::cls::sem_set;
  namespace buffer = ::ceph::buffer;
  buffer::list in;
  ss::decrement call{std::move(key), grace};
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
/// \param grace Don't decrement anything decremented more recently.
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline auto
decrement(std::initializer_list<std::string> keys, ceph::timespan grace = 0ns)
{
  namespace ss = ::cls::sem_set;
  namespace buffer = ::ceph::buffer;
  buffer::list in;
  ss::decrement call{keys, grace};
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
/// \param grace Don't decrement anything decremented more recently.
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline auto
decrement(boost::container::flat_set<std::string> keys, ceph::timespan grace = 0ns)
{
  namespace ss = ::cls::sem_set;
  namespace buffer = ::ceph::buffer;
  buffer::list in;
  ss::decrement call{std::move(keys), grace};
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
/// \param grace Don't decrement anything decremented more recently.
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
template <std::input_iterator I>
[[nodiscard]] inline auto
decrement(I begin, I end, ceph::timespan grace = 0ns)
  requires std::is_convertible_v<typename I::value_type, std::string>
{
  namespace ss = ::cls::sem_set;
  namespace buffer = ::ceph::buffer;
  buffer::list in;
  ss::decrement call{begin, end, grace};
  encode(call, in);
  return ClsWriteOp{[in = std::move(in)](WriteOp& op) {
    op.exec(ss::CLASS, ss::DECREMENT, in);
  }};
}

/// \brief Reset semaphore
///
/// Append a call to a write operation that reset the semaphore
/// on a key to a given value.
///
/// \param key Key to reset
/// \param val Value to set it to
///
/// \note This function exists to be called by radosgw-admin when the
/// administrator wants to reset a semaphore. It should not be called
/// in normal RGW operation and can lead to unreplicated objects.
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline auto reset(std::string key, std::uint64_t val)
{
  namespace ss = ::cls::sem_set;
  buffer::list in;
  ss::reset call{std::move(key), val};
  encode(call, in);
  return ClsWriteOp{[in = std::move(in)](WriteOp& op) {
    op.exec(ss::CLASS, ss::RESET, in);
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
  std::uint64_t count, std::string cursor,
  boost::container::flat_map<std::string, std::uint64_t>* const entries,
  std::string* const new_cursor)
{
  namespace ss = ::cls::sem_set;
  namespace sys = ::boost::system;
  namespace buffer = ::ceph::buffer;
  buffer::list in;
  ::cls::sem_set::list_op call;
  call.count = count;
  call.cursor = std::move(cursor);

  encode(call, in);
  return ClsReadOp{[entries, new_cursor,
		    in = std::move(in)](ReadOp& op) {
    op.exec(ss::CLASS, ss::LIST, in,
	    [entries, new_cursor](sys::error_code ec, const buffer::list& bl) {
	      ss::list_ret ret;
	      if (!ec) {
		auto iter = bl.cbegin();
		try {
		  decode(ret, iter);
		} catch (const sys::system_error& e) {
		  if (e.code() == buffer::errc::end_of_buffer &&
		      bl.length() == 0) {
		    // It looks like if the object doesn't exist the
		    // CLS function isn't called and we don't get
		    // -ENOENT even though we get it for the op, we
		    // just get an empty buffer. This is crap.
		    if (new_cursor) {
		      new_cursor->clear();
		    }
		    return;
		  } else {
		    throw;
		  }
		}
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
			       std::string cursor, I output,
			       std::string* const new_cursor)
{
  namespace ss = ::cls::sem_set;
  using boost::system::error_code;
  namespace buffer = ::ceph::buffer;
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
