// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM
 *
 * See file COPYING for license information.
 *
 */

#pragma once

/// \file neodrados/cls/log.h
///
/// \brief NeoRADOS interface to OMAP based log class
///
/// The `log` object class stores a time-indexed series of entries in
/// the OMAP of a given object.

#include <span>
#include <string>
#include <tuple>
#include <vector>

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

#include "common/ceph_time.h"

#include "cls/log/cls_log_ops.h"
#include "cls/log/cls_log_types.h"

#include "common.h"

namespace neorados::cls::log {
using ::cls::log::entry;
using ::cls::log::header;
static constexpr auto max_list_entries = 1000u;

/// \brief Push entries to the log
///
/// Append a call to a write operation that adds a set of entries to the log
///
/// \param op Write operation to modify
/// \param entries Entries to push
void add(WriteOp& op, std::vector<entry> entries);

/// \brief Push an entry to the log
///
/// Append a call to a write operation that adds an entry to the log
///
/// \param op Write operation to modify
/// \param entry Entry to push
void add(WriteOp& op, const entry& entry);

/// \brief List log entries
///
/// Append a call to a write operation that adds an entry to the log
///
/// \param op Write operation to modify
/// \param timestamp Timestamp of the log entry
/// \param section Annotation string included in the entry
/// \param name Log entry name
/// \param bl Data held in the log entry
void add(WriteOp& op, ceph::real_time timestamp, std::string section,
	 std::string name, buffer::list&& bl);

/// \brief List log entries
///
/// Append a call to a read operation that lists log entries
///
/// \param op Write operation to modify
/// \param from Start of range
/// \param to End of range
/// \param in_marker Point to resume truncated listing
/// \param entries Span giving location to store entries
/// \param result Span giving entries actually stored
/// \param marker Place to store marker to resume truncated listing
/// \param truncated Place to store truncation status (true means
///                  there's more to list)
void list(ReadOp& op, ceph::real_time from, ceph::real_time to,
	  std::optional<std::string> in_marker, std::span<entry> entries,
	  std::span<entry>* result,
	  std::optional<std::string>* const out_marker);

/// \brief List log entries
///
/// Execute an asynchronous operation that lists log entries
///
/// \param r RADOS handle
/// \param o Object associated with log
/// \param ioc Object locator context
/// \param from Start of range
/// \param to End of range
/// \param in_marker Point to resume truncated listing
///
/// \return (entries, marker) in a way appropriate to the
/// completion token. See Boost.Asio documentation.
template<boost::asio::completion_token_for<
	   void(boost::system::error_code, std::span<entry>,
                std::optional<std::string>)> CompletionToken>
auto list(RADOS& r, Object o, IOContext ioc, ceph::real_time from,
	  ceph::real_time to, std::optional<std::string> in_marker,
	  std::span<entry> entries, CompletionToken&& token)
{
  using namespace std::literals;
  ::cls::log::ops::list_op req;
  req.from_time = from;
  req.to_time = to;
  req.marker = std::move(in_marker).value_or("");
  req.max_entries = entries.size();
  return exec<::cls::log::ops::list_ret>(
    r, std::move(o), std::move(ioc),
    "log"s, "list"s, req,
    [entries](const ::cls::log::ops::list_ret& ret) {
      auto res = entries.first(ret.entries.size());
      std::move(ret.entries.begin(), ret.entries.end(),
		res.begin());
      std::optional<std::string> marker;
      if (ret.truncated) {
	marker = std::move(ret.marker);
      }
      return std::make_tuple(res, std::move(marker));
    }, std::forward<CompletionToken>(token));
}

/// \brief Get log header
///
/// Append a call to a read operation that returns the log header
///
/// \param op Write operation to modify
/// \param header Place to store the log header
void info(ReadOp& op, header* const header);

/// \brief Get log header
///
/// Execute an asynchronous operation that returns the log header
///
/// \param r RADOS handle
/// \param o Object associated with log
/// \param ioc Object locator context
///
/// \return The log header in a way appropriate to the completion
/// token. See Boost.Asio documentation.
template<typename CompletionToken>
auto info(RADOS& r, Object o, IOContext ioc, CompletionToken&& token)
{
  using namespace std::literals;
  return exec<::cls::log::ops::list_ret>(
    r, std::move(o), std::move(ioc),
    "log"s, "info"s,
    [](const ::cls::log::ops::info_ret& ret) {
      return ret.header;
    }, std::forward<CompletionToken>(token));
}

// Since trim uses the string markers and ignores the time if the
// string markers are present, there's no benefit to having a function
// that takes both.

/// \brief Trim entries from the log
///
/// Append a call to a write operation that trims a range of entries
/// from the log.
///
/// \param op Write operation to modify
/// \param from_time Start of range, based on the timestamp supplied to add
/// \param to_time End of range, based on the timestamp supplied to add
///
/// \warning This operation may succeed even if not all entries have been trimmed.
/// to ensure completion, call repeatedly until the operation returns
/// boost::system::errc::no_message_available
void trim(WriteOp& op, ceph::real_time from_time, ceph::real_time to_time);

/// \brief Beginning marker for trim
///
/// A before-the-beginning marker for log entries, comparing less than
/// any possible entry.
inline constexpr std::string begin_marker{""};

/// \brief End marker for trim
///
/// An after-the-end marker for log entries, comparing greater than
/// any possible entry.
inline constexpr std::string end_marker{"9"};

/// \brief Trim entries from the log
///
/// Append a call to a write operation that trims a range of entries
/// from the log.
///
/// \param op Write operation to modify
/// \param from_marker Start of range, based on markers from list
/// \param to_marker End of range, based on markers from list
///
/// \note Use \ref begin_marker to trim everything up to a given point.
/// Use \ref end_marker to trim everything after a given point. Use them
/// both together to trim all entries.
///
/// \warning This operation may succeed even if not all entries have been trimmed.
/// to ensure completion, call repeatedly until the operation returns
/// boost::system::errc::no_message_available
void trim(WriteOp& op, std::string from_marker, std::string to_marker);

/// \brief Trim entries from the log
///
/// Execute an asynchronous operation that trims a range of entries
/// from the log.
///
/// \param op Write operation to modify
/// \param from_marker Start of range, based on markers from list
/// \param to_marker End of range, based on markers from list
///
/// \note Use \ref begin_marker to trim everything up to a given point.
/// Use \ref end_marker to trim everything after a given point. Use them
/// both together to trim all entries.
///
/// \return As appropriate to the completion token. See Boost.Asio
/// documentation.
template<typename CompletionToken>
auto trim(RADOS& r, Object oid, IOContext ioc, std::string from_marker,
	  std::string to_marker, CompletionToken&& token)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmismatched-new-delete"
  namespace asio = boost::asio;
  using boost::system::error_code;
  using boost::system::system_error;
  using ceph::real_time;
  using boost::system::errc::no_message_available;

  return asio::async_initiate<CompletionToken,
			      void(error_code)>
    (asio::experimental::co_composed<void(error_code)>
     ([](auto state, RADOS& r, Object oid, IOContext ioc,
	 std::string from_marker, std::string to_marker) -> void {
       try {
	 for (;;) {
	   WriteOp op;
	   trim(op, from_marker, to_marker);
	   co_await r.execute(oid, ioc, std::move(op), asio::deferred);
	 }
       } catch (const system_error& e) {
	 if (e.code() != no_message_available) {
	   co_return e.code();
	 }
       }
       co_return error_code{};
     }, r.get_executor()),
     token, std::ref(r), std::move(oid), std::move(ioc),
     std::move(from_marker), std::move(to_marker));
#pragma GCC diagnostic pop
}

/// \brief Trim entries from the log
///
/// Execute an asynchronous operation that trims a range of entries
/// from the log.
///
/// \param op Write operation to modify
/// \param from_time Start of range, based on the timestamp supplied to add
/// \param to_time End of range, based on the timestamp supplied to add
///
/// \return As appropriate to the completion token. See Boost.Asio
/// documentation.
template<typename CompletionToken>
auto trim(RADOS& r, Object oid, IOContext ioc, ceph::real_time from_time,
	  ceph::real_time to_time, CompletionToken&& token)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmismatched-new-delete"
  namespace asio = boost::asio;
  using boost::system::error_code;
  using boost::system::system_error;
  using ceph::real_time;
  using boost::system::errc::no_message_available;

  return asio::async_initiate<CompletionToken,
			      void(error_code)>
    (asio::experimental::co_composed<void(error_code)>
     ([](auto state, RADOS& r, Object oid, IOContext ioc,
	 real_time from_time, real_time to_time) -> void {
       try {
	 for (;;) {
	   WriteOp op;
	   trim(op, from_time, to_time);
	   co_await r.execute(oid, ioc, std::move(op), asio::deferred);
	 }
       } catch (const system_error& e) {
	 if (e.code() != no_message_available) {
	   co_return e.code();
	 }
       }
       co_return error_code{};
     }, r.get_executor()),
     token, std::ref(r), std::move(oid), std::move(ioc), from_time, to_time);
#pragma GCC diagnostic pop
}
} // namespace neorados::cls::log
