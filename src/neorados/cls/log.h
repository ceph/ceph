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
/// \param entries Entries to push
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline auto add(std::vector<entry> entries)
{
  buffer::list in;
  ::cls::log::ops::add_op call;
  call.entries = std::move(entries);
  encode(call, in);
  return ClsWriteOp{[in = std::move(in)](WriteOp& op) {
    op.exec("log", "add", in);
  }};
}

/// \brief Push an entry to the log
///
/// Append a call to a write operation that adds an entry to the log
///
/// \param entry Entry to push
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline auto add(entry e)
{
  bufferlist in;
  ::cls::log::ops::add_op call;
  call.entries.push_back(std::move(e));
  encode(call, in);
  return ClsWriteOp{[in = std::move(in)](WriteOp& op) {
    op.exec("log", "add", in);
  }};
}

/// \brief Push an entry to the log
///
/// Append a call to a write operation that adds an entry to the log
///
/// \param timestamp Timestamp of the log entry
/// \param section Annotation string included in the entry
/// \param name Log entry name
/// \param bl Data held in the log entry
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline auto add(ceph::real_time timestamp, std::string section,
			      std::string name, buffer::list&& bl)
{
  bufferlist in;
  ::cls::log::ops::add_op call;
  call.entries.emplace_back(timestamp, std::move(section),
			    std::move(name), std::move(bl));
  encode(call, in);
  return ClsWriteOp{[in = std::move(in)](WriteOp& op) {
    op.exec("log", "add", in);
  }};
}

/// \brief List log entries
///
/// Append a call to a read operation that lists log entries
///
/// \param from Start of range
/// \param to End of range
/// \param in_marker Point to resume truncated listing
/// \param entries Span giving location to store entries
/// \param result Span giving entries actually stored
/// \param marker Place to store marker to resume truncated listing
/// \param truncated Place to store truncation status (true means
///                  there's more to list)
///
/// \return The ClsReadOp to be passed to WriteOp::exec
[[nodiscard]] inline auto list(ceph::real_time from, ceph::real_time to,
			       std::optional<std::string> in_marker,
			       std::span<entry> entries, std::span<entry>* result,
			       std::optional<std::string>* const out_marker)
{
  using boost::system::error_code;
  bufferlist in;
  ::cls::log::ops::list_op call;
  call.from_time = from;
  call.to_time = to;
  call.marker = std::move(in_marker).value_or("");
  call.max_entries = entries.size();

  encode(call, in);
  return ClsReadOp{[entries, result, out_marker,
		    in = std::move(in)](ReadOp& op) {
    op.exec("log", "list", in,
	    [entries, result, out_marker](error_code ec, const buffer::list& bl) {
	      ::cls::log::ops::list_ret ret;
	      if (!ec) {
		auto iter = bl.cbegin();
		decode(ret, iter);
		if (result) {
		  *result = entries.first(ret.entries.size());
		  std::move(ret.entries.begin(), ret.entries.end(),
			    entries.begin());
		}
		if (out_marker) {
		  *out_marker = (ret.truncated ?
				 std::move(ret.marker) :
				 std::optional<std::string>{});
		}
	      }
	    });
  }};
}

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
/// \param header Place to store the log header
///
/// \return The ClsReadOp to be passed to WriteOp::exec
[[nodiscard]] inline auto info(header* const header)
{
  using boost::system::error_code;
  buffer::list in;
  ::cls::log::ops::info_op call;

  encode(call, in);

  return ClsReadOp{[header, in = std::move(in)](ReadOp& op) {
    op.exec("log", "info", in,
	    [header](error_code ec,
		     const buffer::list& bl) {
	      ::cls::log::ops::info_ret ret;
	      if (!ec) {
		auto iter = bl.cbegin();
		decode(ret, iter);
		if (header)
		*header = std::move(ret.header);
	      }
	    });
  }};
}

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
  return exec<::cls::log::ops::info_ret>(
    r, std::move(o), std::move(ioc),
    "log"s, "info"s, ::cls::log::ops::info_op{},
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
/// \param from_time Start of range, based on the timestamp supplied to add
/// \param to_time End of range, based on the timestamp supplied to add
///
/// \warning This operation may succeed even if not all entries have been trimmed.
/// to ensure completion, call repeatedly until the operation returns
/// boost::system::errc::no_message_available
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline auto trim(ceph::real_time from_time,
			       ceph::real_time to_time)
{
  bufferlist in;
  ::cls::log::ops::trim_op call;
  call.from_time = from_time;
  call.to_time = to_time;
  encode(call, in);
  return ClsWriteOp{[in = std::move(in)](WriteOp& op) {
    op.exec("log", "trim", in);
  }};
}

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
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline auto trim(std::string from_marker, std::string to_marker)
{
  bufferlist in;
  ::cls::log::ops::trim_op call;
  call.from_marker = std::move(from_marker);
  call.to_marker = std::move(to_marker);
  encode(call, in);
  return ClsWriteOp{[in = std::move(in)](WriteOp& op) {
    op.exec("log", "trim", in);
  }};
}

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
	   co_await r.execute(oid, ioc,
			      WriteOp{}.exec(trim(from_marker, to_marker)),
			      asio::deferred);
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
	   
	   co_await r.execute(oid, ioc, WriteOp{}.exec(trim(from_time, to_time)),
			      asio::deferred);
	 }
       } catch (const system_error& e) {
	 if (e.code() != no_message_available) {
	   co_return e.code();
	 }
       }
       co_return error_code{};
     }, r.get_executor()),
     token, std::ref(r), std::move(oid), std::move(ioc), from_time, to_time);
}
} // namespace neorados::cls::log
