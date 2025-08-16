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

/// \file neorados/cls/fifo.h
///
/// \brief NeoRADOS interface to FIFO class
///
/// The `fifo` object class stores a queue structured log across
/// multiple OSDs. Each FIFO comprises a head object with metadata
/// such as the current head and tail objects, as well as a set of
/// data objects, containing a bunch of entries. New entries may be
/// pushed at the head and trimmed at the tail. Entries may be
/// retrieved for processing with the `list` operation. Each entry
/// comes with a marker that may be used to trim up to (inclusively)
/// that object once processing is complete. The head object is the
/// notional 'name' of the FIFO, provided at creation or opening time.

#include "fifo/detail/fifo.h"

#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>

#include <fmt/format.h>

#include <boost/asio/async_result.hpp>
#include <boost/asio/consign.hpp>
#include <boost/asio/co_composed.hpp>

#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>

#include "include/buffer.h"
#include "include/neorados/RADOS.hpp"

#include "common/dout.h"
#include "common/dout_fmt.h"

#include "cls/fifo/cls_fifo_types.h"

namespace neorados::cls::fifo {
/// This is the FIFO client class. It handles the logic of keeping
/// state synchronized with that on the server, journal processing,
/// and multipart push.
class FIFO {
  friend class FIFOtest;

public:
  using executor_type = neorados::RADOS::executor_type;

private:
  executor_type executor;

public:
  auto get_executor() const {
    return executor;
  }

  /// The default maximum size of every part object (that is, every
  /// object holding entries)
  static constexpr std::uint64_t default_max_part_size = 4 * 1024 * 1024;

  /// The default maximum entry size
  static constexpr std::uint64_t default_max_entry_size = 32 * 1024;

private:

  std::shared_ptr<detail::FIFOImpl> impl;

  FIFO(std::shared_ptr<detail::FIFOImpl>&& impl)
    : executor(impl->get_executor()), impl(std::move(impl)) {}

  /// Make sure each operation has a reference to the implementation
  ///
  /// I don't think we need a work-guard since `co_composed` let us
  /// pass executors to keep alive
  ///
  /// \param token The token to annotate
  template<typename CompletionToken>
  auto consign(CompletionToken&& token) {
    return boost::asio::consign(
      std::forward<CompletionToken>(token),
      // Even though RADOS holds the executor across operations, we
      // still need it live between operations.
      std::make_pair(impl, get_executor()));
  }

public:

  /// \brief Open an existing FIFO
  ///
  /// \param dpp Prefix provider for debug logging
  /// \param rados RADOS handle
  /// \param obj Head object for FIFO
  /// \param ioc Locator
  /// \param token Boost.Asio CompletionToken
  /// \param objv Operation will fail if FIFO is not at this version
  /// \param probe If true, the caller is probing the existence of the
  ///              FIFO. Don't print errors if we can't find it.
  ///
  /// \return A `unique_ptr` to the open FIFO in a way appropriate to
  /// the completion token.
  template<boost::asio::completion_token_for<
	     void(boost::system::error_code, std::unique_ptr<FIFO>)>
	   CompletionToken>
  static auto open(const DoutPrefixProvider* dpp,
		   neorados::RADOS rados,
		   neorados::Object obj,
		   neorados::IOContext ioc,
		   CompletionToken&& token,
		   std::optional<rados::cls::fifo::objv> objv = std::nullopt,
		   bool probe = false) {
    namespace asio = boost::asio;
    namespace sys = boost::system;
    return asio::async_initiate<CompletionToken,
				void(sys::error_code, std::unique_ptr<FIFO>)>
      (asio::co_composed<void(sys::error_code, std::unique_ptr<FIFO>)>
       ([](auto state, const DoutPrefixProvider* dpp, neorados::RADOS rados,
	   neorados::Object obj, neorados::IOContext ioc,
	   std::optional<rados::cls::fifo::objv> objv, bool probe) -> void {
	 try {
	   state.throw_if_cancelled(true);
	   state.reset_cancellation_state(asio::enable_terminal_cancellation());
	   auto e = rados.get_executor();
	   auto impl = std::make_shared<detail::FIFOImpl>(std::move(rados),
							  std::move(obj),
							  std::move(ioc));

	   co_await impl->do_open(dpp, objv, probe,
				  boost::asio::consign(asio::deferred, impl));
	   co_return {sys::error_code{},
	              std::unique_ptr<FIFO>{new FIFO(std::move(impl))}};
	 } catch (const sys::system_error &e) {
           co_return {e.code(), std::unique_ptr<FIFO>{}};
         }
       }, rados.get_executor()),
       token, dpp, std::move(rados), std::move(obj), std::move(ioc),
       std::move(objv), probe);
  }

  /// \brief Create and open a FIFO
  ///
  /// \param dpp Prefix provider for debug logging
  /// \param rados RADOS handle
  /// \param obj Head object for FIFO
  /// \param ioc Locator
  /// \param token Boost.Asio CompletionToken
  /// \param objv Operation will fail if FIFO exists and is not at this version
  /// \param oid_prefix Prefix for all objects
  /// \param exclusive Fail if the FIFO already exists
  /// \param max_part_size Maximum allowed size of parts
  /// \param max_entry_size Maximum allowed size of entries
  ///
  /// \return A `unique_ptr` to the open FIFO in a way appropriate to
  /// the completion token.
  template<boost::asio::completion_token_for<
	     void(boost::system::error_code, std::unique_ptr<FIFO>)>
	   CompletionToken>
  static auto create(const DoutPrefixProvider* dpp,
		     neorados::RADOS rados,
		     neorados::Object obj,
		     neorados::IOContext ioc,
		     CompletionToken&& token,
		     std::optional<rados::cls::fifo::objv> objv = std::nullopt,
		     std::optional<std::string> oid_prefix = std::nullopt,
		     bool exclusive = false,
		     std::uint64_t max_part_size = default_max_part_size,
		     std::uint64_t max_entry_size = default_max_entry_size) {
    namespace asio = boost::asio;
    namespace sys = boost::system;
    return asio::async_initiate<CompletionToken,
				void(sys::error_code, std::unique_ptr<FIFO>)>
      (asio::co_composed<void(sys::error_code,
                                            std::unique_ptr<FIFO>)>
       ([](auto state, const DoutPrefixProvider* dpp, neorados::RADOS rados,
	   neorados::Object obj, neorados::IOContext ioc,
	   std::optional<rados::cls::fifo::objv> objv,
	   std::optional<std::string> oid_prefix, bool exclusive,
	   std::uint64_t max_part_size, std::uint64_t max_entry_size) -> void {
	 try {
	   state.throw_if_cancelled(true);
	   state.reset_cancellation_state(asio::enable_terminal_cancellation());

	   auto impl = std::make_shared<detail::FIFOImpl>(std::move(rados),
							  std::move(obj),
							  std::move(ioc));

	   co_await impl->do_create(dpp, objv, std::move(oid_prefix), exclusive,
				    max_part_size, max_entry_size,
				    asio::deferred);
	   co_return {sys::error_code{},
	              std::unique_ptr<FIFO>{new FIFO(std::move(impl))}};

	 } catch (const sys::system_error& e) {
	   ldpp_dout_fmt(dpp, -1, "FIFO::create:{}: create failed: {}",
			 __LINE__, e.what());
	   co_return {e.code(), std::unique_ptr<FIFO>{}};
	 }
       }, rados.get_executor()),
       token, dpp, std::move(rados), std::move(obj), std::move(ioc),
       std::move(objv), std::move(oid_prefix), exclusive, max_part_size,
       max_entry_size);
  }

  /// \brief Push entries to the FIFO
  ///
  /// \param dpp Prefix provider for debug logging
  /// \param entries Vector of entries
  /// \param token Boost.Asio CompletionToken
  ///
  /// \return Nothing, but may error in a way appropriate to the
  /// completion token.
  template<boost::asio::completion_token_for<
    void(boost::system::error_code)> CompletionToken>
  auto push(const DoutPrefixProvider* dpp,
	    std::deque<buffer::list> entries,
	    CompletionToken&& token) {
    return impl->push(dpp, std::move(entries),
		      consign(std::forward<CompletionToken>(token)));
  }

  /// \brief Push entries to the FIFO
  ///
  /// \param dpp Prefix provider for debug logging
  /// \param entries Entries to push
  /// \param token Boost.Asio CompletionToken
  ///
  /// \return Nothing, but may error in a way appropriate to the
  /// completion token.
  template<boost::asio::completion_token_for<
	     void(boost::system::error_code)> CompletionToken>
  auto push(const DoutPrefixProvider* dpp,
	    std::span<ceph::buffer::list> entries,
	    CompletionToken&& token) {
    namespace buffer = ceph::buffer;
    std::deque<buffer::list> deque{std::make_move_iterator(entries.begin()),
				   std::make_move_iterator(entries.end())};
    return push(dpp, std::move(deque), std::forward<CompletionToken>(token));
  }

  /// \brief Push an entry to the FIFO
  ///
  /// \param dpp Prefix provider for debug logging
  /// \param entry Entry to push
  /// \param token Boost.Asio CompletionToken
  ///
  /// \return Nothing, but may error in a way appropriate to the
  /// completion token.
  template<boost::asio::completion_token_for<
	     void(boost::system::error_code)> CompletionToken>
  auto push(const DoutPrefixProvider* dpp,
	    ceph::buffer::list entry,
	    CompletionToken&& token) {
    namespace buffer = ceph::buffer;
    std::deque<buffer::list> entries;
    entries.push_back(std::move(entry));
    return push(dpp, std::move(entries), std::forward<CompletionToken>(token));
  }

  /// \brief List entries in the FIFO
  ///
  /// \param dpp Prefix provider for debug logging
  /// \param markstr Marker to resume listing
  /// \param entries Space for entries
  /// \param token Boost.Asio CompletionToken
  ///
  /// \return (span<entry>, marker) where the span is long enough to hold
  ///         returned entries, and marker is non-null if the listing was
  ///         incomplete, in a way appropriate to the completion token.
  template<boost::asio::completion_token_for<
    void(boost::system::error_code, std::span<entry>,
	 std::string)> CompletionToken>
  auto list(const DoutPrefixProvider* dpp,
	    std::string markstr, std::span<entry> entries,
	    CompletionToken&& token) {
    return impl->list(dpp, std::move(markstr), entries,
		      consign(std::forward<CompletionToken>(token)));
  }

  /// \brief Trim entries from the FIFO
  ///
  /// \param dpp Prefix provider for debug logging
  /// \param marker Marker to which to trim
  /// \param exclusive If true, exclude the marked element from trim,
  ///                  if false, trim it.
  /// \param token Boost.Asio CompletionToken
  ///
  /// \return Nothing, but may error in a way appropriate to the
  /// completion token.
  template<boost::asio::completion_token_for<
    void(boost::system::error_code)> CompletionToken>
  auto trim(const DoutPrefixProvider* dpp,
	    std::string marker, bool exclusive,
	    CompletionToken&& token) {
    return impl->trim(dpp, std::move(marker), exclusive,
		      consign(std::forward<CompletionToken>(token)));
  }

  /// \brief Get information on the last entry
  ///
  /// \param dpp Prefix provider for debug logging
  /// \param token Boost.Asio CompletionToken
  ///
  /// \return {marker, time} for the latest entry in a way appropriate
  /// to the completion token.
  template<boost::asio::completion_token_for<
    void(boost::system::error_code, std::string,
	 ceph::real_time)> CompletionToken>
  auto last_entry_info(const DoutPrefixProvider* dpp,
		       CompletionToken&& token) {
    return impl->last_entry_info(dpp,
				 consign(std::forward<CompletionToken>(token)));
  }

  static constexpr auto max_list_entries =
    rados::cls::fifo::op::MAX_LIST_ENTRIES;

  /// Return a marker comparing less than any other marker.
  static auto min_marker() {
    using detail::FIFOImpl;
    return FIFOImpl::marker{
      std::numeric_limits<decltype(FIFOImpl::marker::num)>::max(),
      std::numeric_limits<decltype(FIFOImpl::marker::ofs)>::max()}
      .to_string();
  }

  /// Return a marker comparing greater than any other marker.
  static auto max_marker() {
    using detail::FIFOImpl;
    return FIFOImpl::marker{
      std::numeric_limits<decltype(FIFOImpl::marker::num)>::max(),
      std::numeric_limits<decltype(FIFOImpl::marker::ofs)>::max()}
      .to_string();
  }

  /// \brief Retrieve FIFO metadata
  ///
  /// \param rados RADOS handle
  /// \param obj Head object for FIFO
  /// \param ioc Locator
  /// \param token Boost.Asio CompletionToken
  /// \param objv Operation will fail if FIFO is not at this version
  ///
  /// \return The metadata info, part header size, and entry overhead
  /// in a way appropriate to the completion token.
  template<boost::asio::completion_token_for<
    void(boost::system::error_code, rados::cls::fifo::info,
	 uint32_t, uint32_t)> CompletionToken>
  static auto get_meta(neorados::RADOS rados, Object obj, IOContext ioc,
		       std::optional<rados::cls::fifo::objv> objv,
		       CompletionToken&& token) {

    return detail::FIFOImpl::get_meta(rados, std::move(obj), std::move(ioc),
				      std::move(objv),
				      std::forward<CompletionToken>(token));
  }
};
} // namespace neorados::cls::fifo {
