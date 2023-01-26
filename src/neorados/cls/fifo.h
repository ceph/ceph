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

#include <cerrno>
#include <coroutine>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/deferred.hpp>

#include <boost/asio/experimental/co_composed.hpp>

#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>

#include "include/buffer.h"
#include "include/neorados/RADOS.hpp"

#include "common/debug.h"
#include "common/strtol.h"

#include "neorados/cls/common.h"

#include "cls/fifo/cls_fifo_types.h"
#include "cls/fifo/cls_fifo_ops.h"

namespace neorados::cls::fifo {
/// Entries to be returned by the list operation
struct entry {
  /// Data stored in the entry
  ceph::buffer::list data;
  /// Marker (for trimming, continuing list operations)
  std::string marker;
  /// Time stored (set by the OSD)
  ceph::real_time mtime;
};

inline std::ostream& operator <<(std::ostream& m, const entry& e) {
  return m << "[data: " << e.data
	   << ", marker: " << e.marker
	   << ", mtime: " << e.mtime << "]";
}

/// This is the FIFO client class. It handles the logic of keeping
/// state synchronized with that on the server, journal processing,
/// and multipart push.
class FIFO {
  friend class FIFOtest;
  /// A marker is a part number and byte offset used to indicate an
  /// entry.
  struct marker {
    std::int64_t num = 0;
    std::uint64_t ofs = 0;

    /// Default constructor
    marker() = default;

    /// Construct from part and offset
    ///
    /// \param num Part number
    /// \param ofs Offset within the part
    marker(std::int64_t num, std::uint64_t ofs) : num(num), ofs(ofs) {}

    /// Return a string representation of the marker
    std::string to_string() {
      return fmt::format("{:0>20}:{:0>20}", num, ofs);
    }
  };

  /// Maximum number of retries if we run into a conflict. Limited to
  /// keep us from locking up if we never hit the end condition.
  static constexpr auto MAX_RACE_RETRIES = 10;

  /// RADOS handle
  neorados::RADOS& rados;
  /// Head object
  const neorados::Object obj;
  /// Locator
  const neorados::IOContext ioc;
  /// Total size of the part header.
  std::uint32_t part_header_size;
  /// The additional space required to store an entry, above the size
  /// of the entry itself.
  std::uint32_t part_entry_overhead;
  /// Local copy of FIFO data
  rados::cls::fifo::info info;

  /// Mutex protecting local data;
  std::mutex m;

  /// Constructor
  ///
  /// \param rados RADOS handle
  /// \param obj Head object
  /// \param ioc Locator
  /// \param part_header_size Total size of the part header.
  /// \param part_entry_overhead The additional space required to
  ///                            store an entry, above the size of the
  ///                            entry itself.
  FIFO(neorados::RADOS& rados,
       neorados::Object obj,
       neorados::IOContext ioc,
       std::uint32_t part_header_size,
       std::uint32_t part_entry_overhead,
       rados::cls::fifo::info info)
    : rados(rados), obj(std::move(obj)), ioc(std::move(ioc)),
      part_header_size(part_header_size),
      part_entry_overhead(part_entry_overhead),
      info(std::move(info)) {}

  /// \name Primitives
  ///
  /// Low-level coroutininized operations in the FIFO objclass.
  ///@{

public:
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
  static auto get_meta(neorados::RADOS& rados,
		       neorados::Object obj,
		       neorados::IOContext ioc,
		       std::optional<rados::cls::fifo::objv> objv,
		       CompletionToken&& token) {
    namespace fifo = rados::cls::fifo;
    neorados::ReadOp op;
    fifo::op::get_meta gm;
    gm.version = objv;
    return exec<fifo::op::get_meta_reply>(
      rados, std::move(obj), std::move(ioc),
      fifo::op::CLASS, fifo::op::GET_META, std::move(gm),
      [](fifo::op::get_meta_reply&& ret) {
	return std::make_tuple(std::move(ret.info),
			       ret.part_header_size,
			       ret.part_entry_overhead);
      }, std::forward<CompletionToken>(token));
  }

private:
  /// \brief Retrieve part info
  ///
  /// \param part_num Number of part to query
  /// \param token Boost.Asio CompletionToken
  ///
  /// \return The part info in a way appropriate to the completion token.
  template<boost::asio::completion_token_for<
	     void(boost::system::error_code, rados::cls::fifo::part_header)>
	   CompletionToken>
  auto get_part_info(std::int64_t part_num,
		     CompletionToken&& token) {
    namespace fifo = rados::cls::fifo;
    std::unique_lock l(m);
    Object part_oid = info.part_oid(part_num);;
    l.unlock();

    return exec<fifo::op::get_part_info_reply>(
      rados, std::move(std::move(part_oid)), ioc,
      fifo::op::CLASS, fifo::op::GET_PART_INFO,
      fifo::op::get_part_info{},
      [](fifo::op::get_part_info_reply&& ret) {
	return std::move(ret.header);
      }, std::forward<CompletionToken>(token));
  }


  /// \brief Create a new part object
  ///
  /// \param part_num Part number
  /// \param token Boost.Asio CompletionToken
  ///
  /// \return Nothing, but may error in a way appropriate to the
  /// completion token.
  template<boost::asio::completion_token_for<
    void(boost::system::error_code)> CompletionToken>
  auto create_part(std::int64_t part_num, CompletionToken&& token) {
    namespace fifo = rados::cls::fifo;
    namespace buffer = ceph::buffer;
    neorados::WriteOp op;
    op.create(false); /* We don't need exclusivity, part_init ensures we're
			 creating from the same journal entry. */
    std::unique_lock l(m);
    fifo::op::init_part ip;
    ip.params = info.params;
    buffer::list in;
    encode(ip, in);
    op.exec(fifo::op::CLASS, fifo::op::INIT_PART, std::move(in));
    auto oid = info.part_oid(part_num);
    l.unlock();
    return rados.execute(oid, ioc, std::move(op),
			 std::forward<CompletionToken>(token));
  }

  /// \brief Remove a part object
  ///
  /// \param part_num Part number
  /// \param token Boost.Asio CompletionToken
  ///
  /// \return Nothing, but may error in a way appropriate to the
  /// completion token.
  template<boost::asio::completion_token_for<
    void(boost::system::error_code)> CompletionToken>
  auto remove_part(std::int64_t part_num, CompletionToken&& token) {
    namespace fifo = rados::cls::fifo;
    namespace buffer = ceph::buffer;
    neorados::WriteOp op;
    op.remove();
    std::unique_lock l(m);
    auto oid = info.part_oid(part_num);
    l.unlock();
    return rados.execute(oid, ioc, std::move(op),
			 std::forward<CompletionToken>(token));
  }

  /// \brief Update objclass FIFO metadata
  ///
  /// \param objv Current metadata version (objclass will error on mismatch)
  /// \param update Changes to make to metadata
  /// \param token Boost.Asio CompletionToken
  ///
  /// \return Nothing, but may error in a way appropriate to the
  /// completion token.
  template<boost::asio::completion_token_for<
    void(boost::system::error_code)> CompletionToken>
  auto update_meta(const rados::cls::fifo::objv& objv,
		   const rados::cls::fifo::update& update,
		   CompletionToken&& token) {
    namespace fifo = rados::cls::fifo;
    namespace buffer = ceph::buffer;
    neorados::WriteOp op;
    fifo::op::update_meta um;

    um.version = objv;
    um.tail_part_num = update.tail_part_num();
    um.head_part_num = update.head_part_num();
    um.min_push_part_num = update.min_push_part_num();
    um.max_push_part_num = update.max_push_part_num();
    um.journal_entries_add = std::move(update).journal_entries_add();
    um.journal_entries_rm = std::move(update).journal_entries_rm();

    buffer::list in;
    encode(um, in);
    op.exec(fifo::op::CLASS, fifo::op::UPDATE_META, std::move(in));
    return rados.execute(obj, ioc, std::move(op),
			 std::forward<CompletionToken>(token));
  }

  /// \brief Create FIFO head object
  ///
  /// \param rados RADOS handle
  /// \param obj Head object for the FIFO
  /// \param ioc Locator
  /// \param objv Version (error if FIFO exists and this doesn't match)
  /// \param oid_prefix Prefix for all object names
  /// \param exclusive If true, error if object already exists
  /// \param max_part_size Max size of part objects
  /// \param max_entry_size Max size of entries
  /// \param token Boost.Asio CompletionToken
  ///
  /// \return Nothing, but may error in a way appropriate to the
  /// completion token.
  template<boost::asio::completion_token_for<
    void(boost::system::error_code)> CompletionToken>
  static auto create_meta(neorados::RADOS& rados,
			  neorados::Object obj,
			  neorados::IOContext ioc,
			  std::optional<rados::cls::fifo::objv> objv,
			  std::optional<std::string> oid_prefix,
			  bool exclusive,
			  std::uint64_t max_part_size,
			  std::uint64_t max_entry_size,
			  CompletionToken&& token) {
    namespace fifo = rados::cls::fifo;
    namespace buffer = ceph::buffer;
    neorados::WriteOp op;
    fifo::op::create_meta cm;

    cm.id = obj;
    cm.version = objv;
    cm.oid_prefix = oid_prefix;
    cm.max_part_size = max_part_size;
    cm.max_entry_size = max_entry_size;
    cm.exclusive = exclusive;

    buffer::list in;
    encode(cm, in);
    op.exec(fifo::op::CLASS, fifo::op::CREATE_META, in);
    return rados.execute(std::move(obj), std::move(ioc), std::move(op),
			 std::forward<CompletionToken>(token));
  }

  /// \brief Push some entries to a given part
  ///
  /// \param dpp Prefix for debug prints
  /// \param entries Entries to push
  /// \param token Boost.Asio CompletionToken
  ///
  /// \return Possibly errors in a way appropriate to the completion
  /// token.
  template<boost::asio::completion_token_for<
	     void(boost::system::error_code, int)> CompletionToken>
  auto push_entries(const DoutPrefixProvider* dpp,
		    std::deque<ceph::buffer::list> entries,
		    CompletionToken&& token) {
    namespace asio = boost::asio;
    namespace sys = boost::system;
    namespace fifo = rados::cls::fifo;
    namespace buffer = ceph::buffer;
    return asio::async_initiate<CompletionToken, void(sys::error_code, int)>
      (asio::experimental::co_composed<void(sys::error_code, int)>
       ([](auto state, const DoutPrefixProvider* dpp,
	   std::deque<buffer::list> entries, FIFO* f) -> void {
	 try {
	   state.throw_if_cancelled(true);
	   state.reset_cancellation_state(asio::enable_terminal_cancellation());

	   std::unique_lock l(f->m);
	   auto head_part_num = f->info.head_part_num;
	   auto oid = f->info.part_oid(head_part_num);
	   l.unlock();

	   WriteOp op;
	   op.assert_exists();

	   fifo::op::push_part pp;

	   pp.data_bufs = std::move(entries);
	   pp.total_len = 0;

           for (const auto &bl : pp.data_bufs)
             pp.total_len += bl.length();

           buffer::list in;
           encode(pp, in);
           int pushes;
           op.exec(fifo::op::CLASS, fifo::op::PUSH_PART, in,
                   [&pushes](sys::error_code, int r, const buffer::list &) {
                     pushes = r;
                   });
           op.returnvec();
           co_await f->rados.execute(std::move(oid), f->ioc, std::move(op),
                                     asio::deferred);
           co_return {sys::error_code{}, pushes};
	 } catch (const sys::system_error& e) {
	   ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			      << " push entries failed: " << e.what() << dendl;
	   co_return {e.code(), 0};
	 }
       }, rados.get_executor()),
       token, dpp, std::move(entries), this);
  }

  /// \brief List entries from a given part
  ///
  /// \param dpp Prefix provider for debug logging
  /// \param part_num Part number to list
  /// \param ofs Offset from which to list
  /// \param result Span giving space for results
  /// \param token Boost.Asio CompletionToken
  ///
  /// \return A subspan containing results, a bool indicating whether there
  ///         are more entries within the part, and a bool indicating whether
  ///         the part is full all in a way appropriate to the completion
  ///         token.
  template<boost::asio::completion_token_for<
	     void(boost::system::error_code, std::span<entry>, bool, bool)>
	   CompletionToken>
  auto list_part(const DoutPrefixProvider* dpp,
		 std::int64_t part_num, std::uint64_t ofs,
		 std::span<entry> result,
		 CompletionToken&& token) {
    namespace asio = boost::asio;
    namespace sys = boost::system;
    namespace fifo = rados::cls::fifo;
    namespace buffer = ceph::buffer;
    return asio::async_initiate<
      CompletionToken, void(sys::error_code, std::span<entry>, bool, bool)>
      (asio::experimental::co_composed<
       void(sys::error_code, std::span<entry>, bool, bool)>
       ([](auto state, const DoutPrefixProvider* dpp, std::int64_t part_num,
	   std::uint64_t ofs,std::span<entry> result,
	   FIFO* f) -> void {
	 try {
	   state.throw_if_cancelled(true);
	   state.reset_cancellation_state(asio::enable_terminal_cancellation());

	   std::unique_lock l(f->m);
	   auto oid = f->info.part_oid(part_num);
	   l.unlock();

	   ReadOp op;
	   fifo::op::list_part lp;

	   lp.ofs = ofs;
	   lp.max_entries = result.size();

	   buffer::list in;
	   encode(lp, in);
	   buffer::list bl;
	   op.exec(fifo::op::CLASS, fifo::op::LIST_PART, in, &bl, nullptr);
	   co_await f->rados.execute(oid, f->ioc, std::move(op), nullptr,
				     asio::deferred);
	   bool more, full_part;
	   {
	     ceph::buffer::list::const_iterator bi = bl.begin();
	     DECODE_START(1, bi);
	     std::string tag;
	     decode(tag, bi);
	     uint32_t len;
	     decode(len, bi);
	     if (len > result.size()) {
	       throw buffer::end_of_buffer{};
	     }
	     result = result.first(len);
	     for (auto i = 0u; i < len; ++i) {
	       fifo::part_list_entry entry;
	       decode(entry, bi);
	       result[i] = {.data = std::move(entry.data),
			    .marker = marker{part_num, entry.ofs}.to_string(),
			    .mtime = entry.mtime};
	     }
	     decode(more, bi);
	     decode(full_part, bi);
	     DECODE_FINISH(bi);
	   }
	   co_return {sys::error_code{}, std::move(result), more, full_part};
       } catch (const sys::system_error& e) {
	   if (e.code() != sys::errc::no_such_file_or_directory) {
	     ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
				<< " list entries failed: " << e.what() << dendl;
	   }
	 co_return {e.code(), std::span<entry>{}, false, false};
       }
       }, rados.get_executor()),
       token, dpp, part_num, ofs, std::move(result), this);
  }

  /// \brief Trim entries on a given part
  ///
  /// \param dpp Prefix provider for debug logging
  /// \param part_num Part to trim
  /// \param ofs Offset to which to trim
  /// \param exclusive If true, exclude end of range from trim
  /// \param token Boost.Asio CompletionToken
  ///
  /// \return Possibly errors in a way appropriate to the completion
  /// token.
  template<boost::asio::completion_token_for<
	     void(boost::system::error_code)> CompletionToken>
  auto trim_part(const DoutPrefixProvider* dpp,
		 std::int64_t part_num,
		 std::uint64_t ofs,
		 bool exclusive,
		 CompletionToken&& token) {
    namespace asio = boost::asio;
    namespace sys = boost::system;
    namespace fifo = rados::cls::fifo;
    namespace buffer = ceph::buffer;
    return asio::async_initiate<CompletionToken, void(sys::error_code)>
      (asio::experimental::co_composed<void(sys::error_code)>
       ([](auto state, const DoutPrefixProvider* dpp,
	   int64_t part_num, uint64_t ofs, bool exclusive, FIFO* f) -> void {
	 try {
	   state.throw_if_cancelled(true);
	   state.reset_cancellation_state(asio::enable_terminal_cancellation());

	   std::unique_lock l(f->m);
	   auto oid = f->info.part_oid(part_num);
	   l.unlock();

	   WriteOp op;
	   fifo::op::trim_part tp;

	   tp.ofs = ofs;
	   tp.exclusive = exclusive;

	   buffer::list in;
	   encode(tp, in);
	   op.exec(fifo::op::CLASS, fifo::op::TRIM_PART, in);
           co_await f->rados.execute(std::move(oid), f->ioc, std::move(op),
                                     asio::deferred);
	   co_return sys::error_code{};
	 } catch (const sys::system_error& e) {
	   ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			      << " trim part failed: " << e.what() << dendl;
	   co_return e.code();
	 }
       }, rados.get_executor()),
       token, dpp, part_num, ofs, exclusive, this);
  }

  ///@}

  /// \name Logics
  ///
  /// Internal logic used to implement the public interface
  ///
  ///@{

  /// \brief Convert a string to a marker
  ///
  /// This is not a free function since an empty string always refers
  /// to the tail part.
  ///
  /// \param s String representation of the marker
  /// \param l Ownership of mutex
  ///
  /// \returns The marker or nullopt if the string representation is invalid
  std::optional<marker> to_marker(std::string_view s,
				  std::unique_lock<std::mutex>& l) {
    assert(l.owns_lock());
    marker m;
    if (s.empty()) {
      m.num = info.tail_part_num;
      m.ofs = 0;
      return m;
    }

    auto pos = s.find(':');
    if (pos == s.npos) {
      return std::nullopt;
    }

    auto num = s.substr(0, pos);
    auto ofs = s.substr(pos + 1);

    auto n = ceph::parse<decltype(m.num)>(num);
    if (!n) {
      return std::nullopt;
    }
    m.num = *n;
    auto o = ceph::parse<decltype(m.ofs)>(ofs);
    if (!o) {
      return std::nullopt;
    }
    m.ofs = *o;
    return m;
  }

  /// \brief Force re-read of metadata
  ///
  /// \param dpp Prefix provider for debug logging
  /// \param token Boost.Asio CompletionToken
  ///
  /// \return Possibly errors in a way appropriate to the completion
  /// token.
  template<boost::asio::completion_token_for<
	     void(boost::system::error_code)> CompletionToken>
  auto read_meta(const DoutPrefixProvider* dpp,
		 CompletionToken&& token) {
    namespace asio = boost::asio;
    namespace sys = boost::system;
    return asio::async_initiate<CompletionToken, void(sys::error_code)>
      (asio::experimental::co_composed<void(sys::error_code)>
     ([](auto state, const DoutPrefixProvider* dpp,
	 FIFO* f) -> void {
       try {
	 state.throw_if_cancelled(true);
	 state.reset_cancellation_state(asio::enable_terminal_cancellation());

	 ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			    << " entering" << dendl;
	 auto [info, part_header_size, part_entry_overhead]
	   = co_await get_meta(f->rados, f->obj, f->ioc, std::nullopt,
			       asio::deferred);
	 std::unique_lock l(f->m);
	 if (info.version.same_or_later(f->info.version)) {
	   f->info = std::move(info);
	   f->part_header_size = part_header_size;
	   f->part_entry_overhead = part_entry_overhead;
	 }
       } catch (const sys::system_error& e) {
	 ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			    << " read_meta failed: " << e.what() << dendl;
	 co_return e.code();
       }
       co_return sys::error_code{};
     }, rados.get_executor()),
       token, dpp, this);
  }

  /// \brief Update local metadata
  ///
  /// \param dpp Debugging prefix provider
  /// \param objv Current metadata version (objclass will error on mismatch)
  /// \param update Changes to make to metadata
  /// \param token Boost.Asio CompletionToken
  ///
  /// \exception boost::system::system_error equivalent to
  /// boost::system::errc::operation_canceled on version mismatch.
  void apply_update(const DoutPrefixProvider *dpp,
		    rados::cls::fifo::info* info,
		    const rados::cls::fifo::objv& objv,
		    const rados::cls::fifo::update& update) {
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		       << " entering" << dendl;
    std::unique_lock l(m);
    if (objv != info->version) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << " version mismatch, canceling" << dendl;
      throw boost::system::system_error(ECANCELED,
					boost::system::generic_category());
    }

    info->apply_update(update);
  }

  /// \brief Update metadata locally and in the objclass
  ///
  /// \param dpp Debug prefix provider
  /// \param update The update to apply
  /// \param objv Current object version
  /// \param token Boost.Asio CompletionToken
  ///
  /// \return True if the operation was canceled. false otherwise in a
  /// way appropriate to the completion token.
  template<boost::asio::completion_token_for<
	     void(boost::system::error_code, bool)> CompletionToken>
  auto update_meta(const DoutPrefixProvider* dpp,
		   rados::cls::fifo::update update,
		   rados::cls::fifo::objv version,
		   CompletionToken&& token) {
    namespace asio = boost::asio;
    namespace sys = boost::system;
    namespace fifo = rados::cls::fifo;
    return asio::async_initiate<CompletionToken, void(sys::error_code, bool)>
      (asio::experimental::co_composed<void(sys::error_code, bool)>
     ([](auto state, const DoutPrefixProvider* dpp,
	 fifo::update update, fifo::objv version,
	 FIFO* f) -> void {
       try {
	 state.throw_if_cancelled(true);
	 state.reset_cancellation_state(asio::enable_terminal_cancellation());

	 ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			    << " entering" << dendl;

	 auto [ec] = co_await f->update_meta(version, update,
					     asio::as_tuple(asio::deferred));
	 bool canceled;
	 if (ec && ec != sys::errc::operation_canceled) {
	   throw sys::system_error(ec);
	 }
	 canceled = (ec == sys::errc::operation_canceled);
	 if (!canceled) {
	   try {
	     f->apply_update(dpp, &f->info, version, update);
	   } catch (const sys::system_error& e) {
	     if (e.code() == sys::errc::operation_canceled) {
	       canceled = true;
	     } else {
	       throw;
	     }
	   }
	 }
	 if (canceled) {
	   co_await f->read_meta(dpp, asio::deferred);
	 }
	 if (canceled) {
           ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			      << " canceled" << dendl;
	 }
	 co_return {sys::error_code{}, canceled};
       } catch (const sys::system_error& e) {
	 ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			    << " failed with error: " << e.what() << dendl;
	 co_return {e.code(), false};
       }
     }, rados.get_executor()),
       token, dpp, std::move(update), std::move(version), this);
  }

  /// \brief Process the journal
  ///
  /// \param dpp Debug prefix provider
  /// \param token Boost.Asio CompletionToken
  ///
  /// \return Nothing, but may error in a way appropriate to the
  /// completion token.
  template<boost::asio::completion_token_for<
	     void(boost::system::error_code)> CompletionToken>
  auto process_journal(const DoutPrefixProvider* dpp,
		       CompletionToken&& token) {
    namespace asio = boost::asio;
    namespace sys = boost::system;
    namespace fifo = rados::cls::fifo;
    return asio::async_initiate<CompletionToken, void(sys::error_code)>
      (asio::experimental::co_composed<void(sys::error_code)>
     ([](auto state, const DoutPrefixProvider* dpp, FIFO* f) -> void {
       try {
	 state.throw_if_cancelled(true);
	 state.reset_cancellation_state(asio::enable_terminal_cancellation());

	 ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			    << " entering." << dendl;
	 std::vector<fifo::journal_entry> processed;

	 std::unique_lock l(f->m);
	 auto tmpjournal = f->info.journal;
	 auto new_tail = f->info.tail_part_num;
	 auto new_head = f->info.head_part_num;
	 auto new_max = f->info.max_push_part_num;
	 l.unlock();

	 for (auto& entry : tmpjournal) {
	   ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			      << " processing entry: entry=" << entry
			      << dendl;
	   switch (entry.op) {
	     using enum fifo::journal_entry::Op;
	   case create:
	     ldpp_dout(dpp, 10) << __PRETTY_FUNCTION__ << ":" << __LINE__
				<< " Creating part " << entry.part_num << dendl;
	     co_await f->create_part(entry.part_num, asio::deferred);
	     if (entry.part_num > new_max) {
	       new_max = entry.part_num;
	     }
	     break;
	   case set_head:
	     ldpp_dout(dpp, 10) << __PRETTY_FUNCTION__ << ":" << __LINE__
				<< " Setting head to " << entry.part_num << dendl;
	     if (entry.part_num > new_head) {
	       new_head = entry.part_num;
	     }
	     break;
	   case remove:
	     try {
	       ldpp_dout(dpp, 10) << __PRETTY_FUNCTION__ << ":" << __LINE__
				  << " Removing part " << entry.part_num
				  << dendl;

	       co_await f->remove_part(entry.part_num, asio::deferred);
	       if (entry.part_num >= new_tail) {
		 new_tail = entry.part_num + 1;
	       }
	     } catch (const sys::system_error& e) {
	       if (e.code() != sys::errc::no_such_file_or_directory) {
		 throw;
	       }
	     }
	     break;
	   default:
	     ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
				<< " unknown journaled op: entry=" << entry
				<< dendl;
	     throw sys::system_error{EINVAL, sys::generic_category()};
	   }

	   processed.push_back(std::move(entry));
	 }

	 // Postprocess
	 bool canceled = true;

	 for (auto i = 0; canceled && i < MAX_RACE_RETRIES; ++i) {
	   ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			      << " postprocessing: i=" << i << dendl;

	   std::optional<std::int64_t> tail_part_num;
	   std::optional<std::int64_t> head_part_num;
	   std::optional<std::int64_t> max_part_num;

           std::unique_lock l(f->m);
           auto objv = f->info.version;
           if (new_tail > tail_part_num)
             tail_part_num = new_tail;
           if (new_head > f->info.head_part_num)
             head_part_num = new_head;
           if (new_max > f->info.max_push_part_num)
	     max_part_num = new_max;
           l.unlock();

           if (processed.empty() && !tail_part_num && !max_part_num) {
             ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
                                << " nothing to update any more: i=" << i
                                << dendl;
             canceled = false;
             break;
	   }
	   auto u = fifo::update().tail_part_num(tail_part_num)
	     .head_part_num(head_part_num).max_push_part_num(max_part_num)
	     .journal_entries_rm(processed);
	   ldpp_dout(dpp, 10) << __PRETTY_FUNCTION__ << ":" << __LINE__
			      << " Calling update_meta: update=" << u
			      << dendl;

	   canceled = co_await f->update_meta(dpp, u, objv, asio::deferred);
	   if (canceled) {
	     std::vector<fifo::journal_entry> new_processed;
	     std::unique_lock l(f->m);
	     ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
				<< " update canceled, retrying: i=" << i
				<< dendl;
	     for (auto& e : processed) {
	       if (f->info.journal.contains(e)) {
		 new_processed.push_back(e);
	       }
	     }
	     processed = std::move(new_processed);
	   }
	 }
	 if (canceled) {
	   ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			      << " canceled too many times, giving up" << dendl;
	   throw sys::system_error(ECANCELED, sys::generic_category());
	 }
       } catch (const sys::system_error& e) {
	 ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			    << " process_journal failed" << ":" << e.what()
			    << dendl;
	 co_return e.code();
       }
       co_return sys::error_code{};
     }, rados.get_executor()),
       token, dpp, this);
  }

  /// \brief Create a new part
  ///
  /// And possibly set it as head
  ///
  /// \param dpp Debug prefix provider
  /// \param new_part_num New part to create
  /// \param is_head True if part is to be new head
  /// \param token Boost.Asio CompletionToken
  ///
  /// \return Nothing, but may error in a way appropriate to the
  /// completion token.
  template<boost::asio::completion_token_for<
	     void(boost::system::error_code)> CompletionToken>
  auto prepare_new_part(const DoutPrefixProvider* dpp,
			std::int64_t new_part_num, bool is_head,
			CompletionToken&& token) {
    namespace asio = boost::asio;
    namespace sys = boost::system;
    namespace fifo = rados::cls::fifo;
    return asio::async_initiate<CompletionToken, void(sys::error_code)>
      (asio::experimental::co_composed<void(sys::error_code)>
       ([](auto state, const DoutPrefixProvider* dpp,
	   std::int64_t new_part_num, bool is_head, FIFO* f) -> void {
	 try {
	   state.throw_if_cancelled(true);
	   state.reset_cancellation_state(asio::enable_terminal_cancellation());


	   ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			      << " entering" << dendl;
	   std::unique_lock l(f->m);
	   using enum fifo::journal_entry::Op;
	   std::vector<fifo::journal_entry> jentries{{create, new_part_num}};
	   if (f->info.journal.contains({create, new_part_num}) &&
	       (!is_head || f->info.journal.contains({set_head, new_part_num}))) {
	     l.unlock();
	     ldpp_dout(dpp, 5)
		 << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " new part journaled, but not processed"
		 << dendl;
	     co_await f->process_journal(dpp, asio::deferred);
	     co_return sys::error_code{};
	   }
	   auto version = f->info.version;

	   if (is_head) {
	     ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
				<< " needs new head" << dendl;
	     jentries.push_back({set_head, new_part_num});
	   }
	   l.unlock();

	   bool canceled = true;
	   for (auto i = 0; canceled && i < MAX_RACE_RETRIES; ++i) {
	     canceled = false;
	     ldpp_dout(dpp, 20)
		 << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " updating metadata: i=" << i << dendl;
	     auto u = fifo::update{}.journal_entries_add(jentries);
	     canceled = co_await f->update_meta(dpp, u, version,
						asio::deferred);
	     if (canceled) {
	       std::unique_lock l(f->m);
	       version = f->info.version;
	       auto found = (f->info.journal.contains({create, new_part_num}) ||
			     f->info.journal.contains({set_head, new_part_num}));
	       if ((f->info.max_push_part_num >= new_part_num &&
		    f->info.head_part_num >= new_part_num)) {
		 ldpp_dout(dpp, 20)
		     << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << " raced, but journaled and processed: i=" << i
		     << dendl;
		 co_return sys::error_code{};
	       }
	       if (found) {
		 ldpp_dout(dpp, 20)
		     << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << " raced, journaled but not processed: i=" << i
		     << dendl;
		 canceled = false;
	       }
	       l.unlock();
	     }
	   }
	   if (canceled) {
	     ldpp_dout(dpp, -1)
		 << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " canceled too many times, giving up" << dendl;
	     throw sys::system_error{ECANCELED, sys::generic_category()};
	   }
	   co_await f->process_journal(dpp, asio::deferred);
	   co_return sys::error_code{};
	 } catch (const sys::system_error& e) {
	   ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			      << " prepare_new_part failed: " << e.what()
			      << dendl;
	   co_return e.code();
	 }
       }, rados.get_executor()),
       token, dpp, new_part_num, is_head, this);
  }

  /// \brief Set a new part as head
  ///
  /// In practice we create a new part and set it as head in one go.
  ///
  /// \param dpp Debug prefix provider
  /// \param new_head_part_num New head part number
  /// \param token Boost.Asio CompletionToken
  ///
  /// \return Nothing, but may error in a way appropriate to the
  /// completion token.
  template<boost::asio::completion_token_for<
	     void(boost::system::error_code)> CompletionToken>
  auto prepare_new_head(const DoutPrefixProvider* dpp,
			std::int64_t new_head_part_num,
			CompletionToken&& token) {
    namespace asio = boost::asio;
    namespace sys = boost::system;
    namespace fifo = rados::cls::fifo;
    return asio::async_initiate<CompletionToken, void(sys::error_code)>
      (asio::experimental::co_composed<void(sys::error_code)>
       ([](auto state, const DoutPrefixProvider* dpp,
	   std::int64_t new_head_part_num, FIFO* f) -> void {
	 try {
	   state.throw_if_cancelled(true);
	   state.reset_cancellation_state(asio::enable_terminal_cancellation());

	   ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			      << " entering" << dendl;
	   std::unique_lock l(f->m);
	   auto max_push_part_num = f->info.max_push_part_num;
	   auto version = f->info.version;
	   l.unlock();

	   if (max_push_part_num < new_head_part_num) {
	     ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
				<< " need new part" << dendl;
	     co_await f->prepare_new_part(dpp, new_head_part_num, true,
					  asio::deferred);
	     std::unique_lock l(f->m);
	     if (f->info.max_push_part_num < new_head_part_num) {
	       ldpp_dout(dpp, -1)
		 << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " inconsistency, push part less than head part."
		 << dendl;
	       throw sys::system_error(EIO, sys::generic_category());
	     }
	     l.unlock();
	   }

	   using enum fifo::journal_entry::Op;
	   fifo::journal_entry jentry;
	   jentry.op = set_head;
	   jentry.part_num = new_head_part_num;

	   bool canceled = true;
	   for (auto i = 0; canceled && i < MAX_RACE_RETRIES; ++i) {
	     canceled = false;
	     ldpp_dout(dpp, 20)
		 << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " updating metadata: i=" << i << dendl;
	     auto u = fifo::update{}.journal_entries_add({{jentry}});
	     canceled = co_await f->update_meta(dpp, u, version,
						asio::deferred);
	     if (canceled) {
	       std::unique_lock l(f->m);
	       auto found =
		   (f->info.journal.contains({create, new_head_part_num}) ||
		    f->info.journal.contains({set_head, new_head_part_num}));
	       version = f->info.version;
	       if ((f->info.head_part_num >= new_head_part_num)) {
		 ldpp_dout(dpp, 20)
		     << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << " raced, but journaled and processed: i=" << i
		     << dendl;
		 co_return sys::error_code{};
	       }
	       if (found) {
		 ldpp_dout(dpp, 20)
		     << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << " raced, journaled but not processed: i=" << i
		     << dendl;
		 canceled = false;
	       }
	       l.unlock();
	     }
	   }
	   if (canceled) {
	     ldpp_dout(dpp, -1)
		 << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " canceled too many times, giving up" << dendl;
	     throw sys::system_error(ECANCELED, sys::generic_category());
	   }
	   co_await f->process_journal(dpp, asio::deferred);
	   co_return sys::error_code{};
	 } catch (const sys::system_error& e) {
	   ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			      << " prepare_new_head failed: " << e.what()
			      << dendl;
	   co_return e.code();
	 }
       }, rados.get_executor()),
       token, dpp, new_head_part_num, this);
  }

  ///@}


public:

  FIFO(const FIFO&) = delete;
  FIFO& operator =(const FIFO&) = delete;
  FIFO(FIFO&&) = delete;
  FIFO& operator =(FIFO&&) = delete;

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
		   neorados::RADOS& rados,
		   neorados::Object obj,
		   neorados::IOContext ioc,
		   CompletionToken&& token,
		   std::optional<rados::cls::fifo::objv> objv = std::nullopt,
		   bool probe = false) {
    namespace asio = boost::asio;
    namespace sys = boost::system;
    return asio::async_initiate<CompletionToken,
				void(sys::error_code, std::unique_ptr<FIFO>)>
      (asio::experimental::co_composed<void(sys::error_code, std::unique_ptr<FIFO>)>
       ([](auto state, const DoutPrefixProvider* dpp, neorados::RADOS& rados,
	   neorados::Object obj, neorados::IOContext ioc,
	   std::optional<rados::cls::fifo::objv> objv, bool probe) -> void {
	 try {
	   state.throw_if_cancelled(true);
	   state.reset_cancellation_state(asio::enable_terminal_cancellation());
	   ldpp_dout(dpp, 20)
	     << __PRETTY_FUNCTION__ << ":" << __LINE__
	     << " entering" << dendl;
	   auto [info, size, over] = co_await get_meta(rados, obj, ioc, objv,
						     asio::deferred);
	   std::unique_ptr<FIFO> f(new FIFO(rados,
					    std::move(obj),
					    std::move(ioc),
					    size, over, std::move(info)));
	   probe = 0;

	   // If there are journal entries, process them, in case
	   // someone crashed mid-transaction.
	   if (!info.journal.empty()) {
	     ldpp_dout(dpp, 20)
	       << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " processing leftover journal" << dendl;
	     co_await f->process_journal(dpp, asio::deferred);
	   }
	   co_return {sys::error_code{}, std::move(f)};
	 } catch (const sys::system_error& e) {
	   if (!probe ||
	       (probe && !(e.code() == sys::errc::no_such_file_or_directory ||
			   e.code() == sys::errc::no_message_available))) {
	     ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
				<< " open failed:" << e.what() << dendl;
	   }
	   co_return {e.code(), std::unique_ptr<FIFO>{}};
	 }
       }, rados.get_executor()),
       token, dpp, std::ref(rados), std::move(obj), std::move(ioc),
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
		     neorados::RADOS& rados,
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
      (asio::experimental::co_composed<void(sys::error_code,
                                            std::unique_ptr<FIFO>)>
       ([](auto state, const DoutPrefixProvider* dpp, neorados::RADOS& rados,
	   neorados::Object obj, neorados::IOContext ioc,
	   std::optional<rados::cls::fifo::objv> objv,
	   std::optional<std::string> oid_prefix, bool exclusive,
	   std::uint64_t max_part_size, std::uint64_t max_entry_size) -> void {
	 try {
	   state.throw_if_cancelled(true);
	   state.reset_cancellation_state(asio::enable_terminal_cancellation());

	   ldpp_dout(dpp, 20)
	     << __PRETTY_FUNCTION__ << ":" << __LINE__
	     << " entering" << dendl;
	   ldpp_dout(dpp, 10)
	     << __PRETTY_FUNCTION__ << ":" << __LINE__
	     << " Calling create_meta" << dendl;
	   co_await create_meta(rados, obj, ioc, objv, oid_prefix, exclusive,
				max_part_size, max_entry_size, asio::deferred);
	   auto f = co_await open(dpp, rados, std::move(obj), std::move(ioc),
				  asio::deferred, objv);
	   co_return std::make_tuple(sys::error_code{}, std::move(f));
	 } catch (const sys::system_error& e) {
	   ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			      << " create failed: " << e.what() << dendl;
	   co_return {e.code(), std::unique_ptr<FIFO>{}};
	 }
       }, rados.get_executor()),
       token, dpp, std::ref(rados), std::move(obj), std::move(ioc),
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
	    std::deque<ceph::buffer::list> entries,
	    CompletionToken&& token) {
    namespace asio = boost::asio;
    namespace sys = boost::system;
    namespace buffer = ceph::buffer;
    return asio::async_initiate<CompletionToken,
				void(sys::error_code)>
      (asio::experimental::co_composed<void(sys::error_code)>
       ([](auto state, const DoutPrefixProvider* dpp,
	   std::deque<buffer::list> remaining, FIFO* f) -> void {
	 try {
	   state.throw_if_cancelled(true);
	   state.reset_cancellation_state(asio::enable_terminal_cancellation());

	   std::unique_lock l(f->m);
	   auto max_entry_size = f->info.params.max_entry_size;
	   auto need_new_head = f->info.need_new_head();
	   auto head_part_num = f->info.head_part_num;
	   l.unlock();
	   ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			      << " entering" << dendl;
	   if (remaining.empty()) {
	     ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
				<< " empty push, returning success" << dendl;
	     co_return sys::error_code{};
	   }

	   // Validate sizes
	   for (const auto& bl : remaining) {
	     if (bl.length() > max_entry_size) {
	       ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
				  << " entry bigger than max_entry_size"
				  << dendl;
	       co_return sys::error_code{E2BIG, sys::generic_category()};
	     }
	   }

	   if (need_new_head) {
	     ldpp_dout(dpp, 10) << __PRETTY_FUNCTION__ << ":" << __LINE__
				<< " need new head: "
				<< head_part_num + 1 << dendl;
	     co_await f->prepare_new_head(dpp, head_part_num + 1, asio::deferred);
	   }

	   std::deque<buffer::list> batch;

	   uint64_t batch_len = 0;
	   auto retries = 0;
	   bool canceled = true;
	   while ((!remaining.empty() || !batch.empty()) &&
		  (retries <= MAX_RACE_RETRIES)) {
	     ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
				<< " preparing push: remaining=" << remaining.size()
				<< " batch=" << batch.size() << " retries=" << retries
				<< dendl;
	     std::unique_lock l(f->m);
	     head_part_num = f->info.head_part_num;
	     auto max_part_size = f->info.params.max_part_size;
	     auto overhead = f->part_entry_overhead;
	     l.unlock();

	     while (!remaining.empty() &&
		    (remaining.front().length() + batch_len <= max_part_size)) {
	       /* We can send entries with data_len up to max_entry_size,
		  however, we want to also account the overhead when
		  dealing with multiple entries. Previous check doesn't
		  account for overhead on purpose. */
	       batch_len += remaining.front().length() + overhead;
	       batch.push_back(std::move(remaining.front()));
	       remaining.pop_front();
	     }
	     ldpp_dout(dpp, 20)
	       << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " prepared push: remaining=" << remaining.size()
	       << " batch=" << batch.size() << " retries=" << retries
	       << " batch_len=" << batch_len << dendl;

	     auto [ec, n] =
	       co_await f->push_entries(dpp, batch,
					asio::as_tuple(asio::deferred));
	     if (ec == sys::errc::result_out_of_range) {
	       canceled = true;
	       ++retries;
	       ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
				  << " need new head " << head_part_num + 1
				  << dendl;
	       co_await f->prepare_new_head(dpp, head_part_num + 1,
					    asio::deferred);
	       continue;
	     } else if (ec == sys::errc::no_such_file_or_directory) {
	       ldpp_dout(dpp, 20)
		 << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " racing client trimmed part, rereading metadata "
		 << dendl;
	       canceled = true;
	       ++retries;
	       co_await f->read_meta(dpp, asio::deferred);
	       continue;
	     } else if (ec) {
	       ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
				  << " push_entries failed: " << ec.message()
				  << dendl;
	       throw sys::system_error(ec);
	     }
	     assert(n >= 0);
	     // Made forward progress!
	     canceled = false;
	     retries = 0;
	     batch_len = 0;
	     if (n == ssize(batch)) {
	       batch.clear();
	     } else  {
	       batch.erase(batch.begin(), batch.begin() + n);
	       for (const auto& b : batch) {
		 batch_len +=  b.length() + overhead;
	       }
	     }
	   }
	   if (canceled) {
	     ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
				<< " canceled too many times, giving up."
				<< dendl;
	     co_return sys::error_code{ECANCELED, sys::generic_category()};
	   }
	   co_return sys::error_code{};
	 } catch (const sys::system_error& e) {
	   ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			      << " push failed: " << e.what() << dendl;
	   co_return e.code();
	 }
       }, rados.get_executor()),
       token, dpp, std::move(entries), this);
  }

  /// \brief Push an entry to the FIFO
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
                  std::optional<std::string>)> CompletionToken>
  auto list(const DoutPrefixProvider* dpp,
	    std::optional<std::string> markstr,
	    std::span<entry> entries,
	    CompletionToken&& token) {
    namespace asio = boost::asio;
    namespace sys = boost::system;
    return asio::async_initiate<CompletionToken,
				void(sys::error_code, std::span<entry>,
                                     std::optional<std::string>)>
      (asio::experimental::co_composed<void(sys::error_code,
                                            std::span<entry>,
                                            std::optional<std::string>)>
       ([](auto state, const DoutPrefixProvider* dpp,
	   std::optional<std::string> markstr, std::span<entry> entries,
	   FIFO* f) -> void {
	 try {
	   state.throw_if_cancelled(true);
	   state.reset_cancellation_state(asio::enable_terminal_cancellation());

	   ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			      << " entering" << dendl;
	   std::unique_lock l(f->m);
	   std::int64_t part_num = f->info.tail_part_num;
	   std::uint64_t ofs = 0;
	   if (markstr) {
	     auto marker = f->to_marker(*markstr, l);
	     if (!marker) {
	       ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
				  << " invalid marker string: " << markstr
				  << dendl;
	       throw sys::system_error{EINVAL, sys::generic_category()};
	     }
	     part_num = marker->num;
	     ofs = marker->ofs;
	   }
	   l.unlock();

	   bool more = false;

	   auto entries_left = entries;

	   while (entries_left.size() > 0) {
	     more = false;
	     ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
				<< " entries_left.size()="
				<< entries_left.size() << dendl;
	     auto [ec, res, part_more, part_full] =
	       co_await f->list_part(dpp, part_num, ofs, entries_left,
				     asio::as_tuple(asio::deferred));
	     if (ec == sys::errc::no_such_file_or_directory) {
	       ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
				  << " missing part, rereading metadata"
				  << dendl;
	       co_await f->read_meta(dpp, asio::deferred);
	       std::unique_lock l(f->m);
	       if (part_num < f->info.tail_part_num) {
		 /* raced with trim? restart */
		 ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
				    << " raced with trim, restarting" << dendl;
		 entries_left = entries;
		 part_num = f->info.tail_part_num;
		 l.unlock();
		 ofs = 0;
		 continue;
	       }
	       l.unlock();
	       ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
				  << " assuming part was not written yet, "
				  << "so end of data" << dendl;
	       break;
	     } else if (ec) {
	       ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
				  << " list_entries failed: " << ec.message()
				  << dendl;
	       throw sys::system_error(ec);
	     }
	     more = part_full || part_more;
	     entries_left = entries_left.last(entries_left.size() - res.size());

	     if (!part_full) {
	       ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
				  << " head part is not full, so we can assume "
				  << "we're done" << dendl;
	       break;
	     }
	     if (!part_more) {
	       ++part_num;
	       ofs = 0;
	     }
	   }
	   std::optional<std::string> marker;
	   if (entries_left.size() > 0) {
	     entries = entries.first(entries.size() - entries_left.size());
	   }
	   if (more && !entries.empty()) {
	     marker = entries.back().marker;
	   }
	   co_return {sys::error_code{}, std::move(entries), std::move(marker)};
	 } catch (const sys::system_error& e) {
	   ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			      << " list failed: " << e.what() << dendl;
	   co_return {e.code(), std::span<entry>{}, std::nullopt};
	 }
       }, rados.get_executor()),
       token, dpp, std::move(markstr), std::move(entries), this);
  }

  /// \brief Push entries to the FIFO
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
    namespace asio = boost::asio;
    namespace sys = boost::system;
    namespace fifo = rados::cls::fifo;
    return asio::async_initiate<CompletionToken,
				void(sys::error_code)>
      (asio::experimental::co_composed<void(sys::error_code)>
       ([](auto state, const DoutPrefixProvider* dpp,
	   std::string markstr, bool exclusive, FIFO* f) -> void {
	 try {
	   state.throw_if_cancelled(true);
	   state.reset_cancellation_state(asio::enable_terminal_cancellation());

	   ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			      << " entering" << dendl;
	   bool overshoot = false;
	   std::unique_lock l(f->m);
	   auto marker = f->to_marker(markstr, l);
	   if (!marker) {
	     ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
				<< " invalid marker string: " << markstr
				<< dendl;
	     throw sys::system_error{EINVAL, sys::generic_category()};
	   }
	   auto part_num = marker->num;
	   auto ofs = marker->ofs;
	   auto hn = f->info.head_part_num;
	   const auto max_part_size = f->info.params.max_part_size;
	   if (part_num > hn) {
	     l.unlock();
	     co_await f->read_meta(dpp, asio::deferred);
	     l.lock();
	     hn = f->info.head_part_num;
	     if (part_num > hn) {
	       overshoot = true;
	       part_num = hn;
	       ofs = max_part_size;
	     }
	   }
	   if (part_num < f->info.tail_part_num) {
	     throw sys::system_error(ENODATA, sys::generic_category());
	   }
	   auto pn = f->info.tail_part_num;
	   l.unlock();

	   while (pn < part_num) {
	     ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
				<< " pn=" << pn << dendl;
	     auto [ec] = co_await f->trim_part(dpp, pn, max_part_size, false,
					       asio::as_tuple(asio::deferred));
	     if (ec && ec == sys::errc::no_such_file_or_directory) {
	       ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
				  << " trim_part failed: " << ec.message()
				  << dendl;
	       throw sys::system_error(ec);
	     }
	     ++pn;
	   }

	   auto [ec] = co_await f->trim_part(dpp, pn, ofs, exclusive,
					     asio::as_tuple(asio::deferred));
	   if (ec && ec == sys::errc::no_such_file_or_directory) {
	     ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
				<< " trim_part failed: " << ec.message()
				<< dendl;
	     throw sys::system_error(ec);
	   }

	   l.lock();
	   auto tail_part_num = f->info.tail_part_num;
	   auto objv = f->info.version;
	   l.unlock();
	   bool canceled = tail_part_num < part_num;
	   int retries = 0;
	   while ((tail_part_num < part_num) &&
		  canceled &&
		  (retries <= MAX_RACE_RETRIES)) {
             canceled = co_await f->update_meta(dpp, fifo::update{}
                                                .tail_part_num(part_num),
						objv, asio::deferred);
	     if (canceled) {
	       ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
				  << " canceled: retries=" << retries
				  << dendl;
	       l.lock();
	       tail_part_num = f->info.tail_part_num;
	       objv = f->info.version;
	       l.unlock();
	       ++retries;
	     }
	   }
	   if (canceled) {
	     ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
				<< " canceled too many times, giving up"
				<< dendl;
	     throw sys::system_error(EIO, sys::generic_category());
	   }
	   co_return (overshoot ?
		      sys::error_code{ENODATA, sys::generic_category()} :
		      sys::error_code{});
	   co_return sys::error_code{};
	 } catch (const sys::system_error& e) {
	   ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			      << " trim failed: " << e.what() << dendl;
	   co_return e.code();
	 }
       }, rados.get_executor()),
       token, dpp, std::move(marker), exclusive, this);
  }

  static constexpr auto max_list_entries =
    rados::cls::fifo::op::MAX_LIST_ENTRIES;

  /// The default maximum size of every part object (that is, every
  /// object holding entries)
  static constexpr std::uint64_t default_max_part_size = 4 * 1024 * 1024;

  /// The default maximum entry size
  static constexpr std::uint64_t default_max_entry_size = 32 * 1024;

  /// Return a marker comparing less than any other marker.
  static auto min_marker() {
    return marker{std::numeric_limits<decltype(marker::num)>::max(),
                  std::numeric_limits<decltype(marker::ofs)>::max()}
      .to_string();
  }

  /// Return a marker comparing greater than any other marker.
  static auto max_marker() {
    return marker{std::numeric_limits<decltype(marker::num)>::max(),
                  std::numeric_limits<decltype(marker::ofs)>::max()}
      .to_string();
  }

  /// \brief Get information on the last entry
  ///
  /// \param dpp Prefix provider for debug logging
  /// \param token Boost.Asio CompletionToken
  ///
  /// \return {marker, time} for the latest entry in a way appropriate
  /// to the completion token.
  template<boost::asio::completion_token_for<
	     void(boost::system::error_code, std::string, ceph::real_time)>
	   CompletionToken>
  auto last_entry_info(const DoutPrefixProvider* dpp,
		       CompletionToken&& token) {
    namespace asio = boost::asio;
    namespace sys = boost::system;
    namespace buffer = ceph::buffer;
    return asio::async_initiate<
      CompletionToken, void(sys::error_code, std::string, ceph::real_time)>
      (asio::experimental::co_composed<
	void(sys::error_code, std::string, ceph::real_time)>
       ([](auto state, const DoutPrefixProvider* dpp,
	   FIFO* f) -> void {
	 try {
	   state.throw_if_cancelled(true);
	   state.reset_cancellation_state(asio::enable_terminal_cancellation());

	   co_await f->read_meta(dpp, asio::deferred);
	   std::unique_lock l(f->m);
	   auto head_part_num = f->info.head_part_num;
	   l.unlock();

	   if (head_part_num < 0) {
	     co_return {sys::error_code{}, std::string{},
	                ceph::real_clock::zero()};
	   } else {
	     auto header =
	       co_await f->get_part_info(head_part_num, asio::deferred);
	     co_return {sys::error_code{},
	                marker{head_part_num, header.last_ofs}.to_string(),
	                header.max_time};
	   }
	 } catch (const sys::system_error& e) {
	   ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			      << " push failed: " << e.what() << dendl;
	   co_return {e.code(), std::string{}, ceph::real_time{}};
	 }
       }, rados.get_executor()),
       token, dpp, this);
  }
};
} // namespace neorados::cls::fifo {
