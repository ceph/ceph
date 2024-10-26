// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <vector>

#include <fmt/format.h>

#include "include/rados/librados.hpp"
#include "include/buffer.h"
#include "include/function2.hpp"

#include "common/async/yield_context.h"

#include "cls/fifo/cls_fifo_types.h"
#include "cls/fifo/cls_fifo_ops.h"

#include "librados/AioCompletionImpl.h"

#include "rgw_tools.h"

namespace rgw::cls::fifo {
namespace cb = ceph::buffer;
namespace fifo = ::rados::cls::fifo;
namespace lr = librados;

inline constexpr std::uint64_t default_max_part_size = 4 * 1024 * 1024;
inline constexpr std::uint64_t default_max_entry_size = 32 * 1024;

void create_meta(lr::ObjectWriteOperation* op, std::string_view id,
		 std::optional<fifo::objv> objv,
		 std::optional<std::string_view> oid_prefix,
		 bool exclusive = false,
		 std::uint64_t max_part_size = default_max_part_size,
		 std::uint64_t max_entry_size = default_max_entry_size);
int get_meta(const DoutPrefixProvider *dpp, lr::IoCtx& ioctx, const std::string& oid,
	     std::optional<fifo::objv> objv, fifo::info* info,
	     std::uint32_t* part_header_size,
	     std::uint32_t* part_entry_overhead,
	     std::uint64_t tid, optional_yield y,
	     bool probe = false);
struct marker {
  std::int64_t num = 0;
  std::uint64_t ofs = 0;

  marker() = default;
  marker(std::int64_t num, std::uint64_t ofs) : num(num), ofs(ofs) {}
  static marker max() {
    return { std::numeric_limits<decltype(num)>::max(),
	     std::numeric_limits<decltype(ofs)>::max() };
  }

  std::string to_string() {
    return fmt::format("{:0>20}:{:0>20}", num, ofs);
  }
};

struct list_entry {
  cb::list data;
  std::string marker;
  ceph::real_time mtime;
};

using part_info = fifo::part_header;

/// This is an implementation of FIFO using librados to facilitate
/// backports. Please see /src/neorados/cls/fifo.h for full
/// information.
///
/// This library uses optional_yield. Please see
/// /src/common/async/yield_context.h. In summary, optional_yield
/// contains either a boost::asio::yield_context (in which case the current
/// coroutine is suspended until completion) or null_yield (in which
/// case the current thread is blocked until completion.)
///
/// Please see the librados documentation for information on
/// AioCompletion and IoCtx.

class FIFO {
  friend struct Reader;
  friend struct Updater;
  friend struct Trimmer;
  friend struct InfoGetter;
  friend struct Pusher;
  friend struct NewPartPreparer;
  friend struct NewHeadPreparer;
  friend struct JournalProcessor;
  friend struct Lister;

  mutable lr::IoCtx ioctx;
  CephContext* cct = static_cast<CephContext*>(ioctx.cct());
  const std::string oid;
  std::mutex m;
  std::uint64_t next_tid = 0;

  fifo::info info;

  std::uint32_t part_header_size = 0xdeadbeef;
  std::uint32_t part_entry_overhead = 0xdeadbeef;

  std::optional<marker> to_marker(std::string_view s);

  FIFO(lr::IoCtx&& ioc,
       std::string oid)
    : ioctx(std::move(ioc)), oid(oid) {}

  int apply_update(const DoutPrefixProvider *dpp,
                   fifo::info* info,
		   const fifo::objv& objv,
		   const fifo::update& update,
		   std::uint64_t tid);
  int _update_meta(const DoutPrefixProvider *dpp, const fifo::update& update,
		   fifo::objv version, bool* pcanceled,
		   std::uint64_t tid, optional_yield y);
  void _update_meta(const DoutPrefixProvider *dpp, const fifo::update& update,
		    fifo::objv version, bool* pcanceled,
		    std::uint64_t tid, lr::AioCompletion* c);
  int create_part(const DoutPrefixProvider *dpp, int64_t part_num, std::uint64_t tid,
		  optional_yield y);
  int remove_part(const DoutPrefixProvider *dpp, int64_t part_num, std::uint64_t tid,
		  optional_yield y);
  int process_journal(const DoutPrefixProvider *dpp, std::uint64_t tid, optional_yield y);
  void process_journal(const DoutPrefixProvider *dpp, std::uint64_t tid, lr::AioCompletion* c);
  int _prepare_new_part(const DoutPrefixProvider *dpp, std::int64_t new_part_num, bool is_head, std::uint64_t tid, optional_yield y);
  void _prepare_new_part(const DoutPrefixProvider *dpp, std::int64_t new_part_num, bool is_head, std::uint64_t tid, lr::AioCompletion* c);
  int _prepare_new_head(const DoutPrefixProvider *dpp, std::int64_t new_head_part_num,
			std::uint64_t tid, optional_yield y);
  void _prepare_new_head(const DoutPrefixProvider *dpp, std::int64_t new_head_part_num, std::uint64_t tid, lr::AioCompletion* c);
  int push_entries(const DoutPrefixProvider *dpp, const std::deque<cb::list>& data_bufs,
		   std::uint64_t tid, optional_yield y);
  void push_entries(const std::deque<cb::list>& data_bufs,
		    std::uint64_t tid, lr::AioCompletion* c);
  int trim_part(const DoutPrefixProvider *dpp, int64_t part_num, uint64_t ofs,
		bool exclusive, std::uint64_t tid, optional_yield y);
  void trim_part(const DoutPrefixProvider *dpp, int64_t part_num, uint64_t ofs,
		 bool exclusive, std::uint64_t tid, lr::AioCompletion* c);

  /// Force refresh of metadata, yielding/blocking style
  int read_meta(const DoutPrefixProvider *dpp, std::uint64_t tid, optional_yield y);
  /// Force refresh of metadata, with a librados Completion
  void read_meta(const DoutPrefixProvider *dpp, std::uint64_t tid, lr::AioCompletion* c);

public:

  FIFO(const FIFO&) = delete;
  FIFO& operator =(const FIFO&) = delete;
  FIFO(FIFO&&) = delete;
  FIFO& operator =(FIFO&&) = delete;

  /// Open an existing FIFO.
  static int open(const DoutPrefixProvider *dpp, lr::IoCtx ioctx, //< IO Context
		  std::string oid, //< OID for metadata object
		  std::unique_ptr<FIFO>* fifo, //< OUT: Pointer to FIFO object
		  optional_yield y, //< Optional yield context
		  /// Operation will fail if FIFO is not at this version
		  std::optional<fifo::objv> objv = std::nullopt,
		  /// Probing for existence, don't print errors if we
		  /// can't find it.
		  bool probe = false);
  /// Create a new or open an existing FIFO.
  static int create(const DoutPrefixProvider *dpp, lr::IoCtx ioctx, //< IO Context
		    std::string oid, //< OID for metadata object
		    std::unique_ptr<FIFO>* fifo, //< OUT: Pointer to FIFO object
		    optional_yield y, //< Optional yield context
		    /// Operation will fail if the FIFO exists and is
		    /// not of this version.
		    std::optional<fifo::objv> objv = std::nullopt,
		    /// Prefix for all objects
		    std::optional<std::string_view> oid_prefix = std::nullopt,
		    /// Fail if the FIFO already exists
		    bool exclusive = false,
		    /// Maximum allowed size of parts
		    std::uint64_t max_part_size = default_max_part_size,
		    /// Maximum allowed size of entries
		    std::uint64_t max_entry_size = default_max_entry_size);

  /// Force refresh of metadata, yielding/blocking style
  int read_meta(const DoutPrefixProvider *dpp, optional_yield y);
  /// Get currently known metadata
  const fifo::info& meta() const;
  /// Get partition header and entry overhead size
  std::pair<std::uint32_t, std::uint32_t> get_part_layout_info() const;
  /// Push an entry to the FIFO
  int push(const DoutPrefixProvider *dpp, 
           const cb::list& bl, //< Entry to push
	   optional_yield y //< Optional yield
    );
  /// Push an entry to the FIFO
  void push(const DoutPrefixProvider *dpp, const cb::list& bl, //< Entry to push
	    lr::AioCompletion* c //< Async Completion
    );
  /// Push entries to the FIFO
  int push(const DoutPrefixProvider *dpp, 
           const std::vector<cb::list>& data_bufs, //< Entries to push
	   optional_yield y //< Optional yield
    );
  /// Push entries to the FIFO
  void push(const DoutPrefixProvider *dpp, const std::vector<cb::list>& data_bufs, //< Entries to push
	    lr::AioCompletion* c //< Async Completion
    );
  /// List entries
  int list(const DoutPrefixProvider *dpp, 
           int max_entries, //< Maximum entries to list
	   /// Point after which to begin listing. Start at tail if null
	   std::optional<std::string_view> markstr,
	   std::vector<list_entry>* out, //< OUT: entries
	   /// OUT: True if more entries in FIFO beyond the last returned
	   bool* more,
	   optional_yield y //< Optional yield
    );
  void list(const DoutPrefixProvider *dpp, 
            int max_entries, //< Maximum entries to list
	    /// Point after which to begin listing. Start at tail if null
	    std::optional<std::string_view> markstr,
	    std::vector<list_entry>* out, //< OUT: entries
	    /// OUT: True if more entries in FIFO beyond the last returned
	    bool* more,
	    lr::AioCompletion* c //< Async Completion
    );
  /// Trim entries, coroutine/block style
  int trim(const DoutPrefixProvider *dpp, 
           std::string_view markstr, //< Position to which to trim, inclusive
	   bool exclusive, //< If true, do not trim the target entry
			   //< itself, just all those before it.
	   optional_yield y //< Optional yield
    );
  /// Trim entries, librados AioCompletion style
  void trim(const DoutPrefixProvider *dpp, 
            std::string_view markstr, //< Position to which to trim, inclusive
	    bool exclusive, //< If true, do not trim the target entry
	                    //< itself, just all those before it.
	    lr::AioCompletion* c //< librados AIO Completion
    );
  /// Get part info
  int get_part_info(const DoutPrefixProvider *dpp, int64_t part_num, /// Part number
		    fifo::part_header* header, //< OUT: Information
		    optional_yield y //< Optional yield
    );
  /// Get part info
  void get_part_info(int64_t part_num, //< Part number
		    fifo::part_header* header, //< OUT: Information
		    lr::AioCompletion* c //< AIO Completion
    );
  /// A convenience method to fetch the part information for the FIFO
  /// head, using librados::AioCompletion, since
  /// libradio::AioCompletions compose lousily.
  void get_head_info(const DoutPrefixProvider *dpp, fu2::unique_function< //< Function to receive info
		       void(int r, fifo::part_header&&)>,
		     lr::AioCompletion* c //< AIO Completion
    );
};

template<typename T>
struct Completion {
private:
  const DoutPrefixProvider *_dpp;
  lr::AioCompletion* _cur = nullptr;
  lr::AioCompletion* _super;
public:

  using Ptr = std::unique_ptr<T>;

  lr::AioCompletion* cur() const {
    return _cur;
  }
  lr::AioCompletion* super() const {
    return _super;
  }

  Completion(const DoutPrefixProvider *dpp, lr::AioCompletion* super) : _dpp(dpp), _super(super) {
    super->pc->get();
  }

  ~Completion() {
    if (_super) {
      _super->pc->put();
    }
    if (_cur)
      _cur->release();
    _super = nullptr;
    _cur = nullptr;
  }

  // The only times that aio_operate can return an error are:
  // 1. The completion contains a null pointer. This should just
  //    crash, and in our case it does.
  // 2. An attempt is made to write to a snapshot. RGW doesn't use
  //    snapshots, so we don't care.
  //
  // So we will just assert that initiating an Aio operation succeeds
  // and not worry about recovering.
  static lr::AioCompletion* call(Ptr&& p) {
    p->_cur = lr::Rados::aio_create_completion(static_cast<void*>(p.get()),
					       &cb);
    auto c = p->_cur;
    p.release();
    // coverity[leaked_storage:SUPPRESS]
    return c;
  }
  static void complete(Ptr&& p, int r) {
    auto c = p->_super;
    p->_super = nullptr;
    rgw_complete_aio_completion(c, r);
  }

  static void cb(lr::completion_t, void* arg) {
    auto t = static_cast<T*>(arg);
    auto r = t->_cur->get_return_value();
    t->_cur->release();
    t->_cur = nullptr;
    t->handle(t->_dpp, Ptr(t), r);
  }
};

}
