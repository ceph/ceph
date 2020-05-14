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

#ifndef CEPH_RGW_CLS_FIFO_LEGACY_H
#define CEPH_RGW_CLS_FIFO_LEGACY_H

#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <vector>

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/format.h>

#include "include/rados/librados.hpp"
#include "include/buffer.h"

#include "common/async/yield_context.h"

#include "cls/fifo/cls_fifo_types.h"
#include "cls/fifo/cls_fifo_ops.h"

namespace rgw::cls::fifo {
namespace cb = ceph::buffer;
namespace fifo = rados::cls::fifo;
namespace lr = librados;

inline constexpr std::uint64_t default_max_part_size = 4 * 1024 * 1024;
inline constexpr std::uint64_t default_max_entry_size = 32 * 1024;

void create_meta(lr::ObjectWriteOperation* op, std::string_view id,
		 std::optional<fifo::objv> objv,
		 std::optional<std::string_view> oid_prefix,
		 bool exclusive = false,
		 std::uint64_t max_part_size = default_max_part_size,
		 std::uint64_t max_entry_size = default_max_entry_size);
int get_meta(lr::IoCtx& ioctx, const std::string& oid,
	     std::optional<fifo::objv> objv, fifo::info* info,
	     std::uint32_t* part_header_size,
	     std::uint32_t* part_entry_overhead, optional_yield y);
void update_meta(lr::ObjectWriteOperation* op, const fifo::objv& objv,
		 const fifo::update& update);
void part_init(lr::ObjectWriteOperation* op, std::string_view tag,
	       fifo::data_params params);
int push_part(lr::IoCtx& ioctx, const std::string& oid, std::string_view tag,
	      std::deque<cb::list> data_bufs, optional_yield y);
void trim_part(lr::ObjectWriteOperation* op,
	       std::optional<std::string_view> tag, std::uint64_t ofs);
int list_part(lr::IoCtx& ioctx, const std::string& oid,
	      std::optional<std::string_view> tag, std::uint64_t ofs,
	      std::uint64_t max_entries,
	      std::vector<fifo::part_list_entry>* entries,
	      bool* more, bool* full_part, std::string* ptag,
	      optional_yield y);
int get_part_info(lr::IoCtx& ioctx, const std::string& oid,
		  fifo::part_header* header, optional_yield y);

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
/// contains either a spawn::yield_context (in which case the current
/// coroutine is suspended until completion) or null_yield (in which
/// case the current thread is blocked until completion.)
///
/// Please see the librados documentation for information on
/// AioCompletion and IoCtx.

class FIFO {
  friend struct Reader;
  friend struct Updater;
  friend struct Trimmer;

  mutable lr::IoCtx ioctx;
  const std::string oid;
  std::mutex m;

  fifo::info info;

  std::uint32_t part_header_size = 0xdeadbeef;
  std::uint32_t part_entry_overhead = 0xdeadbeef;

  std::optional<marker> to_marker(std::string_view s);

  FIFO(lr::IoCtx&& ioc,
       std::string oid)
    : ioctx(std::move(ioc)), oid(oid) {}

  std::string generate_tag() const;

  int apply_update(fifo::info* info,
		   const fifo::objv& objv,
		   const fifo::update& update);
  int _update_meta(const fifo::update& update,
		   fifo::objv version, bool* pcanceled,
		   optional_yield y);
  int _update_meta(const fifo::update& update,
		   fifo::objv version, bool* pcanceled,
		   lr::AioCompletion* c);
  int create_part(int64_t part_num, std::string_view tag, optional_yield y);
  int remove_part(int64_t part_num, std::string_view tag, optional_yield y);
  int process_journal(optional_yield y);
  int _prepare_new_part(bool is_head, optional_yield y);
  int _prepare_new_head(optional_yield y);
  int push_entries(const std::deque<cb::list>& data_bufs,
		   optional_yield y);
  int trim_part(int64_t part_num, uint64_t ofs,
		std::optional<std::string_view> tag, optional_yield y);
  int trim_part(int64_t part_num, uint64_t ofs,
		std::optional<std::string_view> tag, lr::AioCompletion* c);

  static void trim_callback(lr::completion_t, void* arg);
  static void update_callback(lr::completion_t, void* arg);
  static void read_callback(lr::completion_t, void* arg);

public:

  FIFO(const FIFO&) = delete;
  FIFO& operator =(const FIFO&) = delete;
  FIFO(FIFO&&) = delete;
  FIFO& operator =(FIFO&&) = delete;

  /// Open an existing FIFO.
  static int open(lr::IoCtx ioctx, //< IO Context
		  std::string oid, //< OID for metadata object
		  std::unique_ptr<FIFO>* fifo, //< OUT: Pointer to FIFO object
		  optional_yield y, //< Optional yield context
		  /// Operation will fail if FIFO is not at this version
		  std::optional<fifo::objv> objv = std::nullopt);
  /// Create a new or open an existing FIFO.
  static int create(lr::IoCtx ioctx, //< IO Context
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
  int read_meta(optional_yield y);
  /// Force refresh of metadata, with a librados Completion
  int read_meta(lr::AioCompletion* c);
  /// Get currently known metadata
  const fifo::info& meta() const;
  /// Get partition header and entry overhead size
  std::pair<std::uint32_t, std::uint32_t> get_part_layout_info() const;
  /// Push an entry to the FIFO
  int push(const cb::list& bl, //< Entry to push
	   optional_yield y //< Optional yield
    );
  /// Push entres to the FIFO
  int push(const std::vector<cb::list>& data_bufs, //< Entries to push
	   /// Optional yield
	   optional_yield y);
  /// List entries
  int list(int max_entries, /// Maximum entries to list
	   /// Point after which to begin listing. Start at tail if null
	   std::optional<std::string_view> markstr,
	   std::vector<list_entry>* out, //< OUT: entries
	   /// OUT: True if more entries in FIFO beyond the last returned
	   bool* more,
	   optional_yield y //< Optional yield
    );
  /// Trim entries, coroutine/block style
  int trim(std::string_view markstr, //< Position to which to trim, inclusive
	   optional_yield y //< Optional yield
    );
  /// Trim entries, librados AioCompletion style
  int trim(std::string_view markstr, //< Position to which to trim, inclusive
	   lr::AioCompletion* c //< librados AIO Completion
    );
  /// Get part info
  int get_part_info(int64_t part_num, /// Part number
		    fifo::part_header* header, //< OUT: Information
		    optional_yield y //< Optional yield
    );
};
}

#endif // CEPH_RGW_CLS_FIFO_LEGACY_H
