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

  std::string to_string() {
    return fmt::format("{}:{}", num, ofs);
  }
};

struct list_entry {
  cb::list data;
  std::string marker;
  ceph::real_time mtime;
};

using part_info = fifo::part_header;

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

  static int open(lr::IoCtx ioctx, std::string oid,
		  std::unique_ptr<FIFO>* fifo, optional_yield y,
		  std::optional<fifo::objv> objv = std::nullopt);
  static int create(lr::IoCtx ioctx, std::string oid, std::unique_ptr<FIFO>* fifo,
		    optional_yield y,
		    std::optional<fifo::objv> objv = std::nullopt,
		    std::optional<std::string_view> oid_prefix = std::nullopt,
		    bool exclusive = false,
		    std::uint64_t max_part_size = default_max_part_size,
		    std::uint64_t max_entry_size = default_max_entry_size);

  int read_meta(optional_yield y);
  int read_meta(lr::AioCompletion* c);
  const fifo::info& meta() const;
  std::pair<std::uint32_t, std::uint32_t> get_part_layout_info() const;
  int push(const cb::list& bl, optional_yield y);
  int push(const std::vector<cb::list>& data_bufs, optional_yield y);
  int list(int max_entries,
	   std::optional<std::string_view> markstr,
	   std::vector<list_entry>* out, bool* more,
	   optional_yield y);
  int trim(std::string_view markstr, optional_yield y);
  int trim(std::string_view markstr, lr::AioCompletion* c);
  int get_part_info(int64_t part_num, fifo::part_header* header,
		    optional_yield y);
};
}

#endif // CEPH_RGW_CLS_FIFO_LEGACY_H
