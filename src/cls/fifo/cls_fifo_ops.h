// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 * Copyright (C) 2019 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "include/buffer.h"
#include "include/encoding.h"
#include "include/types.h"

#include "cls/fifo/cls_fifo_types.h"

namespace rados::cls::fifo::op {
struct create_meta
{
  std::string id;
  std::optional<objv> version;
  struct {
    std::string name;
    std::string ns;
  } pool;
  std::optional<std::string> oid_prefix;

  std::uint64_t max_part_size{0};
  std::uint64_t max_entry_size{0};

  bool exclusive{false};

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(version, bl);
    encode(pool.name, bl);
    encode(pool.ns, bl);
    encode(oid_prefix, bl);
    encode(max_part_size, bl);
    encode(max_entry_size, bl);
    encode(exclusive, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(version, bl);
    decode(pool.name, bl);
    decode(pool.ns, bl);
    decode(oid_prefix, bl);
    decode(max_part_size, bl);
    decode(max_entry_size, bl);
    decode(exclusive, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(create_meta)

struct get_meta
{
  std::optional<objv> version;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(version, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(version, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(get_meta)

struct get_meta_reply
{
  fifo::info info;
  std::uint32_t part_header_size{0};
  /* per entry extra data that is stored */
  std::uint32_t part_entry_overhead{0};

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(info, bl);
    encode(part_header_size, bl);
    encode(part_entry_overhead, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(info, bl);
    decode(part_header_size, bl);
    decode(part_entry_overhead, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(get_meta_reply)

struct update_meta
{
  objv version;

  std::optional<std::uint64_t> tail_part_num;
  std::optional<std::uint64_t> head_part_num;
  std::optional<std::uint64_t> min_push_part_num;
  std::optional<std::uint64_t> max_push_part_num;
  std::vector<journal_entry> journal_entries_add;
  std::vector<journal_entry> journal_entries_rm;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(version, bl);
    encode(tail_part_num, bl);
    encode(head_part_num, bl);
    encode(min_push_part_num, bl);
    encode(max_push_part_num, bl);
    encode(journal_entries_add, bl);
    encode(journal_entries_rm, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(version, bl);
    decode(tail_part_num, bl);
    decode(head_part_num, bl);
    decode(min_push_part_num, bl);
    decode(max_push_part_num, bl);
    decode(journal_entries_add, bl);
    decode(journal_entries_rm, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(update_meta)

struct init_part
{
  std::string tag;
  data_params params;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(tag, bl);
    encode(params, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(tag, bl);
    decode(params, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(init_part)

struct push_part
{
  std::string tag;
  std::deque<ceph::buffer::list> data_bufs;
  std::uint64_t total_len{0};

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(tag, bl);
    encode(data_bufs, bl);
    encode(total_len, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(tag, bl);
    decode(data_bufs, bl);
    decode(total_len, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(push_part)

struct trim_part
{
  std::optional<std::string> tag;
  std::uint64_t ofs{0};
  bool exclusive = false;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(tag, bl);
    encode(ofs, bl);
    encode(exclusive, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(tag, bl);
    decode(ofs, bl);
    decode(exclusive, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(trim_part)

struct list_part
{
  std::optional<string> tag;
  std::uint64_t ofs{0};
  int max_entries{100};

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(tag, bl);
    encode(ofs, bl);
    encode(max_entries, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(tag, bl);
    decode(ofs, bl);
    decode(max_entries, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(list_part)
inline constexpr int MAX_LIST_ENTRIES = 512;

struct list_part_reply
{
  std::string tag;
  std::vector<part_list_entry> entries;
  bool more{false};
  bool full_part{false}; /* whether part is full or still can be written to.
                            A non full part is by definition head part */

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(tag, bl);
    encode(entries, bl);
    encode(more, bl);
    encode(full_part, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(tag, bl);
    decode(entries, bl);
    decode(more, bl);
    decode(full_part, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(list_part_reply)

struct get_part_info
{
  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START(1, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(get_part_info)

struct get_part_info_reply
{
  part_header header;

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    encode(header, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(header, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(get_part_info_reply)

inline constexpr auto CLASS = "fifo";
inline constexpr auto CREATE_META = "create_meta";
inline constexpr auto GET_META = "get_meta";
inline constexpr auto UPDATE_META = "update_meta";
inline constexpr auto INIT_PART = "init_part";
inline constexpr auto PUSH_PART = "push_part";
inline constexpr auto TRIM_PART = "trim_part";
inline constexpr auto LIST_PART = "part_list";
inline constexpr auto GET_PART_INFO = "get_part_info";
} // namespace rados::cls::fifo::op
