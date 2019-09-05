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

#include "include/types.h"
#include "include/utime.h"
#include "cls/fifo/cls_fifo_types.h"

struct cls_fifo_meta_create_op
{
  string id;
  std::optional<rados::cls::fifo::fifo_objv_t> objv;
  struct {
    string name;
    string ns;
  } pool;
  std::optional<string> oid_prefix;

  uint64_t max_part_size{0};
  uint64_t max_entry_size{0};

  bool exclusive{false};

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(objv, bl);
    encode(pool.name, bl);
    encode(pool.ns, bl);
    encode(oid_prefix, bl);
    encode(max_part_size, bl);
    encode(max_entry_size, bl);
    encode(exclusive, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(objv, bl);
    decode(pool.name, bl);
    decode(pool.ns, bl);
    decode(oid_prefix, bl);
    decode(max_part_size, bl);
    decode(max_entry_size, bl);
    decode(exclusive, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_fifo_meta_create_op)

struct cls_fifo_meta_get_op
{
  std::optional<rados::cls::fifo::fifo_objv_t> objv;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(objv, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(objv, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_fifo_meta_get_op)

struct cls_fifo_meta_get_op_reply
{
  rados::cls::fifo::fifo_info_t info;
  uint32_t part_header_size{0};
  uint32_t part_entry_overhead{0}; /* per entry extra data that is stored */

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(info, bl);
    encode(part_header_size, bl);
    encode(part_entry_overhead, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(info, bl);
    decode(part_header_size, bl);
    decode(part_entry_overhead, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_fifo_meta_get_op_reply)

struct cls_fifo_meta_update_op
{
  rados::cls::fifo::fifo_objv_t objv;

  std::optional<uint64_t> tail_part_num;
  std::optional<uint64_t> head_part_num;
  std::optional<uint64_t> min_push_part_num;
  std::optional<uint64_t> max_push_part_num;
  std::vector<rados::cls::fifo::fifo_journal_entry_t> journal_entries_add;
  std::vector<rados::cls::fifo::fifo_journal_entry_t> journal_entries_rm;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(objv, bl);
    encode(tail_part_num, bl);
    encode(head_part_num, bl);
    encode(min_push_part_num, bl);
    encode(max_push_part_num, bl);
    encode(journal_entries_add, bl);
    encode(journal_entries_rm, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(objv, bl);
    decode(tail_part_num, bl);
    decode(head_part_num, bl);
    decode(min_push_part_num, bl);
    decode(max_push_part_num, bl);
    decode(journal_entries_add, bl);
    decode(journal_entries_rm, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_fifo_meta_update_op)

struct cls_fifo_part_init_op
{
  string tag;
  rados::cls::fifo::fifo_data_params_t data_params;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(tag, bl);
    encode(data_params, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(tag, bl);
    decode(data_params, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_fifo_part_init_op)

struct cls_fifo_part_push_op
{
  string tag;
  std::vector<bufferlist> data_bufs;
  uint64_t total_len{0};

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(tag, bl);
    encode(data_bufs, bl);
    encode(total_len, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(tag, bl);
    decode(data_bufs, bl);
    decode(total_len, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_fifo_part_push_op)

struct cls_fifo_part_trim_op
{
  std::optional<string> tag;
  uint64_t ofs{0};

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(tag, bl);
    encode(ofs, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(tag, bl);
    decode(ofs, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_fifo_part_trim_op)

struct cls_fifo_part_list_op
{
  std::optional<string> tag;
  uint64_t ofs{0};
  int max_entries{100};

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(tag, bl);
    encode(ofs, bl);
    encode(max_entries, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(tag, bl);
    decode(ofs, bl);
    decode(max_entries, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_fifo_part_list_op)

struct cls_fifo_part_list_op_reply
{
  string tag;
  vector<rados::cls::fifo::cls_fifo_part_list_entry_t> entries;
  bool more{false};
  bool full_part{false}; /* whether part is full or still can be written to.
                            A non full part is by definition head part */

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(tag, bl);
    encode(entries, bl);
    encode(more, bl);
    encode(full_part, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(tag, bl);
    decode(entries, bl);
    decode(more, bl);
    decode(full_part, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_fifo_part_list_op_reply)

struct cls_fifo_part_get_info_op
{
  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_fifo_part_get_info_op)

struct cls_fifo_part_get_info_op_reply
{
  rados::cls::fifo::fifo_part_header_t header;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(header, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(header, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_fifo_part_get_info_op_reply)
