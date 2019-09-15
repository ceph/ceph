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

struct cls_fifo_create_op
{
  string id;
  std::optional<rados::cls::fifo::fifo_objv_t> objv;
  struct {
    string name;
    string ns;
  } pool;
  std::optional<string> oid_prefix;

  uint64_t max_obj_size{0};
  uint64_t max_entry_size{0};

  bool exclusive{false};

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(objv, bl);
    encode(pool.name, bl);
    encode(pool.ns, bl);
    encode(oid_prefix, bl);
    encode(max_obj_size, bl);
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
    decode(max_obj_size, bl);
    decode(max_entry_size, bl);
    decode(exclusive, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_fifo_create_op)

struct cls_fifo_get_info_op
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
WRITE_CLASS_ENCODER(cls_fifo_get_info_op)

struct cls_fifo_get_info_op_reply
{
  rados::cls::fifo::fifo_info_t info;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(info, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(info, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_fifo_get_info_op_reply)

struct cls_fifo_init_part_op
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
WRITE_CLASS_ENCODER(cls_fifo_init_part_op)

struct cls_fifo_part_push_op
{
  string tag;
  bufferlist data;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(tag, bl);
    encode(data, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(tag, bl);
    decode(data, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_fifo_part_push_op)

struct cls_fifo_update_state_op
{
  rados::cls::fifo::fifo_objv_t objv;

  std::optional<uint64_t> tail_obj_num;
  std::optional<uint64_t> head_obj_num;
  std::optional<string> head_tag;
  std::optional<rados::cls::fifo::fifo_prepare_status_t> head_prepare_status;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(objv, bl);
    encode(tail_obj_num, bl);
    encode(head_obj_num, bl);
    encode(head_tag, bl);
    encode(head_prepare_status, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(objv, bl);
    decode(tail_obj_num, bl);
    decode(head_obj_num, bl);
    decode(head_tag, bl);
    decode(head_prepare_status, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_fifo_update_state_op)
