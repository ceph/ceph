// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once


#include "include/encoding.h"
#include "include/types.h"


class JSONObj;

namespace rados {
  namespace cls {
    namespace fifo {
      struct fifo_objv_t {
        string instance;
        uint64_t ver{0};

        void encode(bufferlist &bl) const {
          ENCODE_START(1, 1, bl);
          encode(instance, bl);
          encode(ver, bl);
          ENCODE_FINISH(bl);
        }
        void decode(bufferlist::const_iterator &bl) {
          DECODE_START(1, bl);
          decode(instance, bl);
          decode(ver, bl);
          DECODE_FINISH(bl);
        }
        void dump(Formatter *f) const;
        void decode_json(JSONObj *obj);

        bool operator==(const fifo_objv_t& rhs) const {
          return (instance == rhs.instance &&
                  ver == rhs.ver);
        }

        bool empty() const {
          return instance.empty();
        }

        string to_str() {
          char buf[instance.size() + 32];
          snprintf(buf, sizeof(buf), "%s{%lld}", instance.c_str(), (long long)ver);
          return string(buf);
        }
      };
      WRITE_CLASS_ENCODER(rados::cls::fifo::fifo_objv_t)

      struct fifo_data_params_t {
        uint64_t max_part_size{0};
        uint64_t max_entry_size{0};
        uint64_t full_size_threshold{0};

        void encode(bufferlist &bl) const {
          ENCODE_START(1, 1, bl);
          encode(max_part_size, bl);
          encode(max_entry_size, bl);
          encode(full_size_threshold, bl);
          ENCODE_FINISH(bl);
        }
        void decode(bufferlist::const_iterator &bl) {
          DECODE_START(1, bl);
          decode(max_part_size, bl);
          decode(max_entry_size, bl);
          decode(full_size_threshold, bl);
          DECODE_FINISH(bl);
        }
        void dump(Formatter *f) const;
        void decode_json(JSONObj *obj);

        bool operator==(const fifo_data_params_t& rhs) const {
          return (max_part_size == rhs.max_part_size &&
                  max_entry_size == rhs.max_entry_size &&
                  full_size_threshold == rhs.full_size_threshold);
        }
      };
      WRITE_CLASS_ENCODER(rados::cls::fifo::fifo_data_params_t)

      struct fifo_journal_entry_t {
        enum Op {
          OP_UNKNOWN  = 0,
          OP_CREATE   = 1,
          OP_SET_HEAD = 2,
          OP_REMOVE   = 3,
        } op{OP_UNKNOWN};

        int64_t part_num{0};
        string part_tag;

        void encode(bufferlist &bl) const {
          ENCODE_START(1, 1, bl);
          encode((int)op, bl);
          encode(part_num, bl);
          encode(part_tag, bl);
          ENCODE_FINISH(bl);
        }
        void decode(bufferlist::const_iterator &bl) {
          DECODE_START(1, bl);
          int i;
          decode(i, bl);
          op = (Op)i;
          decode(part_num, bl);
          decode(part_tag, bl);
          DECODE_FINISH(bl);
        }
        void dump(Formatter *f) const;

        bool operator==(const fifo_journal_entry_t& e) {
          return (op == e.op &&
                  part_num == e.part_num &&
                  part_tag == e.part_tag);
        }
      };
      WRITE_CLASS_ENCODER(rados::cls::fifo::fifo_journal_entry_t)

      struct fifo_info_t {
        string id;
        fifo_objv_t objv;
        string oid_prefix;
        fifo_data_params_t data_params;

        int64_t tail_part_num{0};
        int64_t head_part_num{-1};
        int64_t min_push_part_num{0};
        int64_t max_push_part_num{-1};

        string head_tag;
        map<int64_t, string> tags;

        std::multimap<int64_t, fifo_journal_entry_t> journal;

        bool need_new_head() {
          return (head_part_num < min_push_part_num);
        }

        bool need_new_part() {
          return (max_push_part_num < min_push_part_num);
        }

        string part_oid(int64_t part_num);
        void prepare_next_journal_entry(fifo_journal_entry_t *entry, const string& tag);

        int apply_update(std::optional<uint64_t>& _tail_part_num,
                         std::optional<uint64_t>& _head_part_num,
                         std::optional<uint64_t>& _min_push_part_num,
                         std::optional<uint64_t>& _max_push_part_num,
                         std::vector<rados::cls::fifo::fifo_journal_entry_t>& journal_entries_add,
                         std::vector<rados::cls::fifo::fifo_journal_entry_t>& journal_entries_rm,
                         string *err);

        void encode(bufferlist &bl) const {
          ENCODE_START(1, 1, bl);
          encode(id, bl);
          encode(objv, bl);
          encode(oid_prefix, bl);
          encode(data_params, bl);
          encode(tail_part_num, bl);
          encode(head_part_num, bl);
          encode(min_push_part_num, bl);
          encode(max_push_part_num, bl);
          encode(tags, bl);
          encode(head_tag, bl);
          encode(journal, bl);
          ENCODE_FINISH(bl);
        }
        void decode(bufferlist::const_iterator &bl) {
          DECODE_START(1, bl);
          decode(id, bl);
          decode(objv, bl);
          decode(oid_prefix, bl);
          decode(data_params, bl);
          decode(tail_part_num, bl);
          decode(head_part_num, bl);
          decode(min_push_part_num, bl);
          decode(max_push_part_num, bl);
          decode(tags, bl);
          decode(head_tag, bl);
          decode(journal, bl);
          DECODE_FINISH(bl);
        }
        void dump(Formatter *f) const;
        void decode_json(JSONObj *obj);
      };
      WRITE_CLASS_ENCODER(rados::cls::fifo::fifo_info_t)

      struct cls_fifo_part_list_entry_t {
        bufferlist data;
        uint64_t ofs;
        ceph::real_time mtime;

        cls_fifo_part_list_entry_t() {}
        cls_fifo_part_list_entry_t(bufferlist&& _data,
                                   uint64_t _ofs,
                                   ceph::real_time _mtime) : data(std::move(_data)), ofs(_ofs), mtime(_mtime) {}


        void encode(bufferlist &bl) const {
          ENCODE_START(1, 1, bl);
          encode(data, bl);
          encode(ofs, bl);
          encode(mtime, bl);
          ENCODE_FINISH(bl);
        }
        void decode(bufferlist::const_iterator &bl) {
          DECODE_START(1, bl);
          decode(data, bl);
          decode(ofs, bl);
          decode(mtime, bl);
          DECODE_FINISH(bl);
        }
      };
      WRITE_CLASS_ENCODER(rados::cls::fifo::cls_fifo_part_list_entry_t)

      struct fifo_part_header_t {
        string tag;

        fifo_data_params_t params;

        uint64_t magic{0};

        uint64_t min_ofs{0};
        uint64_t max_ofs{0};
        uint64_t min_index{0};
        uint64_t max_index{0};

        void encode(bufferlist &bl) const {
          ENCODE_START(1, 1, bl);
          encode(tag, bl);
          encode(params, bl);
          encode(magic, bl);
          encode(min_ofs, bl);
          encode(max_ofs, bl);
          encode(min_index, bl);
          encode(max_index, bl);
          ENCODE_FINISH(bl);
        }
        void decode(bufferlist::const_iterator &bl) {
          DECODE_START(1, bl);
          decode(tag, bl);
          decode(params, bl);
          decode(magic, bl);
          decode(min_ofs, bl);
          decode(max_ofs, bl);
          decode(min_index, bl);
          decode(max_index, bl);
          DECODE_FINISH(bl);
        }
      };
      WRITE_CLASS_ENCODER(fifo_part_header_t)

    }
  }
}

static inline ostream& operator<<(ostream& os, const rados::cls::fifo::fifo_objv_t& objv)
{
  return os << objv.instance << "{" << objv.ver << "}";
}

