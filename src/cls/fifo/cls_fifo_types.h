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

        string to_str() {
          char buf[instance.size() + 32];
          snprintf(buf, sizeof(buf), "%s{%lld}", instance.c_str(), (long long)ver);
          return string(buf);
        }
      };
      WRITE_CLASS_ENCODER(rados::cls::fifo::fifo_objv_t)

      struct fifo_data_params_t {
        uint64_t max_obj_size{0};
        uint64_t max_entry_size{0};
        uint64_t full_size_threshold{0};

        void encode(bufferlist &bl) const {
          ENCODE_START(1, 1, bl);
          encode(max_obj_size, bl);
          encode(max_entry_size, bl);
          encode(full_size_threshold, bl);
          ENCODE_FINISH(bl);
        }
        void decode(bufferlist::const_iterator &bl) {
          DECODE_START(1, bl);
          decode(max_obj_size, bl);
          decode(max_entry_size, bl);
          decode(full_size_threshold, bl);
          DECODE_FINISH(bl);
        }
        void dump(Formatter *f) const;
        void decode_json(JSONObj *obj);

        bool operator==(const fifo_data_params_t& rhs) const {
          return (max_obj_size == rhs.max_obj_size &&
                  max_entry_size == rhs.max_entry_size &&
                  full_size_threshold == rhs.full_size_threshold);
        }
      };
      WRITE_CLASS_ENCODER(rados::cls::fifo::fifo_data_params_t)

      struct fifo_prepare_status_t {
        enum Status {
          STATUS_INIT = 0,
          STATUS_PREPARE = 1,
          STATUS_COMPLETE = 2,
        } status{STATUS_INIT};
        uint64_t head_num;
        string head_tag;

        void encode(bufferlist &bl) const {
          ENCODE_START(1, 1, bl);
          encode((int)status, bl);
          encode(head_num, bl);
          encode(head_tag, bl);
          ENCODE_FINISH(bl);
        }
        void decode(bufferlist::const_iterator &bl) {
          DECODE_START(1, bl);
          int i;
          decode(i, bl);
          status = (Status)i;
          decode(head_num, bl);
          decode(head_tag, bl);
          DECODE_FINISH(bl);
        }
        void dump(Formatter *f) const;
      };
      WRITE_CLASS_ENCODER(rados::cls::fifo::fifo_prepare_status_t)

      struct fifo_info_t {
        string id;
        fifo_objv_t objv;
        struct {
          string name;
          string ns;
        } pool;
        string oid_prefix;
        fifo_data_params_t data_params;

        uint64_t tail_obj_num{0};
        uint64_t head_obj_num{0};
        string head_tag;

        fifo_prepare_status_t head_prepare_status;

        void encode(bufferlist &bl) const {
          ENCODE_START(1, 1, bl);
          encode(id, bl);
          encode(objv, bl);
          encode(pool.name, bl);
          encode(pool.ns, bl);
          encode(oid_prefix, bl);
          encode(data_params, bl);
          encode(tail_obj_num, bl);
          encode(head_obj_num, bl);
          encode(head_tag, bl);
          encode(head_prepare_status, bl);
          ENCODE_FINISH(bl);
        }
        void decode(bufferlist::const_iterator &bl) {
          DECODE_START(1, bl);
          decode(id, bl);
          decode(objv, bl);
          decode(pool.name, bl);
          decode(pool.ns, bl);
          decode(oid_prefix, bl);
          decode(data_params, bl);
          decode(tail_obj_num, bl);
          decode(head_obj_num, bl);
          decode(head_tag, bl);
          decode(head_prepare_status, bl);
          DECODE_FINISH(bl);
        }
        void dump(Formatter *f) const;
        void decode_json(JSONObj *obj);
      };
      WRITE_CLASS_ENCODER(rados::cls::fifo::fifo_info_t)
    }
  }
}

static inline ostream& operator<<(ostream& os, const rados::cls::fifo::fifo_objv_t& objv)
{
  return os << objv.instance << "{" << objv.ver << "}";
}

