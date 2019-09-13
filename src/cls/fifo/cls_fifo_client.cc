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

#include "include/rados/librados.hpp"

using namespace librados;

#include "cls/fifo/cls_fifo_ops.h"
#include "cls/fifo/cls_fifo_client.h"

namespace rados {
  namespace cls {
    namespace fifo {
      int FIFO::create(librados::ObjectWriteOperation *rados_op,
                       const CreateParams& params) {
        cls_fifo_create_op op;

        auto& state = params.state;

        if (state.id.empty() ||
            state.pool.name.empty()) {
          return -EINVAL;
        }

        op.id = state.id;
        op.objv = state.objv;
        op.pool.name = state.pool.name;
        op.pool.ns = state.pool.ns;
        op.oid_prefix = state.oid_prefix;
        op.max_obj_size = state.max_obj_size;
        op.max_entry_size = state.max_entry_size;
        op.exclusive = state.exclusive;

        if (op.max_obj_size == 0 ||
            op.max_entry_size == 0 ||
            op.max_entry_size > op.max_obj_size) {
          return -EINVAL;
        }

        bufferlist in;
        encode(op, in);
        rados_op->exec("fifo", "fifo_create", in);

        return 0;
      }

      int FIFO::get_info(librados::IoCtx& ioctx,
                         const string& oid,
                         const GetInfoParams& params,
                         fifo_info_t *result) {
        cls_fifo_get_info_op op;

        auto& state = params.state;

        op.objv = state.objv;

        librados::ObjectReadOperation rop;

        bufferlist in;
        bufferlist out;
        int op_ret;
        encode(op, in);
        rop.exec("fifo", "fifo_get_info", in, &out, &op_ret);

        int r = ioctx.operate(oid, &rop, nullptr);
        if (r < 0) {
          return r;
        }

        if (op_ret < 0) {
          return op_ret;
        }

        cls_fifo_get_info_op_reply reply;
        auto iter = out.cbegin();
        try {
          decode(reply, iter);
        } catch (buffer::error& err) {
          return -EIO;
        }

        *result = reply.info;

        return 0;
      }

      int FIFO::init_part(librados::ObjectWriteOperation *rados_op,
                       const InitPartParams& params) {
        cls_fifo_init_part_op op;

        auto& state = params.state;

        if (state.tag.empty()) {
          return -EINVAL;
        }

        op.tag = state.tag;
        op.data_params = state.data_params;

        bufferlist in;
        encode(op, in);
        rados_op->exec("fifo", "fifo_init_part", in);

        return 0;
      }

    } // namespace fifo
  } // namespace cls
} // namespace rados

