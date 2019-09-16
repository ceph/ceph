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
      int FIFO::meta_create(librados::ObjectWriteOperation *rados_op,
                            const MetaCreateParams& params) {
        cls_fifo_meta_create_op op;

        auto& state = params.state;

        if (state.id.empty()) {
          return -EINVAL;
        }

        op.id = state.id;
        op.objv = state.objv;
        op.oid_prefix = state.oid_prefix;
        op.max_part_size = state.max_part_size;
        op.max_entry_size = state.max_entry_size;
        op.exclusive = state.exclusive;

        if (op.max_part_size == 0 ||
            op.max_entry_size == 0 ||
            op.max_entry_size > op.max_part_size) {
          return -EINVAL;
        }

        bufferlist in;
        encode(op, in);
        rados_op->exec("fifo", "fifo_meta_create", in);

        return 0;
      }

      int FIFO::meta_get(librados::IoCtx& ioctx,
                         const string& oid,
                         const MetaGetParams& params,
                         fifo_info_t *result) {
        cls_fifo_meta_get_op op;

        auto& state = params.state;

        op.objv = state.objv;

        librados::ObjectReadOperation rop;

        bufferlist in;
        bufferlist out;
        int op_ret;
        encode(op, in);
        rop.exec("fifo", "fifo_meta_get", in, &out, &op_ret);

        int r = ioctx.operate(oid, &rop, nullptr);
        if (r < 0) {
          return r;
        }

        if (op_ret < 0) {
          return op_ret;
        }

        cls_fifo_meta_get_op_reply reply;
        auto iter = out.cbegin();
        try {
          decode(reply, iter);
        } catch (buffer::error& err) {
          return -EIO;
        }

        *result = reply.info;

        return 0;
      }

      int FIFO::meta_update(librados::ObjectWriteOperation *rados_op,
                             const MetaUpdateParams& params) {
        cls_fifo_meta_update_op op;

        auto& state = params.state;

        if (state.objv.empty()) {
          return -EINVAL;
        }

        op.objv = state.objv;
        op.tail_part_num = state.tail_part_num;
        op.head_part_num = state.head_part_num;
        op.head_tag = state.head_tag;
        op.head_prepare_status = state.head_prepare_status;

        bufferlist in;
        encode(op, in);
        rados_op->exec("fifo", "fifo_meta_update", in);

        return 0;
      }

      int FIFO::part_init(librados::ObjectWriteOperation *rados_op,
                          const PartInitParams& params) {
        cls_fifo_part_init_op op;

        auto& state = params.state;

        if (state.tag.empty()) {
          return -EINVAL;
        }

        op.tag = state.tag;
        op.data_params = state.data_params;

        bufferlist in;
        encode(op, in);
        rados_op->exec("fifo", "fifo_part_init", in);

        return 0;
      }

      int FIFO::push_part(librados::ObjectWriteOperation *rados_op,
                          const PushPartParams& params) {
        cls_fifo_part_push_op op;

        auto& state = params.state;

        if (state.tag.empty()) {
          return -EINVAL;
        }

        op.tag = state.tag;
        op.data = state.data;

        bufferlist in;
        encode(op, in);
        rados_op->exec("fifo", "fifo_push_part", in);

        return 0;
      }

    } // namespace fifo
  } // namespace cls
} // namespace rados

