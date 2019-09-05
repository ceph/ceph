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
        op.pool.name = state.pool.name;
        op.pool.ns = state.pool.ns;
        op.oid_prefix = state.oid_prefix;
        op.exclusive = state.exclusive;

        bufferlist in;
        encode(op, in);
        rados_op->exec("fifo", "fifo_create", in);

        return 0;
      }
    } // namespace fifo
  } // namespace cls
} // namespace rados

