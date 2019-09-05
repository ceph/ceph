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

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#pragma once

#include "cls/fifo/cls_fifo_types.h"

namespace rados {
  namespace cls {
    namespace fifo {

      class FIFO {
      public:
        struct CreateParams {
          struct State {
            std::string id;
            struct {
              std::string name;
              std::string ns;
            } pool;
            std::optional<std::string> oid_prefix;
            bool exclusive{false};
          } state;

          CreateParams& id(const std::string& id) {
            state.id = id;
            return *this;
          }
          CreateParams& pool(const std::string& name, std::optional<std::string> ns = nullopt) {
            state.pool.name = name;
            if (ns) {
              state.pool.ns = *ns;
            }
            return *this;
          }
          CreateParams& oid_prefix(const std::string& oid_prefix) {
            state.oid_prefix = oid_prefix;
            return *this;
          }
          CreateParams& exclusive(bool exclusive) {
            state.exclusive = exclusive;
            return *this;
          }
        };

        static int create(librados::ObjectWriteOperation *op,
                          const CreateParams& params);
      };
    } // namespace fifo
  }  // namespace cls
} // namespace rados
