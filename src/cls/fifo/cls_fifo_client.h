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
            static constexpr uint64_t default_max_obj_size = 4 * 1024 * 1024;
            static constexpr uint64_t default_max_entry_size = 32 * 1024;
            std::string id;
            struct {
              std::string name;
              std::string ns;
            } pool;
            std::optional<std::string> oid_prefix;
            bool exclusive{false};
            uint64_t max_obj_size{default_max_obj_size};
            uint64_t max_entry_size{default_max_entry_size};
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
          CreateParams& max_obj_size(uint64_t max_obj_size) {
            state.max_obj_size = max_obj_size;
            return *this;
          }
          CreateParams& max_entry_size(uint64_t max_entry_size) {
            state.max_entry_size = max_entry_size;
            return *this;
          }
        };

        static int create(librados::ObjectWriteOperation *op,
                          const CreateParams& params);
      };
    } // namespace fifo
  }  // namespace cls
} // namespace rados
