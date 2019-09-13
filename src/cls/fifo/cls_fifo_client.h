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

        /* create */

        struct CreateParams {
          struct State {
            static constexpr uint64_t default_max_obj_size = 4 * 1024 * 1024;
            static constexpr uint64_t default_max_entry_size = 32 * 1024;
            std::string id;
            std::optional<fifo_objv_t> objv;
            std::optional<std::string> oid_prefix;
            bool exclusive{false};
            uint64_t max_obj_size{default_max_obj_size};
            uint64_t max_entry_size{default_max_entry_size};
          } state;

          CreateParams& id(const std::string& id) {
            state.id = id;
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
          CreateParams& objv(const std::string& instance, uint64_t ver) {
            state.objv = fifo_objv_t{instance, ver};
            return *this;
          }
        };

        static int create(librados::ObjectWriteOperation *op,
                          const CreateParams& params);

        /* get info */

        struct GetInfoParams {
          struct State {
            std::optional<fifo_objv_t> objv;
          } state;

          GetInfoParams& objv(const fifo_objv_t& v) {
            state.objv = v;
            return *this;
          }
          GetInfoParams& objv(const std::string& instance, uint64_t ver) {
            state.objv = fifo_objv_t{instance, ver};
            return *this;
          }
        };
        static int get_info(librados::IoCtx& ioctx,
                            const string& oid,
                            const GetInfoParams& params,
                            rados::cls::fifo::fifo_info_t *result);

        /* update */

        struct UpdateStateParams {
          struct State {
            rados::cls::fifo::fifo_objv_t objv;

            std::optional<uint64_t> tail_obj_num;
            std::optional<uint64_t> head_obj_num;
            std::optional<string> head_tag;
            std::optional<rados::cls::fifo::fifo_prepare_status_t> head_prepare_status;
          } state;

          UpdateStateParams& objv(const fifo_objv_t& objv) {
            state.objv = objv;
            return *this;
          }
          UpdateStateParams& tail_obj_num(uint64_t tail_obj_num) {
            state.tail_obj_num = tail_obj_num;
            return *this;
          }
          UpdateStateParams& head_obj_num(uint64_t head_obj_num) {
            state.head_obj_num = head_obj_num;
            return *this;
          }
          UpdateStateParams& head_tag(const std::string& head_tag) {
            state.head_tag = head_tag;
            return *this;
          }
          UpdateStateParams& head_prepare_status(const rados::cls::fifo::fifo_prepare_status_t& head_prepare_status) {
            state.head_prepare_status = head_prepare_status;
            return *this;
          }
        };

        static int update_state(librados::ObjectWriteOperation *rados_op,
                                const UpdateStateParams& params);
        /* init part */

        struct InitPartParams {
          struct State {
            string tag;
            rados::cls::fifo::fifo_data_params_t data_params;
          } state;

          InitPartParams& tag(const std::string& tag) {
            state.tag = tag;
            return *this;
          }
          InitPartParams& data_params(const rados::cls::fifo::fifo_data_params_t& data_params) {
            state.data_params = data_params;
            return *this;
          }
        };

        static int init_part(librados::ObjectWriteOperation *op,
                             const InitPartParams& params);
      };
    } // namespace fifo
  }  // namespace cls
} // namespace rados
