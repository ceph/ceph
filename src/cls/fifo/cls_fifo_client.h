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

        struct MetaCreateParams {
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

          MetaCreateParams& id(const std::string& id) {
            state.id = id;
            return *this;
          }
          MetaCreateParams& oid_prefix(const std::string& oid_prefix) {
            state.oid_prefix = oid_prefix;
            return *this;
          }
          MetaCreateParams& exclusive(bool exclusive) {
            state.exclusive = exclusive;
            return *this;
          }
          MetaCreateParams& max_obj_size(uint64_t max_obj_size) {
            state.max_obj_size = max_obj_size;
            return *this;
          }
          MetaCreateParams& max_entry_size(uint64_t max_entry_size) {
            state.max_entry_size = max_entry_size;
            return *this;
          }
          MetaCreateParams& objv(const std::string& instance, uint64_t ver) {
            state.objv = fifo_objv_t{instance, ver};
            return *this;
          }
        };

        static int meta_create(librados::ObjectWriteOperation *op,
                               const MetaCreateParams& params);

        /* get info */

        struct MetaGetParams {
          struct State {
            std::optional<fifo_objv_t> objv;
          } state;

          MetaGetParams& objv(const fifo_objv_t& v) {
            state.objv = v;
            return *this;
          }
          MetaGetParams& objv(const std::string& instance, uint64_t ver) {
            state.objv = fifo_objv_t{instance, ver};
            return *this;
          }
        };
        static int meta_get(librados::IoCtx& ioctx,
                            const string& oid,
                            const MetaGetParams& params,
                            rados::cls::fifo::fifo_info_t *result);

        /* update */

        struct MetaUpdateParams {
          struct State {
            rados::cls::fifo::fifo_objv_t objv;

            std::optional<uint64_t> tail_obj_num;
            std::optional<uint64_t> head_obj_num;
            std::optional<string> head_tag;
            std::optional<rados::cls::fifo::fifo_prepare_status_t> head_prepare_status;
          } state;

          MetaUpdateParams& objv(const fifo_objv_t& objv) {
            state.objv = objv;
            return *this;
          }
          MetaUpdateParams& tail_obj_num(uint64_t tail_obj_num) {
            state.tail_obj_num = tail_obj_num;
            return *this;
          }
          MetaUpdateParams& head_obj_num(uint64_t head_obj_num) {
            state.head_obj_num = head_obj_num;
            return *this;
          }
          MetaUpdateParams& head_tag(const std::string& head_tag) {
            state.head_tag = head_tag;
            return *this;
          }
          MetaUpdateParams& head_prepare_status(const rados::cls::fifo::fifo_prepare_status_t& head_prepare_status) {
            state.head_prepare_status = head_prepare_status;
            return *this;
          }
        };

        static int meta_update(librados::ObjectWriteOperation *rados_op,
                                const MetaUpdateParams& params);
        /* init part */

        struct PartInitParams {
          struct State {
            string tag;
            rados::cls::fifo::fifo_data_params_t data_params;
          } state;

          PartInitParams& tag(const std::string& tag) {
            state.tag = tag;
            return *this;
          }
          PartInitParams& data_params(const rados::cls::fifo::fifo_data_params_t& data_params) {
            state.data_params = data_params;
            return *this;
          }
        };

        static int part_init(librados::ObjectWriteOperation *op,
                             const PartInitParams& params);

	/* push part */

        struct PushPartParams {
          struct State {
            string tag;
            bufferlist data;
          } state;

          PushPartParams& tag(const std::string& tag) {
            state.tag = tag;
            return *this;
          }
          PushPartParams& data(bufferlist& bl) {
            state.data = bl;
            return *this;
          }
        };

        static int push_part(librados::ObjectWriteOperation *op,
                             const PushPartParams& params);
      };
    } // namespace fifo
  }  // namespace cls
} // namespace rados
