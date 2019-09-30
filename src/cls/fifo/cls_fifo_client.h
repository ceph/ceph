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
            static constexpr uint64_t default_max_part_size = 4 * 1024 * 1024;
            static constexpr uint64_t default_max_entry_size = 32 * 1024;
            std::string id;
            std::optional<fifo_objv_t> objv;
            std::optional<std::string> oid_prefix;
            bool exclusive{false};
            uint64_t max_part_size{default_max_part_size};
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
          MetaCreateParams& max_part_size(uint64_t max_part_size) {
            state.max_part_size = max_part_size;
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

          MetaGetParams& objv(std::optional<fifo_objv_t>& v) {
            state.objv = v;
            return *this;
          }
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

            std::optional<uint64_t> tail_part_num;
            std::optional<uint64_t> head_part_num;
            std::optional<uint64_t> min_push_part_num;
            std::optional<uint64_t> max_push_part_num;
            std::vector<rados::cls::fifo::fifo_journal_entry_t> journal_entries_add;
            std::vector<rados::cls::fifo::fifo_journal_entry_t> journal_entries_rm;
          } state;

          MetaUpdateParams& objv(const fifo_objv_t& objv) {
            state.objv = objv;
            return *this;
          }
          MetaUpdateParams& tail_part_num(uint64_t tail_part_num) {
            state.tail_part_num = tail_part_num;
            return *this;
          }
          MetaUpdateParams& head_part_num(uint64_t head_part_num) {
            state.head_part_num = head_part_num;
            return *this;
          }
          MetaUpdateParams& min_push_part_num(uint64_t num) {
            state.min_push_part_num = num;
            return *this;
          }
          MetaUpdateParams& max_push_part_num(uint64_t num) {
            state.max_push_part_num = num;
            return *this;
          }
          MetaUpdateParams& journal_entry_add(const rados::cls::fifo::fifo_journal_entry_t& entry) {
            state.journal_entries_add.push_back(entry);
            return *this;
          }
          MetaUpdateParams& journal_entries_rm(std::vector<rados::cls::fifo::fifo_journal_entry_t>& entries) {
            state.journal_entries_rm = entries;
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
	/* trim part */

        struct TrimPartParams {
          struct State {
            std::optional<string> tag;
            uint64_t ofs;
          } state;

          TrimPartParams& tag(std::optional<std::string> tag) {
            state.tag = tag;
            return *this;
          }
          TrimPartParams& ofs(uint64_t ofs) {
            state.ofs = ofs;
            return *this;
          }
        };

        static int trim_part(librados::ObjectWriteOperation *op,
                             const TrimPartParams& params);
	/* list part */

        struct ListPartParams {
          struct State {
            std::optional<string> tag;
            uint64_t ofs;
            int max_entries{100};
          } state;

          ListPartParams& tag(const std::string& tag) {
            state.tag = tag;
            return *this;
          }
          ListPartParams& ofs(uint64_t ofs) {
            state.ofs = ofs;
            return *this;
          }
          ListPartParams& max_entries(int _max_entries) {
            state.max_entries = _max_entries;
            return *this;
          }
        };

        static int list_part(librados::IoCtx& ioctx,
                             const string& oid,
                             const ListPartParams& params,
                             std::vector<cls_fifo_part_list_op_reply::entry> *pentries,
                             bool *more,
                             string *ptag = nullptr);
      };

      struct fifo_entry {
        bufferlist data;
        string marker;
        ceph::real_time mtime;
      };

      class Manager {
        CephContext *cct;
        string id;

        string meta_oid;

        std::optional<librados::IoCtx> _ioctx;
        librados::IoCtx *ioctx{nullptr};

        fifo_info_t meta_info;

        bool is_open{false};

        string craft_marker(int64_t part_num,
                        uint64_t part_ofs);

        bool parse_marker(const string& marker,
                          int64_t *part_num,
                          uint64_t *part_ofs);

        int update_meta(FIFO::MetaUpdateParams& update_params,
                        bool *canceled);
        int read_meta(std::optional<fifo_objv_t> objv = std::nullopt);

        int create_part(int64_t part_num, const string& tag);
        int remove_part(int64_t part_num, const string& tag);

        int process_journal_entry(const fifo_journal_entry_t& entry);
        int process_journal_entries(vector<fifo_journal_entry_t> *processed);
        int process_journal();

        int prepare_new_part();
        int prepare_new_head();

        int push_entry(int64_t part_num, bufferlist& bl);
        int trim_part(int64_t part_num,
                      uint64_t ofs,
                      std::optional<string> tag);

      public:
        Manager(CephContext *_cct,
                const string& _id) : cct(_cct),
                                    id(_id) {
          meta_oid = id;
        }

        int init_ioctx(librados::Rados *rados,
                       const string& pool,
                       std::optional<string> pool_ns);

        int init_ioctx(librados::IoCtx *_ioctx) {
          ioctx = ioctx;
          return 0;
        }

        int open(bool create,
                 std::optional<FIFO::MetaCreateParams> create_params = std::nullopt);

        int push(bufferlist& bl);

        int list(int max_entries,
                 std::optional<string> marker,
                 vector<fifo_entry> *result,
                 bool *more);

        int trim(const string& marker);
      };
    } // namespace fifo
  }  // namespace cls
} // namespace rados
