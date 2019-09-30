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
#include "common/dout.h"

using namespace librados;

#include "cls/fifo/cls_fifo_ops.h"
#include "cls/fifo/cls_fifo_client.h"


#define dout_subsys ceph_subsys_objclass


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
        op.min_push_part_num = state.min_push_part_num;
        op.max_push_part_num = state.max_push_part_num;
        op.journal_entries_add = state.journal_entries_add;
        op.journal_entries_rm = state.journal_entries_rm;

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
        rados_op->exec("fifo", "fifo_part_push", in);

        return 0;
      }

      int FIFO::trim_part(librados::ObjectWriteOperation *rados_op,
                          const TrimPartParams& params) {
        cls_fifo_part_trim_op op;

        auto& state = params.state;

        op.tag = state.tag;
        op.ofs = state.ofs;

        bufferlist in;
        encode(op, in);
        rados_op->exec("fifo", "fifo_part_trim", in);

        return 0;
      }

      int FIFO::list_part(librados::IoCtx& ioctx,
                          const string& oid,
                          const ListPartParams& params,
                          std::vector<cls_fifo_part_list_op_reply::entry> *pentries,
                          bool *more,
                          string *ptag)
      {
        cls_fifo_part_list_op op;

        auto& state = params.state;

        op.tag = state.tag;
        op.ofs = state.ofs;
        op.max_entries = state.max_entries;

        librados::ObjectReadOperation rop;

        bufferlist in;
        bufferlist out;
        int op_ret;
        encode(op, in);
        rop.exec("fifo", "fifo_part_list", in, &out, &op_ret);

        int r = ioctx.operate(oid, &rop, nullptr);
        if (r < 0) {
          return r;
        }

        if (op_ret < 0) {
          return op_ret;
        }

        cls_fifo_part_list_op_reply reply;
        auto iter = out.cbegin();
        try {
          decode(reply, iter);
        } catch (buffer::error& err) {
          return -EIO;
        }

        if (pentries) {
          *pentries = std::move(reply.entries);
        }

        if (more) {
          *more = reply.more;
        }

        if (ptag) {
          *ptag = reply.tag;
        }

        return 0;
      }

      string Manager::craft_marker(int64_t part_num,
                                   uint64_t part_ofs)
      {
        char buf[64];
        snprintf(buf, sizeof(buf), "%lld:%lld", (long long)part_num, (long long)part_ofs);
        return string(buf);
      }

      bool Manager::parse_marker(const string& marker,
                                 int64_t *part_num,
                                 uint64_t *part_ofs)
      {
        if (marker.empty()) {
          *part_num = meta_info.tail_part_num;
          *part_ofs = 0;
          return true;
        }

        auto pos = marker.find(':');
        if (pos == string::npos) {
          return false;
        }

        auto first = marker.substr(0, pos);
        auto second = marker.substr(pos + 1);

        string err;

        *part_num = (int64_t)strict_strtoll(first.c_str(), 10, &err);
        if (!err.empty()) {
          return false;
        }

        *part_ofs = (uint64_t)strict_strtoll(second.c_str(), 10, &err);
        if (!err.empty()) {
          return false;
        }

        return true;
      }

      int Manager::init_ioctx(librados::Rados *rados,
                              const string& pool,
                              std::optional<string> pool_ns)
      {
        _ioctx.emplace();
        int r = rados->ioctx_create(pool.c_str(), *_ioctx);
        if (r < 0) {
          return r;
        }

        if (pool_ns && !pool_ns->empty()) {
          _ioctx->set_namespace(*pool_ns);
        }

        ioctx = &(*_ioctx);

        return 0;
      }

      int Manager::update_meta(FIFO::MetaUpdateParams& update_params,
                               bool *canceled)
      {
        update_params.objv(meta_info.objv);

        librados::ObjectWriteOperation wop;
        int r = FIFO::meta_update(&wop, update_params);
        if (r < 0) {
          return r;
        }

        r = ioctx->operate(meta_oid, &wop);
        if (r < 0 && r != -ECANCELED) {
          return r;
        }

        *canceled = (r == -ECANCELED);

        r = read_meta();
        if (r < 0) {
          return r;
        }

        return 0;
      }

      int Manager::read_meta(std::optional<fifo_objv_t> objv)
      {
        FIFO::MetaGetParams get_params;
        if (objv) {
          get_params.objv(*objv);
        }
        int r = FIFO::meta_get(*ioctx,
                               meta_oid,
                               get_params,
                               &meta_info);
        if (r < 0) {
          return r;
        }

        return 0;
      }

      int Manager::create_part(int64_t part_num, const string& tag) {
        librados::ObjectWriteOperation op;

        op.create(true); /* exclusive */
        int r = FIFO::part_init(&op,
                                FIFO::PartInitParams()
                                .tag(tag)
                                .data_params(meta_info.data_params));
        if (r < 0) {
          return r;
        }

        r = ioctx->operate(meta_info.part_oid(part_num), &op);
        if (r < 0) {
          return r;
        }

        return 0;
      }

      int Manager::remove_part(int64_t part_num, const string& tag) {
        librados::ObjectWriteOperation op;
        op.remove();
        int r = ioctx->operate(meta_info.part_oid(part_num), &op);
        if (r == -ENOENT) {
          r = 0;
        }
        if (r < 0) {
          return r;
        }

        return 0;
      }

      int Manager::process_journal_entry(const fifo_journal_entry_t& entry)
      {
        switch (entry.op) {
          case fifo_journal_entry_t::Op::OP_CREATE:
          return create_part(entry.part_num, entry.part_tag);
          case fifo_journal_entry_t::Op::OP_REMOVE:
          return remove_part(entry.part_num, entry.part_tag);
        default:
          /* nothing to do */
          break;
        }

        return 0;
      }

      int Manager::process_journal_entries(vector<fifo_journal_entry_t> *processed)
      {
        for (auto& iter : meta_info.journal) {
          auto& entry = iter.second;
          int r = process_journal_entry(entry);
          if (r < 0) {
            ldout(cct, 10) << __func__ << "(): ERROR: failed processing journal entry for part=" << entry.part_num << dendl;
          } else {
            processed->push_back(entry);
          }
        }

        return 0;
      }

      int Manager::process_journal()
      {
        vector<fifo_journal_entry_t> processed;

        int r = process_journal_entries(&processed);
        if (r < 0) {
          return r;
        }

        if (processed.empty()) {
          return 0;
        }

#define RACE_RETRY 10

        int i;

        for (i = 0; i < RACE_RETRY; ++i) {
          bool canceled;
          r = update_meta(FIFO::MetaUpdateParams()
                          .journal_entries_rm(processed),
                          &canceled);
          if (r < 0) {
            return r;
          }

          if (canceled) {
            vector<fifo_journal_entry_t> new_processed;

            for (auto& e : processed) {
              auto jiter = meta_info.journal.find(e.part_num);
              if (jiter == meta_info.journal.end() || /* journal entry was already processed */
                  !(jiter->second == e)) {
                continue;
              }
              
              new_processed.push_back(e);
            }
            processed = std::move(new_processed);
            continue;
          }
          break;
        }
        if (i == RACE_RETRY) {
          ldout(cct, 0) << "ERROR: " << __func__ << "(): race check failed too many times, likely a bug" << dendl;
          return -ECANCELED;
        }
        return 0;
      }

      int Manager::prepare_new_part()
      {
        fifo_journal_entry_t jentry;

        meta_info.prepare_next_journal_entry(&jentry);

        int r;
        bool canceled;

        int i;

        for (i = 0; i < RACE_RETRY; ++i) {
          r = update_meta(FIFO::MetaUpdateParams()
                          .journal_entry_add(jentry),
                          &canceled);
          if (r < 0) {
            return r;
          }

          if (canceled) {
            if (meta_info.max_push_part_num >= jentry.part_num) { /* raced, but new part was already written */
              return 0;
            }

            auto iter = meta_info.journal.find(jentry.part_num);
            if (iter == meta_info.journal.end()) {
              continue;
            }
          }
          break;
        }
        if (i == RACE_RETRY) {
          ldout(cct, 0) << "ERROR: " << __func__ << "(): race check failed too many times, likely a bug" << dendl;
          return -ECANCELED;
        }

        r = process_journal();
        if (r < 0) {
          return r;
        }

        return 0;
      }

      int Manager::prepare_new_head()
      {
        int64_t new_head_num = meta_info.head_part_num + 1;

        if (meta_info.max_push_part_num < new_head_num) {
          int r = prepare_new_part();
          if (r < 0) {
            return r;
          }

          if (meta_info.max_push_part_num < new_head_num) {
            ldout(cct, 0) << "ERROR: " << __func__ << ": after new part creation: meta_info.max_push_part_num="
              << meta_info.max_push_part_num << " new_head_num=" << meta_info.max_push_part_num << dendl;
            return -EIO;
          }
        }

        int i;

        for (i = 0; i < RACE_RETRY; ++i) {
          bool canceled;
          int r = update_meta(FIFO::MetaUpdateParams()
                          .head_part_num(new_head_num),
                          &canceled);
          if (r < 0) {
            return r;
          }

          if (canceled) {
            if (meta_info.head_part_num < new_head_num) {
              continue;
            }
          }
          break;
        }
        if (i == RACE_RETRY) {
          ldout(cct, 0) << "ERROR: " << __func__ << "(): race check failed too many times, likely a bug" << dendl;
          return -ECANCELED;
        }

        
        return 0;
      }

      int Manager::open(bool create,
                        std::optional<FIFO::MetaCreateParams> create_params)
      {
        if (create) {
          librados::ObjectWriteOperation op;

          FIFO::MetaCreateParams default_params;
          FIFO::MetaCreateParams *params = (create_params ? &(*create_params) : &default_params);

          int r = FIFO::meta_create(&op, *params);
          if (r < 0) {
            return r;
          }

          r = ioctx->operate(meta_oid, &op);
          if (r < 0) {
            return r;
          }
        }

        FIFO::MetaGetParams get_params;
        if (create_params) {
          get_params.objv(create_params->state.objv);
        }
        int r = FIFO::meta_get(*ioctx,
                               meta_oid,
                               get_params,
                               &meta_info);
        if (r < 0) {
          return r;
        }

        is_open = true;

        return 0;
      }

      int Manager::push_entry(int64_t part_num, bufferlist& bl)
      {
        librados::ObjectWriteOperation op;

        int r = FIFO::push_part(&op, FIFO::PushPartParams()
                                          .tag(meta_info.head_tag)
                                          .data(bl));
        if (r < 0) {
          return r;
        }

        r = ioctx->operate(meta_info.part_oid(part_num), &op);
        if (r < 0) {
          return r;
        }

        return 0;
      }

      int Manager::push(bufferlist& bl)
      {
        if (!is_open) {
          return -EINVAL;
        }

        int r;

        if (meta_info.need_new_head()) {
          r = prepare_new_head();
          if (r < 0) {
            return r;
          }
        }

        int i;

        for (i = 0; i < RACE_RETRY; ++i) {
          r = push_entry(meta_info.head_part_num, bl);
          if (r == -ERANGE) {
            r = prepare_new_head();
            if (r < 0) {
              return r;
            }
            continue;
          }
          if (r < 0) {
            return r;
          }
          break;
        }
        if (i == RACE_RETRY) {
          ldout(cct, 0) << "ERROR: " << __func__ << "(): race check failed too many times, likely a bug" << dendl;
          return -ECANCELED;
        }

        return 0;
      }

      int Manager::list(int max_entries,
                        std::optional<string> marker,
                        vector<fifo_entry> *result,
                       bool *more)
      {
        if (!is_open) {
          return -EINVAL;
        }

        *more = false;

        int64_t part_num = meta_info.tail_part_num;
        uint64_t ofs = 0;

        if (marker) {
          if (!parse_marker(*marker, &part_num, &ofs)) {
            ldout(cct, 20) << __func__ << "(): failed to parse marker (" << *marker << ")" << dendl;
            return -EINVAL;
          }
        }

        result->clear();
        result->reserve(max_entries);

        bool part_more{false};
        while (max_entries > 0 &&
               part_num <= meta_info.head_part_num) {
          std::vector<cls_fifo_part_list_op_reply::entry> entries;
          int r = FIFO::list_part(*ioctx,
                                  meta_info.part_oid(part_num),
                                  FIFO::ListPartParams()
                                  .ofs(ofs)
                                  .max_entries(max_entries),
                                  &entries,
                                  &part_more,
                                  nullptr);
          if (r == -ENOENT) {
            r = read_meta();
            if (r < 0) {
              return r;
            }

            if (part_num < meta_info.tail_part_num) {
              /* raced with trim? */
              part_num = meta_info.tail_part_num;
              ofs = 0;
              continue;
            }

            /* assuming part was not written yet, so end of data */

            return 0;
          }
          if (r < 0) {
            ldout(cct, 20) << __func__ << "(): FIFO::list_part() on oid=" << meta_info.part_oid(part_num) << " returned r=" << r << dendl;
            return r;
          }

          for (auto& entry : entries) {
            fifo_entry e;
            e.data = std::move(entry.data);
            e.marker = craft_marker(part_num, entry.ofs);
            e.mtime = entry.mtime;

            result->push_back(e);
          }
          max_entries -= entries.size();

          if (!part_more) {
            ++part_num;
            ofs = 0;
          }
        }

        *more = part_more;

        return 0;
      }
    } // namespace fifo
  } // namespace cls
} // namespace rados

