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

#include "auth/Crypto.h"

using namespace librados;

#include "cls/fifo/cls_fifo_ops.h"
#include "cls/fifo/cls_fifo_client.h"


#define dout_subsys ceph_subsys_objclass


namespace rados {
  namespace cls {
    namespace fifo {
      int ClsFIFO::meta_create(librados::ObjectWriteOperation *rados_op,
                               const string& id,
                               const MetaCreateParams& params) {
        cls_fifo_meta_create_op op;

        auto& state = params.state;

        if (id.empty()) {
          return -EINVAL;
        }

        op.id = id;
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

      int ClsFIFO::meta_get(librados::IoCtx& ioctx,
                            const string& oid,
                            const MetaGetParams& params,
                            fifo_info_t *result,
                            uint32_t *part_header_size,
                            uint32_t *part_entry_overhead) {
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

        if (part_header_size) {
          *part_header_size = reply.part_header_size;
        }

        if (part_entry_overhead) {
          *part_entry_overhead = reply.part_entry_overhead;
        }

        return 0;
      }

      int ClsFIFO::meta_update(librados::ObjectWriteOperation *rados_op,
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

      int ClsFIFO::part_init(librados::ObjectWriteOperation *rados_op,
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

      int ClsFIFO::push_part(librados::ObjectWriteOperation *rados_op,
                             const PushPartParams& params) {
        cls_fifo_part_push_op op;

        auto& state = params.state;

        if (state.tag.empty()) {
          return -EINVAL;
        }

        op.tag = state.tag;
        op.data_bufs = state.data_bufs;
        op.total_len = state.total_len;

        bufferlist in;
        encode(op, in);
        rados_op->exec("fifo", "fifo_part_push", in);

        return 0;
      }

      int ClsFIFO::trim_part(librados::ObjectWriteOperation *rados_op,
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

      int ClsFIFO::list_part(librados::IoCtx& ioctx,
                             const string& oid,
                             const ListPartParams& params,
                             std::vector<cls_fifo_part_list_entry_t> *pentries,
                             bool *more,
                             bool *full_part,
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

        if (full_part) {
          *full_part = reply.full_part;
        }

        if (ptag) {
          *ptag = reply.tag;
        }

        return 0;
      }

      int ClsFIFO::get_part_info(librados::IoCtx& ioctx,
                                 const string& oid,
                                 rados::cls::fifo::fifo_part_header_t *header)
      {
        cls_fifo_part_get_info_op op;

        librados::ObjectReadOperation rop;

        bufferlist in;
        bufferlist out;
        int op_ret;
        encode(op, in);
        rop.exec("fifo", "fifo_part_get_info", in, &out, &op_ret);

        int r = ioctx.operate(oid, &rop, nullptr);
        if (r < 0) {
          return r;
        }

        if (op_ret < 0) {
          return op_ret;
        }

        cls_fifo_part_get_info_op_reply reply;
        auto iter = out.cbegin();
        try {
          decode(reply, iter);
        } catch (buffer::error& err) {
          return -EIO;
        }

        if (header) {
          *header = std::move(reply.header);
        }

        return 0;
      }

      string FIFO::craft_marker(int64_t part_num,
                                   uint64_t part_ofs)
      {
        char buf[64];
        snprintf(buf, sizeof(buf), "%lld:%lld", (long long)part_num, (long long)part_ofs);
        return string(buf);
      }

      bool FIFO::parse_marker(const string& marker,
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

      int FIFO::init_ioctx(librados::Rados *rados,
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

      int ClsFIFO::MetaUpdateParams::apply_update(CephContext *cct,
                                                  fifo_info_t *info)
      {
        string err;

        int r = info->apply_update(state.tail_part_num,
                                   state.head_part_num,
                                   state.min_push_part_num,
                                   state.max_push_part_num,
                                   state.journal_entries_add,
                                   state.journal_entries_rm,
                                   &err);
        if (r < 0) {
          ldout(cct, 0) << __func__ << "(): ERROR: " << err << dendl;
          return r;
        }

        ++info->objv.ver;

        return 0;
      }

      int FIFO::update_meta(ClsFIFO::MetaUpdateParams& update_params,
                            bool *canceled)
      {
        update_params.objv(meta_info.objv);

        librados::ObjectWriteOperation wop;
        int r = ClsFIFO::meta_update(&wop, update_params);
        if (r < 0) {
          return r;
        }

        r = ioctx->operate(meta_oid, &wop);
        if (r < 0 && r != -ECANCELED) {
          return r;
        }

        *canceled = (r == -ECANCELED);

        if (!*canceled) {
          r = update_params.apply_update(cct, &meta_info);
          if (r < 0) { /* should really not happen,
                          but if it does, let's treat it as if race was detected */
            *canceled = true;
          }
        }

        if (*canceled) {
          r = do_read_meta();
        }
        if (r < 0) {
          return r;
        }

        return 0;
      }

      int FIFO::do_read_meta(std::optional<fifo_objv_t> objv)
      {
        ClsFIFO::MetaGetParams get_params;
        if (objv) {
          get_params.objv(*objv);
        }
        int r = ClsFIFO::meta_get(*ioctx,
                                  meta_oid,
                                  get_params,
                                  &meta_info,
                                  &part_header_size,
                                  &part_entry_overhead);
        if (r < 0) {
          return r;
        }

        return 0;
      }

      int FIFO::create_part(int64_t part_num, const string& tag,
                            int64_t& max_part_num) {
        librados::ObjectWriteOperation op;

        op.create(true); /* exclusive */
        int r = ClsFIFO::part_init(&op,
                                   ClsFIFO::PartInitParams()
                                   .tag(tag)
                                   .data_params(meta_info.data_params));
        if (r < 0) {
          return r;
        }

        r = ioctx->operate(meta_info.part_oid(part_num), &op);
        if (r < 0) {
          return r;
        }

        if (part_num > max_part_num) {
          max_part_num = part_num;
        }

        return 0;
      }

      int FIFO::remove_part(int64_t part_num, const string& tag,
                            int64_t& tail_part_num) {
        librados::ObjectWriteOperation op;
        op.remove();
        int r = ioctx->operate(meta_info.part_oid(part_num), &op);
        if (r == -ENOENT) {
          r = 0;
        }
        if (r < 0) {
          return r;
        }

        if (part_num >= tail_part_num) {
          tail_part_num = part_num + 1;
        }

        return 0;
      }

      int FIFO::process_journal_entry(const fifo_journal_entry_t& entry,
                                      int64_t& tail_part_num,
                                      int64_t& head_part_num,
                                      int64_t& max_part_num)
      {

        switch (entry.op) {
          case fifo_journal_entry_t::Op::OP_CREATE:
            return create_part(entry.part_num, entry.part_tag, max_part_num);
          case fifo_journal_entry_t::Op::OP_SET_HEAD:
            if (entry.part_num > head_part_num) {
              head_part_num = entry.part_num;
            }
            return 0;
          case fifo_journal_entry_t::Op::OP_REMOVE:
            return remove_part(entry.part_num, entry.part_tag, tail_part_num);
        default:
          /* nothing to do */
          break;
        }

        return -EIO;
      }

      int FIFO::process_journal_entries(vector<fifo_journal_entry_t> *processed,
                                        int64_t& tail_part_num,
                                        int64_t& head_part_num,
                                        int64_t& max_part_num)
      {
        for (auto& iter : meta_info.journal) {
          auto& entry = iter.second;
          int r = process_journal_entry(entry, tail_part_num, head_part_num, max_part_num);
          if (r < 0) {
            ldout(cct, 10) << __func__ << "(): ERROR: failed processing journal entry for part=" << entry.part_num << dendl;
          } else {
            processed->push_back(entry);
          }
        }

        return 0;
      }

      int FIFO::process_journal()
      {
        vector<fifo_journal_entry_t> processed;

        int64_t new_tail = meta_info.tail_part_num;
        int64_t new_head = meta_info.head_part_num;
        int64_t new_max = meta_info.max_push_part_num;

        int r = process_journal_entries(&processed, new_tail, new_head, new_max);
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

          std::optional<int64_t> tail_part_num;
          std::optional<int64_t> head_part_num;
          std::optional<int64_t> max_part_num;

          if (new_tail > meta_info.tail_part_num) {
            tail_part_num = new_tail;
          }

          if (new_head > meta_info.head_part_num) {
            head_part_num = new_head;
          }

          if (new_max > meta_info.max_push_part_num) {
            max_part_num = new_max;
          }

          if (processed.empty() &&
              !tail_part_num &&
              !max_part_num) {
            /* nothing to update anymore */
            break;
          }

          r = update_meta(ClsFIFO::MetaUpdateParams()
                          .journal_entries_rm(processed)
                          .tail_part_num(tail_part_num)
                          .head_part_num(head_part_num)
                          .max_push_part_num(max_part_num),
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

      static const char alphanum_plain_table[]="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

      void gen_rand_alphanumeric_plain(CephContext *cct, char *dest, int size) /* size should be the required string size + 1 */
      {
        cct->random()->get_bytes(dest, size);

        int i;
        for (i = 0; i < size - 1; i++) {
          int pos = (unsigned)dest[i];
          dest[i] = alphanum_plain_table[pos % (sizeof(alphanum_plain_table) - 1)];
        }
        dest[i] = '\0';
      }

      static string generate_tag(CephContext *cct)
      {
#define HEADER_TAG_SIZE 16
        char buf[HEADER_TAG_SIZE + 1];
        buf[HEADER_TAG_SIZE] = 0;
        gen_rand_alphanumeric_plain(cct, buf, sizeof(buf));
        return string(buf);
      }

      int FIFO::prepare_new_part(bool is_head)
      {
        fifo_journal_entry_t jentry;

        meta_info.prepare_next_journal_entry(&jentry, generate_tag(cct));

        int64_t new_head_part_num = meta_info.head_part_num;

        std::optional<fifo_journal_entry_t> new_head_jentry;
        if (is_head) {
          new_head_jentry = jentry;
          new_head_jentry->op = fifo_journal_entry_t::OP_SET_HEAD;
          new_head_part_num = jentry.part_num;
        }

        int r;
        bool canceled;

        int i;

        for (i = 0; i < RACE_RETRY; ++i) {
          r = update_meta(ClsFIFO::MetaUpdateParams()
                          .journal_entry_add(jentry)
                          .journal_entry_add(new_head_jentry),
                          &canceled);
          if (r < 0) {
            return r;
          }

          if (canceled) {
            if (meta_info.max_push_part_num >= jentry.part_num &&
                meta_info.head_part_num >= new_head_part_num) { /* raced, but new part was already written */
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

      int FIFO::prepare_new_head()
      {
        int64_t new_head_num = meta_info.head_part_num + 1;

        if (meta_info.max_push_part_num < new_head_num) {
          int r = prepare_new_part(true);
          if (r < 0) {
            return r;
          }

          if (meta_info.max_push_part_num < new_head_num) {
            ldout(cct, 0) << "ERROR: " << __func__ << ": after new part creation: meta_info.max_push_part_num="
              << meta_info.max_push_part_num << " new_head_num=" << meta_info.max_push_part_num << dendl;
            return -EIO;
          }

          return 0;
        }

        int i;

        for (i = 0; i < RACE_RETRY; ++i) {
          bool canceled;
          int r = update_meta(ClsFIFO::MetaUpdateParams()
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

      int FIFO::open(bool create,
                        std::optional<ClsFIFO::MetaCreateParams> create_params)
      {
        if (!ioctx) {
          return -EINVAL;
        }

        if (create) {
          librados::ObjectWriteOperation op;

          ClsFIFO::MetaCreateParams default_params;
          ClsFIFO::MetaCreateParams *params = (create_params ? &(*create_params) : &default_params);

          int r = ClsFIFO::meta_create(&op, id, *params);
          if (r < 0) {
            return r;
          }

          r = ioctx->operate(meta_oid, &op);
          if (r < 0) {
            return r;
          }
        }

        std::optional<fifo_objv_t> objv = (create_params ?  create_params->state.objv : nullopt);

        int r = do_read_meta(objv);
        if (r < 0) {
          return r;
        }

        is_open = true;

        return 0;
      }

      int FIFO::read_meta(std::optional<fifo_objv_t> objv)
      {
        if (!is_open) {
          return -EINVAL;
        }

        return do_read_meta(objv);
      }

      int FIFO::push_entries(int64_t part_num, std::vector<bufferlist>& data_bufs)
      {
        if (!is_open) {
          return -EINVAL;
        }

        librados::ObjectWriteOperation op;

        int r = ClsFIFO::push_part(&op, ClsFIFO::PushPartParams()
                                   .tag(meta_info.head_tag)
                                   .data_bufs(data_bufs));
        if (r < 0) {
          return r;
        }

        r = ioctx->operate(meta_info.part_oid(part_num), &op);
        if (r < 0) {
          return r;
        }

        return 0;
      }

      int FIFO::trim_part(int64_t part_num,
                          uint64_t ofs,
                          std::optional<string> tag)
      {
        if (!is_open) {
          return -EINVAL;
        }

        librados::ObjectWriteOperation op;

        int r = ClsFIFO::trim_part(&op, ClsFIFO::TrimPartParams()
                                        .tag(tag)
                                        .ofs(ofs));
        if (r < 0) {
          return r;
        }

        r = ioctx->operate(meta_info.part_oid(part_num), &op);
        if (r < 0) {
          return r;
        }

        return 0;
      }

      int FIFO::push(bufferlist& bl)
      {
        std::vector<bufferlist> data_bufs;
        data_bufs.push_back(bl);

        return push(data_bufs);
      }

      int FIFO::push(vector<bufferlist>& data_bufs)
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

        auto iter = data_bufs.begin();

        while (iter != data_bufs.end()) {
          uint64_t batch_len = 0;

          vector<bufferlist> batch;

          for (; iter != data_bufs.end(); ++iter) {
            auto& data = *iter;
            auto data_len = data.length();
            auto max_entry_size = meta_info.data_params.max_entry_size;

            if (data_len > max_entry_size) {
              ldout(cct, 10) << __func__ << "(): entry too large: " << data_len << " > " <<  meta_info.data_params.max_entry_size << dendl;
              return -EINVAL;
            }

            if (batch_len + data_len > max_entry_size) {
              break;
            }

            batch_len +=  data_len + part_entry_overhead; /* we can send entry with data_len up to max_entry_size,
                                                             however, we want to also account the overhead when dealing
                                                             with multiple entries. Previous check doesn't account
                                                             for overhead on purpose. */

            batch.push_back(data);
          }


          for (i = 0; i < RACE_RETRY; ++i) {
            r = push_entries(meta_info.head_part_num, batch);
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
        }

        return 0;
      }

      int FIFO::list(int max_entries,
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
        bool part_full{false};

        while (max_entries > 0) {
          std::vector<cls_fifo_part_list_entry_t> entries;
          int r = ClsFIFO::list_part(*ioctx,
                                     meta_info.part_oid(part_num),
                                     ClsFIFO::ListPartParams()
                                     .ofs(ofs)
                                     .max_entries(max_entries),
                                     &entries,
                                     &part_more,
                                     &part_full,
                                     nullptr);
          if (r == -ENOENT) {
            r = do_read_meta();
            if (r < 0) {
              return r;
            }

            if (part_num < meta_info.tail_part_num) {
              /* raced with trim? restart */
              result->clear();
              part_num = meta_info.tail_part_num;
              ofs = 0;
              continue;
            }

            /* assuming part was not written yet, so end of data */

            *more = false;

            return 0;
          }
          if (r < 0) {
            ldout(cct, 20) << __func__ << "(): ClsFIFO::list_part() on oid=" << meta_info.part_oid(part_num) << " returned r=" << r << dendl;
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

          if (max_entries > 0 &&
              part_more) {
            continue;
          }

          if (!part_full) { /* head part is not full */
            break;
          }

          ++part_num;
          ofs = 0;
        }

        *more = part_full || part_more;

        return 0;
      }

      int FIFO::trim(const string& marker)
      {
        if (!is_open) {
          return -EINVAL;
        }

        int64_t part_num;
        uint64_t ofs;

        if (!parse_marker(marker, &part_num, &ofs)) {
          ldout(cct, 20) << __func__ << "(): failed to parse marker: marker=" << marker << dendl;
          return -EINVAL;
        }

        for (int64_t pn = meta_info.tail_part_num; pn < part_num; ++pn) {
          int r = trim_part(pn, meta_info.data_params.max_part_size, std::nullopt);
          if (r < 0 &&
              r != -ENOENT) {
            ldout(cct, 0) << __func__ << "(): ERROR: trim_part() on part=" << pn << " returned r=" << r << dendl;
            return r;
          }
        }

        int r = trim_part(part_num, ofs, std::nullopt);
        if (r < 0 &&
            r != -ENOENT) {
          ldout(cct, 0) << __func__ << "(): ERROR: trim_part() on part=" << part_num << " returned r=" << r << dendl;
          return r;
        }

        if (part_num <= meta_info.tail_part_num) {
          /* don't need to modify meta info */
          return 0;
        }

        int i;

        for (i = 0; i < RACE_RETRY; ++i) {
          bool canceled;
          int r = update_meta(ClsFIFO::MetaUpdateParams()
                              .tail_part_num(part_num),
                              &canceled);
          if (r < 0) {
            return r;
          }

          if (canceled) {
            if (meta_info.tail_part_num < part_num) {
              continue;
            }
          }
          break;

          if (i == RACE_RETRY) {
            ldout(cct, 0) << "ERROR: " << __func__ << "(): race check failed too many times, likely a bug" << dendl;
            return -ECANCELED;
          }
        }

        return 0;
      }

      int FIFO::get_part_info(int64_t part_num,
                              fifo_part_info *result)
      {
        if (!is_open) {
          return -EINVAL;
        }

        fifo_part_header_t header;

        int r = ClsFIFO::get_part_info(*ioctx,
                                       meta_info.part_oid(part_num),
                                       &header);
        if (r < 0) {
          return r;
        }

        *result = std::move(header);

        return 0;
      }

    } // namespace fifo
  } // namespace cls
} // namespace rados

