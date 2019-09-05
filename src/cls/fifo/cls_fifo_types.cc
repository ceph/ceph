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

#include "cls_fifo_types.h"

string rados::cls::fifo::fifo_info_t::part_oid(int64_t part_num)
{
  char buf[oid_prefix.size() + 32];
  snprintf(buf, sizeof(buf), "%s.%lld", oid_prefix.c_str(), (long long)part_num);

  return string(buf);
}

void rados::cls::fifo::fifo_info_t::prepare_next_journal_entry(fifo_journal_entry_t *entry, const string& tag)
{
  entry->op = fifo_journal_entry_t::Op::OP_CREATE;
  entry->part_num = max_push_part_num + 1;
  entry->part_tag = tag;
}

int rados::cls::fifo::fifo_info_t::apply_update(std::optional<uint64_t>& _tail_part_num,
                                                std::optional<uint64_t>& _head_part_num,
                                                std::optional<uint64_t>& _min_push_part_num,
                                                std::optional<uint64_t>& _max_push_part_num,
                                                std::vector<rados::cls::fifo::fifo_journal_entry_t>& journal_entries_add,
                                                std::vector<rados::cls::fifo::fifo_journal_entry_t>& journal_entries_rm,
                                                string *err)
{
  if (_tail_part_num) {
    tail_part_num = *_tail_part_num;
  }

  if (_min_push_part_num) {
    min_push_part_num = *_min_push_part_num;
  }

  if (_max_push_part_num) {
    max_push_part_num = *_max_push_part_num;
  }

  for (auto& entry : journal_entries_add) {
    auto iter = journal.find(entry.part_num);
    if (iter != journal.end() &&
        iter->second.op == entry.op) {
      /* don't allow multiple concurrent (same) operations on the same part,
         racing clients should use objv to avoid races anyway */
      if (err) {
        stringstream ss;
        ss << "multiple concurrent operations on same part are not allowed, part num=" << entry.part_num;
        *err = ss.str();
      }
      return -EINVAL;
    }

    if (entry.op == fifo_journal_entry_t::Op::OP_CREATE) {
      tags[entry.part_num] = entry.part_tag;
    }

    journal.insert(std::pair<int64_t, fifo_journal_entry_t>(entry.part_num, std::move(entry)));
  }

  for (auto& entry : journal_entries_rm) {
    journal.erase(entry.part_num);
  }

  if (_head_part_num) {
    tags.erase(head_part_num);
    head_part_num = *_head_part_num;
    auto iter = tags.find(head_part_num);
    if (iter != tags.end()) {
      head_tag = iter->second;
    } else {
      head_tag.erase();
    }
  }

  return 0;
}
