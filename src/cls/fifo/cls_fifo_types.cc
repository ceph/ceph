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
