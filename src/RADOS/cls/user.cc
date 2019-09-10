// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/buffer.h"

#include "cls/user/cls_user_ops.h"

#include "user.h"

namespace RADOS::CLS::user {

void set_buckets(WriteOp& op, const std::vector<cls_user_bucket_entry>& entries,
                 bool add)
{
  cb::list in;
  cls_user_set_buckets_op call;
  call.entries = entries;
  call.add = add;
  call.time = ceph::real_clock::now();
  encode(call, in);
  op.exec("user", "set_buckets_info", in);
}

void complete_stats_sync(WriteOp& op)
{
  cb::list in;
  cls_user_complete_stats_sync_op call;
  call.time = ceph::real_clock::now();
  encode(call, in);
  op.exec("user", "complete_stats_sync", in);
}

void remove_bucket(WriteOp& op, const cls_user_bucket& bucket)
{
  cb::list in;
  cls_user_remove_bucket_op call;
  call.bucket = bucket;
  encode(call, in);
  op.exec("user", "remove_bucket", in);
}

ReadOp bucket_list(std::string_view in_marker,
		   std::string_view end_marker,
		   int max_entries, cb::list* out,
		   bs::error_code* ec)
{
  ReadOp op;
  cls_user_list_buckets_op call;
  cb::list bl;
  call.marker = in_marker;
  call.end_marker = end_marker;
  call.max_entries = max_entries;
  encode(call, bl);
  op.exec("user", "list_buckets", bl, out, ec);
  return op;
}

ReadOp get_header(cb::list* bl, bs::error_code* ec)
{
  ReadOp op;
  cls_user_get_header_op call;
  cb::list in;
  encode(call, in);
  op.exec("user", "get_header", in, bl, ec);
  return op;
}

void reset_stats(WriteOp& op)
{
  cb::list inbl;
  cls_user_reset_stats_op call;
  call.time = ceph::real_clock::now();
  encode(call, inbl);
  op.exec("user", "reset_user_stats", inbl);
}
}
