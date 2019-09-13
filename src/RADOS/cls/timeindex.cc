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

#include "timeindex.h"

namespace RADOS::CLS::timeindex {
cls_timeindex_entry add_prepare_entry(ceph::real_time key_timestamp,
                                      std::optional<std::string_view> key_ext,
                                      cb::list&& bl)
{
  cls_timeindex_entry entry;
  entry.key_ts = key_timestamp;
  if (key_ext)
    entry.key_ext = *key_ext;
  entry.value = std::move(bl);
  return entry;
}

WriteOp add(std::vector<cls_timeindex_entry>&& entries)
{
  WriteOp op;
  cb::list in;
  cls_timeindex_add_op call;
  call.entries = entries;
  encode(call, in);
  op.exec("timeindex", "add", in);
  return op;
}

WriteOp add(cls_timeindex_entry&& entry)
{
  WriteOp op;
  cb::list in;
  cls_timeindex_add_op call;
  call.entries.push_back(entry);
  encode(call, in);
  op.exec("timeindex", "add", in);
  return op;
}

WriteOp add(ceph::real_time timestamp, std::optional<std::string_view> name,
	    cb::list&& bl)
{
  WriteOp op;
  cb::list in;
  cls_timeindex_add_op call;
  call.entries.push_back(add_prepare_entry(timestamp, name, std::move(bl)));
  encode(call, in);
  op.exec("timeindex", "add", in);
  return op;
}

ReadOp list(ceph::real_time from, ceph::real_time to,
	    std::string_view in_marker, int max_entries,
	    cb::list* bl, bs::error_code* ec)
{
  ReadOp op;
  cls_timeindex_list_op call;
  cb::list in;
  call.from_time = from;
  call.to_time = to;
  call.marker = in_marker;
  call.max_entries = max_entries;

  encode(call, in);

  op.exec("timeindex", "list", in, bl, ec);
  return op;
}

WriteOp trim(ceph::real_time from_time, ceph::real_time to_time,
	     std::optional<std::string_view> from_marker,
	     std::optional<std::string_view> to_marker)
{
  WriteOp op;
  cls_timeindex_trim_op call;
  call.from_time = from_time;
  call.to_time = to_time;
  if (from_marker)
    call.from_marker = *from_marker;
  if (to_marker)
    call.to_marker = *to_marker;
  cb::list in;

  op.exec("timeindex", "trim", in);
  return op;
}
}
