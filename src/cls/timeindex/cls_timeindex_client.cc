// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "cls/timeindex/cls_timeindex_ops.h"
#include "cls/timeindex/cls_timeindex_client.h"
#include "include/compat.h"

void cls_timeindex_add(
  librados::ObjectWriteOperation& op,
  std::list<cls_timeindex_entry>& entries)
{
  librados::bufferlist in;
  cls_timeindex_add_op call;
  call.entries = entries;

  encode(call, in);
  op.exec("timeindex", "add", in);
}

void cls_timeindex_add(
  librados::ObjectWriteOperation& op,
  cls_timeindex_entry& entry)
{
  librados::bufferlist in;
  cls_timeindex_add_op call;
  call.entries.push_back(entry);

  encode(call, in);
  op.exec("timeindex", "add", in);
}

void cls_timeindex_add_prepare_entry(
  cls_timeindex_entry& entry,
  const utime_t& key_timestamp,
  const std::string& key_ext,
  const librados::bufferlist& bl)
{
  entry.key_ts = key_timestamp;
  entry.key_ext = key_ext;
  entry.value = bl;
}

void cls_timeindex_add(
  librados::ObjectWriteOperation& op,
  const utime_t& key_timestamp,
  const std::string& key_ext,
  const librados::bufferlist& bl)
{
  cls_timeindex_entry entry;
  cls_timeindex_add_prepare_entry(entry, key_timestamp, key_ext, bl);
  cls_timeindex_add(op, entry);
}

void cls_timeindex_trim(
  librados::ObjectWriteOperation& op,
  const utime_t& from_time,
  const utime_t& to_time,
  const std::string& from_marker,
  const std::string& to_marker)
{
  librados::bufferlist in;
  cls_timeindex_trim_op call;
  call.from_time = from_time;
  call.to_time = to_time;
  call.from_marker = from_marker;
  call.to_marker = to_marker;

  encode(call, in);

  op.exec("timeindex", "trim", in);
}

int cls_timeindex_trim(
  librados::IoCtx& io_ctx,
  const std::string& oid,
  const utime_t& from_time,
  const utime_t& to_time,
  const std::string& from_marker,
  const std::string& to_marker)
{
  bool done = false;

  do {
    librados::ObjectWriteOperation op;
    cls_timeindex_trim(op, from_time, to_time, from_marker, to_marker);
    int r = io_ctx.operate(oid, &op);

    if (r == -ENODATA)
      done = true;
    else if (r < 0)
      return r;
  } while (!done);

  return 0;
}

void cls_timeindex_list(
  librados::ObjectReadOperation& op,
  const utime_t& from,
  const utime_t& to,
  const std::string& in_marker,
  const int max_entries,
  std::list<cls_timeindex_entry>& entries,
  std::string *out_marker,
  bool *truncated)
{
  librados::bufferlist in;
  cls_timeindex_list_op call;
  call.from_time = from;
  call.to_time = to;
  call.marker = in_marker;
  call.max_entries = max_entries;

  encode(call, in);

  op.exec("timeindex", "list", in,
          new TimeindexListCtx(&entries, out_marker, truncated));
}
