// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "include/types.h"
#include "cls/timeindex/cls_timeindex_ops.h"
#include "include/rados/librados.hpp"
#include "include/compat.h"


using namespace librados;


void cls_timeindex_add(librados::ObjectWriteOperation& op, list<cls_timeindex_entry>& entries)
{
  bufferlist in;
  cls_timeindex_add_op call;

  call.entries = entries;

  ::encode(call, in);
  op.exec("timeindex", "add", in);
}

void cls_timeindex_add(librados::ObjectWriteOperation& op, cls_timeindex_entry& entry)
{
  bufferlist in;
  cls_timeindex_add_op call;

  call.entries.push_back(entry);

  ::encode(call, in);
  op.exec("timeindex", "add", in);
}

void cls_timeindex_add_prepare_entry(cls_timeindex_entry& entry,
                                     const utime_t& key_timestamp,
                                     const string& key_ext,
                                     const bufferlist& bl)
{
  entry.key_ts  = key_timestamp;
  entry.key_ext = key_ext;
  entry.value   = bl;
}

void cls_timeindex_add(librados::ObjectWriteOperation& op,
                       const utime_t& key_timestamp,
                       const string& key_ext,
                       const bufferlist& bl)
{
  cls_timeindex_entry entry;

  cls_timeindex_add_prepare_entry(entry, key_timestamp, key_ext, bl);
  cls_timeindex_add(op, entry);
}

void cls_timeindex_trim(librados::ObjectWriteOperation& op,
                        const utime_t& from_time,
                        const utime_t& to_time,
                        const string& from_marker,
                        const string& to_marker)
{
  bufferlist in;
  cls_timeindex_trim_op call;

  call.from_time   = from_time;
  call.to_time     = to_time;
  call.from_marker = from_marker;
  call.to_marker   = to_marker;

  ::encode(call, in);

  op.exec("timeindex", "trim", in);
}

int cls_timeindex_trim(librados::IoCtx& io_ctx,
                       const string& oid,
                       const utime_t& from_time,
                       const utime_t& to_time,
                       const string& from_marker,
                       const string& to_marker)
{
  bool done = false;

  do {
    ObjectWriteOperation op;

    cls_timeindex_trim(op, from_time, to_time, from_marker, to_marker);

    int r = io_ctx.operate(oid, &op);
    if (r == -ENODATA) {
      done = true;
    } else if (r < 0) {
      return r;
    }

  } while (!done);

  return 0;
}

class TimeindexListCtx : public ObjectOperationCompletion {
  list<cls_timeindex_entry> *entries;
  string *marker;
  bool *truncated;

public:
  TimeindexListCtx(list<cls_timeindex_entry> *_entries,
                   string *_marker,
                   bool *_truncated)
    : entries(_entries), marker(_marker), truncated(_truncated) {}

  void handle_completion(int r, bufferlist& outbl) {
    if (r >= 0) {
      cls_timeindex_list_ret ret;
      try {
        bufferlist::iterator iter = outbl.begin();
        ::decode(ret, iter);
        if (entries) {
          *entries = ret.entries;
        }
        if (truncated) {
          *truncated = ret.truncated;
        }
        if (marker) {
          *marker = ret.marker;
        }
      } catch (buffer::error& err) {
        // nothing we can do about it atm
      }
    }
  }
};

void cls_timeindex_list(librados::ObjectReadOperation& op,
                        const utime_t& from,
                        const utime_t& to,
                        const string& in_marker,
                        const int max_entries,
                        list<cls_timeindex_entry>& entries,
                        string *out_marker,
                        bool *truncated)
{
  bufferlist inbl;
  cls_timeindex_list_op call;

  call.from_time = from;
  call.to_time = to;
  call.marker = in_marker;
  call.max_entries = max_entries;

  ::encode(call, inbl);

  op.exec("timeindex", "list", inbl,
          new TimeindexListCtx(&entries, out_marker, truncated));
}
